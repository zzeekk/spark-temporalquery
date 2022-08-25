package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalHelpers._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.annotation.tailrec
import scala.reflect.runtime.universe._

object IntervalQueryImpl extends Logging {

  // helpers

  /**
   * Recursively search logical plan for alias, as long as nodes are unary (only one child).
   */
  @tailrec
  private def getAliasFromPlan(plan: LogicalPlan): Option[String] = plan match {
    case x: SubqueryAlias => Some(x.alias)
    case x: UnaryNode => getAliasFromPlan(x.child)
    case x => None
  }

  /**
   * Search for alias in DataFrame's logical plan.
   */
  def getAlias(df: DataFrame): Option[String] = getAliasFromPlan(df.queryExecution.analyzed)

  /**
   * Keep Dataframe Alias over any given transform.
   */
  def keepAlias(df: DataFrame, transform: (DataFrame) => DataFrame): DataFrame = {
    getAlias(df).map(transform(df).alias).getOrElse(transform(df))
  }

  /**
   * Create a column reference using the DataFrame's alias if existing.
   */
  private[temporalquery] def getColumnRef(df: DataFrame, colName: String): Column = {
    getAlias(df).map(alias => col(s"$alias.$colName")).getOrElse(df(colName))
  }

  /**
   * Create key condition. If possible use alias to reduce ambiguous column errors.
   */
  private[temporalquery] def createAliasKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and getColumnRef(df1,key)===getColumnRef(df2,key) }
  }

  /**
   * Create key condition using renamed columns to avoid ambiguous column errors.
   * DataFrame df1 and df2 must be prepared with `<key>__1` and `<key>__2` columns for each key.
   */
  private def createRenamedKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and col(s"$key$joinColPostFix1")===col(s"$key$joinColPostFix2") }
  }
  private def renameKeys(df: DataFrame, keys:Seq[String], postFix: String): DataFrame = keys.foldLeft(df){
    case (df, key) => df.withColumnRenamed(key, s"$key$postFix")
  }
  private val joinColPostFix1 = "__1"
  private val joinColPostFix2 = "__2"

  private[temporalquery] def roundIntervalsToDiscreteTime[T: Ordering: TypeTag](df: DataFrame)
                                                                               (implicit tc: ClosedIntervalQueryConfig[T]): DataFrame= {
    df.withColumn(tc.fromColName, tc.intervalDef.getCeilExpr(tc.fromCol))
      .withColumn(tc.toColName, tc.intervalDef.getFloorExpr(tc.toCol))
      .where(tc.isValidIntervalExpr)
      // return columns in same order as provided
      .select(df.columns.map(col):_*)
  }

  private[temporalquery] def transformHalfOpenToClosedIntervals[T: Ordering: TypeTag](df: DataFrame)
                                                                                     (implicit tc: ClosedIntervalQueryConfig[T]): DataFrame= {
    df.withColumn(tc.fromColName, tc.intervalDef.getCeilExpr(tc.fromCol))
      .withColumn(tc.toColName, tc.intervalDef.getPredecessorExpr(tc.toCol))
      .where(tc.isValidIntervalExpr)
      // return columns in same order as provided
      .select(df.columns.map(col):_*)
  }

  /**
   * join two interval data frames
   * keys must occur in both data frames df1 and df2 and are consolidated in the result frame with df1 precedence over df2 using coalesce.
   * This is used to implement "natural join" and "join using" sql behaviour.
   */
  private[temporalquery] def joinIntervals[T: Ordering: TypeTag](df1: DataFrame, df2: DataFrame, keys: Seq[String], joinType:String = "inner", additionalJoinCondition: Column = lit(true))
                                                                (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]) : DataFrame = {
    require(!(df2.columns.contains(tc.fromColName2) || df2.columns.contains(tc.toColName2)),
      s"(joinIntervals) Your right-dataframe must not contain columns named ${tc.fromColName2} or ${tc.toColName2}! df.columns = ${df2.columns.mkString(",")}")
    require(keys.diff(df1.columns).isEmpty, s"(joinIntervals) Your left-dataframe doesn't contain column to consolidate ${keys.diff(df1.columns).mkString(" and ")}")
    require(keys.diff(df2.columns).isEmpty, s"(joinIntervals) Your right-dataframe doesn't contain column to consolidate ${keys.diff(df2.columns).mkString(" and ")}")

    // interval join
    // rename keys to avoid column ambiguous errors
    val df1Renamed = renameKeys(df1,keys,joinColPostFix1)
    val df2Renamed = renameKeys(renameIntervalCols2nd(df2),keys,joinColPostFix2)
    val keyCondition = createRenamedKeyCondition(df1Renamed, df2Renamed, keys)
    val dfJoined = df1Renamed.join( df2Renamed, keyCondition and additionalJoinCondition and tc.joinIntervalExpr2(df1Renamed, df2Renamed), joinType)

    // select final schema
    val commonColNames = keys
    val commonCols = keys.map(key => coalesce(df1Renamed(s"$key$joinColPostFix1"),df2Renamed(s"$key$joinColPostFix2")).as(key))
    val colsDf1 = df1.columns.diff(commonColNames ++ tc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(commonColNames ++ tc.technicalColNames).map(df2(_))
    val timeColumns = Seq(greatest(tc.fromCol, tc.fromCol2).as(tc.fromColName),least(tc.toCol, tc.toCol2).as(tc.toColName))
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    dfJoined.select(selCols:_*)
  }
  private[temporalquery] def joinIntervalsWithKeysImpl[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keys:Seq[String], joinType:String = "inner" )
                                                                            (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]) : DataFrame = {
    joinIntervals(df1, df2, keys, joinType)
  }

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extend:Boolean)
                                                                      (implicit ss:SparkSession, tc:IntervalQueryConfig[_,_]) : DataFrame = {
    val ptColName = "_pt"

    require(!df.columns.contains(ptColName), s"(buildIntervalRanges) Your dataframe must not contain column $ptColName! df.columns = ${df.columns.mkString(",")}")

    val keyCols = keys.map(col)
    // get start/end-points for every key
    val dfPoints = df
      .where(tc.isValidIntervalExpr) // filter invalid intervals
      .select(keyCols :+ tc.fromCol.as(ptColName): _*).union(df.select(keyCols :+ tc.getSuccessorIntervalStartExpr(tc.toCol).as(ptColName): _*))
    // if desired, extend every key with min/maxDate-points
    val dfPointsExt = if (extend) {
      dfPoints
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(tc.lowerHorizon)))
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(tc.upperHorizon)))
        .distinct
        .where(tc.isInBoundariesExpr(col(ptColName)))
    } else dfPoints.distinct
    // build ranges
    dfPointsExt
      .withColumnRenamed(ptColName, tc.fromColName)
      .withColumn(tc.toColName, tc.getPredecessorIntervalEndExpr(lead(tc.fromCol, 1).over(Window.partitionBy(keys.map(col): _*).orderBy(tc.fromCol))))
      .where(tc.toCol.isNotNull)
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private[temporalquery] def cleanupExtendIntervals[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)]
                                                  , rnkFilter:Boolean, extend: Boolean = true, fillGapsWithNull: Boolean = true )
                                                   (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]) : DataFrame = {
    if(extend && !fillGapsWithNull) logger.warn("cleanupExtendIntervals: extend=true has no effect if fillGapsWithNull=false!")

    require(!df.columns.contains(tc.fromColName2) && !df.columns.contains(tc.toColName2) && !df.columns.contains(tc.definedColName),
      s"(cleanupExtendIntervals) Your dataframe must not contain columns named ${tc.fromColName2}, ${tc.toColName2} or ${tc.definedColName}! df.columns = ${df.columns.mkString(",")}")

    keepAlias(df, df => {
      // use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions
      val df2nd = copyIntervalCols2nd(df)
      val fenestra = Window.partitionBy(keys.map(col) :+ tc.fromCol2: _*)

      val dfJoin = unifyIntervalRanges(df2nd, keys, extend, fillGapsWithNull)(implicitly[Ordering[T]], implicitly[TypeTag[T]], ss, tc.config2)
        .withColumn(tc.definedColName, tc.toCol.isNotNull)
        .withColumn(tc.fromColName, coalesce(tc.fromCol, tc.fromCol2))
        .withColumn(tc.toColName, coalesce(tc.toCol, tc.toCol2))

      // add aggregations if defined, implemented as analytical functions...
      val dfAgg = aggExpressions.foldLeft(dfJoin) {
        case (df_acc, (name, expr)) => df_acc.withColumn(name, expr.over(fenestra))
      }

      // Prioritize and clean overlaps
      val rnkColName = "_rnk"
      val dfClean = if (rnkExpressions.nonEmpty) {
        require(!df.columns.contains(rnkColName), s"(cleanupExtendIntervals) Your dataframe must not contain columns named $rnkColName if rnkExpressions are defined! df.columns = ${df.columns.mkString(",")}")
        val df_rnk = dfAgg.withColumn(rnkColName, row_number.over(fenestra.orderBy(rnkExpressions: _*)))
        if (rnkFilter) df_rnk.where(col(rnkColName) === 1) else df_rnk
      } else dfAgg

      // select final schema
      val selCols: Seq[Column] = keys.map(dfClean(_)) ++
        df.columns.diff(keys ++ tc.technicalColNames).map(dfClean(_)) ++
        aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Seq()) :+
        dfClean(tc.fromColName2).as(tc.fromColName) :+ dfClean(tc.toColName2).as(tc.toColName) :+ tc.definedCol

      dfClean.select(selCols: _*)
    })
  }

  /**
   * outer join
   * @param doCleanupExtend set to false if cleanupExtendsIntervals is already executed on the input DataFrames.
   */
  private[temporalquery] def outerJoinIntervalsWithKey[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column, joinType:String, doCleanupExtend: Boolean)
                                                      (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]): DataFrame = {
    // extend data frames
    val df1Extended = if ((joinType=="full" || joinType=="right") && doCleanupExtend) cleanupExtendIntervals( df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Seq(), rnkFilter=true ).drop(tc.definedColName) else df1
    val df2Extended = if ((joinType=="full" || joinType=="left") && doCleanupExtend) cleanupExtendIntervals( df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Seq(), rnkFilter=true ).drop(tc.definedColName) else df2
    // join df1 & df2
    joinIntervals(df1Extended, df2Extended, keys, joinType, additionalJoinFilterCondition)
  }

  /**
   * left anti join
   */
  private[temporalquery] def leftAntiJoinIntervals[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keys:Seq[String], additionalJoinFilterCondition:Column )
                                                  (implicit ss:SparkSession, tc:IntervalQueryConfig[T,ClosedInterval[T]]): DataFrame = {
    logger.debug(s"leftAntiJoinIntervals START: keys = ${keys.mkString(", ")}")
    val df1Cols = df1.columns.map(df1(_))
    val df2Renamed = renameIntervalCols2nd(df2)

    val joinCondition: Column = createAliasKeyCondition(df1, df2Renamed, keys)
      .and(tc.joinIntervalExpr2(df1, df2Renamed))
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1.join(df2Renamed, joinCondition, "leftanti")
    logger.debug(s"leftAntiJoinIntervals: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1.except(dfAntiJoin)
    // We need to combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Renamed, joinCondition, "inner")
      .select(df1Cols :+ tc.fromCol2 :+ tc.toCol2 :_*)
    logger.debug(s"leftAntiJoinIntervals: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = combineIntervals(dfJoin.select(tc.fromColName2, tc.toColName2 +: keys :_*), Seq())(implicitly[Ordering[T]], implicitly[TypeTag[T]], ss, tc.config2)
    logger.debug(s"leftAntiJoinIntervals: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (keys.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined)
    else df1ExceptAntiJoin.join(df2Combined, keys, "inner")
    logger.debug(s"leftAntiJoinIntervals: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val udfIntervalComplement = getUdfIntervalComplement[T]
    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(df1Cols:_*)
      .agg(collect_set(struct(tc.fromCol2.as("_1"), tc.toCol2.as("_2"))).as("subtrahend"))
      .withColumn("complement_array", udfIntervalComplement(tc.fromCol, tc.toCol, col("subtrahend")))
      .cache()
    logger.debug(s"leftAntiJoinIntervals: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend",tc.fromColName,tc.toColName)
      .withColumn(tc.fromColName,col("complements._1"))
      .withColumn(tc.toColName,col("complements._2"))
      .select(df1.columns.head, df1.columns.tail:_*)
    logger.debug(s"leftAntiJoinIntervals: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineIntervals[T: Ordering: TypeTag](df:DataFrame, ignoreColNames:Seq[String]  )
                                             (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]): DataFrame = {
    keepAlias(df, df => {
      val dfColumns = df.columns
      val compareCols = dfColumns.diff(ignoreColNames ++ tc.technicalColNames)
      val fenestra = Window.partitionBy(compareCols.map(col): _*).orderBy(tc.fromCol)

      val nbColName = "_nb"
      val consecutiveColName = "_consecutive"
      require(!df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName), s"(combineIntervals) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns.mkString(",")}")
      df.withColumn(consecutiveColName, coalesce(tc.getPredecessorIntervalEndExpr(tc.fromCol) <= lag(tc.toCol, 1).over(fenestra), lit(false)))
        .withColumn(nbColName, sum(when(col(consecutiveColName), lit(0)).otherwise(lit(1))).over(fenestra))
        .groupBy(compareCols.map(col) :+ col(nbColName): _*)
        .agg(min(tc.fromCol).as(tc.fromColName), max(tc.toCol).as(tc.toColName))
        .drop(nbColName)
        .select(dfColumns.head, dfColumns.tail: _*)
    })
  }

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extend: Boolean = false, fillGapsWithNull: Boolean = false )
                                                (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]) = {
    keepAlias(df, df => {
      // get ranges
      val df1Renamed = renameKeys(df, keys, joinColPostFix1)
      val df2Ranges = renameKeys(renameIntervalCols2nd(buildIntervalRanges(df, keys, extend)).as("ranges"), keys, joinColPostFix2)
      val keyCondition = createRenamedKeyCondition(df1Renamed, df2Ranges, keys)
      // join back on input df
      val joinType = if (fillGapsWithNull) "left" else "inner"
      val dfJoin = df2Ranges.join(df1Renamed, keyCondition and tc.isInIntervalExpr(tc.fromCol2), joinType)
      // select result
      val selCols = keys.map(key => col(s"$key$joinColPostFix2").as(key)) ++
        df.columns.diff(keys ++ tc.technicalColNames).map(dfJoin(_)) :+
        tc.fromCol2.as(tc.fromColName) :+ tc.toCol2.as(tc.toColName)
      dfJoin.select(selCols: _*)
    })
  }

  /**
   * extend gueltig_ab/bis to min/maxDate
   */
  private[temporalquery] def extendIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extendMin:Boolean, extendMax:Boolean )
                                                 (implicit ss:SparkSession, tc:IntervalQueryConfig[T,_]): DataFrame = {
    val fromMinColName = s"_${tc.fromColName}_min"
    val toMaxColName = s"_${tc.toColName}_max"
    require(!df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName), s"(extendIntervalRanges) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns.mkString(",")}")
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn(fromMinColName, if( extendMin ) min(tc.fromCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
      .withColumn(toMaxColName, if( extendMax ) max(tc.toCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
    val selCols = df.columns.filter( c => c!=tc.fromColName && c!=tc.toColName ).map(col) :+
      when(tc.fromCol===col(fromMinColName), lit(tc.lowerHorizon)).otherwise(tc.fromCol).as(tc.fromColName) :+
      when(tc.toCol===col(toMaxColName), lit(tc.upperHorizon)).otherwise(tc.toCol).as(tc.toColName)
    df_prep.select( selCols:_* )
  }

  /**
   * Helper method to rename main pair of interval columns to 2nd pair of column names defined in IntervalQueryConfig
   */
  private def renameIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit tc:IntervalQueryConfig[T,_]): DataFrame = {
    assert(df.columns.contains(tc.fromColName) && df.columns.contains(tc.toColName))
    assert(!df.columns.contains(tc.fromColName2) && !df.columns.contains(tc.toColName2))
    df.withColumnRenamed(tc.fromColName,tc.fromColName2).withColumnRenamed(tc.toColName,tc.toColName2)
  }

  /**
   * Helper method to rename 2nd pair of interval columns to main pair of column names defined in IntervalQueryConfig
   */
  private def renameIntervalColsMain[T: Ordering: TypeTag](df: DataFrame)(implicit tc:IntervalQueryConfig[T,_]): DataFrame = {
    assert(df.columns.contains(tc.fromColName2) && df.columns.contains(tc.toColName2))
    assert(!df.columns.contains(tc.fromColName) && !df.columns.contains(tc.toColName))
    df.withColumnRenamed(tc.fromColName2,tc.fromColName).withColumnRenamed(tc.toColName2,tc.toColName)
  }

  /**
   * Helper method to copy main pair of interval columns as 2nd pair of interval columns defined in IntervalQueryConfig
   */
  private def copyIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit tc:IntervalQueryConfig[T,_]): DataFrame = {
    df.withColumn(tc.fromColName2,tc.fromCol).withColumn(tc.toColName2,tc.toCol)
  }
}
