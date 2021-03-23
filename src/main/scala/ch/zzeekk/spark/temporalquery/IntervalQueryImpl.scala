package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalHelpers._
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

import scala.reflect.runtime.universe._

object IntervalQueryImpl extends Logging {

  // helpers

  private[temporalquery] def createKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and df1(key)===df2(key) }
  }

  private[temporalquery] def roundIntervalsToDiscreteTime[T: Ordering: TypeTag](df: DataFrame)
                                                                               (implicit tc:IntervalQueryConfig[T]): DataFrame= {
    df.withColumn(tc.fromColName, tc.intervalDef.getCeilExpr(tc.fromCol))
      .withColumn(tc.toColName, tc.intervalDef.getFloorExpr(tc.toCol))
      .where(tc.fromCol <= tc.toCol)
      // return columns in same order as provided
      .select(df.columns.map(col):_*)
  }

  private[temporalquery] def transformHalfOpenToClosedInterval[T: Ordering: TypeTag](df: DataFrame)
                                                                                    (implicit tc:IntervalQueryConfig[T]): DataFrame= {
    df.withColumn(tc.fromColName, tc.intervalDef.getCeilExpr(tc.fromCol))
      .withColumn(tc.toColName, tc.intervalDef.getPredecessorExpr(tc.toCol))
      .where(tc.fromCol <= tc.toCol)
      // return columns in same order as provided
      .select(df.columns.map(col):_*)
  }

  /**
   * join two temporalquery dataframes
   * colsToConsolidate must occur in both data frames df1 and df2 and are consolidated in the result frame with df1 precedence over df2 using coalesce.
   * This is used to implement "natural join" and "join using" sql behaviour.
   */
  private[temporalquery] def joinIntervals[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keyCondition:Column, joinType:String = "inner", colsToConsolidate: Seq[String] = Seq())
                                                                (implicit ss:SparkSession, tc:IntervalQueryConfig[T]) : DataFrame = {
    require(!(df2.columns.contains(tc.fromColName2) || df2.columns.contains(tc.toColName2)),
      s"(joinIntervals) Your right-dataframe must not contain columns named ${tc.fromColName2} or ${tc.toColName2}! df.columns = ${df2.columns}")
    require(colsToConsolidate.diff(df1.columns).isEmpty, s"(temporalJoinImpl) Your left-dataframe doesn't contain column to consolidate ${colsToConsolidate.diff(df1.columns).mkString(" and ")}")
    require(colsToConsolidate.diff(df2.columns).isEmpty, s"(temporalJoinImpl) Your right-dataframe doesn't contain column to consolidate ${colsToConsolidate.diff(df2.columns).mkString(" and ")}")

    // temporal join
    val df2ren = df2.withColumnRenamed(tc.fromColName,tc.fromColName2).withColumnRenamed(tc.toColName,tc.toColName2)
    val df_join = df1.join( df2ren, keyCondition and tc.joinIntervalExpr2(df1, df2), joinType)

    // select final schema
    val dfCommonColNames = colsToConsolidate
    val commonCols = colsToConsolidate.map(colName => coalesce(df1(colName),df2(colName)).as(colName))
    val colsDf1 = df1.columns.diff(dfCommonColNames ++ tc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(dfCommonColNames ++ tc.technicalColNames).map(df2(_))
    val timeColumns = Seq(greatest(tc.fromCol, tc.fromCol2).as(tc.fromColName),least(tc.toCol, tc.toCol2).as(tc.toColName))
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    df_join.select(selCols:_*)
  }
  private[temporalquery] def joinIntervalsWithKeysImpl[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keys:Seq[String], joinType:String = "inner" )
                                                                            (implicit ss:SparkSession, tc:IntervalQueryConfig[T]) : DataFrame = {
    joinIntervals( df1, df2, createKeyCondition(df1, df2, keys), joinType, keys )
  }

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extend:Boolean)
                                                                      (implicit ss:SparkSession, tc:IntervalQueryConfig[T]) : DataFrame = {
    val ptColName = "_pt"

    require(!(df.columns.contains(tc.fromColName2) || df.columns.contains(tc.toColName2) || df.columns.contains(ptColName)),
      s"(buildIntervalRanges) Your dataframe must not contain columns named ${tc.fromColName2}, ${tc.toColName2} or $ptColName! df.columns = ${df.columns}")

    val keyCols = keys.map(col)
    // get start/end-points for every key
    val df_points = df.select( keyCols :+ tc.fromCol.as(ptColName):_*).union( df.select( keyCols :+ tc.intervalDef.getSuccessorExpr(tc.toCol).as(ptColName):_* ))
    // if desired, extend every key with min/maxDate-points
    val df_pointsExt = if (extend) {
      df_points
        .union( df_points.select( keyCols:_*).distinct.withColumn( ptColName, lit(tc.intervalDef.minValue)))
        .union( df_points.select( keyCols:_*).distinct.withColumn( ptColName, lit(tc.intervalDef.maxValue)))
        .distinct
    } else df_points.distinct
    // build ranges
    df_pointsExt
      .withColumnRenamed(ptColName, tc.fromColName2)
      .withColumn( tc.toColName2, tc.intervalDef.getPredecessorExpr(lead(tc.fromCol2,1).over(Window.partitionBy(keys.map(col):_*).orderBy(tc.fromCol2))))
      .where(tc.toCol2.isNotNull)
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private[temporalquery] def cleanupExtendIntervals[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)]
                                                  , rnkFilter:Boolean, extend: Boolean = true, fillGapsWithNull: Boolean = true )
                                                   (implicit ss:SparkSession, tc:IntervalQueryConfig[T]) : DataFrame = {
    if(extend && !fillGapsWithNull) logger.warn("cleanupExtendIntervals: extend=true has no effect if fillGapsWithNull=false!")

    require(!df.columns.contains(tc.fromColName2) && !df.columns.contains(tc.toColName2) && !df.columns.contains(tc.definedColName),
      s"(cleanupExtendIntervals) Your dataframe must not contain columns named ${tc.fromColName2}, ${tc.toColName2} or ${tc.definedColName}! df.columns = ${df.columns}")

    // use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions
    val df2 = df.withColumn(tc.fromColName2, tc.fromCol).withColumn(tc.toColName2, tc.toCol)
    val tc2 = tc.config2
    val fenestra: WindowSpec = Window.partitionBy( keys.map(col):+ tc2.fromCol :_*)

    val df_join = unifyIntervalRanges( df2, keys, extend, fillGapsWithNull)(implicitly[Ordering[T]], implicitly[TypeTag[T]], ss, tc2)
      .withColumn(tc.fromColName, coalesce(tc.fromCol, tc2.fromCol))
      .withColumn(tc.toColName, coalesce(tc.toCol, tc2.toCol))
      .withColumn(tc.definedColName, tc2.toCol.isNotNull)

    // add aggregations if defined, implemented as analytical functions...£
    val df_agg = aggExpressions.foldLeft( df_join ){
      case (df_acc, (name,expr)) => df_acc.withColumn(name, expr.over(fenestra))
    }

    // Prioritize and clean overlaps
    val rnkColName = "_rnk"
    val df_clean = if (rnkExpressions.nonEmpty) {
      require(!df.columns.contains(rnkColName), s"(cleanupExtendIntervals) Your dataframe must not contain columns named $rnkColName if rnkExpressions are defined! df.columns = ${df.columns}")
      val df_rnk = df_agg.withColumn(rnkColName, row_number.over(fenestra.orderBy(rnkExpressions: _*)))
      if (rnkFilter) df_rnk.where(col(rnkColName)===1) else df_rnk
    } else df_agg

    // select final schema
    val selCols: Seq[Column] = keys.map(df_clean(_)) ++
      df.columns.diff(keys ++ tc.technicalColNames).map(df_clean(_)) ++
      aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Seq()) :+
      df_clean(tc2.fromColName).as(tc.fromColName) :+ df_clean(tc2.toColName).as(tc.toColName) :+ tc.definedCol

    combineIntervals(df_clean.select(selCols:_*) , Seq())
  }

  /**
   * outer join
   */
  private[temporalquery] def outerJoinIntervalsWithKey[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column, joinType:String)
                                                      (implicit ss:SparkSession, tc:IntervalQueryConfig[T]): DataFrame = {
    // extend df2
    val df1_extended = if (joinType=="full" || joinType=="right") cleanupExtendIntervals( df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Seq(), rnkFilter=true ).drop(tc.definedColName) else df1
    val df2_extended = if (joinType=="full" || joinType=="left") cleanupExtendIntervals( df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Seq(), rnkFilter=true ).drop(tc.definedColName) else df2
    // join df1 & df2
    joinIntervals( df1_extended, df2_extended, createKeyCondition(df1, df2, keys) and additionalJoinFilterCondition, joinType, keys )
  }

  /**
   * left outer join
   */
  private[temporalquery] def leftAntiJoinIntervals[T: Ordering: TypeTag](df1:DataFrame, df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column )
                                                  (implicit ss:SparkSession, tc:IntervalQueryConfig[T]): DataFrame = {
    logger.debug(s"leftAntiJoinIntervals START: joinColumns = ${joinColumns.mkString(", ")}")
    val df1Prepared = df1
    val resultColumns: Array[String] = df1Prepared.columns
    val resultColumnsDf1: Array[Column] = df1Prepared.columns.map(df1Prepared(_))
    val df2Prepared = df2
      .withColumnRenamed(tc.fromColName,tc.fromColName2).withColumnRenamed(tc.toColName,tc.toColName2)

    val joinCondition: Column = createKeyCondition(df1Prepared, df2Prepared, joinColumns)
      .and(tc.fromCol <= tc.toCol2)
      .and(tc.fromCol2 <= tc.toCol)
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1Prepared.join(df2Prepared, joinCondition, "leftanti")
    logger.debug(s"leftAntiJoinIntervals: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1Prepared.except(dfAntiJoin)
    // We need to combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Prepared, joinCondition, "inner")
      .select(resultColumnsDf1 :+ tc.fromCol2 :+ tc.toCol2 :_*)
    logger.debug(s"leftAntiJoinIntervals: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = combineIntervals(dfJoin.select(tc.fromColName2, tc.toColName2 +: joinColumns :_*), Seq())(implicitly[Ordering[T]], implicitly[TypeTag[T]], ss, tc.config2)
    logger.debug(s"leftAntiJoinIntervals: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (joinColumns.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined) else {
      df1ExceptAntiJoin.join(df2Combined, joinColumns, "inner")
    }
    logger.debug(s"leftAntiJoinIntervals: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val udfIntervalComplement = getUdfIntervalComplement[T]
    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(resultColumnsDf1:_*)
      .agg(collect_set(struct(tc.fromCol2.as("_1"), tc.toCol2.as("_2"))).as("subtrahend"))
      .withColumn("complement_array", udfIntervalComplement(tc.fromCol, tc.toCol, col("subtrahend")))
      .cache()
    logger.debug(s"leftAntiJoinIntervals: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend",tc.fromColName,tc.toColName)
      .withColumn(tc.fromColName,col("complements._1"))
      .withColumn(tc.toColName,col("complements._2"))
      .select(resultColumns.head, resultColumns.tail :_*)
    logger.debug(s"leftAntiJoinIntervals: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineIntervals[T: Ordering: TypeTag](df:DataFrame, ignoreColNames:Seq[String]  )
                                             (implicit ss:SparkSession, tc:IntervalQueryConfig[T]): DataFrame = {
    val dfColumns = df.columns
    val compairCols: Array[String] = dfColumns.diff( ignoreColNames ++ tc.technicalColNames )
    val fenestra: WindowSpec = Window.partitionBy(compairCols.map(col):_*).orderBy(tc.fromCol)

    val nbColName = "_nb"
    val consecutiveColName = "_consecutive"
    require(!df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName), s"(combineIntervals) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns}")
    roundIntervalsToDiscreteTime(df)
      .withColumn(consecutiveColName, coalesce(tc.intervalDef.getPredecessorExpr(tc.fromCol) <= lag(tc.toCol,1).over(fenestra),lit(false)))
      .withColumn(nbColName, sum(when(col(consecutiveColName),lit(0)).otherwise(lit(1))).over(fenestra))
      .groupBy( compairCols.map(col):+col(nbColName):_*)
      .agg( min(tc.fromCol).as(tc.fromColName) , max(tc.toCol).as(tc.toColName))
      .drop(nbColName)
      .select(dfColumns.head,dfColumns.tail:_*)
  }

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extend: Boolean = false, fillGapsWithNull: Boolean = false )
                                                (implicit ss:SparkSession, tc:IntervalQueryConfig[T]) = {
    // get ranges
    val df_ranges = buildIntervalRanges(df, keys, extend)
    val keyCondition = createKeyCondition( df, df_ranges, keys )
    // join back on input df
    val joinType = if (fillGapsWithNull) "left" else "inner"
    val df_join = df_ranges.join( df, keyCondition and tc.fromCol2.between(tc.fromCol,tc.toCol), joinType )
    // select result
    val selCols = keys.map(df_ranges(_)) ++ df.columns.diff(keys ++ tc.technicalColNames).map(df_join(_)) :+
      tc.fromCol2.as(tc.fromColName) :+ tc.toCol2.as(tc.toColName)
    df_join.select(selCols:_*)
  }

  /**
   * extend gueltig_ab/bis to min/maxDate
   */
  private[temporalquery] def extendIntervalRanges[T: Ordering: TypeTag](df:DataFrame, keys:Seq[String], extendMin:Boolean, extendMax:Boolean )
                                                 (implicit ss:SparkSession, tc:IntervalQueryConfig[T]): DataFrame = {
    val fromMinColName = s"_${tc.fromColName}_min"
    val toMaxColName = s"_${tc.toColName}_max"
    require(!df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName), s"(extendIntervalRanges) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns}")
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn(fromMinColName, if( extendMin ) min(tc.fromCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
      .withColumn(toMaxColName, if( extendMax ) max(tc.toCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
    val selCols = df.columns.filter( c => c!=tc.fromColName && c!=tc.toColName ).map(col) :+
      when(tc.fromCol===col(fromMinColName), lit(tc.intervalDef.minValue)).otherwise(tc.fromCol).as(tc.fromColName) :+
      when(tc.toCol===col(toMaxColName), lit(tc.intervalDef.maxValue)).otherwise(tc.toCol).as(tc.toColName)
    df_prep.select( selCols:_* )
  }

}
