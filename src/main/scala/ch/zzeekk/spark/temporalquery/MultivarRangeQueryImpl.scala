package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalHelpers._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag

object MultivarRangeQueryImpl extends Logging {

  // helpers

  /**
   * Recursively search logical plan for alias, as long as nodes are unary (only one child).
   */
  @tailrec
  private def getAliasFromPlan(plan: LogicalPlan): Option[String] = plan match {
    case x: SubqueryAlias => Some(x.alias)
    case x: UnaryNode     => getAliasFromPlan(x.child)
    case _                => None
  }

  /**
   * Search for alias in DataFrame's logical plan.
   */
  def getAlias(df: DataFrame): Option[String] = getAliasFromPlan(df.queryExecution.analyzed)

  /**
   * Keep Dataframe Alias over any given transform.
   */
  def keepAlias(df: DataFrame, transform: DataFrame => DataFrame): DataFrame =
    getAlias(df).map(transform(df).alias).getOrElse(transform(df))

  /**
   * Create a column reference using the DataFrame's alias if existing.
   */
  private[temporalquery] def getColumnRef(df: DataFrame, colName: String): Column =
    getAlias(df).map(alias => col(s"$alias.$colName")).getOrElse(df(colName))

  /**
   * Create key condition. If possible use alias to reduce ambiguous column errors.
   */
  private[temporalquery] def createAliasKeyCondition(df1: DataFrame, df2: DataFrame, keys: Seq[String]): Column =
    keys.foldLeft(lit(true)) { case (cond, key) => cond and getColumnRef(df1, key) === getColumnRef(df2, key) }

  /**
   * Create key condition using renamed columns to avoid ambiguous column errors. DataFrame df1 and
   * df2 must be prepared with `<key>__1` and `<key>__2` columns for each key.
   */
  private def createRenamedKeyCondition(keys: Seq[String]): Column =
    keys.foldLeft(lit(true)) { case (cond, key) =>
      cond and col(s"$key$joinColPostFix1") === col(s"$key$joinColPostFix2")
    }

  private def renameKeys(df: DataFrame, keys: Seq[String], postFix: String): DataFrame = keys.foldLeft(df) {
    case (df, key) => df.withColumnRenamed(key, s"$key$postFix")
  }

  private val joinColPostFix1 = "__1"
  private val joinColPostFix2 = "__2"

  private[temporalquery] def roundIntervalsToDiscreteTime[T: Ordering: TypeTag](df: DataFrame)(implicit
      iqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame =
    df.withColumn(iqc.fromColName, iqc.intervalDef.getCeilExpr(iqc.fromCol))
      .withColumn(iqc.toColName, iqc.intervalDef.getFloorExpr(iqc.toCol))
      .where(iqc.isValidMultivarRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)

  private[temporalquery] def transformHalfOpenToClosedIntervals[T: Ordering: TypeTag](df: DataFrame)(implicit
      iqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame =
    df.withColumn(iqc.fromColName, iqc.intervalDef.getCeilExpr(iqc.fromCol))
      .withColumn(iqc.toColName, iqc.intervalDef.getPredecessorExpr(iqc.toCol))
      .where(iqc.isValidMultivarRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)

  /**
   * join two interval data frames keys must occur in both data frames df1 and df2 and are
   * consolidated in the result frame with df1 precedence over df2 using coalesce. This is used to
   * implement "natural join" and "join using" SQL behaviour.
   */
  private[temporalquery] def joinIntervals[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      joinType: String = "inner",
      additionalJoinCondition: Column = lit(true)
  )(implicit iqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(
      s"joinIntervals: joinType = $joinType , additionalJoinCondition = $additionalJoinCondition , keys = (${keys.mkString(",")})"
    )
    debugLog(s"joinIntervals: df1.schema = ${df1.schema.catalogString}")
    debugLog(s"joinIntervals: df2.schema = ${df2.schema.catalogString}")
    require(
      df2.columns.intersect(iqc.fromToColnames2).isEmpty,
      s"(joinIntervals) Your right-dataframe must not contain columns named {${iqc.fromToColnames2}}! df.columns = ${df2.columns.mkString(",")}"
    )
    require(
      keys.diff(df1.columns).isEmpty,
      s"(joinIntervals) Your left-dataframe doesn't contain column to consolidate ${keys.diff(df1.columns).mkString(" and ")}"
    )
    require(
      keys.diff(df2.columns).isEmpty,
      s"(joinIntervals) Your right-dataframe doesn't contain column to consolidate ${keys.diff(df2.columns).mkString(" and ")}"
    )

    // interval join
    // rename keys to avoid column ambiguous errors
    val df1Renamed = renameKeys(df1, keys, joinColPostFix1)
    val df2Renamed = renameKeys(renameIntervalCols2nd(df2), keys, joinColPostFix2)
    val keyCondition = createRenamedKeyCondition(keys)
    debugLog(s"joinIntervals: df1Renamed.schema = ${df1Renamed.schema.catalogString}")
    debugLog(s"joinIntervals: df2Renamed.schema = ${df2Renamed.schema.catalogString}")
    debugLog(s"joinIntervals: keyCondition      = $keyCondition")
    val dfJoined = df1Renamed
      .join(df2Renamed, keyCondition and additionalJoinCondition and iqc.joinMultivarRangeExpr(df1Renamed, df2Renamed), joinType)
    debugLog(s"joinIntervals: dfJoined.schema   = ${dfJoined.schema.catalogString}")

    // select final schema
    val commonColNames = keys
    val commonCols = keys
      .map(key => coalesce(df1Renamed(s"$key$joinColPostFix1"), df2Renamed(s"$key$joinColPostFix2")).as(key))
    val colsDf1 = df1.columns.diff(commonColNames ++ iqc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(commonColNames ++ iqc.technicalColNames).map(df2(_))
    // val timeColumns = List(greatest(iqc.fromCol, iqc.fromCol2).as(iqc.fromColName), least(iqc.toCol, iqc.toCol2).as(iqc.toColName))
    val timeColumns = iqc.intervalDimensions.map { dim =>
      List(greatest(dim.fromCol, dim.fromCol2).as(dim.fromColName), least(dim.toCol, dim.toCol2).as(dim.toColName))
    }.reduce((x, y) => x ++ y)
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    debugLog(s"joinIntervals: selCols = ${selCols.mkString(",")}")
    dfJoined.select(selCols: _*)
  }

  private[temporalquery] def joinIntervalsWithKeysImpl[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      joinType: String = "inner"
  )(implicit iqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame =
    joinIntervals(df1, df2, keys, joinType)

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildIntervalRanges[T: Ordering: TypeTag](df: DataFrame, keys: Seq[String], extend: Boolean)(implicit
      iqc: MultivarRangeQueryConfig[_, _],
      logger: Logger
  ): DataFrame = {
    debugLog(s"(buildIntervalRanges) df.schema = ${df.schema.catalogString} , iqc = $iqc")
    val ptColName = "_pt"

    require(
      !df.columns.contains(ptColName),
      s"(buildIntervalRanges) Your dataframe must not contain column $ptColName! df.columns = ${df.columns.mkString(",")}"
    )

    val keyCols = keys.map(col)
    debugLog(s"(buildIntervalRanges) get start/end-points for every key: ${iqc.isValidMultivarRangeExpr}")
    val dfPoints = df
      .where(iqc.isValidMultivarRangeExpr) // filter invalid intervals
      .select(keyCols :+ iqc.fromCol.as(ptColName): _*).union(
        df.select(keyCols :+
            iqc.
              getSuccessorIntervalStartExpr(iqc.toCol).as(ptColName): _*)
      )
    debugLog(s"(buildIntervalRanges) dfPoints.schema = ${dfPoints.schema.catalogString}")

    debugLog("(buildIntervalRanges) if desired, extend every key with min/maxDate-points")
    val dfPointsExt = if (extend) {
      dfPoints
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(iqc.lowerHorizon)))
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(iqc.upperHorizon)))
        .distinct
        .where(iqc.isInBoundariesExpr(List(col(ptColName))))
    } else dfPoints.distinct
    debugLog(s"(buildIntervalRanges) dfPointsExt.schema = ${dfPointsExt.schema.catalogString}")
    debugLog("(buildIntervalRanges) build ranges")
    dfPointsExt
      .withColumnRenamed(ptColName, iqc.fromColName)
      .withColumn(
        iqc.toColName,
        iqc.getPredecessorIntervalEndExpr(lead(iqc.fromCol, 1).over(Window.partitionBy(keys.map(col): _*).orderBy(iqc.fromCol)))
      )
      .where(iqc.toCol.isNotNull)
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private[temporalquery] def cleanupExtendIntervals[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      rnkExpressions: Seq[Column],
      aggExpressions: Seq[(String, Column)],
      rnkFilter: Boolean,
      extend: Boolean = true,
      fillGapsWithNull: Boolean = true
  )(implicit iqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(s"(cleanupExtendIntervals) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(
      s"(cleanupExtendIntervals) rnkExpressions = ${rnkExpressions.mkString(",")} ; aggExpressions = ${aggExpressions.mkString(",")}"
    )
    debugLog(s"(cleanupExtendIntervals) rnkFilter = $rnkFilter , extend = $extend ; fillGapsWithNull = $fillGapsWithNull")
    debugLog(s"(cleanupExtendIntervals) iqc = $iqc")
    if (extend && !fillGapsWithNull) logger.warn("(cleanupExtendIntervals) extend=true has no effect if fillGapsWithNull=false!")
    require(
      df.columns.intersect(iqc.fromToColnames2 :+ iqc.definedColName).isEmpty,
      s"(joinIntervals) Your right-dataframe must not contain columns named {${iqc.fromToColnames2 :+ iqc.definedColName}}! df.columns = ${df.columns.mkString(",")}"
    )
    def transform(df: DataFrame): DataFrame = {
      debugLog(s"(cleanupExtendIntervals.transform) df.schema = ${df.schema.catalogString}")
      debugLog(s"(cleanupExtendIntervals.transform)" +
        s" use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions")
      val df2nd = copyIntervalCols2nd(df)
      debugLog(s"(cleanupExtendIntervals.transform) df2nd.schema = ${df2nd.schema.catalogString}")
      val fenestra = Window.partitionBy(keys.map(col) :+ iqc.fromCol2: _*)

      val dfJoin =
        unifyIntervalRanges(df = df2nd,
          keys = keys,
          extend = extend,
          fillGapsWithNull = fillGapsWithNull)(implicitly[Ordering[T]], implicitly[TypeTag[T]], iqc.config2, logger)
          .withColumn(iqc.definedColName, iqc.toCol.isNotNull)
          .withColumn(iqc.fromColName, coalesce(iqc.fromCol, iqc.fromCol2))
          .withColumn(iqc.toColName, coalesce(iqc.toCol, iqc.toCol2))
      if (logger.isDebugEnabled()) dfJoin.createdLog("dfJoin", showRows = true)

      debugLog("(cleanupExtendIntervals.transform) add aggregations if defined, implemented as analytical functions...")
      val dfAgg = aggExpressions.foldLeft(dfJoin) {
        case (df_acc, (name, expr)) => df_acc.withColumn(name, expr.over(fenestra))
      }
      dfAgg.createdLog("dfAgg")

      debugLog("(cleanupExtendIntervals.transform) Prioritize and clean overlaps")
      val rnkColName = "_rnk"
      val dfClean = if (rnkExpressions.nonEmpty) {
        require(
          !df.columns.contains(rnkColName),
          s"(cleanupExtendIntervals) Your dataframe must not contain columns named $rnkColName if rnkExpressions are defined! df.columns = ${df.columns.mkString(",")}"
        )
        val df_rnk = dfAgg.withColumn(rnkColName, row_number.over(fenestra.orderBy(rnkExpressions: _*)))
        if (rnkFilter) df_rnk.where(col(rnkColName) === 1) else df_rnk
      } else dfAgg

      val selCols: Seq[Column] = keys.map(dfClean(_)) ++
        df.columns.diff(keys ++ iqc.technicalColNames).map(dfClean(_)) ++
        aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Nil) :+
        dfClean(iqc.fromColName2).as(iqc.fromColName) :+ dfClean(iqc.toColName2).as(iqc.toColName) :+ iqc.definedCol
      debugLog(s"(cleanupExtendIntervals.transform) select final schema: selCols = ${selCols.mkString(",")}")

      dfClean.select(selCols: _*)
    }
    keepAlias(df, transform)
  }

  /**
   * outer join
   *
   * @param doCleanupExtend
   *   set to false if cleanupExtendsIntervals is already executed on the input DataFrames.
   */
  private[temporalquery] def outerJoinIntervalsWithKey[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      rnkExpressions: Seq[Column],
      additionalJoinFilterCondition: Column,
      joinType: String,
      doCleanupExtend: Boolean
  )(implicit iqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    // extend data frames
    val df1Extended = if ((joinType == "full" || joinType == "right") && doCleanupExtend)
      cleanupExtendIntervals(df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Nil, rnkFilter = true).drop(iqc.definedColName)
    else df1
    val df2Extended = if ((joinType == "full" || joinType == "left") && doCleanupExtend)
      cleanupExtendIntervals(df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Nil, rnkFilter = true).drop(iqc.definedColName)
    else df2
    // join df1 & df2
    joinIntervals(df1Extended, df2Extended, keys, joinType, additionalJoinFilterCondition)
  }

  /**
   * left anti join
   */
  private[temporalquery] def leftAntiJoinIntervals[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      additionalJoinFilterCondition: Column
      // TODO: Why we require closed interval? Why not IntervalMultidimQueryConfig[T, _]
  )(implicit iqc: MultivarRangeQueryConfig[T, ClosedInterval[T]], logger: Logger): DataFrame = {
    debugLog(s"leftAntiJoinIntervals START: keys = ${keys.mkString(", ")}")
    val df1Cols = df1.columns.map(df1(_))
    val df2Renamed = renameIntervalCols2nd(df2)

    val joinCondition: Column = createAliasKeyCondition(df1, df2Renamed, keys)
      .and(iqc.joinMultivarRangeExpr(df1, df2Renamed))
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1.join(df2Renamed, joinCondition, "leftanti")
    debugLog(s"leftAntiJoinIntervals: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1.except(dfAntiJoin)
    // We need to combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Renamed, joinCondition, "inner")
      .select(df1Cols :+ iqc.fromCol2 :+ iqc.toCol2: _*)
    debugLog(s"leftAntiJoinIntervals: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = combineIntervals(dfJoin.select(iqc.fromColName2, iqc.toColName2 +: keys: _*), Nil)(implicitly[Ordering[T]],
      implicitly[TypeTag[T]], iqc.config2)
    debugLog(s"leftAntiJoinIntervals: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (keys.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined)
    else df1ExceptAntiJoin.join(df2Combined, keys, "inner")
    debugLog(s"leftAntiJoinIntervals: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val udfIntervalComplement = getUdfIntervalComplement[T]
    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(df1Cols: _*)
      .agg(collect_set(struct(iqc.fromCol2.as("_1"), iqc.toCol2.as("_2"))).as("subtrahend"))
      .withColumn("complement_array", udfIntervalComplement(iqc.fromCol, iqc.toCol, col("subtrahend")))
      .cache()
    debugLog(s"leftAntiJoinIntervals: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend", iqc.fromColName, iqc.toColName)
      .withColumn(iqc.fromColName, col("complements._1"))
      .withColumn(iqc.toColName, col("complements._2"))
      .select(df1.columns.map(col): _*)
    debugLog(s"leftAntiJoinIntervals: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineIntervals[T: Ordering: TypeTag](df: DataFrame, ignoreColNames: Seq[String])(implicit
      iqc: MultivarRangeQueryConfig[T, _]
  ): DataFrame =
    keepAlias(
      df = df,
      transform = (df: DataFrame) => {
        val dfColumns = df.columns
        val compareCols = dfColumns.diff(ignoreColNames ++ iqc.technicalColNames)
        val fenestra = Window.partitionBy(compareCols.map(col): _*).orderBy(iqc.fromCol)
        val nbColName = "_nb"
        val consecutiveColName = "_consecutive"
        require(
          !df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName),
          s"(combineIntervals) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns.mkString(",")}"
        )
        df.withColumn(consecutiveColName,
          coalesce(iqc.getPredecessorIntervalEndExpr(iqc.fromCol) <= lag(iqc.toCol, 1).over(fenestra), lit(false)))
          .withColumn(nbColName, sum(when(col(consecutiveColName), lit(0)).otherwise(lit(1))).over(fenestra))
          .groupBy(compareCols.map(col) :+ col(nbColName): _*)
          .agg(min(iqc.fromCol).as(iqc.fromColName), max(iqc.toCol).as(iqc.toColName))
          .drop(nbColName)
          .select(dfColumns.map(col): _*)
      }
    )

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyIntervalRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      extend: Boolean = false,
      fillGapsWithNull: Boolean = false
  )(implicit iqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(s"(unifyIntervalRanges) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(s"(unifyIntervalRanges) extend = $extend ; fillGapsWithNull = $fillGapsWithNull")
    debugLog(s"(unifyIntervalRanges) iqc = $iqc")
    def transform(df: DataFrame): DataFrame = {
      debugLog(s"(unifyIntervalRanges.transform) get ranges. df.schema = ${df.schema.catalogString}")
      val df1Renamed = renameKeys(df, keys, joinColPostFix1)
      debugLog(s"(unifyIntervalRanges.transform)     df1Renamed.schema = ${df1Renamed.schema.catalogString}")
      val df2Ranges = renameKeys(df = renameIntervalCols2nd(df = buildIntervalRanges(df, keys, extend)).as("ranges"),
        keys = keys, postFix = joinColPostFix2)
      if (logger.isDebugEnabled()) df2Ranges.createdLog("df2Ranges", showRows = true)
      val keyCondition = createRenamedKeyCondition(keys)
      val joinType = if (fillGapsWithNull) "left" else "inner"
      val joinCondition = keyCondition and iqc.isInIntervalExpr(List(iqc.fromCol2))
      debugLog(s"(unifyIntervalRanges.transform) join back on input df: df2Ranges.join(df1Renamed) with " +
        s" joinType = $joinType , joinCondition = $joinCondition")
      val dfJoin = df2Ranges.join(right = df1Renamed, joinExprs = joinCondition, joinType = joinType)
      if (logger.isDebugEnabled()) dfJoin.createdLog("dfJoin", showRows = true)
      val selCols = keys.map(key => col(s"$key$joinColPostFix2").as(key)) ++
        df.columns.diff(keys ++ iqc.technicalColNames).map(dfJoin(_)) :+
        iqc.fromCol2.as(iqc.fromColName) :+ iqc.toCol2.as(iqc.toColName)
      debugLog(s"(unifyIntervalRanges.transform) select result: selCols = ${selCols.mkString(",")}")
      dfJoin.select(selCols: _*)
    }
    keepAlias(df, transform)
  }

  /**
   * extend valid_from/to to min/maxDate
   */
  private[temporalquery] def extendIntervalRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      extendMin: Boolean,
      extendMax: Boolean
  )(implicit iqc: MultivarRangeQueryConfig[T, _]): DataFrame = {
    val fromMinColName = s"_${iqc.fromColName}_min"
    val toMaxColName = s"_${iqc.toColName}_max"
    require(
      !df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName),
      s"(extendIntervalRanges) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns.mkString(",")}"
    )
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn(fromMinColName, if (extendMin) min(iqc.fromCol).over(Window.partitionBy(keyCols: _*)) else lit(null))
      .withColumn(toMaxColName, if (extendMax) max(iqc.toCol).over(Window.partitionBy(keyCols: _*)) else lit(null))
    val selCols = df.columns.filter(c => c != iqc.fromColName && c != iqc.toColName).map(col) :+
      when(iqc.fromCol === col(fromMinColName), lit(iqc.lowerHorizon)).otherwise(iqc.fromCol).as(iqc.fromColName) :+
      when(iqc.toCol === col(toMaxColName), lit(iqc.upperHorizon)).otherwise(iqc.toCol).as(iqc.toColName)
    df_prep.select(selCols: _*)
  }

  /**
   * Helper method to rename main pair of interval columns to 2nd pair of column names defined in
   * IntervalQueryConfig
   */
  private def renameIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit iqc: MultivarRangeQueryConfig[T, _]): DataFrame = {
    assert(df.columns.contains(iqc.fromColName) && df.columns.contains(iqc.toColName))
    assert(!df.columns.contains(iqc.fromColName2) && !df.columns.contains(iqc.toColName2))
    df.withColumnRenamed(iqc.fromColName, iqc.fromColName2).withColumnRenamed(iqc.toColName, iqc.toColName2)
  }

  /**
   * Helper method to copy main pair of interval columns as 2nd pair of interval columns defined in
   * IntervalQueryConfig
   */
  private def copyIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit iqc: MultivarRangeQueryConfig[T, _]): DataFrame =
    df.withColumn(iqc.fromColName2, iqc.fromCol).withColumn(iqc.toColName2, iqc.toCol)
}
