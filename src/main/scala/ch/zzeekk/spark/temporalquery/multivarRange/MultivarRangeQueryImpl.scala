package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, IntervalDef, IntervalQueryDimension}
import ch.zzeekk.spark.temporalquery.{getUdfIntervalComplement, Logging}
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
      clmrqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame =
    df.withColumn(clmrqc.fromColName, clmrqc.intervalDef.getCeilExpr(clmrqc.fromCol))
      .withColumn(clmrqc.toColName, clmrqc.intervalDef.getFloorExpr(clmrqc.toCol))
      .where(clmrqc.isValidMultivarRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)

  private[temporalquery] def transformHalfOpenToClosedIntervals[T: Ordering: TypeTag](
      df: DataFrame,
      clmrqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame = {
    val dims = clmrqc.intervalDimensions
    df
      .withColumns(colsMap = dims.map(d => (d.fromColName, d.intDef.getCeilExpr(col(d.fromColName)))).toMap)
      .withColumns(colsMap = dims.map(d => (d.toColName, d.intDef.getPredecessorExpr(col(d.toColName)))).toMap)
      //    .withColumn(clmrqc.fromColName, clmrqc.intervalDef.getCeilExpr(clmrqc.fromCol))
      //    .withColumn(clmrqc.toColName, clmrqc.intervalDef.getPredecessorExpr(clmrqc.toCol))
      .where(clmrqc.isValidMultivarRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)
  }

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
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(
      s"joinIntervals: joinType = $joinType , additionalJoinCondition = $additionalJoinCondition , keys = (${keys.mkString(",")})"
    )
    debugLog(s"joinIntervals: df1.schema = ${df1.schema.catalogString}")
    debugLog(s"joinIntervals: df2.schema = ${df2.schema.catalogString}")
    require(
      df2.columns.intersect(mrqc.fromToColnames2).isEmpty,
      s"(joinIntervals) Your right-dataframe must not contain columns named {${mrqc.fromToColnames2}}! df.columns = ${df2.columns.mkString(",")}"
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
      .join(df2Renamed, keyCondition and additionalJoinCondition and mrqc.joinMultivarRangeExpr(df1Renamed, df2Renamed), joinType)
    debugLog(s"joinIntervals: dfJoined.schema   = ${dfJoined.schema.catalogString}")

    // select final schema
    val commonColNames = keys
    val commonCols = keys
      .map(key => coalesce(df1Renamed(s"$key$joinColPostFix1"), df2Renamed(s"$key$joinColPostFix2")).as(key))
    val colsDf1 = df1.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df2(_))
    // val timeColumns = List(greatest(mrqc.fromCol, mrqc.fromCol2).as(mrqc.fromColName), least(mrqc.toCol, mrqc.toCol2).as(mrqc.toColName))
    val timeColumns = mrqc.intervalDimensions.map { dim =>
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
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame =
    joinIntervals(df1, df2, keys, joinType)

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      extend: Boolean
  )(implicit
      logger: Logger
  ): DataFrame = {
    debugLog(s"(buildDimensionRanges) df.schema = ${df.schema.catalogString} , dim = $dim")
    val intDef = dim.intDef
    val ptColName = "_pt"
    require(
      !df.columns.contains(ptColName),
      s"(buildIntervalRanges) Your dataframe must not contain column $ptColName! df.columns = ${df.columns.mkString(",")}"
    )
    val keyCols = keys.map(col)
    debugLog(s"(buildDimensionRanges) get start/end-points for every key: ${dim.intDef.isValidIntervalExpr(dim.fromCol, dim.toCol)}")
    val dfPoints = df
      .where(intDef.isValidIntervalExpr(dim.fromCol, dim.toCol)) // filter invalid intervals
      .select(keyCols :+ dim.fromCol.as(ptColName): _*).union(
        df.select(keyCols :+
            intDef.getSuccessorExpr(dim.toCol).as(ptColName): _*)
      )
    debugLog(s"(buildDimensionRanges) dfPoints.schema = ${dfPoints.schema.catalogString}")

    debugLog("(buildDimensionRanges) if desired, extend every key with min/maxDate-points")
    val dfPointsExt = if (extend) {
      dfPoints
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(intDef.lowerHorizon)))
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(intDef.upperHorizon)))
        .distinct
        .where(intDef.isInBoundariesExpr(col(ptColName)))
    } else dfPoints.distinct
    debugLog(s"(buildDimensionRanges) dfPointsExt.schema = ${dfPointsExt.schema.catalogString}")
    debugLog("(buildDimensionRanges) build ranges")
    dfPointsExt
      .withColumnRenamed(ptColName, dim.fromColName)
      .withColumn(
        dim.toColName,
        intDef.getPredecessorExpr(lead(dim.fromCol, 1).over(Window.partitionBy(keys.map(col): _*).orderBy(dim.fromCol)))
      )
      .where(dim.toCol.isNotNull)
  }

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildIntervalRanges[T: Ordering: TypeTag](df: DataFrame, keys: Seq[String], extend: Boolean)(implicit
      mrqc: MultivarRangeQueryConfig[_, _],
      logger: Logger
  ): DataFrame = {
    debugLog(s"(buildIntervalRanges) df.schema = ${df.schema.catalogString} , mrqc = $mrqc")
    val ptColName = "_pt"

    require(
      !df.columns.contains(ptColName),
      s"(buildIntervalRanges) Your dataframe must not contain column $ptColName! df.columns = ${df.columns.mkString(",")}"
    )

    val keyCols = keys.map(col)
    debugLog(s"(buildIntervalRanges) get start/end-points for every key: ${mrqc.isValidMultivarRangeExpr}")
    val dfPoints = df
      .where(mrqc.isValidMultivarRangeExpr) // filter invalid intervals
      .select(keyCols :+ mrqc.fromCol.as(ptColName): _*).union(
        df.select(keyCols :+
            mrqc.
              getSuccessorIntervalStartExpr(mrqc.toCol).as(ptColName): _*)
      )
    debugLog(s"(buildIntervalRanges) dfPoints.schema = ${dfPoints.schema.catalogString}")

    debugLog("(buildIntervalRanges) if desired, extend every key with min/maxDate-points")
    val dfPointsExt = if (extend) {
      dfPoints
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(mrqc.lowerHorizon)))
        .union(dfPoints.select(keyCols: _*).distinct.withColumn(ptColName, lit(mrqc.upperHorizon)))
        .distinct
        .where(mrqc.isInBoundariesExpr(List(col(ptColName))))
    } else dfPoints.distinct
    debugLog(s"(buildIntervalRanges) dfPointsExt.schema = ${dfPointsExt.schema.catalogString}")
    debugLog("(buildIntervalRanges) build ranges")
    dfPointsExt
      .withColumnRenamed(ptColName, mrqc.fromColName)
      .withColumn(
        mrqc.toColName,
        mrqc.getPredecessorIntervalEndExpr(lead(mrqc.fromCol, 1).over(Window.partitionBy(keys.map(col): _*).orderBy(mrqc.fromCol)))
      )
      .where(mrqc.toCol.isNotNull)
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
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(s"(cleanupExtendIntervals) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(
      s"(cleanupExtendIntervals) rnkExpressions = ${rnkExpressions.mkString(",")} ; aggExpressions = ${aggExpressions.mkString(",")}"
    )
    debugLog(s"(cleanupExtendIntervals) rnkFilter = $rnkFilter , extend = $extend ; fillGapsWithNull = $fillGapsWithNull")
    debugLog(s"(cleanupExtendIntervals) mrqc = $mrqc")
    if (extend && !fillGapsWithNull) logger.warn("(cleanupExtendIntervals) extend=true has no effect if fillGapsWithNull=false!")
    require(
      df.columns.intersect(mrqc.fromToColnames2 :+ mrqc.definedColName).isEmpty,
      s"(joinIntervals) Your right-dataframe must not contain columns named {${mrqc.fromToColnames2 :+ mrqc.definedColName}}! df.columns = ${df.columns.mkString(",")}"
    )
    def transform(df: DataFrame): DataFrame = {
      debugLog(s"(cleanupExtendIntervals.transform) df.schema = ${df.schema.catalogString}")
      debugLog(s"(cleanupExtendIntervals.transform)" +
        s" use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions")
      val df2nd = copyIntervalCols2nd(df)
      debugLog(s"(cleanupExtendIntervals.transform) df2nd.schema = ${df2nd.schema.catalogString}")
      val fenestra = Window.partitionBy(keys.map(col) :+ mrqc.fromCol2: _*)

      val dfJoin =
        unifyMultivarRanges(df = df2nd,
          keys = keys,
          extend = extend,
          fillGapsWithNull = fillGapsWithNull,
          mrqc = mrqc.config2
        )
          .withColumn(mrqc.definedColName, mrqc.toCol.isNotNull)
          .withColumn(mrqc.fromColName, coalesce(mrqc.fromCol, mrqc.fromCol2))
          .withColumn(mrqc.toColName, coalesce(mrqc.toCol, mrqc.toCol2))
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
        df.columns.diff(keys ++ mrqc.technicalColNames).map(dfClean(_)) ++
        aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Nil) :+
        dfClean(mrqc.fromColName2).as(mrqc.fromColName) :+ dfClean(mrqc.toColName2).as(mrqc.toColName) :+ mrqc.definedCol
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
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    // extend data frames
    val df1Extended = if ((joinType == "full" || joinType == "right") && doCleanupExtend)
      cleanupExtendIntervals(df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Nil, rnkFilter = true).drop(mrqc.definedColName)
    else df1
    val df2Extended = if ((joinType == "full" || joinType == "left") && doCleanupExtend)
      cleanupExtendIntervals(df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Nil, rnkFilter = true).drop(mrqc.definedColName)
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
  )(implicit mrqc: MultivarRangeQueryConfig[T, ClosedInterval[T]], logger: Logger): DataFrame = {
    debugLog(s"leftAntiJoinIntervals START: keys = ${keys.mkString(", ")}")
    val df1Cols = df1.columns.map(df1(_))
    val df2Renamed = renameIntervalCols2nd(df2)

    val joinCondition: Column = createAliasKeyCondition(df1, df2Renamed, keys)
      .and(mrqc.joinMultivarRangeExpr(df1, df2Renamed))
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1.join(df2Renamed, joinCondition, "leftanti")
    debugLog(s"leftAntiJoinIntervals: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1.except(dfAntiJoin)
    // We need to combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Renamed, joinCondition, "inner")
      .select(df1Cols :+ mrqc.fromCol2 :+ mrqc.toCol2: _*)
    debugLog(s"leftAntiJoinIntervals: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = combineIntervals(dfJoin.select(mrqc.fromColName2, mrqc.toColName2 +: keys: _*), Nil)(implicitly[Ordering[T]],
      implicitly[TypeTag[T]], mrqc.config2)
    debugLog(s"leftAntiJoinIntervals: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (keys.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined)
    else df1ExceptAntiJoin.join(df2Combined, keys, "inner")
    debugLog(s"leftAntiJoinIntervals: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val udfIntervalComplement = getUdfIntervalComplement[T]
    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(df1Cols: _*)
      .agg(collect_set(struct(mrqc.fromCol2.as("_1"), mrqc.toCol2.as("_2"))).as("subtrahend"))
      .withColumn("complement_array", udfIntervalComplement(mrqc.fromCol, mrqc.toCol, col("subtrahend")))
      .cache()
    debugLog(s"leftAntiJoinIntervals: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend", mrqc.fromColName, mrqc.toColName)
      .withColumn(mrqc.fromColName, col("complements._1"))
      .withColumn(mrqc.toColName, col("complements._2"))
      .select(df1.columns.map(col): _*)
    debugLog(s"leftAntiJoinIntervals: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineIntervals[T: Ordering: TypeTag](df: DataFrame, ignoreColNames: Seq[String])(implicit
      mrqc: MultivarRangeQueryConfig[T, _]
  ): DataFrame =
    keepAlias(
      df = df,
      transform = (df: DataFrame) => {
        val dfColumns = df.columns
        val compareCols = dfColumns.diff(ignoreColNames ++ mrqc.technicalColNames)
        val fenestra = Window.partitionBy(compareCols.map(col): _*).orderBy(mrqc.fromCol)
        val nbColName = "_nb"
        val consecutiveColName = "_consecutive"
        require(
          !df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName),
          s"(combineIntervals) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns.mkString(",")}"
        )
        df.withColumn(consecutiveColName,
          coalesce(mrqc.getPredecessorIntervalEndExpr(mrqc.fromCol) <= lag(mrqc.toCol, 1).over(fenestra), lit(false)))
          .withColumn(nbColName, sum(when(col(consecutiveColName), lit(0)).otherwise(lit(1))).over(fenestra))
          .groupBy(compareCols.map(col) :+ col(nbColName): _*)
          .agg(min(mrqc.fromCol).as(mrqc.fromColName), max(mrqc.toCol).as(mrqc.toColName))
          .drop(nbColName)
          .select(dfColumns.map(col): _*)
      }
    )

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      extend: Boolean = false,
      fillGapsWithNull: Boolean = false,
      additionalTechnicalColNames: List[String]
  )(implicit logger: Logger): DataFrame = {
    debugLog(s"(unifyDimensionRanges) START keys = ${keys.mkString(",")} ; extend = $extend ;" +
      s" fillGapsWithNull = $fillGapsWithNull ; dim = $dim")
    if (logger.isDebugEnabled()) df.debLog("df")
    val intDef = dim.intDef
    def transform(df: DataFrame): DataFrame = {
      debugLog(s"(unifyDimensionRanges.transform) get ranges. df.schema = ${df.schema.catalogString}")
      val df1Renamed = renameKeys(df, keys, joinColPostFix1)
      debugLog(s"(unifyDimensionRanges.transform)     df1Renamed.schema = ${df1Renamed.schema.catalogString}")
      val df2Ranges = renameKeys(df = renameDimensionCols2nd(df = buildDimensionRanges(df, keys, dim, extend), dim).as("ranges"),
        keys = keys, postFix = joinColPostFix2)
      if (logger.isDebugEnabled()) df2Ranges.createdLog("df2Ranges", showRows = true)
      val keyCondition = createRenamedKeyCondition(keys)
      val joinType = if (fillGapsWithNull) "left" else "inner"
      val joinCondition = keyCondition and intDef.isInIntervalExpr(valueCol = dim.fromCol2, dim.fromCol, dim.toCol)
      debugLog(s"(unifyDimensionRanges.transform) join back on input df: df2Ranges.join(df1Renamed) with " +
        s" joinType = $joinType , joinCondition = $joinCondition")
      val dfJoin = df2Ranges.join(right = df1Renamed, joinExprs = joinCondition, joinType = joinType)
      if (logger.isDebugEnabled()) dfJoin.createdLog("dfJoin", showRows = true)
      val selCols = keys.map(key => col(s"$key$joinColPostFix2").as(key)) ++
        df.columns.diff(keys ++ List(dim.fromColName, dim.toColName) ++ additionalTechnicalColNames).map(dfJoin(_)) :+
        dim.fromCol2.as(dim.fromColName) :+ dim.toCol2.as(dim.toColName)
      debugLog(s"(unifyDimensionRanges.transform) select result: selCols = ${selCols.mkString(",")}")
      dfJoin.select(selCols: _*)
    }
    val result = keepAlias(df, transform)
    if (logger.isDebugEnabled()) result.createdLog("result", showRows = true)
    result
  }

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyMultivarRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      extend: Boolean = false,
      fillGapsWithNull: Boolean = false,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]
  )(implicit logger: Logger): DataFrame = {
    debugLog(s"(unifyMultivarRanges) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(s"(unifyMultivarRanges) extend = $extend ; fillGapsWithNull = $fillGapsWithNull")
    debugLog(s"(unifyMultivarRanges) mrqc = $mrqc")
    val dims = mrqc.intervalDimensions
    dims.zip(dims.inits.toSeq.tail.reverse).foldLeft(df) { case (df, (dim, prevDims)) =>
      unifyDimensionRanges(
        df = df,
        keys = keys ++ prevDims.map(_.fromColName) ++ prevDims.map(_.toColName),
        dim = dim,
        extend = extend,
        fillGapsWithNull = fillGapsWithNull,
        additionalTechnicalColNames = mrqc.additionalTechnicalColNames
      )
    }
  }

  /**
   * extend valid_from/to to min/maxDate
   */
  private[temporalquery] def extendIntervalRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      extendMin: Boolean,
      extendMax: Boolean
  )(implicit mrqc: MultivarRangeQueryConfig[T, _]): DataFrame = {
    val fromMinColName = s"_${mrqc.fromColName}_min"
    val toMaxColName = s"_${mrqc.toColName}_max"
    require(
      !df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName),
      s"(extendIntervalRanges) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns.mkString(",")}"
    )
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn(fromMinColName, if (extendMin) min(mrqc.fromCol).over(Window.partitionBy(keyCols: _*)) else lit(null))
      .withColumn(toMaxColName, if (extendMax) max(mrqc.toCol).over(Window.partitionBy(keyCols: _*)) else lit(null))
    val selCols = df.columns.filter(c => c != mrqc.fromColName && c != mrqc.toColName).map(col) :+
      when(mrqc.fromCol === col(fromMinColName), lit(mrqc.lowerHorizon)).otherwise(mrqc.fromCol).as(mrqc.fromColName) :+
      when(mrqc.toCol === col(toMaxColName), lit(mrqc.upperHorizon)).otherwise(mrqc.toCol).as(mrqc.toColName)
    df_prep.select(selCols: _*)
  }

  /**
   * Helper method to rename main pair of interval columns to 2nd pair of column names defined in
   * IntervalQueryConfig
   */
  private def renameDimensionCols2nd[T](
      df: DataFrame,
      dim: IntervalQueryDimension[T, _]
  ): DataFrame = {
    assert(df.columns.contains(dim.fromColName) && df.columns.contains(dim.toColName))
    assert(!df.columns.contains(dim.fromCol2Name) && !df.columns.contains(dim.toCol2Name))
    df.withColumnRenamed(dim.fromColName, dim.fromCol2Name).withColumnRenamed(dim.toColName, dim.toCol2Name)
  }

  /**
   * Helper method to rename main pair of interval columns to 2nd pair of column names defined in
   * IntervalQueryConfig
   */
  private def renameIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit mrqc: MultivarRangeQueryConfig[T, _]): DataFrame = {
    assert(df.columns.contains(mrqc.fromColName) && df.columns.contains(mrqc.toColName))
    assert(!df.columns.contains(mrqc.fromColName2) && !df.columns.contains(mrqc.toColName2))
    df.withColumnRenamed(mrqc.fromColName, mrqc.fromColName2).withColumnRenamed(mrqc.toColName, mrqc.toColName2)
  }

  /**
   * Helper method to copy main pair of interval columns as 2nd pair of interval columns defined in
   * IntervalQueryConfig
   */
  private def copyIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit mrqc: MultivarRangeQueryConfig[T, _]): DataFrame =
    df.withColumn(mrqc.fromColName2, mrqc.fromCol).withColumn(mrqc.toColName2, mrqc.toCol)
}
