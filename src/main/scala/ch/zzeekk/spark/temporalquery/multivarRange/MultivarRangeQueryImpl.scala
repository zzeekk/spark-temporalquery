package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.Logging
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, IntervalDef, IntervalQueryDimension}
import ch.zzeekk.spark.temporalquery.util.anyToDouble
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataType, DateType, StructType, TimestampType}
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger

import scala.annotation.tailrec
import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

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
  def keepAlias(df: DataFrame, transform: DataFrame => DataFrame)(implicit logger: Logger): DataFrame = {
    debugLog("START keepAlias")
    if (logger.isDebugEnabled()) df.debLog("df")
    val dfTransformed = transform(df)
    if (logger.isDebugEnabled()) dfTransformed.debLog("dfTransformed")
    getAlias(df).map(dfTransformed.alias).getOrElse(dfTransformed)
  }

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

  private[temporalquery] def getDiagonal[T: Ordering: TypeTag](
      df: DataFrame,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      fromColName: String,
      toColName: String
  ): DataFrame = {
    // all interval defs of the dimension are either half-open or closed; no mixture possible
    // so the non - emptiness expression is unique
    // the resulting diagonal data frame uses the interval def,
    // a priori with the finest discreteAxisDef
    val nonEmptyFilter = mrqc.rangeIntervalDefs.head.isNonEmptyExpr(col(fromColName), col(toColName))
    df.select(
      df.columns.diff(mrqc.fromToColnames).map(col) ++
        Array(greatest(mrqc.fromColnames.map(col): _*).as(fromColName),
          least(mrqc.toColnames.map(col): _*).as(toColName)): _*
    ).where(nonEmptyFilter)
  }

  private[temporalquery] def getValues[T: Ordering: TypeTag](
      df: DataFrame,
      coords: Seq[T],
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]
  ): DataFrame = df.where(mrqc.getValuesExpression(coords))

  private[temporalquery] def roundIntervalsToDiscreteTime[T: Ordering: TypeTag](
      df: DataFrame,
      clmrqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame = {
    val dims = clmrqc.rangeDimensions
    df.withColumns(colsMap = dims.map(d => (d.fromColName, d.intDef.getCeilExpr(col(d.fromColName)))).toMap)
      .withColumns(colsMap = dims.map(d => (d.toColName, d.intDef.getFloorExpr(col(d.toColName)))).toMap)
      .where(clmrqc.isNonEmptyRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)
  }

  private[temporalquery] def transformHalfOpenToClosedIntervals[T: Ordering: TypeTag](
      df: DataFrame,
      clmrqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame = {
    val dims = clmrqc.rangeDimensions
    df
      .withColumns(colsMap = dims.map(d => (d.fromColName, d.intDef.getCeilExpr(col(d.fromColName)))).toMap)
      .withColumns(colsMap = dims.map(d => (d.toColName, d.intDef.getPredecessorExpr(col(d.toColName)))).toMap)
      .where(clmrqc.isNonEmptyRangeExpr)
      // return columns in same order as provided
      .select(df.columns.map(col): _*)
  }

  /**
   * join two interval data frames keys must occur in both data frames df1 and df2 and are
   * consolidated in the result frame with df1 precedence over df2 using coalesce. This is used to
   * implement "natural join" and "join using" SQL behaviour.
   */
  private[temporalquery] def joinRanges[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      joinType: String = "inner",
      additionalJoinCondition: Column = lit(true)
  )(implicit logger: Logger): DataFrame = {
    debugLog(
      s"(joinRanges) joinType = $joinType ; additionalJoinCondition = $additionalJoinCondition ;" +
        s" keys = (${keys.mkString(",")}) ; joinColPostFix1 = $joinColPostFix1 ; joinColPostFix2 = $joinColPostFix2"
    )
    debugLog(s"(joinRanges) df1.schema = ${df1.schema.catalogString}")
    debugLog(s"(joinRanges) df2.schema = ${df2.schema.catalogString}")
    require(
      df2.columns.intersect(mrqc.fromToColnames2).isEmpty,
      s"(joinRanges) Your right-dataframe must not contain columns named {${mrqc.fromToColnames2}}! df.columns = ${df2.columns.mkString(",")}"
    )
    require(
      keys.diff(df1.columns).isEmpty,
      s"(joinRanges) Your left-dataframe doesn't contain column to consolidate ${keys.diff(df1.columns).mkString(" and ")}"
    )
    require(
      keys.diff(df2.columns).isEmpty,
      s"(joinRanges) Your right-dataframe doesn't contain column to consolidate ${keys.diff(df2.columns).mkString(" and ")}"
    )

    // interval join
    // rename keys to avoid column ambiguous errors
    val df1Renamed = renameKeys(df = df1, keys = keys, postFix = joinColPostFix1)
    val df2Renamed = renameKeys(df = renameRangeCols2nd(df2, mrqc), keys = keys, postFix = joinColPostFix2)
    val keyCondition = createRenamedKeyCondition(keys)
    debugLog(s"(joinRanges) df1Renamed.schema = ${df1Renamed.schema.catalogString}")
    debugLog(s"(joinRanges) df2Renamed.schema = ${df2Renamed.schema.catalogString}")
    debugLog(s"(joinRanges) keyCondition      = $keyCondition")
    val dfJoined = df1Renamed
      .join(df2Renamed, keyCondition and additionalJoinCondition and mrqc.joinRangeExpr(df1Renamed, df2Renamed), joinType)
    debugLog(s"(joinRanges) dfJoined.schema   = ${dfJoined.schema.catalogString}")

    // select final schema
    val commonColNames = keys
    val commonCols = keys
      .map(key => coalesce(df1Renamed(s"$key$joinColPostFix1"), df2Renamed(s"$key$joinColPostFix2")).as(key))
    val colsDf1 = df1.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df2(_))
    val timeColumns = mrqc.rangeDimensions.map { dim =>
      List(greatest(dim.fromCol, dim.fromCol2).as(dim.fromColName), least(dim.toCol, dim.toCol2).as(dim.toColName))
    }.reduce((x, y) => x ++ y)
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    logger.info(s"(joinRanges) dfJoined.schema = ${dfJoined.schema.catalogString} ; selCols = ${selCols.mkString(",")}")

    Try(dfJoined.select(selCols: _*)) match {
      case Success(df) => df
      case Failure(e)  =>
        logger.error(
          s"(joinRanges) FAILED: joinType = $joinType ; additionalJoinCondition = $additionalJoinCondition ; keys = (${keys.mkString(",")})"
        )
        logger.error(s"(joinRanges) FAILED: mrqc = $mrqc")
        logger.error(s"(joinRanges) df1.printSchema():")
        df1.printSchema()
        logger.error(s"(joinRanges) df2.printSchema():")
        df2.printSchema()
        logger.error(s"(joinRanges) dfJoined.printSchema():")
        dfJoined.printSchema()
        throw e
    }
  }

  private[temporalquery] def joinIntervalsWithKeysImpl[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      joinType: String = "inner"
  )(implicit logger: Logger): DataFrame =
    joinRanges(df1, df2, keys, mrqc, joinType)

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private[temporalquery] def buildDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      extend: Boolean
  )(implicit logger: Logger): DataFrame = {
    debugLog(s"(buildDimensionRanges) df.schema = ${df.schema.catalogString} , dim = $dim")
    val intDef = dim.intDef
    val ptColName = "_pt"
    require(
      !df.columns.contains(ptColName),
      s"(buildIntervalRanges) Your dataframe must not contain column $ptColName! df.columns = ${df.columns.mkString(",")}"
    )
    val keyCols = keys.map(col)
    debugLog(s"(buildDimensionRanges) get start/end-points for every key: ${dim.intDef.isNonEmptyExpr(dim.fromCol, dim.toCol)}")
    val dfPoints = df
      .where(intDef.isNonEmptyExpr(dim.fromCol, dim.toCol)) // filter invalid intervals
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
    val dfResultBuildDimensionRanges = dfPointsExt
      .withColumnRenamed(ptColName, dim.fromColName)
      .withColumn(
        dim.toColName,
        intDef.getPredecessorExpr(lead(dim.fromCol, 1).over(Window.partitionBy(keys.map(col): _*).orderBy(dim.fromCol)))
      )
      .where(dim.toCol.isNotNull)
    debugLog(s"(buildDimensionRanges) dfResultBuildDimensionRanges.schema = ${dfResultBuildDimensionRanges.schema.catalogString}")
    dfResultBuildDimensionRanges
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private[temporalquery] def cleanupExtendRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      mrqc: MultivarRangeQueryConfig[T, _],
      rnkFilter: Boolean = false,
      rnkExpressions: Seq[Column] = Nil,
      aggExpressions: Seq[(String, Column)] = Nil,
      extend: Boolean = true,
      fillGapsWithNull: Boolean = true
  )(implicit logger: Logger): DataFrame = {
    logger.info(
      s"(cleanupExtendRanges) START numDimensions = ${mrqc.numDimensions} ; df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")} ;" +
        s"rnkExpressions = ${rnkExpressions.mkString(",")} ; aggExpressions = ${aggExpressions.mkString(",")} ;" +
        s" rnkFilter = $rnkFilter , extend = $extend ; fillGapsWithNull = $fillGapsWithNull ; mrqc = $mrqc"
    )
    if (extend && !fillGapsWithNull) logger.warn("(cleanupExtendRanges) extend=true has no effect if fillGapsWithNull=false!")
    require(
      df.columns.intersect(mrqc.fromToColnames2 :+ mrqc.definedColName).isEmpty,
      s"(cleanupExtendRanges) Your dataframe must not contain any column" +
        s" with one of the following name: {${(mrqc.fromToColnames2 :+ mrqc.definedColName).mkString(",")}}!" +
        s" df.columns = ${df.columns.mkString(",")}"
    )
    val dims = mrqc.rangeDimensions
    def transform(df: DataFrame): DataFrame = {
      debugLog(s"(cleanupExtendRanges.transform) df.schema = ${df.schema.catalogString}")
      debugLog(s"(cleanupExtendRanges.transform)" +
        s" use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions")
      val df2nd = copyMultivarRangeCols2nd(df, dims)
      debugLog(s"(cleanupExtendRanges.transform) df2nd.schema = ${df2nd.schema.catalogString}")
      val fenestra = Window.partitionBy((keys ++ dims.map(_.fromCol2Name)).map(col): _*)

      val dfJoinCleanExtend =
        unifyMultivarRanges(df = df2nd,
          mrqc = mrqc.config2,
          keys = keys,
          extend = extend,
          fillGapsWithNull = fillGapsWithNull
        )
          .withColumn(mrqc.definedColName,
            mrqc.applyBooleanColumnFunctionToIntervalDefs(boolColFun = dim => col(dim.toColName).isNotNull))
          .withColumns(colsMap = dims.map(d => (d.fromColName, coalesce(col(d.fromColName), col(d.fromCol2Name)))).toMap)
          .withColumns(colsMap = dims.map(d => (d.toColName, coalesce(col(d.toColName), col(d.toCol2Name)))).toMap)
      if (logger.isDebugEnabled()) dfJoinCleanExtend.createdLog("dfJoinCleanExtend")

      debugLog("(cleanupExtendRanges.transform) add aggregations if defined, implemented as analytical functions...")
      val dfAgg = aggExpressions.foldLeft(dfJoinCleanExtend) {
        case (df_acc, (name, expr)) => df_acc.withColumn(name, expr.over(fenestra))
      }

      debugLog(s"(cleanupExtendRanges.transform) Prioritize and clean overlaps:" +
        s" rnkExpressions=${rnkExpressions.mkString(",")}")
      val rnkColName = "_rnk"
      val dfClean = if (rnkExpressions.nonEmpty) {
        require(
          !df.columns.contains(rnkColName),
          s"(cleanupExtendRanges) Your dataframe must not contain columns named $rnkColName" +
            s" if rnkExpressions are defined! df.columns = ${df.columns.mkString(",")}"
        )
        val df_rnk = dfAgg.withColumn(rnkColName, row_number.over(fenestra.orderBy(rnkExpressions: _*)))
        if (rnkFilter) df_rnk.where(col(rnkColName) === 1) else df_rnk
      } else dfAgg

      val selCols: Seq[Column] = keys.map(dfClean(_)) ++
        df.columns.diff(keys ++ mrqc.technicalColNames).map(dfClean(_)) ++
        aggExpressions.map(e => col(e._1)) ++
        (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Nil) ++
        dims.flatMap(d => List(col(d.fromCol2Name).as(d.fromColName), col(d.toCol2Name).as(d.toColName))) :+
        // dfClean(mrqc.fromColName2).as(mrqc.fromColName) :+ dfClean(mrqc.toColName2).as(mrqc.toColName) :+
        mrqc.definedCol
      debugLog(s"(cleanupExtendRanges.transform) select final schema: selCols = ${selCols.mkString(",")}")
      dfClean.select(selCols: _*)
    }
    val resultCleanExtend = keepAlias(df, transform)
    if (logger.isDebugEnabled()) resultCleanExtend.createdLog("resultCleanExtend")
    resultCleanExtend
  }

  /**
   * outer join
   *
   * @param doCleanupExtend
   *   set to false if cleanupExtendsIntervals is already executed on the input DataFrames.
   */
  private[temporalquery] def outerJoinRangesWithKey[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      rnkExpressions: Seq[Column],
      additionalJoinFilterCondition: Column,
      joinType: String,
      doCleanupExtend: Boolean
  )(implicit logger: Logger): DataFrame = {
    // extend data frames
    val df1Extended = if ((joinType == "full" || joinType == "right") && doCleanupExtend)
      cleanupExtendRanges(df = df1, keys = keys, mrqc = mrqc, rnkFilter = true,
        rnkExpressions = rnkExpressions.intersect(df1.columns.map(col))
      )
    else df1
    val df2Extended = if ((joinType == "full" || joinType == "left") && doCleanupExtend)
      cleanupExtendRanges(df = df2, keys = keys, mrqc = mrqc, rnkFilter = true,
        rnkExpressions = rnkExpressions.intersect(df2.columns.map(col))
      )
    else df2
    val (df1ExtendedDefined, df2ExtendedDefined, definedFilter) = if (joinType == "full") {
      (df1Extended.withColumnRenamed(mrqc.definedColName, s"${mrqc.definedColName}_l"),
        df2Extended.withColumnRenamed(mrqc.definedColName, s"${mrqc.definedColName}_r"),
        col(s"${mrqc.definedColName}_l") or col(s"${mrqc.definedColName}_r"))
    } else (df1Extended.drop("_defined"), df2Extended.drop("_defined"), lit(true))
    val dfJoinRaw = joinRanges(df1ExtendedDefined, df2ExtendedDefined, keys, mrqc, joinType, additionalJoinFilterCondition)
      .where(definedFilter)
      .drop(mrqc.definedColName, s"${mrqc.definedColName}_l", s"${mrqc.definedColName}_r")
    logger.info(s"(outerJoinRangesWithKey) dfJoinRaw.schema = ${dfJoinRaw.schema.catalogString}")
    Try(combineMultivarRanges(df = dfJoinRaw, mrqc = mrqc)) match {
      case Success(dfCombined) => dfCombined
      case Failure(e)          =>
        logger.warn(s"(outerJoinRangesWithKey) Could not combine ranges of dfJoinRaw!" +
          s" Returning dfJoinRaw with uncombined ranges ")
        dfJoinRaw
    }
  }

  /**
   * left anti join
   */
  private[temporalquery] def leftAntiJoinRanges[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      additionalJoinFilterCondition: Column
      // TODO: Why we require closed interval? Why not IntervalMultidimQueryConfig[T, _]
  )(implicit logger: Logger): DataFrame = {
    if (keys.isEmpty) logger.warn(s"(leftAntiJoinRanges) no keys specified!" +
      s" additionalJoinFilterCondition = $additionalJoinFilterCondition ; mrqc = $mrqc")
    else debugLog(
      s"(leftAntiJoinRanges) START: additionalJoinFilterCondition = $additionalJoinFilterCondition ;" +
        s" keys = (${keys.mkString(",")})"
    )
    debugLog(s"(leftAntiJoinRanges) df1.schema = ${df1.schema.catalogString}")
    debugLog(s"(leftAntiJoinRanges) df2.schema = ${df2.schema.catalogString}")
    val df1Cols: Array[Column] = df1.columns.map(df1(_))
    val df2Renamed = renameRangeCols2nd(df2, mrqc)

    val joinCondition: Column = createAliasKeyCondition(df1, df2Renamed, keys)
      .and(mrqc.joinRangeExpr(df1, df2Renamed))
      .and(additionalJoinFilterCondition)
    debugLog(s"(leftAntiJoinRanges) joinCondition = $joinCondition")

    debugLog("(leftAntiJoinRanges) dfAntiJoin contains all rows of df1 of which the range does not intersect" +
      " with any range of df2 for the same keys. dfAntiJoin is thus included completely in the result.")
    val dfAntiJoin = df1.join(df2Renamed, joinCondition, "leftanti")
    debugLog(s"(leftAntiJoinRanges) dfAntiJoin.schema = ${dfAntiJoin.schema.catalogString}")

    debugLog("(leftAntiJoinRanges) df1ExceptAntiJoin contains all rows of df1 of which the range intersects" +
      " a range of df2 for the same keys and thus need further treatment.")
    val df1ExceptAntiJoin = df1.except(dfAntiJoin)

    val dfJoinLeftAnti = df1ExceptAntiJoin.join(df2Renamed, joinCondition, "inner")
      .select(df1Cols ++ mrqc.fromToColnames2.map(col): _*)
    debugLog(s"(leftAntiJoinRanges) dfJoinLeftAnti.schema = ${dfJoinLeftAnti.schema.catalogString}")

    debugLog("(leftAntiJoinRanges) df2Combined contains the combined intersecting ranges of df2.")
    val df2Combined = combineMultivarRanges(df = dfJoinLeftAnti.select((keys ++ mrqc.fromToColnames2).map(col): _*),
      mrqc = mrqc.config2)
    debugLog(s"(leftAntiJoinRanges) df2Combined.schema = ${df2Combined.schema.catalogString}")

    debugLog("(leftAntiJoinRanges) dfComplementJoin contains the rows of df1ExceptAntiJoin" +
      " joined with the combined intersecting ranges of df2.")
    val dfComplementJoin = if (keys.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined)
    else df1ExceptAntiJoin.join(df2Combined, keys, "inner")
    debugLog(s"(leftAntiJoinRanges) dfComplementJoin.schema = ${dfComplementJoin.schema.catalogString}")

    // Note: we deliberately avoid Spark's udf()/Encoders here: since T is only known via
    // Ordering/TypeTag (not a concrete type), Spark's reflection-based encoder derivation can not
    // build an encoder for MultivarRange = List[(T,T)] (nested inside the path-dependent
    // MultivarRangeUnion case class), see SPARK's [ENCODER_NOT_FOUND]. We compute the complement
    // via a plain RDD transformation on Rows instead, which needs no encoder for T at all.
    val dims1 = mrqc.rangeDimensions
    val subtrahendRangeCol: Column = array(mrqc.config2.rangeDimensions.map { d =>
      struct(d.fromCol.as("f"), d.toCol.as("t"))
    }: _*)

    debugLog("(leftAntiJoinRanges) dfSubtrahends groups, for every row of df1ExceptAntiJoin," +
      " all ranges of df2Combined intersecting it.")
    val dfSubtrahends = dfComplementJoin
      .groupBy(df1.columns.map(col): _*)
      .agg(collect_set(subtrahendRangeCol).as("subtrahends"))
    debugLog(s"(leftAntiJoinRanges) dfSubtrahends.schema = ${dfSubtrahends.schema.catalogString}")

    val resultSchema = StructType(dfSubtrahends.schema.filterNot(_.name == "subtrahends"))
    val complementRDD: RDD[Row] = Try(dfSubtrahends.rdd.flatMap { row =>
      val minuend: mrqc.MultivarRange = dims1.map(d => (row.getAs[T](d.fromColName), row.getAs[T](d.toColName)))
      // TODO: convert to dataFrame code
      // Row.getAs[Seq[_]] on an array column actually yields a mutable.ArraySeq at runtime, which
      // is not a subtype of the immutable Seq that complementFamily expects. Go via
      // scala.collection.Seq (the common supertype) and force conversion to List with .toList
      // instead of relying on the (dynamically dispatched) map result type.
      val subtrahends: Seq[mrqc.MultivarRange] = row.getAs[scala.collection.Seq[scala.collection.Seq[Row]]]("subtrahends")
        .map(sub => sub.map(r => (r.getAs[T]("f"), r.getAs[T]("t"))).toList)
        .toList
      val complementUnion: mrqc.MultivarRangeUnion = mrqc.complementFamily(minuend, subtrahends)(NOPLogger.NOP_LOGGER)
      complementUnion.rangeFamily.map { mvr =>
        Row.fromSeq(resultSchema.fields.map { field =>
          val dimIdx = dims1.indexWhere(d => d.fromColName == field.name || d.toColName == field.name)
          if (dimIdx < 0) row.getAs[Any](field.name)
          else if (field.name == dims1(dimIdx).fromColName) mvr(dimIdx)._1 else mvr(dimIdx)._2
        })
      }
    }) match {
      case Success(rdd) => rdd
      case Failure(e)   =>
        logger.error(s"(leftAntiJoinRanges) Could not calculate complementRDD !!!")
        dfSubtrahends.createdLog("dfSubtrahends")
        throw e
    }
    val dfComplement = df1.sparkSession.createDataFrame(complementRDD, resultSchema)
    debugLog(s"(leftAntiJoinRanges) dfComplement.schema = ${dfComplement.schema.catalogString}")

    val dfComplementRaw = dfAntiJoin.union(dfComplement.select(df1.columns.map(col): _*))
    logger.info(s"(leftAntiJoinRanges) dfComplementRaw.schema = ${dfComplementRaw.schema.catalogString}")

    combineMultivarRanges(
      df = cleanupExtendRanges(df = dfComplementRaw, keys = keys, mrqc = mrqc, extend = false, fillGapsWithNull = false)
        .drop("_defined"),
      mrqc = mrqc
    )

  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      additionalTechnicalColNames: List[String],
      ignoreColNames: Seq[String]
  )(implicit logger: Logger): DataFrame = Try(
    keepAlias(
      df = df,
      transform = (df: DataFrame) => {
        debugLog(s"(combineDimensionRanges.transform) dim = $dim ;" +
          s" additionalTechnicalColNames = ${additionalTechnicalColNames.mkString(",")} ;" +
          s" ignoreColNames = ${ignoreColNames.mkString(",")} ; ")
        if (logger.isDebugEnabled()) df.debLog("df")
        val dfColumns = df.columns
        val compareCols = dfColumns.diff(ignoreColNames ++ List(dim.fromColName, dim.toColName) ++ additionalTechnicalColNames)
        val fenestra = Window.partitionBy(compareCols.map(col): _*).orderBy(dim.fromCol)
        val nbColName = "_nb"
        val consecutiveColName = "_consecutive"
        require(
          !df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName),
          s"(combineIntervals) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns.mkString(",")}"
        )
        df.withColumn(consecutiveColName,
          coalesce(dim.intDef.getPredecessorExpr(dim.fromCol) <= lag(dim.toCol, 1).over(fenestra), lit(false)))
          .withColumn(nbColName, sum(when(col(consecutiveColName), lit(0)).otherwise(lit(1))).over(fenestra))
          .groupBy(compareCols.map(col) :+ col(nbColName): _*)
          .agg(min(dim.fromCol).as(dim.fromColName), max(dim.toCol).as(dim.toColName))
          .drop(nbColName)
          .select(dfColumns.map(col): _*)
      }
    )
  ) match {
    case Success(dfCombined) => dfCombined
    case Failure(e)          =>
      logger.error(s"(combineDimensionRanges) Could not combine ranges!")
      logger.error(s"(combineDimensionRanges) df.schema                   = ${df.schema.catalogString}")
      logger.error(s"(combineDimensionRanges) dim                         = $dim")
      logger.error(s"(combineDimensionRanges) additionalTechnicalColNames = $additionalTechnicalColNames")
      logger.error(s"(combineDimensionRanges) ignoreColNames              = $ignoreColNames")
      throw e
  }

  /**
   * Combines consecutive records when there is no change in the non-technical columns. The
   * dataframe is first cleaned up via [[roundIntervalsToDiscreteTime]], see its description.
   */
  @tailrec
  private[temporalquery] def combineMultivarRanges[T: Ordering: TypeTag](
      df: DataFrame,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      ignoreColNames: Seq[String] = Nil,
      runId: Int = 1
  )(implicit logger: Logger): DataFrame = {
    logger.info(
      s"(combineMultivarRanges(runId=$runId)) START numDimensions = ${mrqc.numDimensions} ; df1.schema = ${df.schema.catalogString} ;" +
        s" ignoreColNames = ${ignoreColNames.mkString(",")} ; mrqc = $mrqc"
    )
    val dims = mrqc.rangeDimensions
    val resultatCombine = dims.foldLeft(df.where(mrqc.isNonEmptyRangeExpr)) { case (df, dim) =>
      combineDimensionRanges(df.where(mrqc.isNonEmptyRangeExpr), dim, mrqc.additionalTechnicalColNames, ignoreColNames)
    }
    if (logger.isDebugEnabled()) resultatCombine.createdLog("resultatCombine")
    if (mrqc.numDimensions == 1 || df.except(resultatCombine).isEmpty) {
      logger.info(s"(combineMultivarRanges(runId=$runId)) DONE: returning resultatCombine: schema =" +
        s" ${resultatCombine.schema.catalogString} , mrqc=$mrqc")
      resultatCombine
    } else {
      debugLog(s"(combineMultivarRanges(runId=$runId))" +
        s" calling combineMultivarRanges once again to ensure that everything is combined")
      combineMultivarRanges[T](resultatCombine, mrqc, ignoreColNames, runId + 1)
    }
  }

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
      val df2Ranges = renameKeys(df = renameDimensionCols2nd(
          df = buildDimensionRanges(df, keys, dim, extend),
          dim
        ).as("ranges"),
        keys = keys, postFix = joinColPostFix2
      )
      if (logger.isDebugEnabled()) df2Ranges.createdLog("df2Ranges")
      val keyCondition = createRenamedKeyCondition(keys)
      val joinType = if (fillGapsWithNull) "left" else "inner"
      val joinCondition = keyCondition and
        intDef.isInIntervalExpr(valueCol = dim.fromCol2, fromCol = dim.fromCol, toCol = dim.toCol)
      debugLog(s"(unifyDimensionRanges.transform) join back on input df: df2Ranges.join(df1Renamed) with " +
        s" joinType = $joinType , joinCondition = $joinCondition")
      val dfJoinUnify = df2Ranges.join(right = df1Renamed, joinExprs = joinCondition, joinType = joinType)
      if (logger.isDebugEnabled()) dfJoinUnify.createdLog("dfJoinUnify")
      val selCols = keys.map(key => col(s"$key$joinColPostFix2").as(key)) ++
        df.columns.diff(keys ++ List(dim.fromColName, dim.toColName) ++ additionalTechnicalColNames).map(dfJoinUnify(_)) :+
        dim.fromCol2.as(dim.fromColName) :+ dim.toCol2.as(dim.toColName)
      debugLog(s"(unifyDimensionRanges.transform) select result: selCols = ${selCols.mkString(",")}")
      val resultUnifyTransform = dfJoinUnify.select(selCols: _*)
      if (logger.isDebugEnabled()) resultUnifyTransform.createdLog("resultUnifyTransform")
      resultUnifyTransform
    }
    val resultDimUnify = keepAlias(df, transform)
    debugLog(s"(unifyDimensionRanges) resultDimUnify.schema = ${resultDimUnify.schema.catalogString}")
    resultDimUnify
  }

  /**
   * Unify ranges
   */
  private[temporalquery] def unifyMultivarRanges[T: Ordering: TypeTag](
      df: DataFrame,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      keys: Seq[String] = Nil,
      extend: Boolean = false,
      fillGapsWithNull: Boolean = false
  )(implicit logger: Logger): DataFrame = {
    logger.info(
      s"(unifyMultivarRanges) START numDimensions = ${mrqc.numDimensions} ; df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}," +
        s" extend = $extend ; fillGapsWithNull = $fillGapsWithNull, mrqc = $mrqc"
    )
    val dims = mrqc.rangeDimensions
    val dfResultUnify = dims.zip(dims.inits.toSeq.tail.reverse).foldLeft(df) { case (df, (dim, prevDims)) =>
      unifyDimensionRanges(
        df = df,
        keys = keys ++ prevDims.map(_.fromColName) ++ prevDims.map(_.toColName),
        dim = dim,
        extend = extend,
        fillGapsWithNull = fillGapsWithNull,
        additionalTechnicalColNames = mrqc.additionalTechnicalColNames
      )
    }
    logger.info(s"(unifyMultivarRanges) dfResultUnify.schema = ${dfResultUnify.schema.catalogString}")
    dfResultUnify
  }

  /**
   * extend ranges
   */
  private[temporalquery] def extendDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      extendMin: Boolean,
      extendMax: Boolean
  )(implicit logger: Logger): DataFrame = {
    val fromMinColName = s"_${dim.fromColName}_min"
    val toMaxColName = s"_${dim.toColName}_max"
    require(
      !df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName),
      s"(extendIntervalRanges) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns.mkString(",")}"
    )
    debugLog("if no keys are given, we work with the global min/maximum.")
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(false))
    debugLog(s"(extendDimensionRanges) extendMin = $extendMin ; extendMax = $extendMax ;" +
      s" df.schema = ${df.schema.catalogString} ; keyCols = ${keyCols.mkString(",")} ; dim = $dim")
    val df_prep = df
      .withColumn(fromMinColName, if (extendMin) min(col(dim.fromColName)).over(Window.partitionBy(keyCols: _*)) else lit(null))
      .withColumn(toMaxColName, if (extendMax) max(col(dim.toColName)).over(Window.partitionBy(keyCols: _*)) else lit(null))
    if (logger.isDebugEnabled()) df_prep.createdLog("df_prep")
    val selCols = df.columns.filter(c => c != dim.fromColName && c != dim.toColName).map(col) :+
      when(dim.fromCol === col(fromMinColName), lit(dim.lowerHorizon)).otherwise(dim.fromCol).as(dim.fromColName) :+
      when(dim.toCol === col(toMaxColName), lit(dim.upperHorizon)).otherwise(dim.toCol).as(dim.toColName)
    debugLog(s"(extendDimensionRanges) selCols = ${selCols.mkString(",")}")
    df_prep.select(selCols: _*)
  }

  /**
   * extend valid_from/to to min/maxDate
   */
  private[temporalquery] def extendMultivarRanges[T: Ordering: TypeTag](
      df: DataFrame,
      keys: Seq[String],
      extendMin: Boolean,
      extendMax: Boolean
  )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]], logger: Logger): DataFrame = {
    debugLog(s"(extendIntervalRanges) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(s"(extendIntervalRanges) extendMin = $extendMin ; extendMax = $extendMax")
    debugLog(s"(extendIntervalRanges) mrqc = $mrqc")
    if (extendMin || extendMax) {
      val dims = mrqc.rangeDimensions
      dims.foldLeft(df) { case (df, dim) =>
        extendDimensionRanges[T](
          df = df,
          keys = keys,
          dim = dim,
          extendMin = extendMin,
          extendMax = extendMax
        )
      }
    } else {
      logger.warn(s"(extendIntervalRanges) extendMin = $extendMin and extendMax = $extendMax ==> Nothing to do!" +
        s" Why did you call me?")
      df
    }
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
  private def renameRangeCols2nd[T: Ordering: TypeTag](df: DataFrame, mrqc: MultivarRangeQueryConfig[T, _])(implicit
      logger: Logger
  ): DataFrame = {
    val dfColumns = df.columns
    require(
      mrqc.fromToColnames.forall(dfColumns.contains),
      s"(renameRangeCols2nd) DataFrame df must contain all columns ${mrqc.fromToColnames.mkString(",")}" +
        s" but df.schema = ${df.schema.catalogString}!"
    )
    require(
      !mrqc.fromToColnames2.exists(dfColumns.contains),
      s"(renameRangeCols2nd) DataFrame df must not contain any column of ${mrqc.fromToColnames2.mkString(",")}" +
        s" but df.schema = ${df.schema.catalogString}!"
    )
    val renameMap: Map[String, String] = mrqc.rangeDimensions.flatMap { d =>
      List((d.fromColName, d.fromCol2Name), (d.toColName, d.toCol2Name))
    }.toMap
    Try(df.withColumnsRenamed(renameMap)) match {
      case Success(dfRenamed) => dfRenamed
      case Failure(e)         =>
        logger.error(s"(renameRangeCols2nd) Could not rename range columns!")
        logger.error(s"(renameRangeCols2nd) df.schema = ${df.schema.catalogString}")
        logger.error(s"(renameRangeCols2nd) renameMap = $renameMap")
        throw e
    }
  }

  /**
   * Helper method to copy main pair of interval columns as 2nd pair of interval columns defined in
   * IntervalQueryConfig
   */
  private def copyMultivarRangeCols2nd[T: Ordering: TypeTag](
      df: DataFrame,
      dims: List[IntervalQueryDimension[T, _]]
  ): DataFrame = df
    .withColumns(colsMap = dims.map(d => (d.fromCol2Name, col(d.fromColName))).toMap)
    .withColumns(colsMap = dims.map(d => (d.toCol2Name, col(d.toColName))).toMap)

  /**
   * Renders the first two interval dimensions of the DataFrame as an SVG string.
   *
   * Each row becomes a `<rect>` whose horizontal extent maps to the first interval dimension and
   * whose vertical extent maps to the second; higher dimensions are ignored. A bounding rectangle
   * (no fill, black border) frames the entire data space.
   *
   * Colour encoding of `valueCol`:
   *   - Numeric columns (Double, Float, Long, Int, …): HSL heat-map from H=240 (blue, minimum
   *     value) to H=0 (red, maximum value) through the full visible spectrum.
   *   - Other types: a categorical palette of ten distinct colours.
   *   - Null values: grey (#cccccc).
   *
   * For [[ClosedInterval]] dimensions the rendered rectangle extends to `successor(to)` so that the
   * last discrete step is fully covered visually; for [[interval.HalfOpenInterval]] the `to` value
   * is used directly. Rectangles are outlined only for closed intervals.
   *
   * Both axes share the same scale so that the aspect ratio of the data space is preserved; the
   * longer axis fills up to 1024 px.
   *
   * @param valueCol
   *   name of the column whose value determines the rectangle fill colour
   * @param drawDiagonal
   *   if true, draw a black line for the diagonal of the dimension space, i.e. the line containing
   *   all points where the two dimensions' coordinates coincide. Only the part of that line lying
   *   inside the viewbox is drawn; if the diagonal lies completely outside the viewbox, nothing is
   *   drawn.
   */
  private[temporalquery] def toSvg[T: Ordering: TypeTag](
      df: DataFrame,
      valueCol: String,
      svgMax: Double,
      drawDiagonal: Boolean,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]
  ): String = {
    require(1 < mrqc.numDimensions,
      s"toSvg works only for multi-dimensional data but, numDimensions=${mrqc.numDimensions}")

    val dim1 = mrqc.rangeDimensions.head
    val dim2 = mrqc.rangeDimensions(1)
    val isClosed = dim1.intDef.isInstanceOf[ClosedInterval[_]]

    val clampedDf = {
      val rawCols = mrqc.rangeDimensions.flatMap { dim =>
        Seq(
          s"_raw_${dim.fromColName}" -> col(dim.fromColName),
          s"_raw_${dim.toColName}"   -> col(dim.toColName)
        )
      }.toMap
      val clampCols = mrqc.rangeDimensions.flatMap { dim =>
        val lo = lit(dim.lowerHorizon)
        val hi = lit(dim.upperHorizon)
        Seq(
          dim.fromColName -> least(greatest(col(dim.fromColName), lo), hi),
          dim.toColName   -> least(greatest(col(dim.toColName), lo), hi)
        )
      }.toMap
      df.withColumns(rawCols).withColumns(clampCols)
    }
    val rows = clampedDf.collect()
    if (rows.isEmpty) return """<svg xmlns="http://www.w3.org/2000/svg"/>"""

    val schema = clampedDf.schema
    val from1Idx = schema.fieldIndex(dim1.fromColName)
    val to1Idx = schema.fieldIndex(dim1.toColName)
    val from2Idx = schema.fieldIndex(dim2.fromColName)
    val to2Idx = schema.fieldIndex(dim2.toColName)
    val valueIdx = schema.fieldIndex(valueCol)
    val rawFrom1Idx = schema.fieldIndex(s"_raw_${dim1.fromColName}")
    val rawTo1Idx = schema.fieldIndex(s"_raw_${dim1.toColName}")
    val rawFrom2Idx = schema.fieldIndex(s"_raw_${dim2.fromColName}")
    val rawTo2Idx = schema.fieldIndex(s"_raw_${dim2.toColName}")

    // For a closed interval [from, to] the visual extent ends at successor(to): the "to" value
    // is the last *included* discrete step, so the rectangle must cover one full step beyond it.
    // For a half-open interval [from, to) the "to" value is already the exclusive upper bound.
    // We precompute a per-dimension converter (Any => Double) to avoid passing the existential
    // intDef type to a typed parameter — Scala 2 won't auto-upcast the wildcard.
    def toDoubleVia(intDef: Any): Any => Double = intDef match {
      case ci: ClosedInterval[T @unchecked] => (v: Any) => anyToDouble(ci.successor(v.asInstanceOf[T]))
      case _                                => anyToDouble
    }
    val visualTo1 = toDoubleVia(dim1.intDef)
    val visualTo2 = toDoubleVia(dim2.intDef)

    // (from1, visualTo1, from2, visualTo2, value, rawFrom1, rawTo1, rawFrom2, rawTo2)
    val rects = rows.map { row =>
      (anyToDouble(row(from1Idx)), visualTo1(row(to1Idx)),
        anyToDouble(row(from2Idx)), visualTo2(row(to2Idx)),
        row(valueIdx),
        row(rawFrom1Idx), row(rawTo1Idx), row(rawFrom2Idx), row(rawTo2Idx))
    }

    // lowerHorizon / upperHorizon are sentinel "infinity" values (e.g. 1970-01-01 / 9999-12-31).
    // We extract them via a pattern match on Any to avoid Scala 2 existential-type restrictions.
    def horizonsOf(intDef: Any): (Double, Double) = intDef match {
      case id: IntervalDef[_] => (anyToDouble(id.lowerHorizon), anyToDouble(id.upperHorizon))
      case _                  => (Double.NegativeInfinity,      Double.PositiveInfinity)
    }
    val (lowerH1, upperH1) = horizonsOf(dim1.intDef)
    val (lowerH2, upperH2) = horizonsOf(dim2.intDef)

    // Derive the viewable range from the non-sentinel ("real") values only, then pad by 10 %.
    // This prevents the plot from being dominated by intervals that span all the way to the
    // infinity sentinels, which would compress all the interesting data into a thin green strip.
    def realBounds(vals: Array[Double], lo: Double, hi: Double): (Double, Double) = {
      val real = vals.filterNot(v => v == lo || v == hi)
      if (real.isEmpty) (vals.min, vals.max)
      else {
        val rMin = real.min
        val rMax = real.max
        val pad = if (rMax == rMin) math.abs(rMin) * 0.1 + 1d else (rMax - rMin) * 0.1
        (rMin - pad, rMax + pad)
      }
    }
    val (viewMinX, viewMaxX) = realBounds(rects.map(_._1) ++ rects.map(_._2), lowerH1, upperH1)
    val (viewMinY, viewMaxY) = realBounds(rects.map(_._3) ++ rects.map(_._4), lowerH2, upperH2)

    val dataW: Double = viewMaxX - viewMinX
    val dataH: Double = viewMaxY - viewMinY

    val scale = if (dataW < 0.1d && dataH < 0.1d) 1d else svgMax / math.max(dataW, dataH)

    val svgW = math.ceil(dataW * scale).toInt max 1
    val svgH = math.ceil(dataH * scale).toInt max 1

    // Three ticks per axis, at the beginning, the middle and the end of the visible range,
    // labelled with the actual coordinate value (formatted according to the dimension's type).
    val fs = 11 // column-name caption font size
    val tickFs = 10 // tick value label font size
    val tickLen = 4 // tick mark length, in px
    def formatTick(v: Double, dataType: DataType): String = dataType match {
      case DateType      => new java.sql.Date(v.round).toString
      case TimestampType => new java.sql.Timestamp(v.round).toString
      case _             =>
        val rounded = math.round(v * 1000d) / 1000d
        if (rounded == rounded.toLong.toDouble) rounded.toLong.toString else rounded.toString
    }
    val dim1Type = schema(dim1.fromColName).dataType
    val dim2Type = schema(dim2.fromColName).dataType
    val xTickValues = Seq(viewMinX, (viewMinX + viewMaxX) / 2, viewMaxX)
    val yTickValues = Seq(viewMinY, (viewMinY + viewMaxY) / 2, viewMaxY)
    val xTickLabels = xTickValues.map(formatTick(_, dim1Type))
    val yTickLabels = yTickValues.map(formatTick(_, dim2Type))

    // Margins outside the data rectangle, sized to fit the tick labels plus the existing
    // from/to column-name captions. Left margin layout (left to right): rotated caption strip,
    // gap, Y tick labels, gap, Y tick marks. Bottom margin layout (top to bottom): X tick marks,
    // gap, X tick labels, gap, column-name caption line.
    val leftCaptionW = fs + 4
    val yTickLabelW = math.ceil(yTickLabels.map(_.length).max * tickFs * 0.6).toInt
    val marginLeft = leftCaptionW + 4 + yTickLabelW + 3 + tickLen
    val marginBottom = tickLen + 2 + tickFs + 3 + fs + 3
    val marginTop = math.ceil(tickFs / 2.0).toInt + 4
    val totalW = marginLeft + svgW
    val totalH = marginTop + svgH + marginBottom

    // Data rectangle occupies [marginLeft, marginLeft+svgW) × [marginTop, marginTop+svgH).
    // Coordinates that map outside this area are clipped by SVG's overflow:hidden.
    def sx(x: Double): Double = marginLeft + (x - viewMinX) * scale
    def sy(y: Double): Double = marginTop + svgH - (y - viewMinY) * scale

    // Convert HSL (h∈[0,360), s∈[0,1], l∈[0,1]) to a hex colour string
    def hsl2hex(h: Double, s: Double, l: Double): String = {
      val c = (1d - math.abs(2 * l - 1)) * s
      val x = c * (1d - math.abs(h / 60 % 2 - 1))
      val m = l - c / 2
      val (r1, g1, b1) =
        if (h < 60) (c, x, 0d)
        else if (h < 120) (x, c, 0d)
        else if (h < 180) (0d, c, x)
        else if (h < 240) (0d, x, c)
        else if (h < 300) (x, 0d, c)
        else (c,              0d, x)
      f"#${((r1 + m) * 255).round}%02x${((g1 + m) * 255).round}%02x${((b1 + m) * 255).round}%02x"
    }

    val nonNullValues = rects.flatMap { case (_, _, _, _, v, _, _, _, _) => Option(v) }
    val isNumeric = nonNullValues.nonEmpty && nonNullValues.forall(_.isInstanceOf[java.lang.Number])

    val fillOf: Any => String = if (isNumeric) {
      val nums = nonNullValues.map(_.asInstanceOf[java.lang.Number].doubleValue())
      val minVal = nums.min
      val range = nums.max - minVal
      (v: Any) =>
        v match {
          case null                => "#cccccc"
          case n: java.lang.Number =>
            val t = if (range == 0) 0.5 else (n.doubleValue() - minVal) / range
            hsl2hex(240d * (1d - t), 1d, 0.5) // hue: 240 = blue, 0 = red
          case _ => "#cccccc"
        }
    } else {
      val palette = Array("#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf")
      val catMap = nonNullValues.distinct.zipWithIndex
        .map { case (v, i) => v -> palette(i % palette.length) }.toMap
      (v: Any) => if (v == null) "#cccccc" else catMap.getOrElse(v, "#808080")
    }

    val dw = math.min(totalW, totalH) / 100d
    val strokeAttr =
      if (isClosed) """ stroke="black" stroke-width="0.5""""
      else f""" stroke="black" stroke-width="0.5" stroke-dasharray="$dw%.2f,$dw%.2f""""

    val sb = new StringBuilder
    sb.append(s"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 $totalW $totalH" width="$totalW"  height="$totalH">\n""")
    // Data bounding rectangle – positioned at (marginLeft, marginTop), sized svgW × svgH
    sb.append(
      s"""  <rect x="$marginLeft" y="$marginTop" width="$svgW" height="$svgH" fill="none" stroke="black" stroke-width="1"/>\n"""
    )
    // X axis ticks: beginning/middle/end of dim1's visible range, below the bounding rectangle.
    val xTickAnchors = Seq("start", "middle", "end")
    for (((tickX, label), anchor) <- xTickValues.map(sx).zip(xTickLabels).zip(xTickAnchors)) {
      val yLine = marginTop + svgH
      sb.append(
        f"""  <line x1="$tickX%.2f" y1="$yLine" x2="$tickX%.2f" y2="${yLine + tickLen}" stroke="black" stroke-width="1"/>\n"""
      )
      sb.append(
        f"""  <text x="$tickX%.2f" y="${yLine + tickLen + tickFs}" font-size="$tickFs" fill="#444" text-anchor="$anchor">$label</text>\n"""
      )
    }
    // Y axis ticks: beginning/middle/end of dim2's visible range, left of the bounding rectangle.
    for ((tickY, label) <- yTickValues.map(sy).zip(yTickLabels)) {
      sb.append(
        f"""  <line x1="${marginLeft -
            tickLen}%.2f" y1="$tickY%.2f" x2="$marginLeft%.2f" y2="$tickY%.2f" stroke="black" stroke-width="1"/>\n"""
      )
      sb.append(
        f"""  <text x="${marginLeft - tickLen -
            3}%.2f" y="$tickY%.2f" font-size="$tickFs" fill="#444" text-anchor="end" dominant-baseline="middle">$label</text>\n"""
      )
    }
    // Column-name captions in the margin areas, outside the tick labels.
    val xCenter = leftCaptionW / 2 // centre of the leftmost strip for rotated Y captions
    val yLow = marginTop + svgH * 3 / 4 // lower-half position for "from" caption
    val yHigh = marginTop + svgH / 4 // upper-half position for "to" caption
    val yText = totalH - 3 // baseline of the bottom-most caption line
    sb.append(s"""  <text x="${marginLeft +
        2}" y="$yText" font-size="$fs" fill="#444" text-anchor="start">${dim1.fromColName}</text>\n""")
    sb.append(s"""  <text x="${totalW - 2}" y="$yText" font-size="$fs" fill="#444" text-anchor="end">${dim1.toColName}</text>\n""")
    sb.append(
      s"""  <text x="$xCenter" y="$yLow" font-size="$fs" fill="#444" text-anchor="middle" transform="rotate(-90,$xCenter,$yLow)">${dim2.fromColName}</text>\n"""
    )
    sb.append(
      s"""  <text x="$xCenter" y="$yHigh" font-size="$fs" fill="#444" text-anchor="middle" transform="rotate(-90,$xCenter,$yHigh)">${dim2.toColName}</text>\n"""
    )
    for ((from1, to1, from2, to2, value, rawF1, rawT1, rawF2, rawT2) <- rects) {
      sb.append(
        s"  <!-- ${dim1.fromColName}=$rawF1 ${dim1.toColName}=$rawT1 ${dim2.fromColName}=$rawF2 ${dim2.toColName}=$rawT2 value=$value -->\n"
      )
      val x = sx(from1)
      val y = sy(to2)
      val w = (to1 - from1) * scale
      val h = (to2 - from2) * scale
      sb.append(f"""  <rect x="$x%.2f" y="$y%.2f" width="$w%.2f" height="$h%.2f" fill="${fillOf(value)}"$strokeAttr/>\n""")
      // Intersect with the data area [marginLeft, marginLeft+svgW] × [marginTop, marginTop+svgH]
      val vx0 = math.max(x, marginLeft.toDouble)
      val vx1 = math.min(x + w, (marginLeft + svgW).toDouble)
      val vy0 = math.max(y, marginTop.toDouble)
      val vy1 = math.min(y + h, (marginTop + svgH).toDouble)
      val visW = vx1 - vx0
      val visH = vy1 - vy0
      if (visW > 0 && visH > 0) {
        // Cap the font size so the rendered text stays within the rectangle: one bound keeps a
        // single line of text from overflowing along the rectangle's short side, while the other
        // bound accounts for the number of characters, using an average glyph width of ~0.6 *
        // font-size that holds for common sans-serif fonts. For tall, narrow rectangles (height
        // more than 3x the width) the text is rotated 90° so it runs along the height, which is
        // then the constraint the character count is measured against, allowing a larger font.
        val numChars = math.max(String.valueOf(value).length, 1)
        val rotate = visH > 3 * visW
        val (fitW, fitH) = if (rotate) (visH, visW) else (visW, visH)
        val fontSizeByHeight = fitH * 0.8
        val fontSizeByWidth = fitW / (numChars * 0.6)
        val fontSize = math.max(1d, math.min(fontSizeByHeight, fontSizeByWidth)) * 0.8
        val cx = (vx0 + vx1) / 2
        val cy = (vy0 + vy1) / 2
        val cxStr = f"$cx%.2f"
        val cyStr = f"$cy%.2f"
        val transformAttr = if (rotate) " transform=\"rotate(-90," + cxStr + "," + cyStr + ")\"" else ""
        sb.append(
          f"""  <text x="$cxStr" y="$cyStr" font-size="$fontSize%.2f" fill="black" text-anchor="middle" dominant-baseline="middle"$transformAttr>$value</text>\n"""
        )
      }
    }
    if (drawDiagonal) {
      // The diagonal of the dimension space is the line where dim1's coordinate equals dim2's,
      // i.e. all points (t, t). Clip it against the viewbox [viewMinX,viewMaxX] x [viewMinY,viewMaxY]:
      // t must lie in both dimensions' visible ranges at once.
      val tLo = math.max(viewMinX, viewMinY)
      val tHi = math.min(viewMaxX, viewMaxY)
      if (tLo <= tHi) {
        val (x1, y1) = (sx(tLo), sy(tLo))
        val (x2, y2) = (sx(tHi), sy(tHi))
        sb.append(f"""  <line x1="$x1%.2f" y1="$y1%.2f" x2="$x2%.2f" y2="$y2%.2f" stroke="black" stroke-width="1"/>\n""")
      }
    }
    sb.append("</svg>")
    sb.toString()
  }

}
