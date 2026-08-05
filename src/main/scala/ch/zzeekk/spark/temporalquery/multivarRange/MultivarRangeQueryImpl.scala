package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.Logging
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, IntervalDef, IntervalQueryDimension}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias, UnaryNode}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row}
import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger

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
  def keepAlias(df: DataFrame, transform: DataFrame => DataFrame)(implicit logger: Logger): DataFrame = {
    debugLog("START keepAlias")
    if (logger.isDebugEnabled()) df.debLog("df")
    val dfTransformed = transform(df)
    if (logger.isDebugEnabled()) dfTransformed.debLog("dfTransformed")
    getAlias(df).map(transform(dfTransformed).alias).getOrElse(dfTransformed) // .distinct()
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

  private[temporalquery] def roundIntervalsToDiscreteTime[T: Ordering: TypeTag](
      df: DataFrame,
      clmrqc: ClosedMultivarRangeQueryConfig[T]
  ): DataFrame = {
    val dims = clmrqc.rangeDimensions
    df.withColumns(colsMap = dims.map(d => (d.fromColName, d.intDef.getCeilExpr(col(d.fromColName)))).toMap)
      .withColumns(colsMap = dims.map(d => (d.toColName, d.intDef.getFloorExpr(col(d.toColName)))).toMap)
      .where(clmrqc.isValidRangeExpr)
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
      .where(clmrqc.isValidRangeExpr)
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
      joinType: String = "inner",
      additionalJoinCondition: Column = lit(true)
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = {
    debugLog(
      s"(joinIntervals) joinType = $joinType ; additionalJoinCondition = $additionalJoinCondition ;" +
        s" keys = (${keys.mkString(",")}) ; joinColPostFix1 = $joinColPostFix1 ; joinColPostFix2 = $joinColPostFix2"
    )
    debugLog(s"(joinIntervals) df1.schema = ${df1.schema.catalogString}")
    debugLog(s"(joinIntervals) df2.schema = ${df2.schema.catalogString}")
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
    val df1Renamed = renameKeys(df = df1, keys = keys, postFix = joinColPostFix1)
    val df2Renamed = renameKeys(df = renameIntervalCols2nd(df2), keys = keys, postFix = joinColPostFix2)
    val keyCondition = createRenamedKeyCondition(keys)
    debugLog(s"(joinIntervals) df1Renamed.schema = ${df1Renamed.schema.catalogString}")
    debugLog(s"(joinIntervals) df2Renamed.schema = ${df2Renamed.schema.catalogString}")
    debugLog(s"(joinIntervals) keyCondition      = $keyCondition")
    val dfJoined = df1Renamed
      .join(df2Renamed, keyCondition and additionalJoinCondition and mrqc.joinRangeExpr(df1Renamed, df2Renamed), joinType)
    debugLog(s"(joinIntervals) dfJoined.schema   = ${dfJoined.schema.catalogString}")

    // select final schema
    val commonColNames = keys
    val commonCols = keys
      .map(key => coalesce(df1Renamed(s"$key$joinColPostFix1"), df2Renamed(s"$key$joinColPostFix2")).as(key))
    val colsDf1 = df1.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(commonColNames ++ mrqc.technicalColNames).map(df2(_))
    // val timeColumns = List(greatest(mrqc.fromCol, mrqc.fromCol2).as(mrqc.fromColName), least(mrqc.toCol, mrqc.toCol2).as(mrqc.toColName))
    val timeColumns = mrqc.rangeDimensions.map { dim =>
      List(greatest(dim.fromCol, dim.fromCol2).as(dim.fromColName), least(dim.toCol, dim.toCol2).as(dim.toColName))
    }.reduce((x, y) => x ++ y)
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    debugLog(s"(joinIntervals) selCols = ${selCols.mkString(",")}")
    dfJoined.select(selCols: _*)
  }

  private[temporalquery] def joinIntervalsWithKeysImpl[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      joinType: String = "inner"
  )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame =
    joinRanges(df1, df2, keys, joinType)

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
    debugLog(s"(buildIntervalRanges) get start/end-points for every key: ${mrqc.isValidRangeExpr}")
    val dfPoints = df
      .where(mrqc.isValidRangeExpr) // filter invalid intervals
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
    debugLog(s"(cleanupExtendRanges) df.schema = ${df.schema.catalogString} ; keys = ${keys.mkString(",")}")
    debugLog(
      s"(cleanupExtendRanges) rnkExpressions = ${rnkExpressions.mkString(",")} ; aggExpressions = ${aggExpressions.mkString(",")}"
    )
    debugLog(s"(cleanupExtendRanges) rnkFilter = $rnkFilter , extend = $extend ; fillGapsWithNull = $fillGapsWithNull")
    debugLog(s"(cleanupExtendRanges) mrqc = $mrqc")
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
          keys = keys,
          extend = extend,
          fillGapsWithNull = fillGapsWithNull,
          mrqc = mrqc.config2
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
      cleanupExtendRanges(df = df1, keys = keys, mrqc = mrqc, rnkFilter = true,
        rnkExpressions = rnkExpressions.intersect(df1.columns.map(col))
      ).drop(mrqc.definedColName)
    else df1
    val df2Extended = if ((joinType == "full" || joinType == "left") && doCleanupExtend)
      cleanupExtendRanges(df = df2, keys = keys, mrqc = mrqc, rnkFilter = true,
        rnkExpressions = rnkExpressions.intersect(df2.columns.map(col))
      ).drop(mrqc.definedColName)
    else df2
    // join df1 & df2
    joinRanges(df1Extended, df2Extended, keys, joinType, additionalJoinFilterCondition)
  }

  /**
   * left anti join
   */
  private[temporalquery] def leftAntiJoinRanges[T: Ordering: TypeTag](
      df1: DataFrame,
      df2: DataFrame,
      keys: Seq[String],
      additionalJoinFilterCondition: Column
      // TODO: Why we require closed interval? Why not IntervalMultidimQueryConfig[T, _]
  )(implicit mrqc: MultivarRangeQueryConfig[T, ClosedInterval[T]], logger: Logger): DataFrame = {
    debugLog(s"leftAntiJoinIntervals START: keys = ${keys.mkString(", ")}")
    debugLog(
      s"(leftAntiJoinRanges) START: additionalJoinFilterCondition = $additionalJoinFilterCondition ;" +
        s" keys = (${keys.mkString(",")})"
    )
    debugLog(s"(leftAntiJoinRanges) df1.schema = ${df1.schema.catalogString}")
    debugLog(s"(leftAntiJoinRanges) df2.schema = ${df2.schema.catalogString}")
    val df1Cols: Array[Column] = df1.columns.map(df1(_))
    val df2Renamed = renameIntervalCols2nd(df2)

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
    val complementRDD = dfSubtrahends.rdd.flatMap { row =>
      val minuend: mrqc.MultivarRange = dims1.map(d => (row.getAs[T](d.fromColName), row.getAs[T](d.toColName)))
      // Row.getAs[Seq[_]] on an array column actually yields a mutable.ArraySeq at runtime, which
      // is not a subtype of the immutable Seq that complementFamily expects. Go via
      // scala.collection.Seq (the common supertype) and force conversion to List with .toList
      // instead of relying on the (dynamically dispatched) map result type.
      val subtrahends: Seq[mrqc.MultivarRange] = row.getAs[scala.collection.Seq[scala.collection.Seq[Row]]]("subtrahends")
        .map(sub => sub.map(r => (r.getAs[T]("f"), r.getAs[T]("t"))).toList)
        .toList
      val complementUnion = mrqc.complementFamily(minuend, subtrahends)(NOPLogger.NOP_LOGGER)
      complementUnion.rangeFamily.map { mvr =>
        Row.fromSeq(resultSchema.fields.map { field =>
          val dimIdx = dims1.indexWhere(d => d.fromColName == field.name || d.toColName == field.name)
          if (dimIdx < 0) row.getAs[Any](field.name)
          else if (field.name == dims1(dimIdx).fromColName) mvr(dimIdx)._1 else mvr(dimIdx)._2
        })
      }
    }
    val dfComplement = df1.sparkSession.createDataFrame(complementRDD, resultSchema)
    debugLog(s"(leftAntiJoinRanges) dfComplement.schema = ${dfComplement.schema.catalogString}")

    val dfComplementRaw = dfAntiJoin.union(dfComplement.select(df1.columns.map(col): _*))
    logger.info(s"(leftAntiJoinRanges) dfComplementRaw.schema = ${dfComplementRaw.schema.catalogString}")

    combineMultivarRanges(
      df = cleanupExtendRanges(df = dfComplementRaw, keys = keys, mrqc = mrqc, extend = false, fillGapsWithNull = false),
      mrqc = mrqc
    ).drop("_defined")

  }

  /**
   * Combine consecutive records with same data values
   */
  private[temporalquery] def combineDimensionRanges[T: Ordering: TypeTag](
      df: DataFrame,
      dim: IntervalQueryDimension[T, _ <: IntervalDef[T]],
      additionalTechnicalColNames: List[String],
      ignoreColNames: Seq[String]
  )(implicit logger: Logger): DataFrame =
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

  /**
   * Combines consecutive records when there is no change in the non-technical columns. The
   * dataframe is first cleaned up via [[rangeRoundDiscreteTime]], see its description.
   */
  @tailrec
  private[temporalquery] def combineMultivarRanges[T: Ordering: TypeTag](
      df: DataFrame,
      mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
      ignoreColNames: Seq[String] = Nil,
      runId: Int = 1
  )(implicit logger: Logger): DataFrame = {
    debugLog(s"(combineMultivarRanges(runId=$runId)) df1.schema = ${df.schema.catalogString}")
    debugLog(s"(combineMultivarRanges(runId=$runId)) ignoreColNames = ${ignoreColNames.mkString(",")}")
    debugLog(s"(combineMultivarRanges(runId=$runId)) mrqc = $mrqc")
    val dims = mrqc.rangeDimensions
    val resultatCombine = dims.foldLeft(df.where(mrqc.isValidRangeExpr)) { case (df, dim) =>
      combineDimensionRanges(df.where(mrqc.isValidRangeExpr), dim, mrqc.additionalTechnicalColNames, ignoreColNames)
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
    val resultUnify = keepAlias(df, transform)
    if (logger.isDebugEnabled()) resultUnify.createdLog("resultUnify")
    resultUnify
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
    val dims = mrqc.rangeDimensions
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
  private def renameIntervalCols2nd[T: Ordering: TypeTag](df: DataFrame)(implicit mrqc: MultivarRangeQueryConfig[T, _]): DataFrame = {
    val dfColumns = df.columns
    require(
      mrqc.fromToColnames.forall(dfColumns.contains),
      s"(renameIntervalCols2nd) DataFrame df must contain all columns ${mrqc.fromToColnames.mkString(",")}" +
        s" but df.schema = ${df.schema.catalogString}!"
    )
    require(
      !mrqc.fromToColnames2.exists(dfColumns.contains),
      s"(renameIntervalCols2nd) DataFrame df must not contain any column of ${mrqc.fromToColnames2.mkString(",")}" +
        s" but df.schema = ${df.schema.catalogString}!"
    )
//    df.withColumnRenamed(mrqc.fromColName, mrqc.fromColName2).withColumnRenamed(mrqc.toColName, mrqc.toColName2)
    df.withColumnsRenamed(mrqc.rangeDimensions.flatMap { d =>
      List((d.fromColName, d.fromCol2Name), (d.toColName, d.toCol2Name))
    }.toMap)
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

}
