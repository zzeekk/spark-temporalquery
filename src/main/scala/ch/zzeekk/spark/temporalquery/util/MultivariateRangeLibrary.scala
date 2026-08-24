package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.Logging
import ch.zzeekk.spark.temporalquery.interval.IntervalDef
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, MultivarRangeQueryConfig, MultivarRangeQueryImpl}
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.reflect.runtime.universe.TypeTag
import scala.util.{Failure, Success, Try}

object MultivariateRangeLibrary extends Logging {

  /**
   * Pimp-my-library pattern for Columns
   */
  implicit class MultivarRangeColumnExtensions(value: Column) {
    def isInRange[T: Ordering: TypeTag](implicit mrqc: MultivarRangeQueryConfig[T, _]): Column =
      mrqc.isInRangeExpr(List(value))
  }

  /**
   * Pimp-my-library pattern for DataFrame
   */
  implicit class MultivariateRangeFrameExtensions(df1: DataFrame) {

    def getDiagonal[T: Ordering: TypeTag](
        fromColName: String = "_from",
        toColName: String = "_to"
    )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]): DataFrame =
      MultivarRangeQueryImpl.getDiagonal(df1, mrqc, fromColName, toColName)

    def getValues[T: Ordering: TypeTag](coords: Seq[T])(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]): DataFrame =
      MultivarRangeQueryImpl.getValues(df1, coords, mrqc)

    /**
     * Implements an inner join of historical data over a list of equally named columns
     */
    def rangeInnerJoin[T: Ordering: TypeTag](df2: DataFrame, keys: Seq[String])(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.joinIntervalsWithKeysImpl(df1 = df1, df2 = df2, keys = keys, mrqc = mrqc)

    /**
     * Implements an inner join of historical data over an explicit join condition
     */
    def rangeInnerJoin[T: Ordering: TypeTag](df2: DataFrame, keyCondition: Column)(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl
      .joinRanges(df1 = df1, df2 = df2, keys = Nil, mrqc = mrqc, additionalJoinCondition = keyCondition)

    /**
     * Implements a full outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df1 or df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in the other
     *   frame can occur. If df1 or df2 is to be joined as a one-to-many relation (allowing
     *   multiplication of records from df1/df2), set rnkExpressions = Nil to disable this
     *   deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the full join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to both input
     *   DataFrames (default = true)
     */
    def rangeFullJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinRangesWithKey(df1, df2, keys, mrqc, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

    /**
     * Implements a left outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in df1 can
     *   occur. If df2 is to be joined as a one-to-many relation (allowing multiplication of records
     *   from df1), set rnkExpressions = Nil to disable this deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the left join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfRight (default = true)
     */
    def rangeLeftJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]], logger: Logger): DataFrame = Try(
      MultivarRangeQueryImpl.outerJoinRangesWithKey(
        df1 = df1,
        df2 = df2,
        keys = keys,
        mrqc = mrqc,
        rnkExpressions = rnkExpressions,
        additionalJoinFilterCondition = additionalJoinFilterCondition,
        joinType = "left",
        doCleanupExtend = doCleanupExtend
      )
    ) match {
      case Success(df) => df
      case Failure(e)  =>
        logger.error(s"(rangeLeftJoin) Could not join the data frames!")
        logger.error(s"(rangeLeftJoin) df1.schema                    = ${df1.schema.catalogString}")
        logger.error(s"(rangeLeftJoin) df2.schema                    = ${df2.schema.catalogString}")
        logger.error(s"(rangeLeftJoin) keys                          = $keys")
        logger.error(s"(rangeLeftJoin) mrqc                          = $mrqc")
        logger.error(s"(rangeLeftJoin) rnkExpressions                = $rnkExpressions")
        logger.error(s"(rangeLeftJoin) additionalJoinFilterCondition = $additionalJoinFilterCondition")
        logger.error(s"(rangeLeftJoin) doCleanupExtend               = $doCleanupExtend")
        throw e
    }

    /**
     * Implements a right outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df1 or df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in the other
     *   frame can occur. If df1 or df2 is to be joined as a one-to-many relation (allowing
     *   multiplication of records from df1/df2), set rnkExpressions = Nil to disable this
     *   deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the right join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfLeft (default = true)
     */
    def rangeRightJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinRangesWithKey(df1, df2, keys, mrqc, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implements a left anti join of historical data over a list of equally named columns
     *
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the left anti join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def rangeLeftAntiJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        joinColumns: Seq[String],
        additionalJoinFilterCondition: Column = lit(true)
    )(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame =
      MultivarRangeQueryImpl.leftAntiJoinRanges(df1 = df1, df2 = df2, keys = joinColumns, mrqc = mrqc,
        additionalJoinFilterCondition = additionalJoinFilterCondition)

    /**
     * Resolves temporal overlaps
     *
     * @param rnkExpressions
     *   : priority expressions for deduplication
     * @param aggExpressions
     *   : aggregations to compute during deduplication
     * @param rnkFilter
     *   : if false, overlapping sections are only marked with rnk>1 but not filtered out
     * @param extend
     *   : if true and fillGapsWithNull=true, rows with null values are added for each key so that
     *   the entire time axis [minDate , maxDate] is covered for all keys
     * @param fillGapsWithNull
     *   : if true, gaps in the history are filled with null rows. fillGapsWithNull must be set to
     *   true for extend=true to have any effect
     */
    def rangeCleanupExtend[T: Ordering: TypeTag](
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Nil,
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .cleanupExtendRanges(df1, keys, mrqc, rnkFilter, rnkExpressions, aggExpressions, extend, fillGapsWithNull)

    /**
     * Combines consecutive records when there is no change in the non-technical columns. The
     * dataframe is first cleaned up via [[rangeRoundDiscreteTime]], see its description.
     *
     * @param ignoreColNames
     *   columns to be ignored
     * @param mrqc
     *   multi range query configuration
     * @param logger
     *   to write beautiful messages
     * @tparam T
     *   type of your axes
     * @return
     *   compacted data frame
     */
    def rangeCombine[T: Ordering: TypeTag](ignoreColNames: Seq[String] = Nil)(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.combineMultivarRanges(
      df = df1,
      mrqc = mrqc,
      ignoreColNames = ignoreColNames
    )

    /**
     * Cuts records into pieces at overlaps, so that at the start of each overlap all active records
     * are split
     */
    def rangeUnifyRanges[T: Ordering: TypeTag](
        keys: Seq[String] = Nil,
        extend: Boolean = false,
        fillGapsWithNull: Boolean = false
    )(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.unifyMultivarRanges(df1, mrqc, keys, extend, fillGapsWithNull)

    /**
     * Extends the history of the smallest value per key to minDate
     */
    def rangeExtendRange[T: Ordering: TypeTag](
        keys: Seq[String] = Nil,
        extendMin: Boolean = true,
        extendMax: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .extendMultivarRanges(df1, keys, extendMin, extendMax)

    /**
     * Sets the discreteness of the time scale to milliseconds. Hereby the validity intervals may be
     * shortened on the lower bound and extended on the upper bound. To the lower bound ceiling is
     * applied whereas to the upper bound flooring. If the dataframe has a discreteness of
     * millisecond or coarser, then the only two changes are: If a timestamp lies outside of
     * [minDate , maxDate] it will be replaced by minDate, maxDate respectively. Rows for which the
     * validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   temporal dataframe with a discreteness of milliseconds
     */
    def rangeRoundDiscreteTime[T: Ordering: TypeTag](implicit clmrqc: ClosedMultivarRangeQueryConfig[T]): DataFrame =
      MultivarRangeQueryImpl.roundIntervalsToDiscreteTime(df1, clmrqc)

    /**
     * Transforms [[DataFrame]] with dense time, half open time intervals [fromColName , toColName[
     * to discrete time ([fromColName , toColName])
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   [[DataFrame]] with discrete time axis
     */
    def rangeDense2discrete[T: Ordering: TypeTag](implicit clmrqc: ClosedMultivarRangeQueryConfig[T]): DataFrame =
      MultivarRangeQueryImpl.transformHalfOpenToClosedIntervals(df1, clmrqc)

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
     * For [[interval.ClosedInterval]] dimensions the rendered rectangle extends to `successor(to)`
     * so that the last discrete step is fully covered visually; for [[interval.HalfOpenInterval]]
     * the `to` value is used directly. Rectangles are outlined only for closed intervals.
     *
     * Both axes share the same scale so that the aspect ratio of the data space is preserved; the
     * longer axis fills up to 1024 px.
     *
     * @param valueCol
     *   name of the column whose value determines the rectangle fill colour
     * @param drawDiagonal
     *   if true, draw a black line for the diagonal of the dimension space, i.e. the line
     *   containing all points where the two dimensions' coordinates coincide. Only the part of that
     *   line lying inside the viewbox is drawn; if the diagonal lies completely outside the
     *   viewbox, nothing is drawn.
     */
    def toSvg[T: Ordering: TypeTag](valueCol: String, svgMax: Double = 1024d, drawDiagonal: Boolean = false)(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]
    ): String = MultivarRangeQueryImpl.toSvg(df1, valueCol, svgMax, drawDiagonal, mrqc)

  }

}
