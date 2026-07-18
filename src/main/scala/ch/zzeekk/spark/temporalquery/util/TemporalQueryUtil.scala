/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.Logger

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

/**
 * Temporal query utils for interval axis of type Timestamp
 *
 * Usage: import ch.zzeekk.spark.temporalquery.TemporalQueryUtil._ // this imports temporal*
 * implicit functions on DataFrame and Columns implicit val tqc = TemporalQueryConfig() // configure
 * options for temporal query operations implicit val sss = ss // make SparkSession implicitly
 * available val df_joined = df1.temporalJoin(df2) // use temporal query functions with Spark
 */
object TemporalQueryUtil extends Serializable with Logging {

  /**
   * Trait to mark temporal query configurations to make implicit resolution unique if there is also
   * an implicit linear query configuration in scope
   */
  trait TemporalQueryConfigMarker

  /**
   * Type which includes TemporalClosedIntervalQueryConfig and TemporalHalfOpenIntervalQueryConfig
   */
  type TemporalQueryConfig = IntervalMultidimQueryConfig[Timestamp, _] with TemporalQueryConfigMarker

  implicit private val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a, b) => a.before(b))

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class TemporalClosedIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, ClosedInterval[Timestamp])] = Map(
        "valid_from" ->
          ("valid_to",
            ClosedInterval(
              bigBangDay,
              doomsDay,
              DiscreteTimeAxis(ChronoUnit.MILLIS)
            ))
      ),
      override val additionalTechnicalColNames: Seq[String] = Nil
  ) extends ClosedIntervalMultidimQueryConfig[Timestamp] with TemporalQueryConfigMarker {
    override lazy val config2: TemporalClosedIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

  object TemporalClosedIntervalQueryConfig {
    def withDefaultIntervalDef(fromColName: String = "valid_from", toColName: String = "valid_to")(implicit
        intervalDef: ClosedInterval[Timestamp],
        logger: Logger
    ): TemporalClosedIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
      TemporalClosedIntervalQueryConfig(
        dimensionMap = Map(fromColName -> (toColName, intervalDef))
      )
    }
  }

  /**
   * Configuration Parameters for operations on temporal interval axis. An instance of this class is
   * needed as implicit parameter for all temporal query functions.
   */
  case class TemporalHalfOpenIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, HalfOpenInterval[Timestamp])] = Map(
        "valid_from" ->
          ("valid_to",
            HalfOpenInterval(bigBangDay, doomsDay))
      ),
      override val additionalTechnicalColNames: Seq[String] = Nil
  ) extends HalfOpenIntervalMultidimQueryConfig[Timestamp] with TemporalQueryConfigMarker {
    override lazy val config2: TemporalHalfOpenIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

  /**
   * Pimp-my-library pattern for DataFrame
   */
  implicit class TemporalDataFrameExtensions(df1: DataFrame) {

    /**
     * Implements an inner join of historical data over a list of equally named columns
     */
    def temporalInnerJoin(df2: DataFrame, keys: Seq[String])(implicit
        tc: TemporalQueryConfig,
        logger: Logger
    ): DataFrame =
      IntervalQueryImpl
        .joinIntervalsWithKeysImpl(df1, df2, keys)

    /**
     * Implements an inner join of historical data over an explicit join condition
     */
    def temporalInnerJoin(df2: DataFrame, keyCondition: Column)(implicit
        tc: TemporalQueryConfig,
        logger: Logger
    ): DataFrame =
      IntervalQueryImpl
        .joinIntervals(df1, df2, keys = Nil, joinType = "inner", keyCondition)

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
    def temporalFullJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit ss: SparkSession, tc: TemporalQueryConfig, logger: Logger): DataFrame = IntervalQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

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
    def temporalLeftJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit ss: SparkSession, tc: TemporalQueryConfig, logger: Logger): DataFrame = IntervalQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions,
        additionalJoinFilterCondition, "left", doCleanupExtend)

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
    def temporalRightJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit tc: TemporalQueryConfig, logger: Logger): DataFrame = IntervalQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implements a left anti join of historical data over a list of equally named columns
     *
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the left anti join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def temporalLeftAntiJoin(df2: DataFrame, joinColumns: Seq[String], additionalJoinFilterCondition: Column = lit(true))(implicit
        tc: IntervalMultidimQueryConfig[Timestamp, ClosedInterval[Timestamp]],
        logger: Logger
    ): DataFrame =
      IntervalQueryImpl.leftAntiJoinIntervals(df1, df2, joinColumns, additionalJoinFilterCondition)

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
    def temporalCleanupExtend(
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Nil,
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit tc: TemporalQueryConfig, logger: Logger): DataFrame = IntervalQueryImpl
      .cleanupExtendIntervals(df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull)

    /**
     * Combines consecutive records when there is no change in the non-technical columns. The
     * dataframe is first cleaned up via [[temporalRoundDiscreteTime]], see its description.
     */
    def temporalCombine(ignoreColNames: Seq[String] = Nil)(implicit tc: TemporalQueryConfig): DataFrame =
      IntervalQueryImpl
        .combineIntervals(df1, ignoreColNames)

    /**
     * Cuts records into pieces at overlaps, so that at the start of each overlap all active records
     * are split
     */
    def temporalUnifyRanges(keys: Seq[String])(implicit tc: TemporalQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.unifyIntervalRanges(df1, keys)

    /**
     * Extends the history of the smallest value per key to minDate
     */
    def temporalExtendRange(keys: Seq[String] = Nil, extendMin: Boolean = true, extendMax: Boolean = true)(implicit
        tc: TemporalQueryConfig
    ): DataFrame = IntervalQueryImpl
      .extendIntervalRanges(df1, keys, extendMin, extendMax)

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
    def temporalRoundDiscreteTime(implicit tc: TemporalClosedIntervalQueryConfig): DataFrame = IntervalQueryImpl
      .roundIntervalsToDiscreteTime(df1)

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName ,
     * toColName [, to discrete time ([fromColName , toColName])
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   [[DataFrame]] with discrete time axis
     */
    def temporalContinuous2discrete(implicit tc: TemporalClosedIntervalQueryConfig): DataFrame = IntervalQueryImpl
      .transformHalfOpenToClosedIntervals(df1)

  }

  /**
   * Pimp-my-library pattern for Columns
   */
  implicit class TemporalColumnExtensions(value: Column) {
    def isInTemporalInterval(implicit tc: TemporalQueryConfig): Column = tc.isInIntervalExpr(List(value))
  }

}
