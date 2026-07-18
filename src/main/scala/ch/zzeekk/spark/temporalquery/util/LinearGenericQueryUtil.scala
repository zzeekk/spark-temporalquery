/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.Logger

import scala.reflect.runtime.universe._

/**
 * Generic class to provide linear query utils for different interval axis types
 * @tparam T:
 *   scala type for interval axis
 */
class LinearGenericQueryUtil[T: Ordering: TypeTag] extends Serializable with Logging {

  /**
   * Trait to mark linear query configurations to make implicit resolution unique if there is also
   * an implicit temporal query configuration in scope
   */
  trait LinearQueryConfigMarker

  /**
   * Type which includes LinearClosedIntervalQueryConfig and LinearHalfOpenIntervalQueryConfig
   */
  private type LinearQueryConfig = IntervalMultidimQueryConfig[T, _] with LinearQueryConfigMarker

  /**
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearClosedIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_from" -> "position_to"),
      override val additionalTechnicalColNames: Seq[String] = Nil,
      override val intervalDef: ClosedInterval[T]
  ) extends ClosedIntervalMultidimQueryConfig[T] with LinearQueryConfigMarker {
    override def dimensionMap: Map[String, (String, ClosedInterval[T])] = dimensionColNameMap.map { case (f, t) =>
      (f, (t, intervalDef))
    }
    override lazy val config2: LinearClosedIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_from" -> "position_to"),
      override val additionalTechnicalColNames: Seq[String] = Nil,
      override val intervalDef: HalfOpenInterval[T]
  ) extends HalfOpenIntervalMultidimQueryConfig[T] with LinearQueryConfigMarker {
    override def dimensionMap: Map[String, (String, HalfOpenInterval[T])] = dimensionColNameMap
      .map { case (f, t) => (f, (t, intervalDef)) }
    override lazy val config2: LinearHalfOpenIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }
  object LinearHalfOpenIntervalQueryConfig {

    /**
     * Alternative method to create a LinearHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(
        fromColName: String = "position_from",
        toColName: String = "position_to"
    )(implicit intervalDef: HalfOpenInterval[T], logger: Logger): LinearHalfOpenIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
      LinearHalfOpenIntervalQueryConfig(dimensionColNameMap = Map(fromColName -> toColName),
        additionalTechnicalColNames = Nil, intervalDef = intervalDef)
    }

  }

  /**
   * Pimp-my-library pattern for DataFrame
   */
  implicit class LinearDataFrameExtensions(df1: DataFrame) {

    /**
     * Implements an inner join of linear data over a list of equally named columns
     */
    def linearInnerJoin(df2: DataFrame, keys: Seq[String])(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.joinIntervalsWithKeysImpl(df1, df2, keys)

    /**
     * Implements an inner join of linear data over an explicit join condition
     */
    def linearInnerJoin(df2: DataFrame, keyCondition: Column)(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.joinIntervals(df1, df2, keys = Nil, joinType = "inner", keyCondition)

    /**
     * Implements a full outer join of linear data over a list of equally named columns
     * @param rnkExpressions:
     *   In case df1 or df2 does not have a linear 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per value. This corresponds to a
     *   join with the constraint that no multiplication of records in the other DataFrame can
     *   occur. If df1 or df2 is to be joined as a one-to-many relation (allowing multiplication of
     *   records from df1/df2), set rnkExpressions = Nil to disable this deduplication.
     * @param additionalJoinFilterCondition:
     *   additional non-equi join conditions for the join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to both input
     *   DataFrames (default = true)
     */
    def linearFullJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

    /**
     * Implements a left outer join of linear data over a list of equally named columns
     * @param rnkExpressions:
     *   In case df2 does not have a linear 1-1-mapping, i.e. keys :+ fromColName are not unique,
     *   rnkExpressions is used to select exactly one row per value. This corresponds to a join with
     *   the constraint that no multiplication of records in df1 can occur. If df2 is to be joined
     *   as a one-to-many relation (allowing multiplication of records from df1), set rnkExpressions =
     *   Nil to disable this deduplication.
     * @param additionalJoinFilterCondition:
     *   additional non-equi join conditions for the left join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfRight (default = true)
     */
    def linearLeftJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left", doCleanupExtend)

    /**
     * Implements a right outer join of linear data over a list of equally named columns
     * @param rnkExpressions:
     *   In case df1 or df2 does not have a linear 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per value. This corresponds to a
     *   join with the constraint that no multiplication of records in the other DataFrame can
     *   occur. If df1 or df2 is to be joined as a one-to-many relation (allowing multiplication of
     *   records from df1/df2), set rnkExpressions = Nil to disable this deduplication.
     * @param additionalJoinFilterCondition:
     *   additional non-equi join conditions for the right join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfLeft (default = true)
     */
    def linearRightJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implements a left anti join of linear data over a list of equally named columns
     * @param additionalJoinFilterCondition:
     *   additional non-equi join conditions for the left anti join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def linearLeftAntiJoin(
        df2: DataFrame,
        joinColumns: Seq[String],
        additionalJoinFilterCondition: Column = lit(true)
        // TODO: Why we require closed interval? Why not LinearQueryConfig
    )(implicit lqc: LinearClosedIntervalQueryConfig, logger: Logger): DataFrame = {
      assert(lqc.intervalDef.isInstanceOf[ClosedInterval[_]],
        "Only ClosedInterval interval definition in LinearQueryConfig supported for linearLeftAntiJoin()")
      IntervalQueryImpl.leftAntiJoinIntervals(df1, df2, joinColumns, additionalJoinFilterCondition)
    }

    /**
     * Resolves linear overlaps
     * @param rnkExpressions:
     *   priority expressions for deduplication
     * @param aggExpressions:
     *   aggregations to compute during deduplication
     * @param rnkFilter:
     *   if false, overlapping sections are only marked with rnk>1 but not filtered out
     * @param extend:
     *   if true and fillGapsWithNull=true, rows with null values are added for each key so that the
     *   entire linear axis [lowerHorizon , upperHorizon] is covered for all keys
     * @param fillGapsWithNull:
     *   if true, gaps in the linear axis are filled with null rows. fillGapsWithNull must be set to
     *   true for extend=true to have any effect
     */
    def linearCleanupExtend(
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Nil,
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.cleanupExtendIntervals(df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull)

    /**
     * Combines consecutive records when there is no change in the non-technical columns.
     */
    def linearCombine(keys: Seq[String] = Nil, ignoreColNames: Seq[String] = Nil)(implicit
        lqc: LinearQueryConfig,
        logger: Logger
    ): DataFrame = {
      if (keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      IntervalQueryImpl
        .combineIntervals(df1.where(lqc.isValidIntervalExpr), ignoreColNames)
    }

    /**
     * Cuts records into pieces at overlaps, so that at the start of each overlap all active records
     * are split
     */
    def linearUnifyRanges(keys: Seq[String])(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.unifyIntervalRanges(df1, keys)

    /**
     * Extends the range of the smallest valid_from per key to minDate
     */
    def linearExtendRange(keys: Seq[String] = Nil, extendMin: Boolean = true, extendMax: Boolean = true)(implicit
        lqc: LinearQueryConfig
    ): DataFrame =
      IntervalQueryImpl.extendIntervalRanges(df1, keys, extendMin, extendMax)

    /**
     * Round closed intervals according to ClosedInterval discrete axis definition. Sets the
     * discreteness of the linear axis to the value defined in ClosedInterval.discreteAxisDef.
     * Hereby the intervals may be shortened on the lower bound and extended on the upper bound. To
     * the lower bound ceiling is applied whereas to the upper bound flooring. If the dataframe has
     * a discreteness of millisecond or coarser, then the only two changes are: If a timestamp lies
     * outside of [lowerHorizon, upperHorizon] it will be replaced by lowerHorizon, upperHorizon
     * respectively. Rows for which the validity ends before it starts, i.e. with
     * toCol.before(fromCol), are removed.
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval
     * definitions can only be created for axis with Integral-Numeric type, and not
     * Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearRoundClosedIntervals(implicit lqc: LinearClosedIntervalQueryConfig): DataFrame =
      IntervalQueryImpl.roundIntervalsToDiscreteTime(df1)

    /**
     * Transforms [[DataFrame]] with half open time intervals "[fromColName , toColName [" to closed
     * intervals "[fromColName , toColName]"
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval
     * definitions can only be created for axis with Integral-Numeric type, and not
     * Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearConvertToClosedIntervals(implicit lqc: LinearClosedIntervalQueryConfig): DataFrame =
      IntervalQueryImpl.transformHalfOpenToClosedIntervals(df1)

  }

  /**
   * Pimp-my-library pattern for Columns
   */
  implicit class LinearColumnExtensions(value: Column) {
    def isInTemporalInterval(implicit lqc: LinearQueryConfig): Column = lqc.isInIntervalExpr(List(value))
  }

}
