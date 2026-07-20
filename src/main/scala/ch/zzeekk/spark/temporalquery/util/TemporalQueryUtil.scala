/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval}
import org.slf4j.Logger

import java.sql.Timestamp

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
  type TemporalQueryConfig = MultivarRangeQueryConfig[Timestamp, _] with TemporalQueryConfigMarker

  private implicit val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a, b) => a.before(b))

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class TemporalClosedIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, ClosedInterval[Timestamp])] =
        Map("valid_from" -> ("valid_to", stdClosedTemporalInterval)),
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends ClosedMultivarRangeQueryConfig[Timestamp] with TemporalQueryConfigMarker {
    override def config2: TemporalClosedIntervalQueryConfig = this
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
      override val dimensionMap: Map[String, (String, HalfOpenInterval[Timestamp])] =
        Map("valid_from" -> ("valid_to", HalfOpenInterval(bigBangDay, doomsDay))),
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends HalfOpenMultivarRangeQueryConfig[Timestamp] with TemporalQueryConfigMarker {
    override def config2: TemporalHalfOpenIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

}
