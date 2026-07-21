/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util
import ch.zzeekk.spark.temporalquery.multivarRange.{
  ClosedMultivarRangeQueryConfig,
  HalfOpenMultivarRangeQueryConfig,
  MultivarRangeQueryConfig
}
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
object BiTemporalQueryUtil extends Serializable with Logging {

  /**
   * Trait to mark bitemporal query configurations to make implicit resolution unique if there is
   * also an implicit linear query configuration in scope
   */
  trait BiTemporalQueryConfigMarker

  /**
   * Type which includes BiTemporalClosedIntervalQueryConfig and
   * BiTemporalHalfOpenIntervalQueryConfig
   */
  type BiTemporalQueryConfig = MultivarRangeQueryConfig[Timestamp, _] with BiTemporalQueryConfigMarker

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class BiTemporalClosedIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, ClosedInterval[Timestamp])] =
        Map("known_from" -> ("known_to", stdClosedTemporalInterval),
          "valid_from"   -> ("valid_to", stdClosedTemporalInterval)),
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends ClosedMultivarRangeQueryConfig[Timestamp] with BiTemporalQueryConfigMarker {
    override def config2: BiTemporalClosedIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

  object BiTemporalClosedIntervalQueryConfig {
    def withDefaultIntervalDef(
        knownFromColName: String = "known_from",
        knownToColName: String = "known_to",
        validFromColName: String = "valid_from",
        validToColName: String = "valid_to"
    )(implicit
        intervalDef: ClosedInterval[Timestamp],
        logger: Logger
    ): BiTemporalClosedIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) knownFromColName = $knownFromColName ; knownToColName = $knownToColName ;" +
        s" validFromColName = $validFromColName ; validToColName = $validToColName ; intervalDef = $intervalDef")
      BiTemporalClosedIntervalQueryConfig(
        dimensionMap = Map(knownFromColName -> (knownToColName, intervalDef), validFromColName -> (validToColName, intervalDef))
      )
    }
  }

  /**
   * Configuration Parameters for operations on temporal interval axis. An instance of this class is
   * needed as implicit parameter for all temporal query functions.
   */
  case class BiTemporalHalfOpenIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, HalfOpenInterval[Timestamp])] =
        Map("known_from" -> ("known_to", HalfOpenInterval(bigBangDay, doomsDay)),
          "valid_from"   -> ("valid_to", HalfOpenInterval(bigBangDay, doomsDay))),
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends HalfOpenMultivarRangeQueryConfig[Timestamp] with BiTemporalQueryConfigMarker {
    override def config2: BiTemporalHalfOpenIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }
}
