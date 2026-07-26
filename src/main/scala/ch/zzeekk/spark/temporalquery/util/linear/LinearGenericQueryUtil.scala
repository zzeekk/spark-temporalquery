/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval}
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, HalfOpenMultivarRangeQueryConfig}
import org.slf4j.Logger

import scala.reflect.runtime.universe.TypeTag

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
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearClosedIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_from" -> "position_to"),
      override val additionalTechnicalColNames: List[String] = Nil,
      override val intervalDef: ClosedInterval[T]
  ) extends ClosedMultivarRangeQueryConfig[T] with LinearQueryConfigMarker {
    require(
      numDimensions == 1,
      s"LinearClosedIntervalQueryConfig must have exactly 1 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
    override def dimensionMap: Map[String, (String, ClosedInterval[T])] = dimensionColNameMap.map { case (f, t) =>
      (f, (t, intervalDef))
    }
    override lazy val config2: LinearClosedIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }

  object LinearClosedIntervalQueryConfig {

    /**
     * Alternative method to create a LinearHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(
        fromColName: String = "position_from",
        toColName: String = "position_to"
    )(implicit intervalDef: ClosedInterval[T], logger: Logger): LinearClosedIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
      LinearClosedIntervalQueryConfig(dimensionColNameMap = Map(fromColName -> toColName),
        additionalTechnicalColNames = Nil, intervalDef = intervalDef)
    }

  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_from" -> "position_to"),
      override val additionalTechnicalColNames: List[String] = Nil,
      override val intervalDef: HalfOpenInterval[T]
  ) extends HalfOpenMultivarRangeQueryConfig[T] with LinearQueryConfigMarker {
    require(
      numDimensions == 1,
      s"LinearHalfOpenIntervalQueryConfig must have exactly 1 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
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

}
