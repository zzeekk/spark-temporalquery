/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval}
import ch.zzeekk.spark.temporalquery.multivarRange.{
  ClosedMultivarRangeQueryConfig,
  HalfOpenMultivarRangeQueryConfig,
  MultivarRangeQueryConfig
}
import org.slf4j.Logger

import scala.reflect.runtime.universe.TypeTag

/**
 * Generic class to provide BiLinear query utils for different interval axis types
 *
 * @tparam T:
 *   scala type for interval axis
 */
class BiLinearGenericQueryUtil[T: Ordering: TypeTag] extends Serializable with Logging {

  /**
   * Trait to mark BiLinear query configurations to make implicit resolution unique if there is also
   * an implicit temporal query configuration in scope
   */
  trait BiLinearQueryConfigMarker

  /**
   * Type which includes BiLinearClosedIntervalQueryConfig and BiLinearHalfOpenIntervalQueryConfig
   */
  private type BiLinearQueryConfig = MultivarRangeQueryConfig[T, _] with BiLinearQueryConfigMarker

  /**
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class BiLinearClosedIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("h_from" -> "h_to", "v_from" -> "v_to"),
      override val additionalTechnicalColNames: List[String] = Nil,
      override val intervalDef: ClosedInterval[T]
  ) extends ClosedMultivarRangeQueryConfig[T] with BiLinearQueryConfigMarker {
    require(
      numDimensions == 2,
      s"BiLinearClosedIntervalQueryConfig must have exactly 2 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
    override def dimensionMap: Map[String, (String, ClosedInterval[T])] = dimensionColNameMap.map { case (f, t) =>
      (f, (t, intervalDef))
    }
    override lazy val config2: BiLinearClosedIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class BiLinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("h_from" -> "h_to", "v_from" -> "v_to"),
      override val additionalTechnicalColNames: List[String] = Nil,
      override val intervalDef: HalfOpenInterval[T]
  ) extends HalfOpenMultivarRangeQueryConfig[T] with BiLinearQueryConfigMarker {
    require(
      numDimensions == 2,
      s"BiLinearHalfOpenIntervalQueryConfig must have exactly 2 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
    override def dimensionMap: Map[String, (String, HalfOpenInterval[T])] = dimensionColNameMap
      .map { case (f, t) => (f, (t, intervalDef)) }
    override lazy val config2: BiLinearHalfOpenIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }

  object BiLinearHalfOpenIntervalQueryConfig {

    /**
     * Alternative method to create a BiLinearHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(
        horizontalFromColName: String = "h_from",
        horizontalToColName: String = "h_to",
        verticalFromColName: String = "v_from",
        verticalToColName: String = "v_to"
    )(implicit intervalDef: HalfOpenInterval[T], logger: Logger): BiLinearHalfOpenIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) horizontalFromColName = $horizontalFromColName ;" +
        s" horizontalToColName = $horizontalToColName ; verticalFromColName = $verticalFromColName ;" +
        s" verticalToColName = $verticalToColName ; intervalDef = $intervalDef")
      BiLinearHalfOpenIntervalQueryConfig(
        dimensionColNameMap = Map(horizontalFromColName -> horizontalToColName, verticalFromColName -> verticalToColName),
        additionalTechnicalColNames = Nil,
        intervalDef = intervalDef
      )
    }

  }

}
