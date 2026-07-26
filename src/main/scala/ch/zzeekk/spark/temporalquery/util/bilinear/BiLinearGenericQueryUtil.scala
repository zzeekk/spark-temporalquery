/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util.bilinear

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
      dimensionColNameMap: Map[String, String] = Map("x_from" -> "x_to", "y_from" -> "y_to"),
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

  object BiLinearClosedIntervalQueryConfig {

    /**
     * Alternative method to create a BiLinearHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(
        fstFromColName: String = "x_from",
        fstToColName: String = "x_to",
        sndFromColName: String = "y_from",
        sndToColName: String = "y_to"
    )(implicit intervalDef: ClosedInterval[T], logger: Logger): BiLinearClosedIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fstFromColName = $fstFromColName ;" +
        s" fstToColName = $fstToColName ; sndFromColName = $sndFromColName ;" +
        s" sndToColName = $sndToColName ; intervalDef = $intervalDef")
      BiLinearClosedIntervalQueryConfig(
        dimensionColNameMap = Map(fstFromColName -> fstToColName, sndFromColName -> sndToColName),
        additionalTechnicalColNames = Nil,
        intervalDef = intervalDef
      )
    }
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class BiLinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("x_from" -> "x_to", "y_from" -> "y_to"),
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
        fstFromColName: String = "x_from",
        fstToColName: String = "x_to",
        sndFromColName: String = "y_from",
        sndToColName: String = "y_to"
    )(implicit intervalDef: HalfOpenInterval[T], logger: Logger): BiLinearHalfOpenIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fstFromColName = $fstFromColName ;" +
        s" fstToColName = $fstToColName ; sndFromColName = $sndFromColName ;" +
        s" sndToColName = $sndToColName ; intervalDef = $intervalDef")
      BiLinearHalfOpenIntervalQueryConfig(
        dimensionColNameMap = Map(fstFromColName -> fstToColName, sndFromColName -> sndToColName),
        additionalTechnicalColNames = Nil,
        intervalDef = intervalDef
      )
    }
  }

}
