/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval, IntervalQueryDimension}
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, HalfOpenMultivarRangeQueryConfig}
import ch.zzeekk.spark.temporalquery.util.defaultDimLt
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
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class BiLinearClosedIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, ClosedInterval[T])],
      override val additionalTechnicalColNames: List[String] = Nil,
      override val dimLt: (IntervalQueryDimension[T, ClosedInterval[T]], IntervalQueryDimension[T, ClosedInterval[T]]) => Boolean =
        defaultDimLt
  ) extends ClosedMultivarRangeQueryConfig[T] with BiLinearQueryConfigMarker {
    require(
      numDimensions == 2,
      s"BiLinearClosedIntervalQueryConfig must have exactly 2 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
    override def config2: BiLinearClosedIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
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
      logger.info(s"(withDefaultIntervalDef) fstFromColName = $fstFromColName ;" +
        s" fstToColName = $fstToColName ; sndFromColName = $sndFromColName ;" +
        s" sndToColName = $sndToColName ; intervalDef = $intervalDef")
      BiLinearClosedIntervalQueryConfig(
        dimensionMap = Map(fstFromColName -> (fstToColName, intervalDef), sndFromColName -> (sndToColName, intervalDef)),
        additionalTechnicalColNames = Nil
      )
    }
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class BiLinearHalfOpenIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, HalfOpenInterval[T])],
      override val additionalTechnicalColNames: List[String] = Nil,
      override val dimLt: (IntervalQueryDimension[T, HalfOpenInterval[T]], IntervalQueryDimension[T, HalfOpenInterval[T]]) => Boolean =
        defaultDimLt
  ) extends HalfOpenMultivarRangeQueryConfig[T] with BiLinearQueryConfigMarker {
    require(
      numDimensions == 2,
      s"BiLinearHalfOpenIntervalQueryConfig must have exactly 2 dimension but numDimensions = $numDimensions!" +
        s"You may want to use MultivarRangeQueryConfig directly!"
    )
    override def config2: BiLinearHalfOpenIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
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
      logger.info(s"(withDefaultIntervalDef) fstFromColName = $fstFromColName ;" +
        s" fstToColName = $fstToColName ; sndFromColName = $sndFromColName ;" +
        s" sndToColName = $sndToColName ; intervalDef = $intervalDef")
      BiLinearHalfOpenIntervalQueryConfig(
        dimensionMap = Map(fstFromColName -> (fstToColName, intervalDef), sndFromColName -> (sndToColName, intervalDef)),
        additionalTechnicalColNames = Nil
      )
    }
  }

}
