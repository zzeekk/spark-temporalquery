/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval, IntervalQueryDimension}
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, HalfOpenMultivarRangeQueryConfig}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.slf4j.Logger
import ch.zzeekk.spark.temporalquery.util.defaultDimLt

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
      override val dimLt: (IntervalQueryDimension[T, ClosedInterval[T]], IntervalQueryDimension[T, ClosedInterval[T]]) => Boolean =
        defaultDimLt,
      intervalDef: ClosedInterval[T]
  ) extends ClosedMultivarRangeQueryConfig[T] with LinearQueryConfigMarker {
    require(
      numDimensions == 1,
      s"LinearClosedIntervalQueryConfig must have exactly 1 dimension but numDimensions = $numDimensions!" +
        s" You may want to use MultivarRangeQueryConfig directly!"
    )
    override def dimensionMap: Map[String, (String, ClosedInterval[T])] = dimensionColNameMap.map { case (f, t) =>
      (f, (t, intervalDef))
    }
    override def config2: LinearClosedIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })

    def fromColName: String = rangeDimensions.head.fromColName
    def toColName: String = rangeDimensions.head.toColName
    def fromCol: Column = col(fromColName)
    def toCol: Column = col(toColName)
    def fromColName2: String = rangeDimensions.head.fromCol2Name
    def toColName2: String = rangeDimensions.head.toCol2Name
    def fromCol2: Column = col(fromColName2)
    def toCol2: Column = col(toColName2)
    def lowerHorizon: T = rangeDimensions.head.lowerHorizon
    def upperHorizon: T = rangeDimensions.head.upperHorizon

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
      logger.info(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
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
      override val dimLt: (IntervalQueryDimension[T, HalfOpenInterval[T]], IntervalQueryDimension[T, HalfOpenInterval[T]]) => Boolean =
        defaultDimLt,
      intervalDef: HalfOpenInterval[T]
  ) extends HalfOpenMultivarRangeQueryConfig[T] with LinearQueryConfigMarker {
    require(
      numDimensions == 1,
      s"LinearHalfOpenIntervalQueryConfig must have exactly 1 dimension but numDimensions = $numDimensions!" +
        s" You may want to use MultivarRangeQueryConfig directly!"
    )
    override def dimensionMap: Map[String, (String, HalfOpenInterval[T])] = dimensionColNameMap
      .map { case (f, t) => (f, (t, intervalDef)) }
    override def config2: LinearHalfOpenIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })

    def fromColName: String = rangeDimensions.head.fromColName
    def toColName: String = rangeDimensions.head.toColName
    def fromCol: Column = col(fromColName)
    def toCol: Column = col(toColName)
    def fromColName2: String = rangeDimensions.head.fromCol2Name
    def toColName2: String = rangeDimensions.head.toCol2Name
    def fromCol2: Column = col(fromColName2)
    def toCol2: Column = col(toColName2)
    def lowerHorizon: T = rangeDimensions.head.lowerHorizon
    def upperHorizon: T = rangeDimensions.head.upperHorizon

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
      logger.info(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
      LinearHalfOpenIntervalQueryConfig(dimensionColNameMap = Map(fromColName -> toColName),
        additionalTechnicalColNames = Nil, intervalDef = intervalDef)
    }

  }

}
