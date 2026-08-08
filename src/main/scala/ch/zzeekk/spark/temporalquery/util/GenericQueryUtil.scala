/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, HalfOpenInterval}
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, HalfOpenMultivarRangeQueryConfig}
import org.slf4j.Logger

import scala.reflect.runtime.universe.TypeTag

/**
 * Generic class to provide BiLinear query utils for different interval axis types
 *
 * @tparam T:
 *   scala type for interval axis
 */
class GenericQueryUtil[T: Ordering: TypeTag] extends Serializable with Logging {

  def defaultColnameFun(n: Int): String = s"x${"%03d".format(n)}"

  /**
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class GenericClosedIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, ClosedInterval[T])],
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends ClosedMultivarRangeQueryConfig[T] {
    override def config2: GenericClosedIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

  object GenericClosedIntervalQueryConfig {

    /**
     * Alternative method to create a GenericHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(numDim: Int, colNameFun: Int => String = defaultColnameFun)(implicit
        intervalDef: ClosedInterval[T],
        logger: Logger
    ): GenericClosedIntervalQueryConfig = {
      logger.info(s"(withDefaultIntervalDef) numDim = $numDim ; intervalDef = $intervalDef")
      val iter = new scala.collection.immutable.NumericRange.Exclusive(start = 0, end = numDim, step = 1).toSet
      GenericClosedIntervalQueryConfig(
        dimensionMap = iter.map(k => (s"${colNameFun(k)}_from", (s"${colNameFun(k)}_from", intervalDef))).toMap,
        additionalTechnicalColNames = Nil
      )
    }
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class GenericHalfOpenIntervalQueryConfig(
      override val dimensionMap: Map[String, (String, HalfOpenInterval[T])],
      override val additionalTechnicalColNames: List[String] = Nil
  ) extends HalfOpenMultivarRangeQueryConfig[T] {
    override def config2: GenericHalfOpenIntervalQueryConfig = this
      .copy(dimensionMap = dimensionMap.map { case (f, (t, i)) => (increaseColNameNb(f), (increaseColNameNb(t), i)) })
  }

  object GenericHalfOpenIntervalQueryConfig {

    /**
     * Alternative method to create a GenericHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(numDim: Int, colNameFun: Int => String = defaultColnameFun)(implicit
        intervalDef: HalfOpenInterval[T],
        logger: Logger
    ): GenericHalfOpenIntervalQueryConfig = {
      logger.info(s"(withDefaultIntervalDef) numDim = $numDim ; intervalDef = $intervalDef")
      val iter = new scala.collection.immutable.NumericRange.Exclusive(start = 0, end = numDim, step = 1).toSet
      GenericHalfOpenIntervalQueryConfig(
        dimensionMap = iter.map(k => (s"${colNameFun(k)}_from", (s"${colNameFun(k)}_to", intervalDef))).toMap,
        additionalTechnicalColNames = Nil
      )
    }
  }

}
