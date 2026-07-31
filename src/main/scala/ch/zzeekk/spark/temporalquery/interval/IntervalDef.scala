package ch.zzeekk.spark.temporalquery.interval

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{functions, Column}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/**
 * Trait to describe interval behaviour
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class IntervalDef[T: Ordering: TypeTag] extends Serializable {
  private val ordering: Ordering[T] = implicitly[Ordering[T]]

  /**
   * Define lower horizon of the interval axis
   */
  def lowerHorizon: T

  /**
   * Define upper horizon of the interval axis
   */
  def upperHorizon: T

  /**
   * Expression to check if a value is included in a given interval
   */
  def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column

  /**
   * Expression to check if an interval is valid, e.g. start is before end.
   */
  def isValidIntervalExpr(fromCol: Column, toCol: Column): Column

  /**
   * Expression to join two intervals
   */
  def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column

  /**
   * checks whether the value is between lower- and upperHorizon
   * @param valCol
   *   column to check
   * @return
   *   boolean valued column
   */
  final def isInBoundariesExpr(valCol: Column): Column = valCol.between(lit(lowerHorizon), lit(upperHorizon))

  /**
   * make sure value is between lower- and upperHorizon
   */
  @inline def fitToHorizon(value: T): T = least(greatest(value, lowerHorizon), upperHorizon)

  final def getFitToHorizonExpr(valueCol: Column): Column =
    functions.when(valueCol.isNotNull,
      functions.least(functions.greatest(valueCol, functions.lit(lowerHorizon)), functions.lit(upperHorizon)))

  /**
   * Get the predecessor for a scala value of type T for this interval axis definition
   */
  def predecessor(value: T): T

  def getPredecessorExpr(valueCol: Column): Column

  /**
   * Get the successor for a scala value of type T for this interval axis definition
   */
  def successor(value: T): T

  def getSuccessorExpr(valueCol: Column): Column

  final val isEmpty: ((T, T)) => Boolean = i => ordering.lteq(i._2, successor(i._1))

  /**
   * calculates the intersection
   * @param minuend
   *   endpoints of minuend
   * @param subtrahend
   *   endpoints of substrahend
   * @param ordering
   *   ordering of type T
   * @return
   *   list of intervals in descending order which the union of is the complement minuend \
   *   subtrahend
   */
   qfinal val intersect: ((T, T)) => ((T, T)) => (T, T) = left => right => (ordering.max(left._1, right._1), ordering.min(left._2, right._2))

  /**
   * calculates the complement
   * @param minuend
   *   endpoints of minuend
   * @param subtrahend
   *   endpoints of substrahend
   * @param ordering
   *   ordering of type T
   * @return
   *   list of intervals in descending order which the union of is the complement minuend \
   *   subtrahend
   */
  final def complement(minuend: (T, T) = (lowerHorizon, upperHorizon))(subtrahend: (T, T))
      : List[(T, T)] =
    if (ordering.lt(subtrahend._2, minuend._1) || ordering.lt(minuend._2, subtrahend._1)) List(minuend)
    else
      List( // need to be in descending order for usage.
        (successor(subtrahend._2), minuend._2),
        (minuend._1,               predecessor(subtrahend._1))
      ).filterNot(i => isEmpty(i))

  // Helpers
  @inline private def least(values: T*): T = values.min

  @inline private def greatest(values: T*): T = values.max
}
