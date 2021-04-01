package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.reflect.runtime.universe._

/**
 * Trait to describe interval behaviour
 * @tparam T: scala type for interval axis
 */
abstract class IntervalDef[T: Ordering: TypeTag] {

  /**
   * Define min value of the interval axis
   */
  def minValue: T
  /**
   * Define max value of the interval axis
   */
  def maxValue: T
  /**
   * Expression to check if a value is included in a given interval
   */
  def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column
  /**
   * Expression to join two intervals
   */
  def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column
  /**
   * Method to floor a scala value of type T
   */
  def floor(value: T): T
  /**
   * Method to ceil a scala value of type T
   */
  def ceil(value: T): T
  /**
   * Method get the predecessor for a scala value of type T for this interval axis definition
   */
  def predecessor(value: T): T
  /**
   * Method get the successor for a scala value of type T for this interval axis definition
   */
  def successor(value: T): T

  /**
   * Expression to floor a column
   */
  def getFloorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: T) => floor(v))
    udfTransform(valueCol)
  }
  /**
   * Expression to ceil a column
   */
  def getCeilExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: T) => ceil(v))
    udfTransform(valueCol)
  }
  /**
   * Expression get predecessor for a column
   */
  def getPredecessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: T) => predecessor(v))
    udfTransform(valueCol)
  }
  /**
   * Expression get successor for a column
   */
  def getSuccessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: T) => successor(v))
    udfTransform(valueCol)
  }
  /**
   * make sure value is between min- and maxValue
   */
  def fitToBoundaries(value: T): T = least(greatest(value, minValue), maxValue)

  // Helpers
  @inline private def least(values: T*): T = values.min
  @inline private def greatest(values: T*): T = values.max
}

/**
 * A closed interval is an interval which includes its lower and upper bound.
 * Use this for discret interval axis.
 * @param minValue minimum value of the interval axis
 * @param maxValue maximum value of the interval axis
 * @tparam T: scala type for discrete interval axis, e.g. Timestamp, Integer, ...
 */
case class ClosedInterval[T: Ordering: TypeTag](override val minValue: T, override val maxValue: T, discreteAxisDef: DiscreteAxisDef[T]) extends IntervalDef[T] {
  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column = {
    fromCol <= valueCol && valueCol < toCol
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 >= fromCol2
  }
  def floor(value: T): T = fitToBoundaries(discreteAxisDef.floor(value))
  def ceil(value: T): T = fitToBoundaries(discreteAxisDef.ceil(value))
  def predecessor(value: T): T = fitToBoundaries(discreteAxisDef.predecessor(value))
  def successor(value: T): T = fitToBoundaries(discreteAxisDef.successor(value))
}

/**
 * Lower bound is included, upper bound is excluded
 * Use this for continuous interval axis.
 * @param minValue minimum value of the interval axis
 * @param maxValue maximum value of the interval axis
 * @tparam T: scala type for continuous interval axis, e.g. Float, Double...
 */
case class ClosedFromOpenToInterval[T: Ordering: Fractional: TypeTag](override val minValue: T, override val maxValue: T) extends IntervalDef[T] {
  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column = {
    fromCol <= valueCol && valueCol < toCol
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 > fromCol2 // condition for second term is not inclusive
  }
  def floor(value: T): T = value // no rounding
  def ceil(value: T): T = value // no rounding
  def predecessor(value: T): T = value // predecessor is the same
  def successor(value: T): T = value // successor is the same
  override def getFloorExpr(valueCol: Column): Column = valueCol // no rounding
  override def getCeilExpr(valueCol: Column): Column = valueCol // no rounding
  override def getPredecessorExpr(valueCol: Column): Column = valueCol // predecessor is the same
  override def getSuccessorExpr(valueCol: Column): Column = valueCol // successor is the same
}


/**
 * Trait to describe axis behaviour for discrete Axis
 * @tparam T: scala type for interval axis
 */
abstract class DiscreteAxisDef[T] {
  def floor(value: T): T
  def next(value: T): T
  def prev(value: T): T
  def ceil(value: T): T = {
    // round down step and eventually add one step
    val valueFloor = floor(value)
    if (valueFloor.equals(value)) valueFloor
    else next(valueFloor)
  }
  def predecessor(value: T): T = {
    // round down step and eventually remove one step
    val valueFloored = floor(value)
    if (valueFloored.equals(value)) prev(valueFloored)
    else valueFloored
  }
  def successor(value: T): T = {
    // round down step and add one step
    next(floor(value))
  }
}

/**
 * Implementation of axis behaviour for discrete time axis using Timestamp as scala axis type
 * @param timeUnit time unit used as step for discrete time axis
 */
case class DiscreteTimeAxis(timeUnit: ChronoUnit) extends DiscreteAxisDef[Timestamp] {
  override def floor(value: Timestamp): Timestamp = Timestamp.from(value.toInstant.truncatedTo(timeUnit))
  override def next(value: Timestamp): Timestamp = Timestamp.from(value.toInstant.plus(1, timeUnit))
  override def prev(value: Timestamp): Timestamp = Timestamp.from(value.toInstant.minus(1, timeUnit))
}

/**
 * Implementation of axis behaviour for discrete time axis any Integral scala type, e.g. Integer, Long,...
 * @param step step size used for discrete interval axis
 * @tparam T: scala type for interval axis
 */
case class DiscreteNumericAxis[T: Integral](step: T)(implicit f: Integral[T]) extends DiscreteAxisDef[T] {
  implicit private def ops(lhs: T): f.IntegralOps = f.mkNumericOps(lhs)
  override def floor(value: T): T = (value / step) * step // round down to next step
  override def next(value: T): T = f.plus(value, step)
  override def prev(value: T): T = f.minus(value, step)
}