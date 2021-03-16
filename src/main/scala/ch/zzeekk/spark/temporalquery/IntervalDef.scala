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
   * Define lower boundary of the interval axis
   */
  def lowerBound: T
  /**
   * Define upper boundary of the interval axis
   */
  def upperBound: T
  /**
   * Expression to check if a value is included in a given interval
   */
  def isInIntervalExpr(value: Column, from: Column, to: Column): Column
  /**
   * Expression to join two intervals
   */
  def intervalJoinExpr(from1: Column, to1: Column, from2: Column, to2: Column): Column
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
  def getFloorExpr(value: Column): Column = {
    val udfTransform = udf((v: T) => floor(v))
    udfTransform(value)
  }
  /**
   * Expression to ceil a column
   */
  def getCeilExpr(value: Column): Column = {
    val udfTransform = udf((v: T) => ceil(v))
    udfTransform(value)
  }
  /**
   * Expression get predecessor for a column
   */
  def getPredecessorExpr(value: Column): Column = {
    val udfTransform = udf((v: T) => predecessor(v))
    udfTransform(value)
  }
  /**
   * Expression get successor for a column
   */
  def getSuccessorExpr(value: Column): Column = {
    val udfTransform = udf((v: T) => successor(v))
    udfTransform(value)
  }
  /**
   * make sure value is between boundaries
   */
  def fitToBoundaries(value: T): T = least(greatest(value, lowerBound), upperBound)

  // Helpers
  private def least(values: T*): T = values.min
  private def greatest(values: T*): T = values.max
}

/**
 * A closed interval is an interval which includes its lower and upper bound.
 * Use this for discret interval axis.
 * @tparam T: scala type for discrete interval axis, e.g. Timestamp, Integer, ...
 */
case class ClosedInterval[T: Ordering: TypeTag](override val lowerBound: T, override val upperBound: T, discreteAxisDef: DiscreteAxisDef[T]) extends IntervalDef[T] {
  override def isInIntervalExpr(value: Column, from: Column, to: Column): Column = {
    from <= value && value < to
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
 * @tparam T: scala type for continuous interval axis, e.g. Float, Double...
 */
case class ClosedFromOpenToInterval[T: Ordering: Fractional: TypeTag](override val lowerBound: T, override val upperBound: T) extends IntervalDef[T] {
  override def isInIntervalExpr(value: Column, from: Column, to: Column): Column = {
    from <= value && value < to
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 > fromCol2 // condition for second term is not inclusive
  }
  def floor(value: T): T = value // no rounding
  def ceil(value: T): T = value // no rounding
  def predecessor(value: T): T = value // predecessor is the same
  def successor(value: T): T = value // successor is the same
  override def getFloorExpr(value: Column): Column = value // no rounding
  override def getCeilExpr(value: Column): Column = value // no rounding
  override def getPredecessorExpr(value: Column): Column = value // predecessor is the same
  override def getSuccessorExpr(value: Column): Column = value // successor is the same
}


/**
 * Trait to describe axis behaviour for discrete Axis
 * @tparam T: scala type for interval axis
 */
abstract class DiscreteAxisDef[T] {
  def floor(value: T): T
  def next(value: T): T
  def prev(value: T): T
  def ceil(value: T): T = next(floor(value)) // round down to next step and add one step
  def predecessor(value: T): T = {
    val valueFloored = floor(value)
    if (valueFloored.equals(value)) prev(valueFloored)
    else valueFloored
  }
  def successor(value: T): T = {
    val valueCeiled = ceil(value)
    if (valueCeiled.equals(value)) next(valueCeiled)
    else valueCeiled
  }
}

/**
 * Implementation of axis behaviour for discrete time axis using Timestamp as scala type
 */
case class DiscreteTimeAxis(timeUnit: ChronoUnit) extends DiscreteAxisDef[Timestamp] {
  override def floor(value: Timestamp): Timestamp =
    Timestamp.from(value.toInstant.truncatedTo(timeUnit))
  override def next(value: Timestamp): Timestamp =
    Timestamp.from(value.toInstant.plus(1, timeUnit))
  override def prev(value: Timestamp): Timestamp =
    Timestamp.from(value.toInstant.minus(1, timeUnit))
}

/**
 * Implementation of axis behaviour for discrete time axis any Integral scala type, e.g. Integer, Long,...
 */
case class DiscreteNumericAxis[T: Integral](step: T)(implicit f: Integral[T]) extends DiscreteAxisDef[T] {
  implicit private def ops(lhs: T): f.IntegralOps = f.mkNumericOps(lhs)
  override def floor(value: T): T = (value / step) * step // round down to next step
  override def next(value: T): T = f.plus(value, step)
  override def prev(value: T): T = f.minus(value, step)
}