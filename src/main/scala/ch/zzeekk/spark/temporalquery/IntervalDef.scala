package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.{Column, functions}
import org.apache.spark.sql.functions.udf
import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import scala.reflect.runtime.universe._

/**
 * Trait to describe interval behaviour
 * @tparam T: scala type for interval axis
 */
abstract class IntervalDef[T : Ordering : TypeTag] extends Serializable {

  /**
   * Define lower bound of the interval axis
   */
  def lowerBound: T
  /**
   * Define upper bound of the interval axis
   */
  def upperBound: T
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
   * make sure value is between lower- and upperBound
   */
  @inline def fitToBoundaries(value: T): T = least(greatest(value, lowerBound), upperBound)
  def getFitToBoundariesExpr(valueCol: Column): Column = {
    functions.when(valueCol.isNotNull, functions.least(functions.greatest(valueCol, functions.lit(lowerBound)), functions.lit(upperBound)))
  }

  // Helpers
  @inline private def least(values: T*): T = values.min
  @inline private def greatest(values: T*): T = values.max
}

/**
 * A closed interval is an interval which includes its lower and upper bound.
 * Use this for discret interval axis.
 * @param lowerBound minimum value of the interval axis
 * @param upperBound maximum value of the interval axis
 * @tparam T: scala type for discrete interval axis, e.g. Timestamp, Integer, ...
 */
case class ClosedInterval[T: Ordering: TypeTag](override val lowerBound: T, override val upperBound: T, discreteAxisDef: DiscreteAxisDef[T]) extends IntervalDef[T] {
  assert(lowerBound == floor(lowerBound), s"lowerBound $lowerBound is not discrete value of the axis")
  assert(upperBound == floor(upperBound), s"lowerBound $upperBound is not discrete value of the axis")
  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column = {
    fromCol <= valueCol && valueCol <= toCol
  }
  def isValidIntervalExpr(fromCol: Column, toCol: Column): Column = {
    fromCol <= toCol
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 >= fromCol2
  }

  /**
   * Round down a value to the next discrete value of the interval axis, respecting interval axis boundaries.
   */
  def floor(value: T): T = fitToBoundaries(discreteAxisDef.floor(value))
  def getFloorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => floor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Round up a value to the next discrete value of the interval axis, respecting interval axis boundaries.
   */
  def ceil(value: T): T = fitToBoundaries(discreteAxisDef.ceil(value))
  def getCeilExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => ceil(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Get the predecessor for a scala value of type T for this interval axis definition
   */
  def predecessor(value: T): T =
    fitToBoundaries(if (value==upperBound) value else discreteAxisDef.predecessor(value)) // max value has no predecessor
  def getPredecessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => predecessor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Get the successor for a scala value of type T for this interval axis definition
   */
  def successor(value: T): T =
    fitToBoundaries(if (value==lowerBound) value else discreteAxisDef.successor(value)) // min value has no successor
  def getSuccessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => successor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }
}

/**
 * Lower bound is included, upper bound is excluded
 * Use this mainly for continuous interval axis.
 * @param lowerBound minimum value of the interval axis
 * @param upperBound maximum value of the interval axis
 * @tparam T: scala type for continuous interval axis, e.g. Float, Double...
 */
case class HalfOpenInterval[T: Ordering: TypeTag](override val lowerBound: T, override val upperBound: T) extends IntervalDef[T] {
  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column = {
    fromCol <= valueCol && valueCol < toCol
  }
  def isValidIntervalExpr(fromCol: Column, toCol: Column): Column = {
    fromCol < toCol
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 > fromCol2 // condition for second term is not inclusive
  }
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
    if (valueFloor.equals(value)) value
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