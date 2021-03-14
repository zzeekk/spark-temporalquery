package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import scala.reflect.runtime.universe._

/**
 * Trait to describe interval behaviour
 */
abstract class IntervalDef[T: TypeTag] {
  def isInIntervalExpr(value: Column, from: Column, to: Column): Column
  def intervalJoinExpr(from1: Column, to1: Column, from2: Column, to2: Column): Column
  def floor(value: T, tc: IntervalQueryConfig[T]): T
  def ceil(value: T, tc: IntervalQueryConfig[T]): T
  def predecessor(value: T, tc: IntervalQueryConfig[T]): T
  def successor(value: T, tc: IntervalQueryConfig[T]): T
  def getFloorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = {
    val udfTransform = udf((v: T) => floor(v, tc))
    udfTransform(value)
  }
  def getCeilExpr(value: Column, tc: IntervalQueryConfig[T]): Column = {
    val udfTransform = udf((v: T) => ceil(v, tc))
    udfTransform(value)
  }
  def getPredecessorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = {
    val udfTransform = udf((v: T) => predecessor(v, tc))
    udfTransform(value)
  }
  def getSuccessorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = {
    val udfTransform = udf((v: T) => successor(v, tc))
    udfTransform(value)
  }
}

/**
 * A closed interval is an interval which includes its lower and upper bound.
 * Use this for discret interval axis.
 */
case class ClosedInterval[T: TypeTag](discreteAxisType: DiscreteAxisDef[T]) extends IntervalDef[T] {
  override def isInIntervalExpr(value: Column, from: Column, to: Column): Column = {
    from <= value && value < to
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 >= fromCol2
  }
  def floor(value: T, tc: IntervalQueryConfig[T]): T = discreteAxisType.floor(value, tc)
  def ceil(value: T, tc: IntervalQueryConfig[T]): T = discreteAxisType.ceil(value, tc)
  def predecessor(value: T, tc: IntervalQueryConfig[T]): T = discreteAxisType.predecessor(value, tc)
  def successor(value: T, tc: IntervalQueryConfig[T]): T = discreteAxisType.successor(value, tc)
}

/**
 * Lower bound is included, upper bound is excluded
 * Use this for continuous interval axis.
 */
case class ClosedFromOpenToInterval[T: Fractional: TypeTag]() extends IntervalDef[T] {
  override def isInIntervalExpr(value: Column, from: Column, to: Column): Column = {
    from <= value && value < to
  }
  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column = {
    fromCol1 <= toCol2 and toCol1 > fromCol2 // condition for second term is not inclusive
  }
  def floor(value: T, tc: IntervalQueryConfig[T]): T = value // no rounding
  def ceil(value: T, tc: IntervalQueryConfig[T]): T = value // no rounding
  def predecessor(value: T, tc: IntervalQueryConfig[T]): T = value // predecessor is the same
  def successor(value: T, tc: IntervalQueryConfig[T]): T = value // successor is the same
  override def getFloorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = value // no rounding
  override def getCeilExpr(value: Column, tc: IntervalQueryConfig[T]): Column = value // no rounding
  override def getPredecessorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = value // predecessor is the same
  override def getSuccessorExpr(value: Column, tc: IntervalQueryConfig[T]): Column = value // successor is the same
}


/**
 * Trait to describe axis behaviour for discrete Axis
 */
trait DiscreteAxisDef[T] {
  def floor(value: T, tc: IntervalQueryConfig[T]): T
  def ceil(value: T, tc: IntervalQueryConfig[T]): T
  def predecessor(value: T, tc: IntervalQueryConfig[T]): T
  def successor(value: T, tc: IntervalQueryConfig[T]): T
}

case class TimeAxis(timeUnit: ChronoUnit) extends DiscreteAxisDef[Timestamp] {
  override def floor(value: Timestamp, tc: IntervalQueryConfig[Timestamp]): Timestamp =
    TemporalHelpers.floorTimestamp(value, timeUnit, tc.minValue, tc.maxValue)
  override def ceil(value: Timestamp, tc: IntervalQueryConfig[Timestamp]): Timestamp =
    TemporalHelpers.ceilTimestamp(value, timeUnit, tc.minValue, tc.maxValue)
  override def predecessor(value: Timestamp, tc: IntervalQueryConfig[Timestamp]): Timestamp =
    TemporalHelpers.predecessorTime(value, timeUnit, tc.minValue, tc.maxValue)
  override def successor(value: Timestamp, tc: IntervalQueryConfig[Timestamp]): Timestamp =
    TemporalHelpers.successorTime(value, timeUnit, tc.minValue, tc.maxValue)
}