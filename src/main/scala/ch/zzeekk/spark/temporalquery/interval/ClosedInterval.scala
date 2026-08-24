package ch.zzeekk.spark.temporalquery.interval

import ch.zzeekk.spark.temporalquery.axis.DiscreteAxisDef
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.udf

import scala.reflect.runtime.universe.TypeTag

/**
 * A closed interval is an interval which includes its lower and upper bound. Use this for discret
 * interval axis.
 *
 * @param lowerHorizon
 *   negative infinity value of the interval axis. This value is used to denote intervals which have
 *   no lower bound.
 * @param upperHorizon
 *   positive infinity value of the interval axis. This value is used to denote intervals which have
 *   no upper bound.
 * @tparam T
 *   : scala type for discrete interval axis, e.g. Timestamp, Integer, ...
 */
case class ClosedInterval[T: Ordering: TypeTag](
    override val lowerHorizon: T,
    override val upperHorizon: T,
    discreteAxisDef: DiscreteAxisDef[T]
) extends IntervalDef[T] {
  require(lowerHorizon == floor(lowerHorizon), s"lowerHorizon $lowerHorizon is not discrete value of the axis")
  require(upperHorizon == floor(upperHorizon), s"upperHorizon $upperHorizon is not discrete value of the axis")

  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column =
    fromCol <= valueCol && valueCol <= toCol

  def isNonEmptyExpr(fromCol: Column, toCol: Column): Column =
    fromCol <= toCol

  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column =
    fromCol1 <= toCol2 and fromCol2 <= toCol1

  /**
   * Round down a value to the next discrete value of the interval axis, respecting interval axis
   * boundaries.
   */
  def floor(value: T): T = fitToHorizon(discreteAxisDef.floor(value))

  def getFloorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => floor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Round up a value to the next discrete value of the interval axis, respecting interval axis
   * boundaries.
   */
  def ceil(value: T): T = fitToHorizon(discreteAxisDef.ceil(value))

  def getCeilExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => ceil(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Get the predecessor for a scala value of type T for this interval axis definition
   */
  def predecessor(value: T): T =
    fitToHorizon(
      if (value == lowerHorizon || value == upperHorizon) value else discreteAxisDef.predecessor(value)
    ) // max value has no predecessor

  def getPredecessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => predecessor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }

  /**
   * Get the successor for a scala value of type T for this interval axis definition
   */
  def successor(value: T): T =
    fitToHorizon(
      if (value == lowerHorizon || value == upperHorizon) value else discreteAxisDef.successor(value)
    ) // min value has no successor

  def getSuccessorExpr(valueCol: Column): Column = {
    val udfTransform = udf((v: Any) => Option(v).map(x => successor(x.asInstanceOf[T])))
    udfTransform(valueCol)
  }
}
