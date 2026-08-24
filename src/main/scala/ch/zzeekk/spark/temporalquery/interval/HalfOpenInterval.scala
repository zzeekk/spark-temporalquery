package ch.zzeekk.spark.temporalquery.interval

import org.apache.spark.sql.Column

import scala.reflect.runtime.universe.TypeTag

/**
 * Lower bound is included, upper bound is excluded Use this mainly for dense interval axis.
 *
 * @param lowerHorizon
 *   negative infinity value of the interval axis. This value is used to denote intervals which have
 *   no lower bound.
 * @param upperHorizon
 *   positive infinity value of the interval axis. This value is used to denote intervals which have
 *   no upper bound.
 * @tparam T
 *   : scala type for dense interval axis, e.g. Float, Double...
 */
case class HalfOpenInterval[T: Ordering: TypeTag](
    override val lowerHorizon: T,
    override val upperHorizon: T
) extends IntervalDef[T] {

  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column =
    fromCol <= valueCol && valueCol < toCol

  /**
   * Get the predecessor for a scala value of type T for this interval axis definition
   */
  def predecessor(value: T): T = value

  def getPredecessorExpr(valueCol: Column): Column = getFitToHorizonExpr(valueCol)

  /**
   * Get the successor for a scala value of type T for this interval axis definition
   */
  def successor(value: T): T = value

  def getSuccessorExpr(valueCol: Column): Column = getFitToHorizonExpr(valueCol)

  def isNonEmptyExpr(fromCol: Column, toCol: Column): Column =
    fromCol < toCol

  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column =
    fromCol1 < toCol2 and toCol1 > fromCol2
}
