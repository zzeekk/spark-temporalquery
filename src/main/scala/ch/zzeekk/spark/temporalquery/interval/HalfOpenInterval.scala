package ch.zzeekk.spark.temporalquery.interval

import org.apache.spark.sql.Column

import scala.reflect.runtime.universe.TypeTag

/**
 * Lower bound is included, upper bound is excluded Use this mainly for continuous interval axis.
 *
 * @param lowerHorizon
 *   negative infinity value of the interval axis. This value is used to denote intervals which have
 *   no lower bound.
 * @param upperHorizon
 *   positive infinity value of the interval axis. This value is used to denote intervals which have
 *   no upper bound.
 * @tparam T
 *   : scala type for continuous interval axis, e.g. Float, Double...
 */
case class HalfOpenInterval[T: Ordering: TypeTag](
    override val lowerHorizon: T,
    override val upperHorizon: T
) extends IntervalDef[T] {
  override def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column =
    fromCol <= valueCol && valueCol < toCol

  def getPredecessorExpr(valueCol: Column): Column = getFitToHorizonExpr(valueCol)

  def getSuccessorExpr(valueCol: Column): Column = getFitToHorizonExpr(valueCol)

  def isValidIntervalExpr(fromCol: Column, toCol: Column): Column =
    fromCol < toCol

  override def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column =
    fromCol1 < toCol2 and toCol1 > fromCol2
}
