package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Column

abstract class ClosedIntervalMultidimQueryConfig[T: Ordering] extends IntervalMultidimQueryConfig[T, ClosedInterval[T]] {
  def getFloorExpr(value: Column): List[Column] = intervalDimensions.map(_.intDef.getFloorExpr(value))

  def getCeilExpr(value: Column): List[Column] = intervalDimensions.map(_.intDef.getCeilExpr(value))

  @deprecated("simply wrong in multi-dimension case")
  def getPredecessorIntervalEndExpr(startValue: Column): Column = intervalDef.getPredecessorExpr(startValue)

  @deprecated("simply wrong in multi-dimension case")
  def getSuccessorIntervalStartExpr(endValue: Column): Column = intervalDef.getSuccessorExpr(endValue)

}
