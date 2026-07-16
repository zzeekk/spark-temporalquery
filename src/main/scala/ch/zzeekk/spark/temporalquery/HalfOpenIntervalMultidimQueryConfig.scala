package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Column

abstract class HalfOpenIntervalMultidimQueryConfig[T: Ordering] extends IntervalMultidimQueryConfig[T, HalfOpenInterval[T]] {
  def getPredecessorIntervalEndExpr(startValue: Column): List[Column] = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(startValue))

  def getSuccessorIntervalStartExpr(endValue: Column): List[Column] = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(endValue))
}
