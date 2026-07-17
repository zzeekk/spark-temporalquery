package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Column

abstract class HalfOpenIntervalMultidimQueryConfig[T: Ordering] extends IntervalMultidimQueryConfig[T, HalfOpenInterval[T]] {
  def getPredecessorIntervalEndExpr(startValue: Column): Column = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(startValue)).reduce((x, y) => x and y)

  def getSuccessorIntervalStartExpr(endValue: Column): Column = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(endValue)).reduce((x, y) => x and y)
}
