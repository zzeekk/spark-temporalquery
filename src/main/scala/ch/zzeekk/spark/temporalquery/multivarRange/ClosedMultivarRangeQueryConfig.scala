package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import org.apache.spark.sql.Column

abstract class ClosedMultivarRangeQueryConfig[T: Ordering] extends MultivarRangeQueryConfig[T, ClosedInterval[T]] {

  def getFloorExpr(value: Column): List[Column] = rangeDimensions.map(_.intDef.getFloorExpr(value))

  def getCeilExpr(value: Column): List[Column] = rangeDimensions.map(_.intDef.getCeilExpr(value))

}
