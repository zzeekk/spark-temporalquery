package ch.zzeekk.spark.temporalquery.interval

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col

case class IntervalQueryDimension[T, D <: IntervalDef[T]](
    fromColName: String,
    toColName: String,
    fromCol2Name: String,
    toCol2Name: String,
    lowerHorizon: T,
    upperHorizon: T,
    intDef: D
) {
  def fromCol: Column = col(fromColName)
  def toCol: Column = col(toColName)
  def fromCol2: Column = col(fromCol2Name)
  def toCol2: Column = col(toCol2Name)
}
