package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.{col, lit, lower}

/**
 * Base class defining the configuration needed for interval queries with Spark
 * @tparam T: scala type for interval axis
 */
abstract class IntervalQueryConfig[T: Ordering, D <: IntervalDef[T]] extends Serializable { // this is an abstract class because "traits can not have type parameters with context bounds"
  def fromColName: String
  def toColName: String
  def additionalTechnicalColNames: Seq[String]
  def intervalDef: D

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  def config2: IntervalQueryConfig[T, D] // hint: implement with case class copy constructor in subclass

  // 2nd pair of from/to column names
  private def increaseColNameNb(colName: String): String = {
    val regexColNameNb = "(.*)([0-9]+)$".r
    colName match {
      case regexColNameNb(name, nb) => name + (nb.toInt+1).toString
      case _ => colName + "2" // if no number found, start with 2
    }
  }
  val fromColName2: String = increaseColNameNb(fromColName)
  val toColName2: String = increaseColNameNb(toColName)

  // technical column names to be excluded in some operations
  val technicalColNames: Seq[String] = Seq( fromColName, toColName ) ++ additionalTechnicalColNames

  // helper column names
  val definedColName: String = "_defined"

  // prepared column objects (implemented as methods as Column is not serializable)
  def fromCol: Column = col(fromColName)
  def toCol: Column = col(toColName)
  def fromCol2: Column = col(fromColName2)
  def toCol2: Column = col(toColName2)
  def definedCol: Column = col(definedColName)

  def lowerHorizon: T = intervalDef.lowerHorizon
  def upperHorizon: T = intervalDef.upperHorizon

  // interval functions
  def isInIntervalExpr(value: Column): Column = intervalDef.isInIntervalExpr(value, fromCol, toCol)
  def isValidIntervalExpr: Column = intervalDef.isValidIntervalExpr(fromCol, toCol)
  def isInBoundariesExpr(value: Column): Column = value.between(lit(lowerHorizon), lit(upperHorizon))
  def joinIntervalExpr(df1: DataFrame, df2: DataFrame): Column =
    intervalDef.intervalJoinExpr(df1(fromColName), df1(toColName), df2(fromColName), df2(toColName) )
  def joinIntervalExpr2(df1: DataFrame, df2: DataFrame): Column =
    intervalDef.intervalJoinExpr(df1(fromColName), df1(toColName), df2(fromColName2), df2(toColName2) )
  def getPredecessorIntervalEndExpr(startValue: Column): Column
  def getSuccessorIntervalStartExpr(endValue: Column): Column
}

abstract class ClosedIntervalQueryConfig[T: Ordering] extends IntervalQueryConfig[T,ClosedInterval[T]] {
  def getFloorExpr(value: Column): Column = intervalDef.getFloorExpr(value)
  def getCeilExpr(value: Column): Column = intervalDef.getCeilExpr(value)
  def getPredecessorIntervalEndExpr(startValue: Column): Column = intervalDef.getPredecessorExpr(startValue)
  def getSuccessorIntervalStartExpr(endValue: Column): Column = intervalDef.getSuccessorExpr(endValue)
}

abstract class HalfOpenIntervalQueryConfig[T: Ordering] extends IntervalQueryConfig[T, HalfOpenInterval[T]] {
  def getPredecessorIntervalEndExpr(startValue: Column): Column = intervalDef.getFitToHorizonExpr(startValue)
  def getSuccessorIntervalStartExpr(endValue: Column): Column = intervalDef.getFitToHorizonExpr(endValue)
}