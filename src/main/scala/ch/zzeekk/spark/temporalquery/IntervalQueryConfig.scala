package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions.col

/**
 * Base class defining the configuration needed for interval queries with Spark
 * @tparam T: scala type for interval axis
 */
abstract class IntervalQueryConfig[T: Ordering] { // this is an abstract class because "traits can not have type parameters with context bounds"
  def fromColName: String
  def toColName: String
  def additionalTechnicalColNames: Seq[String]
  def intervalDef: IntervalDef[T]

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  def config2: IntervalQueryConfig[T] // hint: implement with case class copy constructor in subclass

  // 2nd pair of from/to column names
  val fromColName2: String = fromColName+"2"
  val toColName2: String = toColName+"2"

  // technical column names to be excluded in some operations
  val technicalColNames: Seq[String] = Seq( fromColName, toColName ) ++ additionalTechnicalColNames

  // helper column names
  val definedColName: String = "_defined"

  // prepared column objects (not serializable)
  @transient lazy val fromCol: Column = col(fromColName)
  @transient lazy val toCol: Column = col(toColName)
  @transient lazy val fromCol2: Column = col(fromColName2)
  @transient lazy val toCol2: Column = col(toColName2)
  @transient lazy val definedCol: Column = col(definedColName)

  // interval functions
  def isInIntervalExpr(value: Column): Column = intervalDef.isInIntervalExpr(value, fromCol, toCol)
  def joinIntervalExpr(df1: DataFrame, df2: DataFrame): Column =
    intervalDef.intervalJoinExpr(df1(fromColName), df1(toColName), df2(fromColName), df2(toColName) )
  def joinIntervalExpr2(df1: DataFrame, df2: DataFrame): Column =
    intervalDef.intervalJoinExpr(df1(fromColName), df1(toColName), df2(fromColName2), df2(toColName2) )
  def getFloorExpr(value: Column): Column = intervalDef.getFloorExpr(value)
  def getCeilExpr(value: Column): Column = intervalDef.getCeilExpr(value)
  def getPredecessorExpr(value: Column): Column = intervalDef.getPredecessorExpr(value)
  def getSuccessorExpr(value: Column): Column = intervalDef.getSuccessorExpr(value)
}
