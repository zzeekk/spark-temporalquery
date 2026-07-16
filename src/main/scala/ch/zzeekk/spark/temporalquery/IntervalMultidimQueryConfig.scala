package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}

case class IntervalQueryDimension[T, D <: IntervalDef[T]](
    fromColName: String,
    toColName: String,
    fromCol2Name: String,
    toCol2Name: String,
    fromCol: Column,
    toCol: Column,
    fromCol2: Column,
    toCol2: Column,
    lowerHorizon: T,
    upperHorizon: T,
    intDef: D
)

/**
 * Base class defining the configuration needed for interval queries with Spark
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class IntervalMultidimQueryConfig[T: Ordering, D <: IntervalDef[T]] extends Serializable {
  // this is an abstract class because "traits can not have type parameters with context bounds"
  def fromColNames: Seq[String]

  val numDimensions: Int = fromColNames.length

  def toColNames: Seq[String]

  def additionalTechnicalColNames: Seq[String]

  def intervalDefs: Seq[D]

  require(fromColNames.nonEmpty, "at least one fromCol name must be specified")
  require(fromColNames.length == toColNames.length, "please provide equal number of fromCol names and toCol name")
  require(fromColNames.length == intervalDefs.length, "please provide equal number of fromCol names and interval definitions")

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  def config2: IntervalMultidimQueryConfig[T, D] // hint: implement with case class copy constructor in subclass

  // 2nd pair of from/to column names
  private def increaseColNameNb(colName: String): String = {
    val regexColNameNb = "(.*)([0-9]+)$".r
    colName match {
      case regexColNameNb(name, nb) => name + (nb.toInt + 1).toString
      case _                        => colName + "2" // if no number found, start with 2
    }
  }

  // technical column names to be excluded in some operations
  val technicalColNames: Seq[String] = fromColNames ++ toColNames ++ additionalTechnicalColNames

  // helper column names
  val definedColName: String = "_defined"

  def definedCol: Column = col(definedColName)

  val intervalDimensions: scala.collection.immutable.IndexedSeq[IntervalQueryDimension[T, D]] = 0 until numDimensions map { n =>
    IntervalQueryDimension(
      fromColName = fromColNames(n),
      toColName = toColNames(n),
      fromCol2Name = fromColNames.map(increaseColNameNb)(n),
      toCol2Name = toColNames.map(increaseColNameNb)(n),
      fromCol = fromColNames.map(col)(n),
      toCol = toColNames.map(col)(n),
      fromCol2 = fromColNames.map(increaseColNameNb).map(col)(n),
      toCol2 = toColNames.map(increaseColNameNb).map(col)(n),
      lowerHorizon = intervalDefs.map(_.lowerHorizon)(n),
      upperHorizon = intervalDefs.map(_.upperHorizon)(n),
      intDef = intervalDefs(n)
    )
  }

  // interval functions

  def applyBooleanColumnFunctionToIntervalDefs(boolColFun: IntervalQueryDimension[T, D] => Column): Column =
    intervalDimensions.map(boolColFun).reduce((x, y) => x and y)

  def checkValue(checkFun: (Column, IntervalQueryDimension[T, D]) => Column)(values: Seq[Column]): Column = {
    require(values.length == numDimensions, "Please provide as many values as dimenions")
    applyBooleanColumnFunctionToIntervalDefs(dim => checkFun(values(intervalDimensions.indexOf(dim)), dim))
  }

  val isInIntervalExpr: Seq[Column] => Column = checkValue(checkFun = (valCol, dim) =>
    dim.intDef.isInIntervalExpr(valCol, dim.fromCol, dim.toCol))

  val isInBoundariesExpr: Seq[Column] => Column = checkValue(checkFun = (valCol, dim) =>
    valCol.between(lit(dim.lowerHorizon), lit(dim.upperHorizon)))

  def isValidIntervalExpr: Column = applyBooleanColumnFunctionToIntervalDefs(dim =>
    dim.intDef.isValidIntervalExpr(dim.fromCol, dim.toCol)
  )

  def isValidIntervalExpr2: Column = intervalDimensions.map {
    dim => dim.intDef.isValidIntervalExpr(dim.fromCol, dim.toCol)
  }.reduce((x, y) => x and y)

  def joinIntervalExpr(df1: DataFrame, df2: DataFrame): Column = applyBooleanColumnFunctionToIntervalDefs(dim =>
    dim.intDef.intervalJoinExpr(df1(dim.fromColName), df1(dim.toColName), df2(dim.fromColName), df2(dim.toColName))
  )

  def joinIntervalExpr2(df1: DataFrame, df2: DataFrame): Column = applyBooleanColumnFunctionToIntervalDefs(dim =>
    dim.intDef.intervalJoinExpr(df1(dim.fromColName), df1(dim.toColName), df2(dim.fromCol2Name), df2(dim.toCol2Name))
  )

  // TODO: Not sure how this can be usefull in multidimensional intervals
  def getPredecessorIntervalEndExpr(startValue: Column): Seq[Column]

  // TODO: Not sure how this can be usefull in multidimensional intervals
  def getSuccessorIntervalStartExpr(endValue: Column): Seq[Column]
}

abstract class ClosedIntervalMultidimQueryConfig[T: Ordering] extends IntervalMultidimQueryConfig[T, ClosedInterval[T]] {
  def getFloorExpr(value: Column): Seq[Column] = intervalDimensions.map(_.intDef.getFloorExpr(value))

  def getCeilExpr(value: Column): Seq[Column] = intervalDimensions.map(_.intDef.getCeilExpr(value))

  def getPredecessorIntervalEndExpr(startValue: Column): Seq[Column] = intervalDimensions
    .map(_.intDef.getPredecessorExpr(startValue))

  def getSuccessorIntervalStartExpr(endValue: Column): Seq[Column] = intervalDimensions
    .map(_.intDef.getSuccessorExpr(endValue))
}

abstract class HalfOpenIntervalMultidimQueryConfig[T: Ordering] extends IntervalMultidimQueryConfig[T, HalfOpenInterval[T]] {
  def getPredecessorIntervalEndExpr(startValue: Column): Seq[Column] = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(startValue))

  def getSuccessorIntervalStartExpr(endValue: Column): Seq[Column] = intervalDimensions
    .map(_.intDef.getFitToHorizonExpr(endValue))
}
