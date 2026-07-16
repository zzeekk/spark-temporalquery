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

  /**
   * dimensionMap assiugns to each fromColName its toCoilName and the interval definition
   * @return
   */
  def dimensionMap: Map[String, (String, D)]
  require(dimensionMap.nonEmpty, "at least one fromCol name must be specified")
  val numDimensions: Int = dimensionMap.size

  def additionalTechnicalColNames: Seq[String]

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  def config2: IntervalMultidimQueryConfig[T, D] // hint: implement with case class copy constructor in subclass

  // 2nd pair of from/to column names
  protected def increaseColNameNb(colName: String): String = {
    val regexColNameNb = "(.*)([0-9]+)$".r
    colName match {
      case regexColNameNb(name, nb) => name + (nb.toInt + 1).toString
      case _                        => colName + "2" // if no number found, start with 2
    }
  }

  // technical column names to be excluded in some operations
  val technicalColNames: List[String] =
    (dimensionMap.keys ++ dimensionMap.values.map(_._1) ++
      additionalTechnicalColNames).toList

  // helper column names
  val definedColName: String = "_defined"
  def definedCol: Column = col(definedColName)

  val intervalDimensions: List[IntervalQueryDimension[T, D]] = dimensionMap.map { case (f, (t, i)) =>
    IntervalQueryDimension(
      fromColName = f,
      toColName = t,
      fromCol2Name = increaseColNameNb(f),
      toCol2Name = increaseColNameNb(t),
      fromCol = col(f),
      toCol = col(t),
      fromCol2 = col(increaseColNameNb(f)),
      toCol2 = col(increaseColNameNb(t)),
      lowerHorizon = i.lowerHorizon,
      upperHorizon = i.upperHorizon,
      intDef = i
    )
  }.toList

  // interval functions

  // TODO: explain this function
  def applyBooleanColumnFunctionToIntervalDefs(boolColFun: IntervalQueryDimension[T, D] => Column): Column =
    intervalDimensions.map(boolColFun).reduce((x, y) => x and y)

  // TODO: explain this function
  def checkValue(checkFun: (Column, IntervalQueryDimension[T, D]) => Column)(values: Seq[Column]): Column = {
    require(values.length == numDimensions, "Please provide as many values as dimensions")
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

}
