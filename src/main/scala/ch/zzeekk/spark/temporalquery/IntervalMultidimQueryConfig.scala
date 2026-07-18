package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

/**
 * Base class defining the configuration needed for interval queries with Spark
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class IntervalMultidimQueryConfig[T: Ordering, D <: IntervalDef[T]] extends Serializable {
  // this is an abstract class because "traits can not have type parameters with context bounds"

  // 2nd pair of from/to column names
  protected def increaseColNameNb(colName: String): String = {
    val regexColNameNb = "(.*)([0-9]+)$".r
    colName match {
      case regexColNameNb(name, nb) => name + (nb.toInt + 1).toString
      case _                        => colName + "2" // if no number found, start with 2
    }
  }

  /**
   * dimensionMap assigns to each fromColName its toColName and the interval definition
   * @return
   */
  def dimensionMap: Map[String, (String, D)]
  require(dimensionMap.nonEmpty, "at least one fromCol name must be specified!")
  private val numDimensions: Int = dimensionMap.size

  def fromToColnames: List[String] = (dimensionMap.keys ++ dimensionMap.values.map(_._1)).toList
  def additionalTechnicalColNames: Seq[String]

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  // hint: implement with case class copy constructor in subclass
  def config2: IntervalMultidimQueryConfig[T, D]

  // TODO: change type to SET[String] if possible
  def fromToColnames2: List[String] = fromToColnames.map(increaseColNameNb)

  // technical column names to be excluded in some operations
  val technicalColNames: List[String] =
    (dimensionMap.keys ++ dimensionMap.values.map(_._1) ++
      additionalTechnicalColNames).toList

  // helper column names
  def definedColName: String = "_defined"
  def definedCol: Column = col(definedColName)

  def intervalDimensions: List[IntervalQueryDimension[T, D]] = dimensionMap.map { case (f, (t, i)) =>
    IntervalQueryDimension(
      fromColName = f,
      toColName = t,
      fromCol2Name = increaseColNameNb(f),
      toCol2Name = increaseColNameNb(t),
      lowerHorizon = i.lowerHorizon,
      upperHorizon = i.upperHorizon,
      intDef = i
    )
  }.toList

  @deprecated("simply wrong in multi-dimension case")
  def fromColName: String = intervalDimensions.head.fromColName
  @deprecated("simply wrong in multi-dimension case")
  def toColName: String = intervalDimensions.head.toColName
  @deprecated("simply wrong in multi-dimension case")
  def fromCol: Column = col(fromColName)
  @deprecated("simply wrong in multi-dimension case")
  def toCol: Column = col(toColName)
  @deprecated("simply wrong in multi-dimension case")
  def fromColName2: String = intervalDimensions.head.fromCol2Name
  @deprecated("simply wrong in multi-dimension case")
  def toColName2: String = intervalDimensions.head.toCol2Name
  @deprecated("simply wrong in multi-dimension case")
  def fromCol2: Column = col(fromColName2)
  @deprecated("simply wrong in multi-dimension case")
  def toCol2: Column = col(toColName2)
  @deprecated("simply wrong in multi-dimension case")
  def lowerHorizon: T = intervalDimensions.head.lowerHorizon
  @deprecated("simply wrong in multi-dimension case")
  def upperHorizon: T = intervalDimensions.head.upperHorizon
  @deprecated("simply wrong in multi-dimension case")
  def intervalDef: D = intervalDimensions.head.intDef

  // interval functions

  // TODO: explain this function
  private def applyBooleanColumnFunctionToIntervalDefs(boolColFun: IntervalQueryDimension[T, D] => Column): Column =
    intervalDimensions.map(boolColFun).reduce((x, y) => x and y)

  // TODO: explain this function
  private def checkValue(checkFun: (Column, IntervalQueryDimension[T, D]) => Column)(values: Seq[Column]): Column = {
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

  def joinIntervalExpr(df1: DataFrame, df2: DataFrame)(implicit logger: Logger): Column = {
    val joinCol = applyBooleanColumnFunctionToIntervalDefs(dim =>
      dim.intDef.intervalJoinExpr(df1(dim.fromColName), df1(dim.toColName), df2(dim.fromCol2Name), df2(dim.toCol2Name))
    )
    logger.debug(s"joinIntervalExpr2: returning joinCol $joinCol")
    joinCol
  }

  def getPredecessorIntervalEndExpr(endValue: Column): Column

  def getSuccessorIntervalStartExpr(endValue: Column): Column

}
