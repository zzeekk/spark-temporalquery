package ch.zzeekk.spark.temporalquery.interval

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{functions, Column}

import scala.language.implicitConversions
import scala.reflect.runtime.universe.TypeTag

/**
 * Trait to describe interval behaviour
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class IntervalDef[T: Ordering: TypeTag] extends Serializable {

  /**
   * Define lower horizon of the interval axis
   */
  def lowerHorizon: T

  /**
   * Define upper horizon of the interval axis
   */
  def upperHorizon: T

  /**
   * Expression to check if a value is included in a given interval
   */
  def isInIntervalExpr(valueCol: Column, fromCol: Column, toCol: Column): Column

  /**
   * Expression to check if an interval is valid, e.g. start is before end.
   */
  def isValidIntervalExpr(fromCol: Column, toCol: Column): Column

  /**
   * Expression to join two intervals
   */
  def intervalJoinExpr(fromCol1: Column, toCol1: Column, fromCol2: Column, toCol2: Column): Column

  /**
   * checks whether the value is between lower- and upperHorizon
   * @param valCol
   *   column to check
   * @return
   *   boolean valued column
   */
  final def isInBoundariesExpr(valCol: Column): Column = valCol.between(lit(lowerHorizon), lit(upperHorizon))

  /**
   * make sure value is between lower- and upperHorizon
   */
  @inline def fitToHorizon(value: T): T = least(greatest(value, lowerHorizon), upperHorizon)

  final def getFitToHorizonExpr(valueCol: Column): Column =
    functions.when(valueCol.isNotNull,
      functions.least(functions.greatest(valueCol, functions.lit(lowerHorizon)), functions.lit(upperHorizon)))

  def getPredecessorExpr(valueCol: Column): Column

  def getSuccessorExpr(valueCol: Column): Column

  // Helpers
  @inline private def least(values: T*): T = values.min

  @inline private def greatest(values: T*): T = values.max
}
