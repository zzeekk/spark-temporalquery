package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.interval.{IntervalDef, IntervalQueryDimension}
import ch.zzeekk.spark.temporalquery.{Crossable, Logging}
import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

/**
 * Base class defining the configuration needed for interval queries with Spark
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class MultivarRangeQueryConfig[T: Ordering, D <: IntervalDef[T]] extends Logging {
  // this is an abstract class because "traits can not have type parameters with context bounds"

  type MultivarRange = List[(T, T)]

  case class MultivarRangeUnion(rangeFamily: Seq[MultivarRange] = Nil) {
    override def toString: String = s"MultivarRangeUnion(${rangeFamily.length} ranges: ${rangeFamily.map(_.toString).mkString(" ∪ ")})"

    /**
     * Calculates the intersection of two family of ranges which are combined by union first
     * @param that
     *   sequence of sequence of range sides, i.e. (start,end) of Interval of type D
     * @return
     *   sequence of range sides, i.e. (start,end) of Interval of type D
     */
    def intersect(that: MultivarRangeUnion): MultivarRangeUnion = MultivarRangeUnion(
      this.rangeFamily.cross(that.rangeFamily).map { case (l, r) => mvrIntersect(l, r) }.filterNot(isEmpty)
    )

  }

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
  val numDimensions: Int = dimensionMap.size

  def fromToColnames: List[String] = (dimensionMap.keys ++ dimensionMap.values.map(_._1)).toList.sorted
  def additionalTechnicalColNames: List[String]

  // copy of configuration with 2nd pair of from/to column names used as main column pair
  // hint: implement with case class copy constructor in subclass
  def config2: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]]

  // TODO: change type to SET[String] if possible
  def fromToColnames2: List[String] = fromToColnames.map(increaseColNameNb).sorted

  // technical column names to be excluded in some operations
  val technicalColNames: List[String] = fromToColnames ++ additionalTechnicalColNames

  // helper column names
  def definedColName: String = "_defined"
  def definedCol: Column = col(definedColName)

  final def rangeDimensions: List[IntervalQueryDimension[T, D]] = dimensionMap.map { case (f, (t, i)) =>
    IntervalQueryDimension(
      fromColName = f,
      toColName = t,
      fromCol2Name = increaseColNameNb(f),
      toCol2Name = increaseColNameNb(t),
      lowerHorizon = i.lowerHorizon,
      upperHorizon = i.upperHorizon,
      intDef = i
    )
  }.toList.sortBy(_.fromColName)

  final def rangeIntervalDefs: List[D] = rangeDimensions.map(_.intDef)

  final def universe: MultivarRange = rangeIntervalDefs.map(d => (d.lowerHorizon, d.upperHorizon))

  @deprecated("simply wrong in multi-dimension case")
  def fromColName: String = rangeDimensions.head.fromColName
  @deprecated("simply wrong in multi-dimension case")
  def toColName: String = rangeDimensions.head.toColName
  @deprecated("simply wrong in multi-dimension case")
  def fromCol: Column = col(fromColName)
  @deprecated("simply wrong in multi-dimension case")
  def toCol: Column = col(toColName)
  @deprecated("simply wrong in multi-dimension case")
  def fromColName2: String = rangeDimensions.head.fromCol2Name
  @deprecated("simply wrong in multi-dimension case")
  def toColName2: String = rangeDimensions.head.toCol2Name
  @deprecated("simply wrong in multi-dimension case")
  def fromCol2: Column = col(fromColName2)
  @deprecated("simply wrong in multi-dimension case")
  def toCol2: Column = col(toColName2)
  @deprecated("simply wrong in multi-dimension case")
  def lowerHorizon: T = rangeDimensions.head.lowerHorizon
  @deprecated("simply wrong in multi-dimension case")
  def upperHorizon: T = rangeDimensions.head.upperHorizon
  @deprecated("simply wrong in multi-dimension case")
  def intervalDef: D = rangeDimensions.head.intDef

  // a bit of set theory for ranges

  final def isEmpty(r: MultivarRange): Boolean = {
    require(
      r.length == numDimensions,
      s"Number of range sides must equal number of dimensions, but numDimensions=$numDimensions" +
        s" and ${r.length} rangeSides given: ${r.mkString(",")} "
    )
    r.zip(rangeIntervalDefs).exists { case (rg, i) => i.isEmpty(rg) }
  }

  /**
   * Calculates the intersection of two ranges
   * @param left
   *   sequence of range sides, i.e. (start,end) of Interval of type D
   * @param right
   *   sequence of range sides, i.e. (start,end) of Interval of type D
   * @return
   *   sequence of range sides, i.e. (start,end) of Interval of type D
   */
  final def mvrIntersect(left: MultivarRange, right: MultivarRange): MultivarRange =
    left.zip(right).zip(rangeIntervalDefs.map(_.intersect)).map { case ((l, r), intersectFun) =>
      intersectFun(l)(r)
    }

  /**
   * Calculates the intersection of a family of ranges
   * @param rangeFamily
   *   sequence of sequence of range sides, i.e. (start,end) of Interval of type D
   * @return
   *   sequence of range sides, i.e. (start,end) of Interval of type D
   */
  final def mvrIntersect(rangeFamily: Seq[MultivarRange]): MultivarRange = rangeFamily.reduce(mvrIntersect)

  /**
   * returns the complement of subtrahend inside minuend: minuend \ subtrahend
   * @param subtrahend
   *   range to be substracted
   * @param minuend
   *   range to substract from, default: whole universe
   * @return
   *   list of ranges of which the union is minuend \ subtrahend Note that the result ranges overlap
   *   if 1 < numDimensions
   */
  final def complement(minuend: MultivarRange = universe)(subtrahend: MultivarRange)(implicit logger: Logger): MultivarRangeUnion = {
    require(
      minuend.length == numDimensions,
      s"Number of minuend range sides must equal number of dimensions, but numDimensions=$numDimensions" +
        s" and ${minuend.length} range sides given: ${minuend.mkString(",")} "
    )
    require(
      subtrahend.length == numDimensions,
      s"Number of subtrahend range sides must equal number of dimensions, but numDimensions=$numDimensions" +
        s" and ${subtrahend.length} range sides given: ${subtrahend.mkString(",")} "
    )
    val iter = new scala.collection.immutable.NumericRange.Exclusive(start = 0, end = numDimensions, step = 1).toList
    val diff = iter.flatMap { n =>
      val (prefMinuendSides: MultivarRange, nextMinuendSides: MultivarRange) = minuend.splitAt(n)
      List(
        List((nextMinuendSides.head._1,                         rangeIntervalDefs(n).predecessor(subtrahend(n)._1))),
        List((rangeIntervalDefs(n).successor(subtrahend(n)._2), nextMinuendSides.head._2))
      ).map(x => prefMinuendSides ++ x ++ nextMinuendSides.tail)
    }.filterNot(isEmpty)
    debugLog(s"(complement) minuend = $minuend ; subtrahend = $subtrahend ; diff = ${diff.mkString(" | ")}")
    MultivarRangeUnion(diff)
  }

  /**
   * returns the complement of subtrahend inside minuend: minuend \ subtrahend
   * @param subtrahends
   *   range to be substracted
   * @param minuend
   *   range to substract from, default: whole universe
   * @return
   *   list of ranges of which the union is minuend \ subtrahend Note that the result ranges overlap
   *   if 1 < numDimensions
   */
  final def complementFamily(minuend: MultivarRange = universe, subtrahends: Seq[MultivarRange])(implicit
      logger: Logger
  ): MultivarRangeUnion = {
    debugLog(s"(complementFamily) START minuend = $minuend")
    debugLog(s"(complementFamily) ${subtrahends.length} subtrahends = ${subtrahends.mkString(" | ")}")
    require(
      minuend.length == numDimensions,
      s"Number of minuend range sides must equal number of dimensions, but numDimensions=$numDimensions" +
        s" and ${minuend.length} range sides given: ${minuend.mkString(",")} !"
    )
    require(
      subtrahends.forall(_.length == numDimensions),
      s"Number of subtrahend range sides must equal number of dimensions, but numDimensions=$numDimensions" +
        s" and dimensions of subtrahends ${subtrahends.map(_.length).mkString(";")} !"
    )
    subtrahends.map(complement(minuend)).reduce[MultivarRangeUnion] { case (r, l) => r.intersect(l) }
  }

  // TODO: explain this function
  final def applyBooleanColumnFunctionToIntervalDefs(boolColFun: IntervalQueryDimension[T, D] => Column): Column =
    rangeDimensions.map(boolColFun).reduce((x, y) => x and y)

  // TODO: explain this function
  private def checkValue(checkFun: (Column, IntervalQueryDimension[T, D]) => Column)(values: Seq[Column]): Column = {
    require(values.length == numDimensions,
      s"Please provide as many values as dimensions! values.length=${values.length} , numDimensions=$numDimensions")
    applyBooleanColumnFunctionToIntervalDefs(dim => checkFun(values(rangeDimensions.indexOf(dim)), dim))
  }

  val isInRangeExpr: Seq[Column] => Column = checkValue(checkFun = (valCol, dim) =>
    dim.intDef.isInIntervalExpr(valCol, dim.fromCol, dim.toCol))

  val isInBoundariesExpr: Seq[Column] => Column = checkValue(checkFun = (valCol, dim) =>
    valCol.between(lit(dim.lowerHorizon), lit(dim.upperHorizon)))

  def isValidRangeExpr: Column = applyBooleanColumnFunctionToIntervalDefs(dim =>
    dim.intDef.isValidIntervalExpr(dim.fromCol, dim.toCol)
  )

  def joinRangeExpr(df1: DataFrame, df2: DataFrame)(implicit logger: Logger): Column = {
    val joinCol = applyBooleanColumnFunctionToIntervalDefs(dim =>
      dim.intDef.intervalJoinExpr(df1(dim.fromColName), df1(dim.toColName), df2(dim.fromCol2Name), df2(dim.toCol2Name))
    )
    logger.debug(s"joinIntervalExpr2: returning joinCol $joinCol")
    joinCol
  }

  def getPredecessorIntervalEndExpr(endValue: Column): Column

  def getSuccessorIntervalStartExpr(endValue: Column): Column

}
