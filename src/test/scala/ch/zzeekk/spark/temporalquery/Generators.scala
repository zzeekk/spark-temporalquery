package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.multivarRange.MultivarRangeQueryConfig
import ch.zzeekk.spark.temporalquery.util.GenericDoubleQueryUtil._
import org.apache.spark.sql.functions.{col, get, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.scalacheck.Gen

trait Generators extends TestUtils {
  import session.implicits._

  val unitDouble: Gen[Double] = Gen.choose[Double](min = 0.1, max = 0.9)

  /////////////////////////////////////////////////////////////////////
  ///// generator of data frames with 2 dimensions of type Double /////
  /////////////////////////////////////////////////////////////////////
  case class Point(x: Double = 0d, y: Double = 0d) extends Ordered[Point] with Serializable {
    def compare(that: Point): Int = this.x.compare(that.x) match {
      case 0 => this.y.compare(that.y)
      case n => n
    }
  }

  case class Rectangle(lowerLeftPoint: Point = Point(), upperRightPoint: Point = Point(x = 1d, y = 1d)) {
    def isPtInside(pt: Point): Boolean = lowerLeftPoint.x < pt.x && lowerLeftPoint.y < pt.y &&
      pt.x < upperRightPoint.x && pt.y < upperRightPoint.y

    def split(splitPoints: Seq[Point]): List[Rectangle] = {
      val splitPtsInside = splitPoints.filter(isPtInside).sorted
      if (splitPtsInside.isEmpty) List(this)
      else {
        val splitPt = splitPtsInside.head
        Rectangle(lowerLeftPoint, splitPt).split(splitPtsInside.tail) ++
          Rectangle(Point(lowerLeftPoint.x, splitPt.y), Point(splitPt.x, upperRightPoint.y)).split(splitPtsInside.tail) ++
          Rectangle(Point(splitPt.x, lowerLeftPoint.y), Point(upperRightPoint.x, splitPt.y)).split(splitPtsInside.tail) ++
          Rectangle(splitPt, upperRightPoint).split(splitPtsInside.tail)
      }
    }

    def toPtTupleWithValue[T](value: T): (Double, Double, Double, Double, T) =
      (lowerLeftPoint.x, upperRightPoint.x, lowerLeftPoint.y, upperRightPoint.y, value)

  }

  val testUnitPoint2: Gen[Point] = Gen
    .zip[Double, Double](g1 = unitDouble, g2 = unitDouble).map { case (x, y) => Point(x, y) }
  val testUnitPoint2s: Gen[List[Point]] = Gen.nonEmptyListOf(g = testUnitPoint2)

  /**
   * generates data frames with two dense dimensions of which the domain is the unit square with
   * constant string value
   *
   * @param value
   *   the constant string value
   * @return
   *   dataFrame with several entries with constant value
   */
  def dfBiTempConstantUnitSplitted(value: String)(implicit mrqc: MultivarRangeQueryConfig[Double, _]): Gen[DataFrame] = {
    require(mrqc.numDimensions == 2, s"(dfBiTempConstantUnitSplitted) mrqc must have 2 dimensions but mrqc = $mrqc")
    val colNames = mrqc.rangeDimensions.map(d => (d.fromColName, d.toColName))
    testUnitPoint2s
      .map(Rectangle().split)
      .map[List[(Double, Double, Double, Double, String)]] {
        (rects: List[Rectangle]) => rects.map(_.toPtTupleWithValue(value))
      }.map(_.toDF(colNames.head._1, colNames.head._2, colNames.last._1, colNames.last._2, "value"))
  }

  /////////////////////////////////////////////////////////////////////
  ///// generator of data frames with n dimensions of type Double /////
  /////////////////////////////////////////////////////////////////////

  case class PointN(coords: Iterable[Double] = List(0d)) extends Ordered[PointN] with Serializable {
    val numDim: Int = coords.size
    def compare(that: PointN): Int = if (coords.isEmpty) {
      if (that.coords.isEmpty) 0 else -1
    } else {
      this.coords.head.compare(that.coords.head) match {
        case 0 => PointN(this.coords.tail).compare(PointN(that.coords.tail))
        case n => n
      }
    }
  }

  def getPointsFromDoubles(xs: Iterable[Double]): List[PointN] = if (xs.isEmpty) Nil
  else {
    val numDim = (2 to xs.size / 2).find(i => xs.size % i == 0).getOrElse(xs.size)
    xs.grouped(numDim).map(PointN).toList
  }

  def getCorners(lowerPt: PointN, upperPt: PointN, splitPt: PointN)(replaceUpper: Iterable[Boolean]): (PointN, PointN) = {
    require(
      lowerPt.numDim == upperPt.numDim && lowerPt.numDim == splitPt.numDim && lowerPt.numDim == replaceUpper.size,
      s"(getCorners) dimensions and length of selector list must match but:" +
        s" lowerPt=$lowerPt ;  upperPt=$upperPt ;  splitPt=$splitPt ;  replaceUpper=$replaceUpper"
    )
    (PointN(lowerPt.coords.zip(splitPt.coords).zip(replaceUpper).map { case ((l, s), p) => if (p) l else s }),
      PointN(upperPt.coords.zip(splitPt.coords).zip(replaceUpper).map { case ((u, s), p) => if (p) s else u }))
  }

  case class Hypercuboid(
      lowerLeftPoint: PointN = PointN(),
      upperRightPoint: PointN = PointN(List(1d))
  ) extends Ordered[Hypercuboid] {

    def compare(that: Hypercuboid): Int = (lowerLeftPoint, upperRightPoint).compare(that.lowerLeftPoint, that.upperRightPoint)

    val numDim: Int = lowerLeftPoint.numDim
    require(0 < numDim, s"(Hypercuboid) lowerLeftPoint must have at least 1 coordinate but lowerLeftPoint = $lowerLeftPoint")
    require(
      numDim == upperRightPoint.numDim,
      s"(Hypercuboid) lowerLeftPoint and upperRightPoint must have the same number of dimensions" +
        s" but lowerLeftPoint = $lowerLeftPoint ; upperRightPoint = $upperRightPoint"
    )

    def isPtInside(pt: PointN): Boolean = lowerLeftPoint.coords.zip(pt.coords).forall { case (x, y) => x < y } &&
      upperRightPoint.coords.zip(pt.coords).forall { case (x, y) => x > y }

    def ranges: List[Double] = lowerLeftPoint.coords.zip(upperRightPoint.coords)
      .flatMap { case (from, to) => List(from, to) }
      .toList

    def split(splitPoints: Seq[PointN]): List[Hypercuboid] = {
      require(splitPoints.forall(_.numDim == numDim),
        s"(Hypercuboid.split) All splitPoints must have the same number of dimensions as hc: hc.numDim = $numDim !")
      val splitPtsInside = splitPoints.filter(isPtInside).distinct.sorted
      debugLog(s"(Hypercuboid.split) this = $this ; ${splitPoints.length} splitPoints ;" +
        s" ${splitPtsInside.length} splitPtsInside: ${splitPtsInside.mkString(", ")}")
      if (splitPtsInside.isEmpty) List(this)
      else {
        debugLog(s"(Hypercuboid.split) splitPtsInside.head = ${splitPtsInside.head}")
        val iter =
          new scala.collection.immutable.NumericRange.Exclusive[Int](start = 0, end = math.pow(2, numDim).toInt, step = 1)
        iter.map(_.toBinaryString.view.reverse.padTo(numDim, '0').mkString.reverse.map('1' == _))
          .map(getCorners(lowerLeftPoint, upperRightPoint, splitPtsInside.head)(_))
          .map(Hypercuboid.apply)
          .toList
          .flatMap(_.split(splitPtsInside.tail))
          .sorted
      }
    }

  }

  object Hypercuboid {
    def apply(llUr: (PointN, PointN)): Hypercuboid = Hypercuboid(lowerLeftPoint = llUr._1, upperRightPoint = llUr._2)

    def unit(numDim: Int): Hypercuboid = Hypercuboid(
      lowerLeftPoint = PointN(List.fill(numDim)(0d)),
      upperRightPoint = PointN(List.fill(numDim)(1d))
    )

  }

  def hypercuboids2dataFrame(valueCol: Column = lit("A").as("value"))(hcs: Iterable[Hypercuboid])(implicit
      mrqc: MultivarRangeQueryConfig[Double, _]
  ): DataFrame = {
    require(hcs.forall(_.numDim == mrqc.numDimensions),
      s"(hypercuboids2dataFrame) All hypercuboids must have the dimension of mrqc = $mrqc")
    logger.info(s"(hypercuboids2dataFrame) mrqc.numDimensions = ${mrqc.numDimensions} ; ${hcs.size} hypercuboids given")
    val dimColNames: List[(String, String)] = mrqc.rangeDimensions.map(d => (d.fromColName, d.toColName))
    val dfRanges = hcs.map(hc => hc.ranges).toList.toDF("ranges")
    val dimCols: Seq[Column] = mrqc.iter.flatMap { i =>
      List(get(col("ranges"), lit(2 * i)).as(dimColNames(i)._1),
        get(col("ranges"), lit(2 * i + 1)).as(dimColNames(i)._2))
    }
    dfRanges.select(dimCols :+ valueCol: _*)
  }

  def getHyperdimDataFrame(
      valueCol: Column,
      maxNumSplitCoords: Int
  )(xs: Iterable[Double]): (DataFrame, GenericHalfOpenIntervalQueryConfig) = {
    val splitPts = getPointsFromDoubles(xs.take(maxNumSplitCoords))
    require(splitPts.nonEmpty, s"(getHyperdimDataFrame) At least one split point needed but splitPts = $splitPts")
    val dims = splitPts.map(_.numDim).distinct
    logger.info(s"(getHyperdimDataFrame) ${splitPts.length} splitPts of dimensions ${dims.mkString(",")}:" +
      s" ${splitPts.take(2).mkString(", ")}, ...")
    require(dims.length == 1, s"(getHyperdimDataFrame) All split points must have the same dimension")
    val mrqc = GenericHalfOpenIntervalQueryConfig.withDefaultIntervalDef(numDim = dims.head)
    logger.info(s"(getHyperdimDataFrame) maxNumSplitCoords = $maxNumSplitCoords ; valueCol = $valueCol ; mrqc = $mrqc")
    (hypercuboids2dataFrame(valueCol)(hcs = Hypercuboid.unit(mrqc.numDimensions).split(splitPts))(mrqc),
      mrqc)
  }

  def generateHyperdimDataFrames(
      valueCol: Column = lit("A").as("value"),
      maxNumSplitCoords: Int = 22
  ): Gen[(DataFrame, GenericHalfOpenIntervalQueryConfig)] =
    Gen.nonEmptyListOf(g = unitDouble).map(getHyperdimDataFrame(valueCol, maxNumSplitCoords))

}
