package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.util.{finisTemporisString, initiumTemporisString}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, nonEmptyListOf}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

trait Generators extends TestUtils {
  import session.implicits._

  /////////////////////////////////////////////////////////////////////
  ///// generator of data frames with 2 dimensions of type Double /////
  /////////////////////////////////////////////////////////////////////
  val testUnitInterval: Gen[Double] = choose[Double](min = 0.1, max = 0.9)

  case class Point(x: Double = 0d, y: Double = 0d) extends Ordered[Point] with Serializable {
    def compare(that: Point): Int = this.x.compare(that.x) match {
      case 0 => this.y.compare(that.y)
      case n => n
    }
  }

  case class Rectangle(lowerLeftPoint: Point = Point(), upperRightPoint: Point = Point(x = 1d, y = 1d)) {
    def isPtInside(pt: Point): Boolean = lowerLeftPoint.x < pt.x && lowerLeftPoint.y < pt.y &&
      pt.x < upperRightPoint.x && pt.y < upperRightPoint.y
  }

  val testUnitPoint2: Gen[Point] = Gen
    .zip[Double, Double](g1 = testUnitInterval, g2 = testUnitInterval).map { case (x, y) => Point(x, y) }
  val testUnitPoint2s: Gen[List[Point]] = nonEmptyListOf(g = testUnitPoint2)

  def splitRectangle(rect: Rectangle = Rectangle())(splitPoints: Seq[Point]): List[Rectangle] = {
    val splitPtsInside = splitPoints.filter(rect.isPtInside).sorted
    if (splitPtsInside.isEmpty) List(rect)
    else {
      val splitPt = splitPtsInside.head
      splitRectangle(Rectangle(rect.lowerLeftPoint, splitPt))(splitPtsInside.tail) ++
        splitRectangle(Rectangle(Point(rect.lowerLeftPoint.x, splitPt.y), Point(splitPt.x, rect.upperRightPoint.y)))(splitPtsInside.tail) ++
        splitRectangle(Rectangle(Point(splitPt.x, rect.lowerLeftPoint.y), Point(rect.upperRightPoint.x, splitPt.y)))(splitPtsInside.tail) ++
        splitRectangle(Rectangle(splitPt, rect.upperRightPoint))(splitPtsInside.tail)
    }
  }

  def rectangleValue2RowTuple[T](value: T)(rect: Rectangle): (Double, Double, Double, Double, T) =
    (rect.lowerLeftPoint.x, rect.upperRightPoint.x, rect.lowerLeftPoint.y, rect.upperRightPoint.y, value)

  /**
   * generates data frames with two dense dimensions of which the domain is the unit square with
   * constant string value
   *
   * @param value
   *   the constant string value
   * @return
   *   dataFrame with several entries with constant value
   */
  def dfConstantUnitSplitted(value: String): Gen[DataFrame] = testUnitPoint2s.map(splitRectangle())
    .map[List[(Double, Double, Double, Double, String)]](rects => rects.map(rectangleValue2RowTuple(value)))
    .map(_.toDF("x_from", "x_to", "y_from", "y_to", "value"))

}
