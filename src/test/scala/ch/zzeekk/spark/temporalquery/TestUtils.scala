package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.multivarRange.MultivarRangeQueryConfig
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}
import org.scalacheck.Gen
import org.scalacheck.Gen.{choose, nonEmptyListOf}
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import scala.util.{Failure, Success, Try}

trait TestUtils extends Logging {

  override protected implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
  protected implicit val session: SparkSession = Try(SparkSession.builder
      .config("spark.port.maxRetries", 100)
      .config("spark.ui.enabled", value = false)
      .config("spark.sql.shuffle.partitions", 1)
      .config("spark.task.maxFailures", 1)
      .master("local").appName("TemporalQueryUtilTest").getOrCreate()) match {
    case Success(ss) => ss
    case Failure(e)  =>
      logger.error(s"Failed to build a Spark Session!")
      throw e
  }

  import session.implicits._

  loggEnv

  def symmetricDifference(df1: DataFrame, df2: DataFrame): DataFrame = {
    // attention, "except" works on Dataset and not on DataFrame. We need to check that schema is equal.
    require(
      df1.columns.toSeq == df2.columns.toSeq,
      s"""Cannot calculate symmetric difference for DataFrames with different schema.
         |schema of df1: ${df1.columns.toSeq.mkString(",")}
         |${df1.schema.treeString}
         |schema of df2: ${df2.columns.toSeq.mkString(",")}
         |${df2.schema.treeString}
         |""".stripMargin
    )
    df1.except(df2).withColumn("_in_first_df", lit(true))
      .union(df2.except(df1).withColumn("_row_in_first_df", lit(false)))
  }

  def reorderCols(dfToReorder: DataFrame, dfRef: DataFrame): DataFrame = {
    require(
      dfRef.columns.toSet == dfToReorder.columns.toSet,
      s"""Cannot reorder columns for DataFrames with different columns.
         |columns of dfRef: ${dfRef.columns.toSeq.mkString(",")}
         |columns of dfToReorder: ${dfToReorder.columns.toSeq.mkString(",")}
         |""".stripMargin
    )
    if (dfRef.columns.toSet.size < dfRef.columns.length)
      dfToReorder // cannot reorder DataFrames with schemas that have duplicate column names
    else dfToReorder.select(dfRef.columns.map(col): _*)
  }

  def schemaEqual(df1: DataFrame, df2: DataFrame): Boolean =
    df1.schema.sql == df2.schema.sql // ignore nullability in comparison

  def dfEqual(df1: DataFrame, df2: DataFrame): Boolean = Try {
    val df1reordered = reorderCols(df1, df2)
    // symmetricDifference ignores duplicates, so compare cardinalities as well
    (0 == symmetricDifference(df1reordered, df2).count) && (df1reordered.count == df2.count) && schemaEqual(df1reordered, df2)
  } match {
    case Success(p) => p
    case Failure(e) =>
      logger.error("!!! dfEqual: Comparison of df1 and df2 failed !!!")
      throw e
  }

  def printFailedTestResult[T](testName: String, arguments: Seq[DataFrame])(actual: DataFrame, expected: DataFrame)(implicit
      logger: Logger
  ): Unit = {
    def printDf(df: DataFrame): Unit = {
      logger.error(df.schema.simpleString)
      df.orderBy(df.columns.map(col): _*).show(false)
    }

    val actualReordered = reorderCols(actual, expected)

    logger.error(s"!!!! Test $testName Failed !!!")
    logger.error("   Arguments ")
    arguments.foreach(printDf)
    logger.error("   Actual ")
    logger.error(s"  actual.count() =  ${actualReordered.count()}")
    printDf(actualReordered)
    logger.error("   Expected ")
    logger.error(s"  expected.count() =  ${expected.count()}")
    printDf(expected)
    logger.error(s"  schemata equal =  ${schemaEqual(actualReordered, expected)}")
    if (schemaEqual(actualReordered, expected)) {
      val dfSymDiff = symmetricDifference(actualReordered, expected)
        .select(when($"_in_first_df", "actual").otherwise("expected").as("_df") +: actual.columns.map(col): _*)
      logger.error(s"   symmetric Difference, dfSymDiff.count = ${dfSymDiff.count()} ")
      printDf(dfSymDiff)
    } else {
      logger.error(s"actual.schema:${actualReordered.schema.treeString}")
      logger.error(s"expected.schema:${expected.schema.treeString}")
    }

  }

  def printFailedTestResult[T](testName: String, argument: DataFrame)(actual: DataFrame, expected: DataFrame)(implicit
      logger: Logger
  ): Unit =
    printFailedTestResult(testName, Seq(argument))(actual, expected)

  def testArgumentExpectedMapWithComment[K, V](experiendum: K => V, argExpMapComm: Map[(String, K), V]): Set[Boolean] = {
    def logFailure(argument: K, actual: V, expected: V, comment: String): Unit = {
      logger.error("Test case failed !")
      logger.error(s"   argument = $argument")
      logger.error(s"   actual   = $actual")
      logger.error(s"   expected = $expected")
      logger.error(s"   comment  = $comment")
    }

    def checkKey(x: (String, K)): Boolean = x match {
      case (comment, argument) =>
        val actual = Try(experiendum(argument)) match {
          case Success(v) => v
          case Failure(e) =>
            logger.error(s"testArgumentExpectedMapWithComment.checkKey: execution of experiendum($argument) failed!")
            throw e
        }
        val expected = argExpMapComm(x)
        val result = actual == expected
        if (!result) logFailure(argument, actual, expected, comment)
        result
      case _ => throw new Exception(s"Something went wrong: checkKey called with parameter x=$x")
    }

    argExpMapComm.keySet.map(checkKey)
  }

  def testArgumentExpectedMap[K, V](experiendum: K => V, argExpMap: Map[K, V]): Set[Boolean] = {
    def addEmptyComment(x: (K, V)): ((String, K), V) = x match {
      case (k, v) => (("", k), v)
    }

    val argExpMapWithReason: Map[(String, K), V] = argExpMap.map(addEmptyComment)
    testArgumentExpectedMapWithComment(experiendum, argExpMapWithReason)
  }

  def makeRowsWithTimeRange[A, B](row: (A, String, String, B)): (A, Timestamp, Timestamp, B) =
    (row._1, Timestamp.valueOf(row._2), Timestamp.valueOf(row._3), row._4)

  def makeRowsWithTimeRangeEnd[A, B](row: (A, B, String, String)): (A, B, Timestamp, Timestamp) =
    (row._1, row._2, Timestamp.valueOf(row._3), Timestamp.valueOf(row._4))

  def makeRowsWithTimeRangeEnd[A, B, C](row: (A, B, C, String, String)): (A, B, C, Timestamp, Timestamp) =
    (row._1, row._2, row._3, Timestamp.valueOf(row._4), Timestamp.valueOf(row._5))

  def makeRowsWithTimeRangeEnd[A, B, C, D](row: (A, B, C, D, String, String)): (A, B, C, D, Timestamp, Timestamp) =
    (row._1, row._2, row._3, row._4, Timestamp.valueOf(row._5), Timestamp.valueOf(row._6))

  /////////////////////////////////////////////////
  ///// generators for property based testing /////
  /////////////////////////////////////////////////

  val testUnitInterval: Gen[Double] = choose[Double](min = 0.1, max = 0.9)

  ///// generator of data frames with 2 dimensions of type Double /////

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
