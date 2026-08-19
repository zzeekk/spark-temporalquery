package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.util.{finisTemporisString, initiumTemporisString}
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.StructType
import org.slf4j.{Logger, LoggerFactory}

import java.sql.{Date, Timestamp}
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

  def schemaEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    def asNullable(schema: StructType): StructType = StructType(schema.map(_.copy(nullable = true)))
    asNullable(df1.schema) == asNullable(df2.schema)
  }

  def dfEqual(df1: DataFrame, df2: DataFrame): Boolean = Try {
    val df1reordered = reorderCols(df1, df2)
    debugLog("(dfEqual) symmetricDifference ignores duplicates, so compare cardinalities as well")
    (0 == symmetricDifference(df1reordered, df2).count) && (df1reordered.count == df2.count) && schemaEqual(df1reordered, df2)
  } match {
    case Success(p) => p
    case Failure(e) =>
      logger.error("(dfEqual) Comparison of df1 and df2 failed !!!")
      logger.error(s"(dfEqual) df1.schema = ${df1.schema.catalogString}")
      logger.error(s"(dfEqual) df2.schema = ${df2.schema.catalogString}")
      throw e
  }

  def printFailedTestResult[T](testName: String, arguments: Seq[DataFrame])(actual: DataFrame, expected: DataFrame)(implicit
      logger: Logger
  ): Unit = {
    def printDf(df: DataFrame): Unit = {
      logger.error(df.schema.simpleString)
      Try(df.orderBy(df.columns.map(col): _*).show(false)) match {
        case Success(_) =>
        case Failure(_) =>
          logger.error(s"(printFailedTestResult.printDf) Cannot show ordered df!" +
            s" df.schema = ${df.schema.catalogString}")
          logger.error(s"(printFailedTestResult.printDf) Showing df unordered!")
          df.show(false)
      }
    }

    val actualReordered = reorderCols(actual, expected)

    logger.error(s"!!!! Test $testName Failed !!!")
    logger.error(s"   ${arguments.length} Arguments ; counts: ${arguments.map(_.count()).mkString(", ")}  ")
    arguments.foreach(printDf)
    logger.error("   Actual ")
    logger.error(s"  actual.count() =  ${actualReordered.count()}")
    printDf(actualReordered)
    logger.error("   Expected ")
    logger.error(s"  expected.count() =  ${expected.count()}")
    printDf(expected)
    logger.error(s"  schemata equal =  ${schemaEqual(actualReordered, expected)}")
    if (schemaEqual(actualReordered, expected)) Try {
      val dfSymDiff = symmetricDifference(actualReordered, expected)
        .select(when($"_in_first_df", "actual").otherwise("expected").as("_df") +: actual.columns.map(col): _*)
      logger.error(s"   symmetric Difference, dfSymDiff.count = ${dfSymDiff.count()} ")
      printDf(dfSymDiff)
    } match {
      case Success(_) =>
      case Failure(e) =>
        logger.error(s"(printFailedTestResult) Cannot show symmetric difference!" +
          s" You need to find the differences on your own!!!")
        logger.error(s"(printFailedTestResult) ${e.getMessage}")
    }
    else {
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

  // helper: (id, known_from, known_to, valid_from, valid_to, value)
  def makeRowsBiTemporal[A, B](row: (A, String, String, String, String, B)): (A, Timestamp, Timestamp, Timestamp, Timestamp, B) =
    (row._1, Timestamp.valueOf(row._2), Timestamp.valueOf(row._3), Timestamp.valueOf(row._4), Timestamp.valueOf(row._5), row._6)

  def makeRowsBiDatoral[A, B](row: (A, String, String, String, String, B)): (A, Date, Date, Date, Date, B) =
    (row._1, Date.valueOf(row._2), Date.valueOf(row._3), Date.valueOf(row._4), Date.valueOf(row._5), row._6)

  val dfDenseTime: DataFrame = List(
    // entity 0, Jan 1–5: original entry on day 1, corrected on Mar 15 (known_to marks the correction)
    (0, "2019-01-01 08:00:00", "2019-03-15 00:00:00", "2019-01-01 00:00:00.123456789", "2019-01-05 12:34:56.123456789", 3.14),
    // entity 0, Jan 5–Feb 1: corrected record, known from the correction date onward
    (0, "2019-03-15 00:00:00", finisTemporisString, "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235", 2.72),
    // entity 0, 1ms pulse on Feb 1: always known
    (0, initiumTemporisString, finisTemporisString, "2019-02-01 02:34:56.1235", "2019-02-01 02:34:56.1245", 42.0),
    // entity 0, Feb–Mar: late addition — fact recorded 7 weeks after validity started
    (0, "2019-03-25 14:00:00", finisTemporisString, "2019-02-01 02:34:56.1245", "2019-03-03 00:00:00", 13.0),
    // entity 0, Mar–Apr: entered one week into the valid period
    (0, "2019-03-10 12:00:00", finisTemporisString, "2019-03-03 00:00:00", "2019-04-04 00:00:00", 12.0),
    // entity 0, Sep blip: known only for the same nanosecond window as validity (momentary knowledge)
    (0, "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239", "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239", 42.0),
    // entity 0, 2020+: entered 5 months after validity started
    (0, "2020-06-01 00:00:00", finisTemporisString, "2020-01-01 01:00:00", "9999-12-31 23:59:59.999999999", 18.17),
    // entity 1, Jan–Feb: always known
    (1, initiumTemporisString, finisTemporisString, "2019-01-01 00:00:00.123456789", "2019-02-02 00:00:00", -1.0),
    // entity 1, Mar 2019–Dec 2021: entered retroactively in Jan 2020
    (1, "2020-01-15 09:00:00", finisTemporisString, "2019-03-03 01:00:00", "2021-12-01 02:34:56.1", -2.0)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

}
