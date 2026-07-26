package ch.zzeekk.spark.temporalquery.util.bilinear

import BiTemporalTestUtils._
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.timestampOrdering
import ch.zzeekk.spark.temporalquery.{saveString2File, TestUtils}
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class BiTemporalClosedIntervalQueryUtilTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering
  logger.info(s"BiTemporalQueryUtilTest: defaultBiTemporalConfig = $defaultBiTemporalConfig")
  private val fromCols: List[Column] = defaultBiTemporalConfig.intervalDimensions.map(_.fromCol)

  "multivarRangeCleanupExtend and multivarRangeCombine" should "extend and combine dfLeft" in {
    val actual = dfLeft
      .multivarRangeCleanupExtend(keys = Seq("id"), rnkExpressions = fromCols)
      .multivarRangeCombine()
      .orderBy(fromCols: _*)
    val expected = List(
      (0, initiumTemporisString, finisTemporisString, initiumTemporisString, "2017-12-09 23:59:59.999", None),
      (0, initiumTemporisString, finisTemporisString, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", Some(4.2)),
      (0, initiumTemporisString, finisTemporisString, "2018-12-09 00:00:00", finisTemporisString,       None)
    ).map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")
      .withColumn(defaultBiTemporalConfig.definedColName, $"value_l".isNotNull)
    val result = dfEqual(reorderCols(actual, expected), expected)
    if (!result) printFailedTestResult("multivarRangeCleanupExtend", dfLeft)(reorderCols(actual, expected), expected)
    result shouldBe true
  }

  "multivarRangeCleanupExtend and multivarRangeCombine" should
    "combine ranges while removing overlaps of dfRight" +
    " without extending or filling gaps" in {
      val actual = dfRight.multivarRangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        extend = false,
        fillGapsWithNull = false
      ).multivarRangeCombine()
      val expected = List(
        (0, initiumTemporisString,     finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15)),
        (0, "2018-01-01 00:00:00",     "2018-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
        (0, "2018-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
        (0, "2020-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
        (1, initiumTemporisString,     finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
      ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
        .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("multivarRangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "multivarRangeCleanupExtend and multivarRangeCombine" should
    "combine ranges while removing overlaps" +
    " and filling gaps of dfRight without extending" in {
      val actual = dfRight.multivarRangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        extend = false
      ).multivarRangeCombine()
      val expected = List(
        (0, initiumTemporisString,     finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15),  true),
        (0, "2018-01-01 00:00:00",     "2018-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15),  true),
        (0, "2018-01-01 00:00:00",     finisTemporisString,   "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", None,         false),
        (0, "2018-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00),  true),
        (0, "2020-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15),  true),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None,         true),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0), true),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0), true),
        (1, initiumTemporisString,     finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None,         true)
      ).map(makeRowsBiTemporalDefined).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r",
        defaultBiTemporalConfig.definedColName)
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("multivarRangeCleanupExtend_dfRight_noExtend_fillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "multivarRangeCleanupExtend and multivarRangeCombine" should
    "combine ranges while removing overlaps of dfRight without extending or filling gaps" +
    " since extend is ignore if not(fillGapsWithNull)" in {
      val actual = dfRight.multivarRangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        fillGapsWithNull = false
      ).multivarRangeCombine()
      val expected = List(
        (0, initiumTemporisString,     finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15)),
        (0, "2018-01-01 00:00:00",     "2018-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
        (0, "2018-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
        (0, "2020-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
        (1, initiumTemporisString,     finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
      ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
        .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("multivarRangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "multivarRangeCleanupExtend and multivarRangeCombine" should
    "combine, extend ranges, fill gaps" +
    " and remove overlaps of dfRight" in {
      val actual = dfRight.multivarRangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols
      ).multivarRangeCombine()
      val expected = List(
        (0, initiumTemporisString,     "2017-12-31 23:59:59.999", initiumTemporisString, "2018-06-01 05:24:10.999", None,         false),
        (0, initiumTemporisString,     "2020-05-31 23:59:59.999", "2020-01-01 00:00:00", finisTemporisString,       None,         false),
        (0, initiumTemporisString,     finisTemporisString,       "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15),  true),
        (0, "2018-01-01 00:00:00",     "2018-06-01 00:00:00",     "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15),  true),
        (0, "2018-01-01 00:00:00",     finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None,         false),
        (0, "2018-01-01 00:00:00",     finisTemporisString,       "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", None,         false),
        (0, "2018-06-01 00:00:00.001", finisTemporisString,       "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00),  true),
        (0, "2020-06-01 00:00:00",     finisTemporisString,       "2020-01-01 00:00:00", finisTemporisString,       Some(97.15),  true),
        (1, initiumTemporisString,     finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None,         false),
        (1, initiumTemporisString,     finisTemporisString,       "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None,         true),
        (1, initiumTemporisString,     finisTemporisString,       "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0), true),
        (1, initiumTemporisString,     finisTemporisString,       "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0), true),
        (1, initiumTemporisString,     finisTemporisString,       "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None,         true),
        (1, initiumTemporisString,     finisTemporisString,       "2100-01-01 00:00:00", finisTemporisString,       None,         false)
      ).map(makeRowsBiTemporalDefined)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r", defaultBiTemporalConfig.definedColName)
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("multivarRangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "multivarRangeCleanupExtend and multivarRangeCombine" should
    "combine, extend ranges, fill gaps and remove overlaps of dfMap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMap.multivarRangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"img"))
        .multivarRangeCombine()
      val expected = List(
        (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
        (0, initiumTemporisString, "2018-02-04 23:59:59.999", "2018-03-01 00:00:00", finisTemporisString,       None),
        (0, initiumTemporisString, finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None),
        (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some("A")),
        (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
        (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-03-04 00:00:00", finisTemporisString,       None),
        (0, "2018-02-05 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00", "2018-03-03 23:59:59.999", Some("C")),
        (0, "2018-02-20 00:00:00", "2018-03-15 23:59:59.999", "2018-03-04 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
        (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-03-01 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
        (0, "2018-02-20 00:00:00", finisTemporisString,       "2018-04-01 00:00:00", finisTemporisString,       None)
      ).map(makeRowsBiTemporal)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
        .withColumn(defaultBiTemporalConfig.definedColName, $"img".isNotNull)
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("multivarRangeCleanupExtend_dfMap", dfMap)(actual, expected)
      result shouldBe true
    }

  "multivarRangeContinuous2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfContinuousTime.multivarRangeContinuous2discrete
    val expected = Seq(
      (0, "2019-01-01 08:00:00", "2019-03-14 23:59:59.999", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2019-03-15 00:00:00", finisTemporisString,       "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, initiumTemporisString, finisTemporisString,       "2019-02-01 02:34:56.124", "2019-02-01 02:34:56.124", 42.0),
      (0, "2019-03-25 14:00:00", finisTemporisString,       "2019-02-01 02:34:56.125", "2019-03-02 23:59:59.999", 13.0),
      (0, "2019-03-10 12:00:00", finisTemporisString,       "2019-03-03 00:00:00",     "2019-04-03 23:59:59.999", 12.0),
      (0, "2020-06-01 00:00:00", finisTemporisString,       "2020-01-01 01:00:00",     finisTemporisString,       18.17),
      (1, initiumTemporisString, finisTemporisString,       "2019-01-01 00:00:00.124", "2019-02-01 23:59:59.999", -1.0),
      (1, "2020-01-15 09:00:00", finisTemporisString,       "2019-03-03 01:00:00",     "2021-12-01 02:34:56.099", -2.0)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeContinuous2discrete", Seq(dfContinuousTime))(actual, expected)
    result shouldBe true
  }

  "multivarRangeRoundDiscreteTime" should "not modify dfLeft" in {
    val actual = dfLeft.multivarRangeRoundDiscreteTime
    val expected = dfLeft
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeRoundDiscreteTime", Seq(dfRight))(actual, expected)
    result shouldBe true
  }

  "multivarRangeRoundDiscreteTime" should "round timestamps of dfDirtyTimeRanges" in {
    val actual = dfDirtyTimeRanges.multivarRangeRoundDiscreteTime
    val rowsExpected = Seq(
      (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2020-01-05 12:34:56.124", "2020-02-01 02:34:56.123", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, "2020-02-01 01:00:0",      "2020-02-01 02:34:56.124", "2019-02-01 01:00:0",      "2019-02-01 02:34:56.124", 2.72),
      (0, "2020-02-01 02:34:56.125", "2020-03-03 00:00:0",      "2019-02-01 02:34:56.125", "2019-03-03 00:00:0",      13.0),
      (0, "2020-03-03 00:00:0",      "2020-04-04 00:00:0",      "2019-03-03 00:00:0",      "2019-04-04 00:00:0",      13.0),
      (0, "2021-01-01 01:00:0",      finisTemporisString,       "2020-01-01 01:00:0",      finisTemporisString,       18.17),
      (1, "2020-03-01 00:00:0",      "2020-03-01 00:00:0",      "2019-03-01 00:00:0",      "2019-03-01 00:00:0",
        0.1), // duration extended to 1 millisecond
      (1, "2020-03-01 00:00:1", "2020-03-01 00:00:1", "2019-03-01 00:00:0.001", "2019-03-01 00:00:0.001",
        0.1), // duration extended to 1 millisecond
      (1, "2020-03-01 00:00:1.001", "2020-03-01 00:00:01.002", "2019-03-01 00:00:1.001", "2019-03-01 00:00:01.002",
        1.2), // duration extended to 2 milliseconds
      (1, "2020-01-01 00:00:0.124", "2020-02-02 00:00:0",    "2019-01-01 00:00:0.124", "2019-02-02 00:00:0",    -1.0),
      (1, "2020-03-03 01:00:0",     "2022-12-01 02:34:56.1", "2019-03-03 01:00:0",     "2021-12-01 02:34:56.1", -2.0)
    )
    val expected = rowsExpected.map(makeRowsBiTemporal[Int, Double])
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeRoundDiscreteTime", Seq(dfDirtyTimeRanges))(actual, expected)
    result shouldBe true
  }

  "multivarRangeUnifyRanges" should "not modify dfMoment as extend and fillGapsWithNull are false" in {
    val actual = dfMoment.multivarRangeUnifyRanges(keys = Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "multivarRangeUnifyRanges" should "extend dfMoment" in {
    val actual = dfMoment.multivarRangeUnifyRanges(keys = Seq("id"), extend = true, fillGapsWithNull = true)
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = List(
      (0, initiumTemporisString,     "2019-11-30 23:59:59.999", initiumTemporisString,     finisTemporisString,       None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     initiumTemporisString,     "2019-11-25 11:12:13.004", None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", Some("A")),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.006", finisTemporisString,       None),
      (0, "2019-12-01 00:00:00.001", finisTemporisString,       initiumTemporisString,     finisTemporisString,       None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "multivarRangeCombine" should "combine everything possible" in {
    val argument = Seq(
      (0, initiumTemporisString, "2017-12-31 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-03-01 00:00:00",     finisTemporisString,       None),
      (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-03-01 00:00:00",     finisTemporisString,       None),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", Some("C")),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-03-04 00:00:00",     finisTemporisString,       None),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", Some("B")),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", Some("C")),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-04-01 00:00:00",     finisTemporisString,       None),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", Some("B")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-25 14:15:16.122", Some("B")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", Some("B")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", Some("C")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-04-01 00:00:00",     finisTemporisString,       None),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", Some("B")),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", Some("C")),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-04-01 00:00:00",     finisTemporisString,       None),
      (0, "2018-03-16 00:00:00", finisTemporisString,       initiumTemporisString,     "2017-12-31 23:59:59.999", None),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", Some("B")),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-03-01 00:00:00",     "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-04-01 00:00:00",     finisTemporisString,       None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val actual = argument.multivarRangeCombine()
    val expected = Seq(
      (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
      (0, initiumTemporisString, "2018-02-04 23:59:59.999", "2018-03-01 00:00:00", finisTemporisString,       None),
      (0, initiumTemporisString, finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None),
      (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some("A")),
      (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
      (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-03-04 00:00:00", finisTemporisString,       None),
      (0, "2018-02-05 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00", "2018-03-03 23:59:59.999", Some("C")),
      (0, "2018-02-20 00:00:00", "2018-03-15 23:59:59.999", "2018-03-04 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-02-20 00:00:00", finisTemporisString,       "2018-04-01 00:00:00", finisTemporisString,       None),
      (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-03-01 00:00:00", "2018-03-31 23:59:59.999", Some("D"))
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)
    if (!result) {
      logger.error(s"!!! Test failed !!! Saving dataFrames actual and expected as SVG to files in repository root.")
      saveString2File("argument.svg")(argument.toSvg("img"))
      saveString2File("actual.svg")(actual.toSvg("img"))
      saveString2File("expected.svg")(expected.toSvg("img"))
      printFailedTestResult("multivarRangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    }
    result shouldBe true
  }

}
