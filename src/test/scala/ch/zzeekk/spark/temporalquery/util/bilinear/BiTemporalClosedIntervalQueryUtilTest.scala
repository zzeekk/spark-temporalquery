package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalTestUtils._
import ch.zzeekk.spark.temporalquery.util.timestampOrdering
import ch.zzeekk.spark.temporalquery.{saveString2File, udf_durationInMillis, TestUtils}
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

  "rangeCleanupExtend and rangeCombine" should "extend and combine dfLeft" in {
    val actual = dfLeft
      .rangeCleanupExtend(keys = Seq("id"), rnkExpressions = fromCols)
      .rangeCombine()
      .orderBy(fromCols: _*)
    val expected = List(
      (0, initiumTemporisString, finisTemporisString, initiumTemporisString, "2017-12-09 23:59:59.999", None),
      (0, initiumTemporisString, finisTemporisString, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", Some(4.2)),
      (0, initiumTemporisString, finisTemporisString, "2018-12-09 00:00:00", finisTemporisString,       None)
    ).map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")
      .withColumn(defaultBiTemporalConfig.definedColName, $"value_l".isNotNull)
    val result = dfEqual(reorderCols(actual, expected), expected)
    if (!result) printFailedTestResult("rangeCleanupExtend", dfLeft)(reorderCols(actual, expected), expected)
    result shouldBe true
  }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges while removing overlaps of dfRight" +
    " without extending or filling gaps" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        extend = false,
        fillGapsWithNull = false
      ).rangeCombine()
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
      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges while removing overlaps" +
    " and filling gaps of dfRight without extending" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        extend = false
      ).rangeCombine()
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
      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_noExtend_fillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges while removing overlaps of dfRight without extending or filling gaps" +
    " since extend is ignore if not(fillGapsWithNull)" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols,
        fillGapsWithNull = false
      ).rangeCombine()
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
      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps" +
    " and remove overlaps of dfRight" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = fromCols
      ).rangeCombine()
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
      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps and remove overlaps of dfMap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMap.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"img"))
        .rangeCombine()
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
      if (!result) printFailedTestResult("rangeCleanupExtend_dfMap", dfMap)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend_dfMap_NoExtendFillgaps" should
    "combine and remove overlaps of dfMap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMap.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"img"),
        extend = false, fillGapsWithNull = false)
        .rangeCombine()
      val expected = List(
        (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
        (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some("A")),
        (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", Some("B")),
        (0, "2018-02-05 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00", "2018-03-03 23:59:59.999", Some("C")),
        (0, "2018-02-20 00:00:00", "2018-03-15 23:59:59.999", "2018-03-04 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
        (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-03-01 00:00:00", "2018-03-31 23:59:59.999", Some("D"))
      ).map(makeRowsBiTemporal)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
        .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCleanupExtend_dfMap_NoExtendFillgaps", dfMap)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps and remove overlaps of dfMsOverlap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMsOverlap.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"valid_from"))
        .rangeCombine()
      val expected = List(
        (0, initiumTemporisString,     "2018-12-31 23:59:59.999", initiumTemporisString,     finisTemporisString,       None),
        (0, "2019-01-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     Some("A")),
        (0, "2019-01-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", Some("B")),
        (0, "2019-01-01 00:00:00",     finisTemporisString,       initiumTemporisString,     "2018-12-31 23:59:59.999", None),
        (0, "2019-01-01 00:00:00",     finisTemporisString,       "2019-01-02 00:00:00",     finisTemporisString,       None),
        (0, "2019-01-01 00:00:00",     finisTemporisString,       "2019-01-01 00:00:00",     "2019-01-01 09:59:59.999", Some("A")),
        (0, "2019-02-01 00:00:00.001", finisTemporisString,       "2019-01-01 10:00:00",     "2019-01-01 23:59:59.999", Some("B"))
      ).map(makeRowsBiTemporal)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
        .withColumn(defaultBiTemporalConfig.definedColName, $"img".isNotNull)
      val result = dfEqual(actual, expected)
      if (!result) {
        saveString2File(fileName = "dfMsOverlap.svg")(str = dfMsOverlap.drop("id").toSvg("img"))
        saveString2File(fileName = "actual.svg")(str = actual.drop("id").toSvg("img"))
        saveString2File(fileName = "expected.svg")(str = expected.drop("id").toSvg("img"))
        printFailedTestResult("rangeCleanupExtend_dfMsOverlapAK", dfMsOverlap)(actual, expected)
      }
      result shouldBe true
    }

  "rangeCleanupExtend_dfDirtyTimeRangesAK" should "return expected results" in {
    val actual =
      dfDirtyTimeRangesAK.rangeRoundDiscreteTime.rangeCleanupExtend(Seq("id"), Seq($"valid_from", $"value"))
        .rangeCombine()
        .orderBy($"id", $"valid_from")
    val expected = List(
      // id = 0, era 1 [2020-01-01,2021-01-01) is only known to cover Jan 1st 2019 onward: everything
      // before that (incl. all of the never-recorded 2018-and-earlier past) is a gap
      (0, None,       false, initiumTemporisString,   "2019-12-31 23:59:59.999", initiumTemporisString,     finisTemporisString),
      (0, None,       false, "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   initiumTemporisString,     "2019-01-01 00:00:00.123"),
      (0, Some(3.14), true,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, Some(2.72), true,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, Some(13.0), true,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, None,       false, "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999", "2019-04-04 00:00:00.001", finisTemporisString),
      (0, None,       false, "2021-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-04-04 00:00:00.001", "2020-01-01 00:59:59.999"),
      // gap between era 1's known window closing and the 2022 audit / era 2 opening
      (0, None, false, "2021-01-01 00:00:00.001", "2022-02-28 23:59:59.999", initiumTemporisString, "2020-01-01 00:59:59.999"),
      // the 2022 audit: 55.0 wins the 10-day known-axis overlap (same valid_from, smaller value), 66.0
      // only survives from the day after the overlap ends; the audit's known window also contributes
      // its own two gap-fill rows, one on either side of the audited valid instant
      (0, None,       false, "2022-03-01 00:00:00.0", "2022-03-15 23:59:59.999", initiumTemporisString,   "2019-06-30 23:59:59.999"),
      (0, Some(55.0), true,  "2022-03-01 00:00:00.0", "2022-03-10 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999"),
      (0, Some(66.0), true,  "2022-03-11 00:00:00.0", "2022-03-15 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999"),
      (0, None,       false, "2022-03-01 00:00:00.0", "2022-03-15 23:59:59.999", "2019-07-06 00:00:00.0", "2020-01-01 00:59:59.999"),
      // era 2 (late addition): the 18.17 fact, only known from 2021 onward
      (0, Some(18.17), true,  "2021-01-01 00:00:00.0", finisTemporisString, "2020-01-01 01:00:0",  finisTemporisString),
      (0, None,        false, "2022-03-16 00:00:00.0", finisTemporisString, initiumTemporisString, "2020-01-01 00:59:59.999"),
      // id = 1, era 1 [2020-01-01,2020-07-01) known window
      (1, None,       false, initiumTemporisString,   "2019-12-31 23:59:59.999", initiumTemporisString,     finisTemporisString),
      (1, None,       false, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   initiumTemporisString,     "2019-01-01 00:00:00.123"),
      (1, Some(-1.0), true,  "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-01-01 00:00:00.124", "2019-02-02 00:00:0"),
      (1, None,       false, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-02-02 00:00:0.001",  "2019-02-28 23:59:59.999"),
      (1, Some(0.1),  true,  "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:0",      "2019-03-01 00:00:00.001"),
      (1, None,       false, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:00.002", "2019-03-01 00:00:1"),
      (1, Some(1.2),  true,  "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002"),
      (1, None,       false, "2020-01-01 00:00:00.0", "2020-06-30 23:59:59.999", "2019-03-01 00:00:1.003",  finisTemporisString),
      (1, None,       false, "2020-07-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:1.003",  "2019-03-03 00:59:59.999"),
      // era 2 (late addition): the long-running -2.0 fact, only known from mid-2020 onward
      (1, Some(-2.0), true,  "2020-07-01 00:00:00.0",   finisTemporisString, "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1"),
      (1, None,       false, "2020-07-01 00:00:00.001", finisTemporisString, initiumTemporisString,     "2019-03-03 00:59:59.999"),
      (1, None,       false, "2020-07-01 00:00:00.0",   finisTemporisString, "2021-12-01 02:34:56.101", finisTemporisString)
      // note: the id=1 audit row (known_to before known_from) is entirely invalid and correctly absent
    ).map { case (id, v, defined, kf, kt, vf, vt) => (id, kf, kt, vf, vt, v, defined) }
      .map(makeRowsBiTemporalDefined)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value", defaultBiTemporalConfig.definedColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRangesAK", dfDirtyTimeRangesAK)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_dfDirtyTimeRangesAK_NoExtendFillgaps" should "return expected results" in {
    val actual =
      dfDirtyTimeRangesAK.rangeRoundDiscreteTime.rangeCleanupExtend(Seq("id"), Seq($"valid_from", $"value"),
        extend = false, fillGapsWithNull = false)
        .rangeCombine()
        .orderBy($"id", $"valid_from")
    val expected = List(
      (0, 3.14,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, 2.72,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, 13.0,  "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0",   "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, 55.0,  "2022-03-01 00:00:00.0", "2022-03-10 23:59:59.999", "2019-07-01 00:00:00.0",   "2019-07-05 23:59:59.999"),
      (0, 66.0,  "2022-03-11 00:00:00.0", "2022-03-15 23:59:59.999", "2019-07-01 00:00:00.0",   "2019-07-05 23:59:59.999"),
      (0, 18.17, "2021-01-01 00:00:00.0", finisTemporisString,       "2020-01-01 01:00:0",      finisTemporisString),
      (1, -1.0,  "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-01-01 00:00:0.124",  "2019-02-02 00:00:0"),
      (1, 0.1,   "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:0",      "2019-03-01 00:00:00.001"),
      (1, 1.2,   "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002"),
      (1, -2.0,  "2020-07-01 00:00:00.0", finisTemporisString,       "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1")
    ).map { case (id, v, kf, kt, vf, vt) => (id, kf, kt, vf, vt, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
      .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
    val result = dfEqual(actual, expected)

    if (!result)
      printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRangesAK_NoExtendFillgaps", dfDirtyTimeRangesAK)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_validityDuration" should "return expected results" in {
    val argument = Seq(
      (1, "A", "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, "A", "2020-07-05 00:00:00", "2020-07-07 23:59:59.999"),
      (1, "B", "2020-07-01 00:00:00", "2020-07-02 23:59:59.999"),
      (1, "B", "2020-07-04 00:00:00", "2020-07-07 23:59:59.999")
    ).map { case (id, v, from, to) => (id, v, initiumTemporisString, finisTemporisString, from, to) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "val", "known_from", "known_to", "valid_from", "valid_to")
    // we want the record with the longest validity period, i.e. maximal valid_to-valid_from
    val actual =
      argument.rangeCleanupExtend(Seq("id"), Seq(udf_durationInMillis($"valid_to", $"valid_from").desc))
        .rangeCombine()
    val expected = List(
      (1, None,      false, initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, Some("A"), true,  "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, Some("B"), true,  "2020-07-04 00:00:00", "2020-07-07 23:59:59.999"),
      (1, None,      false, "2020-07-08 00:00:00", finisTemporisString)
    ).map(row => (row._1, initiumTemporisString, finisTemporisString, row._4, row._5, row._2, row._3))
      .map(makeRowsBiTemporalDefined)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "val", defaultBiTemporalConfig.definedColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCleanupExtend_validityDuration", argument)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_rankExprFromColOnly" should "return expected results" in {
    val argument = Seq(
      (1, "S", initiumTemporisString, finisTemporisString),
      (1, "X", "2020-07-01 00:00:00", finisTemporisString)
    ).map { case (id, v, from, to) => (id, v, initiumTemporisString, finisTemporisString, from, to) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "val", "known_from", "known_to", "valid_from", "valid_to")
    val actual = argument.rangeCleanupExtend(Seq("id"), Seq($"valid_from"))
      .rangeCombine()
    val expected = Seq(
      (1, "S", initiumTemporisString, finisTemporisString)
    ).map { case (id, v, from, to) => (id, v, initiumTemporisString, finisTemporisString, from, to) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "val", "known_from", "known_to", "valid_from", "valid_to")
      .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCleanupExtend_rankExprFromColOnly", argument)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_rankExpr2Cols" should "return expected results" in {
    val argument = Seq(
      (1, "S", initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, "X", "2020-07-01 00:00:00", "2020-09-23 23:59:59.999"),
      (1, "B", "2020-08-03 00:00:00", finisTemporisString),
      (1, "G", "2020-09-24 00:00:00", finisTemporisString)
    ).map { case (id, v, from, to) => (id, v, initiumTemporisString, finisTemporisString, from, to) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "val", "known_from", "known_to", "valid_from", "valid_to")
    val actual = argument
      .rangeCleanupExtend(Seq("id"), Seq($"valid_to".desc, $"valid_from".asc))
      .rangeCombine()
    val expected = Seq(
      (1, "S", initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, "X", "2020-07-01 00:00:00", "2020-08-02 23:59:59.999"),
      (1, "B", "2020-08-03 00:00:00", finisTemporisString)
    ).map { case (id, v, from, to) => (id, v, initiumTemporisString, finisTemporisString, from, to) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "val", "known_from", "known_to", "valid_from", "valid_to")
      .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
    val result2 = dfEqual(actual, expected)
    if (!result2) printFailedTestResult("rangeCleanupExtend_rankExpr2Cols", argument)(actual, expected)
    result2 shouldBe true
  }

  // Note: rangeExtendRange only acts on the first interval dimension sorted by fromColName, i.e. the
  // known dimension. dfLeftKnownOnly / dfRightKnownOnly carry the interesting data on that dimension
  // and pin the valid dimension to a constant instant, so we can verify both effects.
  "rangeExtendRange_dfLeftKnownOnly" should "extend the known dimension and leave valid untouched" in {
    val actual = dfLeftKnownOnly.rangeExtendRange(Seq("id"))
    val rowsExpected = Seq(
      (0, 4.2, defaultBiTemporalConfig.lowerHorizon, defaultBiTemporalConfig.upperHorizon,
        Timestamp.valueOf(constValidInstant), Timestamp.valueOf(constValidInstant))
    )
    val expected = rowsExpected.toDF("id", "value_l", "known_from", "known_to", "valid_from", "valid_to")
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)

    if (!result) printFailedTestResult("rangeExtendRange_dfLeftKnownOnly", dfLeftKnownOnly)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeExtendRange_dfRightKnownOnly_id" should "extend the known dimension per id and leave valid untouched" in {
    val actual = dfRightKnownOnly.rangeExtendRange(Seq("id"))
    val expected = Seq(
      (0, Some(97.15),  initiumTemporisString,   "2018-01-31 23:59:59.999"),
      (0, Some(97.15),  "2018-06-01 05:24:11.0", "2018-10-23 03:50:09.999"),
      (0, Some(97.15),  "2018-10-23 03:50:10",   "2019-12-31 23:59:59.999"),
      (0, Some(97.15),  "2020-01-01 00:00:00",   finisTemporisString),
      (1, None,         initiumTemporisString,   "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999"),
      (1, None,         "2021-01-01 00:00:00.0", finisTemporisString)
    ).map { case (id, v, kf, kt) => (id, v, kf, kt, constValidInstant, constValidInstant) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("rangeExtendRange_dfRightKnownOnly_id", dfRightKnownOnly)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeExtendRange_dfRightKnownOnly" should "return expected results" in {
    // argument: dfRightKnownOnly from object BiTemporalTestUtils
    val actual = dfRightKnownOnly.rangeExtendRange()
    val expected = Seq(
      (0, Some(97.15),  initiumTemporisString,   "2018-01-31 23:59:59.999"),
      (0, Some(97.15),  "2018-06-01 05:24:11.0", "2018-10-23 03:50:09.999"),
      (0, Some(97.15),  "2018-10-23 03:50:10",   "2019-12-31 23:59:59.999"),
      (0, Some(97.15),  "2020-01-01 00:00:00",   finisTemporisString),
      (1, None,         initiumTemporisString,   "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999"),
      (1, None,         "2021-01-01 00:00:00.0", "2099-12-31 23:59:59.999")
    ).map { case (id, v, kf, kt) => (id, v, kf, kt, constValidInstant, constValidInstant) }
      .map { case (id, v, kf, kt, vf, vt) =>
        (id, v, Timestamp.valueOf(kf), Timestamp.valueOf(kt), Timestamp.valueOf(vf), Timestamp.valueOf(vt))
      }
      .toDF("id", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("rangeExtendRange_dfRightKnownOnly", dfRightKnownOnly)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeContinuous2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfContinuousTime.rangeContinuous2discrete
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
    if (!result) printFailedTestResult("rangeContinuous2discrete", Seq(dfContinuousTime))(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime" should "not modify dfLeft" in {
    val actual = dfLeft.rangeRoundDiscreteTime
    val expected = dfLeft
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeRoundDiscreteTime", Seq(dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime" should "round timestamps of dfDirtyTimeRanges" in {
    val actual = dfDirtyTimeRanges.rangeRoundDiscreteTime
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
    if (!result) printFailedTestResult("rangeRoundDiscreteTime", Seq(dfDirtyTimeRanges))(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "not modify dfMoment as extend and fillGapsWithNull are false" in {
    val actual = dfMoment.rangeUnifyRanges(keys = Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "extend dfMoment" in {
    val actual = dfMoment.rangeUnifyRanges(keys = Seq("id"), extend = true, fillGapsWithNull = true)
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = List(
      (0, initiumTemporisString,     "2019-11-30 23:59:59.999", initiumTemporisString,     finisTemporisString,       None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     initiumTemporisString,     "2019-11-25 11:12:13.004", None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", Some("A")),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.006", finisTemporisString,       None),
      (0, "2019-12-01 00:00:00.001", finisTemporisString,       initiumTemporisString,     finisTemporisString,       None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "rangeCombine_dfRightAK" should "return expected results" in {
    val actual = dfRightAK.rangeCombine()
    val rowsExpected = Seq(
      (0, "2018-01-01 00:00:00.0", "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2018-06-01 05:24:11.0", finisTemporisString,       Some(97.15)),
      (1, "2018-01-01 00:00:00.0", "2018-12-31 23:59:59.999", None),
      (1, "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999", Some(2019.0)),
      (1, "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999", Some(2020.0)),
      (1, "2021-01-01 00:00:00.0", "2099-12-31 23:59:59.999", None)
    )
    val expected = rowsExpected.map { case (id, from, to, v) => (id, initiumTemporisString, finisTemporisString, from, to, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dfRightAK", dfRightAK)(actual, expected)
    result shouldBe true
  }

  "rangeCombine dropped column" should "return expected results" in {
    val actual = dfRightAK
      .withColumn("test_column", lit("please drop me"))
      .drop("test_column")
      .rangeCombine()
    val rowsExpected = Seq(
      (0, "2018-01-01 00:00:00.0", "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2018-06-01 05:24:11.0", finisTemporisString,       Some(97.15)),
      (1, "2018-01-01 00:00:00.0", "2018-12-31 23:59:59.999", None),
      (1, "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999", Some(2019.0)),
      (1, "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999", Some(2020.0)),
      (1, "2021-01-01 00:00:00.0", "2099-12-31 23:59:59.999", None)
    )
    val expected = rowsExpected.map { case (id, from, to, v) => (id, initiumTemporisString, finisTemporisString, from, to, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine dropped column", dfRightAK)(actual, expected)
    result shouldBe true
  }

  "rangeCombine_dfMapToCombineAK" should "return expected results" in {
    val actual = dfMapToCombineAK.rangeCombine()
    val rowsExpected = Seq(
      (0, "2018-01-01 00:00:00",     "2018-12-31 23:59:59.999", Some("A")),
      (0, "2018-01-01 00:00:00",     "2018-02-03 23:59:59.999", Some("B")),
      (0, "2018-02-01 00:00:00",     "2020-04-30 23:59:59.999", None),
      (0, "2020-06-01 00:00:00",     "2020-12-31 23:59:59.999", None),
      (1, "2018-02-01 00:00:00",     "2020-04-30 23:59:59.999", Some("one")),
      (1, "2020-06-01 00:00:00",     "2020-12-31 23:59:59.999", Some("one")),
      (0, "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999", Some("D")),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", Some("X"))
    )
    val expected = rowsExpected.map { case (id, from, to, v) => (id, initiumTemporisString, finisTemporisString, from, to, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dfMapToCombineAK", dfMapToCombineAK)(actual, expected)
    result shouldBe true
  }

  "rangeCombine_dirtyTimeRangesAK" should "return expected results" in {
    val actual = dfDirtyTimeRangesAK.rangeRoundDiscreteTime.rangeCombine()
    val rowsExpected = Seq(
      (0, "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72),
      (0, "2020-01-01 00:00:00.0", "2021-01-01 00:00:00.0", "2019-02-01 02:34:56.125", "2019-04-04 00:00:0",      13.0),
      (0, "2021-01-01 00:00:00.0", finisTemporisString,     "2020-01-01 01:00:0",      finisTemporisString,       18.17),
      // known-axis dirtiness: rangeCombine does not resolve overlaps, so both audit entries survive
      // exactly as recorded, still overlapping for 10 days on the known axis
      (0, "2022-03-01 00:00:00.0", "2022-03-10 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999", 55.0),
      (0, "2022-03-05 00:00:00.0", "2022-03-15 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999", 66.0),
      (1, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0",   "2019-03-01 00:00:0",    "2019-03-01 00:00:0.001",
        0.1), // duration extended to 2 milliseconds
      (1, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0", "2019-03-01 00:00:1.001", "2019-03-01 00:00:01.002",
        1.2), // duration extended to 2 milliseconds
      (1, "2020-01-01 00:00:00.0", "2020-07-01 00:00:00.0", "2019-01-01 00:00:00.124", "2019-02-02 00:00:00",   -1.0),
      (1, "2020-07-01 00:00:00.0", finisTemporisString,     "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1", -2.0)
      // note: the id=1 audit row (known_to before known_from) is entirely invalid and correctly absent
    )
    val expected = rowsExpected.map { case (id, kf, kt, vf, vt, v) => (id, kf, kt, vf, vt, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dirtyTimeRangesAK", dfDirtyTimeRangesAK)(actual, expected)
    result shouldBe true
  }

  "rangeCombine_documentationAK" should "return expected results" in {
    val actual = dfDocumentationAK.rangeRoundDiscreteTime.rangeCombine()
    val rowsExpected = Seq(
      (1, "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72), // overlaps with previous record
      (1, "2019-01-01 00:00:0",      "2019-12-31 23:59:59.999", 42.0)
    )
    val expected = rowsExpected.map { case (id, from, to, v) => (id, initiumTemporisString, finisTemporisString, from, to, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_documentationAK", dfDocumentationAK)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "split properly the ranges of dfMsOverlap without producing dublettes" in {
    val actual = dfMsOverlap.rangeUnifyRanges(keys = Seq("id"))
    val expected = Seq(
      (0, "2019-01-01 00:00:00",     "2019-01-31 23:59:59.999", "2019-01-01 00:00:00",     "2019-01-01 9:59:59.999",  "A"),
      (0, "2019-01-01 00:00:00",     "2019-01-31 23:59:59.999", "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "A"),
      (0, "2019-01-01 00:00:00",     "2019-01-31 23:59:59.999", "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "B"),
      (0, "2019-01-01 00:00:00",     "2019-01-31 23:59:59.999", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 00:00:00",     "2019-01-01 9:59:59.999",  "A"),
      (0, "2019-02-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "A"),
      (0, "2019-02-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "B"),
      (0, "2019-02-01 00:00:00",     "2019-02-01 00:00:00",     "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00.001", finisTemporisString,       "2019-01-01 00:00:00",     "2019-01-01 09:59:59.999", "A"),
      (0, "2019-02-01 00:00:00.001", finisTemporisString,       "2019-01-01 10:00:00",     "2019-01-01 23:59:59.999", "B")
    ).map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMsOverlapAK", dfMsOverlap)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges dfMapAK" should "return expected results" in {
    val actual = dfMapAK.rangeUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, "B", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      // img = {B,C}
      (0, "B", "2018-02-01 00:00:00", "2018-02-19 23:59:59.999"),
      (0, "C", "2018-02-01 00:00:00", "2018-02-19 23:59:59.999"),
      // img = {B,C,D}
      (0, "B", "2018-02-20 00:00:00", "2018-02-25 14:15:16.122"),
      (0, "C", "2018-02-20 00:00:00", "2018-02-25 14:15:16.122"),
      (0, "D", "2018-02-20 00:00:00", "2018-02-25 14:15:16.122"),
      // img = {B,C,D,X}
      (0, "B", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      (0, "C", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      (0, "D", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      (0, "X", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      // img = {B,C,D}
      (0, "B", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999"),
      (0, "C", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999"),
      (0, "D", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999"),
      // img = {D}
      (0, "D", "2018-03-01 00:00:00", "2018-03-31 23:59:59.999")
    ).map { case (id, img, vf, vt) => (id, initiumTemporisString, finisTemporisString, vf, vt, img) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeUnifyRanges dfMapAK", dfMapAK)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges dfMicrosecTimeRangesAK" should "return expected results" in {
    logger.info(
      "\n*** Educational test case to highlight the behaviour of rangeUnifyRanges when the time has a granularity of smaller than 1ms. ***"
    )
    val actual = dfMicrosecTimeRangesAK.rangeUnifyRanges(Seq("id"))
    val expected = Seq(
      (0, 3.14, "2018-06-01 00:00:00       ", "2018-06-01 09:00:00"),
      (0, 42.0, "2018-06-01 09:00:00.000124", "2018-06-01 09:00:00"),
      (0, 2.72, "2018-06-01 09:00:00.000130", "2018-06-01 09:00:00"),
      (0, 2.72, "2018-06-01 09:00:00.001",    "2018-06-01 17:00:00.123")
    ).map { case (id, v, vf, vt) => (id, initiumTemporisString, finisTemporisString, vf, vt, v) }
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeUnifyRanges dfMicrosecTimeRangesAK", dfMicrosecTimeRangesAK)(actual, expected)
    result shouldBe true
  }

  "rangeCombine" should "combine everything possible" in {
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
    val actual = argument.rangeCombine()
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
      printFailedTestResult("rangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    }
    result shouldBe true
  }

}
