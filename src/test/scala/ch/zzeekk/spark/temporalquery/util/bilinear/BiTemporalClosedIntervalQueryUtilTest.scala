package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util._
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalTestUtils._
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
  private val fromCols: List[Column] = defaultBiTemporalConfig.rangeDimensions.map(_.fromCol)

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
        (0, "2028-01-01 00:00:00",     "2028-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
        (0, "2028-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
        (0, "2030-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
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
        (0, initiumTemporisString,     finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15), true),
        (0, "2028-01-01 00:00:00",     "2028-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15), true),
        (0, "2028-01-01 00:00:00",     finisTemporisString,   "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", None,        false),
        (0, "2028-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00), true),
        (0, "2030-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15), true),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None,        true),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d), true),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d), true),
        (1, initiumTemporisString,     finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None,        true)
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
        (0, "2028-01-01 00:00:00",     "2028-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
        (0, "2028-06-01 00:00:00.001", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
        (0, "2030-06-01 00:00:00",     finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
        (1, initiumTemporisString,     finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
        (1, initiumTemporisString,     finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
        (1, initiumTemporisString,     finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
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
        (0, initiumTemporisString,     "2027-12-31 23:59:59.999", initiumTemporisString, "2018-06-01 05:24:10.999", None,        false),
        (0, initiumTemporisString,     "2030-05-31 23:59:59.999", "2020-01-01 00:00:00", finisTemporisString,       None,        false),
        (0, initiumTemporisString,     finisTemporisString,       "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15), true),
        (0, "2028-01-01 00:00:00",     "2028-06-01 00:00:00",     "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15), true),
        (0, "2028-01-01 00:00:00",     finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None,        false),
        (0, "2028-01-01 00:00:00",     finisTemporisString,       "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", None,        false),
        (0, "2028-06-01 00:00:00.001", finisTemporisString,       "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00), true),
        (0, "2030-06-01 00:00:00",     finisTemporisString,       "2020-01-01 00:00:00", finisTemporisString,       Some(97.15), true),
        (1, initiumTemporisString,     finisTemporisString,       initiumTemporisString, "2017-12-31 23:59:59.999", None,        false),
        (1, initiumTemporisString,     finisTemporisString,       "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None,        true),
        (1, initiumTemporisString,     finisTemporisString,       "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d), true),
        (1, initiumTemporisString,     finisTemporisString,       "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d), true),
        (1, initiumTemporisString,     finisTemporisString,       "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None,        true),
        (1, initiumTemporisString,     finisTemporisString,       "2100-01-01 00:00:00", finisTemporisString,       None,        false)
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

  "rangeCombine" should "combine the rows of dfRight" in {
    val actual = dfRight.rangeCombine()
    val expected = List(
      (0, initiumTemporisString, finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2028-01-01 00:00:00", "2028-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2028-06-01 00:00:00", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
      (0, "2030-06-01 00:00:00", finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
      (1, initiumTemporisString, finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
      (1, initiumTemporisString, finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
      (1, initiumTemporisString, finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
      (1, initiumTemporisString, finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine dfRight", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeCombine dropped column" should "combine the rows of dfRight with column add/drop" in {
    val actual = dfRight
      .withColumn("test_column", lit("please drop me"))
      .drop("test_column")
      .rangeCombine()
    val expected = List(
      (0, initiumTemporisString, finisTemporisString,   "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2028-01-01 00:00:00", "2028-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2028-06-01 00:00:00", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
      (0, "2030-06-01 00:00:00", finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
      (1, initiumTemporisString, finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
      (1, initiumTemporisString, finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
      (1, initiumTemporisString, finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
      (1, initiumTemporisString, finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine dfRight column add/drop", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeCombine" should "combine ranges of dfMapToCombine" in {
    val actual = dfMapToCombine.rangeCombine()
    val expected = dfMap
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCombine_dfMapToCombineAK", dfMapToCombine)(actual, expected)
    result shouldBe true
  }

  "rangeDense2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfDenseTime.rangeDense2discrete
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
    if (!result) printFailedTestResult("rangeDense2discrete", Seq(dfDenseTime))(actual, expected)
    result shouldBe true
  }

  "rangeExtendRange" should "extend the ranges of dfMoment" in {
    val actual = dfMoment.rangeExtendRange(keys = Seq("id"))
    val expected = Seq(
      (0, initiumTemporisString, finisTemporisString, initiumTemporisString, finisTemporisString, "A")
    ).map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("rangeExtendRange_dfMoment", dfMoment)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeExtendRange" should "extend the ranges of dfRight" in {
    val actual = dfRight.rangeExtendRange(Seq("id"))
    val expected = List(
      (0, initiumTemporisString, finisTemporisString,   "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
      (0, initiumTemporisString, finisTemporisString,   "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2028-01-01 00:00:00", "2028-06-01 00:00:00", initiumTemporisString, "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2028-06-01 00:00:00", finisTemporisString,   initiumTemporisString, "2018-01-31 23:59:59.999", Some(98.00)),
      (0, "2030-06-01 00:00:00", finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
      (1, initiumTemporisString, finisTemporisString,   initiumTemporisString, "2018-12-31 23:59:59.999", None),
      (1, initiumTemporisString, finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
      (1, initiumTemporisString, finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
      (1, initiumTemporisString, finisTemporisString,   "2021-01-01 00:00:00", finisTemporisString,       None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeExtendRange_dfRight", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeExtendRange" should "extend the ranges of dfRight using global min/max" in {
    val actual = dfRight.rangeExtendRange()
    val expected = List(
      (0, initiumTemporisString, finisTemporisString,   "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
      (0, initiumTemporisString, finisTemporisString,   "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2028-01-01 00:00:00", "2028-06-01 00:00:00", initiumTemporisString, "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2028-06-01 00:00:00", finisTemporisString,   initiumTemporisString, "2018-01-31 23:59:59.999", Some(98.00)),
      (0, "2030-06-01 00:00:00", finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
      (1, initiumTemporisString, finisTemporisString,   initiumTemporisString, "2018-12-31 23:59:59.999", None),
      (1, initiumTemporisString, finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
      (1, initiumTemporisString, finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
      (1, initiumTemporisString, finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeExtendRange_dfRight", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin dfLeft with dfRight" should "return expected results" in {
    val actual = dfLeft.rangeFullJoin(df2 = dfRight, keys = Seq("id"))
    val expected = List(
      // 0: NULL, 97.15
      (0, None, Some(97.15),
        bigBangDay, doomsDay, Timestamp.valueOf("2018-12-09 00:00:00"), Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (0,           None,     Some(97.15),
        Timestamp.valueOf("2030-06-01 00:00:00"), doomsDay, Timestamp.valueOf("2020-01-01 00:00:00"), doomsDay),
      // 0: 4.2, NULL
      (0, Some(4.2), None,
        bigBangDay, Timestamp.valueOf("2027-12-31 23:59:59.999"),
        Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-06-01 05:24:10.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2028-01-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2028-01-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-06-01 05:24:10.999")),
      // 0: 4.2, 97.15
      (0, Some(4.2), Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         Some(4.2), Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      // 0: 4.2, 98
      (0, Some(4.2), Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      // id 1: rows from dfRight
      (1, None, None, bigBangDay, doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-12-31 23:59:59.999")),
      (1,                                         None, Some(2019d), bigBangDay, doomsDay,
        Timestamp.valueOf("2019-01-01 00:00:00"), Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (1,                                         None, Some(2020d), bigBangDay, doomsDay,
        Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-12-31 23:59:59.999")),
      (1,                                         None, None, bigBangDay, doomsDay,
        Timestamp.valueOf("2021-01-01 00:00:00"), Timestamp.valueOf("2099-12-31 23:59:59.999"))
    ).toDF("id", "value_l", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_dfLeft_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin dfLeft with dfMap" should "return expected results" in {
    val actual = dfLeft.rangeFullJoin(df2 = dfMap, keys = Seq("id"))
    val expected = List(
      // 4.2, NULL
      (0, Some(4.2), None,
        bigBangDay, Timestamp.valueOf("2018-02-04 23:59:59.999"),
        Timestamp.valueOf("2018-03-01 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         Some(4.2), None,
        bigBangDay, doomsDay,
        Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2018-02-05 00:00:00"), Timestamp.valueOf("2018-02-19 23:59:59.999"),
        Timestamp.valueOf("2018-03-04 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2018-02-20 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-04-01 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      // 4.2, something
      (0, Some(4.2), Some("A"),
        Timestamp.valueOf("2018-01-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         Some(4.2), Some("B"),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,                                         Some(4.2), Some("C"),
        Timestamp.valueOf("2018-02-05 00:00:00"), Timestamp.valueOf("2018-03-15 23:59:59.999"),
        Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-03-03 23:59:59.999")),
      (0,                                         Some(4.2), Some("D"),
        Timestamp.valueOf("2018-02-20 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-02-20 00:00:00"), Timestamp.valueOf("2018-03-31 23:59:59.999")),
      (0,                                         Some(4.2), Some("X"),
        Timestamp.valueOf("2018-03-01 00:00:00"), Timestamp.valueOf("2018-03-01 23:59:59.999"),
        Timestamp.valueOf("2018-02-25 14:15:16.123"), Timestamp.valueOf("2018-02-25 14:15:16.123"))
    ).toDF("id", "value_l", "img", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_dfLeft_dfRight", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin dfLeft with dfMap using rnkExpressions" should "return expected results" in {
    // Testing rangeFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeFullJoin(df2 = dfMap, keys = Seq("id"),
      rnkExpressions = $"img" +: defaultBiTemporalConfig.dimensionMap.keys.toList.sorted.map(col))
    val expected = List(
      // 4.2, NULL
      (0, Some(4.2), None,
        bigBangDay, Timestamp.valueOf("2018-02-04 23:59:59.999"),
        Timestamp.valueOf("2018-03-01 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         Some(4.2), None,
        bigBangDay, doomsDay,
        Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2018-02-05 00:00:00"), Timestamp.valueOf("2018-02-19 23:59:59.999"),
        Timestamp.valueOf("2018-03-04 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         Some(4.2), None,
        Timestamp.valueOf("2018-02-20 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-04-01 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      // 4.2, something
      (0, Some(4.2), Some("A"),
        Timestamp.valueOf("2018-01-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         Some(4.2), Some("B"),
        bigBangDay, Timestamp.valueOf("2017-12-31 23:59:59.999"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,                                         Some(4.2), Some("B"),
        Timestamp.valueOf("2018-01-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,                                         Some(4.2), Some("C"),
        Timestamp.valueOf("2018-02-05 00:00:00"), Timestamp.valueOf("2018-03-15 23:59:59.999"),
        Timestamp.valueOf("2018-03-01 00:00:00"), Timestamp.valueOf("2018-03-03 23:59:59.999")),
      (0,                                         Some(4.2), Some("D"),
        Timestamp.valueOf("2018-02-20 00:00:00"), Timestamp.valueOf("2018-03-15 23:59:59.999"),
        Timestamp.valueOf("2018-03-04 00:00:00"), Timestamp.valueOf("2018-03-31 23:59:59.999")),
      (0,                                         Some(4.2), Some("D"),
        Timestamp.valueOf("2018-03-16 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-03-01 00:00:00"), Timestamp.valueOf("2018-03-31 23:59:59.999"))
    ).toDF("id", "value_l", "img", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_dfLeft_dfMap_rnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin dfLeft with dfRight with 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRight.as("dfR"), keyCondition = $"dfL.id" === $"dfR.id")
    debugLog(s"${actual.columns.length} actual.columns: ${actual.columns.mkString(",")}")
    actual.columns.count(_ == "id") shouldBe 2
    val expected = List(
      (0, 4.2, 0, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,                                         4.2, 0, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         4.2, 0, Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         4.2, 0, Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))
    ).toDF("id", "value_l", "id", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight 'on' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin dfLeft with dfRight with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRight.as("dfR"), keys = Seq("id"))
    debugLog(s"${actual.columns.length} actual.columns: ${actual.columns.mkString(",")}")
    val expected = List(
      (0, 4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,                                         4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         4.2, Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         4.2, Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))
    ).toDF("id", "value_l", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight 'on' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoinrangeInnerJoin dfLeft with dfRightDouble with 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(dfRightDouble.as("dfR"), $"dfL.id" === $"dfR.id")
    assert(actual.columns.count(_ == "id") == 2)
    val expected = List(
      (0, 4.2, 0d, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,                                         4.2, 0d, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         4.2, 0d, Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         4.2, 0d, Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))
    ).toDF("id", "value_l", "id", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected) // && actual.schema == expectedSchema
    if (!result) printFailedTestResult("rangeInnerJoin dfRightDouble 'on' semantics", Seq(dfLeft, dfRightDouble))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoinrangeInnerJoin dfLeft with dfRightDouble with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRightDouble.as("dfR"), keys = Seq("id"))
    val expected = List(
      (0d, 4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0d,                                        4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0d,                                        4.2, Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0d,                                        4.2, Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))
    ).toDF("id", "value_l", "value_r", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected) // && actual.schema == expectedSchema
    if (!result) printFailedTestResult("rangeInnerJoin dfRightDouble 'on' semantics", Seq(dfLeft, dfRightDouble))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.rangeInnerJoin(df2 = dfR, keys = Seq("id"))
    val expected = List(
      (0, 4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,                                         4.2, Some(97.15),
        bigBangDay, doomsDay,
        Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2018-12-08 23:59:59.999")),
      (0,                                         4.2, Some(97.15),
        Timestamp.valueOf("2028-01-01 00:00:00"), Timestamp.valueOf("2028-06-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,                                         4.2, Some(98d),
        Timestamp.valueOf("2028-06-01 00:00:00"), doomsDay,
        Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))
    ).toDF("id", "value", "value", "known_from", "known_to", "valid_from", "valid_to")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin with equally named columns apart join columns", Seq(dfL, dfR))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfLeft with dfRight" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin(df2 = dfRight, joinColumns = Seq("id"))
    val expected = List(
      (0, initiumTemporisString, "2027-12-31 23:59:59.999", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", 4.2),
      (0, initiumTemporisString, finisTemporisString,       "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, initiumTemporisString, finisTemporisString,       "2018-02-01 00:00:0",  "2018-06-01 05:24:10.999", 4.2)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfLeft with dfMap" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin(dfMap, Seq("id"))
    val expected = List(
      (0, initiumTemporisString, "2018-02-04 23:59:59.999", "2018-03-01 00:00:00", "2018-03-03 23:59:59.999", 4.2),
      (0, initiumTemporisString, "2018-02-19 23:59:59.999", "2018-03-04 00:00:00", "2018-03-31 23:59:59.999", 4.2),
      (0, initiumTemporisString, finisTemporisString,       "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, initiumTemporisString, finisTemporisString,       "2018-04-01 00:00:0",  "2018-12-08 23:59:59.999", 4.2)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfLeft_dfMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfRight with dfMap" should "return expected results" in {
    val actual = dfRight.rangeLeftAntiJoin(dfMap, Seq("id"))
    val expected = List(
      (0, initiumTemporisString, finisTemporisString, "2018-06-01 05:24:11", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2030-06-01 00:00:00", finisTemporisString, "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
      (1, initiumTemporisString, finisTemporisString, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
      (1, initiumTemporisString, finisTemporisString, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019d)),
      (1, initiumTemporisString, finisTemporisString, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020d)),
      (1, initiumTemporisString, finisTemporisString, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfRight_dfMap", Seq(dfRight, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfMap with dfRight" should "return expected results" in {
    val actual = dfMap.rangeLeftAntiJoin(dfRight, Seq("id"))
    val expected = List(
      (0, initiumTemporisString, "2027-12-31 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
      (0, initiumTemporisString, finisTemporisString,       "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
      (0, "2018-01-01 00:00:00", "2027-12-31 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
      (0, "2018-02-05 00:00:00", "2018-03-15 23:59:59.999", "2018-02-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
      (0, "2018-02-20 00:00:00", finisTemporisString,       "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999", "D"),
      (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfMap_dfRight", Seq(dfMap, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime, rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps and remove overlaps of dfDirtyTimeRanges," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value" in {
      val actual = dfDirtyTimeRanges
        .rangeRoundDiscreteTime
        .rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"value"))
        .rangeCombine()
        .orderBy($"id", $"valid_from")
      val expected = List(
        // id=0: known tile [initiumTemporis, 2020-01-01 00:00:00.123] — before first data
        (0, initiumTemporisString, "2020-01-01 00:00:00.123", initiumTemporisString, finisTemporisString, None),
        // id=0: known tile [2020-01-01 00:00:00.124, 2020-01-05 12:34:56.123] — A row only
        (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", initiumTemporisString,     "2019-01-01 00:00:00.123", None),
        (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", Some(3.14)),
        (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", "2019-01-05 12:34:56.124", finisTemporisString,       None),
        // id=0: known tile [2020-01-05 12:34:56.124, 2020-02-01 00:59:59.999] — B only (before C starts)
        (0, "2020-01-05 12:34:56.124", "2020-02-01 00:59:59.999", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", Some(2.72)),
        (0, "2020-01-05 12:34:56.124", "2020-02-01 00:59:59.999", "2019-02-01 02:34:56.124", finisTemporisString,       None),
        // merged gap from tile [2020-01-05 .124, 2020-02-01 00:59:59.999] and [2020-02-01 01:00, 2020-02-01 .123]
        (0, "2020-01-05 12:34:56.124", "2020-02-01 02:34:56.123", initiumTemporisString, "2019-01-05 12:34:56.123", None),
        // id=0: known tile [2020-02-01 01:00, 2020-02-01 .123] — B and C overlap, B wins (valid_from=.124 < .001)
        (0, "2020-02-01 01:00:00", "2020-02-01 02:34:56.123", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", Some(2.72)),
        (0, "2020-02-01 01:00:00", "2020-02-01 02:34:56.124", "2019-02-01 02:34:56.125", finisTemporisString,       None),
        // id=0: known tile [2020-02-01 02:34:56.124, 2020-02-01 02:34:56.124] — C only (moment tile)
        (0, "2020-02-01 02:34:56.124", "2020-02-01 02:34:56.124", initiumTemporisString, "2019-02-01 00:59:59.999", None),
        (0, "2020-02-01 02:34:56.124", "2020-02-01 02:34:56.124", "2019-02-01 01:00:00", "2019-02-01 02:34:56.124", Some(2.72)),
        // id=0: known tile [2020-02-01 02:34:56.125, 2020-03-02 23:59:59.999] — D only (before D-E touch)
        (0, "2020-02-01 02:34:56.125", "2020-03-02 23:59:59.999", "2019-02-01 02:34:56.125", "2019-03-03 00:00:00",     Some(13.0)),
        (0, "2020-02-01 02:34:56.125", "2020-03-02 23:59:59.999", "2019-03-03 00:00:00.001", finisTemporisString,       None),
        (0, "2020-02-01 02:34:56.125", "2020-03-03 00:00:00",     initiumTemporisString,     "2019-02-01 02:34:56.124", None),
        // id=0: known tile [2020-03-03, 2020-03-03] — D-E overlap (touch point, D wins)
        (0, "2020-03-03 00:00:00", "2020-03-03 00:00:00", "2019-02-01 02:34:56.125", "2019-04-04 00:00:00", Some(13.0)),
        (0, "2020-03-03 00:00:00", "2020-04-04 00:00:00", "2019-04-04 00:00:00.001", finisTemporisString,   None),
        // id=0: known tile [2020-03-03 .001, 2020-04-04] — E only
        (0, "2020-03-03 00:00:00.001", "2020-04-04 00:00:00", initiumTemporisString, "2019-03-02 23:59:59.999", None),
        (0, "2020-03-03 00:00:00.001", "2020-04-04 00:00:00", "2019-03-03 00:00:00", "2019-04-04 00:00:00",     Some(13.0)),
        // id=0: known tile [2020-04-04 .001, 2021-01-01 00:59:59.999] — gap
        (0, "2020-04-04 00:00:00.001", "2021-01-01 00:59:59.999", initiumTemporisString, finisTemporisString, None),
        // id=0: known tile [2021-01-01 01:00, 2022-02-28 23:59:59.999] — F only (before G)
        (0, "2021-01-01 01:00:00", "2022-02-28 23:59:59.999", initiumTemporisString, "2020-01-01 00:59:59.999", None),
        (0, "2021-01-01 01:00:00", finisTemporisString,       "2020-01-01 01:00:00", finisTemporisString,       Some(18.17)),
        // id=0: G only, G-H overlap, H only tiles — null fills merge across all three on known axis
        (0, "2022-03-01 00:00:00", "2022-03-10 23:59:59.999", "2019-07-01 00:00:00", "2019-07-05 23:59:59.999", Some(55.0)),
        (0, "2022-03-01 00:00:00", "2022-03-15 23:59:59.999", initiumTemporisString, "2019-06-30 23:59:59.999", None),
        (0, "2022-03-01 00:00:00", "2022-03-15 23:59:59.999", "2019-07-06 00:00:00", "2020-01-01 00:59:59.999", None),
        (0, "2022-03-11 00:00:00", "2022-03-15 23:59:59.999", "2019-07-01 00:00:00", "2019-07-05 23:59:59.999", Some(66.0)),
        (0, "2022-03-16 00:00:00", finisTemporisString,       initiumTemporisString, "2020-01-01 00:59:59.999", None),
        // id=1: known tile [initiumTemporis, 2020-01-01 00:00:00.123] — before id=1 data
        (1, initiumTemporisString, "2020-01-01 00:00:00.123", initiumTemporisString, finisTemporisString, None),
        // id=1: known tile [2020-01-01 00:00:00.124, 2020-02-02 00:00:00] — id=1 row A
        (1, "2020-01-01 00:00:00.124", "2020-02-02 00:00:00", initiumTemporisString,     "2019-01-01 00:00:00.123", None),
        (1, "2020-01-01 00:00:00.124", "2020-02-02 00:00:00", "2019-01-01 00:00:00.124", "2019-02-02 00:00:00",     Some(-1.0)),
        (1, "2020-01-01 00:00:00.124", "2020-02-02 00:00:00", "2019-02-02 00:00:00.001", finisTemporisString,       None),
        // id=1: known tile [2020-02-02 00:00:00.001, 2020-02-29 23:59:59.999] — gap
        (1, "2020-02-02 00:00:00.001", "2020-02-29 23:59:59.999", initiumTemporisString, finisTemporisString, None),
        // id=1: known tile [2020-03-01, 2020-03-01] — id=1 moment row
        (1, "2020-03-01 00:00:00", "2020-03-01 00:00:00", initiumTemporisString,     "2019-02-28 23:59:59.999", None),
        (1, "2020-03-01 00:00:00", "2020-03-01 00:00:00", "2019-03-01 00:00:00",     "2019-03-01 00:00:00",     Some(0.1)),
        (1, "2020-03-01 00:00:00", "2020-03-01 00:00:00", "2019-03-01 00:00:00.001", finisTemporisString,       None),
        // id=1: known tile [2020-03-01 00:00:00.001, 2020-03-01 00:00:00.999] — gap
        (1, "2020-03-01 00:00:00.001", "2020-03-01 00:00:00.999", initiumTemporisString, finisTemporisString, None),
        // id=1: known tile [2020-03-01 00:00:01, 2020-03-01 00:00:01] — id=1 second moment row
        (1, "2020-03-01 00:00:01", "2020-03-01 00:00:01", initiumTemporisString,     "2019-03-01 00:00:00",     None),
        (1, "2020-03-01 00:00:01", "2020-03-01 00:00:01", "2019-03-01 00:00:00.001", "2019-03-01 00:00:00.001", Some(0.1)),
        (1, "2020-03-01 00:00:01", "2020-03-01 00:00:01", "2019-03-01 00:00:00.002", finisTemporisString,       None),
        // id=1: known tile [2020-03-01 00:00:01.001, 2020-03-01 00:00:01.002] — id=1 1.2 row
        (1, "2020-03-01 00:00:01.001", "2020-03-01 00:00:01.002", initiumTemporisString,     "2019-03-01 00:00:01",     None),
        (1, "2020-03-01 00:00:01.001", "2020-03-01 00:00:01.002", "2019-03-01 00:00:01.001", "2019-03-01 00:00:01.002", Some(1.2)),
        (1, "2020-03-01 00:00:01.001", "2020-03-01 00:00:01.002", "2019-03-01 00:00:01.003", finisTemporisString,       None),
        // id=1: known tile [2020-03-01 00:00:01.003, 2020-03-03 00:59:59.999] — gap
        (1, "2020-03-01 00:00:01.003", "2020-03-03 00:59:59.999", initiumTemporisString, finisTemporisString, None),
        // id=1: known tile [2020-03-03 01:00:00, 2022-12-01 02:34:56.1] — id=1 -2.0 row
        (1, "2020-03-03 01:00:00", "2022-12-01 02:34:56.1", initiumTemporisString,     "2019-03-03 00:59:59.999", None),
        (1, "2020-03-03 01:00:00", "2022-12-01 02:34:56.1", "2019-03-03 01:00:00",     "2021-12-01 02:34:56.1",   Some(-2.0)),
        (1, "2020-03-03 01:00:00", "2022-12-01 02:34:56.1", "2021-12-01 02:34:56.101", finisTemporisString,       None),
        // id=1: known tile [2022-12-01 02:34:56.101, finisTemporis] — gap
        (1, "2022-12-01 02:34:56.101", finisTemporisString, initiumTemporisString, finisTemporisString, None)
      ).map(makeRowsBiTemporal)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
        .withColumn(defaultBiTemporalConfig.definedColName, $"value".isNotNull)
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRanges", dfDirtyTimeRanges)(actual, expected)
      result shouldBe true
    }

  "rangeRoundDiscreteTime, rangeCleanupExtend and rangeCombine" should
    "combine and remove overlaps of dfDirtyTimeRanges," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value" +
    " without extending ranges or filling gaps" in {
      val actual = dfDirtyTimeRanges.rangeRoundDiscreteTime
        .rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"value"), extend = false, fillGapsWithNull = false)
        .rangeCombine()
        .orderBy($"id", $"valid_from")
      val expected = Seq(
        (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
        (0, "2020-01-05 12:34:56.124", "2020-02-01 00:59:59.999", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
        (0, "2020-02-01 01:00:00",     "2020-02-01 02:34:56.123", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72),
        (0, "2020-02-01 02:34:56.124", "2020-02-01 02:34:56.124", "2019-02-01 01:00:00",     "2019-02-01 02:34:56.124", 2.72),
        (0, "2020-02-01 02:34:56.125", "2020-03-02 23:59:59.999", "2019-02-01 02:34:56.125", "2019-03-03 00:00:00",     13.0),
        (0, "2020-03-03 00:00:00",     "2020-03-03 00:00:00",     "2019-02-01 02:34:56.125", "2019-04-04 00:00:00",     13.0),
        (0, "2020-03-03 00:00:00.001", "2020-04-04 00:00:00",     "2019-03-03 00:00:00",     "2019-04-04 00:00:00",     13.0),
        (0, "2021-01-01 01:00:00",     finisTemporisString,       "2020-01-01 01:00:00",     finisTemporisString,       18.17),
        (0, "2022-03-01 00:00:00",     "2022-03-10 23:59:59.999", "2019-07-01 00:00:00",     "2019-07-05 23:59:59.999", 55.0),
        (0, "2022-03-11 00:00:00",     "2022-03-15 23:59:59.999", "2019-07-01 00:00:00",     "2019-07-05 23:59:59.999", 66.0),
        (1, "2020-01-01 00:00:00.124", "2020-02-02 00:00:00",     "2019-01-01 00:00:00.124", "2019-02-02 00:00:00",     -1.0),
        (1, "2020-03-01 00:00:00",     "2020-03-01 00:00:00",     "2019-03-01 00:00:00",     "2019-03-01 00:00:00",     0.1),
        (1, "2020-03-01 00:00:01",     "2020-03-01 00:00:01",     "2019-03-01 00:00:00.001", "2019-03-01 00:00:00.001", 0.1),
        (1, "2020-03-01 00:00:01.001", "2020-03-01 00:00:01.002", "2019-03-01 00:00:01.001", "2019-03-01 00:00:01.002", 1.2),
        (1, "2020-03-03 01:00:00",     "2022-12-01 02:34:56.1",   "2019-03-03 01:00:00",     "2021-12-01 02:34:56.1",   -2.0)
      ).map(makeRowsBiTemporal)
        .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
        .withColumn(defaultBiTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)

      if (!result)
        printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRanges_NoExtendFillgaps", dfDirtyTimeRanges)(actual, expected)
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
      (0, "2022-03-01 00:00:00.0",   "2022-03-10 23:59:59.999", "2019-07-01 00:00:00.0",   "2019-07-05 23:59:59.999", 55.0),
      (0, "2022-03-05 00:00:00.0",   "2022-03-15 23:59:59.999", "2019-07-01 00:00:00.0",   "2019-07-05 23:59:59.999", 66.0),
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

  "rangeRoundDiscreteTime and rangeCombine" should "combine dfDirtyTimeRanges" in {
    val actual = dfDirtyTimeRanges.rangeRoundDiscreteTime.rangeCombine()
    val rowsExpected = Seq(
      (0, "2020-01-01 00:00:00.124", "2020-01-05 12:34:56.123", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2020-01-05 12:34:56.124", "2020-02-01 02:34:56.123", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, "2020-02-01 01:00:00",     "2020-02-01 02:34:56.124", "2019-02-01 01:00:00",     "2019-02-01 02:34:56.124", 2.72),
      (0, "2020-02-01 02:34:56.125", "2020-03-03 00:00:00",     "2019-02-01 02:34:56.125", "2019-03-03 00:00:00",     13.0),
      (0, "2020-03-03 00:00:00",     "2020-04-04 00:00:00",     "2019-03-03 00:00:00",     "2019-04-04 00:00:00",     13.0),
      (0, "2021-01-01 01:00:00",     finisTemporisString,       "2020-01-01 01:00:00",     finisTemporisString,       18.17),
      (0, "2022-03-01 00:00:00",     "2022-03-10 23:59:59.999", "2019-07-01 00:00:00",     "2019-07-05 23:59:59.999", 55.0),
      (0, "2022-03-05 00:00:00",     "2022-03-15 23:59:59.999", "2019-07-01 00:00:00",     "2019-07-05 23:59:59.999", 66.0),
      (1, "2020-01-01 00:00:00.124", "2020-02-02 00:00:00",     "2019-01-01 00:00:00.124", "2019-02-02 00:00:00",     -1.0),
      (1, "2020-03-01 00:00:00",     "2020-03-01 00:00:00",     "2019-03-01 00:00:00",     "2019-03-01 00:00:00",     0.1),
      (1, "2020-03-01 00:00:01",     "2020-03-01 00:00:01",     "2019-03-01 00:00:00.001", "2019-03-01 00:00:00.001", 0.1),
      (1, "2020-03-01 00:00:01.001", "2020-03-01 00:00:01.002", "2019-03-01 00:00:01.001", "2019-03-01 00:00:01.002", 1.2),
      (1, "2020-03-03 01:00:00",     "2022-12-01 02:34:56.1",   "2019-03-03 01:00:00",     "2021-12-01 02:34:56.1",   -2.0)
    )
    val expected = rowsExpected
      .map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dirtyTimeRanges", dfDirtyTimeRanges)(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime and rangeCombine" should "combine dfDocumentation" in {
    val actual = dfDocumentation.rangeRoundDiscreteTime.rangeCombine()
    val expected = Seq(
      (1, "2020-01-01 00:00:00", "2021-01-01 00:00:00", "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72),
      (1, "2020-09-01 00:00:00", "2023-01-01 00:00:00", "2019-01-01 00:00:0",      "2019-12-31 23:59:59.999", 42.0)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_documentationAK", dfDocumentation)(actual, expected)
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

  "rangeUnifyRanges" should "split properly the ranges of dfMsOverlap" in {
    val actual = dfMsOverlap.rangeUnifyRanges(keys = Seq("id"))
    val expected = Seq(
      (0, "2019-01-01 00:00:00", "2019-01-31 23:59:59.999", "2019-01-01 00:00:00",     "2019-01-01 9:59:59.999",  "A"),
      (0, "2019-01-01 00:00:00", "2019-01-31 23:59:59.999", "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "A"),
      (0, "2019-01-01 00:00:00", "2019-01-31 23:59:59.999", "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "B"),
      (0, "2019-01-01 00:00:00", "2019-01-31 23:59:59.999", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00",     "2019-01-01 00:00:00",     "2019-01-01 9:59:59.999",  "A"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00",     "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "A"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00",     "2019-01-01 10:00:00",     "2019-01-01 10:00:00",     "B"),
      // the following 4-let is expected as the information is stored twice
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      (0, "2019-02-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999", "B"),
      //
      (0, "2019-02-01 00:00:00.001", finisTemporisString, "2019-01-01 00:00:00", "2019-01-01 09:59:59.999", "A"),
      (0, "2019-02-01 00:00:00.001", finisTemporisString, "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B")
    ).map(makeRowsBiTemporal)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMsOverlapAK", dfMsOverlap)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "split properly the ranges of dfMap" in {
    val actual = dfMap.rangeUnifyRanges(keys = Seq("id"))
    val expected = dfMapToCombine
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMap", dfMap)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges dfMicrosecTimeRanges" should
    "show what happens when time is more precise than the granularity of 1ms " in {
      logger.info(
        "\n*** Educational test case to highlight the behaviour of rangeUnifyRanges" +
          " when the time has a granularity of smaller than 1ms. ***"
      )
      val actual = dfMicrosecTimeRanges.rangeUnifyRanges(keys = Seq("id"))
      val expected = Seq(
        (0, "2022-03-01 00:00:00",        "2022-03-01 09:00:00",     "2018-06-01 00:00:00",        "2018-06-01 09:00:00",     3.14),
        (0, "2022-03-01 09:00:00.000124", "2022-03-01 09:00:00",     "2018-06-01 09:00:00.000124", "2018-06-01 09:00:00",     42.0),
        (0, "2022-03-01 09:00:00.001",    "2022-03-01 10:00:00",     "2018-06-01 09:00:00.000124", "2018-06-01 09:00:00",     42.0),
        (0, "2022-03-01 10:00:00.00013",  "2022-03-01 10:00:00",     "2018-06-01 09:00:00.00013",  "2018-06-01 17:00:00.123", 2.72),
        (0, "2022-03-01 10:00:00.001",    "2022-03-07 09:00:00.123", "2018-06-01 09:00:00.00013",  "2018-06-01 17:00:00.123", 2.72)
      ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeUnifyRanges dfMicrosecTimeRangesAK", dfMicrosecTimeRanges)(actual, expected)
      result shouldBe true
    }

}
