package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.linear.TemporalTestUtils._
import ch.zzeekk.spark.temporalquery.util.{finisTemporisString, initiumTemporisString, timestampOrdering}
import ch.zzeekk.spark.temporalquery.{udf_durationInMillis, TestUtils}
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class TemporalClosedIntervalQueryUtilTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering

  logger.info(s"TemporalQueryUtilTest: defaultTemporalConfig = $defaultTemporalConfig")

  "MultivarRangeUnion.intersect" should "return the intersection of 2 MultivarRangeUnions" in {
    val left = defaultTemporalConfig.MultivarRangeUnion(Set(
        List((Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999"))),
        List((Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")))
      ))
    val right = defaultTemporalConfig.MultivarRangeUnion(Set(
        List((Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-06-01 05:24:10.999")))
      ))
    val actual = left.intersect(right)
    val expected = defaultTemporalConfig.MultivarRangeUnion(Set(
        List((Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999"))),
        List((Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-06-01 05:24:10.999")))
      ))
    actual shouldBe expected
  }

  "complementFamily" should "return the complement of a family" in {
    val minuend = List((Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val subtrahends: Seq[List[(Timestamp, Timestamp)]] = Seq(
      List((Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"))),
      List((Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2019-12-31 23:59:59.999")))
    )
    val actual = defaultTemporalConfig.complementFamily(minuend, subtrahends)
    val expected = defaultTemporalConfig.MultivarRangeUnion(Set(
        List((Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2017-12-31 23:59:59.999"))),
        List((Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-06-01 05:24:10.999")))
      ))
    actual shouldBe expected
  }

  "rangeCleanupExtend and rangeCombine" should "extend and combine dfLeft" in {
    val actual = dfLeft.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq(defaultFromCol))
      .rangeCombine()
      .orderBy(defaultFromCol)
    val expected = List(
      (0, None,      false, initiumTemporisString, "2017-12-09 23:59:59.999"),
      (0, Some(4.2), true,  "2017-12-10 00:00:00", "2018-12-08 23:59:59.999"),
      (0, None,      false, "2018-12-09 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Boolean])
      .toDF("id", "value_l", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)

    val result = dfEqual(reorderCols(actual, expected), expected)
    if (!result) printFailedTestResult("rangeCleanupExtend", dfLeft)(reorderCols(actual, expected), expected)
    result shouldBe true
  }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges of dfRight" +
    " without extending or filling gaps" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = Seq(defaultFromCol),
        extend = false,
        fillGapsWithNull = false
      ).rangeCombine()
      val expected = Seq(
        (0, Some(97.15),  "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, Some(97.15),  "2018-06-01 05:24:11", finisTemporisString),
        (1, None,         "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
        (1, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
        (1, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
        (1, None,         "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
      ).map(makeRowsWithTimeRangeEnd[Int, Option[Double]])
        .toDF("id", "value_r", defaultFromColName, defaultToColName)
        .withColumn(defaultTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges and fill gaps of dfRight" +
    " without extending" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = Seq(defaultFromCol),
        extend = false
      ).rangeCombine()
      val expected = Seq(
        (0, Some(97.15),  true,  "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, None,         false, "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
        (0, Some(97.15),  true,  "2018-06-01 05:24:11", finisTemporisString),
        (1, None,         true,  "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
        (1, Some(2019.0), true,  "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
        (1, Some(2020.0), true,  "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
        (1, None,         true,  "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
      ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Boolean])
        .toDF("id", "value_r", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_fillGaps_noExtend", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine ranges of dfRight without extending or filling gaps since extend is ignore if not(fillGapsWithNull)" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = Seq(defaultFromCol),
        fillGapsWithNull = false
      ).rangeCombine()
      val expected = Seq(
        (0, Some(97.15),  "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, Some(97.15),  "2018-06-01 05:24:11", finisTemporisString),
        (1, None,         "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
        (1, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
        (1, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
        (1, None,         "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
      ).map(makeRowsWithTimeRangeEnd[Int, Option[Double]])
        .toDF("id", "value_r", defaultFromColName, defaultToColName)
        .withColumn(defaultTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_extend_nofillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps" +
    " and remove overlaps of dfRight" in {
      val actual = dfRight.rangeCleanupExtend(
        keys = Seq("id"),
        rnkExpressions = Seq(defaultFromCol)
      ).rangeCombine()
        .orderBy($"id", defaultFromCol)
      val expected = Seq(
        (0, None,         false, initiumTemporisString, "2017-12-31 23:59:59.999"),
        (0, Some(97.15),  true,  "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, None,         false, "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
        (0, Some(97.15),  true,  "2018-06-01 05:24:11", finisTemporisString),
        (1, None,         false, initiumTemporisString, "2017-12-31 23:59:59.999"),
        (1, None,         true,  "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
        (1, Some(2019.0), true,  "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
        (1, Some(2020.0), true,  "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
        (1, None,         true,  "2021-01-01 00:00:00", "2099-12-31 23:59:59.999"),
        (1, None,         false, "2100-01-01 00:00:00", finisTemporisString)
      ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Boolean])
        .toDF("id", "value_r", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeCleanupExtend_dfRight_extend_fillGaps", dfRight)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend and rangeCombine" should
    "combine, extend ranges, fill gaps and remove overlaps of dfMap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMap.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq($"img"))
        .rangeCombine()
      val expected = Seq(
        (0, None,      false, initiumTemporisString, "2017-12-31 23:59:59.999"),
        (0, Some("A"), true,  "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, Some("B"), true,  "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
        (0, Some("D"), true,  "2018-03-01 00:00:00", "2018-03-31 23:59:59.999"),
        (0, None,      false, "2018-04-01 00:00:00", finisTemporisString)
      ).map(makeRowsWithTimeRangeEnd[Int, Option[String], Boolean])
        .toDF("id", "img", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCleanupExtend_dfMap", dfMap)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend_dfMap_NoExtendFillgaps" should
    "combine and remove overlaps of dfMap," +
    " and then convert dfMap to a 1-1-relation by selecting the smallest value of img" in {
      val actual = dfMap.rangeCleanupExtend(Seq("id"), Seq($"img"), extend = false, fillGapsWithNull = false)
        .rangeCombine()
      val expected = Seq(
        (0, Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
        (0, Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
        (0, Some("D"), "2018-03-01 00:00:00", "2018-03-31 23:59:59.999")
      ).map(makeRowsWithTimeRangeEnd[Int, Option[String]])
        .toDF("id", "img", defaultFromColName, defaultToColName)
        .withColumn(defaultTemporalConfig.definedColName, lit(true))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCleanupExtend_dfMap_NoExtendFillgaps", dfMap)(actual, expected)
      result shouldBe true
    }

  "rangeCleanupExtend_dfMsOverlap" should "return expected results" in {
    val actual = dfMsOverlap.rangeCleanupExtend(Seq("id"), Seq(defaultFromCol))
      .rangeCombine()
    val expected = Seq(
      (0, None,      false, initiumTemporisString,     "2018-12-31 23:59:59.999"),
      (0, Some("A"), true,  "2019-01-01 00:00:00",     "2019-01-01 10:00:00"),
      (0, Some("B"), true,  "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999"),
      (0, None,      false, "2019-01-02 00:00:00",     finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[String], Boolean])
      .toDF("id", "img", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCleanupExtend_dfMap", dfMap)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_dfDirtyTimeRanges" should "return expected results" in {
    val actual =
      dfDirtyTimeRanges.rangeRoundDiscreteTime.rangeCleanupExtend(Seq("id"), Seq(defaultFromCol, $"value"))
        .rangeCombine()
        .orderBy($"id", defaultFromCol)
    val expected = Seq(
      (0, None,        false, initiumTemporisString,     "2019-01-01 00:00:00.123"),
      (0, Some(3.14),  true,  "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, Some(2.72),  true,  "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, Some(13.0),  true,  "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, None,        false, "2019-04-04 00:00:00.001", "2020-01-01 00:59:59.999"),
      (0, Some(18.17), true,  "2020-01-01 01:00:0",      finisTemporisString),
      (1, None,        false, initiumTemporisString,     "2019-01-01 00:00:00.123"),
      (1, Some(-1.0),  true,  "2019-01-01 00:00:0.124",  "2019-02-02 00:00:0"),
      (1, None,        false, "2019-02-02 00:00:0.001",  "2019-02-28 23:59:59.999"),
      (1, Some(0.1),   true,  "2019-03-01 00:00:0",      "2019-03-01 00:00:00.001"),
      (1, None,        false, "2019-03-01 00:00:00.002", "2019-03-01 00:00:1"),
      (1, Some(1.2),   true,  "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002"),
      (1, None,        false, "2019-03-01 00:00:1.003",  "2019-03-03 00:59:59.999"),
      (1, Some(-2.0),  true,  "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1"),
      (1, None,        false, "2021-12-01 02:34:56.101", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Boolean])
      .toDF("id", "value", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRanges", dfDirtyTimeRanges)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_dfDirtyTimeRanges_NoExtendFillgaps" should "return expected results" in {
    val actual =
      dfDirtyTimeRanges.rangeRoundDiscreteTime.rangeCleanupExtend(Seq("id"), Seq(defaultFromCol, $"value"),
        extend = false, fillGapsWithNull = false)
        .rangeCombine()
        .orderBy($"id", defaultFromCol)
    val expected = Seq(
      (0, 3.14,  "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, 2.72,  "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, 13.0,  "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, 18.17, "2020-01-01 01:00:0",      finisTemporisString),
      (1, -1.0,  "2019-01-01 00:00:0.124",  "2019-02-02 00:00:0"),
      (1, 0.1,   "2019-03-01 00:00:0",      "2019-03-01 00:00:00.001"),
      (1, 1.2,   "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002"),
      (1, -2.0,  "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1")
    ).map(makeRowsWithTimeRangeEnd[Int, Double])
      .toDF("id", "value", defaultFromColName, defaultToColName)
      .withColumn(defaultTemporalConfig.definedColName, lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCleanupExtend_dfDirtyTimeRanges_NoExtendFillgaps", dfDirtyTimeRanges)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_validityDuration" should "return expected results" in {
    val argument = Seq(
      (1, "A", "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, "A", "2020-07-05 00:00:00", "2020-07-07 23:59:59.999"),
      (1, "B", "2020-07-01 00:00:00", "2020-07-02 23:59:59.999"),
      (1, "B", "2020-07-04 00:00:00", "2020-07-07 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultFromColName, defaultToColName)
    // we want the record with the longest validity period, i.e. maximal toColName-fromColName
    val actual =
      argument.rangeCleanupExtend(keys = Seq("id"),
        rnkExpressions = Seq(udf_durationInMillis(defaultToCol, defaultFromCol).desc)
      ).rangeCombine()
    val expected = Seq(
      (1, None,      false, initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, Some("A"), true,  "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, Some("B"), true,  "2020-07-04 00:00:00", "2020-07-07 23:59:59.999"),
      (1, None,      false, "2020-07-08 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[String], Boolean])
      .toDF("id", "val", defaultTemporalConfig.definedColName, defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCleanupExtend_validityDuration", argument)(actual, expected)
    result shouldBe true
  }

  "rangeCleanupExtend_rankExprFromColOnly" should "return expected results" in {
    val argument = Seq(
      (1, "S", initiumTemporisString, finisTemporisString),
      (1, "X", "2020-07-01 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultFromColName, defaultToColName)
    val actual = argument.rangeCleanupExtend(keys = Seq("id"), rnkExpressions = Seq(defaultFromCol))
      .rangeCombine()
    val expected = Seq(
      (1, "S", initiumTemporisString, finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultFromColName, defaultToColName)
      .withColumn(defaultTemporalConfig.definedColName, lit(true))
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
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultFromColName, defaultToColName)
    val actual = argument.rangeCleanupExtend(keys = Seq("id"),
      rnkExpressions = Seq(defaultToCol.desc, defaultFromCol.asc)
    ).rangeCombine()
    val expected = Seq(
      (1, "S", initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, "X", "2020-07-01 00:00:00", "2020-08-02 23:59:59.999"),
      (1, "B", "2020-08-03 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultFromColName, defaultToColName)
      .withColumn(defaultTemporalConfig.definedColName, lit(true))
    val result2 = dfEqual(actual, expected)
    if (!result2) printFailedTestResult("rangeCleanupExtend_rankExpr2Cols", argument)(actual, expected)
    result2 shouldBe true
  }

  "rangeCombine_dfRight" should "return expected results" in {
    val actual = dfRight.rangeCombine()
    val rowsExpected = Seq(
      (0, "2018-01-01 00:00:00.0", "2018-01-31 23:59:59.999", Some(97.15)),
      (0, "2018-06-01 05:24:11.0", finisTemporisString,       Some(97.15)),
      (1, "2018-01-01 00:00:00.0", "2018-12-31 23:59:59.999", None),
      (1, "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999", Some(2019.0)),
      (1, "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999", Some(2020.0)),
      (1, "2021-01-01 00:00:00.0", "2099-12-31 23:59:59.999", None)
    )
    val expected =
      rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dfRight", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeCombine dropped column" should "combine the rows of dfRight with column add/drop" in {
    val actual = dfRight
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
    val expected =
      rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine dropped column", dfRight)(actual, expected)
    result shouldBe true
  }

  "rangeCombine_dfMapToCombine" should "return expected results" in {
    val actual = dfMapToCombine.rangeCombine()
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
    val expected =
      rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dfMapToCombine", dfMapToCombine)(actual, expected)
    result shouldBe true
  }

  "rangeDense2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfDenseTime.rangeDense2discrete
    val expected = Seq(
      (0, "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, "2019-02-01 02:34:56.124", "2019-02-01 02:34:56.124", 42.0),
      (0, "2019-02-01 02:34:56.125", "2019-03-02 23:59:59.999", 13.0),
      (0, "2019-03-03 00:00:0",      "2019-04-03 23:59:59.999", 12.0),
      (0, "2020-01-01 01:00:0",      finisTemporisString,       18.17),
      (1, "2019-01-01 00:00:0.124",  "2019-02-01 23:59:59.999", -1.0),
      (1, "2019-03-03 01:00:0",      "2021-12-01 02:34:56.099", -2.0)
    ).map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultFromColName, defaultToColName, "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeDense2discrete", Seq(dfDenseTime))(actual, expected)
    result shouldBe true
  }

  "rangeExtendRange_dfLeft" should "return expected results" in {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.rangeExtendRange(Seq("id"))
    val rowsExpected = Seq((0, 4.2, defaultLowerHorizon, defaultUpperHorizon))
    val expected = rowsExpected.toDF("id", "value_L", defaultFromColName, defaultToColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)

    if (!result) printFailedTestResult("rangeExtendRange_dfLeft", dfLeft)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeExtendRange_dfRight_id" should "return expected results" in {
    val actual = dfRight.rangeExtendRange(Seq("id"))
    val expected = Seq(
      (0, Some(97.15),  initiumTemporisString,   "2018-01-31 23:59:59.999"),
      (0, Some(97.15),  "2018-06-01 05:24:11.0", "2018-10-23 03:50:09.999"),
      (0, Some(97.15),  "2018-10-23 03:50:10",   "2019-12-31 23:59:59.999"),
      (0, Some(97.15),  "2020-01-01 00:00:00",   finisTemporisString),
      (1, None,         initiumTemporisString,   "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999"),
      (1, None,         "2021-01-01 00:00:00.0", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double]])
      .toDF("id", "value_r", defaultFromColName, defaultToColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("rangeExtendRange_dfRight_id", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeExtendRange" should "extend the ranges of dfRight" in {
    val actual = dfRight.rangeExtendRange()
    val expected = Seq(
      (0, Some(97.15),  initiumTemporisString,   "2018-01-31 23:59:59.999"),
      (0, Some(97.15),  "2018-06-01 05:24:11.0", "2018-10-23 03:50:09.999"),
      (0, Some(97.15),  "2018-10-23 03:50:10",   "2019-12-31 23:59:59.999"),
      (0, Some(97.15),  "2020-01-01 00:00:00",   finisTemporisString),
      (1, None,         initiumTemporisString,   "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00.0", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00.0", "2020-12-31 23:59:59.999"),
      (1, None,         "2021-01-01 00:00:00.0", "2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double]])
      .toDF("id", "value_r", defaultFromColName, defaultToColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("rangeExtendRange_dfRight", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeInnerJoin dfLeft with dfRight with 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRight.as("dfR"), keyCondition = $"dfL.id" === $"dfR.id")
    actual.columns.count(_ == "id") shouldBe 2
    val expected = Seq(
      (0, 4.2, 0, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, 0, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0, 4.2, 0, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Int, Option[Double]])
      .toDF("id", "value_l", "id", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight 'on' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin dfLeft with dfRight with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRight.as("dfR"), keys = Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value_l", $"dfR.value_r").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0, 4.2, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[Double]])
      .toDF("id", "value_l", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight with 'using' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoinrangeInnerJoin dfLeft with dfRightDouble with 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRightDouble.as("dfR"), keyCondition = $"dfL.id" === $"dfR.id")
    assert(actual.columns.count(_ == "id") == 2)
    val expected = Seq(
      (0, 4.2, 0.0, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, 0.0, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0, 4.2, 0.0, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Double, Option[Double]])
      .toDF("id", "value_l", "id", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected) // && actual.schema == expectedSchema
    if (!result) printFailedTestResult("rangeInnerJoin dfRightDouble 'on' semantics", Seq(dfLeft, dfRightDouble))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoinrangeInnerJoin dfLeft with dfRightDouble with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin(df2 = dfRightDouble.as("dfR"), keys = Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value_l", $"dfR.value_r").count())
    val expected = Seq(
      (0.0, 4.2, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0.0, 4.2, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0.0, 4.2, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Double, Double, Option[Double]])
      .toDF("id", "value_l", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result)
      printFailedTestResult("rangeInnerJoin dfRightDouble with 'using' semantics", Seq(dfLeft, dfRightDouble))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.rangeInnerJoin(df2 = dfR, keys = Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0, 4.2, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[Double]])
      .toDF("id", "value", "value", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin with equally named columns apart join columns", Seq(dfL, dfR))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfLeft with dfRight" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin(df2 = dfRight, joinColumns = Seq("id"))
    val expected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", 4.2)
    ).map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfLeft with dfMap" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin(dfMap, Seq("id"))
    val expected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999", 4.2)
    ).map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfLeft_dfMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfRight with dfMap" should "return expected results" in {
    val actual = dfRight.rangeLeftAntiJoin(dfMap, Seq("id"))
    val rowsExpected: Seq[(Int, String, String, Option[Double])] = Seq(
      (0, "2018-06-01 05:24:11", finisTemporisString,       Some(97.15)),
      (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
      (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019)),
      (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020)),
      (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfRight_dfMap", Seq(dfRight, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin dfMap with dfRight" should "return expected results" in {
    val actual = dfMap.rangeLeftAntiJoin(dfRight, Seq("id"))
    val rowsExpected: Seq[(Int, String, String, String)] = Seq(
      (0, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
      (0, "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_dfMap_dfRight", Seq(dfMap, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftAntiJoin_segmented" should "return expected results" in {
    val minuend = Seq(
      (1, "2019-01-01 00:00:0", "2020-01-01 00:00:0"),
      (2, "2019-01-01 00:00:0", "2020-01-01 00:00:5"),
      (3, "2020-01-01 00:05:7", "2021-12-31 23:59:59.999"),
      (4, "2020-01-01 00:02:5", "2020-01-01 00:03:5"),
      (5, "2019-01-01 00:00:0", "2020-01-01 00:01:9.999"),
      (6, "2019-01-01 00:00:0", "2021-12-31 23:59:59.999")
    ).map(x => (x._1, Timestamp.valueOf(x._2), Timestamp.valueOf(x._3)))
      .toDF("id", defaultFromColName, defaultToColName)
    val subtrahend = Seq(
      ("2020-01-01 00:04:4", "2020-01-01 00:05:0"),
      ("2020-01-01 00:00:1", "2020-01-01 00:01:0"),
      ("2020-01-01 00:03:3", "2020-01-01 00:04:0"),
      ("2020-01-01 00:05:5", "2020-01-01 00:06:0"),
      ("2020-01-01 00:02:2", "2020-01-01 00:03:0")
    ).map(x => (0, Timestamp.valueOf(x._1), Timestamp.valueOf(x._2)))
      .toDF("id", defaultFromColName, defaultToColName)

    val actual = minuend.rangeLeftAntiJoin(subtrahend, Nil)
    val expected = Seq(
      (1, "2019-01-01 00:00:0",     "2020-01-01 00:00:0"),
      (2, "2019-01-01 00:00:0",     "2020-01-01 00:00:0.999"),
      (3, "2020-01-01 00:06:0.001", "2021-12-31 23:59:59.999"),
      (4, "2020-01-01 00:03:0.001", "2020-01-01 00:03:2.999"),
      (5, "2020-01-01 00:01:0.001", "2020-01-01 00:01:9.999"),
      (5, "2019-01-01 00:00:0",     "2020-01-01 00:00:0.999"),
      (6, "2020-01-01 00:06:0.001", "2021-12-31 23:59:59.999"),
      (6, "2020-01-01 00:05:0.001", "2020-01-01 00:05:4.999"),
      (6, "2020-01-01 00:04:0.001", "2020-01-01 00:04:3.999"),
      (6, "2020-01-01 00:03:0.001", "2020-01-01 00:03:2.999"),
      (6, "2020-01-01 00:01:0.001", "2020-01-01 00:02:1.999"),
      (6, "2019-01-01 00:00:0",     "2020-01-01 00:00:0.999")
    ).map(x => (x._1, Timestamp.valueOf(x._2), Timestamp.valueOf(x._3)))
      .toDF("id", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftAntiJoin_segmented", Seq(minuend, subtrahend))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin_dfRight" should "return expected results" in {
    val actual = dfLeft.rangeFullJoin(dfRight, Seq("id")).rangeCombine()
      .rangeCombine()
      .orderBy($"id", defaultFromCol)
    val expected = Seq(
      // id = 0
      (Some(0), None,      None,        initiumTemporisString, "2017-12-09 23:59:59.999"),
      (Some(0), Some(4.2), None,        "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      (Some(0), Some(4.2), Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (Some(0), Some(4.2), None,        "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
      (Some(0), Some(4.2), Some(97.15), "2018-06-01 05:24:11", "2018-12-08 23:59:59.999"),
      (Some(0), None,      Some(97.15), "2018-12-09 00:00:00", finisTemporisString),
      // id = 1
      (Some(1), None, None,         initiumTemporisString, "2018-12-31 23:59:59.999"),
      (Some(1), None, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
      (Some(1), None, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
      (Some(1), None, None,         "2021-01-01 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int], Option[Double], Option[Double]])
      .toDF("id", "value_l", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin_rightMap" should "return expected results" in {
    // Testing rangeFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeFullJoin(df2 = dfMap, keys = Seq("id"))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None,      initiumTemporisString,     "2017-12-09 23:59:59.999"),
      (Some(0), Some(4.2), None,      "2017-12-10 00:00:00",     "2017-12-31 23:59:59.999"),
      (Some(0), Some(4.2), Some("A"), "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999"),
      (Some(0), Some(4.2), Some("B"), "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (Some(0), Some(4.2), Some("C"), "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (Some(0), Some(4.2), Some("D"), "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999"),
      (Some(0), Some(4.2), Some("X"), "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      (Some(0), Some(4.2), None,      "2018-04-01 00:00:00",     "2018-12-08 23:59:59.999"),
      (Some(0), None,      None,      "2018-12-09 00:00:00",     finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int], Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin_rightMapWithrnkExpressions" should "return expected results" in {
    // Testing rangeFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeFullJoin(df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None, initiumTemporisString, "2017-12-09 23:59:59.999"),
      (Some(0), Some(4.2), None, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      // img = {A}
      (Some(0), Some(4.2), Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      // img = {B}
      (Some(0), Some(4.2), Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      // img = {D}
      (Some(0), Some(4.2), Some("D"), "2018-03-01 00:00:00", "2018-03-31 23:59:59.999"),
      // img = {}
      (Some(0), Some(4.2), None, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999"),
      (Some(0), None,      None, "2018-12-09 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int], Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeFullJoin_rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing rangeFullJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00",     "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    )
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "img")
    val actual =
      dfLeft.rangeFullJoin(df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
        .rangeCombine()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None, initiumTemporisString, "2017-12-09 23:59:59.999"),
      (Some(0), Some(4.2), None, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      // img = {A}
      (Some(0), Some(4.2), Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      // img = {B}
      (Some(0), Some(4.2), Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      // img = null
      (Some(0), Some(4.2), None, "2018-03-01 00:00:00", "2018-03-29 23:59:59.999"),
      // img = {D}
      (Some(0), Some(4.2), Some("D"), "2018-03-30 00:00:00", "2018-03-31 23:59:59.999"),
      // img = {}
      (Some(0), Some(4.2), None, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999"),
      (Some(0), None,      None, "2018-12-09 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int], Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeFullJoin_rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin_dfRight" should "return expected results" in {
    val actual = dfLeft.rangeLeftJoin(dfRight, Seq("id"))
      .rangeCombine()
    val expected = Seq(
      (0, 4.2, None,        "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      (0, 4.2, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, None,        "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
      (0, 4.2, Some(97.15), "2018-06-01 05:24:11", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[Double]])
      .toDF("id", "value_l", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeLeftJoin_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin_dfEmpty" should "return expected results" in {
    val dfEmpty = dfRight.where(lit(false))
    val actual = dfLeft.rangeLeftJoin(dfEmpty, Seq("id"))
    val expected = dfLeft.withColumn("value_r", lit(null).cast("double"))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeLeftJoin_dfEmpty", Seq(dfLeft, dfEmpty))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin_rightMap" should "return expected results" in {
    // Testing rangeLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeLeftJoin(df2 = dfMap, keys = Seq("id"))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, 4.2, None,      "2017-12-10 00:00:00",     "2017-12-31 23:59:59.999"),
      (0, 4.2, Some("A"), "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999"),
      (0, 4.2, Some("B"), "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, 4.2, Some("C"), "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, 4.2, Some("D"), "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999"),
      (0, 4.2, Some("X"), "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123"),
      (0, 4.2, None,      "2018-04-01 00:00:00",     "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeLeftJoin_rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin_rightMapWithrnkExpressions" should "return expected results" in {
    // Testing rangeLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeLeftJoin(df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      // img = {A}
      (0, 4.2, Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      // img = {B}
      (0, 4.2, Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      // img = {D}
      (0, 4.2, Some("D"), "2018-03-01 00:00:00", "2018-03-31 23:59:59.999"),
      // img = {}
      (0, 4.2, None, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeLeftJoin_rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin_rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing rangeLeftJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00",     "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    )
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "img")
    val actual =
      dfLeft.rangeLeftJoin(df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
        .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      // img = {A}
      (0, 4.2, Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      // img = {B}
      (0, 4.2, Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      // img = null
      (0, 4.2, None, "2018-03-01 00:00:00", "2018-03-29 23:59:59.999"),
      // img = {D}
      (0, 4.2, Some("D"), "2018-03-30 00:00:00", "2018-03-31 23:59:59.999"),
      // img = {}
      (0, 4.2, None, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999")
    )
      .map(makeRowsWithTimeRangeEnd[Int, Double, Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeLeftJoin_rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "rangeLeftJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.rangeLeftJoin(dfR, Seq("id"))
      // .rangeCombine() // temporal combine not possible with equally named columns in the same DataFrame.
      .orderBy($"id", defaultFromCol)
    assert(5 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, None,        "2017-12-10 00:00:00", "2017-12-31 23:59:59.999"),
      (0, 4.2, Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, 4.2, None,        "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
      (0, 4.2, Some(97.15), "2018-06-01 05:24:11", "2018-10-23 03:50:09.999"),
      (0, 4.2, Some(97.15), "2018-10-23 03:50:10", "2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Double, Option[Double]])
      .toDF("id", "value", "value", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result)
      printFailedTestResult("rangeLeftJoin with equally named columns apart join columns", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeRightJoin_dfRight" should "return expected results" in {
    val actual = dfLeft.rangeRightJoin(dfRight, Seq("id"))
      .rangeCombine()
    val expected = Seq(
      // id = 0
      (0, Some(4.2), Some(97.15), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, Some(4.2), Some(97.15), "2018-06-01 05:24:11", "2018-12-08 23:59:59.999"),
      (0, None,      Some(97.15), "2018-12-09 00:00:00", finisTemporisString),
      // id = 1
      (1, None, None,         "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
      (1, None, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
      (1, None, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
      (1, None, None,         "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Option[Double]])
      .toDF("id", "value_l", "value_r", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeRightJoin_dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeRightJoin_rightMap" should "return expected results" in {
    // Testing rangeRightJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeRightJoin(df2 = dfMap, keys = Seq("id"))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999"),
      (0, Some(4.2), Some("B"), "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("C"), "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("D"), "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999"),
      (0, Some(4.2), Some("X"), "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeRightJoin_rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeRightJoin_rightMapWithrnkExpressions" should "return expected results" in {
    // Testing rangeRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    val actual = dfLeft.rangeRightJoin(df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
      .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999"),
      (0, Some(4.2), Some("B"), "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("C"), "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("D"), "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999"),
      (0, Some(4.2), Some("X"), "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeRightJoin_rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "rangeRightJoin_rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing rangeRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    // and gaps are of the left frame only are filled
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00",     "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    )
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultFromColName, defaultToColName, "img")

    val actual =
      dfLeft.rangeRightJoin(df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultFromCol))
        .rangeCombine()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999"),
      (0, Some(4.2), Some("B"), "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("C"), "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999"),
      (0, Some(4.2), Some("D"), "2018-03-30 00:00:00",     "2018-03-31 23:59:59.999"),
      (0, Some(4.2), Some("X"), "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int, Option[Double], Option[String]])
      .toDF("id", "value_l", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result)
      printFailedTestResult("rangeRightJoin_rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
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
    val rowsExpected: Seq[(Int, String, String, Double)] = Seq(
      (0, "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, "2019-02-01 01:00:0",      "2019-02-01 02:34:56.124", 2.72),
      (0, "2019-02-01 02:34:56.125", "2019-03-03 00:00:0",      13.0),
      (0, "2019-03-03 00:00:0",      "2019-04-04 00:00:0",      13.0),
      (0, "2020-01-01 01:00:0",      finisTemporisString,       18.17),
      (1, "2019-03-01 00:00:0",      "2019-03-01 00:00:0",      0.1), // duration extended to 1 millisecond
      (1, "2019-03-01 00:00:0.001",  "2019-03-01 00:00:0.001",  0.1), // duration extended to 1 millisecond
      (1, "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002", 1.2), // duration extended to 2 milliseconds
      (1, "2019-01-01 00:00:0.124",  "2019-02-02 00:00:0",      -1.0),
      (1, "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1",   -2.0)
    )
    val expected =
      rowsExpected.map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultFromColName, defaultToColName,
        "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeRoundDiscreteTime", Seq(dfDirtyTimeRanges))(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime and rangeCombine" should "combine dfDirtyTimeRanges" in {
    val actual = dfDirtyTimeRanges.rangeRoundDiscreteTime.rangeCombine()
    val rowsExpected = Seq(
      (0, "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72),
      (0, "2019-02-01 02:34:56.125", "2019-04-04 00:00:0",      13.0),
      (0, "2020-01-01 01:00:0",      finisTemporisString,       18.17),
      (1, "2019-03-01 00:00:0",      "2019-03-01 00:00:0.001",  0.1), // duration extended to 2 milliseconds
      (1, "2019-03-01 00:00:1.001",  "2019-03-01 00:00:01.002", 1.2), // duration extended to 2 milliseconds
      (1, "2019-01-01 00:00:00.124", "2019-02-02 00:00:00",     -1.0),
      (1, "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1",   -2.0)
    )
    val expected =
      rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_dirtyTimeRanges", dfDirtyTimeRanges)(actual, expected)
    result shouldBe true
  }

  "rangeRoundDiscreteTime and rangeCombine" should "combine dfDocumentation" in {
    val actual = dfDocumentation.rangeRoundDiscreteTime.rangeCombine()
    val rowsExpected = Seq(
      (1, "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124", 2.72), // overlaps with previous record
      (1, "2019-01-01 00:00:0",      "2019-12-31 23:59:59.999", 42.0)
    )
    val expected =
      rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeCombine_documentation", dfDocumentation)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "not modify dfMoment as extend and fillGapsWithNull are false" in {
    val actual = dfMoment.rangeUnifyRanges(Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment
    logger.info("expected:")
    expected.show(false)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "extend dfMoment" in {
    val actual = dfMoment.rangeUnifyRanges(keys = Seq("id"), extend = true, fillGapsWithNull = true)
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = List(
      (0, "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", Some("A")),
      (0, initiumTemporisString,     "2019-11-25 11:12:13.004", None),
      (0, "2019-11-25 11:12:13.006", finisTemporisString,       None)
    )
      .map(makeRowsWithTimeRange).toDF("id", defaultFromColName, defaultToColName, "img")
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges dfMsOverlap" should "return expected results" in {
    val actual = dfMsOverlap.rangeUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", "2019-01-01 00:00:00",     "2019-01-01 9:59:59.999"),
      (0, "A", "2019-01-01 10:00:00",     "2019-01-01 10:00:00"),
      (0, "B", "2019-01-01 10:00:00",     "2019-01-01 10:00:00"),
      (0, "B", "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeUnifyRanges dfMsOverlap", dfMsOverlap)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges" should "split properly the ranges of dfMap" in {
    val actual = dfMap.rangeUnifyRanges(Seq("id"))
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
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "img", defaultFromColName, defaultToColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("rangeUnifyRanges dfMap", dfMap)(actual, expected)
    result shouldBe true
  }

  "rangeUnifyRanges dfMicrosecTimeRanges" should
    "show what happens when time is more precise than the granularity of 1ms " in {
      logger.info(
        "\n*** Educational test case to highlight the behaviour of rangeUnifyRanges when the time has a granularity of smaller than 1ms. ***"
      )
      logger.info("\n*** Argument = ")
      dfMicrosecTimeRanges.orderBy("id", "valid_from").show(false)
      val actual = dfMicrosecTimeRanges.rangeUnifyRanges(Seq("id"))
      logger.info("\n*** Argument.rangeUnifyRanges(Seq(\"id\")) = ")
      actual.orderBy("id", "valid_from").show(false)
      val expected = Seq(
        (0, 3.14, "2018-06-01 00:00:00       ", "2018-06-01 09:00:00"),
        (0, 42.0, "2018-06-01 09:00:00.000124", "2018-06-01 09:00:00"),
        (0, 2.72, "2018-06-01 09:00:00.000130", "2018-06-01 09:00:00"),
        (0, 2.72, "2018-06-01 09:00:00.001",    "2018-06-01 17:00:00.123")
      ).map(makeRowsWithTimeRangeEnd[Int, Double])
        .toDF("id", "value", defaultFromColName, defaultToColName)
      val result = dfEqual(actual, expected)

      if (!result) printFailedTestResult("rangeUnifyRanges dfMicrosecTimeRanges", dfMicrosecTimeRanges)(actual, expected)
      result shouldBe true
    }

}
