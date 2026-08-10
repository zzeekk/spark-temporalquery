package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.linear.LinearDoubleTestUtils._
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LinearDoubleQueryUtilTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  logger.info(s"LinearDoubleQueryUtilTest: defaultLinearConfig = $defaultLinearConfig")

  "linear join condition symmetricity of half-open intervals" should "return expected results" in {
    val df1 = List((1, 1d, 2d)).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val df2 = List((1, 2d, 3d)).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    df1.rangeInnerJoin[Double](df2, Seq("id")).isEmpty && df2.rangeInnerJoin[Double](df1, Seq("id")).isEmpty shouldBe true
  }

  "linearCleanupExtend dfLeft" should "return expected results" in {
    val actual = dfLeft.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq(defaultLinearConfig.fromCol))
      .rangeCombine[Double]()
      .orderBy(defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,      false, intervalMinValue, 171210d),
      (0, Some(4.2), true,  171210d,          181209d),
      (0, None,      false, 181209d,          intervalMaxValue)
    ).toDF("id", "value_l", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("temporalCleanupExtend_dfLeft", dfLeft)(reorderCols(actual, expected), expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_noExtend_nofillGaps" should "return expected results" in {
    val actual = dfRight.rangeCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      extend = false,
      fillGapsWithNull = false
    ).rangeCombine[Double]()
    val expected = Seq(
      (0, Some(97.15), 180101d,       180201d),
      (0, Some(97.15), 180601.052411, intervalMaxValue),
      (1, None,        180101d,       190101d),
      (1, Some(2019d), 190101d,       200101d),
      (1, Some(2020d), 200101d,       210101d),
      (1, None,        210101d,       intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("temporalCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_fillGaps_noExtend" should "return expected results" in {
    val actual = dfRight.rangeCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      extend = false
    ).rangeCombine[Double]()
    val expected = Seq(
      (0, Some(97.15), true,  180101d,       180201d),
      (0, None,        false, 180201d,       180601.052411),
      (0, Some(97.15), true,  180601.052411, intervalMaxValue),
      (1, None,        true,  180101d,       190101d),
      (1, Some(2019d), true,  190101d,       200101d),
      (1, Some(2020d), true,  200101d,       210101d),
      (1, None,        true,  210101d,       intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_fillGaps_noExtend", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_extend_nofillGaps" should "return expected results" in {
    val actual = dfRight.rangeCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      fillGapsWithNull = false
    ).rangeCombine[Double]()
    val expected = Seq(
      (0, Some(97.15), 180101d,       180201d),
      (0, Some(97.15), 180601.052411, intervalMaxValue),
      (1, None,        180101d,       190101d),
      (1, Some(2019d), 190101d,       200101d),
      (1, Some(2020d), 200101d,       210101d),
      (1, None,        210101d,       intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_extend_nofillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_extend_fillGaps" should "return expected results" in {
    val actual = dfRight.rangeCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol)
    ).rangeCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,        false, intervalMinValue, 180101d),
      (0, Some(97.15), true,  180101d,          180201d),
      (0, None,        false, 180201d,          180601.052411),
      (0, Some(97.15), true,  180601.052411,    intervalMaxValue),
      (1, None,        false, intervalMinValue, 180101d),
      (1, None,        true,  180101d,          190101d),
      (1, Some(2019d), true,  190101d,          200101d),
      (1, Some(2020d), true,  200101d,          210101d),
      (1, None,        true,  210101d,          intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_extend_fillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfMap" should "return expected results" in {
    val actual = dfMap.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq($"img"))
      .rangeCombine[Double]()
    val expected = Seq(
      (0, None,      false, intervalMinValue, 180101d),
      (0, Some("A"), true,  180101d,          180201d),
      (0, Some("B"), true,  180201d,          180301d),
      (0, Some("D"), true,  180301d,          180401d),
      (0, None,      false, 180401d,          intervalMaxValue)
    ).toDF("id", "img", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfMap", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfMap_NoExtendFillgaps" should "return expected results" in {
    val actual = dfMap.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq($"img"),
      extend = false, fillGapsWithNull = false)
      .rangeCombine[Double]()
    val expected = Seq(
      (0, Some("A"), 180101d, 180201d),
      (0, Some("B"), 180201d, 180301d),
      (0, Some("D"), 180301d, 180401d)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfMap_NoExtendFillgaps", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfSmallOverlap" should "return expected results" in {
    val actual = dfSmallOverlap.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq(defaultLinearConfig.fromCol))
      .rangeCombine[Double]()
    val expected = Seq(
      (0, None,      false, intervalMinValue, 190101d),
      (0, Some("A"), true,  190101d,          190101.100001),
      (0, Some("B"), true,  190101.100001,    190102d),
      (0, None,      false, 190102d,          intervalMaxValue)
    ).toDF("id", "img", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfSmallOverlap", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfDirtyIntervals" should "return expected results" in {
    val actual = dfDirtyIntervals.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq(defaultLinearConfig.fromCol, $"value"))
      .rangeCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,        false, intervalMinValue,   190101.00000012346),
      (0, Some(3.14),  true,  190101.00000012346, 190105.12345612346),
      (0, Some(2.72),  true,  190105.12345612346, 190201.0234561245),
      (0, Some(13d),   true,  190201.0234561245,  190404d),
      (0, None,        false, 190404d,            190905.0234561231),
      (0, Some(42d),   true,  190905.0234561231,  190905.0234561239),
      (0, None,        false, 190905.0234561239,  200101.01),
      (0, Some(18.17), true,  200101.01,          intervalMaxValue),
      (1, None,        false, intervalMinValue,   190101.00000012346),
      (1, Some(-1d),   true,  190101.00000012346, 190202d),
      (1, None,        false, 190202d,            190301d),
      (1, Some(0.1),   true,  190301d,            190301.0000000002),
      (1, Some(0.8),   true,  190301.0000000002,  190301.000000001),
      (1, Some(0.1),   true,  190301.000000001,   190301.000000002),
      (1, None,        false, 190301.000000002,   190301.0000010009),
      (1, Some(1.2),   true,  190301.0000010009,  190301.0000010021),
      (1, None,        false, 190301.0000010021,  190303.01000),
      (1, Some(-2d),   true,  190303.01000,       211201.0234561),
      (1, None,        false, 211201.0234561,     intervalMaxValue)
    ).toDF("id", "value", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("v dfDirtyIntervals", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps" should "return expected results" in {
    val actual =
      dfDirtyIntervals.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq(defaultLinearConfig.fromCol, $"value"),
        extend = false, fillGapsWithNull = false)
        .rangeCombine[Double]()
        .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, 3.14,  190101.00000012346, 190105.12345612346),
      (0, 2.72,  190105.12345612346, 190201.0234561245),
      (0, 13d,   190201.0234561245,  190404d),
      (0, 42d,   190905.0234561231,  190905.0234561239),
      (0, 18.17, 200101.01000,       intervalMaxValue),
      (1, -1d,   190101.00000012346, 190202d),
      (1, 0.1,   190301d,            190301.0000000002),
      (1, 0.8,   190301.0000000002,  190301.000000001),
      (1, 0.1,   190301.000000001,   190301.000000002),
      (1, 1.2,   190301.0000010009,  190301.0000010021),
      (1, -2d,   190303.01000,       211201.0234561)
    ).toDF("id", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend validityDuration" should "return expected results" in {
    val argument = Seq(
      (1, "A", 200701d, 200704d),
      (1, "A", 200705d, 200708d),
      (1, "B", 200701d, 200703d),
      (1, "B", 200704d, 200708d)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    // we want the record with the longest interval, i.e. maximal toColName-fromColName
    val actual = argument.rangeCleanupExtend[Double](keys = Seq("id"),
      rnkExpressions = Seq((defaultLinearConfig.toCol - defaultLinearConfig.fromCol).desc))
      .rangeCombine[Double]()
    val expected = Seq(
      (1, None,      false, intervalMinValue, 200701d),
      (1, Some("A"), true,  200701d,          200704d),
      (1, Some("B"), true,  200704d,          200708d),
      (1, None,      false, 200708d,          intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend validityDuration", argument)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend rankExprFromColOnly" should "return expected results" in {
    val argument = Seq(
      (1, "S", intervalMinValue, intervalMaxValue),
      (1, "X", 200701d,          intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val actual = argument.rangeCleanupExtend[Double](keys = Seq("id"), rnkExpressions = Seq(defaultLinearConfig.fromCol))
      .rangeCombine[Double]()
    val expected = Seq(
      (1, "S", intervalMinValue, intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend rankExprFromColOnly", argument)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend rankExpr2Cols" should "return expected results" in {
    val argument = Seq(
      (1, "S", intervalMinValue, 200701d),
      (1, "X", 200701d,          200924d),
      (1, "B", 200803d,          intervalMaxValue),
      (1, "G", 200924d,          intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val actual = argument
      .rangeCleanupExtend[Double](keys = Seq("id"),
        rnkExpressions = Seq(defaultLinearConfig.toCol.desc, defaultLinearConfig.fromCol.asc))
      .rangeCombine[Double]()
    val expected = Seq(
      (1, "S", intervalMinValue, 200701d),
      (1, "X", 200701d,          200803d),
      (1, "B", 200803d,          intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result2 = dfEqual(actual, expected)
    if (!result2) printFailedTestResult("linearCleanupExtend rankExpr2Cols", argument)(actual, expected)
    assert(result2)
  }

  "linearExtendRange dfLeft" should "return expected results" in {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.rangeExtendRange[Double](Seq("id"))
    val rowsExpected = Seq((0, 4.2, intervalMinValue, intervalMaxValue))
    val expected = rowsExpected.toDF("id", "value_L", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)

    if (!result) printFailedTestResult("linearExtendRange dfLeft", dfLeft)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "linearExtendRange dfRight_id" should "return expected results" in {
    val actual = dfRight.rangeExtendRange[Double](Seq("id"))
    val expected = Seq(
      (0, Some(97.15), intervalMinValue, 180201d),
      (0, Some(97.15), 180601.0524110,   181023.035010),
      (0, Some(97.15), 181023.035010,    200101d),
      (0, Some(97.15), 200101d,          intervalMaxValue),
      (1, None,        intervalMinValue, 190101d),
      (1, Some(2019d), 190101.0000000,   200101d),
      (1, Some(2020d), 200101.0000000,   210101d),
      (1, None,        210101.0000000,   intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("linearExtendRange dfRight_id", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "linearExtendRange dfRight" should "return expected results" in {
    // argument: dfRight from object TestUtils
    val actual = dfRight.rangeExtendRange[Double]()
    val expected = Seq(
      (0, Some(97.15), intervalMinValue, 180201d),
      (0, Some(97.15), 180601.0524110,   181023.035010),
      (0, Some(97.15), 181023.035010,    200101d),
      (0, Some(97.15), 200101d,          intervalMaxValue),
      (1, None,        intervalMinValue, 190101d),
      (1, Some(2019d), 190101.0000000,   200101d),
      (1, Some(2020d), 200101.0000000,   210101d),
      (1, None,        210101.0000000,   intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("linearExtendRange dfRight", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "rangeInnerJoin dfRight 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin[Double](dfRight.as("dfR"), $"dfL.id" === $"dfR.id")
    assert(actual.columns.count(_ == "id") == 2)
    val expected = Seq(
      (0, 4.2, 0, Some(97.15), 180101d,       180201d),
      (0, 4.2, 0, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, 0, Some(97.15), 181023.035010, 181209d)
    ).toDF("id", "value_l", "id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight 'on' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin dfRight with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").rangeInnerJoin[Double](dfRight.as("dfR"), Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value_l", $"dfR.value_r").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), 180101d,       180201d),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209d)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeInnerJoin dfRight with 'using' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "rangeInnerJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.rangeInnerJoin[Double](dfR, Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), 180101d,       180201d),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209d)
    ).toDF("id", "value", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("temporalInnerJoin with equally named columns apart join columns", Seq(dfL, dfR))(actual, expected)
    result shouldBe true
  }

  "linearLeftAntiJoin dfRight" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin[Double](df2 = dfRight, joinColumns = Seq("id"))
    val expected = Seq(
      (0, 171210d, 180101d,       4.2),
      (0, 180201d, 180601.052411, 4.2)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftAntiJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftAntiJoin dfMap" should "return expected results" in {
    val actual = dfLeft.rangeLeftAntiJoin[Double](dfMap, Seq("id"))
    val expected = Seq(
      (0, 171210d, 180101d, 4.2),
      (0, 180401d, 181209d, 4.2)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_l")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftAntiJoin dfMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftAntiJoin dfRight_dfMap" should "return expected results" in {
    val actual = dfRight.rangeLeftAntiJoin[Double](dfMap, Seq("id"))
    val expected = Seq(
      (0, 180601.052411, intervalMaxValue, Some(97.15)),
      (1, 180101d,       190101d,          None),
      (1, 190101d,       200101d,          Some(2019d)),
      (1, 200101d,       210101d,          Some(2020d)),
      (1, 210101d,       intervalMaxValue, None)
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftAntiJoin dfRight_dfMap", Seq(dfRight, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftAntiJoin dfMap_dfRight" should "return expected results" in {
    val actual = dfMap.rangeLeftAntiJoin[Double](dfRight, Seq("id"))
    val expected = Seq(
      (0, 180201d, 180301d, "B"),
      (0, 180201d, 180301d, "C"),
      (0, 180220d, 180401d, "D")
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("temporalLeftAntiJoin_dfMap_dfRight", Seq(dfMap, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftAntiJoin segmented" should "return expected results" in {
    val minuend = Seq(
      (1, 190101d,      200101d),
      (2, 190101d,      200101.00005),
      (3, 200101.00057, 220101d),
      (4, 200101.00025, 200101.00035),
      (5, 190101d,      200101.000109999),
      (6, 190101d,      220101d)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val subtrahend = Seq(
      (0, 200101.00001, 200101.0001),
      (0, 200101.00022, 200101.0003),
      (0, 200101.00033, 200101.0004),
      (0, 200101.00044, 200101.0005),
      (0, 200101.00055, 200101.0006)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)

    val actual = minuend.rangeLeftAntiJoin[Double](df2 = subtrahend, joinColumns = Nil)
    val expected = Seq(
      (1, 190101d,     200101d),
      (2, 190101d,     200101.00001),
      (3, 200101.0006, 220101d),
      (4, 200101.0003, 200101.00033),
      (5, 190101d,     200101.00001),
      (5, 200101.0001, 200101.000109999),
      (6, 190101d,     200101.00001),
      (6, 200101.0001, 200101.00022),
      (6, 200101.0003, 200101.00033),
      (6, 200101.0004, 200101.00044),
      (6, 200101.0005, 200101.00055),
      (6, 200101.0006, 220101d)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftAntiJoin segmented", Seq(minuend, subtrahend))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin dfRight" should "return expected results" in {
    val actual = dfLeft.rangeFullJoin[Double](dfRight, Seq("id")).rangeCombine[Double]()
      .rangeCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      // id = 0
      (0, Some(4.2), None,        171210d,       180101d),
      (0, Some(4.2), Some(97.15), 180101d,       180201d),
      (0, Some(4.2), None,        180201d,       180601.052411),
      (0, Some(4.2), Some(97.15), 180601.052411, 181209d),
      (0, None,      Some(97.15), 181209d,       intervalMaxValue),
      // id = 1
      (1, None, Some(2019d), 190101d, 200101d),
      (1, None, Some(2020d), 200101d, 210101d),
      (1, None, None,        180101d, 190101d),
      (1, None, None,        210101d, intervalMaxValue)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMap" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeFullJoin[Double](df2 = dfMap, keys = Seq("id"))
      .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), None,      171210d, 180101d),
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      (0, Some(4.2), Some("B"), 180101d, 180301d),
      (0, Some(4.2), Some("C"), 180201d, 180301d),
      (0, Some(4.2), Some("D"), 180220d, 180401d),
      (0, Some(4.2), None,      180401d, 181209d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual =
      dfLeft.rangeFullJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), None, 171210d, 180101d),
      // img = {A}
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      // img = {B}
      (0, Some(4.2), Some("B"), 180201d, 180301d),
      // img = {D}
      (0, Some(4.2), Some("D"), 180301d, 180401d),
      // img = {}
      (0, Some(4.2), None, 180401d, 181209d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, 180101d,          180201d,          "A"),
      (0, 180101d,          180301d,          "B"),
      (0, 180201d,          180301d,          "C"),
      (0, 180330d,          180401d,          "D"),
      (0, 180225.141516123, 180225.141516123, "X")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val actual =
      dfLeft.rangeFullJoin[Double](df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), None, 171210d, 180101d),
      // img = {A}
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      // img = {B}
      (0, Some(4.2), Some("B"), 180201d, 180301d),
      // img = null
      (0, Some(4.2), None, 180301d, 180330d),
      // img = {D}
      (0, Some(4.2), Some("D"), 180330d, 180401d),
      // img = {}
      (0, Some(4.2), None, 180401d, 181209d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin dfRight" should "return expected results" in {
    val actual = dfLeft.rangeLeftJoin[Double](dfRight, Seq("id"))
      .rangeCombine[Double]()
    val expected = Seq(
      (0, 4.2, None,        171210d,       180101d),
      (0, 4.2, Some(97.15), 180101d,       180201d),
      (0, 4.2, None,        180201d,       180601.052411),
      (0, 4.2, Some(97.15), 180601.052411, 181209d)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMap" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeLeftJoin[Double](df2 = dfMap, keys = Seq("id"))
      .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None,      171210d, 180101d),
      (0, 4.2, Some("A"), 180101d, 180201d),
      (0, 4.2, Some("B"), 180101d, 180301d),
      (0, 4.2, Some("C"), 180201d, 180301d),
      (0, 4.2, Some("D"), 180220d, 180401d),
      (0, 4.2, None,      180401d, 181209d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual =
      dfLeft.rangeLeftJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, 171210d, 180101d),
      // img = {A}
      (0, 4.2, Some("A"), 180101d, 180201d),
      // img = {B}
      (0, 4.2, Some("B"), 180201d, 180301d),
      // img = {D}
      (0, 4.2, Some("D"), 180301d, 180401d),
      // img = {}
      (0, 4.2, None, 180401d, 181209d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftJoin rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, 180101d,          180201d,          "A"),
      (0, 180101d,          180301d,          "B"),
      (0, 180201d,          180301d,          "C"),
      (0, 180330d,          180401d,          "D"),
      (0, 180225.141516123, 180225.141516123, "X")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val actual =
      dfLeft.rangeLeftJoin[Double](df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, 171210d, 180101d),
      // img = {A}
      (0, 4.2, Some("A"), 180101d, 180201d),
      // img = {B}
      (0, 4.2, Some("B"), 180201d, 180301d),
      // img = null
      (0, 4.2, None, 180301d, 180330d),
      // img = {D}
      (0, 4.2, Some("D"), 180330d, 180401d),
      // img = {}
      (0, 4.2, None, 180401d, 181209d)
    )
      .toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.rangeLeftJoin[Double](dfR, Seq("id"))
      // .intervalCombine[Double]() // temporal combine not possible with equally named columns in the same DataFrame.
      .orderBy($"id", defaultLinearConfig.fromCol)
    assert(5 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, None,        171210d,       180101d),
      (0, 4.2, Some(97.15), 180101d,       180201d),
      (0, 4.2, None,        180201d,       180601.052411),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209d)
    ).toDF("id", "value", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result)
      printFailedTestResult("linearLeftJoin with equally named columns apart join columns", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin dfRight" should "return expected results" in {
    val actual = dfLeft.rangeRightJoin[Double](dfRight, Seq("id"))
      .rangeCombine[Double]()
    val expected = Seq(
      // id = 0
      (0, Some(4.2), Some(97.15), 180101d,       180201d),
      (0, Some(4.2), Some(97.15), 180601.052411, 181209d),
      (0, None,      Some(97.15), 181209d,       intervalMaxValue),
      // id = 1
      (1, None, None,        180101d, 190101d),
      (1, None, Some(2019d), 190101d, 200101d),
      (1, None, Some(2020d), 200101d, 210101d),
      (1, None, None,        210101d, intervalMaxValue)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearRightJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin rightMap" should "return expected results" in {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.rangeRightJoin[Double](df2 = dfMap, keys = Seq("id"))
      .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      (0, Some(4.2), Some("B"), 180101d, 180301d),
      (0, Some(4.2), Some("C"), 180201d, 180301d),
      (0, Some(4.2), Some("D"), 180220d, 180401d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("temporalRightJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    val actual =
      dfLeft.rangeRightJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      (0, Some(4.2), Some("B"), 180101d, 180301d),
      (0, Some(4.2), Some("C"), 180201d, 180301d),
      (0, Some(4.2), Some("D"), 180220d, 180401d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearRightJoin rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    // and gaps are of the left frame only are filled
    val argumentRight = Seq(
      (0, 180101d, 180201d, "A"),
      (0, 180101d, 180301d, "B"),
      (0, 180201d, 180301d, "C"),
      (0, 180330d, 180401d, "D")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")

    val actual =
      dfLeft.rangeRightJoin[Double](df2 = argumentRight, keys = Seq("id"),
        rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .rangeCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101d, 180201d),
      (0, Some(4.2), Some("B"), 180101d, 180301d),
      (0, Some(4.2), Some("C"), 180201d, 180301d),
      (0, Some(4.2), Some("D"), 180330d, 180401d)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearRightJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearCombine dfRight" should "return expected results" in {
    val actual = dfRight.rangeCombine[Double]()
    val expected = Seq(
      (0, 180101.0000000, 180201d,          Some(97.15)),
      (0, 180601.0524110, intervalMaxValue, Some(97.15)),
      (1, 180101.0000000, 190101d,          None),
      (1, 190101.0000000, 200101d,          Some(2019d)),
      (1, 200101.0000000, 210101d,          Some(2020d)),
      (1, 210101.0000000, intervalMaxValue, None)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dfRight", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCombine dropped column" should "return expected results" in {
    val actual = dfRight
      .withColumn("test_column", lit("please drop me"))
      .drop("test_column")
      .rangeCombine[Double]()
    val expected = Seq(
      (0, 180101.0000000, 180201d,          Some(97.15)),
      (0, 180601.0524110, intervalMaxValue, Some(97.15)),
      (1, 180101.0000000, 190101d,          None),
      (1, 190101.0000000, 200101d,          Some(2019d)),
      (1, 200101.0000000, 210101d,          Some(2020d)),
      (1, 210101.0000000, intervalMaxValue, None)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dropped column", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCombine dfMapToCombine" should "return expected results" in {
    val actual = dfMapToCombine.rangeCombine[Double]()
    val expected = Seq(
      (0, 180101d, 190101d, Some("A")),
      (0, 180101d, 180204d, Some("B")),
      (0, 180201d, 200501d, None),
      (0, 200601d, 210101d, None),
      (1, 180201d, 200501d, Some("one")),
      (1, 200601d, 210101d, Some("one")),
      (0, 180220d, 180401d, Some("D"))
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dfMapToCombine", dfMapToCombine)(actual, expected)
    result shouldBe true
  }

  "linearCombine dfDirtyIntervals" should "return expected results" in {
    val actual = dfDirtyIntervals.rangeCombine[Double]()
    val expected = Seq(
      (0, 190101.00000012346, 190105.12345612346, 3.14),
      (0, 190105.12345612346, 190201.0234561245,  2.72),
      (0, 190201.0234561245,  190404d,            13d),
      (0, 190905.0234561231,  190905.0234561239,  42d),
      (0, 200101.01000,       intervalMaxValue,   18.17),
      (1, 190101.00000012346, 190202d,            -1d),
      (1, 190301d,            190301.0000000002,  0.1),
      (1, 190301.0000000009,  190301.000000002,   0.1),
      (1, 190301.0000010009,  190301.0000010021,  1.2),
      (1, 190301.0000000001,  190301.000000001,   0.8),
      (1, 190303.01000,       211201.0234561,     -2d)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dfDirtyIntervals", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCombine documentation" should "return expected results" in {
    val actual = dfDocumentation.rangeCombine[Double]()
    val expected = Seq(
      (1, 190105.12345612346, 190201.0234561245, 2.72), // overlaps with previous record
      (1, 190101d,            200101d,           42d)
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine documentation", dfDocumentation)(actual, expected)
    result shouldBe true
  }

  "linearUnifyRanges dfMoment" should "return expected results" in {
    // Note that a Moment can not be modeled with HalfOpenInterval - result is therefore empty
    val actual = dfMoment.rangeUnifyRanges[Double](Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment.where(lit(false)) // empty data frame expected
    logger.info("expected:")
    expected.show(false)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "temporalUnifyRanges dfSmallOverlap" should "return expected results" in {
    val actual = dfSmallOverlap.rangeUnifyRanges[Double](Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", 190101d,       190101.100000),
      (0, "A", 190101.100000, 190101.100001),
      (0, "B", 190101.100000, 190101.100001),
      (0, "B", 190101.100001, 190102d)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearUnifyRanges dfSmallOverlap", dfSmallOverlap)(actual, expected)
    result shouldBe true
  }

  "linearUnifyRanges dfMap" should "return expected results" in {
    val actual = dfMap.rangeUnifyRanges[Double](Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", 180101d, 180201d),
      (0, "B", 180101d, 180201d),
      // img = {B,C}
      (0, "B", 180201d, 180220d),
      (0, "C", 180201d, 180220d),
      // img = {B,C,D}
      (0, "B", 180220d, 180225.141516123),
      (0, "C", 180220d, 180225.141516123),
      (0, "D", 180220d, 180225.141516123),
      // img = {B,C,D}
      (0, "B", 180225.141516123, 180301d),
      (0, "C", 180225.141516123, 180301d),
      (0, "D", 180225.141516123, 180301d),
      // img = {D}
      (0, "D", 180301d, 180401d)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearUnifyRanges dfMap", dfMap)(actual, expected)
    result shouldBe true
  }
}
