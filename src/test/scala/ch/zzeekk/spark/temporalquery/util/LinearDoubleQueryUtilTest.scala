package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.LinearDoubleTestUtils._
import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.util.IntervalLibrary.IntervalDataFrameExtensions
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LinearDoubleQueryUtilTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  logger.info(s"LinearDoubleQueryUtilTest: defaultLinearConfig = $defaultLinearConfig")

  "linear join condition symmetricity of half-open intervals" should "return expected results" in {
    val df1 = List((1, 1d, 2d)).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val df2 = List((1, 2d, 3d)).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    df1.intervalInnerJoin[Double](df2, Seq("id")).isEmpty && df2.intervalInnerJoin[Double](df1, Seq("id")).isEmpty shouldBe true
  }

  "linearCleanupExtend dfLeft" should "return expected results" in {
    val actual = dfLeft.intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
      .orderBy(defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,      false, intervalMinValue, 171210.0),
      (0, Some(4.2), true,  171210.000000,    181209.0),
      (0, None,      false, 181209.000000,    intervalMaxValue)
    ).toDF("id", "value_l", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("temporalCleanupExtend_dfLeft", dfLeft)(reorderCols(actual, expected), expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_noExtend_nofillGaps" should "return expected results" in {
    val actual = dfRight.intervalCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      extend = false,
      fillGapsWithNull = false
    ).intervalCombine[Double]()
    val expected = Seq(
      (0, Some(97.15),  180101.000000, 180201.0),
      (0, Some(97.15),  180601.052411, intervalMaxValue),
      (1, None,         180101.000000, 190101.0),
      (1, Some(2019.0), 190101.000000, 200101.0),
      (1, Some(2020.0), 200101.000000, 210101.0),
      (1, None,         210101.000000, intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("temporalCleanupExtend_dfRight_noExtend_nofillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_fillGaps_noExtend" should "return expected results" in {
    val actual = dfRight.intervalCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      extend = false
    ).intervalCombine[Double]()
    val expected = Seq(
      (0, Some(97.15),  true,  180101.000000, 180201.0),
      (0, None,         false, 180201.000000, 180601.052411),
      (0, Some(97.15),  true,  180601.052411, intervalMaxValue),
      (1, None,         true,  180101.000000, 190101.0),
      (1, Some(2019.0), true,  190101.000000, 200101.0),
      (1, Some(2020.0), true,  200101.000000, 210101.0),
      (1, None,         true,  210101.000000, intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_fillGaps_noExtend", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_extend_nofillGaps" should "return expected results" in {
    val actual = dfRight.intervalCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol),
      fillGapsWithNull = false
    ).intervalCombine[Double]()
    val expected = Seq(
      (0, Some(97.15),  180101.000000, 180201.0),
      (0, Some(97.15),  180601.052411, intervalMaxValue),
      (1, None,         180101.000000, 190101.0),
      (1, Some(2019.0), 190101.000000, 200101.0),
      (1, Some(2020.0), 200101.000000, 210101.0),
      (1, None,         210101.000000, intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_extend_nofillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfRight_extend_fillGaps" should "return expected results" in {
    val actual = dfRight.intervalCleanupExtend[Double](
      keys = Seq("id"),
      rnkExpressions = Seq(defaultLinearConfig.fromCol)
    ).intervalCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,         false, intervalMinValue, 180101.0),
      (0, Some(97.15),  true,  180101.000000,    180201.0),
      (0, None,         false, 180201.000000,    180601.052411),
      (0, Some(97.15),  true,  180601.052411,    intervalMaxValue),
      (1, None,         false, intervalMinValue, 180101.0),
      (1, None,         true,  180101.000000,    190101.0),
      (1, Some(2019.0), true,  190101.000000,    200101.0),
      (1, Some(2020.0), true,  200101.000000,    210101.0),
      (1, None,         true,  210101.000000,    intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfRight_extend_fillGaps", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfMap" should "return expected results" in {
    val actual = dfMap.intervalCleanupExtend[Double](Seq("id"), Seq($"img"))
      .intervalCombine[Double]()
    val expected = Seq(
      (0, None,      false, intervalMinValue, 180101.0),
      (0, Some("A"), true,  180101.000000,    180201.0),
      (0, Some("B"), true,  180201.000000,    180301.0),
      (0, Some("D"), true,  180301.000000,    180401.0),
      (0, None,      false, 180401.000000,    intervalMaxValue)
    ).toDF("id", "img", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfMap", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfMap_NoExtendFillgaps" should "return expected results" in {
    val actual = dfMap.intervalCleanupExtend[Double](Seq("id"), Seq($"img"), extend = false, fillGapsWithNull = false)
      .intervalCombine[Double]()
    val expected = Seq(
      (0, Some("A"), 180101.000000, 180201.0),
      (0, Some("B"), 180201.000000, 180301.0),
      (0, Some("D"), 180301.000000, 180401.0)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfMap_NoExtendFillgaps", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfSmallOverlap" should "return expected results" in {
    val actual = dfSmallOverlap.intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
    val expected = Seq(
      (0, None,      false, intervalMinValue, 190101.0),
      (0, Some("A"), true,  190101.000000,    190101.100001),
      (0, Some("B"), true,  190101.100001,    190102.0),
      (0, None,      false, 190102.000000,    intervalMaxValue)
    ).toDF("id", "img", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend dfSmallOverlap", dfMap)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfDirtyIntervals" should "return expected results" in {
    val actual = dfDirtyIntervals.intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.fromCol, $"value"))
      .intervalCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, None,        false, intervalMinValue,   190101.00000012346),
      (0, Some(3.14),  true,  190101.00000012346, 190105.12345612346),
      (0, Some(2.72),  true,  190105.12345612346, 190201.0234561245),
      (0, Some(13.0),  true,  190201.0234561245,  190404.0),
      (0, None,        false, 190404.0,           190905.0234561231),
      (0, Some(42.0),  true,  190905.0234561231,  190905.0234561239),
      (0, None,        false, 190905.0234561239,  200101.01),
      (0, Some(18.17), true,  200101.01,          intervalMaxValue),
      (1, None,        false, intervalMinValue,   190101.00000012346),
      (1, Some(-1.0),  true,  190101.00000012346, 190202.0),
      (1, None,        false, 190202.0,           190301.00000),
      (1, Some(0.1),   true,  190301.00000,       190301.0000000002),
      (1, Some(0.8),   true,  190301.0000000002,  190301.000000001),
      (1, Some(0.1),   true,  190301.000000001,   190301.000000002),
      (1, None,        false, 190301.000000002,   190301.0000010009),
      (1, Some(1.2),   true,  190301.0000010009,  190301.0000010021),
      (1, None,        false, 190301.0000010021,  190303.01000),
      (1, Some(-2.0),  true,  190303.01000,       211201.0234561),
      (1, None,        false, 211201.0234561,     intervalMaxValue)
    ).toDF("id", "value", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("v dfDirtyIntervals", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps" should "return expected results" in {
    val actual =
      dfDirtyIntervals.intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.fromCol, $"value"), extend = false,
        fillGapsWithNull = false)
        .intervalCombine[Double]()
        .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      (0, 3.14,  190101.00000012346, 190105.12345612346),
      (0, 2.72,  190105.12345612346, 190201.0234561245),
      (0, 13.0,  190201.0234561245,  190404.00000),
      (0, 42.0,  190905.0234561231,  190905.0234561239),
      (0, 18.17, 200101.01000,       intervalMaxValue),
      (1, -1.0,  190101.00000012346, 190202.00000),
      (1, 0.1,   190301.00000,       190301.0000000002),
      (1, 0.8,   190301.0000000002,  190301.000000001),
      (1, 0.1,   190301.000000001,   190301.000000002),
      (1, 1.2,   190301.0000010009,  190301.0000010021),
      (1, -2.0,  190303.01000,       211201.0234561)
    ).toDF("id", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend validityDuration" should "return expected results" in {
    val argument = Seq(
      (1, "A", 200701.000000, 200704.0),
      (1, "A", 200705.000000, 200708.0),
      (1, "B", 200701.000000, 200703.0),
      (1, "B", 200704.000000, 200708.0)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    // we want the record with the longest interval, i.e. maximal toColName-fromColName
    val actual = argument.intervalCleanupExtend[Double](Seq("id"), Seq((defaultLinearConfig.toCol - defaultLinearConfig.fromCol).desc))
      .intervalCombine[Double]()
    val expected = Seq(
      (1, None,      false, intervalMinValue, 200701.0),
      (1, Some("A"), true,  200701.000000,    200704.0),
      (1, Some("B"), true,  200704.000000,    200708.0),
      (1, None,      false, 200708.000000,    intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.definedColName, defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearCleanupExtend validityDuration", argument)(actual, expected)
    result shouldBe true
  }

  "linearCleanupExtend rankExprFromColOnly" should "return expected results" in {
    val argument = Seq(
      (1, "S", intervalMinValue, intervalMaxValue),
      (1, "X", 200701.000000,    intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val actual = argument.intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
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
      (1, "S", intervalMinValue, 200701.000000),
      (1, "X", 200701.000000,    200924.0),
      (1, "B", 200803.000000,    intervalMaxValue),
      (1, "G", 200924.000000,    intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val actual = argument
      .intervalCleanupExtend[Double](Seq("id"), Seq(defaultLinearConfig.toCol.desc, defaultLinearConfig.fromCol.asc))
      .intervalCombine[Double]()
    val expected = Seq(
      (1, "S", intervalMinValue, 200701.0),
      (1, "X", 200701.000000,    200803.0),
      (1, "B", 200803.000000,    intervalMaxValue)
    ).toDF("id", "val", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
      .withColumn("_defined", lit(true))
    val result2 = dfEqual(actual, expected)
    if (!result2) printFailedTestResult("linearCleanupExtend rankExpr2Cols", argument)(actual, expected)
    assert(result2)
  }

  "linearExtendRange dfLeft" should "return expected results" in {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.intervalExtendRange[Double](Seq("id"))
    val rowsExpected = Seq((0, 4.2, intervalMinValue, intervalMaxValue))
    val expected = rowsExpected.toDF("id", "value_L", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)

    if (!result) printFailedTestResult("linearExtendRange dfLeft", dfLeft)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "linearExtendRange dfRight_id" should "return expected results" in {
    val actual = dfRight.intervalExtendRange[Double](Seq("id"))
    val expected = Seq(
      (0, Some(97.15),  intervalMinValue, 180201.0),
      (0, Some(97.15),  180601.0524110,   181023.035010),
      (0, Some(97.15),  181023.035010,    200101.0),
      (0, Some(97.15),  200101.000000,    intervalMaxValue),
      (1, None,         intervalMinValue, 190101.0),
      (1, Some(2019.0), 190101.0000000,   200101.0),
      (1, Some(2020.0), 200101.0000000,   210101.0),
      (1, None,         210101.0000000,   intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("linearExtendRange dfRight_id", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "linearExtendRange dfRight" should "return expected results" in {
    // argument: dfRight from object TestUtils
    val actual = dfRight.intervalExtendRange[Double]()
    val expected = Seq(
      (0, Some(97.15),  intervalMinValue, 180201.0),
      (0, Some(97.15),  180601.0524110,   181023.035010),
      (0, Some(97.15),  181023.035010,    200101.0),
      (0, Some(97.15),  200101.000000,    intervalMaxValue),
      (1, None,         intervalMinValue, 190101.0),
      (1, Some(2019.0), 190101.0000000,   200101.0),
      (1, Some(2020.0), 200101.0000000,   210101.0),
      (1, None,         210101.0000000,   intervalMaxValue)
    ).toDF("id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col): _*)
    val result = dfEqual(actual, expectedWithActualColumns)
    if (!result) printFailedTestResult("linearExtendRange dfRight", dfRight)(actual, expectedWithActualColumns)
    result shouldBe true
  }

  "linearInnerJoin dfRight 'on' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").intervalInnerJoin[Double](dfRight.as("dfR"), $"dfL.id" === $"dfR.id")
    assert(actual.columns.count(_ == "id") == 2)
    val expected = Seq(
      (0, 4.2, 0, Some(97.15), 180101.000000, 180201.0),
      (0, 4.2, 0, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, 0, Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "value_l", "id", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearInnerJoin dfRight 'on' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearInnerJoin dfRight with 'using' semantics" should "return expected results" in {
    val actual = dfLeft.as("dfL").intervalInnerJoin[Double](dfRight.as("dfR"), Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value_l", $"dfR.value_r").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), 180101.000000, 180201.0),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearInnerJoin dfRight with 'using' semantics", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearInnerJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.intervalInnerJoin[Double](dfR, Seq("id"))
    assert(3 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, Some(97.15), 180101.000000, 180201.0),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "value", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("temporalInnerJoin with equally named columns apart join columns", Seq(dfL, dfR))(actual, expected)
    result shouldBe true
  }

  /*
    "linearLeftAntiJoin dfRight" should "return expected results" in {
      val actual = dfLeft.intervalLeftAntiJoin(dfRight,Seq("id"))
      val expected = Seq(
        (0,  171210.000000,  180101.0, 4.2),
        (0,  180201.000000,  180601.052411, 4.2)
      ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"value_l")
      val result = dfEqual(actual,expected)
  
      if (!result) printFailedTestResult("linearLeftAntiJoin dfRight",Seq(dfLeft,dfRight))(actual,expected)
      result shouldBe true
    }
  
    ignore("linearLeftAntiJoin dfMap" should "return expected results" in {
      val actual = dfLeft.intervalLeftAntiJoin(dfMap,Seq("id"))
      val expected = Seq(
        (0,  171210.000000,  180101.0, 4.2),
        (0,  180401.000000,  181209.0, 4.2)
      ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"value_l")
      val result = dfEqual(actual,expected)
  
      if (!result) printFailedTestResult("linearLeftAntiJoin dfMap",Seq(dfLeft,dfMap))(actual,expected)
      result shouldBe true
    })
  
    ignore("linearLeftAntiJoin dfRight_dfMap" should "return expected results" in {
      val actual = dfRight.intervalLeftAntiJoin(dfMap,Seq("id"))
      val expected = Seq(
        (0,  180601.052411,  181023.035010, Some(97.15)),
        (0,  181023.035010,  200101.0,      Some(97.15)),
        (0,  200101.000000, intervalMaxValue , Some(97.15)),
        (1,  180101.000000,  190101.0,      None),
        (1,  190101.000000,  200101.0,      Some(2019.0)),
        (1,  200101.000000,  210101.0,      Some(2020.0)),
        (1,  210101.000000,  intervalMaxValue,      None))
        .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"value_r")
      val result = dfEqual(actual,expected)
  
      if (!result) printFailedTestResult("linearLeftAntiJoin dfRight_dfMap",Seq(dfRight,dfMap))(actual,expected)
      result shouldBe true
    })
  
    ignore("linearLeftAntiJoin dfMap_dfRight" should "return expected results" in {
      val actual = dfMap.intervalLeftAntiJoin(dfRight,Seq("id"))
      val expected = Seq(
        (0,  180201.000000,  180301.0, "B"),
        (0,  180201.000000,  180301.0, "C"),
        (0,  180220.000000,  180401.0, "D"),
        (0,  180225.141516123,  180225.141516123, "X")
      ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
      val result = dfEqual(actual,expected)
  
      if (!result) printFailedTestResult("temporalLeftAntiJoin_dfMap_dfRight",Seq(dfMap,dfRight))(actual,expected)
      result shouldBe true
    })
  
    ignore("linearLeftAntiJoin segmented" should "return expected results" in {
      val minuend = Seq(
        (1, 190101.00000,  200101.00000),
        (2, 190101.00000,  200101.00005),
        (3, 200101.00057,  220101.0),
        (4, 200101.00025,  200101.00035),
        (5, 190101.00000,  200101.000109999),
        (6, 190101.00000,  220101.0)
      ).toDF("id",defaultConfig.fromColName, defaultConfig.toColName)
      val subtrahend = Seq(
        (0, 200101.00044, 200101.00050),
        (0, 200101.00001, 200101.00010),
        (0, 200101.00033, 200101.00040),
        (0, 200101.00055, 200101.00060),
        (0, 200101.00022, 200101.00030)
      ).toDF("id",defaultConfig.fromColName, defaultConfig.toColName)
  
      val actual = minuend.intervalLeftAntiJoin(subtrahend,Nil)
      val expected = Seq(
        (1, 190101.00000    , 200101.00000),
        (2, 190101.00000    , 200101.000001),
        (3, 200101.000600001, 220101.0),
        (4, 200101.000300001, 200101.000303),
        (5, 200101.000100001, 200101.00011),
        (5, 190101.00000    , 200101.000001),
        (6, 200101.000600001, 220101.0),
        (6, 200101.000500001, 200101.000505),
        (6, 200101.000400001, 200101.000404),
        (6, 200101.000300001, 200101.000303),
        (6, 200101.000100001, 200101.000202),
        (6, 190101.00000    , 200101.000001)
      ).toDF("id",defaultConfig.fromColName, defaultConfig.toColName)
      val result = dfEqual(actual,expected)
  
      if (!result) printFailedTestResult("linearLeftAntiJoin segmented",Seq(minuend,subtrahend))(actual,expected)
      result shouldBe true
    })
   */

  "linearFullJoin dfRight" should "return expected results" in {
    val actual = dfLeft.intervalFullJoin[Double](dfRight, Seq("id")).intervalCombine[Double]()
      .intervalCombine[Double]()
      .orderBy($"id", defaultLinearConfig.fromCol)
    val expected = Seq(
      // id = 0
      (Some(0), None,      None,        intervalMinValue, 171210.0),
      (Some(0), Some(4.2), None,        171210.000000,    180101.0),
      (Some(0), Some(4.2), Some(97.15), 180101.000000,    180201.0),
      (Some(0), Some(4.2), None,        180201.000000,    180601.052411),
      (Some(0), Some(4.2), Some(97.15), 180601.052411,    181209.0),
      (Some(0), None,      Some(97.15), 181209.000000,    intervalMaxValue),
      // id = 1
      (Some(1), None, None,         intervalMinValue, 190101.0),
      (Some(1), None, Some(2019.0), 190101.000000,    200101.0),
      (Some(1), None, Some(2020.0), 200101.000000,    210101.0),
      (Some(1), None, None,         210101.000000,    intervalMaxValue)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMap" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.intervalFullJoin[Double](df2 = dfMap, keys = Seq("id"))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None,      intervalMinValue, 171210.0),
      (Some(0), Some(4.2), None,      171210.000000,    180101.0),
      (Some(0), Some(4.2), Some("A"), 180101.000000,    180201.0),
      (Some(0), Some(4.2), Some("B"), 180101.000000,    180301.0),
      (Some(0), Some(4.2), Some("C"), 180201.000000,    180301.0),
      (Some(0), Some(4.2), Some("D"), 180220.000000,    180401.0),
      (Some(0), Some(4.2), None,      180401.000000,    181209.0),
      (Some(0), None,      None,      181209.000000,    intervalMaxValue)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.intervalFullJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None, intervalMinValue, 171210.0),
      (Some(0), Some(4.2), None, 171210.000000,    180101.0),
      // img = {A}
      (Some(0), Some(4.2), Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (Some(0), Some(4.2), Some("B"), 180201.000000, 180301.0),
      // img = {D}
      (Some(0), Some(4.2), Some("D"), 180301.000000, 180401.0),
      // img = {}
      (Some(0), Some(4.2), None, 180401.000000, 181209.0),
      (Some(0), None,      None, 181209.000000, intervalMaxValue)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearFullJoin rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, 180101.000000,    180201.0,         "A"),
      (0, 180101.000000,    180301.0,         "B"),
      (0, 180201.000000,    180301.0,         "C"),
      (0, 180330.000000,    180401.0,         "D"),
      (0, 180225.141516123, 180225.141516123, "X")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val actual =
      dfLeft.intervalFullJoin[Double](df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (Some(0), None,      None, intervalMinValue, 171210.0),
      (Some(0), Some(4.2), None, 171210.000000,    180101.0),
      // img = {A}
      (Some(0), Some(4.2), Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (Some(0), Some(4.2), Some("B"), 180201.000000, 180301.0),
      // img = null
      (Some(0), Some(4.2), None, 180301.000000, 180330.0),
      // img = {D}
      (Some(0), Some(4.2), Some("D"), 180330.000000, 180401.0),
      // img = {}
      (Some(0), Some(4.2), None, 180401.000000, 181209.0),
      (Some(0), None,      None, 181209.000000, intervalMaxValue)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearFullJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin dfRight" should "return expected results" in {
    val actual = dfLeft.intervalLeftJoin[Double](dfRight, Seq("id"))
      .intervalCombine[Double]()
    val expected = Seq(
      (0, 4.2, None,        171210.000000, 180101.0),
      (0, 4.2, Some(97.15), 180101.000000, 180201.0),
      (0, 4.2, None,        180201.000000, 180601.052411),
      (0, 4.2, Some(97.15), 180601.052411, 181209.0)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMap" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.intervalLeftJoin[Double](df2 = dfMap, keys = Seq("id"))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None,      171210.000000, 180101.0),
      (0, 4.2, Some("A"), 180101.000000, 180201.0),
      (0, 4.2, Some("B"), 180101.000000, 180301.0),
      (0, 4.2, Some("C"), 180201.000000, 180301.0),
      (0, 4.2, Some("D"), 180220.000000, 180401.0),
      (0, 4.2, None,      180401.000000, 181209.0)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.intervalLeftJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, 171210.000000, 180101.0),
      // img = {A}
      (0, 4.2, Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (0, 4.2, Some("B"), 180201.000000, 180301.0),
      // img = {D}
      (0, 4.2, Some("D"), 180301.000000, 180401.0),
      // img = {}
      (0, 4.2, None, 180401.000000, 181209.0)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearLeftJoin rightMapWithrnkExpressions", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin rightMapWithGapsAndRnkExpressions" should "return expected results" in {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, 180101.000000,    180201.0,         "A"),
      (0, 180101.000000,    180301.0,         "B"),
      (0, 180201.000000,    180301.0,         "C"),
      (0, 180330.000000,    180401.0,         "D"),
      (0, 180225.141516123, 180225.141516123, "X")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val actual =
      dfLeft.intervalLeftJoin[Double](df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, 4.2, None, 171210.000000, 180101.0),
      // img = {A}
      (0, 4.2, Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (0, 4.2, Some("B"), 180201.000000, 180301.0),
      // img = null
      (0, 4.2, None, 180301.000000, 180330.0),
      // img = {D}
      (0, 4.2, Some("D"), 180330.000000, 180401.0),
      // img = {}
      (0, 4.2, None, 180401.000000, 181209.0)
    )
      .toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearLeftJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearLeftJoin with equally named columns apart join columns" should "return expected results" in {
    val dfL = dfLeft.withColumnRenamed("value_l", "value").as("dfL")
    val dfR = dfRight.withColumnRenamed("value_r", "value").as("dfR")
    val actual = dfL.intervalLeftJoin[Double](dfR, Seq("id"))
      // .intervalCombine[Double]() // temporal combine not possible with equally named columns in the same DataFrame.
      .orderBy($"id", defaultLinearConfig.fromCol)
    assert(5 == actual.select($"id", $"dfL.value", $"dfR.value").count())
    val expected = Seq(
      (0, 4.2, None,        171210.000000, 180101.0),
      (0, 4.2, Some(97.15), 180101.000000, 180201.0),
      (0, 4.2, None,        180201.000000, 180601.052411),
      (0, 4.2, Some(97.15), 180601.052411, 181023.035010),
      (0, 4.2, Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "value", "value", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result)
      printFailedTestResult("linearLeftJoin with equally named columns apart join columns", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin dfRight" should "return expected results" in {
    val actual = dfLeft.intervalRightJoin[Double](dfRight, Seq("id"))
      .intervalCombine[Double]()
    val expected = Seq(
      // id = 0
      (0, Some(4.2), Some(97.15), 180101.000000, 180201.0),
      (0, Some(4.2), Some(97.15), 180601.052411, 181209.0),
      (0, None,      Some(97.15), 181209.000000, intervalMaxValue),
      // id = 1
      (1, None, None,         180101.000000, 190101.0),
      (1, None, Some(2019.0), 190101.000000, 200101.0),
      (1, None, Some(2020.0), 200101.000000, 210101.0),
      (1, None, None,         210101.000000, intervalMaxValue)
    ).toDF("id", "value_l", "value_r", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearRightJoin dfRight", Seq(dfLeft, dfRight))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin rightMap" should "return expected results" in {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.intervalRightJoin[Double](df2 = dfMap, keys = Seq("id"))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101.000000, 180201.0),
      (0, Some(4.2), Some("B"), 180101.000000, 180301.0),
      (0, Some(4.2), Some("C"), 180201.000000, 180301.0),
      (0, Some(4.2), Some("D"), 180220.000000, 180401.0)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("temporalRightJoin rightMap", Seq(dfLeft, dfMap))(actual, expected)
    result shouldBe true
  }

  "linearRightJoin rightMapWithrnkExpressions" should "return expected results" in {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    val actual = dfLeft.intervalRightJoin[Double](df2 = dfMap, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
      .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101.000000, 180201.0),
      (0, Some(4.2), Some("B"), 180101.000000, 180301.0),
      (0, Some(4.2), Some("C"), 180201.000000, 180301.0),
      (0, Some(4.2), Some("D"), 180220.000000, 180401.0)
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
      (0, 180101.000000, 180201.0, "A"),
      (0, 180101.000000, 180301.0, "B"),
      (0, 180201.000000, 180301.0, "C"),
      (0, 180330.000000, 180401.0, "D")
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")

    val actual =
      dfLeft.intervalRightJoin[Double](df2 = argumentRight, keys = Seq("id"), rnkExpressions = Seq($"img", defaultLinearConfig.fromCol))
        .intervalCombine[Double]()
    val expected = Seq(
      // img = {}
      (0, Some(4.2), Some("A"), 180101.000000, 180201.0),
      (0, Some(4.2), Some("B"), 180101.000000, 180301.0),
      (0, Some(4.2), Some("C"), 180201.000000, 180301.0),
      (0, Some(4.2), Some("D"), 180330.000000, 180401.0)
    ).toDF("id", "value_l", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearRightJoin rightMapWithGapsAndRnkExpressions", Seq(dfLeft, argumentRight))(actual, expected)
    result shouldBe true
  }

  "linearCombine dfRight" should "return expected results" in {
    val actual = dfRight.intervalCombine[Double]()
    val expected = Seq(
      (0, 180101.0000000, 180201.0,         Some(97.15)),
      (0, 180601.0524110, intervalMaxValue, Some(97.15)),
      (1, 180101.0000000, 190101.0,         None),
      (1, 190101.0000000, 200101.0,         Some(2019.0)),
      (1, 200101.0000000, 210101.0,         Some(2020.0)),
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
      .intervalCombine[Double]()
    val expected = Seq(
      (0, 180101.0000000, 180201.0,         Some(97.15)),
      (0, 180601.0524110, intervalMaxValue, Some(97.15)),
      (1, 180101.0000000, 190101.0,         None),
      (1, 190101.0000000, 200101.0,         Some(2019.0)),
      (1, 200101.0000000, 210101.0,         Some(2020.0)),
      (1, 210101.0000000, intervalMaxValue, None)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value_r")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dropped column", dfRight)(actual, expected)
    result shouldBe true
  }

  "linearCombine dfMapToCombine" should "return expected results" in {
    val actual = dfMapToCombine.intervalCombine[Double]()
    val expected = Seq(
      (0, 180101.000000, 190101.0, Some("A")),
      (0, 180101.000000, 180204.0, Some("B")),
      (0, 180201.000000, 200501.0, None),
      (0, 200601.000000, 210101.0, None),
      (1, 180201.000000, 200501.0, Some("one")),
      (1, 200601.000000, 210101.0, Some("one")),
      (0, 180220.000000, 180401.0, Some("D"))
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "img")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dfMapToCombine", dfMapToCombine)(actual, expected)
    result shouldBe true
  }

  "linearCombine dfDirtyIntervals" should "return expected results" in {
    val actual = dfDirtyIntervals.intervalCombine[Double]()
    val expected = Seq(
      (0, 190101.00000012346, 190105.12345612346, 3.14),
      (0, 190105.12345612346, 190201.0234561245,  2.72),
      (0, 190201.0234561245,  190404.00000,       13.0),
      (0, 190905.0234561231,  190905.0234561239,  42.0),
      (0, 200101.01000,       intervalMaxValue,   18.17),
      (1, 190101.00000012346, 190202.000000,      -1.0),
      (1, 190301.00000,       190301.0000000002,  0.1),
      (1, 190301.0000000009,  190301.000000002,   0.1),
      (1, 190301.0000010009,  190301.0000010021,  1.2),
      (1, 190301.0000000001,  190301.000000001,   0.8),
      (1, 190303.01000,       211201.0234561,     -2.0)
    ).toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine dfDirtyIntervals", dfDirtyIntervals)(actual, expected)
    result shouldBe true
  }

  "linearCombine documentation" should "return expected results" in {
    val actual = dfDocumentation.intervalCombine[Double]()
    val expected = Seq(
      (1, 190105.12345612346, 190201.0234561245, 2.72), // overlaps with previous record
      (1, 190101.00000,       200101.0,          42.0)
    )
      .toDF("id", defaultLinearConfig.fromColName, defaultLinearConfig.toColName, "value")
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearCombine documentation", dfDocumentation)(actual, expected)
    result shouldBe true
  }

  "linearUnifyRanges dfMoment" should "return expected results" in {
    // Note that a Moment can not be modeled with HalfOpenInterval - result is therefore empty
    val actual = dfMoment.intervalUnifyRanges[Double](Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment.where(lit(false)) // empty data frame expected
    logger.info("expected:")
    expected.show(false)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "temporalUnifyRanges dfSmallOverlap" should "return expected results" in {
    val actual = dfSmallOverlap.intervalUnifyRanges[Double](Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", 190101.000000, 190101.100000),
      (0, "A", 190101.100000, 190101.100001),
      (0, "B", 190101.100000, 190101.100001),
      (0, "B", 190101.100001, 190102.0)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("linearUnifyRanges dfSmallOverlap", dfSmallOverlap)(actual, expected)
    result shouldBe true
  }

  "linearUnifyRanges dfMap" should "return expected results" in {
    val actual = dfMap.intervalUnifyRanges[Double](Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0, "A", 180101.000000, 180201.0),
      (0, "B", 180101.000000, 180201.0),
      // img = {B,C}
      (0, "B", 180201.000000, 180220.0),
      (0, "C", 180201.000000, 180220.0),
      // img = {B,C,D}
      (0, "B", 180220.000000, 180225.141516123),
      (0, "C", 180220.000000, 180225.141516123),
      (0, "D", 180220.000000, 180225.141516123),
      // img = {B,C,D}
      (0, "B", 180225.141516123, 180301.0),
      (0, "C", 180225.141516123, 180301.0),
      (0, "D", 180225.141516123, 180301.0),
      // img = {D}
      (0, "D", 180301.000000, 180401.0)
    ).toDF("id", "img", defaultLinearConfig.fromColName, defaultLinearConfig.toColName)
    val result = dfEqual(actual, expected)

    if (!result) printFailedTestResult("linearUnifyRanges dfMap", dfMap)(actual, expected)
    result shouldBe true
  }
}
