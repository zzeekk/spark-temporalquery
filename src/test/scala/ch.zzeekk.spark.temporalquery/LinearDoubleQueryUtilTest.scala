package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.LinearDoubleQueryUtil._
import ch.zzeekk.spark.temporalquery.LinearDoubleTestUtils._
import org.apache.spark.sql.functions.{col, lit}
import org.scalatest.FunSuite

class LinearDoubleQueryUtilTest extends FunSuite with Logging {
  import session.implicits._

  /*
  // Not possible with Double as it is no Integral type.
  test("linearConvertToClosedIntervals") {
    implicit val defaultConfig: LinearQueryConfig = LinearQueryConfig(intervalDef = ClosedInterval(0f, Double.MaxValue, DiscreteNumericAxis(0.000000001)))
    val actual = dfContinuous.linearConvertToClosedIntervals()
    val expected = Seq(
      (0, 190101.123456124, 190105.123456123,3.14 ),
      (0, 190105.123456124, 190201.023456123,2.72 ),
      (0, 190201.023456124, 190201.023456124,42.0 ),
      (0, 190201.023456125, 190302.235959999,13.0 ),
      (0, 190303.00000    , 190403.235959999,12.0 ),
      (0, 200101.01000    , intervalMaxValue,18.17),
      (1, 190101.000000124, 190201.235959999,-1.0 ),
      (1, 190303.01000    , 211201.023456099,-2.0 )
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert")

    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearConvertToClosedIntervals",Seq(dfContinuous))(actual,expected)
    assert(resultat)
  }
  */

  /*
  // Not possible with Double as it is no Integral type.
  test("temporalRoundDiscreteTime_dfLeft") {
    val actual = dfLeft.linearRoundClosedIntervals(defaultConfig)
    val expected = dfLeft

    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearRoundClosedIntervals",Seq(dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearRoundClosedIntervals dfDirtyTimeRanges") {
    val actual = dfDirtyIntervals.linearRoundClosedIntervals(defaultConfig)
    val zeilen_expected: Seq[(Int, String, String, Double)] = Seq(
      (0, 190101.000000124, 190105.123456123, 3.14),
      (0, 190105.123456124, 190201.023456123, 2.72),
      (0, 190201.01000     , 190201.023456124, 2.72),
      (0, 190201.023456125, 190303.00000     ,13.0 ),
      (0, 190303.00000     , 190404.00000     ,13.0 ),
      (0, 200101.01000     ,intervalMaxValue      ,18.17),
      (1, 190301.00000     , 190301.00000     , 0.1 ), // duration extended to 1 millisecond
      (1, 190301.000000001 , 190301.000000001 , 0.1 ), // duration extended to 1 millisecond
      (1, 190301.000001001 , 190301.000001002, 1.2 ), // duration extended to 2 milliseconds
      (1, 190101.000000124 , 190202.00000     ,-1.0 ),
      (1, 190303.01000     , 211201.0234561  ,-2.0 ))
    val expected = zeilen_expected.toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearRoundClosedIntervals",Seq(dfDirtyIntervals))(actual,expected)
    assert(resultat)
  }
  */

  test("linearCleanupExtend dfLeft") {
    val actual = dfLeft.linearCleanupExtend(Seq("id"),Seq(defaultConfig.fromCol))
      .linearCombine()
      .orderBy(defaultConfig.fromCol)
    val expected = Seq(
      (0, None     , false, intervalMinValue,  171210.0),
      (0, Some(4.2), true ,  171210.000000  ,  181209.0),
      (0, None     , false,  181209.000000  , intervalMaxValue)
    ).toDF("id","wert_l",defaultConfig.definedColName,defaultConfig.fromColName,defaultConfig.toColName)
    val resultat = dfEqual(reorderCols(actual,expected), expected)
    
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfLeft",dfLeft)(reorderCols(actual,expected),expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfRight_noExtend_nofillGaps") {
    val actual = dfRight.linearCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(defaultConfig.fromCol),
      extend = false,
      fillGapsWithNull = false
    ).linearCombine()
    val expected = Seq(
      (0, Some(97.15) ,  180101.000000,  180201.0),
      (0, Some(97.15) ,  180601.052411, intervalMaxValue),
      (1, None        ,  180101.000000,  190101.0),
      (1, Some(2019.0),  190101.000000,  200101.0),
      (1, Some(2020.0),  200101.000000,  210101.0),
      (1, None        ,  210101.000000,  intervalMaxValue)
    ).toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfRight_noExtend_nofillGaps",dfRight)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfRight_fillGaps_noExtend") {
    val actual = dfRight.linearCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(defaultConfig.fromCol),
      extend = false
    ).linearCombine()
    val expected = Seq(
      (0, Some(97.15) , true,  180101.000000,  180201.0),
      (0, None        , false, 180201.000000,  180601.052411),
      (0, Some(97.15) , true,  180601.052411,  intervalMaxValue),
      (1, None        , true,  180101.000000,  190101.0),
      (1, Some(2019.0), true,  190101.000000,  200101.0),
      (1, Some(2020.0), true,  200101.000000,  210101.0),
      (1, None        , true,  210101.000000,  intervalMaxValue)
    ).toDF("id","wert_r",defaultConfig.definedColName,defaultConfig.fromColName,defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    
    if (!resultat) printFailedTestResult("linearCleanupExtend dfRight_fillGaps_noExtend",dfRight)(actual,expected)
    assert(resultat)
  }
  
  test("linearCleanupExtend dfRight_extend_nofillGaps") {
    val actual = dfRight.linearCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(defaultConfig.fromCol),
      fillGapsWithNull = false
    ).linearCombine()
    val expected = Seq(
      (0, Some(97.15) ,  180101.000000, 180201.0),
      (0, Some(97.15) ,  180601.052411, intervalMaxValue),
      (1, None        ,  180101.000000, 190101.0),
      (1, Some(2019.0),  190101.000000, 200101.0),
      (1, Some(2020.0),  200101.000000, 210101.0),
      (1, None        ,  210101.000000, intervalMaxValue)
    ).toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCleanupExtend dfRight_extend_nofillGaps",dfRight)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfRight_extend_fillGaps") {
    val actual = dfRight.linearCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(defaultConfig.fromCol)
    ).linearCombine()
      .orderBy($"id", defaultConfig.fromCol)
    val expected = Seq(
      (0, None        , false, intervalMinValue ,  180101.0),
      (0, Some(97.15) , true,   180101.000000,  180201.0),
      (0, None        , false,   180201.000000,  180601.052411),
      (0, Some(97.15) , true,  180601.052411,intervalMaxValue),
      (1, None        , false, intervalMinValue,  180101.0),
      (1, None        , true,  180101.000000,  190101.0),
      (1, Some(2019.0), true,   190101.000000,  200101.0),
      (1, Some(2020.0), true,   200101.000000,  210101.0),
      (1, None        , true,   210101.000000,  intervalMaxValue)
    ).toDF("id","wert_r",defaultConfig.definedColName,defaultConfig.fromColName,defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCleanupExtend dfRight_extend_fillGaps",dfRight)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfMap") {
    val actual = dfMap.linearCleanupExtend(Seq("id"),Seq($"img"))
      .linearCombine()
    val expected = Seq(
      (0, None     , false, intervalMinValue,  180101.0),
      (0, Some("A"), true,   180101.000000,    180201.0),
      (0, Some("B"), true,   180201.000000,    180301.0),
      (0, Some("D"), true,   180301.000000,    180401.0),
      (0, None     , false,  180401.000000,    intervalMaxValue)
    ).toDF("id","img",defaultConfig.definedColName,defaultConfig.fromColName,defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearCleanupExtend dfMap",dfMap)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfMap_NoExtendFillgaps") {
    val actual = dfMap.linearCleanupExtend(Seq("id"),Seq($"img"),extend=false,fillGapsWithNull=false)
      .linearCombine()
    val expected = Seq(
      (0, Some("A"),  180101.000000,  180201.0),
      (0, Some("B"),  180201.000000,  180301.0),
      (0, Some("D"),  180301.000000,  180401.0)
    ).toDF("id","img",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearCleanupExtend dfMap_NoExtendFillgaps",dfMap)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfSmallOverlap") {
    val actual = dfSmallOverlap.linearCleanupExtend(Seq("id"),Seq(defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      (0, None     , false, intervalMinValue  , 190101.0),
      (0, Some("A"), true,   190101.000000    , 190101.100001),
      (0, Some("B"), true,   190101.100001    , 190102.0),
      (0, None     , false,  190102.000000    , intervalMaxValue)
    ).toDF("id", "img", defaultConfig.definedColName, defaultConfig.fromColName, defaultConfig.toColName)

    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearCleanupExtend dfSmallOverlap",dfMap)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfDirtyIntervals") {
    val actual = dfDirtyIntervals.linearCleanupExtend(Seq("id"),Seq(defaultConfig.fromCol,$"wert"))
      .linearCombine()
      .orderBy($"id", defaultConfig.fromCol)
    val expected = Seq(
      (0, None       , false,  intervalMinValue  ,  190101.00000012346),
      (0, Some(3.14) , true,   190101.00000012346,  190105.12345612346),
      (0, Some(2.72) , true,   190105.12345612346,  190201.0234561245),
      (0, Some(13.0) , true,   190201.0234561245 ,  190404.0),
      (0, None       , false,  190404.0          ,  190905.0234561231),
      (0, Some(42.0) , true,   190905.0234561231 ,  190905.0234561239 ),
      (0, None       , false,  190905.0234561239 ,  200101.01),
      (0, Some(18.17), true,   200101.01         ,  intervalMaxValue),
      (1, None       , false,  intervalMinValue  ,  190101.00000012346),
      (1, Some(-1.0) , true,   190101.00000012346,  190202.0),
      (1, None       , false,  190202.0          ,  190301.00000),
      (1, Some(0.1)  , true,   190301.00000      ,  190301.0000000002),
      (1, Some(0.8)  , true,   190301.0000000002 ,  190301.000000001),
      (1, Some(0.1)  , true,   190301.000000001  ,  190301.000000002),
      (1, None       , false,  190301.000000002  ,  190301.0000010009),
      (1, Some(1.2)  , true,   190301.0000010009 ,  190301.0000010021),
      (1, None       , false,  190301.0000010021 ,  190303.01000),
      (1, Some(-2.0) , true,   190303.01000      ,  211201.0234561),
      (1, None       , false,  211201.0234561    , intervalMaxValue)
    ).toDF("id","wert",defaultConfig.definedColName,defaultConfig.fromColName,defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("v dfDirtyIntervals",dfDirtyIntervals)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps") {
    val actual = dfDirtyIntervals.linearCleanupExtend(Seq("id"),Seq(defaultConfig.fromCol,$"wert"),extend=false,fillGapsWithNull=false)
      .linearCombine()
      .orderBy($"id", defaultConfig.fromCol)
    val expected = Seq(
      (0, 3.14,  190101.00000012346,  190105.12345612346),
      (0, 2.72,  190105.12345612346,  190201.0234561245),
      (0,13.0 ,  190201.0234561245 ,  190404.00000),
      (0,42.0 ,  190905.0234561231 ,  190905.0234561239),
      (0,18.17,  200101.01000      , intervalMaxValue),
      (1,-1.0 ,  190101.00000012346,  190202.00000),
      (1, 0.1 ,  190301.00000      ,  190301.0000000002),
      (1, 0.8 ,  190301.0000000002 ,  190301.000000001),
      (1, 0.1 ,  190301.000000001  ,  190301.000000002),
      (1, 1.2 ,  190301.0000010009 ,  190301.0000010021),
      (1,-2.0 ,  190303.01000     ,  211201.0234561)
    ).toDF("id","wert",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCleanupExtend dfDirtyIntervals_NoExtendFillgaps",dfDirtyIntervals)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend validityDuration") {
    val argument = Seq(
      (1, "A",  200701.000000,  200704.0),
      (1, "A",  200705.000000,  200708.0),
      (1, "B",  200701.000000,  200703.0),
      (1, "B",  200704.000000,  200708.0)
    ).toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
    // we want the record with the longest interval, i.e. maximal toColName-fromColName
    val actual = argument.linearCleanupExtend(Seq("id"), Seq((defaultConfig.toCol-defaultConfig.fromCol).desc))
      .linearCombine()
    val expected = Seq(
      (1, None     , false, intervalMinValue,  200701.0),
      (1, Some("A"), true,   200701.000000,  200704.0),
      (1, Some("B"), true,   200704.000000,  200708.0),
      (1, None     , false,  200708.000000, intervalMaxValue)
    ).toDF("id", "val", defaultConfig.definedColName, defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearCleanupExtend validityDuration", argument)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend rankExprFromColOnly") {
    val argument = Seq(
      (1, "S", intervalMinValue, intervalMaxValue),
      (1, "X",  200701.000000, intervalMaxValue)
    ).toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
    val actual = argument.linearCleanupExtend(Seq("id"), Seq(defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      (1, "S", intervalMinValue, intervalMaxValue)
    ).toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined", lit(true))
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearCleanupExtend rankExprFromColOnly", argument)(actual,expected)
    assert(resultat)
  }

  test("linearCleanupExtend rankExpr2Cols") {
    val argument = Seq(
      (1,"S",intervalMinValue, 200701.000000),
      (1,"X", 200701.000000, 200924.0),
      (1,"B", 200803.000000,intervalMaxValue),
      (1,"G", 200924.000000,intervalMaxValue)
    ).toDF("id","val",defaultConfig.fromColName, defaultConfig.toColName)
    val actual = argument.linearCleanupExtend(Seq("id"),Seq(defaultConfig.toCol.desc,defaultConfig.fromCol.asc))(session,defaultConfig)
      .linearCombine()
    val expected = Seq(
      (1,"S",intervalMinValue, 200701.0),
      (1,"X", 200701.000000, 200803.0),
      (1,"B", 200803.000000,intervalMaxValue)
    ).toDF("id","val",defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat2 = dfEqual(actual,expected)
    if (!resultat2) printFailedTestResult("linearCleanupExtend rankExpr2Cols",argument)(actual,expected)
    assert(resultat2)
  }

  test("linearExtendRange dfLeft") {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.linearExtendRange(Seq("id"))
    val rowsExpected = Seq((0,4.2,intervalMinValue,intervalMaxValue))
    val expected = rowsExpected.toDF("id", "Wert_L", defaultConfig.fromColName, defaultConfig.toColName )
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual,expectedWithActualColumns)

    if (!resultat) printFailedTestResult("linearExtendRange dfLeft",dfLeft)(actual,expectedWithActualColumns)
    assert(resultat)
  }

  test("linearExtendRange dfRight_id") {
    val actual = dfRight.linearExtendRange(Seq("id"))
    val expected = Seq(
      (0,Some(97.15) ,intervalMinValue  , 180201.0),
      (0,Some(97.15) , 180601.0524110, 181023.035010),
      (0,Some(97.15) , 181023.035010  , 200101.0),
      (0,Some(97.15) , 200101.000000  ,intervalMaxValue),
      (1,None        ,intervalMinValue  , 190101.0),
      (1,Some(2019.0), 190101.0000000, 200101.0),
      (1,Some(2020.0), 200101.0000000, 210101.0),
      (1,None        , 210101.0000000,intervalMaxValue)
    ).toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual,expectedWithActualColumns)
    if (!resultat) printFailedTestResult("linearExtendRange dfRight_id",dfRight)(actual,expectedWithActualColumns)
    assert(resultat)
  }

  test("linearExtendRange dfRight") {
    // argument: dfRight from object TestUtils
    val actual = dfRight.linearExtendRange()
    val expected = Seq(
      (0,Some(97.15) ,intervalMinValue  , 180201.0),
      (0,Some(97.15) , 180601.0524110, 181023.035010),
      (0,Some(97.15) , 181023.035010  , 200101.0),
      (0,Some(97.15) , 200101.000000  ,intervalMaxValue),
      (1,None        ,intervalMinValue  , 190101.0),
      (1,Some(2019.0), 190101.0000000, 200101.0),
      (1,Some(2020.0), 200101.0000000, 210101.0),
      (1,None        , 210101.0000000, intervalMaxValue)
    ).toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual,expectedWithActualColumns)
    if (!resultat) printFailedTestResult("linearExtendRange dfRight",dfRight)(actual,expectedWithActualColumns)
    assert(resultat)
  }

  test("linearInnerJoin dfRight 'on' semantics") {
    val actual = dfLeft.as("dfL").linearInnerJoin(dfRight.as("dfR"),$"dfL.id"===$"dfR.id")
    assert(actual.columns.count(_ == "id") == 2)
    val expected = Seq(
      (0,4.2,0,Some(97.15), 180101.000000, 180201.0),
      (0,4.2,0,Some(97.15), 180601.052411, 181023.035010),
      (0,4.2,0,Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "wert_l", "id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearInnerJoin dfRight 'on' semantics",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearInnerJoin dfRight with 'using' semantics") {
    val actual = dfLeft.as("dfL").linearInnerJoin(dfRight.as("dfR"),Seq("id"))
    assert(3 == actual.select($"id",$"dfL.wert_l",$"dfR.wert_r").count())
    val expected = Seq(
      (0,4.2,Some(97.15), 180101.000000, 180201.0),
      (0,4.2,Some(97.15), 180601.052411, 181023.035010),
      (0,4.2,Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearInnerJoin dfRight with 'using' semantics",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearInnerJoin with equally named columns apart join columns") {
    val dfL = dfLeft.withColumnRenamed("wert_l","wert").as("dfL")
    val dfR = dfRight.withColumnRenamed("wert_r","wert").as("dfR")
    val actual = dfL.linearInnerJoin(dfR,Seq("id"))
    assert(3 == actual.select($"id",$"dfL.wert",$"dfR.wert").count())
    val expected = Seq(
      (0,4.2,Some(97.15), 180101.000000, 180201.0),
      (0,4.2,Some(97.15), 180601.052411, 181023.035010),
      (0,4.2,Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "wert", "wert", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("temporalInnerJoin with equally named columns apart join columns",Seq(dfL,dfR))(actual,expected)
    assert(resultat)
  }

  ignore("linearLeftAntiJoin dfRight") {
    val actual = dfLeft.linearLeftAntiJoin(dfRight,Seq("id"))
    val expected = Seq(
      (0,  171210.000000,  180101.0, 4.2),
      (0,  180201.000000,  180601.052411, 4.2)
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearLeftAntiJoin dfRight",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  ignore("linearLeftAntiJoin dfMap") {
    val actual = dfLeft.linearLeftAntiJoin(dfMap,Seq("id"))
    val expected = Seq(
      (0,  171210.000000,  180101.0, 4.2),
      (0,  180401.000000,  181209.0, 4.2)
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearLeftAntiJoin dfMap",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  ignore("linearLeftAntiJoin dfRight_dfMap") {
    val actual = dfRight.linearLeftAntiJoin(dfMap,Seq("id"))
    val expected = Seq(
      (0,  180601.052411,  181023.035010, Some(97.15)),
      (0,  181023.035010,  200101.0,      Some(97.15)),
      (0,  200101.000000, intervalMaxValue , Some(97.15)),
      (1,  180101.000000,  190101.0,      None),
      (1,  190101.000000,  200101.0,      Some(2019.0)),
      (1,  200101.000000,  210101.0,      Some(2020.0)),
      (1,  210101.000000,  intervalMaxValue,      None))
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearLeftAntiJoin dfRight_dfMap",Seq(dfRight,dfMap))(actual,expected)
    assert(resultat)
  }

  ignore("linearLeftAntiJoin dfMap_dfRight") {
    val actual = dfMap.linearLeftAntiJoin(dfRight,Seq("id"))
    val expected = Seq(
      (0,  180201.000000,  180301.0, "B"),
      (0,  180201.000000,  180301.0, "C"),
      (0,  180220.000000,  180401.0, "D"),
      (0,  180225.141516123,  180225.141516123, "X")
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfMap_dfRight",Seq(dfMap,dfRight))(actual,expected)
    assert(resultat)
  }

  ignore("linearLeftAntiJoin segmented") {
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

    val actual = minuend.linearLeftAntiJoin(subtrahend,Seq())
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
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearLeftAntiJoin segmented",Seq(minuend,subtrahend))(actual,expected)
    assert(resultat)
  }

  test("linearFullJoin dfRight") {
    val actual = dfLeft.linearFullJoin(dfRight,Seq("id")).linearCombine()
      .linearCombine()
      .orderBy($"id", defaultConfig.fromCol)
    val expected = Seq(
      // id = 0
      (Some(0),None     ,None       ,intervalMinValue, 171210.0),
      (Some(0),Some(4.2),None       , 171210.000000, 180101.0),
      (Some(0),Some(4.2),Some(97.15), 180101.000000, 180201.0),
      (Some(0),Some(4.2),None       , 180201.000000, 180601.052411),
      (Some(0),Some(4.2),Some(97.15), 180601.052411, 181209.0),
      (Some(0),None     ,Some(97.15), 181209.000000,intervalMaxValue),
      // id = 1
      (Some(1),None,None        ,intervalMinValue  , 190101.0),
      (Some(1),None,Some(2019.0), 190101.000000, 200101.0),
      (Some(1),None,Some(2020.0), 200101.000000, 210101.0),
      (Some(1),None,None        , 210101.000000,intervalMaxValue)
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearFullJoin dfRight",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearFullJoin rightMap") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.linearFullJoin(df2=dfMap, keys=Seq("id"))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (Some(0),None     ,None     ,intervalMinValue  , 171210.0),
      (Some(0),Some(4.2),None     , 171210.000000, 180101.0),
      (Some(0),Some(4.2),Some("A"), 180101.000000, 180201.0),
      (Some(0),Some(4.2),Some("B"), 180101.000000, 180301.0),
      (Some(0),Some(4.2),Some("C"), 180201.000000, 180301.0),
      (Some(0),Some(4.2),Some("D"), 180220.000000, 180401.0),
      (Some(0),Some(4.2),None     , 180401.000000, 181209.0),
      (Some(0),None     ,None     , 181209.000000,intervalMaxValue)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearFullJoin rightMap",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearFullJoin rightMapWithrnkExpressions") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.linearFullJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (Some(0),None     ,None     ,intervalMinValue, 171210.0),
      (Some(0),Some(4.2),None     , 171210.000000, 180101.0),
      // img = {A}
      (Some(0),Some(4.2),Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (Some(0),Some(4.2),Some("B"), 180201.000000, 180301.0),
      // img = {D}
      (Some(0),Some(4.2),Some("D"), 180301.000000, 180401.0),
      // img = {}
      (Some(0),Some(4.2),None     , 180401.000000, 181209.0),
      (Some(0),None     ,None     , 181209.000000,intervalMaxValue)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearFullJoin rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearFullJoin rightMapWithGapsAndRnkExpressions") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0,  180101.000000,  180201.0, "A"),
      (0,  180101.000000,  180301.0, "B"),
      (0,  180201.000000,  180301.0, "C"),
      (0,  180330.000000,  180401.0, "D"),
      (0,  180225.141516123,  180225.141516123, "X"))
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val actual = dfLeft.linearFullJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (Some(0),None     ,None     ,intervalMinValue, 171210.0),
      (Some(0),Some(4.2),None     , 171210.000000, 180101.0),
      // img = {A}
      (Some(0),Some(4.2),Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (Some(0),Some(4.2),Some("B"), 180201.000000, 180301.0),
      // img = null
      (Some(0),Some(4.2),None,      180301.000000, 180330.0),
      // img = {D}
      (Some(0),Some(4.2),Some("D"), 180330.000000, 180401.0),
      // img = {}
      (Some(0),Some(4.2),None     , 180401.000000, 181209.0),
      (Some(0),None     ,None     , 181209.000000,intervalMaxValue)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearFullJoin rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual,expected)
    assert(resultat)
  }

  test("linearLeftJoin dfRight") {
    val actual = dfLeft.linearLeftJoin(dfRight,Seq("id"))
      .linearCombine()
    val expected = Seq(
      (0,4.2,None       , 171210.000000, 180101.0),
      (0,4.2,Some(97.15), 180101.000000, 180201.0),
      (0,4.2,None       , 180201.000000, 180601.052411),
      (0,4.2,Some(97.15), 180601.052411, 181209.0)
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearLeftJoin dfRight",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearLeftJoin rightMap") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.linearLeftJoin(df2=dfMap, keys=Seq("id"))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (0,4.2,None     , 171210.000000, 180101.0),
      (0,4.2,Some("A"), 180101.000000, 180201.0),
      (0,4.2,Some("B"), 180101.000000, 180301.0),
      (0,4.2,Some("C"), 180201.000000, 180301.0),
      (0,4.2,Some("D"), 180220.000000, 180401.0),
      (0,4.2,None     , 180401.000000, 181209.0)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearLeftJoin rightMap",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearLeftJoin rightMapWithrnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.linearLeftJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected =   Seq(
      // img = {}
      (0,4.2,None     , 171210.000000, 180101.0),
      // img = {A}
      (0,4.2,Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (0,4.2,Some("B"), 180201.000000, 180301.0),
      // img = {D}
      (0,4.2,Some("D"), 180301.000000, 180401.0),
      // img = {}
      (0,4.2,None     , 180401.000000, 181209.0)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearLeftJoin rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearLeftJoin rightMapWithGapsAndRnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0,  180101.000000,  180201.0, "A"),
      (0,  180101.000000,  180301.0, "B"),
      (0,  180201.000000,  180301.0, "C"),
      (0,  180330.000000,  180401.0, "D"),
      (0,  180225.141516123,  180225.141516123, "X"))
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val actual = dfLeft.linearLeftJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (0,4.2,None     , 171210.000000, 180101.0),
      // img = {A}
      (0,4.2,Some("A"), 180101.000000, 180201.0),
      // img = {B}
      (0,4.2,Some("B"), 180201.000000, 180301.0),
      // img = null
      (0,4.2,None,      180301.000000, 180330.0),
      // img = {D}
      (0,4.2,Some("D"), 180330.000000, 180401.0),
      // img = {}
      (0,4.2,None     , 180401.000000, 181209.0))
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearLeftJoin rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual,expected)
    assert(resultat)
  }

  test("linearLeftJoin with equally named columns apart join columns") {
    val dfL = dfLeft.withColumnRenamed("wert_l","wert").as("dfL")
    val dfR = dfRight.withColumnRenamed("wert_r","wert").as("dfR")
    val actual = dfL.linearLeftJoin(dfR,Seq("id"))
      //.linearCombine() // temporal combine not possible with equally named columns in the same DataFrame.
      .orderBy($"id", defaultConfig.fromCol)
    assert(5 == actual.select($"id",$"dfL.wert",$"dfR.wert").count())
    val expected = Seq(
      (0,4.2,None       , 171210.000000, 180101.0),
      (0,4.2,Some(97.15), 180101.000000, 180201.0),
      (0,4.2,None       , 180201.000000, 180601.052411),
      (0,4.2,Some(97.15), 180601.052411, 181023.035010),
      (0,4.2,Some(97.15), 181023.035010, 181209.0)
    ).toDF("id", "wert", "wert", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearLeftJoin with equally named columns apart join columns",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearRightJoin dfRight") {
    val actual = dfLeft.linearRightJoin(dfRight,Seq("id"))
      .linearCombine()
    val expected = Seq(
      // id = 0
      (0,Some(4.2),Some(97.15), 180101.000000, 180201.0),
      (0,Some(4.2),Some(97.15), 180601.052411, 181209.0),
      (0,None     ,Some(97.15), 181209.000000,intervalMaxValue),
      // id = 1
      (1,None,None        , 180101.000000, 190101.0),
      (1,None,Some(2019.0), 190101.000000, 200101.0),
      (1,None,Some(2020.0), 200101.000000, 210101.0),
      (1,None,None        , 210101.000000, intervalMaxValue)
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearRightJoin dfRight",Seq(dfLeft,dfRight))(actual,expected)
    assert(resultat)
  }

  test("linearRightJoin rightMap") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.linearRightJoin(df2=dfMap, keys=Seq("id"))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"), 180101.000000, 180201.0),
      (0,Some(4.2),Some("B"), 180101.000000, 180301.0),
      (0,Some(4.2),Some("C"), 180201.000000, 180301.0),
      (0,Some(4.2),Some("D"), 180220.000000, 180401.0)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("temporalRightJoin rightMap",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearRightJoin rightMapWithrnkExpressions") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    val actual = dfLeft.linearRightJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"), 180101.000000, 180201.0),
      (0,Some(4.2),Some("B"), 180101.000000, 180301.0),
      (0,Some(4.2),Some("C"), 180201.000000, 180301.0),
      (0,Some(4.2),Some("D"), 180220.000000, 180401.0)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearRightJoin rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual,expected)
    assert(resultat)
  }

  test("linearRightJoin rightMapWithGapsAndRnkExpressions") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    // and gaps are of the left frame only are filled
    val argumentRight = Seq(
      (0,  180101.000000,  180201.0, "A"),
      (0,  180101.000000,  180301.0, "B"),
      (0,  180201.000000,  180301.0, "C"),
      (0,  180330.000000,  180401.0, "D"))
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

    val actual = dfLeft.linearRightJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",defaultConfig.fromCol))
      .linearCombine()
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"), 180101.000000, 180201.0),
      (0,Some(4.2),Some("B"), 180101.000000, 180301.0),
      (0,Some(4.2),Some("C"), 180201.000000, 180301.0),
      (0,Some(4.2),Some("D"), 180330.000000, 180401.0)
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearRightJoin rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual,expected)
    assert(resultat)
  }

  test("linearCombine dfRight") {
    val actual = dfRight.linearCombine()
    val expected = Seq(
      (0, 180101.0000000, 180201.0,        Some(97.15) ),
      (0, 180601.0524110,intervalMaxValue, Some(97.15) ),
      (1, 180101.0000000, 190101.0,        None        ),
      (1, 190101.0000000, 200101.0,        Some(2019.0)),
      (1, 200101.0000000, 210101.0,        Some(2020.0)),
      (1, 210101.0000000, intervalMaxValue,None        )
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_r")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCombine dfRight",dfRight)(actual,expected)
    assert(resultat)
  }

  test("linearCombine dropped column") {
    val actual = dfRight
      .withColumn("test_column",lit("please drop me"))
      .drop("test_column")
      .linearCombine()
    val expected = Seq(
      (0, 180101.0000000, 180201.0,        Some(97.15) ),
      (0, 180601.0524110,intervalMaxValue, Some(97.15) ),
      (1, 180101.0000000, 190101.0,        None       ),
      (1, 190101.0000000, 200101.0,        Some(2019.0)),
      (1, 200101.0000000, 210101.0,        Some(2020.0)),
      (1, 210101.0000000, intervalMaxValue,None        )
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_r")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCombine dropped column",dfRight)(actual,expected)
    assert(resultat)
  }

  test("linearCombine dfMapToCombine") {
    val actual = dfMapToCombine.linearCombine()
    val expected = Seq(
      (0, 180101.000000, 190101.0, Some("A")),
      (0, 180101.000000, 180204.0, Some("B")),
      (0, 180201.000000, 200501.0, None),
      (0, 200601.000000, 210101.0, None),
      (1, 180201.000000, 200501.0, Some("one")),
      (1, 200601.000000, 210101.0, Some("one")),
      (0, 180220.000000, 180401.0, Some("D"))
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCombine dfMapToCombine",dfMapToCombine)(actual,expected)
    assert(resultat)
  }

  test("linearCombine dfDirtyIntervals") {
    val actual = dfDirtyIntervals.linearCombine()
    val expected = Seq(
      (0, 190101.00000012346, 190105.12345612346, 3.14),
      (0, 190105.12345612346, 190201.0234561245 , 2.72),
      (0, 190201.0234561245 , 190404.00000      ,13.0 ),
      (0, 190905.0234561231 , 190905.0234561239 ,42.0 ),
      (0, 200101.01000     , intervalMaxValue   ,18.17),
      (1, 190101.00000012346, 190202.000000     ,-1.0 ),
      (1, 190301.00000      , 190301.0000000002 , 0.1 ),
      (1, 190301.0000000009 , 190301.000000002  , 0.1 ),
      (1, 190301.0000010009 , 190301.0000010021 , 1.2 ),
      (1, 190301.0000000001 , 190301.000000001  , 0.8 ),
      (1, 190303.01000     , 211201.0234561     ,-2.0 )
    ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCombine dfDirtyIntervals",dfDirtyIntervals)(actual,expected)
    assert(resultat)
  }

  test("linearCombine documentation") {
    val actual = dfDocumentation.linearCombine()
    val expected = Seq(
      (1, 190105.12345612346, 190201.0234561245, 2.72), // overlaps with previous record
      (1, 190101.00000      , 200101.0         ,42.0 ))
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearCombine documentation",dfDocumentation)(actual,expected)
    assert(resultat)
  }

  test("linearUnifyRanges dfMoment") {
    // Note that a Moment can not be modeled with ClosedFromOpenToInterval - result is therefore empty
    val actual = dfMoment.linearUnifyRanges(Seq("id"))
      .select(dfMoment.columns.map(col):_*) // re-order columns
    val expected = dfMoment.where(lit(false)) // empty data frame expected
    logger.info("expected:")
    expected.show(false)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearUnifyRanges dfMoment",dfMoment)(actual,expected)
    assert(resultat)
  }

  test("temporalUnifyRanges dfSmallOverlap") {
    val actual = dfSmallOverlap.linearUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0,"A", 190101.000000, 190101.100000),
      (0,"A", 190101.100000, 190101.100001),
      (0,"B", 190101.100000, 190101.100001),
      (0,"B", 190101.100001, 190102.0)
    ).toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)
    if (!resultat) printFailedTestResult("linearUnifyRanges dfSmallOverlap",dfSmallOverlap)(actual,expected)
    assert(resultat)
  }

  test("linearUnifyRanges dfMap") {
    val actual = dfMap.linearUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0,"A", 180101.000000, 180201.0),
      (0,"B", 180101.000000, 180201.0),
      // img = {B,C}
      (0,"B", 180201.000000, 180220.0),
      (0,"C", 180201.000000, 180220.0),
      // img = {B,C,D}
      (0,"B", 180220.000000, 180225.141516123),
      (0,"C", 180220.000000, 180225.141516123),
      (0,"D", 180220.000000, 180225.141516123),
      // img = {B,C,D}
      (0,"B", 180225.141516123, 180301.0),
      (0,"C", 180225.141516123, 180301.0),
      (0,"D", 180225.141516123, 180301.0),
      // img = {D}
      (0,"D", 180301.000000, 180401.0)
    ).toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual,expected)

    if (!resultat) printFailedTestResult("linearUnifyRanges dfMap",dfMap)(actual,expected)
    assert(resultat)
  }
}
