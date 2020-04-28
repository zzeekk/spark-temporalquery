package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import org.apache.spark.sql.functions.{col,lit}
import org.scalatest.FunSuite

import TemporalHelpers._
import TemporalQueryUtil._
import TestUtils._

class TemporalQueryUtilTest extends FunSuite {
  import session.implicits._

  test("temporalContinuous2discrete") {
    val actual = dfContinuousTime.temporalContinuous2discrete(defaultConfig)
    val expected = Seq(
      (0,"2019-01-01 00:00:00.124","2019-01-05 12:34:56.123", 3.14),
      (0,"2019-01-05 12:34:56.124","2019-02-01 02:34:56.123", 2.72),
      (0,"2019-02-01 02:34:56.124","2019-02-01 02:34:56.124",42.0 ),
      (0,"2019-02-01 02:34:56.125","2019-03-02 23:59:59.999",13.0 ),
      (0,"2019-03-03 00:00:0"     ,"2019-04-03 23:59:59.999",12.0 ),
      (0,"2020-01-01 01:00:0"     ,finisTemporisString,18.17),
      (1,"2019-01-01 00:00:0.124" ,"2019-02-01 23:59:59.999",-1.0 ),
      (1,"2019-03-03 01:00:0"     ,"2021-12-01 02:34:56.099",-2.0 )
    ).map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalContinuous2discrete",Seq(dfContinuousTime))(actual)(expected)
    assert(resultat)
  }

  test("temporalRoundDiscreteTime_dfLeft") {
    val actual = dfLeft.temporalRoundDiscreteTime(defaultConfig)
    val expected = dfLeft

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalRoundDiscreteTime",Seq(dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalRoundDiscreteTime_dfDirtyTimeRanges") {
    val actual = dfDirtyTimeRanges.temporalRoundDiscreteTime(defaultConfig)
    val zeilen_expected: Seq[(Int, String, String, Double)] = Seq(
      (0,"2019-01-01 00:00:00.124","2019-01-05 12:34:56.123", 3.14),
      (0,"2019-01-05 12:34:56.124","2019-02-01 02:34:56.123", 2.72),
      (0,"2019-02-01 01:00:0"     ,"2019-02-01 02:34:56.124", 2.72),
      (0,"2019-02-01 02:34:56.125","2019-03-03 00:00:0"     ,13.0 ),
      (0,"2019-03-03 00:00:0"     ,"2019-04-04 00:00:0"     ,13.0 ),
      (0,"2020-01-01 01:00:0"     ,finisTemporisString      ,18.17),
      (1,"2019-03-01 00:00:0"     ,"2019-03-01 00:00:0"     , 0.1 ), // duration extended to 1 millisecond
      (1,"2019-03-01 00:00:0.001" ,"2019-03-01 00:00:0.001" , 0.1 ), // duration extended to 1 millisecond
      (1,"2019-03-01 00:00:1.001" ,"2019-03-01 00:00:01.002", 1.2 ), // duration extended to 2 milliseconds
      (1,"2019-01-01 00:00:0.124" ,"2019-02-02 00:00:0"     ,-1.0 ),
      (1,"2019-03-03 01:00:0"     ,"2021-12-01 02:34:56.1"  ,-2.0 ))
    val expected = zeilen_expected.map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalRoundDiscreteTime",Seq(dfDirtyTimeRanges))(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfLeft") {
    val actual = dfLeft.temporalCleanupExtend(Seq("id"),Seq(col(defaultConfig.fromColName)))
    val expected = Seq(
      (0, None     , initiumTemporisString, "2017-12-09 23:59:59.999"),
      (0, Some(4.2), "2017-12-10 00:00:00", "2018-12-08 23:59:59.999"),
      (0, None     , "2018-12-09 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert_l",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfLeft",dfLeft)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfRight_noExtend_nofillGaps") {
    val actual = dfRight.temporalCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(col(defaultConfig.fromColName)),
      extend = false,
      fillGapsWithNull = false
    )
    val expected = Seq(
      (0, Some(97.15) , "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, Some(97.15) , "2018-06-01 05:24:11", finisTemporisString),
      (1, None        , "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
      (1, None        , "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfRight_noExtend_nofillGaps",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfRight_fillGaps_noExtend") {
    val actual = dfRight.temporalCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(col(defaultConfig.fromColName)),
      extend = false
    )
    val expected = Seq(
      (0, Some(97.15) , "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, None        , "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
      (0, Some(97.15) , "2018-06-01 05:24:11", finisTemporisString),
      (1, None        , "2018-01-01 00:00:00", "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
      (1, None        , "2021-01-01 00:00:00", "2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfRight_fillGaps_noExtend",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfRight_extend_nofillGaps") {
    val actual = dfRight.temporalCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(col(defaultConfig.fromColName)),
      fillGapsWithNull = false
    )
    val expected = Seq(
      (0, Some(97.15) , "2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0, Some(97.15) , "2018-06-01 05:24:11",finisTemporisString),
      (1, None        , "2018-01-01 00:00:00","2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00","2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00","2020-12-31 23:59:59.999"),
      (1, None        , "2021-01-01 00:00:00","2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfRight_extend_nofillGaps",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfRight_extend_fillGaps") {
    val actual = dfRight.temporalCleanupExtend(
      keys=Seq("id"),
      rnkExpressions=Seq(col(defaultConfig.fromColName)))
    val expected = Seq(
      (0, None        , initiumTemporisString , "2017-12-31 23:59:59.999"),
      (0, Some(97.15) , "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, None        , "2018-02-01 00:00:00", "2018-06-01 05:24:10.999"),
      (0, Some(97.15) , "2018-06-01 05:24:11",finisTemporisString),
      (1, None        , initiumTemporisString, "2018-12-31 23:59:59.999"),
      (1, Some(2019.0), "2019-01-01 00:00:00", "2019-12-31 23:59:59.999"),
      (1, Some(2020.0), "2020-01-01 00:00:00", "2020-12-31 23:59:59.999"),
      (1, None        , "2021-01-01 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert_r",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfRight_extend_fillGaps",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfMap") {
    val actual = dfMap.temporalCleanupExtend(Seq("id"),Seq($"img"))
    val expected = Seq(
      (0, None     , initiumTemporisString, "2017-12-31 23:59:59.999"),
      (0, Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      (0, Some("D"), "2018-03-01 00:00:00", "2018-03-31 23:59:59.999"),
      (0, None     , "2018-04-01 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[String]])
      .toDF("id","img",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfMap",dfMap)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfMap_NoExtendFillgaps") {
    val actual = dfMap.temporalCleanupExtend(Seq("id"),Seq($"img"),extend=false,fillGapsWithNull=false)
    val expected = Seq(
      (0, Some("A"), "2018-01-01 00:00:00", "2018-01-31 23:59:59.999"),
      (0, Some("B"), "2018-02-01 00:00:00", "2018-02-28 23:59:59.999"),
      (0, Some("D"), "2018-03-01 00:00:00", "2018-03-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[String]])
      .toDF("id","img",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfMap_NoExtendFillgaps",dfMap)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfMsOverlap") {
    val actual = dfMsOverlap.temporalCleanupExtend(Seq("id"),Seq(col(defaultConfig.fromColName)))
    actual.printSchema()
    val expected = Seq(
      (0, None     , initiumTemporisString    , "2018-12-31 23:59:59.999"),
      (0, Some("A"), "2019-01-01 00:00:00"    , "2019-01-01 10:00:00"),
      (0, Some("B"), "2019-01-01 10:00:00.001", "2019-01-01 23:59:59.999"),
      (0, None     , "2019-01-02 00:00:00"    , finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[String]])
      .toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    expected.printSchema()

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfMap",dfMap)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfDirtyTimeRanges") {
    val actual = dfDirtyTimeRanges.temporalCleanupExtend(Seq("id"),Seq(col(defaultConfig.fromColName),$"wert"))
    val expected = Seq(
      (0, None       , initiumTemporisString    , "2019-01-01 00:00:00.123"),
      (0, Some(3.14) , "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, Some(2.72) , "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, Some(13.0) , "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, None       , "2019-04-04 00:00:00.001", "2020-01-01 00:59:59.999"),
      (0, Some(18.17), "2020-01-01 01:00:0"     , finisTemporisString),
      (1, None       , initiumTemporisString    , "2019-01-01 00:00:00.123"),
      (1, Some(-1.0) , "2019-01-01 00:00:0.124" , "2019-02-02 00:00:0"),
      (1, None       , "2019-02-02 00:00:0.001" , "2019-02-28 23:59:59.999"),
      (1, Some(0.1)  , "2019-03-01 00:00:0"     , "2019-03-01 00:00:00.001"),
      (1, None       , "2019-03-01 00:00:00.002", "2019-03-01 00:00:1"),
      (1, Some(1.2)  , "2019-03-01 00:00:1.001" , "2019-03-01 00:00:01.002"),
      (1, None       , "2019-03-01 00:00:1.003" , "2019-03-03 00:59:59.999"),
      (1, Some(-2.0) , "2019-03-03 01:00:0"     , "2021-12-01 02:34:56.1"),
      (1, None       , "2021-12-01 02:34:56.101", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfDirtyTimeRanges",dfDirtyTimeRanges.where($"id"===1))(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_dfDirtyTimeRanges_NoExtendFillgaps") {
    val actual = dfDirtyTimeRanges.temporalCleanupExtend(Seq("id"),Seq(col(defaultConfig.fromColName),$"wert"),extend=false,fillGapsWithNull=false)
    val expected = Seq(
      (0, Some(3.14) , "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123"),
      (0, Some(2.72) , "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.124"),
      (0, Some(13.0) , "2019-02-01 02:34:56.125", "2019-04-04 00:00:0"),
      (0, Some(18.17), "2020-01-01 01:00:0"     , finisTemporisString),
      (1, Some(-1.0) , "2019-01-01 00:00:0.124" , "2019-02-02 00:00:0"),
      (1, Some(0.1)  , "2019-03-01 00:00:0"     , "2019-03-01 00:00:00.001"),
      (1, Some(1.2)  , "2019-03-01 00:00:1.001" , "2019-03-01 00:00:01.002"),
      (1, Some(-2.0) , "2019-03-03 01:00:0"     , "2021-12-01 02:34:56.1")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id","wert",defaultConfig.fromColName,defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCleanupExtend_dfDirtyTimeRanges_NoExtendFillgaps"
      ,dfDirtyTimeRanges.where($"id"===1))(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_validityDuration") {
    val argument = Seq(
      (1, "A", "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, "A", "2020-07-05 00:00:00", "2020-07-07 23:59:59.999"),
      (1, "B", "2020-07-01 00:00:00", "2020-07-02 23:59:59.999"),
      (1, "B", "2020-07-04 00:00:00", "2020-07-07 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
    // we want the record with the longest validity period, i.e. maximal toColName-fromColName
    val actual = argument.temporalCleanupExtend(Seq("id"), Seq(udf_durationInMillis(col(defaultConfig.toColName),col(defaultConfig.fromColName)).desc))
    val expected = Seq(
      (1, None     , initiumTemporisString, "2020-06-30 23:59:59.999"),
      (1, Some("A"), "2020-07-01 00:00:00", "2020-07-03 23:59:59.999"),
      (1, Some("B"), "2020-07-04 00:00:00", "2020-07-07 23:59:59.999"),
      (1, None     , "2020-07-08 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, Option[String]])
      .toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined", lit(true))
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_validityDuration", argument)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_rankExprFromColOnly") {
    val argument = Seq(
      (1, "S", initiumTemporisString, finisTemporisString),
      (1, "X", "2020-07-01 00:00:00", finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
    val actual = argument.temporalCleanupExtend(Seq("id"), Seq(col(defaultConfig.fromColName)))
    val expected = Seq(
      (1, "S", initiumTemporisString, finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int, String])
      .toDF("id", "val", defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined", lit(true))
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalCleanupExtend_rankExprFromColOnly", argument)(actual)(expected)
    assert(resultat)
  }

  test("temporalCleanupExtend_rankExpr2Cols") {
    val argument = Seq(
      (1,"S",initiumTemporisString,"2020-06-30 23:59:59.999"),
      (1,"X","2020-07-01 00:00:00","2020-09-23 23:59:59.999"),
      (1,"B","2020-08-03 00:00:00",finisTemporisString),
      (1,"G","2020-09-24 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,String])
      .toDF("id","val",defaultConfig.fromColName, defaultConfig.toColName)
    val actual = argument.temporalCleanupExtend(Seq("id"),Seq(col(defaultConfig.toColName).desc,col(defaultConfig.fromColName).asc))(session,defaultConfig)
    val expected = Seq(
      (1,"S",initiumTemporisString,"2020-06-30 23:59:59.999"),
      (1,"X","2020-07-01 00:00:00","2020-08-02 23:59:59.999"),
      (1,"B","2020-08-03 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,String])
      .toDF("id","val",defaultConfig.fromColName, defaultConfig.toColName)
      .withColumn("_defined",lit(true))
    val resultat2 = dfEqual(actual)(expected)
    if (!resultat2) printFailedTestResult("temporalCleanupExtend_rankExpr2Cols",argument)(actual)(expected)
    assert(resultat2)
  }

  test("temporalExtendRange_dfLeft") {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.temporalExtendRange(Seq("id"))
    val rowsExpected = Seq((0,4.2,defaultConfig.minDate,defaultConfig.maxDate))
    val expected = rowsExpected.toDF("id", "Wert_L", defaultConfig.fromColName, defaultConfig.toColName )
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithActualColumns)

    if (!resultat) printFailedTestResult("temporalExtendRange_dfLeft",dfLeft)(actual)(expectedWithActualColumns)
    assert(resultat)
  }

  test("temporalExtendRange_dfRight_id") {
    val actual = dfRight.temporalExtendRange(Seq("id"))
    val expected = Seq(
      (0,Some(97.15) ,initiumTemporisString  ,"2018-01-31 23:59:59.999"),
      (0,Some(97.15) ,"2018-06-01 05:24:11.0","2018-10-23 03:50:09.999"),
      (0,Some(97.15) ,"2018-10-23 03:50:10"  ,"2019-12-31 23:59:59.999"),
      (0,Some(97.15) ,"2020-01-01 00:00:00"  ,finisTemporisString),
      (1,None        ,initiumTemporisString  ,"2018-12-31 23:59:59.999"),
      (1,Some(2019.0),"2019-01-01 00:00:00.0","2019-12-31 23:59:59.999"),
      (1,Some(2020.0),"2020-01-01 00:00:00.0","2020-12-31 23:59:59.999"),
      (1,None        ,"2021-01-01 00:00:00.0",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithActualColumns)
    if (!resultat) printFailedTestResult("temporalExtendRange_dfRight_id",dfRight)(actual)(expectedWithActualColumns)
    assert(resultat)
  }

  test("temporalExtendRange_dfRight") {
    // argument: dfRight from object TestUtils
    val actual = dfRight.temporalExtendRange()
    val expected = Seq(
      (0,Some(97.15) ,initiumTemporisString  ,"2018-01-31 23:59:59.999"),
      (0,Some(97.15) ,"2018-06-01 05:24:11.0","2018-10-23 03:50:09.999"),
      (0,Some(97.15) ,"2018-10-23 03:50:10"  ,"2019-12-31 23:59:59.999"),
      (0,Some(97.15) ,"2020-01-01 00:00:00"  ,finisTemporisString),
      (1,None        ,initiumTemporisString  ,"2018-12-31 23:59:59.999"),
      (1,Some(2019.0),"2019-01-01 00:00:00.0","2019-12-31 23:59:59.999"),
      (1,Some(2020.0),"2020-01-01 00:00:00.0","2020-12-31 23:59:59.999"),
      (1,None        ,"2021-01-01 00:00:00.0","2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double]])
      .toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithActualColumns)
    if (!resultat) printFailedTestResult("temporalExtendRange_dfRight",dfRight)(actual)(expectedWithActualColumns)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfRight") {
    val actual = dfLeft.temporalLeftAntiJoin(dfRight,Seq("id"))
    val expected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", 4.2)
    ).map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfMap") {
    val actual = dfLeft.temporalLeftAntiJoin(dfMap,Seq("id"))
    val expected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999", 4.2)
    ).map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfRight_dfMap") {
    val actual = dfRight.temporalLeftAntiJoin(dfMap,Seq("id"))
    val rowsExpected: Seq[(Int, String, String, Option[Double])] = Seq(
      (0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
      (0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
      (0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
      (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
      (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019)),
      (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020)),
      (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None))
    val expected = rowsExpected.map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfRight_dfMap",Seq(dfRight,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfMap_dfRight") {
    val actual = dfMap.temporalLeftAntiJoin(dfRight,Seq("id"))
    val rowsExpected: Seq[(Int, String, String, String)] = Seq(
      (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
      (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfMap_dfRight",Seq(dfMap,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_segmented") {
    val minuend = Seq(
      (1,"2019-01-01 00:00:0", "2020-01-01 00:00:0"),
      (2,"2019-01-01 00:00:0", "2020-01-01 00:00:5"),
      (3,"2020-01-01 00:05:7", "2021-12-31 23:59:59.999"),
      (4,"2020-01-01 00:02:5", "2020-01-01 00:03:5"),
      (5,"2019-01-01 00:00:0", "2020-01-01 00:01:9.999"),
      (6,"2019-01-01 00:00:0", "2021-12-31 23:59:59.999")
    ).map(x => (x._1,Timestamp.valueOf(x._2),Timestamp.valueOf(x._3)))
      .toDF("id",defaultConfig.fromColName, defaultConfig.toColName)
    val subtrahend = Seq(
      ("2020-01-01 00:04:4","2020-01-01 00:05:0"),
      ("2020-01-01 00:00:1","2020-01-01 00:01:0"),
      ("2020-01-01 00:03:3","2020-01-01 00:04:0"),
      ("2020-01-01 00:05:5","2020-01-01 00:06:0"),
      ("2020-01-01 00:02:2","2020-01-01 00:03:0")
    ).map(x => (0,Timestamp.valueOf(x._1),Timestamp.valueOf(x._2)))
      .toDF("id",defaultConfig.fromColName, defaultConfig.toColName)

    val actual = minuend.temporalLeftAntiJoin(subtrahend,Seq())
    val expected = Seq(
      (1,"2019-01-01 00:00:0"    ,"2020-01-01 00:00:0"),
      (2,"2019-01-01 00:00:0"    ,"2020-01-01 00:00:0.999"),
      (3,"2020-01-01 00:06:0.001","2021-12-31 23:59:59.999"),
      (4,"2020-01-01 00:03:0.001","2020-01-01 00:03:2.999"),
      (5,"2020-01-01 00:01:0.001","2020-01-01 00:01:9.999"),
      (5,"2019-01-01 00:00:0"    ,"2020-01-01 00:00:0.999"),
      (6,"2020-01-01 00:06:0.001","2021-12-31 23:59:59.999"),
      (6,"2020-01-01 00:05:0.001","2020-01-01 00:05:4.999"),
      (6,"2020-01-01 00:04:0.001","2020-01-01 00:04:3.999"),
      (6,"2020-01-01 00:03:0.001","2020-01-01 00:03:2.999"),
      (6,"2020-01-01 00:01:0.001","2020-01-01 00:02:1.999"),
      (6,"2019-01-01 00:00:0"    ,"2020-01-01 00:00:0.999")
    ).map(x => (x._1,Timestamp.valueOf(x._2),Timestamp.valueOf(x._3)))
      .toDF("id",defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_segmented",Seq(minuend,subtrahend))(actual)(expected)
    assert(resultat)
  }

  test("temporalFullJoin_dfRight") {
    val actual = dfLeft.temporalFullJoin(dfRight,Seq("id"))
    val expected = Seq(
      // id = 0
      (0,None     ,None       ,initiumTemporisString,"2017-12-09 23:59:59.999"),
      (0,Some(4.2),None       ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      (0,Some(4.2),Some(97.15),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),None       ,"2018-02-01 00:00:00","2018-06-01 05:24:10.999"),
      (0,Some(4.2),Some(97.15),"2018-06-01 05:24:11","2018-12-08 23:59:59.999"),
      (0,None     ,Some(97.15),"2018-12-09 00:00:00",finisTemporisString),
      // id = 1
      (1,None,None        ,initiumTemporisString  ,"2018-12-31 23:59:59.999"),
      (1,None,Some(2019.0),"2019-01-01 00:00:00","2019-12-31 23:59:59.999"),
      (1,None,Some(2020.0),"2020-01-01 00:00:00","2020-12-31 23:59:59.999"),
      (1,None,None        ,"2021-01-01 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[Double]])
      .toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalFullJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalFullJoin_rightMap") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalFullJoin(df2=dfMap, keys=Seq("id"))
    val expected = Seq(
      // img = {}
      (0,None     ,None     ,initiumTemporisString  ,"2017-12-09 23:59:59.999"),
      (0,Some(4.2),None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      (0,Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),Some("B"),"2018-01-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("C"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("D"),"2018-02-20 00:00:00","2018-03-31 23:59:59.999"),
      (0,Some(4.2),Some("X"),"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      (0,Some(4.2),None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999"),
      (0,None     ,None     ,"2018-12-09 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalFullJoin_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalFullJoin_rightMapWithrnkExpressions") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalFullJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected = Seq(
      // img = {}
      (Some(0),None     ,None     ,initiumTemporisString  ,"2017-12-09 23:59:59.999"),
      (Some(0),Some(4.2),None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      // img = {A}
      (Some(0),Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      // img = {B}
      (Some(0),Some(4.2),Some("B"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      // img = {D}
      (Some(0),Some(4.2),Some("D"),"2018-03-01 00:00:00","2018-03-31 23:59:59.999"),
      // img = {}
      (Some(0),Some(4.2),None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999"),
      (Some(0),None     ,None     ,"2018-12-09 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int],Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalFullJoin_rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalFullJoin_rightMapWithGapsAndRnkExpressions") {
    // Testing temporalFullJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00", "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X"))
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val actual = dfLeft.temporalFullJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected = Seq(
      // img = {}
      (Some(0),None     ,None     ,initiumTemporisString  ,"2017-12-09 23:59:59.999"),
      (Some(0),Some(4.2),None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      // img = {A}
      (Some(0),Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      // img = {B}
      (Some(0),Some(4.2),Some("B"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      // img = null
      (Some(0),Some(4.2),None,"2018-03-01 00:00:00","2018-03-29 23:59:59.999"),
      // img = {D}
      (Some(0),Some(4.2),Some("D"),"2018-03-30 00:00:00","2018-03-31 23:59:59.999"),
      // img = {}
      (Some(0),Some(4.2),None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999"),
      (Some(0),None     ,None     ,"2018-12-09 00:00:00",finisTemporisString)
    ).map(makeRowsWithTimeRangeEnd[Option[Int],Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalFullJoin_rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_dfRight") {
    val actual = dfLeft.temporalLeftJoin(dfRight,Seq("id"))
    val expected = Seq(
      (0,4.2,None       ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      (0,4.2,Some(97.15),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,4.2,None       ,"2018-02-01 00:00:00","2018-06-01 05:24:10.999"),
      (0,4.2,Some(97.15),"2018-06-01 05:24:11","2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Double,Option[Double]])
      .toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalLeftJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMap") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"))
    val expected = Seq(
      // img = {}
      (0,4.2,None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      (0,4.2,Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,4.2,Some("B"),"2018-01-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,4.2,Some("C"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,4.2,Some("D"),"2018-02-20 00:00:00","2018-03-31 23:59:59.999"),
      (0,4.2,Some("X"),"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      (0,4.2,None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Double,Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMapWithrnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected =   Seq(
      // img = {}
      (0,4.2,None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      // img = {A}
      (0,4.2,Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      // img = {B}
      (0,4.2,Some("B"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      // img = {D}
      (0,4.2,Some("D"),"2018-03-01 00:00:00","2018-03-31 23:59:59.999"),
      // img = {}
      (0,4.2,None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Double,Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMapWithGapsAndRnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00", "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X"))
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")
    val actual = dfLeft.temporalLeftJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected = Seq(
      // img = {}
      (0,4.2,None     ,"2017-12-10 00:00:00","2017-12-31 23:59:59.999"),
      // img = {A}
      (0,4.2,Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      // img = {B}
      (0,4.2,Some("B"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      // img = null
      (0,4.2,None,"2018-03-01 00:00:00","2018-03-29 23:59:59.999"),
      // img = {D}
      (0,4.2,Some("D"),"2018-03-30 00:00:00","2018-03-31 23:59:59.999"),
      // img = {}
      (0,4.2,None     ,"2018-04-01 00:00:00","2018-12-08 23:59:59.999"))
      .map(makeRowsWithTimeRangeEnd[Int,Double,Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalRightJoin_dfRight") {
    val actual = dfLeft.temporalRightJoin(dfRight,Seq("id"))
    val expected = Seq(
      // id = 0
      (0,Some(4.2),Some(97.15),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),Some(97.15),"2018-06-01 05:24:11","2018-10-23 03:50:09.999"),
      (0,Some(4.2),Some(97.15),"2018-10-23 03:50:10","2018-12-08 23:59:59.999"),
      (0,None     ,Some(97.15),"2018-12-09 00:00:00","2019-12-31 23:59:59.999"),
      (0,None     ,Some(97.15),"2020-01-01 00:00:00",finisTemporisString),
      // id = 1
      (1,None,None        ,"2018-01-01 00:00:00","2018-12-31 23:59:59.999"),
      (1,None,Some(2019.0),"2019-01-01 00:00:00","2019-12-31 23:59:59.999"),
      (1,None,Some(2020.0),"2020-01-01 00:00:00","2020-12-31 23:59:59.999"),
      (1,None,None        ,"2021-01-01 00:00:00","2099-12-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[Double]])
      .toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalRightJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalRightJoin_rightMap") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalRightJoin(df2=dfMap, keys=Seq("id"))
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),Some("B"),"2018-01-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("C"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("D"),"2018-02-20 00:00:00","2018-03-31 23:59:59.999"),
      (0,Some(4.2),Some("X"),"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalRightJoin_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalRightJoin_rightMapWithrnkExpressions") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    val actual = dfLeft.temporalRightJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),Some("B"),"2018-01-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("C"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("D"),"2018-02-20 00:00:00","2018-03-31 23:59:59.999"),
      (0,Some(4.2),Some("X"),"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalRightJoin_rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalRightJoin_rightMapWithGapsAndRnkExpressions") {
    // Testing temporalRightJoin where the right dataFrame is not unique for join attributes
    // but in a right join rnkExpressions are applied to left data frame
    // and gaps are of the left frame only are filled
    val argumentRight = Seq(
      (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
      (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
      (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
      (0, "2018-03-30 00:00:00", "2018-03-31 23:59:59.999", "D"),
      (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X"))
      .map(makeRowsWithTimeRange)
      .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

    val actual = dfLeft.temporalRightJoin(df2=argumentRight, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val expected = Seq(
      // img = {}
      (0,Some(4.2),Some("A"),"2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,Some(4.2),Some("B"),"2018-01-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("C"),"2018-02-01 00:00:00","2018-02-28 23:59:59.999"),
      (0,Some(4.2),Some("D"),"2018-03-30 00:00:00","2018-03-31 23:59:59.999"),
      (0,Some(4.2),Some("X"),"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123")
    ).map(makeRowsWithTimeRangeEnd[Int,Option[Double],Option[String]])
      .toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalRightJoin_rightMapWithGapsAndRnkExpressions",Seq(dfLeft,argumentRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine_dfRight") {
    val actual = dfRight.temporalCombine()
    val rowsExpected = Seq(
      (0,"2018-01-01 00:00:00.0","2018-01-31 23:59:59.999",Some(97.15) ),
      (0,"2018-06-01 05:24:11.0",finisTemporisString      ,Some(97.15) ),
      (1,"2018-01-01 00:00:00.0","2018-12-31 23:59:59.999",None        ),
      (1,"2019-01-01 00:00:00.0","2019-12-31 23:59:59.999",Some(2019.0)),
      (1,"2020-01-01 00:00:00.0","2020-12-31 23:59:59.999",Some(2020.0)),
      (1,"2021-01-01 00:00:00.0","2099-12-31 23:59:59.999",None        )
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_r")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCombine_dfRight",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine_dfMapToCombine") {
    val actual = dfMapToCombine.temporalCombine()
    val rowsExpected = Seq(
      (0,"2018-01-01 00:00:00","2018-12-31 23:59:59.999", Some("A")),
      (0,"2018-01-01 00:00:00","2018-02-03 23:59:59.999", Some("B")),
      (0,"2018-02-01 00:00:00","2020-04-30 23:59:59.999", None),
      (0,"2020-06-01 00:00:00","2020-12-31 23:59:59.999", None),
      (1,"2018-02-01 00:00:00","2020-04-30 23:59:59.999", Some("one")),
      (1,"2020-06-01 00:00:00","2020-12-31 23:59:59.999", Some("one")),
      (0,"2018-02-20 00:00:00","2018-03-31 23:59:59.999", Some("D")),
      (0,"2018-02-25 14:15:16.123","2018-02-25 14:15:16.123", Some("X"))
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCombine_dfMapToCombine",dfMapToCombine)(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine_dirtyTimeRanges") {
    val actual = dfDirtyTimeRanges.temporalCombine()
    val rowsExpected = Seq(
      (0,"2019-01-01 00:00:00.124","2019-01-05 12:34:56.123", 3.14),
      (0,"2019-01-05 12:34:56.124","2019-02-01 02:34:56.124", 2.72),
      (0,"2019-02-01 02:34:56.125","2019-04-04 00:00:0"     ,13.0 ),
      (0,"2020-01-01 01:00:0"     ,finisTemporisString      ,18.17),
      (1,"2019-03-01 00:00:0"     ,"2019-03-01 00:00:0.001" , 0.1 ), // duration extended to 2 milliseconds
      (1,"2019-03-01 00:00:1.001" ,"2019-03-01 00:00:01.002", 1.2 ), // duration extended to 2 milliseconds
      (1,"2019-01-01 00:00:00.124","2019-02-02 00:00:00"    ,-1.0 ),
      (1,"2019-03-03 01:00:0"     ,"2021-12-01 02:34:56.1"  ,-2.0 )
    )
    val expected = rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCombine_dirtyTimeRanges",dfMapToCombine)(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine_documentation") {
    val actual = dfDocumentation.temporalCombine()
    val rowsExpected = Seq(
      (1,"2019-01-05 12:34:56.124","2019-02-01 02:34:56.124", 2.72), // overlaps with previous record
      (1,"2019-01-01 00:00:0"     ,"2019-12-31 23:59:59.999",42.0 ))
    val expected = rowsExpected.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalCombine_documentation",dfDocumentation)(actual)(expected)
    assert(resultat)
  }

  test("temporalUnifyRanges1") {
    val actual = dfMoment.temporalUnifyRanges(Seq("id"))
      .select(dfMoment.columns.map(col):_*) // re-order columns
    val expected = dfMoment
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalUnifyRanges1",dfMoment)(actual)(expected)
    assert(resultat)
  }

  test("temporalUnifyRanges2") {
    val actual = dfMsOverlap.temporalUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0,"A","2019-01-01 00:00:00","2019-01-01 9:59:59.999"),
      (0,"A","2019-01-01 10:00:00","2019-01-01 10:00:00"),
      (0,"B","2019-01-01 10:00:00","2019-01-01 10:00:00"),
      (0,"B","2019-01-01 10:00:00.001","2019-01-01 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,String])
      .toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalUnifyRanges2",dfMsOverlap)(actual)(expected)
    assert(resultat)
  }

  test("temporalUnifyRanges3") {
    val actual = dfMap.temporalUnifyRanges(Seq("id"))
    val expected = Seq(
      // img = {A,B}
      (0,"A","2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      (0,"B","2018-01-01 00:00:00","2018-01-31 23:59:59.999"),
      // img = {B,C}
      (0,"B","2018-02-01 00:00:00","2018-02-19 23:59:59.999"),
      (0,"C","2018-02-01 00:00:00","2018-02-19 23:59:59.999"),
      // img = {B,C,D}
      (0,"B","2018-02-20 00:00:00","2018-02-25 14:15:16.122"),
      (0,"C","2018-02-20 00:00:00","2018-02-25 14:15:16.122"),
      (0,"D","2018-02-20 00:00:00","2018-02-25 14:15:16.122"),
      // img = {B,C,D,X}
      (0,"B","2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      (0,"C","2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      (0,"D","2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      (0,"X","2018-02-25 14:15:16.123","2018-02-25 14:15:16.123"),
      // img = {B,C,D}
      (0,"B","2018-02-25 14:15:16.124","2018-02-28 23:59:59.999"),
      (0,"C","2018-02-25 14:15:16.124","2018-02-28 23:59:59.999"),
      (0,"D","2018-02-25 14:15:16.124","2018-02-28 23:59:59.999"),
      // img = {D}
      (0,"D","2018-03-01 00:00:00","2018-03-31 23:59:59.999")
    ).map(makeRowsWithTimeRangeEnd[Int,String])
      .toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalUnifyRanges3",dfMap)(actual)(expected)
    assert(resultat)
  }
}
