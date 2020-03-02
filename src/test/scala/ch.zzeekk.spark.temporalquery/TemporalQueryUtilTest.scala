package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import org.apache.spark.sql.functions.col
import org.scalatest.FunSuite

import TemporalQueryUtil._
import TestUtils._

class TemporalQueryUtilTest extends FunSuite {
  import ss.implicits._

  test("roundDiscreteTime_dfLeft") {
    val actual = dfLeft.roundDiscreteTime(defaultConfig)
    val expected = dfLeft

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("roundDiscreteTime",Seq(dfRight))(actual)(expected)
    assert(resultat)
  }

  test("roundDiscreteTime_dfDirtyTimeRanges") {
    val actual = dfDirtyTimeRanges.roundDiscreteTime(defaultConfig)
    val zeilen_expected: Seq[(Int, String, String, Double)] = Seq(
      (0,"2019-01-01 00:00:00.124","2019-01-05 12:34:56.123", 3.14),
      (0,"2019-01-05 12:34:56.124","2019-02-01 02:34:56.123", 2.72),
      (0,"2019-02-01 01:00:0"     ,"2019-02-01 02:34:56.124", 2.72),
      (0,"2019-02-01 02:34:56.125","2019-03-03 00:00:0"     ,13.0 ),
      (0,"2019-03-03 00:00:0"     ,"2019-04-04 00:00:0"     ,13.0 ),
      (0,"2020-01-01 01:00:0"     ,"9999-12-31 00:00:0"     ,18.17),
      (1,"2019-03-01 00:00:0"     ,"2019-03-01 00:00:0"     , 0.1 ), // duration extended to 1 millisecond
      (1,"2019-03-01 00:00:0.001" ,"2019-03-01 00:00:0.001" , 0.1 ), // duration extended to 1 millisecond
      (1,"2019-03-01 00:00:1.001" ,"2019-03-01 00:00:01.002", 1.2 ), // duration extended to 2 milliseconds
      (1,"2019-01-01 00:00:0.124" ,"2019-02-02 00:00:0"     ,-1.0 ),
      (1,"2019-03-03 01:00:0"     ,"2021-12-01 02:34:56.1"  ,-2.0 ))
    val expected = zeilen_expected.map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("roundDiscreteTime",Seq(dfDirtyTimeRanges))(actual)(expected)
    assert(resultat)
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
    val rowsExpected = Seq(
      (0,Some(97.15) ,defaultConfig.minDate                     ,Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2018-10-23 03:50:10")  ,Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2020-01-01 00:00:00")  ,defaultConfig.maxDate),
      (1,None        ,defaultConfig.minDate                     ,Timestamp.valueOf("2018-12-31 23:59:59.999")),
      (1,Some(2019.0),Timestamp.valueOf("2019-01-01 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (1,Some(2020.0),Timestamp.valueOf("2020-01-01 00:00:00.0"),Timestamp.valueOf("2020-12-31 23:59:59.999")),
      (1,None        ,Timestamp.valueOf("2021-01-01 00:00:00.0"),defaultConfig.maxDate)
    )
    val expected = rowsExpected.toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithActualColumns)

    if (!resultat) printFailedTestResult("temporalExtendRange_dfRight_id",dfRight)(actual)(expectedWithActualColumns)
    assert(resultat)
  }

  test("temporalExtendRange_dfRight") {
    // argument: dfRight from object TestUtils
    val actual = dfRight.temporalExtendRange()
    val rowsExpected = Seq(
      (0,Some(97.15) ,defaultConfig.minDate                     ,Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2018-10-23 03:50:10")  ,Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (0,Some(97.15) ,Timestamp.valueOf("2020-01-01 00:00:00")  ,defaultConfig.maxDate),
      (1,None        ,defaultConfig.minDate                     ,Timestamp.valueOf("2018-12-31 23:59:59.999")),
      (1,Some(2019.0),Timestamp.valueOf("2019-01-01 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999")),
      (1,Some(2020.0),Timestamp.valueOf("2020-01-01 00:00:00.0"),Timestamp.valueOf("2020-12-31 23:59:59.999")),
      (1,None        ,Timestamp.valueOf("2021-01-01 00:00:00.0"),Timestamp.valueOf("2099-12-31 23:59:59.999"))
    )
    val expected = rowsExpected.toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithActualColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithActualColumns)

    if (!resultat) printFailedTestResult("temporalExtendRange_dfRight",dfRight)(actual)(expectedWithActualColumns)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfRight") {
    val actual = dfLeft.temporalLeftAntiJoin(dfRight,Seq("id"))
    val rowsExpected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-02-01 00:00:00", "2018-06-01 05:24:10.999", 4.2)
    )
    val expected = rowsExpected.toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfMap") {
    val actual = dfLeft.temporalLeftAntiJoin(dfMap,Seq("id"))
    val rowsExpected = Seq(
      (0, "2017-12-10 00:00:00", "2017-12-31 23:59:59.999", 4.2),
      (0, "2018-04-01 00:00:00", "2018-12-08 23:59:59.999", 4.2)
    )
    val expected = rowsExpected.toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftAntiJoin_dfMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftAntiJoin_dfRight_dfMap") {
    val actual = dfRight.temporalLeftAntiJoin(dfMap,Seq("id"))
    val rowsExpected: Seq[(Int, String, String, Option[Double])] = Seq(
      (0, "2018-06-01 05:24:11", "9999-12-31 00:00:00",     Some(97.15)),
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

  test("temporalLeftJoinNew_dfRight") {
    val actual = dfLeft.temporalLeftJoinNew(dfRight,Seq("id"))
    val expected = Seq(
      (0,4.2,None       ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,None       ,Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-06-01 05:24:10.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-06-01 05:24:11"),Timestamp.valueOf("2018-12-08 23:59:59.999"))
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoinNew_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoinNew_rightMap") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoinNew(df2=dfMap, keys=Seq("id"))
    val expected = Seq(
      (0,4.2,None     ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,4.2,Some("A"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("D"),Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")),
      (0,4.2,Some("X"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999"))
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoinNew_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_dfRight") {
    val actual = dfLeft.temporalLeftJoin(dfRight,Seq("id"))
    val expected = Seq(
      (0,4.2,None       ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,None       ,Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-06-01 05:24:10.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-06-01 05:24:11"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-10-23 03:50:10"),Timestamp.valueOf("2018-12-08 23:59:59.999"))
    ).toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_dfRight",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMap") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"))
    val expected = Seq(
      // img = {}
      (0,4.2,None     ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      // img = {A,B}
      (0,4.2,Some("A"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      // img = {B,C}
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-19 23:59:59.999")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-19 23:59:59.999")),
      // img = {B,C,D}
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      (0,4.2,Some("D"),Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      // img = {B,C,D,X}
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,4.2,Some("D"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,4.2,Some("X"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      // img = {B,C,D}
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("D"),Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      // img = {D}
      (0,4.2,Some("D"),Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")),
      // img = {}
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999"))
    ).toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMapWithrnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq($"img",col(defaultConfig.fromColName)))
    val rowsExpected = Seq(
      // img = {}
      (0,4.2,None     ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      // img = {A}
      (0,4.2,Some("A"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      // img = {B}
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-19 23:59:59.999")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      // img = {D}
      (0,4.2,Some("D"),Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")),
      // img = {}
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine_dfRight") {
    val actual = dfRight.temporalCombine()
    val rowsExpected = Seq(
      (0,"2018-01-01 00:00:00.0","2018-01-31 23:59:59.999",Some(97.15) ),
      (0,"2018-06-01 05:24:11.0","9999-12-31 00:00:0"     ,Some(97.15) ),
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
      (0,"2020-01-01 01:00:0"     ,"9999-12-31 00:00:0"     ,18.17),
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
    val rowsExpected = Seq(
      // img = {A,B}
      (0,"A",Timestamp.valueOf("2019-01-01 00:00:00"),Timestamp.valueOf("2019-01-01 9:59:59.999")),
      (0,"A",Timestamp.valueOf("2019-01-01 10:00:00"),Timestamp.valueOf("2019-01-01 10:00:00")),
      (0,"B",Timestamp.valueOf("2019-01-01 10:00:00"),Timestamp.valueOf("2019-01-01 10:00:00")),
      (0,"B",Timestamp.valueOf("2019-01-01 10:00:00.001"),Timestamp.valueOf("2019-01-01 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)
    if (!resultat) printFailedTestResult("temporalUnifyRanges2",dfMsOverlap)(actual)(expected)
    assert(resultat)
  }

  test("temporalUnifyRanges3") {
    val actual = dfMap.temporalUnifyRanges(Seq("id"))
    val rowsExpected = Seq(
      // img = {A,B}
      (0,"A",Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,"B",Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      // img = {B,C}
      (0,"B",Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-19 23:59:59.999")),
      (0,"C",Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-19 23:59:59.999")),
      // img = {B,C,D}
      (0,"B",Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      (0,"C",Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      (0,"D",Timestamp.valueOf("2018-02-20 00:00:00"),Timestamp.valueOf("2018-02-25 14:15:16.122")),
      // img = {B,C,D,X}
      (0,"B",Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,"C",Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,"D",Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      (0,"X",Timestamp.valueOf("2018-02-25 14:15:16.123"),Timestamp.valueOf("2018-02-25 14:15:16.123")),
      // img = {B,C,D}
      (0,"B",Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,"C",Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,"D",Timestamp.valueOf("2018-02-25 14:15:16.124"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      // img = {D}
      (0,"D",Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalUnifyRanges3",dfMap)(actual)(expected)
    assert(resultat)
  }

}
