package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import org.apache.spark.sql.functions.col
import TemporalQueryUtil._
import TestUtils._
import org.scalatest.FunSuite

class TemporalQueryUtilTest extends FunSuite {
  import ss.implicits._

  test("plusMillisecond") {
    val argument = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual = plusMillisecond(argument)
    val expected = Timestamp.valueOf("2019-09-01 14:00:00.001")
    assert(actual==expected)
  }

  test("minusMillisecond") {
    val argument = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual = minusMillisecond(argument)
    val expected = Timestamp.valueOf("2019-09-01 13:59:59.999")
    assert(actual==expected)
  }

  test("temporalExtendRange1") {
    // argument: dfLeft from object TestUtils
    val actual = dfLeft.temporalExtendRange(Seq("id"))
    val rowsExpected = Seq((0,4.2,defaultConfig.minDate,defaultConfig.maxDate))
    val expected = rowsExpected.toDF("id", "Wert_L", defaultConfig.fromColName, defaultConfig.toColName )
      .orderBy("Id",defaultConfig.fromColName)
    val expectedWithArgumentColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat) printFailedTestResult("temporalExtendRange1",dfLeft)(actual)(expected)
    assert(resultat)
  }

  test("temporalExtendRange2") {
    // argument: dfRight from object TestUtils
    val actual = dfRight.temporalExtendRange(Seq("id"))
    val rowsExpected = Seq(
      (0,Some(97.15),defaultConfig.minDate                     ,Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,Some(97.15),Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,Some(97.15),Timestamp.valueOf("2018-10-23 03:50:10.0"),defaultConfig.maxDate),
      (1,None       ,defaultConfig.minDate                     ,Timestamp.valueOf("2018-12-31 23:59:59.999")),
      (1,None       ,Timestamp.valueOf("2018-10-23 00:00:00.0"),defaultConfig.maxDate))
    val expected = rowsExpected.toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
      .orderBy("id",defaultConfig.fromColName)
    val expectedWithArgumentColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat) printFailedTestResult("temporalExtendRange2",dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin1") {
    val actual = dfLeft.temporalLeftJoin(dfRight,Seq("id"),Seq(col(defaultConfig.fromColName)))
    val rowsExpected = Seq(
      (0,4.2,None       ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,None       ,Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-06-01 05:24:10.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-06-01 05:24:11"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      (0,4.2,Some(97.15),Timestamp.valueOf("2018-10-23 03:50:10"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_l", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
      .orderBy("Id",defaultConfig.fromColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin1",Seq(dfLeft,dfRight))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMap") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq())
    val rowsExpected = Seq(
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
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMap",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin_rightMapWithrnkExpressions") {
    // Testing temporalLeftJoin where the right dataFrame is not unique for join attributes
    val actual = dfLeft.temporalLeftJoin(df2=dfMap, keys=Seq("id"), rnkExpressions=Seq(col(defaultConfig.fromColName)))
    val rowsExpected = Seq(
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
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin_rightMapWithrnkExpressions",Seq(dfLeft,dfMap))(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine") {
    // argument: dfRight from object TestUtils
    val actual = dfRight.temporalCombine(Seq("id"))
    val rowsExpected = Seq(
      (0,Some(97.15),Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,Some(97.15),Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("9999-12-31 23:59:59.999")),
      (1,None       ,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-12-31 23:59:59.999")),
      (1,None       ,Timestamp.valueOf("2018-10-23 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_r", defaultConfig.fromColName, defaultConfig.toColName)
    val expectedWithArgumentColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat) printFailedTestResult("temporalCombine",dfRight)(actual)(expected)
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
    //val expectedWithArgumentColumns = expected.select(actual.columns.map(col):_*)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalUnifyRanges3",dfMap)(actual)(expected)
    assert(resultat)
  }

}
