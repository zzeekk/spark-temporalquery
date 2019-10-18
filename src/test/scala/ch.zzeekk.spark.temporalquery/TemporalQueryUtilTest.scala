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

    if (!resultat) printFailedTestResult("temporalExtendRange1")(dfLeft)(actual)(expected)
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

    if (!resultat) printFailedTestResult("temporalExtendRange2")(dfRight)(actual)(expected)
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

    if (!resultat) printFailedTestResult("temporalLeftJoin1")(dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin2") {
    /* Testing temporalLeftJoin where the join attributes violate uniqueness in right frame
     * right frame: dfMap which maps a set of images img to id over time:
     * e.g. 1 ↦ {A,B} in Jan 2018 ; 1 ↦ {B,C} in Feb 2018 ; 1 ↦ {D} in Mar 2018 */
    val actual = dfLeft.temporalLeftJoin(dfMap, Seq("id"), Seq(col(defaultConfig.fromColName)))
    val rowsExpected = Seq(
      (0,4.2,None     ,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      (0,4.2,Some("A"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      (0,4.2,Some("B"),Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("C"),Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      (0,4.2,Some("D"),Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")),
      (0,4.2,None     ,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
    val expected = rowsExpected.toDF("id", "wert_l", "img", defaultConfig.fromColName, defaultConfig.toColName)
    val resultat = dfEqual(actual)(expected)

    if (!resultat) printFailedTestResult("temporalLeftJoin2")(dfRight)(actual)(expected)
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

    if (!resultat) printFailedTestResult("temporalExtendRange1")(dfRight)(actual)(expected)
    assert(resultat)
  }

}
