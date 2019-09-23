package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import org.apache.spark.sql.{Row, _}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._
import TemporalQueryUtil._
import TestUtils._

class TemporalQueryUtilTest extends org.scalatest.FunSuite {
  test("plusMillisecond") {
    val argument: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual: Timestamp = plusMillisecond(argument)(defaultConfig)
    val expected: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00.001")
    assert(actual==expected)
  }

  test("minusMillisecond") {
    val argument: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual: Timestamp = minusMillisecond(argument)(defaultConfig)
    val expected: Timestamp = Timestamp.valueOf("2019-09-01 13:59:59.999")
    assert(actual==expected)
  }

  test("temporalExtendRange1") {
    // argument: dfLeft from object TestUtils
    val actual: DataFrame = dfLeft.temporalExtendRange(Seq("id"))(ss,defaultConfig)

    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_L", DoubleType),
      StructField(defaultConfig.fromColName, TimestampType),
      StructField(defaultConfig.toColName, TimestampType)))
    val rowsExpected = List(Row(0,4.2,defaultConfig.minDate,defaultConfig.maxDate)).asJava
    val expected: DataFrame = ss.createDataFrame(rowsExpected, schemaExpected).orderBy("Id",defaultConfig.fromColName)
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange1")(dfLeft)(actual)(expected)
    assert(resultat)
  }

  test("temporalExtendRange2") {
    // argument: dfRight from object TestUtils
    val actual: DataFrame = dfRight.temporalExtendRange(Seq("id"))(ss,defaultConfig)

    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_R", DoubleType),
      StructField(defaultConfig.fromColName, TimestampType),
      StructField(defaultConfig.toColName, TimestampType)))
    val rowsExpected = List(
      Row(0,97.15,defaultConfig.minDate                     ,Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,97.15,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      Row(0,97.15,Timestamp.valueOf("2018-10-23 03:50:10.0"),defaultConfig.maxDate),
      Row(1, null,defaultConfig.minDate                     ,Timestamp.valueOf("2018-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-10-23 00:00:00.0"),defaultConfig.maxDate))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rowsExpected, schemaExpected).orderBy("Id",defaultConfig.fromColName)
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange2")(dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin1") {
    val actual: DataFrame = dfLeft.temporalLeftJoin(dfRight,Seq("id"),Seq(col(defaultConfig.fromColName)))(ss,defaultConfig)
    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_L", DoubleType),
      StructField("Wert_R", DoubleType),
      StructField(defaultConfig.fromColName, TimestampType),
      StructField(defaultConfig.toColName, TimestampType)))
    val rowsExpected = List(
      Row(0,4.2, null,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      Row(0,4.2,97.15,Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,4.2, null,Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-06-01 05:24:10.999")),
      Row(0,4.2,97.15,Timestamp.valueOf("2018-06-01 05:24:11"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      Row(0,4.2,97.15,Timestamp.valueOf("2018-10-23 03:50:10"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rowsExpected, schemaExpected).orderBy("Id",defaultConfig.fromColName)
    val resultat: Boolean = dfEqual(actual)(expected)

    if (!resultat)printFailedTestResult("temporalLeftJoin1")(dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalLeftJoin2") {
    val actual: DataFrame = dfLeft.temporalLeftJoin(dfMap,Seq("id"),Seq(col(defaultConfig.fromColName)))(ss,defaultConfig)
    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_L", DoubleType),
      StructField("img", StringType),
      StructField(defaultConfig.fromColName, TimestampType),
      StructField(defaultConfig.toColName, TimestampType)))
    val rowsExpected = List(
      Row(0,4.2,null,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2017-12-31 23:59:59.999")),
      Row(0,4.2, "A",Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,4.2, "B",Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      Row(0,4.2, "C",Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999")),
      Row(0,4.2, "D",Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999")),
      Row(0,4.2,null,Timestamp.valueOf("2018-04-01 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999")))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rowsExpected, schemaExpected).orderBy("Id",defaultConfig.fromColName)
    val resultat: Boolean = dfEqual(actual)(expected)

    if (!resultat)printFailedTestResult("temporalLeftJoin2")(dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine") {
    // argument: dfRight from object TestUtils
    val actual: DataFrame = dfRight.temporalCombine(Seq("id"))(ss,defaultConfig)

    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_R", DoubleType),
      StructField(defaultConfig.fromColName, TimestampType),
      StructField(defaultConfig.toColName, TimestampType)))
    val rowsExpected = List(
      Row(0,97.15,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,97.15,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("9999-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-10-23 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999")))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rowsExpected, schemaExpected).orderBy("Id",defaultConfig.fromColName)
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange1")(dfRight)(actual)(expected)
    assert(resultat)
  }

}
