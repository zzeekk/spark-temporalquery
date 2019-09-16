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
      StructField("Wert_A", DoubleType),
      StructField("gueltig_ab", TimestampType),
      StructField("gueltig_bis", TimestampType)))
    val rowsExpected = List(Row(0,4.2,defaultConfig.minDate,defaultConfig.maxDate)).asJava
    val expected: DataFrame = ss.createDataFrame(rows=rowsExpected, schemaExpected).orderBy("Id","gueltig_ab")
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange1")(dfLeft)(actual)(expected)
    assert(resultat)
  }

  test("temporalExtendRange2") {
    // argument: dfRight from object TestUtils
    val actual: DataFrame = dfRight.temporalExtendRange(Seq("id"))(ss,defaultConfig)

    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_B", DoubleType),
      StructField("gueltig_ab", TimestampType),
      StructField("gueltig_bis", TimestampType)))
    val rowsExpected = List(
      Row(0,97.15,defaultConfig.minDate                     ,Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,97.15,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("2018-10-23 03:50:09.999")),
      Row(0,97.15,Timestamp.valueOf("2018-10-23 03:50:10.0"),defaultConfig.maxDate),
      Row(1, null,defaultConfig.minDate                     ,Timestamp.valueOf("2018-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-10-23 00:00:00.0"),defaultConfig.maxDate))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rows=rowsExpected, schemaExpected).orderBy("Id","gueltig_ab")
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange2")(dfRight)(actual)(expected)
    assert(resultat)
  }

  test("temporalCombine") {
    // argument: dfRight from object TestUtils
    val actual: DataFrame = dfRight.temporalCombine(Seq("id"))(ss,defaultConfig)

    val schemaExpected = StructType(Array(StructField("id", IntegerType),
      StructField("Wert_B", DoubleType),
      StructField("gueltig_ab", TimestampType),
      StructField("gueltig_bis", TimestampType)))
    val rowsExpected = List(
      Row(0,97.15,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-01-31 23:59:59.999")),
      Row(0,97.15,Timestamp.valueOf("2018-06-01 05:24:11.0"),Timestamp.valueOf("9999-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-12-31 23:59:59.999")),
      Row(1, null,Timestamp.valueOf("2018-10-23 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999")))
      .asJava
    val expected: DataFrame = ss.createDataFrame(rows=rowsExpected, schemaExpected).orderBy("Id","gueltig_ab")
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange1")(dfRight)(actual)(expected)
    assert(resultat)
  }

}
