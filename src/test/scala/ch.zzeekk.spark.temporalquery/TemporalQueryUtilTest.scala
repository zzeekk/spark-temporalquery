package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import org.apache.spark.sql._
import TemporalQueryUtil._
import TestUtils._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types._
import scala.collection.JavaConverters._

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
    val rowsExpected = List(Row(0,97.15,defaultConfig.minDate,Timestamp.valueOf("2018-10-23 03:50:09.999")),
      Row(0,97.15,Timestamp.valueOf("2018-10-23 03:50:10.000"),defaultConfig.maxDate)).asJava
    val expected: DataFrame = ss.createDataFrame(rows=rowsExpected, schemaExpected).orderBy("Id","gueltig_ab")
    val expectedWithArgumentColumns: DataFrame = expected.select(actual.columns.map(col):_*)
    val resultat: Boolean = dfEqual(actual)(expectedWithArgumentColumns)

    if (!resultat)printFailedTestResult("temporalExtendRange1")(dfRight)(actual)(expected)
    assert(resultat)
  }

}
