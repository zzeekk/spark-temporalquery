package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType, TimestampType}
import scala.collection.JavaConverters._

object TestUtils {
  implicit val ss: SparkSession = SparkSession.builder.master("local").appName("TemporalQueryUtilTest").getOrCreate()

  val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()

  def makeRow(zeile: Tuple4[Int, String, String, Double]): Row = Row(zeile._1,Timestamp.valueOf(zeile._2),Timestamp.valueOf(zeile._3),zeile._4)

  val schemaLeft = StructType(Array(StructField("id", IntegerType),
    StructField("gueltig_ab", TimestampType),
    StructField("gueltig_bis", TimestampType),
    StructField("Wert_A", DoubleType)))
  val rowsLeft = List((0,"2017-12-10 00:00:00.0","2018-12-08 23:59:59.999",4.2))
  val dfLeft: DataFrame = ss.createDataFrame(rows=rowsLeft.map(makeRow).asJava, schemaLeft).orderBy("Id","gueltig_ab")

  val schemaRight = StructType(Array(StructField("id", IntegerType),
    StructField("gueltig_ab", TimestampType),
    StructField("gueltig_bis", TimestampType),
    StructField("Wert_B", DoubleType)))
  val rowList = List(
    (0,"2018-01-01 00:00:00.0","2018-01-31 23:59:59.999",97.15),
    // gap in history
    (0,"2018-06-01 05:24:11.0","2018-10-23 03:50:09.999",97.15),
    (0,"2018-10-23 03:50:10.0","9999-12-31 23:59:59.999",97.15))
  val rowsRight: List[Row] = rowList.map(makeRow) :+ Row(1,Timestamp.valueOf("2018-01-01 00:00:00.0"),Timestamp.valueOf("2018-12-31 23:59:59.999"),null) :+ Row(1,Timestamp.valueOf("2018-10-23 00:00:00.0"),Timestamp.valueOf("2019-12-31 23:59:59.999"),null)

  val dfRight: DataFrame = ss.createDataFrame(rows=rowsRight.asJava, schemaRight)

  def symmetricDifference(df1: DataFrame)(df2: DataFrame): DataFrame = {
    df1.except(df2).withColumn("_df",lit(1)).union(df2.except(df1).withColumn("_df",lit(2)))
  }

  def dfEqual(df1: DataFrame)(df2: DataFrame): Boolean = {
    // symmetricDifference ignoriert Doubletten, daher Kardinalitäten vergleichen
    (0 == symmetricDifference(df1)(df2).count) && (df1.count == df2.count)
  }

  def printFailedTestResult(testName: String)(argument: DataFrame)(actual: DataFrame)(expected: DataFrame): Unit = {
    println(s"!!!! Test $testName Failed !!!")
    println("   argument "+argument.schema.simpleString)
    argument.show(false)
    println("   actual "+actual.schema.simpleString)
    actual.show(false)
    println("   expected "+expected.schema.simpleString)
    expected.show(false)
    println("   symmetricDifference ")
    symmetricDifference(actual)(expected).show(false)
  }

}
