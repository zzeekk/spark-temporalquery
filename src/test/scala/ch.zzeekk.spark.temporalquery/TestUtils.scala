package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions.lit

object TestUtils {
  implicit val ss: SparkSession = SparkSession.builder.master("local").appName("TemporalQueryUtilTest").getOrCreate()
  ss.conf.set("spark.sql.shuffle.partitions", 10)
  import ss.implicits._

  implicit val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()

  //def javaDouble(d: Double)

  val rowsLeft = Seq((0,Timestamp.valueOf("2017-12-10 00:00:00"),Timestamp.valueOf("2018-12-08 23:59:59.999"),4.2))
  val dfLeft = rowsLeft.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_l")
    .orderBy("id",defaultConfig.fromColName)

  val rowsRight = Seq(
    (0,Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999"),Some(97.15)),
    // gap in history
    (0,Timestamp.valueOf("2018-06-01 05:24:11"),Timestamp.valueOf("2018-10-23 03:50:09.999"),Some(97.15)),
    (0,Timestamp.valueOf("2018-10-23 03:50:10"),Timestamp.valueOf("9999-12-31 23:59:59.999"),Some(97.15)),
    (1,Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-12-31 23:59:59.999"),None),
    (1,Timestamp.valueOf("2018-10-23 00:00:00"),Timestamp.valueOf("2019-12-31 23:59:59.999"),None))
  val dfRight = rowsRight.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_r")

  val rowsMap = Seq(
    (1,Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-01-31 23:59:59.999"),"A"),
    (1,Timestamp.valueOf("2018-01-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999"),"B"),
    (1,Timestamp.valueOf("2018-02-01 00:00:00"),Timestamp.valueOf("2018-02-28 23:59:59.999"),"C"),
    (1,Timestamp.valueOf("2018-03-01 00:00:00"),Timestamp.valueOf("2018-03-31 23:59:59.999"),"D"))
  val dfMap: DataFrame = rowsMap.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")

  def symmetricDifference(df1: DataFrame)(df2: DataFrame): DataFrame = {
    df1.except(df2).withColumn("_df",lit(1))
      .union(df2.except(df1).withColumn("_df",lit(2)))
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
