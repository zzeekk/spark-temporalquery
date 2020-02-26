package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit,when}

object TestUtils {
  implicit val ss: SparkSession = SparkSession.builder.master("local").appName("TemporalQueryUtilTest").getOrCreate()
  ss.conf.set("spark.sql.shuffle.partitions", 1)

  import ss.implicits._

  implicit val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()

  //def javaDouble(d: Double)

  val rowsLeft: Seq[(Int, Timestamp, Timestamp, Double)] = Seq((0, Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999"), 4.2))
  val dfLeft: DataFrame = rowsLeft.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_l")
    .orderBy("id", defaultConfig.fromColName)


  val rowsRight: Seq[(Int, Timestamp, Timestamp, Option[Double])] = Seq(
    (0, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"), Some(97.15)),
    // gap in history
    (0, Timestamp.valueOf("2018-06-01 05:24:11"), Timestamp.valueOf("2018-10-23 03:50:09.999"), Some(97.15)),
    (0, Timestamp.valueOf("2018-10-23 03:50:10"), Timestamp.valueOf("2019-12-31 23:59:59.999"), Some(97.15)),
    (0, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 23:59:59.999"), Some(97.15)),
    (1, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-12-31 23:59:59.999"), None),
    (1, Timestamp.valueOf("2019-01-01 00:00:00"), Timestamp.valueOf("2019-12-31 23:59:59.999"), Some(2019)),
    (1, Timestamp.valueOf("2020-01-01 00:00:00"), Timestamp.valueOf("2020-12-31 23:59:59.999"), Some(2020)),
    (1, Timestamp.valueOf("2021-01-01 00:00:00"), Timestamp.valueOf("2099-12-31 23:59:59.999"), None))
  val dfRight: DataFrame = rowsRight.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "wert_r")
  /*
    * dfMap: dfMap which maps a set of images img to id over time:
    * e.g. 0 ↦ {A,B} in Jan 2018 ; 0 ↦ {B,C} 1.-19. Feb 2018 ; 0 ↦ {B,C,D} 20.-28. Feb 2018 ausser für eine 1ms mit 0 ↦ {B,C,D,X} ; 0 ↦ {D} in Mar 2018
  */
  val rowsMap: Seq[(Int, Timestamp, Timestamp, String)] = Seq(
    (0, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-01-31 23:59:59.999"), "A"),
    (0, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-02-28 23:59:59.999"), "B"),
    (0, Timestamp.valueOf("2018-02-01 00:00:00"), Timestamp.valueOf("2018-02-28 23:59:59.999"), "C"),
    (0, Timestamp.valueOf("2018-02-20 00:00:00"), Timestamp.valueOf("2018-03-31 23:59:59.999"), "D"),
    (0, Timestamp.valueOf("2018-02-25 14:15:16.123"), Timestamp.valueOf("2018-02-25 14:15:16.123"), "X"))
  val dfMap: DataFrame = rowsMap.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")

  val rowsMoment: Seq[(Int, Timestamp, Timestamp, String)] = Seq((0, Timestamp.valueOf("2019-11-25 11:12:13.005"), Timestamp.valueOf("2019-11-25 11:12:13.005"), "A"))
  val dfMoment: DataFrame = rowsMoment.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")

  // for 1ms, namely [10:00:00.000,10:00:00.001[, the image set is {A,B}
  val rowsMsOverlap: Seq[(Int, Timestamp, Timestamp, String)] = Seq((0, Timestamp.valueOf("2019-01-01 00:00:00"), Timestamp.valueOf("2019-01-01 10:00:00"), "A"),
    (0, Timestamp.valueOf("2019-01-01 10:00:00"), Timestamp.valueOf("2019-01-01 23:59:59.999"), "B"))
  val dfMsOverlap: DataFrame = rowsMsOverlap.toDF("id", defaultConfig.fromColName, defaultConfig.toColName, "img")

  def symmetricDifference(df1: DataFrame)(df2: DataFrame): DataFrame = {
    // attention, "except" works on Dataset and not on DataFrame. We need to check that schema is equal.
    require(df1.columns.toSeq==df2.columns.toSeq, "Cannot calculate symmetric difference for DataFrames with different schema")
    df1.except(df2).withColumn("_df", lit(1))
      .union(df2.except(df1).withColumn("_df", lit(2)))
  }

  def dfEqual(df1: DataFrame)(df2: DataFrame): Boolean = {
    // symmetricDifference ignoriert Doubletten, daher Kardinalitäten vergleichen
    (0 == symmetricDifference(df1)(df2).count) && (df1.count == df2.count)
  }

  def printFailedTestResult(testName: String, arguments: Seq[DataFrame])(actual: DataFrame)(expected: DataFrame): Unit = {
    def printDf(df: DataFrame): Unit = {
      println(df.schema.simpleString)
      df.orderBy(df.columns.head,df.columns.tail:_*).show(false)
    }

    println(s"!!!! Test $testName Failed !!!")
    println("   Arguments ")
    arguments.foreach(printDf)
    println("   Actual ")
    printDf(actual)
    println("   Expected ")
    printDf(expected)
    println("   symmetric Difference ")
    symmetricDifference(actual)(expected)
      .withColumn("_df", when($"_df" === 1,"actual").otherwise("expected"))
      .show(false)
  }

  def printFailedTestResult(testName: String, argument: DataFrame)(actual: DataFrame)(expected: DataFrame): Unit = printFailedTestResult(testName, Seq(argument))(actual)(expected)

}
