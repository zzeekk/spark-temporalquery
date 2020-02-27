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

  // some beautiful nasty data frames for testing

  def makeRowsWithTimeRange[A,B](zeile: (A, String, String, B)): (A, Timestamp, Timestamp, B) = (zeile._1,Timestamp.valueOf(zeile._2),Timestamp.valueOf(zeile._3),zeile._4)
/*  def makeDfFromTimeRangeRows(idColName: String, valueColName: String)(zeilen: Seq[(Int, String, String, B)]): DataFrame = zeilen.map(makeRowsWithTimeRange[Int, B]).toDF(idColName, defaultConfig.fromColName, defaultConfig.toColName,valueColName)
  def makeDfFromTimeRangeRows(idColName: String, valueColName: String)(zeilen: Seq[(Int, String, String, Double)]): DataFrame = zeilen.map(makeRowsWithTimeRange[Int, Double]).toDF(idColName, defaultConfig.fromColName, defaultConfig.toColName,valueColName)
  def makeDfFromTimeRangeRowsString(idColName: String, valueColName: String)(zeilen: Seq[(Int, String, String, String)]): DataFrame = zeilen.map(makeRowsWithTimeRange[Int, String]).toDF(idColName, defaultConfig.fromColName, defaultConfig.toColName,valueColName)
  def makeDfFromTimeRangeRowsNullable(idColName: String, valueColName: String)(zeilen: Seq[(Int, String, String, Option[Double])]): DataFrame = zeilen.map(makeRowsWithTimeRange[Int, Option[Double]]).toDF(idColName, defaultConfig.fromColName, defaultConfig.toColName,valueColName)
*/
  val rowsLeft: Seq[(Int, String, String, Double)] = Seq((0, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", 4.2))
  val dfLeft: DataFrame = rowsLeft.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")

  val rowsRight: Seq[(Int, String, String, Option[Double])] = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0, "2020-01-01 00:00:00", "9999-12-31 23:59:59.999", Some(97.15)),
    (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019)),
    (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020)),
    (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None))
  val dfRight: DataFrame = rowsRight.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")
  /*
    * dfMap: dfMap which maps a set of images img to id over time:
    * e.g. 0 ↦ {A,B} in Jan 2018 ; 0 ↦ {B,C} 1.-19. Feb 2018 ; 0 ↦ {B,C,D} 20.-28. Feb 2018 ausser für eine 1ms mit 0 ↦ {B,C,D,X} ; 0 ↦ {D} in Mar 2018
  */
  val rowsMap: Seq[(Int, String, String, String)] = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
    (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", "D"),
    (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
  )
  val dfMap: DataFrame = rowsMap.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val rowsMapToCombine = Seq(
    (0, "2018-01-01 00:00:00", "2018-03-31 23:59:59.999", Some("A")),
    (0, "2018-04-01 00:00:00", "2018-07-31 23:59:59.999", Some("A")),
    (0, "2018-08-01 00:00:00", "2018-08-31 23:59:59.999", Some("A")),
    (0, "2018-09-01 00:00:00", "2018-12-31 23:59:59.999", Some("A")),
    (0, "2018-01-01 00:00:00", "2018-01-15 23:59:59.999", Some("B")),
    (0, "2018-01-16 00:00:00", "2018-02-03 23:59:59.999", Some("B")),
    (0, "2018-02-01 00:00:00", "2020-02-29 23:59:59.999", None),
    (0, "2020-03-01 00:00:00", "2020-04-30 23:59:59.999", None),
    (0, "2020-06-01 00:00:00", "2020-12-31 23:59:59.999", None),
    (1, "2018-02-01 00:00:00", "2020-02-29 23:59:59.999", Some("one")),
    (1, "2020-03-01 00:00:00", "2020-04-30 23:59:59.999", Some("one")),
    (1, "2020-06-01 00:00:00", "2020-12-31 23:59:59.999", Some("one")),
    (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
    (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", Some("X"))
  )
  val dfMapToCombine: DataFrame = rowsMapToCombine.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val rowsMoment: Seq[(Int, String, String, String)] = Seq((0, "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", "A"))
  val dfMoment: DataFrame = rowsMoment.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // for 1ms, namely [10:00:00.000,10:00:00.001[, the image set is {A,B}
  val rowsMsOverlap: Seq[(Int, String, String, String)] = Seq((0, "2019-01-01 00:00:00", "2019-01-01 10:00:00", "A"),
    (0, "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B"))
  val dfMsOverlap: DataFrame = rowsMsOverlap.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // Data Frame df_TimeRanges
  val zeilen_TimeRanges: Seq[(Int, String, String, Double)] = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245"     , 2.72),
    (0,"2019-02-01 02:34:56.1236"     ,"2019-02-01 02:34:56.1245"     ,42.0 ),
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,12.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ),
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:00"          ,-1.0 ),
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 ))
  val df_TimeRanges: DataFrame = zeilen_TimeRanges.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

}
