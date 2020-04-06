package ch.zzeekk.spark.temporalquery

import com.typesafe.scalalogging.LazyLogging
import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{lit, when}

import TemporalQueryUtil.TemporalQueryConfig

object TestUtils extends LazyLogging {
  implicit val session: SparkSession = SparkSession.builder.master("local").appName("TemporalQueryUtilTest").getOrCreate()
  import session.implicits._
  session.conf.set("spark.sql.shuffle.partitions", 1)

  implicit val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()
  val finisTemporisString: String = defaultConfig.maxDate.toString

  def symmetricDifference(df1: DataFrame)(df2: DataFrame): DataFrame = {
    // attention, "except" works on Dataset and not on DataFrame. We need to check that schema is equal.
   require(df1.columns.toSeq==df2.columns.toSeq,
      s"""Cannot calculate symmetric difference for DataFrames with different schema.
         |schema of df1: ${df1.columns.toSeq.mkString(" ;")}
         |${df1.schema.treeString}
         |schema of df2: ${df2.columns.toSeq.mkString(" ;")}
         |${df2.schema.treeString}
         |         |""".stripMargin)
    df1.except(df2).withColumn("_in_first_df",lit(true))
      .union(df2.except(df1).withColumn("_row_in_first_df",lit(false)))
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
    println(s"  actual.count() =  ${actual.count()}")
    printDf(actual)
    println("   Expected ")
    println(s"  expected.count() =  ${expected.count()}")
    printDf(expected)
    println(s"  schemata equal =  ${actual.schema == expected.schema}")
    if (actual.schema != expected.schema) {
      println(s"actual.schema:${actual.schema.treeString}")
      println(s"expected.schema:${expected.schema.treeString}")
    }
    println("   symmetric Difference ")
    symmetricDifference(actual)(expected)
      .withColumn("_df", when($"_in_first_df","actual").otherwise("expected"))
      .drop($"_in_first_df")
      .show(false)
  }

  def printFailedTestResult(testName: String, argument: DataFrame)(actual: DataFrame)(expected: DataFrame): Unit = printFailedTestResult(testName, Seq(argument))(actual)(expected)

  def testArgumentExpectedMapWithComment[K,V](experiendum: K=>V, argExpMapComm: Map[(String,K),V]): Unit = {
    def logFailure(argument: K, actual:V, expected: V, comment: String): Unit = {
      logger.error("Test case failed !")
      logger.error(s"   argument = $argument")
      logger.error(s"   actual   = $actual")
      logger.error(s"   expected = $expected")
      logger.error(s"   comment  = $comment")
    }
    def checkKey(x: (String,K)): Boolean = x match {
      case (comment, argument) =>
        val actual = experiendum(argument)
        val expected = argExpMapComm(x)
        val resultat = actual == expected
        if (!resultat) logFailure(argument, actual, expected, comment)
        resultat
      case _ => throw new Exception(s"Something went wrong: checkKey called with parameter x=$x")
    }
    val results: Set[Boolean] = argExpMapComm.keySet.map(checkKey)
    assert(results.forall(p=>p))
  }

  def testArgumentExpectedMap[K,V](experiendum: K=>V, argExpMap: Map[K,V]): Unit = {
    def addEmptyComment(x : (K,V)): ((String,K),V) = x match {case (k,v) => (("",k),v)}
    val argExpMapWithReason: Map[(String,K),V] = argExpMap.map(addEmptyComment)
    testArgumentExpectedMapWithComment(experiendum, argExpMapWithReason)
  }

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
    (0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
    (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
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

  val rowsMapToCombine: Seq[(Int, String, String, Option[String])] = Seq(
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

  // Data Frame dfDirtyTimeRanges
  val rowsDirtyTimeRanges: Seq[(Int, String, String, Double)] = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245"     , 2.72), // overlaps with previous record
    (0,"2019-03-01 00:00:00.0"        ,"2019-02-01 00:00:00.0"        , 2.72),
    (0,"2019-02-01 02:34:56.1236"     ,"2019-02-01 02:34:56.1235"     ,42.0 ), // ends before it starts
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,13.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ), // duration less than a millisecond without touching bounds of millisecond intervall
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    // id = 1
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:0"           ,-1.0 ),
    (1,"2019-03-01 00:00:0"           ,"2019-03-01 00:00:00.0001"     , 0.1 ), // duration less than a millisecond
    (1,"2019-03-01 00:00:0.0009"      ,"2019-03-01 00:00:00.001"      , 0.1 ), // duration less than a millisecond, overlaps with previous record
    (1,"2019-03-01 00:00:1.0009"      ,"2019-03-01 00:00:01.0021"     , 1.2 ), // duration less than a millisecond
    (1,"2019-03-01 00:00:0.0001"      ,"2019-03-01 00:00:00.0009"     , 0.8 ), // duration less than a millisecond
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 ))
  val dfDirtyTimeRanges: DataFrame = rowsDirtyTimeRanges.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val rowsDocumentation: Seq[(Int, String, String, Double)] = Seq(
    (1,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235", 2.72),
    (1,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245", 2.72), // overlaps with previous record
    (1,"2019-02-01 02:34:56.125"      ,"2019-02-01 02:34:56.1245", 2.72), // ends before it starts
    (1,"2019-01-01 00:00:0"           ,"2019-12-31 23:59:59.999" ,42.0 ))
  val dfDocumentation: DataFrame = rowsDocumentation.map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val rowsContinuousTime: Seq[(Int, String, String, Double)] = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 02:34:56.1235"     ,"2019-02-01 02:34:56.1245"     ,42.0 ),
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,12.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ),
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:00"          ,-1.0 ),
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 ))
  val dfContinuousTime: DataFrame = rowsContinuousTime.map(makeRowsWithTimeRange[Int, Double]).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

}
