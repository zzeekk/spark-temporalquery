package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}

object TestUtils extends Logging {
  implicit val session: SparkSession = SparkSession.builder.master("local").appName("TemporalQueryUtilTest").getOrCreate()
  import session.implicits._
  session.conf.set("spark.sql.shuffle.partitions", 1)

  implicit val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()
  val initiumTemporisString: String = defaultConfig.minDate.toString
  val finisTemporisString: String = defaultConfig.maxDate.toString

  def symmetricDifference(df1: DataFrame, df2: DataFrame): DataFrame = {
    // attention, "except" works on Dataset and not on DataFrame. We need to check that schema is equal.
    require(df1.columns.toSeq==df2.columns.toSeq,
      s"""Cannot calculate symmetric difference for DataFrames with different schema.
         |schema of df1: ${df1.columns.toSeq.mkString(",")}
         |${df1.schema.treeString}
         |schema of df2: ${df2.columns.toSeq.mkString(",")}
         |${df2.schema.treeString}
         |""".stripMargin)
    df1.except(df2).withColumn("_in_first_df",lit(true))
      .union(df2.except(df1).withColumn("_row_in_first_df",lit(false)))
  }

  def reorderCols(dfToReorder: DataFrame, dfRef: DataFrame): DataFrame = {
    require(dfRef.columns.toSet == dfToReorder.columns.toSet,
      s"""Cannot reorder columns for DataFrames with different columns.
       |columns of dfRef: ${dfRef.columns.toSeq.mkString(",")}
       |columns of dfToReorder: ${dfToReorder.columns.toSeq.mkString(",")}
       |""".stripMargin)
    if (dfRef.columns.toSet.size < dfRef.columns.length) dfToReorder // cannot reorder DataFrames with schemas that have duplicate column names
    else dfToReorder.select(dfRef.columns.map(col):_*)
  }

  def dfEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    val df1reordered = reorderCols(df1, df2)
    // symmetricDifference ignoriert Doubletten, daher Kardinalitäten vergleichen
    (0 == symmetricDifference(df1reordered, df2).count) && (df1reordered.count == df2.count) && (df1reordered.schema == df2.schema)
  }

  def printFailedTestResult(testName: String, arguments: Seq[DataFrame])(actual: DataFrame,expected: DataFrame): Unit = {
    def printDf(df: DataFrame): Unit = {
      println(df.schema.simpleString)
      df.show(false)
    }
    val actualReordered = reorderCols(actual, expected)

    println(s"!!!! Test $testName Failed !!!")
    println("   Arguments ")
    arguments.foreach(printDf)
    println("   Actual ")
    println(s"  actual.count() =  ${actualReordered.count()}")
    printDf(actualReordered)
    println("   Expected ")
    println(s"  expected.count() =  ${expected.count()}")
    printDf(expected)
    println(s"  schemata equal =  ${actualReordered.schema == expected.schema}")
    if (actualReordered.schema != expected.schema) {
      println(s"actual.schema:${actualReordered.schema.treeString}")
      println(s"expected.schema:${expected.schema.treeString}")
    }
    println("   symmetric Difference ")
    printDf(symmetricDifference(actualReordered,expected)
      .withColumn("_df", when($"_in_first_df","actual").otherwise("expected"))
      .drop($"_in_first_df"))
  }

  def printFailedTestResult(testName: String, argument: DataFrame)(actual: DataFrame, expected: DataFrame): Unit =
    printFailedTestResult(testName, Seq(argument))(actual,expected)

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
  def makeRowsWithTimeRangeEnd[A,B](zeile: (A, B, String, String)): (A, B, Timestamp, Timestamp) = (zeile._1,zeile._2,Timestamp.valueOf(zeile._3),Timestamp.valueOf(zeile._4))
  def makeRowsWithTimeRangeEnd[A,B,C](zeile: (A, B, C, String, String)): (A, B, C, Timestamp, Timestamp) = (zeile._1,zeile._2,zeile._3,Timestamp.valueOf(zeile._4),Timestamp.valueOf(zeile._5))
  def makeRowsWithTimeRangeEnd[A,B,C,D](zeile: (A, B, C, D, String, String)): (A, B, C, D, Timestamp, Timestamp) = (zeile._1,zeile._2,zeile._3,zeile._4,Timestamp.valueOf(zeile._5),Timestamp.valueOf(zeile._6))

  val dfLeft: DataFrame = Seq((0, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", 4.2))
    .map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")

  val dfRight: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
    (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")

  val dfRightDouble: DataFrame = Seq(
    (0.0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0.0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0.0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0.0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
    (1.0, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1.0, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1.0, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1.0, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")

  /*
    * dfMap: dfMap which maps a set of images img to id over time:
    * e.g. 0 ↦ {A,B} in Jan 2018 ; 0 ↦ {B,C} 1.-19. Feb 2018 ; 0 ↦ {B,C,D} 20.-28. Feb 2018 ausser für eine 1ms mit 0 ↦ {B,C,D,X} ; 0 ↦ {D} in Mar 2018
  */
  val dfMap: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
    (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", "D"),
    (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMapToCombine: DataFrame = Seq(
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
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMoment: DataFrame = Seq((0, "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", "A"))
    .map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // for 1ms, namely [10:00:00.000,10:00:00.001[, the image set is {A,B}
  val dfMsOverlap: DataFrame = Seq(
    (0, "2019-01-01 00:00:00", "2019-01-01 10:00:00", "A"),
    (0, "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B")
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // Data Frame dfDirtyTimeRanges
  val dfMicrosecTimeRanges: DataFrame = Seq(
    (0,"2018-06-01 00:00:00"       ,"2018-06-01 09:00:00.000123", 3.14),
    (0,"2018-06-01 09:00:00.000124","2018-06-01 09:00:00.000129",42.0),
    (0,"2018-06-01 09:00:00.000130","2018-06-01 17:00:00.123456", 2.72)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  // Data Frame dfDirtyTimeRanges
  val dfDirtyTimeRanges: DataFrame = Seq(
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
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 )
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val dfDocumentation: DataFrame = Seq(
    (1,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235", 2.72),
    (1,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245", 2.72), // overlaps with previous record
    (1,"2019-02-01 02:34:56.125"      ,"2019-02-01 02:34:56.1245", 2.72), // ends before it starts
    (1,"2019-01-01 00:00:0"           ,"2019-12-31 23:59:59.999" ,42.0 )
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val dfContinuousTime: DataFrame = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 02:34:56.1235"     ,"2019-02-01 02:34:56.1245"     ,42.0 ),
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,12.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ),
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:00"          ,-1.0 ),
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 )
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

}
