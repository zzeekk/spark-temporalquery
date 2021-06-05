package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, lit, when}
import org.apache.spark.sql.types.{DataType, DataTypes, StructType}

trait TestUtils extends Logging {

  implicit val session: SparkSession = SparkSession.builder
    .config("spark.port.maxRetries", 100)
    .config("spark.ui.enabled", false)
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.task.maxFailures", 1)
    .master("local").appName("TemporalQueryUtilTest").getOrCreate()
  import session.implicits._

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

  def schemaEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    df1.schema.toDDL == df2.schema.toDDL // ignore nullability in comparision
  }

  def dfEqual(df1: DataFrame, df2: DataFrame): Boolean = {
    val df1reordered = reorderCols(df1, df2)
    // symmetricDifference ignoriert Doubletten, daher Kardinalitäten vergleichen
    (0 == symmetricDifference(df1reordered, df2).count) && (df1reordered.count == df2.count) && schemaEqual(df1reordered, df2)
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
    println(s"  schemata equal =  ${schemaEqual(actualReordered, expected)}")
    if (schemaEqual(actualReordered, expected)) {
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

  def makeRowsWithTimeRange[A,B](zeile: (A, String, String, B)): (A, Timestamp, Timestamp, B) = (zeile._1,Timestamp.valueOf(zeile._2),Timestamp.valueOf(zeile._3),zeile._4)
  def makeRowsWithTimeRangeEnd[A,B](zeile: (A, B, String, String)): (A, B, Timestamp, Timestamp) = (zeile._1,zeile._2,Timestamp.valueOf(zeile._3),Timestamp.valueOf(zeile._4))
  def makeRowsWithTimeRangeEnd[A,B,C](zeile: (A, B, C, String, String)): (A, B, C, Timestamp, Timestamp) = (zeile._1,zeile._2,zeile._3,Timestamp.valueOf(zeile._4),Timestamp.valueOf(zeile._5))
  def makeRowsWithTimeRangeEnd[A,B,C,D](zeile: (A, B, C, D, String, String)): (A, B, C, D, Timestamp, Timestamp) = (zeile._1,zeile._2,zeile._3,zeile._4,Timestamp.valueOf(zeile._5),Timestamp.valueOf(zeile._6))

}
