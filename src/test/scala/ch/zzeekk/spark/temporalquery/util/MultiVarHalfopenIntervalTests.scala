package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.Generators
import ch.zzeekk.spark.temporalquery.util.GenericDoubleQueryUtil._
import org.apache.spark.sql.DataFrame
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.apache.spark.sql.functions.{col, lit}

class MultiVarHalfopenIntervalTests extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
    with Generators {

  private def logDf(dfName: String, df: DataFrame): Unit = logger
    .info(s"$dfName.count() = ${df.count()} ; $dfName.schema = ${df.schema.catalogString}")

  "rangeCombine" should "combine everything possible of every generated dataFrame" in
    forAll(genA = generateHyperdimDataFrames()) { case (df, mrqc, splitPts) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logDf("df", df)
      val dfCombined = df.rangeCombine[Double]()
      val dfCombinedCount = dfCombined.count()
      val actual = if (dfCombinedCount == 1L) dfCombined
      else {
        logger.warn(s"dfCombined still has $dfCombinedCount rows!")
        dfCombined.createdLog("dfCombinedCount")
        logger.warn(s"splitPts: ${splitPts.mkString(" ; ")}")
        logger.warn(s"We need to unify the ranges and try again!")
        dfCombined.rangeUnifyRanges[Double]().rangeCombine[Double]()
      }
      val expected = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCombine generated hyperdim dataFrame", df)(actual, expected)
      println()
      result shouldBe true
    }

  "rangeCombine" should "combine to one row but cannot do so without unifying ranges first" in {
    import session.implicits._
    implicit val mrqc: GenericHalfOpenIntervalQueryConfig = GenericHalfOpenIntervalQueryConfig.withDefaultIntervalDef(numDim = 3)
    logger.info(s"mrqc = $mrqc")

    val argument = List(
      (0.0,  0.2,  0.0,  1.0,  0.4,  1.0),
      (0.0,  1.0,  0.0,  1.0,  0.0,  0.4),
      (0.2,  0.26, 0.1,  0.7,  0.54, 1.0),
      (0.2,  1.0,  0.0,  0.1,  0.54, 1.0),
      (0.2,  1.0,  0.0,  0.7,  0.4,  0.54),
      (0.2,  1.0,  0.7,  1.0,  0.4,  1.0),
      (0.26, 0.28, 0.1,  0.7,  0.54, 0.84),
      (0.26, 0.28, 0.23, 0.7,  0.84, 1.0),
      (0.26, 1.0,  0.1,  0.23, 0.84, 1.0),
      (0.28, 1.0,  0.1,  0.23, 0.54, 0.84),
      (0.28, 1.0,  0.23, 0.7,  0.54, 1.0)
    ).toDF("x000_from", "x000_to", "x001_from", "x001_to", "x002_from", "x002_to")
      .withColumn("value", lit(false))

    // Without rangeUnifyRanges, rangeCombine cannot do anything
    val actual1 = argument.rangeCombine[Double]()
    val result1 = dfEqual(actual1, argument)
    if (result1) {
      logger.warn("rangeCombine could not simplify the data frame :(")
      logger.warn(s"argument.count = ${argument.count()} ; actual1.count = ${actual1.count()}")
    } else printFailedTestResult("rangeCombine not unified", argument)(actual1, argument)
    result1 shouldBe true

    // With rangeUnifyRanges, rangeCombine can compact the data frame to 1 row
    val actual2 = argument.rangeUnifyRanges[Double]().rangeCombine[Double]()
    val expected = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
    val result2 = dfEqual(actual2, expected)
    if (result2) logger
      .info("rangeCombine could simplify the same data frame down to 1 row after unifying the argument :)")
    else printFailedTestResult("rangeCombine unified", argument)(actual2, expected)
    println()
    result2 shouldBe true
  }

  "rangeinnerJoin df1 with df2" should "return the same as rangeinnerJoin df2 with df1" in
    forAll(genA = unitDoubles) { xs =>
      val (df1, mrqc, splitPts1) = getHyperdimDataFrame((col("x000_from") + col("x000_to")).as("value_l"),
        maxNumSplitCoords = 10)(xs)
      logDf("df1", df1)
      val (df2, _, splitPts2) = getHyperdimDataFrame((col("x000_from") * col("x000_to")).as("value_r"),
        maxNumSplitCoords = 10)(xs.reverse)
      logDf("df2", df2)
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      val actual = df1.rangeInnerJoin[Double](df2, Nil)
      val expected = df2.rangeInnerJoin[Double](df1, Nil)
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) {
        logger.error(s"Test case failed. splitPts1: ${splitPts1.mkString(" ; ")} | splitPts2: ${splitPts2.mkString(" ; ")}")
        printFailedTestResult("rangeLeftAntiJoin dfUnit with dfUnitSplitted", List(df1, df2))(actual, expected)
      }
      println()
      result shouldBe true
    }

  "rangeinnerJoin with itself" should "return the same data frame" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"))) { case (df, mrqc, splitPts) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logDf("df", df)
      val dfLeft = df.withColumnRenamed("value", "value_l").withColumn("id", col("value_l") < 1d)
      val dfRight = dfLeft.withColumnRenamed("value_l", "value_r")
      val actual = dfLeft.rangeInnerJoin[Double](dfRight, List("id"))
      val expected = dfLeft.withColumn("value_r", col("value_l"))
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) {
        logger.error(s"Test case failed. splitPts: ${splitPts.mkString(" ; ")}")
        printFailedTestResult("rangeinnerJoin with itself", List(dfLeft, dfRight))(actual, expected)
      }
      println()
      result shouldBe true
    }

  "rangeLeftAntiJoin dfUnit with dfUnitSplitted" should "return an empty data frame" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"))) {
      case (dfUnitSplitted, mrqc, splitPts) =>
        implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
        logDf("dfUnitSplitted", dfUnitSplitted)
        val dfUnit = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
        val actual = dfUnit.rangeLeftAntiJoin[Double](df2 = dfUnitSplitted, joinColumns = Nil)
        val expected = dfUnit.where(lit(false))
        logger.info(s"Java Memory : $getMemoryUsage")
        val result = dfEqual(actual, expected)
        if (!result) {
          logger.error(s"Test case failed. splitPts: ${splitPts.mkString(" ; ")}")
          printFailedTestResult("rangeLeftAntiJoin dfUnit with dfUnitSplitted", List(dfUnit, dfUnitSplitted))(actual, expected)
        }
        println()
        result shouldBe true
    }

}
