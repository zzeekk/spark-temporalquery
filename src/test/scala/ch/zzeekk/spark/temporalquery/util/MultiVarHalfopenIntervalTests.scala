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
    forAll(genA = generateHyperdimDataFrames()) { case (df, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logDf("df", df)
      val actual = df.rangeCombine[Double]()
      val expected = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCombine generated hyperdim dataFrame", df)(actual, expected)
      println()
      result shouldBe true
    }

  "rangeCombine" should "combine to one row" in {
    import session.implicits._
    implicit val mrqc: GenericHalfOpenIntervalQueryConfig = GenericHalfOpenIntervalQueryConfig.withDefaultIntervalDef(numDim = 3)
    logger.info(s"mrqc = $mrqc")
    val argument = List(
      (0d,   0.2,  0d,   1d,   0.4,  1d,   "A"),
      (0d,   1d,   0d,   1d,   0d,   0.4,  "A"),
      (0.2,  0.26, 0.1,  0.7,  0.54, 1d,   "A"),
      (0.2,  1d,   0.7,  1d,   0.4,  1d,   "A"),
      (0.2,  1d,   0d,   0.1,  0.54, 1d,   "A"),
      (0.2,  1d,   0d,   0.7,  0.4,  0.54, "A"),
      (0.26, 0.28, 0.1,  0.7,  0.54, 0.84, "A"),
      (0.26, 0.28, 0.23, 0.7,  0.84, 1d,   "A"),
      (0.26, 1d,   0.1,  0.23, 0.84, 1d,   "A"),
      (0.28, 1d,   0.1,  0.23, 0.54, 0.84, "A"),
      (0.28, 1d,   0.23, 0.7,  0.54, 1d,   "A")
    ).toDF("x000_from", "x000_to", "x001_from", "x001_to", "x002_from", "x002_to", "value")
    val actual = argument.rangeCombine[Double]()
    val expected = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeCombine", argument)(actual, expected)
    println()
    result shouldBe true
  }

  "rangeinnerJoin df1 with df2" should "return the same as rangeinnerJoin df2 with df1" in
    forAll(genA = unitDoubles) { xs =>
      val (df1, mrqc) = getHyperdimDataFrame((col("x000_from") + col("x000_to")).as("value_l"),
        maxNumSplitCoords = 10)(xs)
      logDf("df1", df1)
      val (df2, _) = getHyperdimDataFrame((col("x000_from") * col("x000_to")).as("value_r"),
        maxNumSplitCoords = 10)(xs.reverse)
      logDf("df2", df2)
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      val actual = df1.rangeInnerJoin[Double](df2, Nil)
      val expected = df2.rangeInnerJoin[Double](df1, Nil)
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeLeftAntiJoin dfUnit with dfUnitSplitted", List(df1, df2))(actual, expected)
      println()
      result shouldBe true
    }

  "rangeinnerJoin with itself" should "return the same data framne" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"))) { case (df, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logDf("df", df)
      val dfLeft = df.withColumnRenamed("value", "value_l").withColumn("id", col("value_l") < 1d)
      val dfRight = dfLeft.withColumnRenamed("value_l", "value_r")
      val actual = dfLeft.rangeInnerJoin[Double](dfRight, List("id"))
      val expected = dfLeft.withColumn("value_r", col("value_l"))
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeinnerJoin with itself", List(dfLeft, dfRight))(actual, expected)
      println()
      result shouldBe true
    }

  "rangeLeftAntiJoin dfUnit with dfUnitSplitted" should "return an empty data frame" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"))) { case (dfUnitSplitted, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logDf("dfUnitSplitted", dfUnitSplitted)
      val dfUnit = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
      val actual = dfUnit.rangeLeftAntiJoin[Double](df2 = dfUnitSplitted, joinColumns = Nil)
      val expected = dfUnit.where(lit(false))
      logger.info(s"Java Memory : $getMemoryUsage")
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeLeftAntiJoin dfUnit with dfUnitSplitted", List(dfUnit, dfUnitSplitted))(actual, expected)
      println()
      result shouldBe true
    }

}
