package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.Generators
import ch.zzeekk.spark.temporalquery.util.GenericDoubleQueryUtil.GenericHalfOpenIntervalQueryConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks
import org.apache.spark.sql.functions.{col, lit}

class MultiVarHalfopenIntervalTests extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
    with Generators {

  "rangeCombine" should "combine everything possible of every generated dataFrame" in
    forAll(genA = generateHyperdimDataFrames()) { case (df, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logger.info(s"df.count() = ${df.count()} ; df.distinct().count() = ${df.distinct().count()} ; df.schema = ${df.schema.catalogString}")
      val actual = df.rangeCombine[Double]()
      val expected = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCombine generated hyperdim dataFrame", df)(actual, expected)
      println()
      result shouldBe true
    }

  "rangeinnerJoin with itself" should "return the same data framne" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"),
      maxNumSplitCoords = 16)) { case (df, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logger.info(s"df.count() = ${df.count()} ; df.distinct().count() = ${df.distinct().count()} ; df.schema = ${df.schema.catalogString}")
      val dfLeft = df.withColumnRenamed("value", "value_l").withColumn("id", col("value_l") < 1d)
      val dfRight = dfLeft.withColumnRenamed("value_l", "value_r")
      val actual = dfLeft.rangeInnerJoin[Double](dfRight, List("id"))
      val expected = dfLeft.withColumn("value_r", col("value_l"))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeinnerJoin with itself", List(dfLeft, dfRight))(actual, expected)
      println()
      result shouldBe true
    }

  "rangeLeftAntiJoin dfUnit with dfUnitSplitted" should "return an empty data framne" in
    forAll(genA = generateHyperdimDataFrames(valueCol = (col("x000_from") + col("x000_to")).as("value"),
      maxNumSplitCoords = 12)) { case (dfUnitSplitted, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logger.info(s"dfUnitSplitted.count() = ${dfUnitSplitted.count()} ; dfUnitSplitted.schema = ${dfUnitSplitted.schema.catalogString}")
      val dfUnit = hypercuboids2dataFrame()(List(Hypercuboid.unit(mrqc.numDimensions)))
      val actual = dfUnit.rangeLeftAntiJoin[Double](df2 = dfUnitSplitted, joinColumns = Nil)
      val expected = dfUnit.where(lit(false))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeLeftAntiJoin dfUnit with dfUnitSplitted", List(dfUnit, dfUnitSplitted))(actual, expected)
      println()
      result shouldBe true
    }

}
