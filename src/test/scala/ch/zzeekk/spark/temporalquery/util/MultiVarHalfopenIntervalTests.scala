package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.Generators
import ch.zzeekk.spark.temporalquery.util.GenericDoubleQueryUtil.GenericHalfOpenIntervalQueryConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class MultiVarHalfopenIntervalTests extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
    with Generators {

  "rangeCombine" should "combine everything possible of every generated dataFrame" in {
    val myValue = "A"

    logger.info(s"maxProductNumberSplitpointsDimensions = $maxProductNumberSplitpointsDimensions")
    println()

    forAll(genA = generateHyperdimDataFrames(myValue)) { case (df, mrqc) =>
      implicit val mrqcImpl: GenericHalfOpenIntervalQueryConfig = mrqc
      logger.info(s"df.count() = ${df.count()} ; df.distinct().count() = ${df.distinct().count()} ; df.schema = ${df.schema.catalogString}")
      val actual = df.rangeCombine[Double]()
      val expected = hypercuboids2dataFrame(myValue)(List(Hypercuboid.unit(mrqc.numDimensions)))
      val result = dfEqual(actual, expected)
      if (!result) printFailedTestResult("rangeCombine generated hyperdim dataFrame", df)(actual, expected)
      println()
      result shouldBe true
    }

  }

}
