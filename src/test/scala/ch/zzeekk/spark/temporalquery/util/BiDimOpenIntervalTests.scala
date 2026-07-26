package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.util.BiLinearDoubleQueryUtil._
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.{saveString2File, TestUtils}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BiDimOpenIntervalTests extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks with TestUtils {

  import session.implicits._
  implicit val biDimConfig: BiLinearHalfOpenIntervalQueryConfig = BiLinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef()
  logger.info(s"BiDimOpenIntervalTests: biDimConfig = $biDimConfig")

  "multivarRangeCombine" should "combine everything possible of every generated dataFrame" in {
    val expected = List((0d, 1d, 0d, 1d, "A")).toDF("x_from", "x_to", "y_from", "y_to", "value")

    forAll(genA = dfConstantUnitSplitted("A")) { df =>
      val actual = df.multivarRangeCombine[Double]()
      val result = dfEqual(actual, expected)
      if (!result) {
        logger.error(s"!!! Test failed !!! Saving dataFrames actual and expected as SVG to files in repository root.")
        saveString2File("argument.svg")(df.toSvg[Double]("value"))
        saveString2File("actual.svg")(actual.toSvg[Double]("value"))
        saveString2File("expected.svg")(expected.toSvg[Double]("value"))
        printFailedTestResult("multivarRangeUnifyRanges dfMoment", df)(actual, expected)
      }
      result shouldBe true
    }
  }

}
