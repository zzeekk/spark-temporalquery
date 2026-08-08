package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.bilinear.BiLinearDoubleQueryUtil._
import ch.zzeekk.spark.temporalquery.{saveString2File, Generators}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.scalacheck.ScalaCheckPropertyChecks

class BiDimHalfopenIntervalTests extends AnyFlatSpec with Matchers with ScalaCheckPropertyChecks
    with Generators {

  import session.implicits._
  implicit val biDimConfig: BiLinearHalfOpenIntervalQueryConfig = BiLinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef()
  logger.info(s"BiDimHalfopenIntervalTests: biDimConfig = $biDimConfig")

  "rangeCombine" should "combine everything possible of every generated dataFrame" in {
    val expected = List((0d, 1d, 0d, 1d, "A")).toDF("x_from", "x_to", "y_from", "y_to", "value")
    logger.info("expected result:")
    expected.show(false)

    forAll(genA = dfBiTempConstantUnitSplitted("A")) { df =>
      logger.info(s"df.count() = ${df.count()} ; df.distinct().count() = ${df.distinct().count()} ; df.schema = ${df.schema.catalogString}")
      val actual = df.rangeCombine[Double]()
      val result = dfEqual(actual, expected)
      if (!result) {
        logger.error(s"!!! Test failed !!! Saving dataFrames actual and expected as SVG to files in repository root.")
        saveString2File("argument.svg")(df.toSvg[Double]("value"))
        saveString2File("actual.svg")(actual.toSvg[Double]("value"))
        saveString2File("expected.svg")(expected.toSvg[Double]("value"))
        printFailedTestResult("rangeUnifyRanges dfMoment", df)(actual, expected)
      }
      result shouldBe true
    }
  }

}
