package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.BiTemporalTestUtils._
import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.reflect.runtime.universe.typeTag

class SVGexportTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering
  logger.info(s"SVGexportTest: defaultBiTemporalConfig = $defaultBiTemporalConfig")

  "toSvg" should "produce a well-formed SVG for dfContinuousTime (id=0) with half-open intervals" in {
    logger.info(s"SVGexportTest: halfopenBiTemporalConfig = $halfopenBiTemporalConfig")
    // Both timeOrdering and typeTag are required alongside the mrqc implicit.
    // We pass all three explicitly to avoid the ambiguity between defaultBiTemporalConfig
    // (from the class-level wildcard import) and halfopenBiTemporalConfig as MultivarRangeQueryConfig[Timestamp,_].
    val argument = dfContinuousTime.where($"id" === 0)
    val actual = argument.toSvg[Timestamp]("value")(timeOrdering, typeTag[Timestamp], halfopenBiTemporalConfig)
    logger.info(s"Voilà df dfContinuousTime as SVG:")
    argument.orderBy(halfopenBiTemporalConfig.intervalDimensions.map(dim => col(dim.fromColName)): _*).show(false)
    println(actual)
    // structural checks
    actual should startWith("<svg")
    actual should endWith("</svg>")
    // 7 data rows + 1 bounding rectangle
    actual.split("<rect").length - 1 shouldBe 8
    // half-open intervals: data rectangles carry no stroke outline
    actual should not include "stroke-width=\"0.5\""
    // bounding rectangle is still outlined
    actual should include("stroke-width=\"1\"")
    // numeric value column → gradient fill colours (hex literals)
    actual should include("fill=\"#")
  }

  "toSvg" should "produce a well-formed SVG for dfContinuousTime (id=0) discretised with closed intervals" in {
    // defaultBiTemporalConfig is implicit in scope; timeOrdering and TypeTag[Timestamp] resolve implicitly.
    // multivarRangeContinuous2discrete drops the 8-nanosecond pulse (sub-millisecond, collapses after rounding),
    // leaving 6 rows for id=0.
    val argument = dfContinuousTime.where($"id" === 0).multivarRangeContinuous2discrete
    val actual = argument.toSvg[Timestamp]("value")
    argument.orderBy(defaultBiTemporalConfig.intervalDimensions.map(dim => col(dim.fromColName)): _*).show(false)
    println(actual)
    actual should startWith("<svg")
    actual should endWith("</svg>")
    // 6 data rows + 1 bounding rectangle
    actual.split("<rect").length - 1 shouldBe 7
    // closed intervals: data rectangles have a stroke outline
    actual should include("stroke-width=\"0.5\"")
    // bounding rectangle is still outlined
    actual should include("stroke-width=\"1\"")
    actual should include("fill=\"#")
  }

}
