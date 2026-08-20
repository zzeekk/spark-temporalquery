package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.timestampOrdering
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import scala.reflect.runtime.universe.typeTag

class SVGexportTest extends AnyFlatSpec with Matchers with BiTemporalTestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering
  logger.info(s"SVGexportTest: defaultBiTemporalConfig = $defaultBiTemporalConfig")

  "toSvg" should "produce a well-formed SVG for dfDenseTime (id=0) with half-open intervals" in {
    logger.info(s"SVGexportTest: halfopenBiTemporalConfig = $halfopenBiTemporalConfig")
    // Both timeOrdering and typeTag are required alongside the mrqc implicit.
    // We pass all three explicitly to avoid the ambiguity between defaultBiTemporalConfig
    // (from the class-level wildcard import) and halfopenBiTemporalConfig as MultivarRangeQueryConfig[Timestamp,_].
    val argument = dfDenseTime.where($"id" === 0)
    val actual = argument.toSvg[Timestamp]("value")(timeOrdering, typeTag[Timestamp], halfopenBiTemporalConfig)
    logger.info(s"Voilà df dfDenseTime as SVG:")
    argument.orderBy(halfopenBiTemporalConfig.rangeDimensions.map(dim => col(dim.fromColName)): _*).show(false)
    println(actual)
    // structural checks
    actual should startWith("<svg")
    actual should endWith("</svg>")
    // 7 data rows + 1 bounding rectangle
    actual.split("<rect").length - 1 shouldBe 8
    // half-open intervals: data rectangles are outlined with a dashed stroke
    actual should include("stroke-dasharray=")
    // bounding rectangle is still outlined
    actual should include("stroke-width=\"1\"")
    // numeric value column → gradient fill colours (hex literals)
    actual should include("fill=\"#")
  }

  "toSvg" should "produce a well-formed SVG for dfDenseTime (id=0) discretised with closed intervals" in {
    // defaultBiTemporalConfig is implicit in scope; timeOrdering and TypeTag[Timestamp] resolve implicitly.
    // rangeDense2discrete drops the 8-nanosecond pulse (sub-millisecond, collapses after rounding),
    // leaving 6 rows for id=0.
    val argument = dfDenseTime.where($"id" === 0).rangeDense2discrete
    val actual = argument.toSvg[Timestamp]("value")
    argument.orderBy(defaultBiTemporalConfig.rangeDimensions.map(dim => col(dim.fromColName)): _*).show(false)
    println(actual)
    actual should startWith("<svg")
    actual should endWith("</svg>")
    // 6 data rows + 1 bounding rectangle
    actual.split("<rect").length - 1 shouldBe 7
    // closed intervals: data rectangles have a solid (non-dashed) stroke outline
    actual should include("stroke-width=\"0.5\"")
    actual should not include "stroke-dasharray="
    // bounding rectangle is still outlined
    actual should include("stroke-width=\"1\"")
    actual should include("fill=\"#")
  }

}
