package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.BiTemporalTestUtils._
import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import org.apache.spark.sql.functions.col
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp

class BiTemporalQueryUtilTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering
  logger.info(s"BiTemporalQueryUtilTest: defaultBiTemporalConfig = $defaultBiTemporalConfig")

  "multivarRangeContinuous2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfContinuousTime.multivarRangeContinuous2discrete
    val expected = Seq(
      (0, "2019-01-01 08:00:00", "2019-03-14 23:59:59.999", "2019-01-01 00:00:00.124", "2019-01-05 12:34:56.123", 3.14),
      (0, "2019-03-15 00:00:00", finisTemporisString,       "2019-01-05 12:34:56.124", "2019-02-01 02:34:56.123", 2.72),
      (0, initiumTemporisString, finisTemporisString,       "2019-02-01 02:34:56.124", "2019-02-01 02:34:56.124", 42.0),
      (0, "2019-03-25 14:00:00", finisTemporisString,       "2019-02-01 02:34:56.125", "2019-03-02 23:59:59.999", 13.0),
      (0, "2019-03-10 12:00:00", finisTemporisString,       "2019-03-03 00:00:00",     "2019-04-03 23:59:59.999", 12.0),
      (0, "2020-06-01 00:00:00", finisTemporisString,       "2020-01-01 01:00:00",     finisTemporisString,       18.17),
      (1, initiumTemporisString, finisTemporisString,       "2019-01-01 00:00:00.124", "2019-02-01 23:59:59.999", -1.0),
      (1, "2020-01-15 09:00:00", finisTemporisString,       "2019-03-03 01:00:00",     "2021-12-01 02:34:56.099", -2.0)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("temporalContinuous2discrete", Seq(dfContinuousTime))(actual, expected)
    result shouldBe true
  }

  "multivarRangeUnifyRanges" should "not modify dfMoment as extend and fillGapsWithNull are false" in {
    val actual = dfMoment.multivarRangeUnifyRanges(keys = Seq("id"))
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = dfMoment
    logger.info("expected:")
    expected.show(false)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

  "multivarRangeUnifyRanges" should "extend dfMoment" in {
    val actual = dfMoment.multivarRangeUnifyRanges(keys = Seq("id"), extend = true, fillGapsWithNull = true)
      .select(dfMoment.columns.map(col): _*) // re-order columns
    val expected = List(
      (0, initiumTemporisString,     "2019-11-30 23:59:59.999", initiumTemporisString,     finisTemporisString,       None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     initiumTemporisString,     "2019-11-25 11:12:13.004", None),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", Some("A")),
      (0, "2019-12-01 00:00:00",     "2019-12-01 00:00:00",     "2019-11-25 11:12:13.006", finisTemporisString,       None),
      (0, "2019-12-01 00:00:00.001", finisTemporisString,       initiumTemporisString,     finisTemporisString,       None)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")
    logger.info("expected:")
    expected.show(false)
    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("multivarRangeUnifyRanges dfMoment", dfMoment)(actual, expected)
    result shouldBe true
  }

}
