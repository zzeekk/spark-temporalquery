package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.axis.DiscreteTimeAxis
import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util._
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalClosedIntervalQueryUtil._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

class BiTemporalClosedMixedTest extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._
  private implicit val timeOrdering: Ordering[Timestamp] = timestampOrdering
  private val intervalDays = ClosedInterval(
    lowerHorizon = bigBangDay,
    upperHorizon = doomsDay,
    discreteAxisDef = DiscreteTimeAxis(ChronoUnit.DAYS)
  )
  private val intervalSeconds = ClosedInterval(
    lowerHorizon = bigBangDay,
    upperHorizon = doomsDay,
    discreteAxisDef = DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private implicit val mrqc: BiLinearClosedIntervalQueryConfig = BiLinearClosedIntervalQueryConfig(
    dimensionMap = Map("known_from" -> ("known_to", intervalDays), "valid_from" -> ("valid_to", intervalSeconds))
  )

  logger.info(s"BiTemporalQueryUtilTest: mrqc = $mrqc")

  "rangeDense2discrete" should "round to ms without adding gaps or overlaps" in {
    val actual = dfDenseTime.rangeDense2discrete
    val expected = Seq(
      (0, "2019-01-02 0:0:0", "2019-03-14 0:0:0",  "2019-01-01 00:00:01", "2019-01-05 12:34:56", 3.14),
      (0, "2019-03-11 0:0:0", finisTemporisString, "2019-03-03 00:00:00", "2019-04-03 23:59:59", 12.0),
      (0, "2019-03-15 0:0:0", finisTemporisString, "2019-01-05 12:34:57", "2019-02-01 02:34:56", 2.72),
      (0, "2019-03-26 0:0:0", finisTemporisString, "2019-02-01 02:34:57", "2019-03-02 23:59:59", 13.0),
      (0, "2020-06-01 0:0:0", finisTemporisString, "2020-01-01 01:00:00", finisTemporisString,   18.17),
      (1, "1970-01-01 0:0:0", finisTemporisString, "2019-01-01 00:00:01", "2019-02-01 23:59:59", -1.0),
      (1, "2020-01-16 0:0:0", finisTemporisString, "2019-03-03 01:00:00", "2021-12-01 02:34:56", -2.0)
    ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

    val result = dfEqual(actual, expected)
    if (!result) printFailedTestResult("rangeDense2discrete", Seq(dfDenseTime))(actual, expected)
    result shouldBe true
  }

}
