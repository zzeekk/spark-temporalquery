package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalHelpers.intervalComplement
import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalClosedIntervalQueryConfig
import org.apache.spark.sql.Row
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

class ClosedIntervalTest extends AnyFlatSpec with Matchers with TestUtils {

  implicit private val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a, b) => a.before(b))
  private val millisIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.MILLIS)
  )
  private val secondIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private val limitedIntervalDef = ClosedInterval(
    Timestamp.valueOf("1900-01-01 00:00:00"), Timestamp.valueOf("2999-12-31 59:59:59"), DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private val longStep2IntervalDef = ClosedInterval(
    0L, Long.MaxValue - 1, DiscreteNumericAxis[Long](2L)
  )


  "ceil timestamp millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("round up to next millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("no change as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.ceil, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "floor timestamp millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.floor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "predecessor millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("subtract a millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("add millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:59:59.999")) -> Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0.001")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "ceil timestamp second" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("round up to next second", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:57"),
      ("no change as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.ceil, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "floor timestamp second" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of second", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56"),
      ("no change as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.floor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "predecessor second" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of second", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56"),
      ("subtract a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59"),
      ("max value has no predecessor", secondIntervalDef.upperHorizon) -> secondIntervalDef.upperHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor second" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of second", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:57"),
      ("add a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:59:59")) -> Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:1"),
      ("min value has no successor", secondIntervalDef.lowerHorizon) -> secondIntervalDef.lowerHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "cut off at boundaries" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut off lower boundary", Timestamp.valueOf("1234-09-05 14:34:56.123456789")) -> limitedIntervalDef.lowerHorizon,
      ("cut off upper boundary", Timestamp.valueOf("3456-03-03 00:59:59")) -> limitedIntervalDef.upperHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](limitedIntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "intervalComplement" should "return expected results" in {
    val subtrahends = Seq(
      ("2020-01-01 00:04:4", "2020-01-01 00:05:0"),
      ("2020-01-01 00:00:1", "2020-01-01 00:01:0"),
      ("2020-01-01 00:03:3", "2020-01-01 00:04:0"),
      ("2020-01-01 00:05:5", "2020-01-01 00:06:0"),
      ("2020-01-01 00:02:2", "2020-01-01 00:03:0")
    ).map(x => Row(Timestamp.valueOf(x._1), Timestamp.valueOf(x._2)))
    val argExpMap = Seq(
      ("no intersection => no change",
        "2019-01-01 00:00:0", "2020-01-01 00:00:0",
        Seq(("2019-01-01 00:00:0", "2020-01-01 00:00:0"))
      ),
      ("right hand intersection => interval ends earlier",
        "2019-01-01 00:00:0", "2020-01-01 00:00:5",
        Seq(("2019-01-01 00:00:0", "2020-01-01 00:00:0.999"))
      ),
      ("left hand intersection => interval starts later",
        "2020-01-01 00:05:7", "2021-12-31 23:59:59.999",
        Seq(("2020-01-01 00:06:0.001", "2021-12-31 23:59:59.999"))
      ),
      ("left and right hand intersection => interval starts later and ends earlier",
        "2020-01-01 00:02:5", "2020-01-01 00:03:5",
        Seq(("2020-01-01 00:03:0.001", "2020-01-01 00:03:2.999"))
      ),
      ("middle intersection => interval segmented into 2 parts",
        "2019-01-01 00:00:0", "2020-01-01 00:01:9.999",
        Seq(("2020-01-01 00:01:0.001", "2020-01-01 00:01:9.999"), ("2019-01-01 00:00:0", "2020-01-01 00:00:0.999"))
      ),
      ("several middle intersections => interval segmented into many parts",
        "2019-01-01 00:00:0", "2021-12-31 23:59:59.999",
        Seq(("2020-01-01 00:06:0.001", "2021-12-31 23:59:59.999")
          , ("2020-01-01 00:05:0.001", "2020-01-01 00:05:4.999")
          , ("2020-01-01 00:04:0.001", "2020-01-01 00:04:3.999")
          , ("2020-01-01 00:03:0.001", "2020-01-01 00:03:2.999")
          , ("2020-01-01 00:01:0.001", "2020-01-01 00:02:1.999")
          , ("2019-01-01 00:00:0", "2020-01-01 00:00:0.999")
        )
      ),
      ("subset => empty result", "2020-01-01 00:05:6", "2020-01-01 00:05:9", Seq())
    ).map { case (comment, validFrom, validTo, resultSeq) => ((comment, (Timestamp.valueOf(validFrom), Timestamp.valueOf(validTo))), resultSeq.map(y => (Timestamp.valueOf(y._1), Timestamp.valueOf(y._2)))) }
      .toMap

    implicit val intervalConfig: TemporalClosedIntervalQueryConfig = TemporalClosedIntervalQueryConfig(intervalDef = millisIntervalDef)
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[(Timestamp, Timestamp), Seq[(Timestamp, Timestamp)]](x => intervalComplement(x._1, x._2, subtrahends), argExpMap)
    results.forall(p => p) shouldBe true
  }

  "ceil long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round up", 35L) -> 36L,
      ("no change", 36L) -> 36L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.ceil, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "floor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round down", 35L) -> 34L,
      ("no change", 36L) -> 36L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.floor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "predecessor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round down", 35L) -> 34L,
      ("remove one step", 36L) -> 34L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round up", 35L) -> 36L,
      ("add one step", 36L) -> 38L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }
}
