package ch.zzeekk.spark.temporalquery.interval

import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.axis.{DiscreteNumericAxis, DiscreteTimeAxis}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

class ClosedIntervalTest extends AnyFlatSpec with Matchers with TestUtils {
  override protected implicit lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private val millisIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"),
    Timestamp.valueOf("9999-12-31 00:00:00"),
    DiscreteTimeAxis(ChronoUnit.MILLIS)
  )
  private val secondIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"),
    Timestamp.valueOf("9999-12-31 00:00:00"),
    DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private val limitedIntervalDef = ClosedInterval(
    Timestamp.valueOf("1900-01-01 00:00:00"),
    Timestamp.valueOf("2999-12-31 59:59:59"),
    DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private val longStep2IntervalDef = ClosedInterval(
    0L,
    Long.MaxValue - 1,
    DiscreteNumericAxis[Long](2L)
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
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) ->
        Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.floor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "predecessor millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) ->
        Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("subtract a millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) ->
        Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor millis" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of millisecond", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) ->
        Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("add millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:59:59.999")) ->
        Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of millisecond", Timestamp.valueOf("2019-03-03 00:00:0")) ->
        Timestamp.valueOf("2019-03-03 00:00:0.001")
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
      ("subtract a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0")) ->
        Timestamp.valueOf("2019-03-02 23:59:59"),
      ("max value has no predecessor", secondIntervalDef.upperHorizon) -> secondIntervalDef.upperHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor second" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut of fraction of second", Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:57"),
      ("add a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:59:59")) -> Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of second", Timestamp.valueOf("2019-03-03 00:00:0"))  -> Timestamp.valueOf("2019-03-03 00:00:1"),
      ("min value has no successor",                 secondIntervalDef.lowerHorizon)           -> secondIntervalDef.lowerHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "cut off at boundaries" should "return expected results" in {
    val argExpMap: Map[(String, Timestamp), Timestamp] = Map(
      ("cut off lower boundary", Timestamp.valueOf("1234-09-05 14:34:56.123456789")) -> limitedIntervalDef.lowerHorizon,
      ("cut off upper boundary", Timestamp.valueOf("3456-03-03 00:59:59"))           -> limitedIntervalDef.upperHorizon
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[Timestamp, Timestamp](limitedIntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "ceil long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round up",  35L) -> 36L,
      ("no change", 36L) -> 36L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.ceil, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "floor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round down", 35L) -> 34L,
      ("no change",  36L) -> 36L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.floor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "predecessor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round down",      35L) -> 34L,
      ("remove one step", 36L) -> 34L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.predecessor, argExpMap)
    results.forall(p => p) shouldBe true
  }

  "successor long step2" should "return expected results" in {
    val argExpMap = Map(
      ("round up",     35L) -> 36L,
      ("add one step", 36L) -> 38L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment(longStep2IntervalDef.successor, argExpMap)
    results.forall(p => p) shouldBe true
  }
}
