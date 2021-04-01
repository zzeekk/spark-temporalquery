package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import ch.zzeekk.spark.temporalquery.TemporalHelpers.intervalComplement
import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import ch.zzeekk.spark.temporalquery.TestUtils.{testArgumentExpectedMap, testArgumentExpectedMapWithComment}
import org.apache.spark.sql.Row
import org.scalatest.FunSuite

class ClosedIntervalTest extends FunSuite {

  implicit private val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a,b) => a.before(b))
  private val millisIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.MILLIS)
  )
  private val secondIntervalDef = ClosedInterval(
    Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.SECONDS)
  )
  private val limitedIntervalDef = ClosedInterval(
    Timestamp.valueOf("1900-01-01 00:00:00"), Timestamp.valueOf("2999-12-31 59:59:59"), DiscreteTimeAxis(ChronoUnit.SECONDS)
  )


  test("ceil timestamp millis") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("round up to next millisecond"           ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.ceil, argExpMap)
  }

  test("floor timestamp millis") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.floor, argExpMap)
  }

  test("predecessor millis") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("subtract a millisecond as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.predecessor, argExpMap)
  }

  test("successor millis") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("add millisecond as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:59:59.999")) -> Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0.001")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](millisIntervalDef.successor, argExpMap)
  }

  test("ceil timestamp second") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("round up to next second     "           ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:57"),
      ("no change as no fraction of second"     ,Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.ceil, argExpMap)
  }

  test("floor timestamp second") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of second"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56"),
      ("no change as no fraction of second",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.floor, argExpMap)
  }

  test("predecessor second") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of second"                      ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56"),
      ("subtract a millisecond as no fraction of second",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.predecessor, argExpMap)
  }

  test("successor second") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of second"                 ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:57"),
      ("add a millisecond as no fraction of second",Timestamp.valueOf("2019-03-03 00:59:59")) -> Timestamp.valueOf("2019-03-03 01:00:0"),
      ("add a millisecond as no fraction of second",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:1")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](secondIntervalDef.successor, argExpMap)
  }

  test("cut off at boundaries") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut off lower boundary",Timestamp.valueOf("1234-09-05 14:34:56.123456789")) -> limitedIntervalDef.minValue,
      ("cut off upper boundary",Timestamp.valueOf("3456-03-03 00:59:59")) -> limitedIntervalDef.maxValue
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](limitedIntervalDef.successor, argExpMap)
  }

  test("intervalComplement") {
    val subtrahends = Seq(
      ("2020-01-01 00:04:4","2020-01-01 00:05:0"),
      ("2020-01-01 00:00:1","2020-01-01 00:01:0"),
      ("2020-01-01 00:03:3","2020-01-01 00:04:0"),
      ("2020-01-01 00:05:5","2020-01-01 00:06:0"),
      ("2020-01-01 00:02:2","2020-01-01 00:03:0")
    ).map(x => Row(Timestamp.valueOf(x._1),Timestamp.valueOf(x._2)))
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

    val intervalConfig = TemporalQueryConfig(intervalDef = millisIntervalDef)
    testArgumentExpectedMapWithComment[(Timestamp,Timestamp), Seq[(Timestamp,Timestamp)]](x => intervalComplement(x._1, x._2, subtrahends, intervalConfig), argExpMap)
  }
}
