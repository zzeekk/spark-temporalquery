package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TestUtils.testArgumentExpectedMapWithComment
import org.scalatest.FunSuite

class ClosedFromOpenToIntervalTest extends FunSuite {

  private val floatIntervalDef = ClosedFromOpenToInterval[Float](minValue = 0f, maxValue = Float.MaxValue)
  private val limitedIntervalDef = ClosedFromOpenToInterval[Float](minValue = 23.34f, maxValue = 45.32f)

  test("ceil float") {
    val argExpMap = Map(("ceil is the same",35.9393f) -> 35.9393f)
    testArgumentExpectedMapWithComment[Float, Float](floatIntervalDef.ceil, argExpMap)
  }

  test("floor float") {
    val argExpMap = Map(("floor is the same",35.9393f) -> 35.9393f)
    testArgumentExpectedMapWithComment[Float, Float](floatIntervalDef.floor, argExpMap)
  }

  test("predecessor float") {
    val argExpMap = Map(("predecessor is the same",35.9393f) -> 35.9393f)
    testArgumentExpectedMapWithComment[Float, Float](floatIntervalDef.predecessor, argExpMap)
  }

  test("successor float") {
    val argExpMap = Map(("successor is the same",35.9393f) -> 35.9393f)
    testArgumentExpectedMapWithComment[Float, Float](floatIntervalDef.successor, argExpMap)
  }

  test("cut off at boundaries)") {
    val argExpMap = Map(
      ("cut off lower boundary",10f) -> limitedIntervalDef.minValue,
      ("cut off upper boundary",999f) -> limitedIntervalDef.maxValue
    )
    testArgumentExpectedMapWithComment[Float, Float](limitedIntervalDef.successor, argExpMap)
  }

  // TODO: adapt intervalComplement for ClosedFromOpenToInterval
  /*
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
  */
}
