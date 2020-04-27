package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import org.apache.spark.sql.Row
import org.scalatest.FunSuite

import TestUtils._
import TemporalHelpers._

class TemporalHelpersTest extends FunSuite {

  test("addMillisecond") {
    val argExpMap: Map[(Int,Timestamp),Timestamp] = Map(
      ( 1,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124456789"),
      ( 1,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0.001"),
      (-1,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.122456789"),
      (-1,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-02 23:59:59.999"),
      ( 0,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0"),
      ( 0,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMap[(Int,Timestamp), Timestamp](x=>addMillisecond(x._1)(x._2), argExpMap)
  }

  test("udf_ceilTimestamp") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("round up to next millisecond"           ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](ceilTimestamp, argExpMap)
  }

  test("udf_floorTimestamp") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](floorTimestamp, argExpMap)
  }

  test("udf_predecessorTime") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("subtract a millisecond as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](predecessorTime, argExpMap)
  }

  test("udf_successorTime") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("round up millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("add a millisecond as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0.001")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](successorTime, argExpMap)
  }

  test("udf_temporalComplement") {
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

    testArgumentExpectedMapWithComment[(Timestamp,Timestamp), Seq[(Timestamp,Timestamp)]](x => temporalComplement(x._1, x._2, subtrahends), argExpMap)
  }

}
