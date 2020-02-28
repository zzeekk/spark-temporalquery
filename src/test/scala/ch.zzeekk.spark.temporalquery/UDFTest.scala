package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import TemporalQueryUtil._
import TestUtils._
import UDF._

class UDFTest extends org.scalatest.FunSuite {


  test("addMillisecond") {
    val argExpMap: Map[(Int,Timestamp),Timestamp] = Map(
      ( 1,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124456789"),
      ( 1,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0.001"),
      (-1,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.122456789"),
      (-1,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-02 23:59:59.999"),
      ( 0,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0"),
      ( 0,Timestamp.valueOf("2019-03-03 00:00:0"))            -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMap[(Int,Timestamp), Timestamp](x=>addMillisecond(x._1)(x._2))(argExpMap)
  }

  test("udf_floorTimestamp") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](floorTimestamp)(argExpMap)
  }

  test("udf_ceilTimestamp") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("round up to next millisecond"           ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.124"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-03 00:00:0")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](ceilTimestamp)(argExpMap)
  }

  test("udf_predecessorTime") {
    val argExpMap: Map[(String,Timestamp),Timestamp] = Map(
      ("cut of fraction of millisecond"         ,Timestamp.valueOf("1998-09-05 14:34:56.123456789")) -> Timestamp.valueOf("1998-09-05 14:34:56.123"),
      ("no change as no fraction of millisecond",Timestamp.valueOf("2019-03-03 00:00:0")) -> Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    testArgumentExpectedMapWithComment[Timestamp, Timestamp](predecessorTime)(argExpMap)
  }

}
