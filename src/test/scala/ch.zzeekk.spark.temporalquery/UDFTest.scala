package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import TemporalQueryUtil._
import TestUtils._
import UDF._

class UDFTest extends org.scalatest.FunSuite {


  test("plusMillisecond") {
    val argExpMap: Map[Timestamp,Timestamp] = Map(
      Timestamp.valueOf("1998-09-05 14:34:56.123456789") -> Timestamp.valueOf("1998-09-05 14:34:56.124456789"),
      Timestamp.valueOf("2019-03-03 00:00:0") -> Timestamp.valueOf("2019-03-03 00:00:0.001")
    )
    testArgumentExpectedMap[Timestamp, Timestamp](addMillisecond(1))(argExpMap)
  }

  test("minusMillisecond") {
    val argExpMap: Map[Timestamp,Timestamp] = Map(
      Timestamp.valueOf("1998-09-05 14:34:56.123456789") -> Timestamp.valueOf("1998-09-05 14:34:56.122456789"),
      Timestamp.valueOf("2019-03-03 00:00:0") -> Timestamp.valueOf("2019-03-02 23:59:59.999")
    )
    testArgumentExpectedMap[Timestamp, Timestamp](addMillisecond(-1))(argExpMap)
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
