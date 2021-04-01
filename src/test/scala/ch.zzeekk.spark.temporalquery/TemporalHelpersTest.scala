package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}

import ch.zzeekk.spark.temporalquery.TemporalHelpers._
import ch.zzeekk.spark.temporalquery.TestUtils._
import org.scalatest.FunSuite

class TemporalHelpersTest extends FunSuite {

  test("durationInMillis") {
    println(s"test durationInMillis: Calendar.getInstance().get(Calendar.DST_OFFSET) = ${Calendar.getInstance().get(Calendar.DST_OFFSET)}")
    val dstOffset: Int = TimeZone.getDefault.getDSTSavings
    val argExpMap = Map[(String, (String, String)), Long](
      ("january 2019: 31 Tage", ("2019-01-31 23:59:59.999", "2019-01-01 00:00:0")) -> 31 * millisPerDay,
      ("Winterzeit überzogen 2019", ("2019-03-31 02:59:59.999", "2019-03-31 02:00:0")) -> millisPerHour,
      ("Winter->Sommerzeit 2019", ("2019-03-31 03:00:0", "2019-03-31 02:00:0")) -> (1L + millisPerHour - dstOffset),
      ("März 2019: 31d - 1Stunde", ("2019-03-31 23:59:59.999", "2019-03-01 00:00:0")) -> (31 * millisPerDay - dstOffset),
      ("Sommer -> Winterzeit 2019", ("2019-10-27 02:00:0", "2019-10-27 1:59:59.999")) -> (2L + dstOffset),
      ("Sommer -> Winterzeit 2019", ("2019-10-27 01:59:59.999", "2019-10-27 1:00:0")) -> millisPerHour,
      ("Sommer -> Winterzeit 2019", ("2019-10-27 02:00:0", "2019-10-27 1:00:0")) -> (1L + millisPerHour + dstOffset),
      ("oct 2019: 31d + 1Stunde", ("2019-10-31 23:59:59.999", "2019-10-01 00:00:0")) -> (31 * millisPerDay + dstOffset),
      ("year 2019: 365 Tage", ("2019-12-31 23:59:59.999", "2019-01-01 00:00:0")) -> 365 * millisPerDay,
      ("leap year 2020: 366 Tage", ("2020-12-31 23:59:59.999", "2020-01-01 00:00:0")) -> 366 * millisPerDay,
      ("leap second 1995: 365 Tage (+ 1 second)", ("1995-12-31 23:59:59.999", "1995-01-01 00:00:0")) -> 365 * millisPerDay,
      ("2 leap seconds and leap year 1972: 366 Tage  (+ 2 second)", ("1972-12-31 23:59:59.999", "1972-01-01 00:00:0")) -> 366 * millisPerDay,
      ("just a moment", ("2020-03-17 10:00:0", "2020-03-17 10:00:0")) -> 1L)
    testArgumentExpectedMapWithComment[(String, String), Long](x => durationInMillis(Timestamp.valueOf(x._1), Timestamp.valueOf(x._2)),argExpMap)
  }

}
