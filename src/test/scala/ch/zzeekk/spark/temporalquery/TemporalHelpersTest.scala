package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalTestUtils._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Timestamp
import java.util.{Calendar, TimeZone}

class TemporalHelpersTest extends AnyFlatSpec with Matchers {

  "durationInMillis" should "return expected results" in {
    println(s"test durationInMillis: Calendar.getInstance().get(Calendar.DST_OFFSET) = ${Calendar.getInstance().get(Calendar.DST_OFFSET)}")
    val dstOffset: Int = TimeZone.getDefault.getDSTSavings
    val argExpMap = Map[(String, (String, String)), Long](
      ("january 2019: 31 days",                   ("2019-01-31 23:59:59.999", "2019-01-01 00:00:0")) -> 31 * millisPerDay,
      ("DST winter overlap 2019",                 ("2019-03-31 02:59:59.999", "2019-03-31 02:00:0")) -> millisPerHour,
      ("Winter->Summer time 2019",                ("2019-03-31 03:00:0", "2019-03-31 02:00:0"))      -> (1L + millisPerHour - dstOffset),
      ("March 2019: 31d - 1 hour",                ("2019-03-31 23:59:59.999", "2019-03-01 00:00:0")) -> (31 * millisPerDay - dstOffset),
      ("Summer -> Winter time 2019",              ("2019-10-27 02:00:0", "2019-10-27 1:59:59.999"))  -> (2L + dstOffset),
      ("Summer -> Winter time 2019",              ("2019-10-27 01:59:59.999", "2019-10-27 1:00:0"))  -> millisPerHour,
      ("Summer -> Winter time 2019",              ("2019-10-27 02:00:0", "2019-10-27 1:00:0"))       -> (1L + millisPerHour + dstOffset),
      ("oct 2019: 31d + 1 hour",                  ("2019-10-31 23:59:59.999", "2019-10-01 00:00:0")) -> (31 * millisPerDay + dstOffset),
      ("year 2019: 365 days",                     ("2019-12-31 23:59:59.999", "2019-01-01 00:00:0")) -> 365 * millisPerDay,
      ("leap year 2020: 366 days",                ("2020-12-31 23:59:59.999", "2020-01-01 00:00:0")) -> 366 * millisPerDay,
      ("leap second 1995: 365 days (+ 1 second)", ("1995-12-31 23:59:59.999", "1995-01-01 00:00:0")) -> 365 * millisPerDay,
      ("2 leap seconds and leap year 1972: 366 days (+ 2 seconds)", ("1972-12-31 23:59:59.999", "1972-01-01 00:00:0")) ->
        366 * millisPerDay,
      ("just a moment", ("2020-03-17 10:00:0", "2020-03-17 10:00:0")) -> 1L
    )
    val results: Set[Boolean] = testArgumentExpectedMapWithComment[(String, String), Long](
      x => durationInMillis(Timestamp.valueOf(x._1), Timestamp.valueOf(x._2)),
      argExpMap
    )
    results.forall(p => p) shouldBe true
  }

}
