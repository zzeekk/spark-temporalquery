package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.util.BiTemporalQueryUtil.BiTemporalClosedIntervalQueryConfig
import ch.zzeekk.spark.temporalquery.util.{bigBangDay, doomsDay}
import org.apache.spark.sql.DataFrame

import java.sql.Timestamp

object BiTemporalTestUtils extends TestUtils {

  import session.implicits._

  implicit val defaultBiTemporalConfig: BiTemporalClosedIntervalQueryConfig = BiTemporalClosedIntervalQueryConfig()

  val initiumTemporisString: String = bigBangDay.toString
  val finisTemporisString: String = doomsDay.toString

  // helper: (id, known_from, known_to, valid_from, valid_to, value)
  def makeRowsBiTemporal[A, B](row: (A, String, String, String, String, B)): (A, Timestamp, Timestamp, Timestamp, Timestamp, B) =
    (row._1, Timestamp.valueOf(row._2), Timestamp.valueOf(row._3), Timestamp.valueOf(row._4), Timestamp.valueOf(row._5), row._6)

  val dfContinuousTime: DataFrame = Seq(
    // entity 0, Jan 1–5: original entry on day 1, corrected on Mar 15 (known_to marks the correction)
    (0, "2019-01-01 08:00:00", "2019-03-15 00:00:00", "2019-01-01 00:00:00.123456789", "2019-01-05 12:34:56.123456789", 3.14),
    // entity 0, Jan 5–Feb 1: corrected record, known from the correction date onward
    (0, "2019-03-15 00:00:00", finisTemporisString, "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235", 2.72),
    // entity 0, 1ms pulse on Feb 1: always known
    (0, initiumTemporisString, finisTemporisString, "2019-02-01 02:34:56.1235", "2019-02-01 02:34:56.1245", 42.0),
    // entity 0, Feb–Mar: late addition — fact recorded 7 weeks after validity started
    (0, "2019-03-25 14:00:00", finisTemporisString, "2019-02-01 02:34:56.1245", "2019-03-03 00:00:00", 13.0),
    // entity 0, Mar–Apr: entered one week into the valid period
    (0, "2019-03-10 12:00:00", finisTemporisString, "2019-03-03 00:00:00", "2019-04-04 00:00:00", 12.0),
    // entity 0, Sep blip: known only for the same nanosecond window as validity (momentary knowledge)
    (0, "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239", "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239", 42.0),
    // entity 0, 2020+: entered 5 months after validity started
    (0, "2020-06-01 00:00:00", finisTemporisString, "2020-01-01 01:00:00", "9999-12-31 23:59:59.999999999", 18.17),
    // entity 1, Jan–Feb: always known
    (1, initiumTemporisString, finisTemporisString, "2019-01-01 00:00:00.123456789", "2019-02-02 00:00:00", -1.0),
    // entity 1, Mar 2019–Dec 2021: entered retroactively in Jan 2020
    (1, "2020-01-15 09:00:00", finisTemporisString, "2019-03-03 01:00:00", "2021-12-01 02:34:56.1", -2.0)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

  // Simple always-known left table
  val dfLeft: DataFrame = List(
    (0, initiumTemporisString, finisTemporisString, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", 4.2)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")

  /*
   * dfRight covers three bi-temporal patterns:
   *   entity 0, Jan 2018: correction — originally recorded with value 97.15 on 2018-01-01,
   *                        corrected to 98.00 on 2018-06-01
   *   entity 0, Jun–Dec 2018 / 2019: gap in valid history, always known since bigBangDay
   *   entity 0, 2020+: late addition — fact entered into the system 6 months after validity started
   *   entity 1: simple always-known history with None/Some values
   */
  val dfRight: DataFrame = List(
    // entity 0: correction (original record)
    (0, "2018-01-01 00:00:00", "2018-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // entity 0: correction (corrected record)
    (0, "2018-06-01 00:00:00", finisTemporisString, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
    // entity 0: gap in valid history (always known)
    (0, initiumTemporisString, finisTemporisString, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, initiumTemporisString, finisTemporisString, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    // entity 0: late addition — recorded 6 months into the valid period
    (0, "2020-06-01 00:00:00", finisTemporisString, "2020-01-01 00:00:00", finisTemporisString, Some(97.15)),
    // entity 1: always-known history
    (1, initiumTemporisString, finisTemporisString, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, initiumTemporisString, finisTemporisString, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, initiumTemporisString, finisTemporisString, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1, initiumTemporisString, finisTemporisString, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")

  /*
   * dfMap: maps a set of images to id over valid time with varying knowledge timestamps:
   *   "A": recorded immediately on the day validity started (2018-01-01)
   *   "B": always known since bigBangDay
   *   "C": recorded 4 days after validity started (known from 2018-02-05)
   *   "D": recorded on the day validity started (2018-02-20)
   *   "X": added retrospectively on 2018-03-01, valid for only 1ms on 2018-02-25
   */
  val dfMap: DataFrame = List(
    (0, "2018-01-01 00:00:00", finisTemporisString, "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, initiumTemporisString, finisTemporisString, "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-05 00:00:00", finisTemporisString, "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", finisTemporisString, "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999", "D"),
    (0, "2018-03-01 00:00:00", finisTemporisString, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

  // Moment DF: a single-point-in-time fact, known for a moment
  val dfMoment: DataFrame = List(
    (0, "2019-12-01 00:00:00", "2019-12-01 00:00:00", "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", "A")
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

}
