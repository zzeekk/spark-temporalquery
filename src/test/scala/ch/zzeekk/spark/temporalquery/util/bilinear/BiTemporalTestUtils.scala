package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.TestUtils
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalClosedIntervalQueryUtil._
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalHalfOpenIntervalQueryUtil.defaultHalfOpenIntervalDef
import ch.zzeekk.spark.temporalquery.util.{bigBangDay, doomsDay}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DoubleType

import java.sql.Timestamp

object BiTemporalTestUtils extends TestUtils {

  import session.implicits._

  implicit val defaultBiTemporalConfig: BiLinearClosedIntervalQueryConfig = BiLinearClosedIntervalQueryConfig
    .withDefaultIntervalDef(fstFromColName = "known_from", fstToColName = "known_to",
      sndFromColName = "valid_from", sndToColName = "valid_to")

  val halfopenBiTemporalConfig: BiLinearHalfOpenIntervalQueryConfig = BiLinearHalfOpenIntervalQueryConfig
    .withDefaultIntervalDef(fstFromColName = "known_from", fstToColName = "known_to",
      sndFromColName = "valid_from", sndToColName = "valid_to")

  val initiumTemporisString: String = bigBangDay.toString
  val finisTemporisString: String = doomsDay.toString

  // helper: (id, known_from, known_to, valid_from, valid_to, value)
  def makeRowsBiTemporal[A, B](row: (A, String, String, String, String, B)): (A, Timestamp, Timestamp, Timestamp, Timestamp, B) =
    (row._1, Timestamp.valueOf(row._2), Timestamp.valueOf(row._3), Timestamp.valueOf(row._4), Timestamp.valueOf(row._5), row._6)

  // helper: (id, known_from, known_to, valid_from, valid_to, value, _defined)
  def makeRowsBiTemporalDefined[A, B](row: (A, String, String, String, String, B, Boolean))
      : (A, Timestamp, Timestamp, Timestamp, Timestamp, B, Boolean) =
    (row._1, Timestamp.valueOf(row._2), Timestamp.valueOf(row._3), Timestamp.valueOf(row._4), Timestamp.valueOf(row._5), row._6, row._7)

  // helper: turns a uni-temporal (id, valid_from, valid_to, value) row into a bi-temporal row which
  // is always known, i.e. known_from = initiumTemporisString, known_to = finisTemporisString
  def wrapAlwaysKnown[A, B](row: (A, String, String, B)): (A, String, String, String, String, B) =
    (row._1, initiumTemporisString, finisTemporisString, row._2, row._3, row._4)

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

  val dfDocumentation: DataFrame = Seq(
    (1, "2020-01-01 00:00:00", "2021-01-01 00:00:00", "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235", 2.72),
    // known and valid overlap with previous record
    (1, "2020-01-01 00:00:00", "2021-01-01 00:00:00", "2019-02-01 01:00:00.0", "2019-02-01 02:34:56.1245",
      2.72),
    // valid ends before it starts
    (1, "2020-03-01 00:00:00", "2021-04-01 00:00:00", "2019-02-01 02:34:56.125", "2019-02-01 02:34:56.1245", 5.03),
    // known ends before it starts
    (1, "2020-06-03 12:00:00", "2020-06-03 10:00:00", "2019-07-01 00:00:0", "2019-08-01 00:00:0",      4.13),
    (1, "2020-09-01 00:00:00", "2023-01-01 00:00:00", "2019-01-01 00:00:0", "2019-12-31 23:59:59.999", 42.0)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

  val dfDirtyTimeRanges: DataFrame = Seq(
    // id = 0
    (0, "2020-01-01 00:00:00.123456789", "2020-01-05 12:34:56.123456789", "2019-01-01 00:00:00.123456789", "2019-01-05 12:34:56.123456789",
      3.14),
    (0, "2020-01-05 12:34:56.123456789", "2020-02-01 02:34:56.1235", "2019-01-05 12:34:56.123456789", "2019-02-01 02:34:56.1235", 2.72),
    // overlaps with previous record (both axes)
    (0, "2020-02-01 01:00:00.0", "2020-02-01 02:34:56.1245", "2019-02-01 01:00:00.0", "2019-02-01 02:34:56.1245", 2.72),
    // ends before it starts (both axes)
    (0, "2020-03-01 00:00:00.0", "2020-02-01 00:00:00.0", "2019-03-01 00:00:00.0", "2019-02-01 00:00:00.0", 2.72),
    // ends before it starts (both axes)
    (0, "2020-02-01 02:34:56.1236", "2020-02-01 02:34:56.1235", "2019-02-01 02:34:56.1236", "2019-02-01 02:34:56.1235", 42.0),
    (0, "2020-02-01 02:34:56.1245", "2020-03-03 00:00:0",       "2019-02-01 02:34:56.1245", "2019-03-03 00:00:0",       13.0),
    (0, "2020-03-03 00:00:0",       "2020-04-04 00:00:0",       "2019-03-03 00:00:0",       "2019-04-04 00:00:0",       13.0),
    // duration less than a millisecond without touching bounds of millisecond interval (both axes)
    (0, "2020-09-05 02:34:56.1231", "2020-09-05 02:34:56.1239",      "2019-09-05 02:34:56.1231", "2019-09-05 02:34:56.1239",      42.0),
    (0, "2021-01-01 01:00:0",       "9999-12-31 23:59:59.999999999", "2020-01-01 01:00:0",       "9999-12-31 23:59:59.999999999", 18.17),
    // known-axis overlap: two entries for the same valid period, overlapping 5 days on the known axis
    (0, "2022-03-01 00:00:00.0", "2022-03-10 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999", 55.0),
    (0, "2022-03-05 00:00:00.0", "2022-03-15 23:59:59.999", "2019-07-01 00:00:00.0", "2019-07-05 23:59:59.999", 66.0),
    // id = 1
    (1, "2020-01-01 00:00:0.123456789", "2020-02-02 00:00:0", "2019-01-01 00:00:0.123456789", "2019-02-02 00:00:0", -1.0),
    // duration less than a millisecond (both axes)
    (1, "2020-03-01 00:00:0", "2020-03-01 00:00:00.0001", "2019-03-01 00:00:0", "2019-03-01 00:00:00.0001", 0.1),
    // duration less than a millisecond (both axes), overlaps with previous record (valid axis)
    (1, "2020-03-01 00:00:1", "2020-03-01 00:00:1.0005", "2019-03-01 00:00:0.00009", "2019-03-01 00:00:00.001", 0.1),
    // duration less than a millisecond (both axes)
    (1, "2020-03-01 00:00:1.0009", "2020-03-01 00:00:01.0021", "2019-03-01 00:00:1.0009", "2019-03-01 00:00:01.0021", 1.2),
    // duration less than a millisecond (both axes)
    (1, "2020-03-01 00:00:0.0001", "2020-03-01 00:00:00.0009", "2019-03-01 00:00:0.0001", "2019-03-01 00:00:00.0009", 0.8),
    (1, "2020-03-03 01:00:0",      "2022-12-01 02:34:56.1",    "2019-03-03 01:00:0",      "2021-12-01 02:34:56.1",    -2.0)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

  // Simple always-known left table
  val dfLeft: DataFrame = List(
    (0, initiumTemporisString, finisTemporisString, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", 4.2)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")

  /*
   * dfMap: maps a set of images to id over valid time with varying knowledge timestamps:
   *   "A": got to know on New Year's Day: valid in January
   *   "B": always known to be valid in February since bigBangDay
   *   "C": recorded beginning February but deleted mid March
   *   "D": recorded on February 20: valid from that day until the end of March
   *   "X": some typo mistake which remained in the data solely on March 1st: valid for a milli second on Feb 25
   */
  val dfMap: DataFrame = List(
    (0, initiumTemporisString, finisTemporisString,       "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-01-01 00:00:00", finisTemporisString,       "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-02-05 00:00:00", "2018-03-15 23:59:59.999", "2018-02-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", finisTemporisString,       "2018-02-20 00:00:00",     "2018-03-31 23:59:59.999", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

  val dfMapToCombine: DataFrame = Seq(
    (0, initiumTemporisString, "2017-12-31 23:59:59.999", "2018-01-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-01-01 00:00:00", "2018-02-04 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-05 00:00:00", "2018-02-19 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "B"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "D"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "D"),
    (0, "2018-02-20 00:00:00", "2018-02-28 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "B"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "C"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-25 14:15:16.122", "B"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-25 14:15:16.122", "C"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-25 14:15:16.122", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "B"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "C"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999", "B"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999", "C"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-02-25 14:15:16.124", "2018-02-28 23:59:59.999", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "D"),
    (0, "2018-03-01 00:00:00", "2018-03-01 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", "D"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "B"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "C"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "C"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "D"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "C"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-03-01 00:00:00",     "2018-03-03 23:59:59.999", "D"),
    (0, "2018-03-02 00:00:00", "2018-03-15 23:59:59.999", "2018-03-04 00:00:00",     "2018-03-31 23:59:59.999", "D"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "A"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-01-01 00:00:00",     "2018-01-31 23:59:59.999", "B"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-02-01 00:00:00",     "2018-02-19 23:59:59.999", "B"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "B"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-02-20 00:00:00",     "2018-02-28 23:59:59.999", "D"),
    (0, "2018-03-16 00:00:00", finisTemporisString,       "2018-03-01 00:00:00",     "2018-03-31 23:59:59.999", "D")
  ).map(makeRowsBiTemporal)
    .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

  // Moment DF: a single-point-in-time fact, known for a moment
  val dfMoment: DataFrame = List(
    (0, "2019-12-01 00:00:00", "2019-12-01 00:00:00", "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", "A")
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

  // in January we believe overlap for 1ms, namely [10:00:00.000,10:00:00.001[, the image set is {A,B}
  // from February onwards we believe overlap does not exists anymore
  // but overlap in known-axis during the first ms in february
  val dfMsOverlap: DataFrame = Seq(
    (0, "2019-01-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 00:00:00", "2019-01-01 10:00:00",     "A"),
    (0, "2019-01-01 00:00:00", "2019-02-01 00:00:00", "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B"),
    (0, "2019-02-01 00:00:00", finisTemporisString,   "2019-01-01 00:00:00", "2019-01-01 09:59:59.999", "A"),
    (0, "2019-02-01 00:00:00", finisTemporisString,   "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B")
  ).map(makeRowsBiTemporal)
    .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "img")

  val dfMicrosecTimeRanges: DataFrame = Seq(
    (0, "2022-03-01 00:00:00",        "2022-03-01 09:00:00.000123", "2018-06-01 00:00:00",        "2018-06-01 09:00:00.000123", 3.14),
    (0, "2022-03-01 09:00:00.000124", "2022-03-01 10:00:00.000129", "2018-06-01 09:00:00.000124", "2018-06-01 09:00:00.000129", 42.0),
    (0, "2022-03-01 10:00:00.000130", "2022-03-07 09:00:00.123456", "2018-06-01 09:00:00.000130", "2018-06-01 17:00:00.123456", 2.72)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value")

  val dfRight: DataFrame = List(
    (0, initiumTemporisString, finisTemporisString,   "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, initiumTemporisString, finisTemporisString,   "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0, "2018-01-01 00:00:00", "2018-06-01 00:00:00", "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    (0, "2018-06-01 00:00:00", finisTemporisString,   "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(98.00)),
    (0, "2020-06-01 00:00:00", finisTemporisString,   "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
    (1, initiumTemporisString, finisTemporisString,   "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, initiumTemporisString, finisTemporisString,   "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, initiumTemporisString, finisTemporisString,   "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1, initiumTemporisString, finisTemporisString,   "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsBiTemporal).toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")

  val dfRightDouble: DataFrame = dfRight.withColumn("id", $"id".cast(DoubleType))

  /*
   * "KnownOnly" fixtures: some operations reused from the single-dimensional linear implementation
   * (e.g. rangeExtendRange) only ever act on the first interval dimension sorted by fromColName,
   * which for BiLinearClosedIntervalQueryConfig is the known dimension ("known_from" < "valid_from"
   * alphabetically). These fixtures put the interesting/varying data on the known dimension and pin
   * the valid dimension to a constant, distinctive instant, so that a test can verify both that the
   * known dimension is extended exactly as its uni-temporal counterpart would be, and that the valid
   * dimension is left completely untouched.
   */
  val dfLeftKnownOnly: DataFrame = Seq(
    (0, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", "2020-06-15 00:00:00", "2020-06-15 00:00:00", 4.2)
  ).map(makeRowsBiTemporal)
    .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_l")

  val dfRightKnownOnly: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0, "2020-01-01 00:00:00", finisTemporisString,       Some(97.15)),
    (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map { case (id, kf, kt, v) => (id, kf, kt, "2020-06-15 00:00:00", "2020-06-15 00:00:00", v) }
    .map(makeRowsBiTemporal)
    .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "value_r")

}
