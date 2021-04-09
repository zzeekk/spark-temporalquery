package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.TemporalQueryUtil.TemporalQueryConfig
import org.apache.spark.sql.DataFrame

object TemporalTestUtils extends TestUtils {

  import session.implicits._

  implicit val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()
  val initiumTemporisString: String = defaultConfig.minDate.toString
  val finisTemporisString: String = defaultConfig.maxDate.toString

  // some beautiful nasty data frames for testing

  val dfLeft: DataFrame = Seq((0, "2017-12-10 00:00:00", "2018-12-08 23:59:59.999", 4.2))
    .map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")

  val dfRight: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
    (1, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")

  val dfRightDouble: DataFrame = Seq(
    (0.0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", Some(97.15)),
    // gap in history
    (0.0, "2018-06-01 05:24:11", "2018-10-23 03:50:09.999", Some(97.15)),
    (0.0, "2018-10-23 03:50:10", "2019-12-31 23:59:59.999", Some(97.15)),
    (0.0, "2020-01-01 00:00:00", finisTemporisString      , Some(97.15)),
    (1.0, "2018-01-01 00:00:00", "2018-12-31 23:59:59.999", None),
    (1.0, "2019-01-01 00:00:00", "2019-12-31 23:59:59.999", Some(2019.0)),
    (1.0, "2020-01-01 00:00:00", "2020-12-31 23:59:59.999", Some(2020.0)),
    (1.0, "2021-01-01 00:00:00", "2099-12-31 23:59:59.999", None)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")

  /*
    * dfMap: dfMap which maps a set of images img to id over time:
    * e.g. 0 ↦ {A,B} in Jan 2018 ; 0 ↦ {B,C} 1.-19. Feb 2018 ; 0 ↦ {B,C,D} 20.-28. Feb 2018 ausser für eine 1ms mit 0 ↦ {B,C,D,X} ; 0 ↦ {D} in Mar 2018
  */
  val dfMap: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-01-31 23:59:59.999", "A"),
    (0, "2018-01-01 00:00:00", "2018-02-28 23:59:59.999", "B"),
    (0, "2018-02-01 00:00:00", "2018-02-28 23:59:59.999", "C"),
    (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", "D"),
    (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", "X")
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMapToCombine: DataFrame = Seq(
    (0, "2018-01-01 00:00:00", "2018-03-31 23:59:59.999", Some("A")),
    (0, "2018-04-01 00:00:00", "2018-07-31 23:59:59.999", Some("A")),
    (0, "2018-08-01 00:00:00", "2018-08-31 23:59:59.999", Some("A")),
    (0, "2018-09-01 00:00:00", "2018-12-31 23:59:59.999", Some("A")),
    (0, "2018-01-01 00:00:00", "2018-01-15 23:59:59.999", Some("B")),
    (0, "2018-01-16 00:00:00", "2018-02-03 23:59:59.999", Some("B")),
    (0, "2018-02-01 00:00:00", "2020-02-29 23:59:59.999", None),
    (0, "2020-03-01 00:00:00", "2020-04-30 23:59:59.999", None),
    (0, "2020-06-01 00:00:00", "2020-12-31 23:59:59.999", None),
    (1, "2018-02-01 00:00:00", "2020-02-29 23:59:59.999", Some("one")),
    (1, "2020-03-01 00:00:00", "2020-04-30 23:59:59.999", Some("one")),
    (1, "2020-06-01 00:00:00", "2020-12-31 23:59:59.999", Some("one")),
    (0, "2018-02-20 00:00:00", "2018-03-31 23:59:59.999", Some("D")),
    (0, "2018-02-25 14:15:16.123", "2018-02-25 14:15:16.123", Some("X"))
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMoment: DataFrame = Seq((0, "2019-11-25 11:12:13.005", "2019-11-25 11:12:13.005", "A"))
    .map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // for 1ms, namely [10:00:00.000,10:00:00.001[, the image set is {A,B}
  val dfMsOverlap: DataFrame = Seq(
    (0, "2019-01-01 00:00:00", "2019-01-01 10:00:00", "A"),
    (0, "2019-01-01 10:00:00", "2019-01-01 23:59:59.999", "B")
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // Data Frame dfDirtyTimeRanges
  val dfMicrosecTimeRanges: DataFrame = Seq(
    (0,"2018-06-01 00:00:00"       ,"2018-06-01 09:00:00.000123", 3.14),
    (0,"2018-06-01 09:00:00.000124","2018-06-01 09:00:00.000129",42.0),
    (0,"2018-06-01 09:00:00.000130","2018-06-01 17:00:00.123456", 2.72)
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  // Data Frame dfDirtyTimeRanges
  val dfDirtyTimeRanges: DataFrame = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245"     , 2.72), // overlaps with previous record
    (0,"2019-03-01 00:00:00.0"        ,"2019-02-01 00:00:00.0"        , 2.72),
    (0,"2019-02-01 02:34:56.1236"     ,"2019-02-01 02:34:56.1235"     ,42.0 ), // ends before it starts
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,13.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ), // duration less than a millisecond without touching bounds of millisecond intervall
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    // id = 1
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:0"           ,-1.0 ),
    (1,"2019-03-01 00:00:0"           ,"2019-03-01 00:00:00.0001"     , 0.1 ), // duration less than a millisecond
    (1,"2019-03-01 00:00:0.0009"      ,"2019-03-01 00:00:00.001"      , 0.1 ), // duration less than a millisecond, overlaps with previous record
    (1,"2019-03-01 00:00:1.0009"      ,"2019-03-01 00:00:01.0021"     , 1.2 ), // duration less than a millisecond
    (1,"2019-03-01 00:00:0.0001"      ,"2019-03-01 00:00:00.0009"     , 0.8 ), // duration less than a millisecond
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 )
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val dfDocumentation: DataFrame = Seq(
    (1,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235", 2.72),
    (1,"2019-02-01 01:00:00.0"        ,"2019-02-01 02:34:56.1245", 2.72), // overlaps with previous record
    (1,"2019-02-01 02:34:56.125"      ,"2019-02-01 02:34:56.1245", 2.72), // ends before it starts
    (1,"2019-01-01 00:00:0"           ,"2019-12-31 23:59:59.999" ,42.0 )
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val dfContinuousTime: DataFrame = Seq(
    (0,"2019-01-01 00:00:00.123456789","2019-01-05 12:34:56.123456789", 3.14),
    (0,"2019-01-05 12:34:56.123456789","2019-02-01 02:34:56.1235"     , 2.72),
    (0,"2019-02-01 02:34:56.1235"     ,"2019-02-01 02:34:56.1245"     ,42.0 ),
    (0,"2019-02-01 02:34:56.1245"     ,"2019-03-03 00:00:0"           ,13.0 ),
    (0,"2019-03-03 00:00:0"           ,"2019-04-04 00:00:0"           ,12.0 ),
    (0,"2019-09-05 02:34:56.1231"     ,"2019-09-05 02:34:56.1239"     ,42.0 ),
    (0,"2020-01-01 01:00:0"           ,"9999-12-31 23:59:59.999999999",18.17),
    (1,"2019-01-01 00:00:0.123456789" ,"2019-02-02 00:00:00"          ,-1.0 ),
    (1,"2019-03-03 01:00:0"           ,"2021-12-01 02:34:56.1"        ,-2.0 )
  ).map(makeRowsWithTimeRange).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")


}
