package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.DataFrame
import ch.zzeekk.spark.temporalquery.LinearDoubleQueryUtil._

object LinearDoubleTestUtils extends TestUtils {

  import session.implicits._

  implicit val defaultConfig: LinearHalfOpenIntervalQueryConfig = LinearHalfOpenIntervalQueryConfig(intervalDef = HalfOpenInterval(0d, Double.MaxValue))
  val intervalMinValue: Double = defaultConfig.lowerBound
  val intervalMaxValue: Double = defaultConfig.upperBound
  
  // some beautiful nasty data frames for testing

  val dfLeft: DataFrame = Seq((0,  171210.000000,  181209.0, 4.2))
    .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_l")

  val dfRight: DataFrame = Seq(
    (0,  180101.000000,  180201.0, Some(97.15)),
    // gap in history
    (0,  180601.052411,  181023.035010, Some(97.15)),
    (0,  181023.035010,  200101.0     , Some(97.15)),
    (0,  200101.000000,  intervalMaxValue, Some(97.15)),
    (1,  180101.000000,  190101.0     , None),
    (1,  190101.000000,  200101.0     , Some(2019.0)),
    (1,  200101.000000,  210101.0     , Some(2020.0)),
    (1,  210101.000000,  intervalMaxValue, None)
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert_r")

  /*
    * dfMap: dfMap which maps a set of images img to id over time:
    * e.g. 0 ↦ {A,B} in Jan 2018 ; 0 ↦ {B,C} 1.-19. Feb 2018 ; 0 ↦ {B,C,D} 20.-28. Feb 2018 ausser für eine 1ms mit 0 ↦ {B,C,D,X} ; 0 ↦ {D} in Mar 2018
  */
  val dfMap: DataFrame = Seq(
    (0,  180101.000000,     180201.0, "A"),
    (0,  180101.000000,     180301.0, "B"),
    (0,  180201.000000,     180301.0, "C"),
    (0,  180220.000000,     180401.0, "D"),
    (0,  180225.141516123,  180225.141516123, "X")
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMapToCombine: DataFrame = Seq(
    (0,  180101.000000,  180401.0, Some("A")),
    (0,  180401.000000,  180801.0, Some("A")),
    (0,  180801.000000,  180901.0, Some("A")),
    (0,  180901.000000,  190101.0, Some("A")),
    (0,  180101.000000,  180116.0, Some("B")),
    (0,  180116.000000,  180204.0, Some("B")),
    (0,  180201.000000,  200301.0, None),
    (0,  200301.000000,  200501.0, None),
    (0,  200601.000000,  210101.0, None),
    (1,  180201.000000,  200301.0, Some("one")),
    (1,  200301.000000,  200501.0, Some("one")),
    (1,  200601.000000,  210101.0, Some("one")),
    (0,  180220.000000,  180401.0, Some("D")),
    (0,  180225.141516123,  180225.141516123, Some("X"))
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  val dfMoment: DataFrame = Seq((0,  191125.111213005,  191125.111213005, "A"))
    .toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // for 0.000001 the image set is {A,B}
  val dfSmallOverlap: DataFrame = Seq(
    (0,  190101.000000,  190101.100001, "A"),
    (0,  190101.100000,  190102.0, "B")
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"img")

  // Data Frame dfDirtyIntervals
  val dfDirtyIntervals: DataFrame = Seq(
    (0, 190101.000000123456789, 190105.123456123456789, 3.14),
    (0, 190105.123456123456789, 190201.0234561235     , 2.72),
    (0, 190201.0100000        , 190201.0234561245     , 2.72), // overlaps with previous record
    (0, 190201.0000000        , 190201.0000000        , 2.72),
    (0, 190201.0234561236     , 190201.0234561236     ,42.0 ), // ends before it starts
    (0, 190201.0234561245     , 190303.000000         ,13.0 ),
    (0, 190303.000000         , 190404.000000         ,13.0 ),
    (0, 190905.0234561231     , 190905.0234561239     ,42.0 ), // small duration
    (0, 200101.010000         , intervalMaxValue      ,18.17),
    // id = 1
    (1, 190101.000000123456789 , 190202.000000         ,-1.0 ),
    (1, 190301.000000          , 190301.0000000002     , 0.1 ), // small duration
    (1, 190301.0000000009      , 190301.000000002      , 0.1 ), // small duration, overlaps with previous record
    (1, 190301.0000010009      , 190301.0000010021     , 1.2 ), // small duration
    (1, 190301.0000000001      , 190301.000000001     , 0.8 ), // small duration
    (1, 190303.010000          , 211201.0234561        ,-2.0 )
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

  val dfDocumentation: DataFrame = Seq(
    (1, 190105.123456123456789, 190201.0234561235, 2.72),
    (1, 190201.0100000        , 190201.0234561245, 2.72), // overlaps with previous record
    (1, 190201.023456125      , 190201.0234561245, 2.72), // ends before it starts
    (1, 190101.000000         , 200101.0         ,42.0 )
  ).toDF("id", defaultConfig.fromColName, defaultConfig.toColName,"wert")

}
