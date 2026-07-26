package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval
import ch.zzeekk.spark.temporalquery.util.stdHalfOpenTemporalInterval

import java.sql.Timestamp

/**
 * Linear query utils for interval axis of type Double
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearDoubleQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiTemporalHalfOpenIntervalQueryUtil extends BiLinearGenericQueryUtil[Timestamp] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Timestamp] = stdHalfOpenTemporalInterval
}
