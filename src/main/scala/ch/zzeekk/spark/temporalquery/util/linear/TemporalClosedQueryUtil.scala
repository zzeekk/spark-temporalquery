package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.util.stdClosedTemporalInterval

import java.sql.Timestamp

/**
 * Linear query utils for interval axis of type Timestamp
 *
 * Usage: import ch.zzeekk.spark.temporalquery.LinearTimestampQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object TemporalClosedQueryUtil extends LinearGenericQueryUtil[Timestamp] {
  implicit val defaultClosedIntervalDef: ClosedInterval[Timestamp] = stdClosedTemporalInterval
}
