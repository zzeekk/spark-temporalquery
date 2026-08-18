package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.util.stdClosedTemporalInterval

import java.sql.Timestamp

/**
 * Linear query utils for interval axis of type Timestamp
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearTimestampQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiTemporalClosedIntervalQueryUtil extends BiLinearGenericQueryUtil[Timestamp] {
  implicit val defaultClosedIntervalDef: ClosedInterval[Timestamp] = stdClosedTemporalInterval
}
