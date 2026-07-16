package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.HalfOpenInterval

/**
 * Linear query utils for interval axis of type Double
 *
 * Usage: import ch.zzeekk.spark.temporalquery.LinearDoubleQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object LinearDoubleQueryUtil extends LinearGenericQueryUtil[Double] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Double] = HalfOpenInterval(Double.NegativeInfinity, Double.PositiveInfinity)
}
