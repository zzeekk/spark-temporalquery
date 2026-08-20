package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval
import ch.zzeekk.spark.temporalquery.util.linear.LinearFloatQueryUtil.LinearHalfOpenIntervalQueryConfig

/**
 * Linear query utils for interval axis of type Float
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearFloatQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiLinearFloatQueryUtil extends BiLinearGenericQueryUtil[Float] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Float] = HalfOpenInterval(Float.NegativeInfinity, Float.PositiveInfinity)

  def drop(bDblQc: BiLinearHalfOpenIntervalQueryConfig, fromColName: String): LinearHalfOpenIntervalQueryConfig = {

    val reducedMap: Map[String, (String, HalfOpenInterval[Float])] = bDblQc.dimensionMap
      .filterNot { case (f, _) => fromColName == f }

    LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap = reducedMap.map { case (f, (t, _)) => (f, t) },
      additionalTechnicalColNames = bDblQc.additionalTechnicalColNames,
      dimLt = bDblQc.dimLt,
      intervalDef = reducedMap.head._2._2
    )

  }

}
