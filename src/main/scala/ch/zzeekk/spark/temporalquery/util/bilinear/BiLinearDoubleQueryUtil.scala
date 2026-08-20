package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval
import ch.zzeekk.spark.temporalquery.util.linear.LinearDoubleQueryUtil.LinearHalfOpenIntervalQueryConfig

/**
 * Linear query utils for interval axis of type Double
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearDoubleQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiLinearDoubleQueryUtil extends BiLinearGenericQueryUtil[Double] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Double] = HalfOpenInterval(Double.NegativeInfinity, Double.PositiveInfinity)

  def drop(bDblQc: BiLinearHalfOpenIntervalQueryConfig, fromColName: String): LinearHalfOpenIntervalQueryConfig = {

    val reducedMap: Map[String, (String, HalfOpenInterval[Double])] = bDblQc.dimensionMap
      .filterNot { case (f, _) => fromColName == f }

    LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap = reducedMap.map { case (f, (t, _)) => (f, t) },
      additionalTechnicalColNames = bDblQc.additionalTechnicalColNames,
      dimLt = bDblQc.dimLt,
      intervalDef = reducedMap.head._2._2
    )

  }

}
