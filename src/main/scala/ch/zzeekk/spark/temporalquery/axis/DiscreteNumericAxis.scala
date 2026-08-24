package ch.zzeekk.spark.temporalquery.axis

import scala.language.implicitConversions

/**
 * Implementation of axis behaviour for discrete time axis any Integral scala type, e.g. Integer,
 * Long,...
 *
 * @param step
 *   step size used for discrete interval axis
 * @tparam T
 *   : scala type for interval axis
 */
case class DiscreteNumericAxis[T](step: T)(implicit f: Integral[T]) extends DiscreteAxisDef[T] {
  implicit private def ops(lhs: T): f.IntegralOps = f.mkNumericOps(lhs)

  override def floor(value: T): T = (value / step) * step // round down to next step

  override def next(value: T): T = f.plus(value, step)

  override def prev(value: T): T = f.minus(value, step)
}
