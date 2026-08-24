package ch.zzeekk.spark.temporalquery.axis

/**
 * Trait to describe axis behaviour for discrete Axis
 *
 * @tparam T
 *   : scala type for interval axis
 */
abstract class DiscreteAxisDef[T] {
  def floor(value: T): T

  def next(value: T): T

  def prev(value: T): T

  def ceil(value: T): T = {
    // round down step and eventually add one step
    val valueFloor: T = floor(value)
    if (valueFloor == value) value
    else next(valueFloor)
  }

  def predecessor(value: T): T = {
    // round down step and eventually remove one step
    val valueFloored: T = floor(value)
    if (valueFloored == value) prev(valueFloored)
    else valueFloored
  }

  def successor(value: T): T =
    // round down step and add one step
    next(floor(value))
}
