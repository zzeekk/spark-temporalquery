package ch.zzeekk.spark.temporalquery.axis

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

/**
 * Implementation of axis behaviour for discrete time axis using Timestamp as scala axis type
 *
 * @param timeUnit
 *   time unit used as step for discrete time axis
 */
case class DiscreteTimeAxis(timeUnit: ChronoUnit) extends DiscreteAxisDef[Timestamp] {
  override def floor(value: Timestamp): Timestamp = Timestamp.valueOf(value.toLocalDateTime.truncatedTo(timeUnit))

  override def next(value: Timestamp): Timestamp = Timestamp.from(value.toInstant.plus(1, timeUnit))

  override def prev(value: Timestamp): Timestamp = Timestamp.from(value.toInstant.minus(1, timeUnit))
}
