package ch.zzeekk.spark.temporalquery

import ch.zzeekk.spark.temporalquery.axis.DiscreteTimeAxis
import ch.zzeekk.spark.temporalquery.interval.ClosedInterval

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

package object util {

  // The time begins with bigBangDay: What happened before cannot be known as there is no before.
  val bigBangDay: Timestamp = Timestamp.valueOf("1970-01-01 00:00:00")

  // The time ends with doomsDay: What will happen afterwards cannot be known as there is no afterwards.
  val doomsDay: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")

  val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a, b) => a.before(b))

  val stdClosedTemporalInterval: ClosedInterval[Timestamp] = ClosedInterval(
    lowerHorizon = bigBangDay,
    upperHorizon = doomsDay,
    discreteAxisDef = DiscreteTimeAxis(ChronoUnit.MILLIS)
  )

}
