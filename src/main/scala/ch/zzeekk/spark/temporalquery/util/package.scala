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

  /**
   * Converts any value to a Double if possible
   *
   * @param v
   *   the value to be converted
   * @return
   *   a double or an exception
   */
  def anyToDouble(v: Any): Double = v match {
    case x: java.sql.Timestamp    => x.getTime.toDouble
    case x: java.util.Date        => x.getTime.toDouble
    case x: java.lang.Integer     => x.toDouble
    case x: java.lang.Long        => x.toDouble
    case x: java.lang.Double      => x
    case x: java.lang.Float       => x.toDouble
    case x: java.math.BigDecimal  => x.doubleValue()
    case x: scala.math.BigDecimal => x.toDouble
    case other                    => throw new IllegalArgumentException(
        s"anyToDouble: Cannot convert v=$v of ${other.getClass.getName} to Double"
      )
  }

}
