package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import scala.reflect.runtime.universe._

object TemporalHelpers extends Serializable with Logging {
  // "extends Serializable" needed to avoid
  // org.apache.spark.SparkException: Task not serializable

  val millisPerHour: Long = 1000L * 3600
  val millisPerDay: Long = 24 * millisPerHour

  /**
   * Add time units to timestamp respecting min/maxDate
    */
  def addTimeUnits(tempus: Timestamp, amount: Int, unit: ChronoUnit, minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    if (tempus == null) null
    earliest(latest(Timestamp.from(tempus.toInstant.plus(amount, unit)), minDate), maxDate)
  }

  implicit private val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a,b) => a.before(b))
  private def latest(tempis: Timestamp*): Timestamp = tempis.max
  private def earliest(tempis: Timestamp*): Timestamp = tempis.min

  /**
   * returns the length of the time interval [subtrahend ; minuend] in milliseconds
   * considering switch from winter to daylight saving time in March and Octobre
   *
   * @param minuend: the end of the time interval
   * @param subtrahend: the beginning of the time interval
   * @return number of milliseconds
   */
  def durationInMillis(minuend: Timestamp, subtrahend: Timestamp): Long = {
    require(!(minuend==null || subtrahend==null),
      s"Null values not supported: minuend=$minuend subtrahend=$subtrahend")
    1 + minuend.getTime - subtrahend.getTime
  }
  val udf_durationInMillis: UserDefinedFunction = udf(durationInMillis _)

  /**
   * rounds down timestamp tempus to the next ChronoUnit
   * if tempus is not before hc.maxDate then return hc.maxDate
   * @param tempus: timestamp to truncate
   * @param unit: chrono unit for time step
   * @param maxDate: maximum of time axis
   * @return rounded down timestamp
   */
  def floorTimestamp(tempus: Timestamp, unit: ChronoUnit = ChronoUnit.MILLIS, minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    // return hc.maxDate in case input tempus is after or equal hc.maxDate
    if (tempus == null) null
    earliest(Timestamp.from(tempus.toInstant.truncatedTo(unit)), maxDate)
  }

  /**
   * rounds up timestamp to next ChronoUnit
   * but at most up to hc.maxDate
   * @param tempus: timestamp to round up
   * @param unit: chrono unit for time step
   * @return truncated timestamp
   */
  def ceilTimestamp(tempus: Timestamp, unit: ChronoUnit = ChronoUnit.MILLIS, minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    if (tempus == null) null
    addTimeUnits(predecessorTime(tempus, unit, minDate, maxDate), 1, unit, minDate, maxDate)
  }

  /**
   * returns the predecessor timestamp with respect to ChronoUnit
   * @param tempus: timestamp to manipulate
   * @param unit: chrono unit for time step
   * @return predecessor timestamp
   */
  def predecessorTime(tempus: Timestamp, unit: ChronoUnit = ChronoUnit.MILLIS, minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    if (tempus == null) null
    else {
      val tempusFloored = floorTimestamp(tempus, unit, minDate, maxDate)
      if (tempusFloored.equals(tempus)) addTimeUnits(tempusFloored, -1, unit, minDate, maxDate)
      else tempusFloored
    }
  }

  /**
   * returns the predecessor timestamp with respect to ChronoUnit
   * @param tempus: timestamp to manipulate
   * @param unit: chrono unit for time step
   * @return successor timestamp
   */
  def successorTime(tempus: Timestamp, unit: ChronoUnit = ChronoUnit.MILLIS, minDate: Timestamp, maxDate: Timestamp): Timestamp = {
    if (tempus == null) null
    else {
      val tempusCeiled = ceilTimestamp(tempus, unit, minDate, maxDate)
      if (tempusCeiled.equals(tempus)) addTimeUnits(tempusCeiled, 1, unit, minDate, maxDate)
      else tempusCeiled
    }
  }

  /**
   * returns the complement of union of subtrahends relative to the interval [validFrom, validTo]
   * Hereby we use A ∖ (⋃ B_i) = A ∖ B₀∖ B₁∖ B₂∖ ...
   * @param validFrom: start of time interval
   * @param validTo: start of time interval
   * @param subtrahends: sequence of which the temporal complement is taken
   * @return [validFrom, validTo] ∖ (⋃ subtrahends)
   */
  def temporalComplement[T: Ordering](validFrom: T, validTo: T, subtrahends: Seq[Row], ic: IntervalQueryConfig[T])
                           (implicit ordering: Ordering[T]): Seq[(T,T)] = {
    logger.debug(s"temporalComplement: START validity = [$validFrom , $validTo]")
    val subtrahendsSorted: List[(T, T)] = subtrahends
      .map(r => (r.getAs[T](0),r.getAs[T](1)))
      .sorted(Ordering.Tuple2(ordering, ordering))
      .filterNot(x => ordering.lt(validTo, x._1))
      .filterNot(x => ordering.gt(validFrom, x._2))
      .toList
    logger.debug(s"temporalComplement: subtrahendsSorted = ${subtrahendsSorted.mkString(" U ")}")

    def getOneComplement(minuend: (T,T), subtrahend: (T,T)):  Seq[(T,T)] = {
      List(
        (ic.intervalDef.successor(subtrahend._2, ic), minuend._2),
        (minuend._1, ic.intervalDef.predecessor(subtrahend._1, ic))
      ).filterNot(x => ordering.lt(x._2, x._1))
    }

    def subtractOneSubtrahend(res: Seq[(T,T)], subtrahend: (T,T)):  Seq[(T,T)] = {
      logger.debug(s"temporalComplement.subtractOneSubtrahend: START subtrahend = $subtrahend")
      getOneComplement(res.head, subtrahend) ++ res.tail
    }

    subtrahendsSorted.foldLeft(Seq((validFrom, validTo)))(subtractOneSubtrahend)
  }
  def getUdfTemporalComplement[T: Ordering: TypeTag](implicit hc:IntervalQueryConfig[T]): UserDefinedFunction = udf(temporalComplement[T] _)

}
