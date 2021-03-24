package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.sql.Timestamp
import scala.reflect.runtime.universe._

object TemporalHelpers extends Serializable with Logging {
  // "extends Serializable" needed to avoid
  // org.apache.spark.SparkException: Task not serializable

  val millisPerHour: Long = 1000L * 3600
  val millisPerDay: Long = 24 * millisPerHour

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
   * returns the complement of union of subtrahends relative to the interval [validFrom, validTo]
   * Hereby we use A ∖ (⋃ B_i) = A ∖ B₀∖ B₁∖ B₂∖ ...
   * @param validFrom: start of time interval
   * @param validTo: start of time interval
   * @param subtrahends: sequence of which the interval complement is taken
   * @return [validFrom, validTo] ∖ (⋃ subtrahends)
   */
  def intervalComplement[T: Ordering](validFrom: T, validTo: T, subtrahends: Seq[Row], ic: IntervalQueryConfig[T])
                                     (implicit ordering: Ordering[T]): Seq[(T,T)] = {
    logger.debug(s"intervalComplement: START validity = [$validFrom , $validTo]")
    val subtrahendsSorted: List[(T, T)] = subtrahends
      .map(r => (r.getAs[T](0),r.getAs[T](1)))
      .sorted(Ordering.Tuple2(ordering, ordering))
      .filterNot(x => ordering.lt(validTo, x._1))
      .filterNot(x => ordering.gt(validFrom, x._2))
      .toList
    logger.debug(s"intervalComplement: subtrahendsSorted = ${subtrahendsSorted.mkString(" U ")}")

    def getOneComplement(minuend: (T,T), subtrahend: (T,T)):  Seq[(T,T)] = {
      List(
        (ic.intervalDef.successor(subtrahend._2), minuend._2),
        (minuend._1, ic.intervalDef.predecessor(subtrahend._1))
      ).filterNot(x => ordering.lt(x._2, x._1))
    }

    def subtractOneSubtrahend(res: Seq[(T,T)], subtrahend: (T,T)):  Seq[(T,T)] = {
      logger.debug(s"intervalComplement.subtractOneSubtrahend: START subtrahend = $subtrahend")
      getOneComplement(res.head, subtrahend) ++ res.tail
    }

    subtrahendsSorted.foldLeft(Seq((validFrom, validTo)))(subtractOneSubtrahend)
  }
  def getUdfIntervalComplement[T: Ordering: TypeTag](implicit hc:IntervalQueryConfig[T]): UserDefinedFunction = udf(intervalComplement[T] _)

}
