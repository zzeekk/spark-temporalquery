package ch.zzeekk.spark

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.multivarRange.MultivarRangeQueryConfig
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.slf4j.Logger

import java.io.PrintWriter
import java.sql.Timestamp
import scala.reflect.runtime.universe.TypeTag

package object temporalquery extends Serializable with Logging {

  def saveString2File(fileName: String)(str: String): Unit =
    new PrintWriter(fileName) {
      try write(str)
      finally close()
    }

  val millisPerHour: Long = 1000L * 3600
  val millisPerDay: Long = 24 * millisPerHour

  /**
   * returns the length of the time interval [subtrahend ; minuend] in milliseconds considering
   * switch from winter to daylight saving time in March and October
   *
   * @param minuend:
   *   the end of the time interval
   * @param subtrahend:
   *   the beginning of the time interval
   * @return
   *   number of milliseconds
   */
  def durationInMillis(minuend: Timestamp, subtrahend: Timestamp): Long = {
    require(!(minuend == null || subtrahend == null),
      s"Null values not supported: minuend=$minuend subtrahend=$subtrahend")
    1 + minuend.getTime - subtrahend.getTime
  }
  val udf_durationInMillis: UserDefinedFunction = udf(durationInMillis _)

  // Source - https://stackoverflow.com/a/14740340
  // Posted by Travis Brown
  // Retrieved 2026-07-30, License - CC BY-SA 3.0

  implicit class Crossable[X](xs: Set[X]) {
    def cross[Y](ys: Set[Y]): Set[(X, Y)] = xs.flatMap(x => ys.map(y => (x, y)))
  }

  /**
   * returns the complement of union of subtrahends relative to the interval [validFrom, validTo]
   * Hereby we use A ∖ (⋃ B_i) = A ∖ B₀∖ B₁∖ B₂∖ ...
   * @param validFrom:
   *   start of time interval
   * @param validTo:
   *   start of time interval
   * @param subtrahends:
   *   sequence of which the interval complement is taken
   * @return
   *   [validFrom, validTo] ∖ (⋃ subtrahends)
   */
  @deprecated("simply wrong in multi-dimension case")
  def rangeComplement[T: Ordering](validFrom: T, validTo: T, subtrahends: Seq[Row])(implicit
      ordering: Ordering[T],
      mrqc: MultivarRangeQueryConfig[T, ClosedInterval[T]],
      logger: Logger
  ): Seq[(T, T)] = {
    debugLog(s"(rangeComplement) START validity = [$validFrom , $validTo] ; ${subtrahends.length} subtrahends")
    val subtrahendsSorted = subtrahends
      .map(r => (r.getAs[T](0), r.getAs[T](1)))
      .filterNot(x => ordering.lt(validTo, x._1))
      .filterNot(x => ordering.gt(validFrom, x._2))
      .sorted(Ordering.Tuple2(ordering, ordering))
      .toList
    debugLog(s"(rangeComplement) ${subtrahendsSorted.length} subtrahendsSorted = ${subtrahendsSorted.mkString(" U ")}")

    def subtractOneSubtrahend(minuends: Seq[(T, T)], subtrahend: (T, T)): Seq[(T, T)] = {
      val res = mrqc.intervalDef.complement(minuends.head)(subtrahend) ++ minuends.tail
      debugLog(s"(rangeComplement.subtractOneSubtrahend) minuends = ${minuends.mkString(",")} ;" +
        s" subtrahend = $subtrahend ; res = ${res.mkString(",")}")
      res
    }

    subtrahendsSorted.foldLeft(Seq((validFrom, validTo)))(subtractOneSubtrahend)
  }

}
