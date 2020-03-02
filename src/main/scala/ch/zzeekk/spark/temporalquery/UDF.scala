package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import TemporalQueryUtil.TemporalQueryConfig
import org.apache.spark.sql.Row

object UDF extends Serializable {
  // "extends Serializable" needed to avoid
  // org.apache.spark.SparkException: Task not serializable

  /**
   * rounds down timestamp tempus to the nearest millisecond
   * if tempus is not before hc.maxDate then return hc.maxDate
   * @param numMillis: number of milliseconds to add
   * @param tempus: timestamp to which the milliseconds are to be added
   * @return rounded down timestamp
   */
  def addMillisecond(numMillis: Int)(tempus: Timestamp)(implicit hc:TemporalQueryConfig) : Timestamp = {
    if (tempus == null) null
    else if (!tempus.before(hc.maxDate)) hc.maxDate
    else if (!tempus.after(hc.minDate)) hc.minDate
    else Timestamp.from(tempus.toInstant.plusMillis(numMillis))
  }
  def udf_plusMillisecond(implicit hc:TemporalQueryConfig): UserDefinedFunction = udf(addMillisecond(1) _)

  /**
   * rounds down timestamp tempus to the nearest millisecond
   * if tempus is not before hc.maxDate then return hc.maxDate
   * @param tempus: timestamp to truncate
   * @return rounded down timestamp
   */
  def floorTimestamp(tempus: Timestamp)(implicit hc:TemporalQueryConfig): Timestamp = {
    // return hc.maxDate in case input tempus is after or equal hc.maxDate
    if (tempus.before(hc.maxDate)) {
      // to start with: resultat = truncate tempus to SECONDS
      val resultat: Timestamp = new Timestamp(1000 * (tempus.getTime / 1000)) // mutable and will be mutated
      resultat.setNanos(1000000 * (tempus.getNanos / 1000000))
      resultat
    }  else hc.maxDate
  }
  def udf_floorTimestamp(implicit hc:TemporalQueryConfig): UserDefinedFunction = udf(floorTimestamp _)

  /**
   * rounds up timestamp to next ChronoUnit
   * but at most up to hc.maxDate
   * @param tempus: timestamp to round up
   * @return truncated timestamp
   */
  def ceilTimestamp(tempus: Timestamp)(implicit hc:TemporalQueryConfig): Timestamp = addMillisecond(1)(predecessorTime(tempus)(hc))
  def udf_ceilTimestamp(implicit hc:TemporalQueryConfig): UserDefinedFunction = udf(ceilTimestamp _)

  /**
   * returns the predecessor timestamp with respect to ChronoUnit
   * @param tempus: timestamp to truncate
   * @return truncated timestamp
   */
  def predecessorTime(tempus: Timestamp)(implicit hc:TemporalQueryConfig): Timestamp = {
    val resultat: Timestamp = floorTimestamp(tempus)(hc)
    if (resultat.equals(tempus)) addMillisecond(-1)(resultat) else resultat
  }
  def udf_predecessorTime(implicit hc:TemporalQueryConfig): UserDefinedFunction = udf(predecessorTime _)


  /**
   * returns the complement of union of subtrahends relative to the interval [validFrom, validTo]
   * Hereby we use A ∖ (⋃ B_i) = A ∖ B₀∖ B₁∖ B₂∖ ...
   * @param validFrom: start of time interval
   * @param validTo: start of time interval
   * @param subtrahends: sequence of which the temporal complement is taken
   * @return [validFrom, validTo] ∖ (⋃ subtrahends)
   */
  def temporalComplement(validFrom: Timestamp, validTo: Timestamp, subtrahends: Seq[Row])(implicit hc:TemporalQueryConfig): List[(Timestamp,Timestamp)] = {
    val subtrahendsSorted: List[(Timestamp, Timestamp)] = subtrahends
      .map(r => (r.getTimestamp(0),r.getTimestamp(1)))
      .sortWith({(x,y) => x._1.before(y._1)})
      .toList
      .filterNot({x => validTo.before(x._1)})
      .filterNot({x => validFrom.after(x._2)})
    println("************************************")
    println(s"temporalComplement: subtrahendsSorted.size=${subtrahendsSorted.size}")
    if (0<subtrahendsSorted.size) {
      println(s"temporalComplement: subtrahendsSorted.head=${subtrahendsSorted.head}")
      println(s"temporalComplement: subtrahendsSorted.last=${subtrahendsSorted.last}")
    }

    def getOneComplement(minuend: (Timestamp, Timestamp), subtrahend: (Timestamp,Timestamp)):  List[(Timestamp,Timestamp)] = {
      List( (addMillisecond(1)(subtrahend._2),minuend._2) , (minuend._1,predecessorTime(subtrahend._1)) )
        .filterNot(x => x._2.before(x._1))
    }

    def subtractOneSubtrahend(res: List[(Timestamp, Timestamp)], subtrahend: (Timestamp,Timestamp)):  List[(Timestamp,Timestamp)] = {
      println(s"subtractOneSubtrahend: res.size = ${res.size}")
      println(s"subtractOneSubtrahend: res.head = ${res.head}")
      println(s"subtractOneSubtrahend: res.last = ${res.last}")
      println(s"subtractOneSubtrahend: subtrahend = $subtrahend")
      getOneComplement(res.head, subtrahend) ++ res.tail
    }

    subtrahendsSorted.foldLeft(List((validFrom, validTo)))(subtractOneSubtrahend)
  }

  def udf_temporalComplement(implicit hc:TemporalQueryConfig): UserDefinedFunction = udf(temporalComplement _)

}
