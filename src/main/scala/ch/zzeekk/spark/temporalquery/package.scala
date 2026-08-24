package ch.zzeekk.spark

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf

import java.io.PrintWriter
import java.sql.Timestamp

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

  implicit class Crossable[X](xs: Set[X]) {
    def cross[Y](ys: Set[Y]): Set[(X, Y)] = xs.flatMap(x => ys.map(y => (x, y)))
  }

}
