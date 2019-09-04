package ch.zzeekk.spark.temporalquery

class TemporalQueryUtilTest extends org.scalatest.FunSuite {
  import TemporalQueryUtil._
  import java.sql.Timestamp

  val defaultConfig: TemporalQueryConfig = TemporalQueryConfig()

  test("plusMillisecond") {
    val argument: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual: Timestamp = plusMillisecond(argument)(defaultConfig)
    val expected: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00.001")
    assert(actual==expected)
  }

  test("minusMillisecond") {
    val argument: Timestamp = Timestamp.valueOf("2019-09-01 14:00:00")
    val actual: Timestamp = minusMillisecond(argument)(defaultConfig)
    val expected: Timestamp = Timestamp.valueOf("2019-09-01 13:59:59.999")
    assert(actual==expected)
  }

}
