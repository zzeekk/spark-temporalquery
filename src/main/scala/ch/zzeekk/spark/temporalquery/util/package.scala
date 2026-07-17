package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

package object util {

  // The time begins with bigBangDay: What happend before cannot be known as there is no before.
  val bigBangDay: Timestamp = Timestamp.valueOf("1970-01-01 00:00:00")

  // The time ends with doomsDay: What will happen afterwards cannot be known as there is no afterwards.
  val doomsDay: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")

}
