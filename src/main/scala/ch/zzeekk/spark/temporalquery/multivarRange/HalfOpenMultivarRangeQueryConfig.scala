package ch.zzeekk.spark.temporalquery.multivarRange

import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval
import org.apache.spark.sql.Column

abstract class HalfOpenMultivarRangeQueryConfig[T: Ordering] extends MultivarRangeQueryConfig[T, HalfOpenInterval[T]]
