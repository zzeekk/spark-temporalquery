package ch.zzeekk.spark.temporalquery

import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)
}