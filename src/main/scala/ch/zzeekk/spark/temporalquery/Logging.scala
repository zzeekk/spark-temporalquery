package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  protected var _loggEnvDone: Boolean = false

  protected def loggEnv(implicit session: SparkSession): Unit = {
    if (!_loggEnvDone) {
      val javaVersion: String = System.getProperty("java.version")
      val scalaVersion: String = scala.util.Properties.versionString
      val sparkVersion = session.sparkContext.version

      logger.info(s"logger.isDebugEnabled ? ${logger.isDebugEnabled()}")
      logger.info(s"Java  Version : $javaVersion")
      logger.info(s"Scala Version : $scalaVersion")
      logger.info(s"Spark Version : $sparkVersion")

      _loggEnvDone = true
    }
  }

}