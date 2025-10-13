package ch.zzeekk.spark.temporalquery

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.slf4j.{Logger, LoggerFactory}

trait Logging {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var _loggEnvDone: Boolean = false

  protected def loggEnv(implicit session: SparkSession, logger: Logger): Unit = {
    if (!_loggEnvDone) {
      val sparkConfSettings = List("spark.driver.host", "spark.driver.port", "spark.driver.cores",
        "spark.driver.maxResultSize", "spark.driver.memory",
        "spark.dynamicAllocation.enabled", "spark.dynamicAllocation.executorAllocationRatio",
        "spark.dynamicAllocation.executorIdleTimeout", "spark.dynamicAllocation.maxExecutors",
        "spark.dynamicAllocation.minExecutors", "spark.executor.cores", "spark.executor.memory",
        "spark.executor.memoryOverhead", "spark.sql.maxPlanStringLength")
      val runtimeConfigSettings = List("spark.sql.hive.filesourcePartitionFileCacheSize",
        "spark.sql.hive.version", "spark.sql.mapKeyDedupPolicy",
        "spark.sql.optimizer.maxIterations", "spark.shuffle.file.buffer",
        "spark.sql.maxPlanStringLength", "spark.sql.shuffle.partitions",
        "spark.sql.warehouse.dir")

      import session.implicits._
      val javaVersion: String = System.getProperty("java.version")
      val scalaVersion: String = scala.util.Properties.versionString
      val sparkContext: SparkContext = session.sparkContext

      def getSparkConfSetting(propName: String): Option[String] = sparkContext.getConf.getOption(propName)

      def getRuntimeConfigSetting(propName: String): Option[String] = session.conf.getOption(propName)

      logger.info(s"logger.isDebugEnabled ? ${logger.isDebugEnabled()}")
      logger.info(s"Java  Version : $javaVersion")
      logger.info(s"Java  Command : ${System.getProperty("sun.java.command")}")
      logger.info(s"Java TimeZone : ${java.util.TimeZone.getDefault.getDisplayName()}")
      logger.info(s"Scala Version : $scalaVersion")
      logger.info(s"Spark Version : ${sparkContext.version}")
      logger.info(s"Spark AppId   : ${sparkContext.getConf.getAppId}")

      logger.info("Spark Conf Settings :")
      sparkConfSettings
        .map(k => (k, getSparkConfSetting(k))).toDF("spark_conf", "value")
        .orderBy("spark_conf").show(false)

      logger.info("Runtime Config Settings :")
      runtimeConfigSettings
        .map(k => (k, getRuntimeConfigSetting(k))).toDF("runtime_config", "value")
        .orderBy("runtime_config").show(false)

      logger.info(s"Documentation: https://spark.apache.org/docs/${sparkContext.version}/configuration.html")
      _loggEnvDone = true
    }
  }

}