package ch.zzeekk.spark.temporalquery

import org.apache.spark.SparkContext
import org.apache.spark.sql.{Dataset, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

import java.lang.management.{ManagementFactory, MemoryUsage}
import scala.util.{Failure, Success, Try}

trait Logging extends Serializable {
  @transient protected lazy val logger: Logger = LoggerFactory.getLogger(getClass.getName)

  private var _loggEnvDone: Boolean = false

  protected def getMemoryUsage: String = {
    def getMem(numBytes: Long): String = s"${math.round(numBytes.toDouble / math.pow(2, 20))} MiB"
    lazy val memUsage = ManagementFactory.getMemoryMXBean.getHeapMemoryUsage
    s"init = ${getMem(memUsage.getInit)} | used = ${getMem(memUsage.getUsed)} |" +
      s" committed = ${getMem(memUsage.getCommitted)} | max = ${getMem(memUsage.getMax)}"
  }

  protected def loggEnv(implicit session: SparkSession, logger: Logger): Unit =
    if (!_loggEnvDone) {
      val sparkConfSettings = List(
        "spark.driver.host",
        "spark.driver.port",
        "spark.driver.cores",
        "spark.driver.maxResultSize",
        "spark.driver.memory",
        "spark.dynamicAllocation.enabled",
        "spark.dynamicAllocation.executorAllocationRatio",
        "spark.dynamicAllocation.executorIdleTimeout",
        "spark.dynamicAllocation.maxExecutors",
        "spark.dynamicAllocation.minExecutors",
        "spark.executor.cores",
        "spark.executor.memory",
        "spark.executor.memoryOverhead",
        "spark.sql.maxPlanStringLength"
      )
      val runtimeConfigSettings = List(
        "spark.sql.hive.filesourcePartitionFileCacheSize",
        "spark.sql.hive.version",
        "spark.sql.mapKeyDedupPolicy",
        "spark.sql.optimizer.maxIterations",
        "spark.shuffle.file.buffer",
        "spark.sql.maxPlanStringLength",
        "spark.sql.shuffle.partitions",
        "spark.sql.warehouse.dir"
      )

      import session.implicits._
      val javaVersion: String = System.getProperty("java.version")
      val scalaVersion: String = scala.util.Properties.versionString
      val sparkContext: SparkContext = session.sparkContext

      def getSparkConfSetting(propName: String): Option[String] = sparkContext.getConf.getOption(propName)

      def getRuntimeConfigSetting(propName: String): Option[String] = session.conf.getOption(propName)

      logger.info(s"logger.isDebugEnabled ? ${logger.isDebugEnabled()}")
      logger.info(s"Java  Version : $javaVersion")
      logger.info(s"Java  Memory  : $getMemoryUsage")
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

  // use debugLog because logger.debug is too eager, i.e. its argument is evaluated even if not logger.isDebugEnabled
  // For this end we have to declare msg as lazy arguments using =>
  def debugLog(msg: => String)(implicit logger: Logger): Unit = if (logger.isDebugEnabled) logger.debug(msg)

  def debLogFun(msg: => String)(implicit logger: Logger): Unit =
    if (logger.isDebugEnabled) debugLog(msg) else logger.info(msg)

  implicit class DsQuality[T](ds: Dataset[T]) {

    /**
     * debLog shows some detailed information which can be used for debugging
     * @param dsName
     *   Name of dataset
     * @param showRows
     *   shall some content be shown? Use with care! If set to true confidential data may be written
     *   to log files in clear text!
     * @param logger
     *   your messenger
     */
    def debLog(dsName: String, showRows: Boolean = false)(implicit logger: Logger): Unit = {
      debugLog(s"$dsName.schema (${ds.columns.length} columns): ${ds.schema.catalogString}")
      debugLog(s"$dsName: number of partitions = ${ds.rdd.getNumPartitions}")
      val cntRows = ds.count()
      val cntDistinctRows = Try(ds.distinct().count()) match {
        case Success(n) => n
        case Failure(e) =>
          logger.warn(s"debaLog($dsName): could not count distinct rows of $dsName")
          logger.warn(e.getMessage)
          logger.warn("debLog($dsName): ignoring this problem and returning -1")
          -1L
      }
      if (cntRows != cntDistinctRows) logger.warn(s"DataFrame $dsName has duplicates !")
      debLogFun(s"$dsName.count() = $cntRows") // may take a long time
      debLogFun(s"$dsName.distinct().count() = $cntDistinctRows") // may take even much longer time
      if (showRows) ds.orderBy(ds.columns.map(org.apache.spark.sql.functions.col): _*).show(32, truncate = false)
    }

    /**
     * createdLog shows a success message that your dataset has been created
     * @param dsName
     *   Name of dataset
     * @param debug
     *   shall debLog be called. If set to true this may take some time.
     * @param showRows
     *   shall some content be shown in case of debug? Use with care! If set to true confidential
     *   data may be written to log files in clear text!
     * @param logger
     *   your messenger
     */
    def createdLog(dsName: String, debug: Option[Boolean] = None, showRows: Boolean = false)(implicit logger: Logger): Unit = {
      logger.info(s"DataSet $dsName created :)")
      logger.info(s"$dsName.schema: ${ds.schema.catalogString}")
      if (debug.getOrElse(logger.isDebugEnabled)) debLog(dsName, showRows)
    }

  }
}
