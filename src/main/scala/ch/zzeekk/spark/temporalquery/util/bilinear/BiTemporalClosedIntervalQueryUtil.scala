package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.multivarRange.MultivarRangeQueryImpl
import ch.zzeekk.spark.temporalquery.util.linear.TemporalClosedQueryUtil.LinearClosedIntervalQueryConfig
import ch.zzeekk.spark.temporalquery.util.stdClosedTemporalInterval
import org.slf4j.Logger

import java.sql.Timestamp
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, equal_null, lit, not}

import java.time.LocalDateTime

/**
 * Linear query utils for interval axis of type Timestamp
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearTimestampQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiTemporalClosedIntervalQueryUtil extends BiLinearGenericQueryUtil[Timestamp] {
  implicit val defaultClosedIntervalDef: ClosedInterval[Timestamp] = stdClosedTemporalInterval

  def drop(btqc: BiLinearClosedIntervalQueryConfig, fromColName: String): LinearClosedIntervalQueryConfig = {

    val reducedMap: Map[String, (String, ClosedInterval[Timestamp])] = btqc.dimensionMap
      .filterNot { case (f, _) => fromColName == f }
    logger.debug(s"(drop) reducedMap = $reducedMap")

    LinearClosedIntervalQueryConfig(
      dimensionColNameMap = reducedMap.map { case (f, (t, _)) => (f, t) },
      additionalTechnicalColNames = btqc.additionalTechnicalColNames,
      dimLt = btqc.dimLt,
      intervalDef = reducedMap.head._2._2
    )

  }

  /**
   * Some methods unique to bi-temporal data
   * @param df1
   *   your dataFrame to work on
   */

  // TODO: find a polymorphic way in order to avoid code duplication
  // cf. BiTemporalDataFrameExtensions, BiDatoralDataFrameExtensions
  implicit class BiTemporalDataFrameExtensions(df1: DataFrame) {

    def audit(
        keys: Seq[String],
        fromColName: String,
        toColName: String,
        auditPoint: Timestamp = Timestamp.valueOf(LocalDateTime.now())
    )(implicit mrqc: BiLinearClosedIntervalQueryConfig, logger: Logger): DataFrame = {
      logger.info(s"(audit) START fromColName=$fromColName ; toColName = $toColName ; auditPoint = $auditPoint ;" +
        s" keys = (${keys.mkString(",")}) ; mrqc = $mrqc")
      logger.debug(s"(audit) mrqc = $mrqc")
      val tqc: LinearClosedIntervalQueryConfig = drop(mrqc, fromColName)
      logger.info(s"(audit) tqc = $tqc")
      val valueColNames = df1.columns.diff(keys ++ mrqc.fromToColnames).map(cn => (cn, s"_audit_$cn"))
      logger.info(s"(audit) valueColNames = ${valueColNames.mkString("Array(", ", ", ")")}")
      val dfAudit = df1.where(col(fromColName) <= auditPoint and col(toColName) > auditPoint)
        .drop(fromColName, toColName)
        .withColumnsRenamed(valueColNames.toMap)
      // localCheckpoint breaks the lineage shared with dfAudit (both are derived from df1): without it,
      // Spark's ambiguous self-join detection fails the later join of dfAudit and dfDiagonal because it
      // can't tell which side a shared column originates from
      val dfDiagonal = MultivarRangeQueryImpl.getDiagonal[Timestamp](df1, mrqc, tqc.fromColnames.head, tqc.toColnames.head)
        .localCheckpoint()
      val notEqualFilter = valueColNames.map { case (cn, auditCn) => not(equal_null(col(cn), col(auditCn))) }
        .foldLeft(lit(false)) { case (cl, cr) => cl or cr }
      logger.info(s"(audit) notEqualFilter = $notEqualFilter")
      MultivarRangeQueryImpl.outerJoinRangesWithKey(
        df1 = dfAudit,
        df2 = dfDiagonal,
        keys = keys,
        mrqc = tqc,
        rnkExpressions = Nil,
        additionalJoinFilterCondition = lit(true),
        joinType = "left",
        doCleanupExtend = true
      )
        .where(notEqualFilter)
    }

  }

}
