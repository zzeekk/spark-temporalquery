package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval
import ch.zzeekk.spark.temporalquery.multivarRange.MultivarRangeQueryImpl
import ch.zzeekk.spark.temporalquery.util.linear.DatoralHalfOpenIntervalQueryUtil.LinearHalfOpenIntervalQueryConfig
import ch.zzeekk.spark.temporalquery.util.stdHalfOpenDatoralInterval
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, equal_null, lit, not}
import org.slf4j.Logger

import java.sql.Date
import java.time.LocalDate

/**
 * Linear query utils for interval axis of type Date
 *
 * Usage: import ch.zzeekk.spark.temporalquery.BiLinearDateQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object BiDatoralHalfOpenIntervalQueryUtil extends BiLinearGenericQueryUtil[Date] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Date] = stdHalfOpenDatoralInterval

  def drop(bDateQc: BiLinearHalfOpenIntervalQueryConfig, fromColName: String): LinearHalfOpenIntervalQueryConfig = {

    val reducedMap: Map[String, (String, HalfOpenInterval[Date])] = bDateQc.dimensionMap
      .filterNot { case (f, _) => fromColName == f }

    LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap = reducedMap.map { case (f, (t, _)) => (f, t) },
      additionalTechnicalColNames = bDateQc.additionalTechnicalColNames,
      dimLt = bDateQc.dimLt,
      intervalDef = reducedMap.head._2._2
    )

  }

  // TODO: find a polymorphic way in order to avoid code duplication
  // cf. BiTemporalDataFrameExtensions, BiDatoralDataFrameExtensions
  implicit class BiDatoralDataFrameExtensions(df1: DataFrame) {

    def audit(
        keys: Seq[String],
        fromColName: String,
        toColName: String,
        auditPoint: Date = Date.valueOf(LocalDate.now())
    )(implicit mrqc: BiLinearHalfOpenIntervalQueryConfig, logger: Logger): DataFrame = {
      logger.info(s"(audit) START fromColName=$fromColName ; toColName = $toColName ; auditPoint = $auditPoint ;" +
        s" keys = (${keys.mkString(",")}) ; mrqc = $mrqc")
      logger.debug(s"(audit) mrqc = $mrqc")
      val tqc: LinearHalfOpenIntervalQueryConfig = drop(mrqc, fromColName)
      logger.info(s"(audit) tqc = $tqc")
      val valueColNames = df1.columns.diff(keys ++ mrqc.fromToColnames).map(cn => (cn, s"_audit_$cn"))
      logger.info(s"(audit) valueColNames = ${valueColNames.mkString("Array(", ", ", ")")}")
      val dfAudit = df1.where(col(fromColName) <= auditPoint and col(toColName) > auditPoint)
        .drop(fromColName, toColName)
        .withColumnsRenamed(valueColNames.toMap)
      // localCheckpoint breaks the lineage shared with dfAudit (both are derived from df1): without it,
      // Spark's ambiguous self-join detection fails the later join of dfAudit and dfDiagonal because it
      // can't tell which side a shared column originates from
      val dfDiagonal = MultivarRangeQueryImpl.getDiagonal[Date](df1, mrqc, tqc.fromColnames.head, tqc.toColnames.head)
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
