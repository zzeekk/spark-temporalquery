package ch.zzeekk.spark.temporalquery.util.linear

import ch.zzeekk.spark.temporalquery.interval.ClosedInterval
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, MultivarRangeQueryImpl}
import ch.zzeekk.spark.temporalquery.util.stdClosedTemporalInterval
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import java.sql.Timestamp

/**
 * Linear query utils for interval axis of type Timestamp
 *
 * Usage: import ch.zzeekk.spark.temporalquery.LinearTimestampQueryUtil._ // this imports linear*
 * implicit functions on DataFrame & Columns implicit val tqc =
 * LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query
 * operations if needed implicit val sss = ss // make SparkSession implicitly available val
 * df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object TemporalClosedQueryUtil extends LinearGenericQueryUtil[Timestamp] {
  implicit val defaultClosedIntervalDef: ClosedInterval[Timestamp] = stdClosedTemporalInterval

  /**
   * Legacy
   */
  @deprecated("Use implicit class MultivariateRangeFrameExtensions of object MultivariateRangeLibrary", "4.0.0")
  implicit class TemporalDataFrameExtensions(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von historisierten Daten über eine Liste von gleichbenannten
     * Spalten
     */

    @deprecated("Use rangeInnerJoin", "4.0.0")
    def temporalInnerJoin(df2: DataFrame, keys: Seq[String])(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.joinIntervalsWithKeysImpl[Timestamp](df1 = df1, df2 = df2, keys = keys, mrqc = clmrqc)

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    @deprecated("Use rangeInnerJoin", "4.0.0")
    def temporalInnerJoin(df2: DataFrame, keyCondition: Column)(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl
      .joinRanges[Timestamp](df1 = df1, df2 = df2, keys = Nil, mrqc = clmrqc, additionalJoinCondition = keyCondition)

    /**
     * Implementiert ein full-outer-join von historisierten Daten über eine Liste von
     * gleichbenannten Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df1 oder df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName
     *   nicht eindeutig sind, wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine
     *   Zeile ausgewählt. Dies entspricht also ein join mit der Einschränkung, dass kein
     *   Muliplikation der Records im anderen frame stattfinden kann. Soll df1 oder df2 aber als
     *   eine one-to-many Relation gejoined werden und damit auch die Multiplikation von Records aus
     *   df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung
     *   ausgeschaltet.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den full-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf beiden Input-DataFrames
     *   bereits ausgeführt wurde (default = true)
     */
    @deprecated("Use rangeFullJoin", "4.0.0")
    def temporalFullJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Seq(),
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinRangesWithKey[Timestamp](df1, df2, keys, clmrqc, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

    /**
     * Implementiert ein left-outer-join von historisierten Daten über eine Liste von
     * gleichbenannten Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht
     *   eindeutig sind, wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine Zeile
     *   ausgewählt. Dies entspricht also ein join mit der Einschränkung, dass kein Muliplikation
     *   der Records in df1 stattfinden kann. Soll df2 aber als eine one-to-many Relation gejoined
     *   werden und damit auch die Multiplikation von Records aus df1 möglich sein, so kann durch
     *   setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfRight
     *   bereits ausgeführt wurde (default = true)
     */
    @deprecated("Use rangeLeftJoin", "4.0.0")
    def temporalLeftJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Seq(),
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp], logger: Logger): DataFrame =
      MultivarRangeQueryImpl.outerJoinRangesWithKey[Timestamp](
        df1 = df1,
        df2 = df2,
        keys = keys,
        mrqc = clmrqc,
        rnkExpressions = rnkExpressions,
        additionalJoinFilterCondition = additionalJoinFilterCondition,
        joinType = "left",
        doCleanupExtend = doCleanupExtend
      )

    /**
     * Implementiert ein righ-outer-join von historisierten Daten über eine Liste von
     * gleichbenannten Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df1 oder df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName
     *   nicht eindeutig sind, wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine
     *   Zeile ausgewählt. Dies entspricht also ein join mit der Einschränkung, dass kein
     *   Muliplikation der Records im anderen frame stattfinden kann. Soll df1 oder df2 aber als
     *   eine one-to-many Relation gejoined werden und damit auch die Multiplikation von Records aus
     *   df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung
     *   ausgeschaltet.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den right-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfLeft
     *   bereits ausgeführt wurde (default = true)
     */
    @deprecated("Use rangeRightJoin", "4.0.0")
    def temporalRightJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Seq(),
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp], logger: Logger): DataFrame =
      MultivarRangeQueryImpl
        .outerJoinRangesWithKey[Timestamp](df1, df2, keys, clmrqc, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implementiert ein left-anti-join von historisierten Daten über eine Liste von gleichbenannten
     * Spalten
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-anti-join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    @deprecated("Use rangeLeftAntiJoin", "4.0.0")
    def temporalLeftAntiJoin(df2: DataFrame, joinColumns: Seq[String], additionalJoinFilterCondition: Column = lit(true))(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame =
      MultivarRangeQueryImpl.leftAntiJoinRanges[Timestamp](df1 = df1, df2 = df2, keys = joinColumns, mrqc = clmrqc,
        additionalJoinFilterCondition = additionalJoinFilterCondition)

    /**
     * Löst zeitliche Überlappungen
     * @param rnkExpressions:
     *   Priorität zum Bereinigen
     * @param aggExpressions:
     *   Beim Bereinigen zu erstellende Aggregationen
     * @param rnkFilter:
     *   Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
     * @param extend:
     *   Wenn true und fillGapsWithNull=true, dann werden für jeden key Zeilen mit Null-werten
     *   hinzugefügt, sodass die ganze Zeitachse [minDate , maxDate] von allen keys abgedeckt wird
     * @param fillGapsWithNull:
     *   Wenn true, dann werden Lücken in der Historie mit Nullzeilen geschlossen. !
     *   fillGapsWithNull muss auf true gesetzt werden, damit extend=true etwas bewirkt !
     */
    @deprecated("Use rangeCleanupExtend", "4.0.0")
    def temporalCleanupExtend(
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Seq(),
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .cleanupExtendRanges[Timestamp](df1, keys, clmrqc, rnkFilter, rnkExpressions, aggExpressions, extend, fillGapsWithNull)

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung
     * gibt. Zuerst wird der Dataframe mittels [[temporalRoundDiscreteTime]] etwas bereinigt, siehe
     * Beschreibung dort
     */
    @deprecated("Use rangeCombine", "4.0.0")
    def temporalCombine(ignoreColNames: Seq[String] = Seq())(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.combineMultivarRanges[Timestamp](
      df = df1,
      mrqc = clmrqc,
      ignoreColNames = ignoreColNames
    )

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle
     * gültigen Records aufgeteilt werden
     */
    @deprecated("Use rangeUnifyRanges", "4.0.0")
    def temporalUnifyRanges(keys: Seq[String])(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame =
      MultivarRangeQueryImpl.unifyMultivarRanges[Timestamp](df1, clmrqc, keys)

    /**
     * Erweitert die Historie des kleinsten Werts pro Key auf minDate
     */
    @deprecated("Use rangeExtendRange", "4.0.0")
    def temporalExtendRange(keys: Seq[String] = Seq(), extendMin: Boolean = true, extendMax: Boolean = true)(implicit
        clmrqc: ClosedMultivarRangeQueryConfig[Timestamp],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.extendMultivarRanges[Timestamp](df1, keys, extendMin, extendMax)

    /**
     * Sets the discreteness of the time scale to milliseconds. Hereby the validity intervals may be
     * shortened on the lower bound and extended on the upper bound. To the lower bound ceiling is
     * applied whereas to the upper bound flooring. If the dataframe has a discreteness of
     * millisecond or coarser, then the only two changes are: If a timestamp lies outside of
     * [minDate , maxDate] it will be replaced by minDate, maxDate respectively. Rows for which the
     * validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   temporal dataframe with a discreteness of milliseconds
     */
    @deprecated("Use rangeRoundDiscreteTime", "4.0.0")
    def temporalRoundDiscreteTime(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp]): DataFrame =
      MultivarRangeQueryImpl.roundIntervalsToDiscreteTime[Timestamp](df1, clmrqc)

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName ,
     * toColName [, to discrete time ([fromColName , toColName])
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   [[DataFrame]] with discrete time axis
     */
    @deprecated("Use rangeDense2discrete", "4.0.0")
    def temporalContinuous2discrete(implicit clmrqc: ClosedMultivarRangeQueryConfig[Timestamp]): DataFrame =
      MultivarRangeQueryImpl.transformHalfOpenToClosedIntervals[Timestamp](df1, clmrqc)

  }

}
