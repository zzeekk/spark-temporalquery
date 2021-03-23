/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import java.sql.Timestamp
import java.time.temporal.ChronoUnit

/**
 *  Temporal query utils for interval axis of type Timestamp
 *
 * Usage:
 * import ch.zzeekk.spark-temporalquery.TemporalQueryUtil._ // this imports temporal* implicit functions on DataFrame and Columns
 * implicit val tqc = TemporalQueryConfig() // configure options for temporal query operations
 * implicit val sss = ss // make SparkSession implicitly available
 * val df_joined = df1.temporalJoin(df2) // use temporal query functions with Spark
 */
object TemporalQueryUtil extends Logging {

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class TemporalQueryConfig( override val fromColName: String    = "gueltig_ab",
                                  override val toColName: String      = "gueltig_bis",
                                  override val additionalTechnicalColNames: Seq[String] = Seq(),
                                  override val intervalDef: IntervalDef[Timestamp] = ClosedInterval(
                                    Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.MILLIS)
                                  )
                                 ) extends IntervalQueryConfig[Timestamp] {
    val minDate: Timestamp = intervalDef.minValue
    val maxDate: Timestamp = intervalDef.maxValue
    override val config2: TemporalQueryConfig = this.copy(fromColName = fromColName2, toColName = toColName2)
  }

  implicit private val timestampOrdering: Ordering[Timestamp] = Ordering.fromLessThan[Timestamp]((a,b) => a.before(b))

  /**
   * Pimp-my-library pattern für's DataFrame
   */
  implicit class TemporalDataFrameExtensions(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     */
    def temporalInnerJoin( df2:DataFrame, keys:Seq[String] )
                         (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.joinIntervalsWithKeysImpl( df1, df2, keys )

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    def temporalInnerJoin( df2:DataFrame, keyCondition:Column )
                         (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.joinIntervals( df1, df2, keyCondition )

    /**
     * Implementiert ein full-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df1 oder df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine Zeile ausgewählt. Dies entspricht also ein join
     *   mit der Einschränkung, dass kein Muliplikation der Records im anderen frame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def temporalFullJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full" )

    /**
     * Implementiert ein left-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine Zeile ausgewählt. Dies entspricht also ein join
     *   mit der Einschränkung, dass kein Muliplikation der Records in df1 stattfinden kann.
     *   Soll df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def temporalLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left" )

    /**
     * Implementiert ein righ-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df1 oder df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe des rnkExpressions für jeden Zeitpunkt genau eine Zeile ausgewählt. Dies entspricht also ein join
     *   mit der Einschränkung, dass kein Muliplikation der Records im anderen frame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def temporalRightJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right" )

    /**
     * Implementiert ein left-anti-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-anti-join
     */
    def temporalLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true) )
                            (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.leftAntiJoinIntervals( df1, df2, joinColumns, additionalJoinFilterCondition )

    /**
     * Löst zeitliche Überlappungen
     * - rnkExpressions: Priorität zum Bereinigen
     * - aggExpressions: Beim Bereinigen zu erstellende Aggregationen
     * - rnkFilter: Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
     * - extend: Wenn true und fillGapsWithNull=true, dann werden für jeden key Zeilen mit Null-werten hinzugefügt,
     *           sodass die ganze Zeitachse [minDate , maxDate] von allen keys abgedeckt wird
     * - fillGapsWithNull: Wenn true, dann werden Lücken in der Historie mit Nullzeilen geschlossen.
     *   ! fillGapsWithNull muss auf true gesetzt werden, damit extend=true etwas bewirkt !
     */
    def temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Seq()
                               , rnkFilter:Boolean = true , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                             (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.cleanupExtendIntervals( df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull )

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung gibt.
     * Zuerst wird der Dataframe mittels [[temporalRoundDiscreteTime]] etwas bereinigt, siehe Beschreibung dort
     */
    def temporalCombine( keys:Seq[String] = Seq() , ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      if(keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      IntervalQueryImpl.combineIntervals( df1, ignoreColNames )
    }

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle gültigen Records aufgeteilt werden
     */
    def temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame =
      IntervalQueryImpl.unifyIntervalRanges( df1, keys )

    /**
     * Erweitert die Historie des kleinsten Werts pro Key auf minDate
     */
    def temporalExtendRange( keys:Seq[String]=Seq(), extendMin:Boolean=true, extendMax:Boolean=true )(implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame =
      IntervalQueryImpl.extendIntervalRanges( df1, keys, extendMin, extendMax )

    /**
     * Sets the discreteness of the time scale to milliseconds.
     * Hereby the validity intervals may be shortened on the lower bound and extended on the upper bound.
     * To the lower bound ceiling is applied whereas to the upper bound flooring.
     * If the dataframe has a discreteness of millisecond or coarser,
     * then the only two changes are:
     * If a timestamp lies outside of [minDate , maxDate] it will be replaced by minDate, maxDate respectively.
     * Rows for which the validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     *
     * @return temporal dataframe with a discreteness of milliseconds
     */
    def temporalRoundDiscreteTime(implicit tc:TemporalQueryConfig): DataFrame =
      IntervalQueryImpl.roundIntervalsToDiscreteTime(df1)

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName , toColName [, to discrete time ([fromColName , toColName])
     * @return [[DataFrame]] with discrete time axis
     */
    def temporalContinuous2discrete(implicit tc:TemporalQueryConfig): DataFrame =
      IntervalQueryImpl.transformHalfOpenToClosedInterval(df1)

  }

  /**
   * Pimp-my-library pattern für Columns
   */
  implicit class TemporalColumnExtensions(value: Column) {
    def isInTemporalInterval(implicit tc:TemporalQueryConfig): Column = tc.isInIntervalExpr(value)
  }

}
