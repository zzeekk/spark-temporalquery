/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.slf4j.Logger

import scala.reflect.runtime.universe._

/**
 * Generic class to provide linear query utils for different interval axis types
 * @tparam T:
 *   scala type for interval axis
 */
class LinearGenericQueryUtil[T: Ordering: TypeTag] extends Serializable with Logging {

  /**
   * Trait to mark linear query configurations to make implicit resolution unique if there is also
   * an implicit temporal query configuration in scope
   */
  trait LinearQueryConfigMarker

  /**
   * Type which includes LinearClosedIntervalQueryConfig and LinearHalfOpenIntervalQueryConfig
   */
  private type LinearQueryConfig = IntervalMultidimQueryConfig[T, _] with LinearQueryConfigMarker

  /**
   * Configuration Parameters for operations on closed intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearClosedIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_von" -> "position_bis"),
      override val additionalTechnicalColNames: Seq[String] = Nil,
      override val intervalDef: ClosedInterval[T]
  ) extends ClosedIntervalMultidimQueryConfig[T] with LinearQueryConfigMarker {
    override def dimensionMap: Map[String, (String, ClosedInterval[T])] = dimensionColNameMap.map { case (f, t) =>
      (f, (t, intervalDef))
    }
    override lazy val config2: LinearClosedIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is
   * needed as implicit parameter.
   */
  case class LinearHalfOpenIntervalQueryConfig(
      dimensionColNameMap: Map[String, String] = Map("position_von" -> "position_bis"),
      override val additionalTechnicalColNames: Seq[String] = Nil,
      override val intervalDef: HalfOpenInterval[T]
  ) extends HalfOpenIntervalMultidimQueryConfig[T] with LinearQueryConfigMarker {
    override def dimensionMap: Map[String, (String, HalfOpenInterval[T])] = dimensionColNameMap
      .map { case (f, t) => (f, (t, intervalDef)) }
    override lazy val config2: LinearHalfOpenIntervalQueryConfig = this
      .copy(dimensionColNameMap = dimensionColNameMap.map { case (f, t) => (increaseColNameNb(f), increaseColNameNb(t)) })
  }
  object LinearHalfOpenIntervalQueryConfig {

    /**
     * Alternative method to create a LinearHalfOpenIntervalQueryConfig providing a default
     * intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(
        fromColName: String = "position_von",
        toColName: String = "position_bis"
    )(implicit intervalDef: HalfOpenInterval[T], logger: Logger): LinearHalfOpenIntervalQueryConfig = {
      debugLog(s"(withDefaultIntervalDef) fromColName = $fromColName ; toColName = $toColName ; intervalDef = $intervalDef")
      LinearHalfOpenIntervalQueryConfig(dimensionColNameMap = Map(fromColName -> toColName),
        additionalTechnicalColNames = Nil, intervalDef = intervalDef)
    }

  }

  /**
   * Pimp-my-library pattern für's DataFrame
   */
  implicit class LinearDataFrameExtensions(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von linearen Daten über eine Liste von gleich benannten Spalten
     */
    def linearInnerJoin(df2: DataFrame, keys: Seq[String])(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.joinIntervalsWithKeysImpl(df1, df2, keys)

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    def linearInnerJoin(df2: DataFrame, keyCondition: Column)(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.joinIntervals(df1, df2, keys = Nil, joinType = "inner", keyCondition)

    /**
     * Implementiert ein full-outer-join von linearen Daten über eine Liste von gleich benannten
     * Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName
     *   nicht eindeutig sind, wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile
     *   ausgewählt. Dies entspricht also einem join mit der Einschränkung, dass keine Muliplikation
     *   der Records im anderen DataFrame stattfinden kann. Soll df1 oder df2 aber als eine
     *   one-to-many Relation gejoined werden und damit auch die Multiplikation von Records aus
     *   df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Nil diese Bereinigung
     *   ausgeschaltet werden.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf beiden Input-DataFrames
     *   bereits ausgeführt wurde (default = true)
     */
    def linearFullJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

    /**
     * Implementiert ein left-outer-join von historisierten Daten über eine Liste von gleich
     * benannten Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht
     *   eindeutig sind, wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile
     *   ausgewählt. Dies entspricht also einem join mit der Einschränkung, dass keine Muliplikation
     *   der Records in df1 stattfinden kann. Soll df2 aber als eine one-to-many Relation gejoined
     *   werden und damit auch die Multiplikation von Records aus df1 möglich sein, so kann durch
     *   setzen von rnkExpressions = Nil diese Bereinigung ausgeschaltet werden.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfRight
     *   bereits ausgeführt wurde (default = true)
     */
    def linearLeftJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left", doCleanupExtend)

    /**
     * Implementiert ein right-outer-join von linearen Daten über eine Liste von gleich benannten
     * Spalten
     * @param rnkExpressions:
     *   Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName
     *   nicht eindeutig sind, wird mit Hilfe der rnkExpressions für jeden Zeitpunkt genau eine
     *   Zeile ausgewählt. Dies entspricht also einem join mit der Einschränkung, dass keine
     *   Muliplikation der Records im anderen DataFrame stattfinden kann. Soll df1 oder df2 aber als
     *   eine one-to-many Relation gejoined werden und damit auch die Multiplikation von Records aus
     *   df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Nil diese Bereinigung
     *   ausgeschaltet werden.
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend
     *   Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfLeft
     *   bereits ausgeführt wurde (default = true)
     */
    def linearRightJoin(
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implementiert einen left-anti-join von linearen Daten über eine Liste von gleich benannten
     * Spalten
     * @param additionalJoinFilterCondition:
     *   zusätzliche non-equi-join Bedingungen für den left-anti-join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def linearLeftAntiJoin(
        df2: DataFrame,
        joinColumns: Seq[String],
        additionalJoinFilterCondition: Column = lit(true)
    )(
        implicit
        lqc: LinearClosedIntervalQueryConfig,
        logger: Logger
    ): DataFrame = {
      assert(lqc.intervalDef.isInstanceOf[ClosedInterval[_]],
        "Only ClosedInterval interval definition in LinearQueryConfig supported for linearLeftAntiJoin()")
      IntervalQueryImpl.leftAntiJoinIntervals(df1, df2, joinColumns, additionalJoinFilterCondition)
    }

    /**
     * Löst lineare Überlappungen
     * @param rnkExpressions:
     *   Priorität zum Bereinigen
     * @param aggExpressions:
     *   Beim Bereinigen zu erstellende Aggregationen
     * @param rnkFilter:
     *   Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
     * @param extend:
     *   Wenn true und fillGapsWithNull=true, dann werden für jeden key Zeilen mit Null-werten
     *   hinzugefügt, sodass die ganze lineare Achse [lowerHorizon , upperHorizon] von allen keys
     *   abgedeckt wird
     * @param fillGapsWithNull:
     *   Wenn true, dann werden Lücken in der linearen Achse mit Nullzeilen geschlossen. !
     *   fillGapsWithNull muss auf true gesetzt werden, damit extend=true etwas bewirkt !
     */
    def linearCleanupExtend(
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Nil,
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.cleanupExtendIntervals(df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull)

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung
     * gibt.
     */
    def linearCombine(keys: Seq[String] = Nil, ignoreColNames: Seq[String] = Nil)(implicit
        lqc: LinearQueryConfig,
        logger: Logger
    ): DataFrame = {
      if (keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      IntervalQueryImpl
        .combineIntervals(df1.where(lqc.isValidIntervalExpr), ignoreColNames)
    }

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle
     * gültigen Records aufgeteilt werden
     */
    def linearUnifyRanges(keys: Seq[String])(implicit lqc: LinearQueryConfig, logger: Logger): DataFrame =
      IntervalQueryImpl.unifyIntervalRanges(df1, keys)

    /**
     * Erweitert die Versionierung des kleinsten gueltig_ab pro Key auf minDate
     */
    def linearExtendRange(keys: Seq[String] = Nil, extendMin: Boolean = true, extendMax: Boolean = true)(implicit
        lqc: LinearQueryConfig
    ): DataFrame =
      IntervalQueryImpl.extendIntervalRanges(df1, keys, extendMin, extendMax)

    /**
     * Round closed intervals according to ClosedInterval discrete axis definition. Sets the
     * discreteness of the linear axis to the value defined in ClosedInterval.discreteAxisDef.
     * Hereby the intervals may be shortened on the lower bound and extended on the upper bound. To
     * the lower bound ceiling is applied whereas to the upper bound flooring. If the dataframe has
     * a discreteness of millisecond or coarser, then the only two changes are: If a timestamp lies
     * outside of [lowerHorizon, upperHorizon] it will be replaced by lowerHorizon, upperHorizon
     * respectively. Rows for which the validity ends before it starts, i.e. with
     * toCol.before(fromCol), are removed.
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval
     * definitions can only be created for axis with Integral-Numeric type, and not
     * Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearRoundClosedIntervals(implicit lqc: LinearClosedIntervalQueryConfig): DataFrame =
      IntervalQueryImpl.roundIntervalsToDiscreteTime(df1)

    /**
     * Transforms [[DataFrame]] with half open time intervals "[fromColName , toColName [" to closed
     * intervals "[fromColName , toColName]"
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval
     * definitions can only be created for axis with Integral-Numeric type, and not
     * Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearConvertToClosedIntervals(implicit lqc: LinearClosedIntervalQueryConfig): DataFrame =
      IntervalQueryImpl.transformHalfOpenToClosedIntervals(df1)

  }

  /**
   * Pimp-my-library pattern für Columns
   */
  implicit class LinearColumnExtensions(value: Column) {
    def isInTemporalInterval(implicit lqc: LinearQueryConfig): Column = lqc.isInIntervalExpr(List(value))
  }

}
