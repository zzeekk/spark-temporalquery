/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 */

package ch.zzeekk.spark.temporalquery

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.reflect.runtime.universe._

/**
 * Linear query utils for interval axis of type Float
 *
 * Usage:
 * import ch.zzeekk.spark.temporalquery.LinearFloatQueryUtil._ // this imports linear* implicit functions on DataFrame & Columns
 * implicit val tqc = LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query operations if needed
 * implicit val sss = ss // make SparkSession implicitly available
 * val df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object LinearFloatQueryUtil extends LinearGenericQueryUtil[Float] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Float] = HalfOpenInterval(Float.NegativeInfinity, Float.PositiveInfinity)
}

/**
 * Linear query utils for interval axis of type Double
 *
 * Usage:
 * import ch.zzeekk.spark.temporalquery.LinearDoubleQueryUtil._ // this imports linear* implicit functions on DataFrame & Columns
 * implicit val tqc = LinearHalfOpenIntervalQueryConfig.withDefaultIntervalDef() // configure options for linear query operations if needed
 * implicit val sss = ss // make SparkSession implicitly available
 * val df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object LinearDoubleQueryUtil extends LinearGenericQueryUtil[Double] {
  implicit val defaultHalfOpenIntervalDef: HalfOpenInterval[Double] = HalfOpenInterval(Double.NegativeInfinity, Double.PositiveInfinity)
}

/**
 * Generic class to provide linear query utils for different interval axis types
 * @tparam T: scala type for interval axis
 */
class LinearGenericQueryUtil[T: Ordering: TypeTag]() extends Serializable with Logging {

  /**
   * Configuration Parameters for operations on closed intervals. An instance of this class is needed as implicit parameter.
   */
  case class LinearClosedIntervalQueryConfig( override val fromColName: String = "position_von",
                                              override val toColName: String = "position_bis",
                                              override val additionalTechnicalColNames: Seq[String] = Seq(),
                                              override val intervalDef: ClosedInterval[T]
                                            ) extends ClosedIntervalQueryConfig[T] with LinearQueryConfigMarker {
    override lazy val config2: LinearClosedIntervalQueryConfig = this.copy(fromColName = fromColName2, toColName = toColName2)
  }

  /**
   * Configuration Parameters for operations on half-open intervals. An instance of this class is needed as implicit parameter.
   */
  case class LinearHalfOpenIntervalQueryConfig( override val fromColName: String = "position_von",
                                                override val toColName: String = "position_bis",
                                                override val additionalTechnicalColNames: Seq[String] = Seq(),
                                                override val intervalDef: HalfOpenInterval[T]
                                              ) extends HalfOpenIntervalQueryConfig[T] with LinearQueryConfigMarker {
    override lazy val config2: LinearHalfOpenIntervalQueryConfig = this.copy(fromColName = fromColName2, toColName = toColName2)
  }
  object LinearHalfOpenIntervalQueryConfig {
    /**
     * Alternative method to create a LinearHalfOpenIntervalQueryConfig providing a default intervalDef by an implicit parameter
     */
    def withDefaultIntervalDef(fromColName: String = "position_von", toColName: String = "position_bis",  additionalTechnicalColNames: Seq[String] = Seq())(implicit intervalDef: HalfOpenInterval[T]): LinearHalfOpenIntervalQueryConfig = {
      LinearHalfOpenIntervalQueryConfig(fromColName, toColName, Seq(), intervalDef = intervalDef)
    }
  }

  /**
   * Trait to mark linear query configurations to make implicit resolution unique if there is also an implicit temporal query configuration in scope
   */
  trait LinearQueryConfigMarker

  /**
   * Type which includes LinearClosedIntervalQueryConfig and LinearHalfOpenIntervalQueryConfig
   */
  type LinearQueryConfig = IntervalQueryConfig[T,_] with LinearQueryConfigMarker

  /**
   * Pimp-my-library pattern für's DataFrame
   */
  implicit class LinearDataFrameExtensions(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von linearen Daten über eine Liste von gleichbenannten Spalten
     */
    def linearInnerJoin( df2:DataFrame, keys:Seq[String] )
                       (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.joinIntervalsWithKeysImpl( df1, df2, keys )

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    def linearInnerJoin( df2:DataFrame, keyCondition:Column )
                       (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.joinIntervals(df1, df2, keys = Seq(), joinType = "inner", keyCondition)

    /**
     * Implementiert ein full-outer-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * @param rnkExpressions: Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records im anderen DataFrame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * @param additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend Kann auf false gesetzt werden, falls cleanupExtend Operation auf beiden Input-DataFrames bereits ausgeführt wurde (default = true)
     */
    def linearFullJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true), doCleanupExtend: Boolean = true )
                      (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend )

    /**
     * Implementiert ein left-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * @param rnkExpressions: Für den Fall, dass df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records in df1 stattfinden kann.
     *   Soll df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * @param additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfRight bereits ausgeführt wurde (default = true)
     */
    def linearLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true), doCleanupExtend: Boolean = true )
                      (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left", doCleanupExtend )

    /**
     * Implementiert ein right-outer-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * @param rnkExpressions: Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Zeitpunkt genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records im anderen DataFrame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * @param additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     * @param doCleanupExtend Kann auf false gesetzt werden, falls cleanupExtend Operation auf Input-DataFrame dfLeft bereits ausgeführt wurde (default = true)
     */
    def linearRightJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true), doCleanupExtend: Boolean = true )
                       (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend )

    /**
     * Implementiert einen left-anti-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * @param additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-anti-join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def linearLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true) )
                          (implicit ss:SparkSession, tc:LinearClosedIntervalQueryConfig) : DataFrame = {
      assert(tc.intervalDef.isInstanceOf[ClosedInterval[_]], "Only ClosedInterval interval definition in LinearQueryConfig supported for linearLeftAntiJoin()")
      IntervalQueryImpl.leftAntiJoinIntervals( df1, df2, joinColumns, additionalJoinFilterCondition )
    }

    /**
     * Löst lineare Überlappungen
     * @param rnkExpressions: Priorität zum Bereinigen
     * @param aggExpressions: Beim Bereinigen zu erstellende Aggregationen
     * @param rnkFilter: Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
     * @param extend: Wenn true und fillGapsWithNull=true, dann werden für jeden key Zeilen mit Null-werten hinzugefügt,
     *           sodass die ganze lineare Achse [lowerHorizon , upperHorizon] von allen keys abgedeckt wird
     * @param fillGapsWithNull: Wenn true, dann werden Lücken in der linearen Achse mit Nullzeilen geschlossen.
     *   ! fillGapsWithNull muss auf true gesetzt werden, damit extend=true etwas bewirkt !
     */
    def linearCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Seq()
                           , rnkFilter:Boolean = true , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                           (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.cleanupExtendIntervals( df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull )

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung gibt.
     */
    def linearCombine( keys:Seq[String] = Seq() , ignoreColNames:Seq[String] = Seq() )
                     (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame = {
      if(keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      IntervalQueryImpl
        .combineIntervals( df1.where(tc.isValidIntervalExpr), ignoreColNames )
    }

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle gültigen Records aufgeteilt werden
     */
    def linearUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.unifyIntervalRanges( df1, keys )

    /**
     * Erweitert die Versionierung des kleinsten gueltig_ab pro Key auf minDate
     */
    def linearExtendRange( keys:Seq[String]=Seq(), extendMin:Boolean=true, extendMax:Boolean=true )
                         (implicit ss:SparkSession, tc:LinearQueryConfig): DataFrame =
      IntervalQueryImpl.extendIntervalRanges( df1, keys, extendMin, extendMax )

    /**
     * Round closed intervals according to ClosedInterval discrete axis definition.
     * Sets the discreteness of the linear axis to the value defined in ClosedInterval.discreteAxisDef.
     * Hereby the intervals may be shortened on the lower bound and extended on the upper bound.
     * To the lower bound ceiling is applied whereas to the upper bound flooring.
     * If the dataframe has a discreteness of millisecond or coarser, then the only two changes are:
     * If a timestamp lies outside of [lowerHorizon, upperHorizon] it will be replaced by lowerHorizon, upperHorizon respectively.
     * Rows for which the validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval definitions can only
     * be created for axis with Integral-Numeric type, and not Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearRoundClosedIntervals(implicit tc:LinearClosedIntervalQueryConfig): DataFrame = {
      IntervalQueryImpl.roundIntervalsToDiscreteTime(df1)
    }

    /**
     * Transforms [[DataFrame]] with half open time intervals "[fromColName , toColName [" to closed intervals "[fromColName , toColName]"
     *
     * Note: This function needs LinearQueryConfig with a ClosedInterval definition. ClosedInterval definitions can only
     * be created for axis with Integral-Numeric type, and not Fractional-Numeric type (e.g. Float or Double dont work).
     */
    def linearConvertToClosedIntervals(implicit tc:LinearClosedIntervalQueryConfig): DataFrame = {
      IntervalQueryImpl.transformHalfOpenToClosedIntervals(df1)
    }

  }

  /**
   * Pimp-my-library pattern für Columns
   */
  implicit class LinearColumnExtensions(value: Column) {
    def isInTemporalInterval(implicit tc:LinearQueryConfig): Column = tc.isInIntervalExpr(value)
  }

}
