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
 * import ch.zzeekk.spark-temporalquery.LinearFloatQueryUtil._ // this imports linear* implicit functions on DataFrame & Columns
 * implicit val tqc =  LinearQueryConfig(intervalDef = ClosedFromOpenToInterval(0f, Float.MaxValue)) // configure options for linear query operations
 * implicit val sss = ss // make SparkSession implicitly available
 * val df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object LinearFloatQueryUtil extends LinearGenericQueryUtil[Float]

/**
 * Linear query utils for interval axis of type Double
 *
 * Usage:
 * import ch.zzeekk.spark-temporalquery.LinearDoubleQueryUtil._ // this imports linear* implicit functions on DataFrame & Columns
 * implicit val tqc =  LinearQueryConfig(intervalDef = ClosedFromOpenToInterval(0d, Double.MaxValue)) // configure options for linear query operations
 * implicit val sss = ss // make SparkSession implicitly available
 * val df_joined = df1.linearJoin(df2) // use linear query functions with Spark
 */
object LinearDoubleQueryUtil extends LinearGenericQueryUtil[Double]

/**
 * Generic class to provide linear query utils for different interval axis types
 * @tparam T: scala type for interval axis
 */
case class LinearGenericQueryUtil[T: Ordering: TypeTag]() extends Logging {

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class LinearQueryConfig( override val fromColName: String    = "position_von",
                                override val toColName: String      = "position_bis",
                                override val additionalTechnicalColNames: Seq[String] = Seq(),
                                override val intervalDef: IntervalDef[T]
                              ) extends IntervalQueryConfig[T] {
    override lazy val config2: LinearQueryConfig = this.copy(fromColName = fromColName2, toColName = toColName2)
  }

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
      IntervalQueryImpl.joinIntervals( df1, df2, keyCondition )

    /**
     * Implementiert ein full-outer-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records im anderen DataFrame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def linearFullJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                      (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full" )

    /**
     * Implementiert ein left-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df2 kein zeitliches 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Wert genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records in df1 stattfinden kann.
     *   Soll df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def linearLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                      (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left" )

    /**
     * Implementiert ein righ-outer-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * - rnkExpressions: Für den Fall, dass df1 oder df2 kein lineares 1-1-mapping ist, also keys :+ fromColName nicht eindeutig sind,
     *   wird mit Hilfe der rnkExpressions für jeden Zeitpunkt genau eine Zeile ausgewählt. Dies entspricht also einem join
     *   mit der Einschränkung, dass keine Muliplikation der Records im anderen DataFrame stattfinden kann.
     *   Soll df1 oder df2 aber als eine one-to-many Relation gejoined werden und damit auch die Multiplikation von
     *   Records aus df1/df2 möglich sein, so kann durch setzen von rnkExpressions = Seq() diese Bereinigung ausgeschaltet werden.
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
     */
    def linearRightJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )
                       (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.outerJoinIntervalsWithKey( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right" )

    /**
     * Implementiert einen left-anti-join von linearen Daten über eine Liste von gleichbenannten Spalten
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-anti-join
     */
    def linearLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true) )
                          (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.leftAntiJoinIntervals( df1, df2, joinColumns, additionalJoinFilterCondition )

    /**
     * Löst lineare Überlappungen
     * - rnkExpressions: Priorität zum Bereinigen
     * - aggExpressions: Beim Bereinigen zu erstellende Aggregationen
     * - rnkFilter: Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
     * - extend: Wenn true und fillGapsWithNull=true, dann werden für jeden key Zeilen mit Null-werten hinzugefügt,
     *           sodass die ganze lineare Achse [minValue , maxValue] von allen keys abgedeckt wird
     * - fillGapsWithNull: Wenn true, dann werden Lücken in der linearen Achse mit Nullzeilen geschlossen.
     *   ! fillGapsWithNull muss auf true gesetzt werden, damit extend=true etwas bewirkt !
     */
    def linearCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Seq()
                           , rnkFilter:Boolean = true , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                           (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame =
      IntervalQueryImpl.cleanupExtendIntervals( df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull )

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung gibt.
     * Zuerst wird der Dataframe mittels [[linearRoundClosedIntervals]] etwas bereinigt, siehe Beschreibung dort
     */
    def linearCombine( keys:Seq[String] = Seq() , ignoreColNames:Seq[String] = Seq() )
                     (implicit ss:SparkSession, tc:LinearQueryConfig) : DataFrame = {
      if(keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      IntervalQueryImpl.combineIntervals( df1, ignoreColNames )
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
     * If a timestamp lies outside of [minvalue, maxValue] it will be replaced by minValue, maxValue respectively.
     * Rows for which the validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     */
    def linearRoundClosedIntervals(implicit tc:LinearQueryConfig): DataFrame = {
      assert(tc.intervalDef.isInstanceOf[ClosedInterval[_]], "ClosedInterval interval definition needed in LinearQueryConfig for linearRoundDiscreteTime()")
      IntervalQueryImpl.roundIntervalsToDiscreteTime(df1)
    }

    /**
     * Transforms [[DataFrame]] with half open time intervals "[fromColName , toColName [" to closed intervals "[fromColName , toColName]"
     */
    def linearConvertToClosedIntervals(implicit tc:LinearQueryConfig): DataFrame = {
      assert(tc.intervalDef.isInstanceOf[ClosedFromOpenToInterval[_]], "ClosedInterval interval definition needed in LinearQueryConfig for linearConvertToClosedIntervals()")
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
