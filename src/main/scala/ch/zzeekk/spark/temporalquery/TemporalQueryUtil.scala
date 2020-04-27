package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import ch.zzeekk.spark.temporalquery.TemporalHelpers._
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window, WindowSpec}
import org.apache.spark.sql.functions._

/**
 * Copyright (c) 2017 Zacharias Kull under MIT Licence
 *
 * Usage:
 *
 * import ch.zzeekk.spark-temporalquery.TemporalQueryUtil._ // this imports temporalquery* implicit functions on DataFrame
 * implicit val tqc = TemporalQueryConfig() // configure options for temporalquery operations
 * implicit val sss = ss // make SparkSession implicitly available
 *
 * val df_joined = df1.temporalJoin(df2)
 */
object TemporalQueryUtil extends Logging {

  /**
   * Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class TemporalQueryConfig ( minDate:Timestamp = Timestamp.valueOf("0001-01-01 00:00:00")
                                   , maxDate:Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")
                                   , fromColName:String    = "gueltig_ab"
                                   , toColName:String      = "gueltig_bis"
                                   , additionalTechnicalColNames:Seq[String] = Seq()) {
    val fromColName2:String = fromColName+"2"
    val toColName2:String = toColName+"2"
    val technicalColNames:Seq[String] = Seq( fromColName, toColName ) ++ additionalTechnicalColNames
  }

  /**
   * Pimp-my-library pattern für's DataFrame
   */
  implicit class TemporalDataFrame(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     */
    def temporalInnerJoin( df2:DataFrame, keys:Seq[String] )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalKeyJoinImpl( df1, df2, keys )
    }

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    def temporalInnerJoin( df2:DataFrame, keyCondition:Column )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalJoinImpl( df1, df2, keyCondition )
    }

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
                        (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = temporalKeyOuterJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full" )

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
                        (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalKeyOuterJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "left" )
    }

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
                        (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalKeyOuterJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right" )
    }

    /**
     * Implementiert ein left-anti-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-anti-join
     */
    def temporalLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true) )
                            (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalLeftAntiJoinImpl( df1, df2, joinColumns, additionalJoinFilterCondition )
    }

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
    // TODO: entkoppele Parameter extend und fillGapsWithNull
    def temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Seq()
                               , rnkFilter:Boolean = true , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                             (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalCleanupExtendImpl( df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull )
    }

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung gibt.
     * Zuerst wird der Dataframe mittels [[temporalRoundDiscreteTime]] etwas bereinigt, siehe Beschreibung dort
     *
     * @return temporal dataframe with combined validities
     */
    def temporalCombine( keys:Seq[String] = Seq() , ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      if(keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      temporalCombineImpl( df1, ignoreColNames )
    }

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle gültigen Records aufgeteilt werden
     */
    def temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalUnifyRangesImpl( df1, keys )
    }

    /**
     * Erweitert die Versionierung des kleinsten gueltig_ab pro Key auf minDate
     */
    def temporalExtendRange( keys:Seq[String]=Seq(), extendMin:Boolean=true, extendMax:Boolean=true )(implicit ss:SparkSession, hc:TemporalQueryConfig): DataFrame = {
      temporalExtendRangeImpl( df1, keys, extendMin, extendMax )
    }

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
    def temporalRoundDiscreteTime(implicit hc:TemporalQueryConfig): DataFrame = shrinkValidityImpl(df1)(udf_floorTimestamp(hc))

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName , toColName [, to discrete time ([fromColName , toColName])
     * @return [[DataFrame]] with discrete time axis
     */
    def temporalContinuous2discrete(implicit hc:TemporalQueryConfig): DataFrame = shrinkValidityImpl(df1)(udf_predecessorTime)

  }

  // helpers

  private def createKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and df1(key)===df2(key) }
  }

  private def shrinkValidityImpl(df: DataFrame)(udfFloorOrPred: UserDefinedFunction)(implicit hc:TemporalQueryConfig): DataFrame= {
    val spalten: Seq[String] = df.columns
    df.withColumn(hc.fromColName,udf_ceilTimestamp(hc)(col({hc.fromColName})))
      .withColumn(hc.toColName,udfFloorOrPred(col({hc.toColName})))
      .where(s"${hc.fromColName} <= ${hc.toColName}")
      // return columns in same order as provided
      .select(spalten.map(col):_*)
  }

  /**
   * join two temporalquery dataframes
   *
   * Nota bene:
   * Columns which occur in both data frames df1 and df2 will occur only once in the result frame with df1 precedence over df2 using coalesce.
   */
  private def temporalJoinImpl( df1:DataFrame, df2:DataFrame, keyCondition:Column, joinType:String = "inner" )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    // history join
    val df2ren = df2.withColumnRenamed(hc.fromColName,hc.fromColName2).withColumnRenamed(hc.toColName,hc.toColName2)
    val df_join = df1.join( df2ren, keyCondition and col(hc.fromColName)<=col(hc.toColName2) and col(hc.toColName)>=col(hc.fromColName2), joinType)
    // select final schema
    val dfCommonColNames = df1.columns.intersect(df2.columns).diff(hc.technicalColNames)
    val commonCols: Array[Column] = dfCommonColNames.map(colName => coalesce(df1(colName),df2(colName)).as(colName))
    val colsDf1: Array[Column] = df1.columns.diff(dfCommonColNames ++ hc.technicalColNames).map(df1(_))
    val colsDf2: Array[Column] = df2.columns.diff(dfCommonColNames ++ hc.technicalColNames).map(df2(_))
    val timeColumns: Array[Column] = Array(greatest(col(hc.fromColName), col(hc.fromColName2)).as(hc.fromColName),least(col(hc.toColName), col(hc.toColName2)).as(hc.toColName))
    val selCols: Array[Column] = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns

    df_join.select(selCols:_*)
  }
  private def temporalKeyJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], joinType:String = "inner" )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    temporalJoinImpl( df1, df2, createKeyCondition(df1, df2, keys), joinType )
  }

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private def temporalRangesImpl( df:DataFrame, keys:Seq[String], extend:Boolean )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    val keyCols = keys.map(col)
    // get start/end-points for every key
    val df_points = df.select( keyCols :+ col(hc.fromColName).as("_dt"):_*).union( df.select( keyCols :+ udf_successorTime(hc)(col(hc.toColName)).as("_dt"):_* ))
    // if desired, extend every key with min/maxDate-points
    val df_pointsExt = if (extend) {
      df_points
        .union( df_points.select( keyCols:_*).distinct.withColumn( "_dt", lit(hc.minDate)))
        .union( df_points.select( keyCols:_*).distinct.withColumn( "_dt", lit(hc.maxDate)))
        .distinct
    } else df_points.distinct
    // build ranges
    df_pointsExt
      .withColumnRenamed("_dt", "range_von")
      .withColumn( "range_bis",
        udf_predecessorTime(hc)(lead(col("range_von"),1).over(Window.partitionBy(keys.map(col):_*).orderBy(col("range_von")))))
      .where(col("range_bis").isNotNull)
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private def temporalCleanupExtendImpl( df:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)]
                                         , rnkFilter:Boolean , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                                       (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    import ss.implicits._
    if(extend && !fillGapsWithNull) logger.warn("temporalCleanupExtendImpl: extend=true has no effect if fillGapsWithNull=false!")

    println(s"rnkExpressions = $rnkExpressions")
    val df_join = temporalUnifyRangesImpl( df, keys, extend, fillGapsWithNull)
      .withColumn("_defined", col(hc.toColName).isNotNull)
    println(s"df_join: ${df_join.schema.simpleString}")
    df_join.show(false)

    val fenestra: WindowSpec = Window.partitionBy( keys.map(df_join(_)):+ df_join(hc.fromColName) :_*)

    // add aggregations if defined, implemented as analytical functions...£
    val df_agg = aggExpressions.foldLeft( df_join ){
      case (df_acc, (name,expr)) => df_acc.withColumn(name, expr.over(fenestra))
    }
    println(s"df_agg: ${df_agg.schema.simpleString}")
    df_agg.show(false)
    // Prioritize and clean overlaps
    val df_clean = if (rnkExpressions.nonEmpty) {
      val df_rnk = df_agg.withColumn("_rnk", row_number.over(fenestra.orderBy(rnkExpressions: _*)))
      println(s"df_rnk: ${df_rnk.schema.simpleString}")
      df_rnk.show(false)
      if (rnkFilter) df_rnk.where($"_rnk"===1) else df_rnk
    } else df_agg
    // select final schema
    val selCols: Seq[Column] = keys.map(df_join(_)) ++ df.columns.diff(keys ++ hc.technicalColNames).map(df_clean(_)) ++ aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq($"_rnk") else Seq()) :+
      df_join(hc.fromColName) :+ df_join(hc.toColName) :+ $"_defined"

    val df_cleanSelectedCols = df_clean.select(selCols:_*)
    println(s"df_cleanSelectedCols: ${df_cleanSelectedCols.schema.simpleString}")
    df_cleanSelectedCols.show(false)
    temporalCombineImpl( df_cleanSelectedCols , Seq() )
  }

  /**
   * outer join
   */
  private def temporalKeyOuterJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column, joinType:String)
                                     (implicit ss:SparkSession, hc:TemporalQueryConfig): DataFrame = {
    import ss.implicits._
    // extend df2
    val df1_extended = if (joinType=="full" || joinType=="right") temporalCleanupExtendImpl( df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Seq(), rnkFilter=true ) else df1
    val df2_extended = if (joinType=="full" || joinType=="left") temporalCleanupExtendImpl( df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Seq(), rnkFilter=true ) else df2
    // join df1 & df2
    temporalJoinImpl( df1_extended, df2_extended, createKeyCondition(df1, df2, keys) and additionalJoinFilterCondition, joinType ).drop($"_defined")
  }

  /**
   * left outer join
   */
  private def temporalLeftAntiJoinImpl( df1:DataFrame, df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column )
                                      (implicit ss:SparkSession, hc:TemporalQueryConfig): DataFrame = {
    logger.debug(s"temporalLeftAntiJoinImpl START: joinColumns = ${joinColumns.mkString(", ")}")
    val df1Prepared = df1
    val resultColumns: Array[String] = df1Prepared.columns
    val resultColumnsDf1: Array[Column] = df1Prepared.columns.map(df1Prepared(_))
    val df2Prepared = df2
      .withColumnRenamed(hc.fromColName,hc.fromColName2).withColumnRenamed(hc.toColName,hc.toColName2)

    val joinCondition: Column = createKeyCondition(df1Prepared, df2Prepared, joinColumns)
      .and(col(hc.fromColName) <= col(hc.toColName2))
      .and(col(hc.fromColName2) <= col(hc.toColName))
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1Prepared.join(df2Prepared, joinCondition, "leftanti")
    logger.debug(s"temporalLeftAntiJoinImpl: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1Prepared.except(dfAntiJoin)
    // We need to temporally combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Prepared, joinCondition, "inner")
      .select(resultColumnsDf1 :+ col(hc.fromColName2) :+ col(hc.toColName2) :_*)
    logger.debug(s"temporalLeftAntiJoinImpl: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = temporalCombineImpl(dfJoin.select(hc.fromColName2, hc.toColName2+:joinColumns :_*), Seq())(ss,TemporalQueryConfig(hc.minDate:Timestamp,hc.maxDate,hc.fromColName2,hc.toColName2,hc.additionalTechnicalColNames))
    logger.debug(s"temporalLeftAntiJoinImpl: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (joinColumns.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined) else {
      df1ExceptAntiJoin.join(df2Combined, joinColumns, "inner")
    }
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(resultColumnsDf1:_*)
      .agg(collect_set(struct(col(hc.fromColName2).as("_1"),col(hc.toColName2).as("_1"))).as("subtrahend"))
      .withColumn("complement_array", udf_temporalComplement(hc)(col(hc.fromColName), col(hc.toColName), col("subtrahend")))
      .cache()
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend",hc.fromColName,hc.toColName)
      .withColumn(hc.fromColName,col("complements._1"))
      .withColumn(hc.toColName,col("complements._2"))
      .select(resultColumns.head, resultColumns.tail :_*)
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private def temporalCombineImpl( df:DataFrame, ignoreColNames:Seq[String]  )
                                 (implicit ss:SparkSession, hc:TemporalQueryConfig): DataFrame = {
    import ss.implicits._
    val dfColumns = df.columns
    val compairCols: Array[String] = dfColumns.diff( ignoreColNames ++ hc.technicalColNames )
    val fenestra: WindowSpec = Window.partitionBy(compairCols.map(col):_*).orderBy(col(hc.fromColName))

    df.temporalRoundDiscreteTime(hc)
      .withColumn("_consecutive", coalesce(udf_predecessorTime(hc)(col(hc.fromColName)) <= lag(col(hc.toColName),1).over(fenestra),lit(false)))
      .withColumn("_nb", sum(when($"_consecutive",lit(0)).otherwise(lit(1))).over(fenestra))
      .groupBy( compairCols.map(col):+$"_nb":_*)
      .agg( min(col(hc.fromColName)).as(hc.fromColName) , max(col(hc.toColName)).as(hc.toColName))
      .drop($"_nb")
      .select(dfColumns.head,dfColumns.tail:_*)
  }

  /**
   * Unify ranges
   */
  private def temporalUnifyRangesImpl( df:DataFrame, keys:Seq[String], extend: Boolean = false, fillGapsWithNull: Boolean = false )
                                     (implicit ss:SparkSession, hc:TemporalQueryConfig) = {
    import ss.implicits._
    // get ranges
    val df_ranges = temporalRangesImpl( df, keys, extend )
    val keyCondition = createKeyCondition( df, df_ranges, keys )
    // join back on input df
    val joinType = if (fillGapsWithNull) "left" else "inner"
    val df_join = df_ranges.join( df, keyCondition and $"range_von".between(col(hc.fromColName),col(hc.toColName)), joinType )
    // select result
    val selCols = keys.map(df_ranges(_)) ++ df.columns.diff(keys ++ hc.technicalColNames).map(df_join(_)) :+
      $"range_von".as(hc.fromColName) :+ $"range_bis".as(hc.toColName)
    df_join.select(selCols:_*)
  }

  /**
   * extend gueltig_ab/bis to min/maxDate
   */
  def temporalExtendRangeImpl( df:DataFrame, keys:Seq[String], extendMin:Boolean, extendMax:Boolean )(implicit ss:SparkSession, hc:TemporalQueryConfig): DataFrame = {
    import ss.implicits._
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn( "_gueltig_ab_min", if( extendMin ) min(col(hc.fromColName)).over(Window.partitionBy(keyCols:_*)) else lit(null))
      .withColumn( "_gueltig_bis_max", if( extendMax ) max(col(hc.toColName)).over(Window.partitionBy(keyCols:_*)) else lit(null))
    val selCols = df.columns.filter( c => c!=hc.fromColName && c!=hc.toColName ).map(col) :+ when(col(hc.fromColName)===$"_gueltig_ab_min", lit(hc.minDate)).otherwise(col(hc.fromColName)).as(hc.fromColName) :+
      when(col(hc.toColName)===$"_gueltig_bis_max", lit(hc.maxDate)).otherwise(col(hc.toColName)).as(hc.toColName)
    df_prep.select( selCols:_* )
  }

}
