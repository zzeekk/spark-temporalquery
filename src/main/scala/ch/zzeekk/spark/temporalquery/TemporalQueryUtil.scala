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
  case class TemporalQueryConfig ( minDate: Timestamp = Timestamp.valueOf("0001-01-01 00:00:00")
                                   , maxDate: Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")
                                   , fromColName: String    = "gueltig_ab"
                                   , toColName: String      = "gueltig_bis"
                                   , additionalTechnicalColNames: Seq[String] = Seq()) {
    // 2nd pair of from/to column names
    val fromColName2: String = fromColName+"2"
    val toColName2: String = toColName+"2"
    // technical column names to be excluded in some operations
    val technicalColNames: Seq[String] = Seq( fromColName, toColName ) ++ additionalTechnicalColNames
    // helper column names
    val definedColName: String = "_defined"
    // prepared column objects (not serializable)
    @transient lazy val fromCol: Column = col(fromColName)
    @transient lazy val toCol: Column = col(toColName)
    @transient lazy val fromCol2: Column = col(fromColName2)
    @transient lazy val toCol2: Column = col(toColName2)
    @transient lazy val definedCol: Column = col(definedColName)
    // configuration with 2nd pair of from/to column names used as main pair
    def config2: TemporalQueryConfig = this.copy(fromColName = fromColName2, toColName = toColName2)
  }

  /**
   * Pimp-my-library pattern für's DataFrame
   */
  implicit class TemporalDataFrame(df1: DataFrame) {

    /**
     * Implementiert ein inner-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     */
    def temporalInnerJoin( df2:DataFrame, keys:Seq[String] )
                         (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      temporalKeyJoinImpl( df1, df2, keys )
    }

    /**
     * Implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
     */
    def temporalInnerJoin( df2:DataFrame, keyCondition:Column )
                         (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
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
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = temporalKeyOuterJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full" )

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
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
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
                        (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      temporalKeyOuterJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right" )
    }

    /**
     * Implementiert ein left-anti-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     * - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-anti-join
     */
    def temporalLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true) )
                            (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
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
                             (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      temporalCleanupExtendImpl( df1, keys, rnkExpressions, aggExpressions, rnkFilter, extend, fillGapsWithNull )
    }

    /**
     * Kombiniert aufeinanderfolgende Records wenn es in den nichttechnischen Spalten keine Änderung gibt.
     * Zuerst wird der Dataframe mittels [[temporalRoundDiscreteTime]] etwas bereinigt, siehe Beschreibung dort
     *
     * @return temporal dataframe with combined validities
     */
    def temporalCombine( keys:Seq[String] = Seq() , ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      if(keys.nonEmpty) logger.warn("Parameter keys is superfluous and therefore ignored. Please refrain from using it!")
      temporalCombineImpl( df1, ignoreColNames )
    }

    /**
     * Schneidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle gültigen Records aufgeteilt werden
     */
    def temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
      temporalUnifyRangesImpl( df1, keys )
    }

    /**
     * Erweitert die Versionierung des kleinsten gueltig_ab pro Key auf minDate
     */
    def temporalExtendRange( keys:Seq[String]=Seq(), extendMin:Boolean=true, extendMax:Boolean=true )(implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame = {
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
    def temporalRoundDiscreteTime(implicit tc:TemporalQueryConfig): DataFrame = shrinkValidityImpl(df1)(udf_floorTimestamp(tc))

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName , toColName [, to discrete time ([fromColName , toColName])
     * @return [[DataFrame]] with discrete time axis
     */
    def temporalContinuous2discrete(implicit tc:TemporalQueryConfig): DataFrame = shrinkValidityImpl(df1)(udf_predecessorTime)

  }

  // helpers

  private def createKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and df1(key)===df2(key) }
  }

  private def shrinkValidityImpl(df: DataFrame)(udfFloorOrPred: UserDefinedFunction)(implicit tc:TemporalQueryConfig): DataFrame= {
    val spalten: Seq[String] = df.columns
    df.withColumn(tc.fromColName,udf_ceilTimestamp(tc)(tc.fromCol))
      .withColumn(tc.toColName,udfFloorOrPred(tc.toCol))
      .where(tc.fromCol <= tc.toCol)
      // return columns in same order as provided
      .select(spalten.map(col):_*)
  }

  /**
   * join two temporalquery dataframes
   * colsToConsolidate must occur in both data frames df1 and df2 and are consolidated in the result frame with df1 precedence over df2 using coalesce.
   * This is used to implement "natural join" and "join using" sql behaviour.
   */
  private def temporalJoinImpl( df1:DataFrame, df2:DataFrame, keyCondition:Column, joinType:String = "inner", colsToConsolidate: Seq[String] = Seq())(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
    require(!(df2.columns.contains(tc.fromColName2) || df2.columns.contains(tc.toColName2)),
      s"(temporalJoinImpl) Your right-dataframe must not contain columns named ${tc.fromColName2} or ${tc.toColName2}! df.columns = ${df2.columns}")
    require(colsToConsolidate.diff(df1.columns).isEmpty, s"(temporalJoinImpl) Your left-dataframe doesn't contain column to consolidate ${colsToConsolidate.diff(df1.columns).mkString(" and ")}")
    require(colsToConsolidate.diff(df2.columns).isEmpty, s"(temporalJoinImpl) Your right-dataframe doesn't contain column to consolidate ${colsToConsolidate.diff(df2.columns).mkString(" and ")}")

    // temporal join
    val df2ren = df2.withColumnRenamed(tc.fromColName,tc.fromColName2).withColumnRenamed(tc.toColName,tc.toColName2)
    val df_join = df1.join( df2ren, keyCondition and tc.fromCol <= tc.toCol2 and tc.toCol >= tc.fromCol2, joinType)

    // select final schema
    val dfCommonColNames = colsToConsolidate
    val commonCols = colsToConsolidate.map(colName => coalesce(df1(colName),df2(colName)).as(colName))
    val colsDf1 = df1.columns.diff(dfCommonColNames ++ tc.technicalColNames).map(df1(_))
    val colsDf2 = df2.columns.diff(dfCommonColNames ++ tc.technicalColNames).map(df2(_))
    val timeColumns = Seq(greatest(tc.fromCol, tc.fromCol2).as(tc.fromColName),least(tc.toCol, tc.toCol2).as(tc.toColName))
    val selCols = commonCols ++ colsDf1 ++ colsDf2 ++ timeColumns
    df_join.select(selCols:_*)
  }
  private def temporalKeyJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], joinType:String = "inner" )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
    temporalJoinImpl( df1, df2, createKeyCondition(df1, df2, keys), joinType, keys )
  }

  /**
   * build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
   */
  private def temporalRangesImpl( df:DataFrame, keys:Seq[String], extend:Boolean )(implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
    val ptColName = "_pt"

    require(!(df.columns.contains(tc.fromColName2) || df.columns.contains(tc.toColName2) || df.columns.contains(ptColName)),
      s"(temporalRangesImpl) Your dataframe must not contain columns named ${tc.fromColName2}, ${tc.toColName2} or $ptColName! df.columns = ${df.columns}")

    val keyCols = keys.map(col)
    // get start/end-points for every key
    val df_points = df.select( keyCols :+ tc.fromCol.as(ptColName):_*).union( df.select( keyCols :+ udf_successorTime(tc)(tc.toCol).as(ptColName):_* ))
    // if desired, extend every key with min/maxDate-points
    val df_pointsExt = if (extend) {
      df_points
        .union( df_points.select( keyCols:_*).distinct.withColumn( ptColName, lit(tc.minDate)))
        .union( df_points.select( keyCols:_*).distinct.withColumn( ptColName, lit(tc.maxDate)))
        .distinct
    } else df_points.distinct
    // build ranges
    df_pointsExt
      .withColumnRenamed(ptColName, tc.fromColName2)
      .withColumn( tc.toColName2, udf_predecessorTime(tc)(lead(tc.fromCol2,1).over(Window.partitionBy(keys.map(col):_*).orderBy(tc.fromCol2))))
      .where(tc.toCol2.isNotNull)
  }

  /**
   * cleanup overlaps, fill holes and extend to min/maxDate
   */
  private def temporalCleanupExtendImpl( df:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)]
                                         , rnkFilter:Boolean , extend: Boolean = true, fillGapsWithNull: Boolean = true )
                                       (implicit ss:SparkSession, tc:TemporalQueryConfig) : DataFrame = {
    if(extend && !fillGapsWithNull) logger.warn("temporalCleanupExtendImpl: extend=true has no effect if fillGapsWithNull=false!")

    require(!df.columns.contains(tc.fromColName2) && !df.columns.contains(tc.toColName2) && !df.columns.contains(tc.definedColName),
      s"(temporalCleanupExtendImpl) Your dataframe must not contain columns named ${tc.fromColName2}, ${tc.toColName2} or ${tc.definedColName}! df.columns = ${df.columns}")

    // use 2nd pair of from/to column names so that original pair can still be used in rnk- & aggExpressions
    val df2 = df.withColumn(tc.fromColName2, tc.fromCol).withColumn(tc.toColName2, tc.toCol)
    val tc2 = tc.config2
    val fenestra: WindowSpec = Window.partitionBy( keys.map(col):+ tc2.fromCol :_*)

    val df_join = temporalUnifyRangesImpl( df2, keys, extend, fillGapsWithNull)(ss, tc2)
      .withColumn(tc.fromColName, coalesce(tc.fromCol, tc2.fromCol))
      .withColumn(tc.toColName, coalesce(tc.toCol, tc2.toCol))
      .withColumn(tc.definedColName, tc2.toCol.isNotNull)

    // add aggregations if defined, implemented as analytical functions...£
    val df_agg = aggExpressions.foldLeft( df_join ){
      case (df_acc, (name,expr)) => df_acc.withColumn(name, expr.over(fenestra))
    }

    // Prioritize and clean overlaps
    val rnkColName = "_rnk"
    val df_clean = if (rnkExpressions.nonEmpty) {
      require(!df.columns.contains(rnkColName), s"(temporalCleanupExtendImpl) Your dataframe must not contain columns named $rnkColName if rnkExpressions are defined! df.columns = ${df.columns}")
      val df_rnk = df_agg.withColumn(rnkColName, row_number.over(fenestra.orderBy(rnkExpressions: _*)))
      if (rnkFilter) df_rnk.where(col(rnkColName)===1) else df_rnk
    } else df_agg

    // select final schema
    val selCols: Seq[Column] = keys.map(df_clean(_)) ++
      df.columns.diff(keys ++ tc.technicalColNames).map(df_clean(_)) ++
      aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter && rnkExpressions.nonEmpty) Seq(col(rnkColName)) else Seq()) :+
      df_clean(tc2.fromColName).as(tc.fromColName) :+ df_clean(tc2.toColName).as(tc.toColName) :+ tc.definedCol

    temporalCombineImpl( df_clean.select(selCols:_*) , Seq() )(ss,tc)
  }

  /**
   * outer join
   */
  private def temporalKeyOuterJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column, joinType:String)
                                     (implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame = {
    // extend df2
    val df1_extended = if (joinType=="full" || joinType=="right") temporalCleanupExtendImpl( df1, keys, rnkExpressions.intersect(df1.columns.map(col)), Seq(), rnkFilter=true ) else df1
    val df2_extended = if (joinType=="full" || joinType=="left") temporalCleanupExtendImpl( df2, keys, rnkExpressions.intersect(df2.columns.map(col)), Seq(), rnkFilter=true ) else df2
    // join df1 & df2
    temporalJoinImpl( df1_extended, df2_extended, createKeyCondition(df1, df2, keys) and additionalJoinFilterCondition, joinType, keys ).drop(tc.definedCol)
  }

  /**
   * left outer join
   */
  private def temporalLeftAntiJoinImpl( df1:DataFrame, df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column )
                                      (implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame = {
    logger.debug(s"temporalLeftAntiJoinImpl START: joinColumns = ${joinColumns.mkString(", ")}")
    val df1Prepared = df1
    val resultColumns: Array[String] = df1Prepared.columns
    val resultColumnsDf1: Array[Column] = df1Prepared.columns.map(df1Prepared(_))
    val df2Prepared = df2
      .withColumnRenamed(tc.fromColName,tc.fromColName2).withColumnRenamed(tc.toColName,tc.toColName2)

    val joinCondition: Column = createKeyCondition(df1Prepared, df2Prepared, joinColumns)
      .and(tc.fromCol <= tc.toCol2)
      .and(tc.fromCol2 <= tc.toCol)
      .and(additionalJoinFilterCondition)

    val dfAntiJoin = df1Prepared.join(df2Prepared, joinCondition, "leftanti")
    logger.debug(s"temporalLeftAntiJoinImpl: dfAntiJoin.schema = ${dfAntiJoin.schema.treeString}")

    val df1ExceptAntiJoin = df1Prepared.except(dfAntiJoin)
    // We need to temporally combine df2 but without the columns which are used in additionalJoinFilterCondition
    val dfJoin = df1ExceptAntiJoin.join(df2Prepared, joinCondition, "inner")
      .select(resultColumnsDf1 :+ tc.fromCol2 :+ tc.toCol2 :_*)
    logger.debug(s"temporalLeftAntiJoinImpl: dfJoin.schema = ${dfJoin.schema.treeString}")
    val df2Combined = temporalCombineImpl(dfJoin.select(tc.fromColName2, tc.toColName2 +: joinColumns :_*), Seq())(ss, tc.config2)
    logger.debug(s"temporalLeftAntiJoinImpl: df2Combined.schema = ${df2Combined.schema.treeString}")

    val dfComplementJoin = if (joinColumns.isEmpty) df1ExceptAntiJoin.crossJoin(df2Combined) else {
      df1ExceptAntiJoin.join(df2Combined, joinColumns, "inner")
    }
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplementJoin.schema = ${dfComplementJoin.schema.treeString}")

    val dfComplementJoin_complementArray = dfComplementJoin
      .groupBy(resultColumnsDf1:_*)
      .agg(collect_set(struct(tc.fromCol2.as("_1"), tc.toCol2.as("_2"))).as("subtrahend"))
      .withColumn("complement_array", udf_temporalComplement(tc)(tc.fromCol, tc.toCol, col("subtrahend")))
      .cache()
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplementJoin_complementArray.schema = ${dfComplementJoin_complementArray.schema.treeString}")

    val dfComplement = dfComplementJoin_complementArray
      .withColumn("complements", explode(col("complement_array")))
      .drop("subtrahend",tc.fromColName,tc.toColName)
      .withColumn(tc.fromColName,col("complements._1"))
      .withColumn(tc.toColName,col("complements._2"))
      .select(resultColumns.head, resultColumns.tail :_*)
    logger.debug(s"temporalLeftAntiJoinImpl: dfComplement.schema = ${dfComplement.schema.treeString}")

    dfAntiJoin.union(dfComplement)
  }

  /**
   * Combine consecutive records with same data values
   */
  private def temporalCombineImpl( df:DataFrame, ignoreColNames:Seq[String]  )
                                 (implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame = {
    val dfColumns = df.columns
    val compairCols: Array[String] = dfColumns.diff( ignoreColNames ++ tc.technicalColNames )
    val fenestra: WindowSpec = Window.partitionBy(compairCols.map(col):_*).orderBy(tc.fromCol)

    val nbColName = "_nb"
    val consecutiveColName = "_consecutive"
    require(!df.columns.contains(nbColName) && !df.columns.contains(consecutiveColName), s"(temporalCombineImpl) Your dataframe must not contain columns named $nbColName or $consecutiveColName! df.columns = ${df.columns}")
    df.temporalRoundDiscreteTime(tc)
      .withColumn(consecutiveColName, coalesce(udf_predecessorTime(tc)(tc.fromCol) <= lag(tc.toCol,1).over(fenestra),lit(false)))
      .withColumn(nbColName, sum(when(col(consecutiveColName),lit(0)).otherwise(lit(1))).over(fenestra))
      .groupBy( compairCols.map(col):+col(nbColName):_*)
      .agg( min(tc.fromCol).as(tc.fromColName) , max(tc.toCol).as(tc.toColName))
      .drop(nbColName)
      .select(dfColumns.head,dfColumns.tail:_*)
  }

  /**
   * Unify ranges
   */
  private def temporalUnifyRangesImpl( df:DataFrame, keys:Seq[String], extend: Boolean = false, fillGapsWithNull: Boolean = false )
                                     (implicit ss:SparkSession, tc:TemporalQueryConfig) = {
    // get ranges
    val df_ranges = temporalRangesImpl( df, keys, extend )( ss,tc )
    val keyCondition = createKeyCondition( df, df_ranges, keys )
    // join back on input df
    val joinType = if (fillGapsWithNull) "left" else "inner"
    val df_join = df_ranges.join( df, keyCondition and tc.fromCol2.between(tc.fromCol,tc.toCol), joinType )
    // select result
    val selCols = keys.map(df_ranges(_)) ++ df.columns.diff(keys ++ tc.technicalColNames).map(df_join(_)) :+
      tc.fromCol2.as(tc.fromColName) :+ tc.toCol2.as(tc.toColName)
    df_join.select(selCols:_*)
  }

  /**
   * extend gueltig_ab/bis to min/maxDate
   */
  def temporalExtendRangeImpl( df:DataFrame, keys:Seq[String], extendMin:Boolean, extendMax:Boolean )(implicit ss:SparkSession, tc:TemporalQueryConfig): DataFrame = {
    val fromMinColName = s"_${tc.fromColName}_min"
    val toMaxColName = s"_${tc.toColName}_max"
    require(!df.columns.contains(fromMinColName) && !df.columns.contains(toMaxColName), s"(temporalCombineImpl) Your dataframe must not contain columns named $fromMinColName or $toMaxColName! df.columns = ${df.columns}")
    val keyCols = if (keys.nonEmpty) keys.map(col) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn(fromMinColName, if( extendMin ) min(tc.fromCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
      .withColumn(toMaxColName, if( extendMax ) max(tc.toCol).over(Window.partitionBy(keyCols:_*)) else lit(null))
    val selCols = df.columns.filter( c => c!=tc.fromColName && c!=tc.toColName ).map(col) :+
      when(tc.fromCol===col(fromMinColName), lit(tc.minDate)).otherwise(tc.fromCol).as(tc.fromColName) :+
      when(tc.toCol===col(toMaxColName), lit(tc.maxDate)).otherwise(tc.toCol).as(tc.toColName)
    df_prep.select( selCols:_* )
  }

}
