package ch.zzeekk.spark.temporalquery

import java.sql.Timestamp

import org.apache.spark.sql._
import org.apache.spark.sql.expressions.{UserDefinedFunction, Window}
import org.apache.spark.sql.functions._

/*
Copyright (c) 2017 Zacharias Kull under MIT Licence

Usage:

import ch.zzeekk.spark-temporalquery.TemporalQueryUtil._ // this imports temporalquery* implicit functions on DataFrame
implicit val tqc = TemporalQueryConfig() // configure options for temporalquery operations
implicit val sss = ss // make SparkSession implicitly available

val df_joined = df1.temporalJoin(df2)
*/

object TemporalQueryUtil {

  /*
   Configuration Parameters. An instance of this class is needed as implicit parameter.
   */
  case class TemporalQueryConfig ( minDate:Timestamp = Timestamp.valueOf("0000-01-01 00:00:00")
                                 , maxDate:Timestamp = Timestamp.valueOf("9999-12-31 00:00:00")
                                 , fromColName:String    = "gueltig_ab"
                                 , toColName:String      = "gueltig_bis"
                                 , additionalTechnicalColNames:Seq[String] = Seq()) {
    val fromColName2:String = fromColName+"2"
    val toColName2:String = toColName+"2"
    val technicalColNames:Seq[String] = Seq( fromColName, toColName ) ++ additionalTechnicalColNames
  }

  /*
   Pimp-my-library pattern für's DataFrame
    */
  implicit class TemporalDataFrame(df1: Dataset[Row]) {
    /*
     implementiert ein inner-join von historisierten Daten über eine Liste von gleichbenannten Spalten
      */
    def temporalInnerJoin( df2:DataFrame, keys:Seq[String] )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalKeyJoinImpl( df1, df2, keys )
    }

    /*
     implementiert ein inner-join von historisierten Daten über eine ausformulierte Join-Bedingung
      */
    def temporalInnerJoin( df2:DataFrame, keyCondition:Column )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalJoinImpl( df1, df2, keyCondition )
    }

    /*
     implementiert ein left-outer-join von historisierten Daten über eine Liste von gleichbenannten Spalten
     - rnkExpressions: Priorität zum vorgängigen Bereinigen von Überlappungen in df2
     - additionalJoinFilterCondition: zusätzliche non-equi-join Bedingungen für den left-join
      */
    def temporalLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column = lit(true) )
                        (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalKeyLeftJoinImpl( df1, df2, keys, rnkExpressions, additionalJoinFilterCondition )
    }

    /*
     Löst zeitliche Überlappungen
     - rnkExpressions: Priorität zum Bereinigen
     - aggExpressions: Beim Bereinigen zu erstellende Aggregationen
     - rnkFilter: Wenn false werden überlappende Abschnitte nur mit rnk>1 markiert aber nicht gefiltert
      */
    def temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Seq(), rnkFilter:Boolean = true )
                             (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalCleanupExtendImpl( df1, keys, rnkExpressions, aggExpressions, rnkFilter )
    }

    /*
     Kombiniert aufeinanderfolgende Records des gleichen Keys, wenn es auf den Attributen keine Änderungen gibt
      */
    def temporalCombine( keys:Seq[String], ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalCombineImpl( df1, keys, ignoreColNames )
    }

    /*
     Scheidet bei Überlappungen die Records in Stücke, so dass beim Start der Überlappung alle gültigen Records aufgeteilt werden
      */
    def temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
      temporalUnifyRangesImpl( df1, keys )
    }

    /*
     Erweitert die Versionierung des kleinsten gueltig_ab pro Key auf minDate
      */
    def temporalExtendRange( keys:Seq[String], extendMin:Boolean=true, extendMax:Boolean=true )(implicit ss:SparkSession, hc:TemporalQueryConfig) = {
      temporalExtendRangeImpl( df1, keys, extendMin, extendMax )
    }
  }

  // helpers
  def plusMillisecond(tstmp: Timestamp)(implicit hc:TemporalQueryConfig) : Timestamp = {
    if (tstmp==null) tstmp
    else if (tstmp.before(hc.maxDate)) Timestamp.from(tstmp.toInstant.plusMillis(1))
    else tstmp
  }
  def minusMillisecond(tstmp: Timestamp)(implicit hc:TemporalQueryConfig) : Timestamp = {
    if (tstmp==null) tstmp
    else if (tstmp.before(hc.maxDate)) Timestamp.from(tstmp.toInstant.minusMillis(1))
    else tstmp
  }
  private val udf_hash:UserDefinedFunction = udf((row: Row) => row.hashCode)
  private def createKeyCondition( df1:DataFrame, df2:DataFrame, keys:Seq[String] ) : Column = {
    keys.foldLeft(lit(true)){ case (cond,key) => cond and df1(key)===df2(key) }
  }

  /*
   join two temporalquery dataframes
    */
  private def temporalJoinImpl( df1:DataFrame, df2:DataFrame, keyCondition:Column, joinType:String = "inner" )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    // history join
    val df2ren = df2.withColumnRenamed(hc.fromColName,hc.fromColName2).withColumnRenamed(hc.toColName,hc.toColName2)
    val df_join = df1.join( df2ren, keyCondition and col(hc.fromColName)<=col(hc.toColName2) and col(hc.toColName)>=col(hc.fromColName2), joinType)
    // select final schema
    val df1Cols = df1.columns.diff(hc.technicalColNames)
    val df2Cols = df2.columns.diff(df1Cols ++ hc.technicalColNames)
    val selCols = df1Cols.map(df1(_)) ++ df2Cols.map(df2(_)) :+ greatest(col(hc.fromColName), col(hc.fromColName2)).as(hc.fromColName) :+ least(col(hc.toColName), col(hc.toColName2)).as(hc.toColName)
    df_join.select(selCols:_*)
  }
  private def temporalKeyJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], joinType:String = "inner" )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    temporalJoinImpl( df1, df2, createKeyCondition(df1, df2, keys), joinType )
  }

  /*
   build ranges for keys to resolve overlaps, fill holes or extend to min/maxDate
    */
  private def temporalRangesImpl( df:DataFrame, keys:Seq[String], extend:Boolean )(implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    val udf_plusMillisecond = udf(plusMillisecond _)
    val udf_minusMillisecond = udf(minusMillisecond _)
    val keyCols = keys.map(col)
    // get start/end-points for every key
    val df_points = df.select( keyCols :+ col(hc.fromColName).as("_dt"):_*).union( df.select( keyCols :+ udf_plusMillisecond(col(hc.toColName)).as("_dt"):_* ))
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
      .withColumn( "range_bis", udf_minusMillisecond(lead(col("range_von"),1).over(Window.partitionBy(keys.map(col):_*).orderBy(col("range_von")))))
      .where(col("range_bis").isNotNull)
  }

  /*
   cleanup overlaps, fill holes and extend to min/maxDate
    */
  private def temporalCleanupExtendImpl( df:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)], rnkFilter:Boolean )
                           (implicit ss:SparkSession, hc:TemporalQueryConfig) : DataFrame = {
    import ss.implicits._
    // get ranges
    val df_ranges = temporalRangesImpl( df, keys, extend=true )
    val keyCondition = createKeyCondition( df, df_ranges, keys )
    // left join back on input df
    val df_join = df_ranges.join( df, keyCondition and $"range_von">=col(hc.fromColName) and $"range_von"<col(hc.toColName), "left" )
      .withColumn("_defined", col(hc.toColName).isNotNull)
    // add aggregations if defined, implemented as analytical functions...£
    val df_agg = aggExpressions.foldLeft( df_join ){
      case (df_acc, (name,expr)) => df_acc.withColumn(name, expr.over(Window.partitionBy( keys.map(df_ranges(_)):+$"range_von":_*)))
    }
    // Prioritize and clean overlaps
    val df_rnk = df_agg
      .withColumn("_rnk", row_number.over(Window.partitionBy( keys.map(df_ranges(_)):+$"range_von":_*).orderBy( rnkExpressions:_* )))
    val df_clean = if (rnkFilter) df_rnk.where($"_rnk"===1) else df_rnk
    // select final schema
    val selCols = keys.map(df_ranges(_)) ++ df.columns.diff(keys ++ hc.technicalColNames).map(df_clean(_)) ++ aggExpressions.map(e => col(e._1)) ++ (if (!rnkFilter) Seq($"_rnk") else Seq()) :+
      $"range_von".as(hc.fromColName) :+ $"range_bis".as(hc.toColName) :+ $"_defined"
    df_clean.select(selCols:_*)
  }

  /*
   left outer join
    */
  private def temporalKeyLeftJoinImpl( df1:DataFrame, df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) = {
    import ss.implicits._
    // extend df2
    val df2_extended = temporalCleanupExtendImpl( df2, keys, rnkExpressions, Seq(), rnkFilter=true )
    // left join df1 & df2
    temporalJoinImpl( df1, df2_extended, createKeyCondition(df1, df2, keys) and additionalJoinFilterCondition, "left" ).drop($"_defined")
  }

  /*
   Combine consecutive records with same data values
    */
  private def temporalCombineImpl( df:DataFrame, keys:Seq[String], ignoreColNames:Seq[String]  )
                     (implicit ss:SparkSession, hc:TemporalQueryConfig) = {
    import ss.implicits._
    val udf_plusMillisecond = udf(plusMillisecond _)
    val dataCols = df.columns.diff( keys ++ hc.technicalColNames )
    val hashCols = dataCols.diff( ignoreColNames )
    df.withColumn("_hash", if(hashCols.isEmpty) lit(1) else udf_hash(struct(hashCols.map(col(_)):_*)))
      .withColumn("_hash_prev", lag($"_hash",1).over(Window.partitionBy(keys.map(col):_*).orderBy(col(hc.fromColName))))
      .withColumn("_ersetzt_prev", lag(col(hc.toColName),1).over(Window.partitionBy(keys.map(col):_*).orderBy(col(hc.fromColName))))
      .withColumn("_consecutive", $"_hash_prev".isNotNull and $"_hash"===$"_hash_prev" and udf_plusMillisecond($"_ersetzt_prev")===col(hc.fromColName))
      .withColumn("_nb", sum(when($"_consecutive",lit(0)).otherwise(lit(1))).over(Window.partitionBy(keys.map(col):_*).orderBy(col(hc.fromColName))))
      .groupBy( keys.map(col):+$"_nb":_*).agg( count("*").as("_cnt"), dataCols.map(c => first(col(c)).as(c)) :+ min(col(hc.fromColName)).as(hc.fromColName) :+ max(col(hc.toColName)).as(hc.toColName):_* )
      .drop($"_cnt").drop($"_nb")
  }

  /*
   Unify ranges
    */
  private def temporalUnifyRangesImpl( df:DataFrame, keys:Seq[String] )
                         (implicit ss:SparkSession, hc:TemporalQueryConfig) = {
    import ss.implicits._
    // get ranges
    val df_ranges = temporalRangesImpl( df, keys, extend=false )
    val keyCondition = createKeyCondition( df, df_ranges, keys )
    // join back on input df
    val df_join = df_ranges.join( df, keyCondition and $"range_von">=col(hc.fromColName) and $"range_von"<col(hc.toColName) )
    // select result
    val selCols = keys.map(df_ranges(_)) ++ df.columns.diff(keys ++ hc.technicalColNames).map(df_join(_)) :+
      $"range_von".as(hc.fromColName) :+ $"range_bis".as(hc.toColName)
    df_join.select(selCols:_*)
  }

  /*
   extend gueltig_ab/bis to min/maxDate
    */
  def temporalExtendRangeImpl( df:DataFrame, keys:Seq[String], extendMin:Boolean, extendMax:Boolean )(implicit ss:SparkSession, hc:TemporalQueryConfig) = {
    import ss.implicits._
    val keyCols = if (keys.nonEmpty) keys.map(col(_)) else Seq(lit(1)) // if no keys are given, we work with the global minimum.
    val df_prep = df
      .withColumn( "_gueltig_ab_min", if( extendMin ) min(col(hc.fromColName)).over(Window.partitionBy(keyCols:_*)) else lit(null))
      .withColumn( "_gueltig_bis_max", if( extendMax ) max(col(hc.toColName)).over(Window.partitionBy(keyCols:_*)) else lit(null))
    val selCols = df.columns.filter( c => c!=hc.fromColName && c!=hc.toColName ).map(col) :+ when(col(hc.fromColName)===$"_gueltig_ab_min", lit(hc.minDate)).otherwise(col(hc.fromColName)).as(hc.fromColName) :+
      when(col(hc.toColName)===$"_gueltig_bis_max", lit(hc.maxDate)).otherwise(col(hc.toColName)).as(hc.toColName)
    df_prep.select( selCols:_* )
  }

}
