# spark-temporalquery
Implicit functions for querying temporal data with Apache Spark/Scala

## usage

- add maven dependency to project
  repo: https://dl.bintray.com/zach/zzeekk-release
  artifact: ch.zzeekk, spark-temporalquery, 0.9.5.1

- initialize
```
// this imports temporalquery* implicit functions on DataFrame
import ch.zzeekk.spark-temporalquery.TemporalQueryUtil._
// configure options for temporalquery operations
implicit val tqc = TemporalQueryConfig( fromColName="valid_from", toColName="valid_to")
// make SparkSession implicitly available
implicit val sss = spark
```

## operations
You can then use the following additional functions on Dataset/DataFrame
- `temporalInnerJoin( df2:DataFrame, keys:Seq[String] )`
  Inner Join of two temporal datasets using a list of key-columns named the same as condition (using-join)
- `temporalInnerJoin( df2:DataFrame, keyCondition:Column )`
  Inner Join of two temporal datasets using a given expression as join condition
- `def temporalLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column], additionalJoinFilterCondition:Column = lit(true))`
  Left Outer Join of two temporal datasets using a list of key-columns named the same as condition (using-join)
  - rnkExpressions: Defines the priorities to cleanup potential temporal overlaps in df2
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)], rnkFilter:Boolean = true )`
  Solve temporal overlaps by a prioritizing Records according to rnkExpressions and extend the temporal range of each key to be defined over the whole timeline. The resulting DataFrame has an additional column `_defined` which is false for extended ranges.
  - aggExpressions: Aggregates to be calculated on overlapping records (e.g. count)
  - rnkFilter: Flag if overlapping records should be tagged or filtered (default=filtered=true)
- `def temporalCombine( keys:Seq[String], ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, hc:TemporalQueryConfig)`
  Combine successive Records of the same key, if there are no changes on the attributes
  - ignoreColName: A list of columns to be ignored in change detection
- `temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, hc:TemporalQueryConfig)`
  Unify temporal ranges in group of records defined by 'keys' (needed for temporal aggregations).
- `def temporalExtendRange( keys:Seq[String], extendMin:Boolean=true, extendMax:Boolean=true )`
  Extend temporal range to min/maxDate according to TemporalQueryConfig
