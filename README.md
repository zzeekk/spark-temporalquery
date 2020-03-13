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
  Inner Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Inner join" means that the result for a given key contains only periods which are defined in both DataFrames.
- `temporalInnerJoin( df2:DataFrame, keyCondition:Column )`
  Inner Join of two temporal datasets using a given expression as join condition
- `temporalLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )`
  Left Outer Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Left join" means that the result for a given key contains all periods from DataFrame 1 with null values for attributes of DataFrame 2 where it is not defined.
  - rnkExpressions: In case df2 is not a temporal 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a temporal 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df2 is a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.  
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true))`
  Left Anti Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Anti left join" means that the result contains all periods from DataFrame 1 which do not occur in DataFrame2 for the given joinColumns.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)], rnkFilter:Boolean = true )`
  Solve temporal overlaps by a prioritizing Records according to rnkExpressions and extend the temporal range of each key to be defined over the whole timeline. The resulting DataFrame has an additional column `_defined` which is false for extended ranges.
  - aggExpressions: Aggregates to be calculated on overlapping records (e.g. count)
  - rnkFilter: Flag if overlapping records should be tagged or filtered (default=filtered=true)
- `temporalCombine( keys:Seq[String] = Seq(), ignoreColNames:Seq[String] = Seq() )(implicit ss:SparkSession, hc:TemporalQueryConfig)`
  Combines successive records if there are no changes on the non-technical attributes.
  **The parameter `keys` is superfluous. Please refrain from using it!** It is still present in order not to break the API.
  - ignoreColName: A list of columns to be ignored in change detection
- `temporalUnifyRanges( keys:Seq[String] )(implicit ss:SparkSession, hc:TemporalQueryConfig)`
  Unify temporal ranges in group of records defined by 'keys' (needed for temporal aggregations).
- `temporalExtendRange( keys:Seq[String], extendMin:Boolean=true, extendMax:Boolean=true )`
  Extend temporal range to min/maxDate according to TemporalQueryConfig
