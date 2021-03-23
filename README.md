# spark-temporalquery
Implicit functions for querying temporal data with Apache Spark/Scala.

## Usage

- add maven dependency to project
  repo: https://dl.bintray.com/zach/zzeekk-release
  artifact: ch.zzeekk, spark-temporalquery, 1.0.0

- initialize
```scala
// this imports temporalquery* implicit functions on DataFrame
import ch.zzeekk.spark.temporalquery.TemporalQueryUtil._
// configure options for temporalquery operations
implicit val tqc = TemporalQueryConfig( fromColName="valid_from", toColName="valid_to")
// make SparkSession implicitly available
implicit val sss = spark
```

## Precondition

The library assumes a discrete and closed time axis with 1 millisecond as smallest time unit. The time starts at `TemporalQueryConfig.minDate` and ends at `TemporalQueryConfig.maxDate`.

Before using the operations below you must ensure that your data satisfies this conditions.
Moreover the data frame must not contain temporally overlapping entries or entries where validTo < validFrom as this will lead to wrong results.

### temporalCombine() to clean up data frames
You may use the method `temporalCombine()` in order to clean up your data frame. For example
<table>
    <tr><th>Id</th><th>val</th><th>validFrom</th><th>validTo</th><th>comment</th></tr>
    <tr><td>1</td><td>2.72</td><td>2019-01-05 12:34:56.123456789</td><td>2019-02-01 02:34:56.1235</td><td>nanoseconds</td></tr>
    <tr><td>1</td><td>2.72</td><td>2019-02-01 01:00:00.0</td><td>2019-02-01 02:34:56.1245</td><td>overlaps with previous</td></tr>
    <tr><td>1</td><td>2.72</td><td>2019-02-10 00:00:0</td><td>2019-02-09 00:00:0</td><td>ends before it starts</td></tr>
    <tr><td>1</td><td>42.0</td><td>2019-01-01 00:00:0</td><td>2019-12-31 23:59:59.999</td><td>does not overlap because different value:<br />many-to-many relation</td></tr>
</table>
is combined to
<table>
    <tr><th>Id</th><th>val</th><th>validFrom</th><th>validTo</th></tr>
    <tr><td>1</td><td>2.72</td><td>2019-01-05 12:34:56.124</td><td>2019-02-01 02:34:56.124</td></tr>
    <tr><td>1</td><td>42.0</td><td>2019-01-01 00:00:0</td><td>2019-12-31 23:59:59.999</td></tr>
</table>


## Operations
You can then use the following additional functions on Dataset/DataFrame
- `temporalInnerJoin( df2:DataFrame, keys:Seq[String] )`
  Inner Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Inner join" means that the result for a given key contains only periods which are defined in both DataFrames.
- `temporalInnerJoin( df2:DataFrame, keyCondition:Column )`
  Inner Join of two temporal datasets using a given expression as join condition
- `temporalFullJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )`
  Full Outer Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Outer join" means that the result for a given key contains all periods from DataFrame 1, 2 respectively, with null values for attributes of DataFrame 2, 1 respectively, where the period is missing from Data Frame 2, 1 respectively.
  - rnkExpressions: In case df1 or df2 are not a temporal 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a temporal 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df1 or df2 are a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.  
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )`
  Left Outer Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Left join" means that the result for a given key contains all periods from DataFrame 1 with null values for attributes of DataFrame 2 where the period is missing from Data Frame 2.
  - rnkExpressions: In case df2 is not a temporal 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a temporal 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df2 is a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.  
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalRightJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Seq(), additionalJoinFilterCondition:Column = lit(true) )`
  Right Outer Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Right join" means that the result for a given key contains all periods from DataFrame 2 with null values for attributes of DataFrame 1 where the period is missing from Data Frame 1.
  - rnkExpressions: In case df1 is not a temporal 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a temporal 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df1 is a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.  
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true))`
  Left Anti Join of two temporal datasets using a list of key-columns named the same as condition (using-join). "Anti left join" means that the result contains all periods from DataFrame 1 which do not occur in DataFrame2 for the given joinColumns.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)], rnkFilter:Boolean = true, extend: Boolean = true, fillGapsWithNull: Boolean = true )`
  Solve temporal overlaps by a prioritizing Records according to rnkExpressions and extend the temporal range of each key to be defined over the whole timeline. The resulting DataFrame has an additional column `_defined` which is false for extended ranges.
  - aggExpressions: Aggregates to be calculated on overlapping records (e.g. count)
  - rnkFilter: Flag if overlapping records should be tagged or filtered (default=filtered=true)
  - extend: If true and fillGapsWithNull=true, every key is extended with additional records with null values, so that for every key the whole timeline [minDate , maxDate] is covered (default=extend=true)
  - fillGapsWithNull: If true, gaps in history are filled with records with null values for every key (default=fillGapsWithNull=true)
  - Note: extend=true needs fillGapsWithNull=true in order to work
- `temporalCombine( keys:Seq[String] = Seq(), ignoreColNames:Seq[String] = Seq() )`
  Combines successive records if there are no changes on the non-technical attributes.
  **The parameter `keys` is superfluous. Please refrain from using it!**
  It is still present in order not to break the API.
  - ignoreColName: A list of columns to be ignored in change detection
- `temporalUnifyRanges( keys:Seq[String] )`
  Unify temporal ranges in group of records defined by 'keys' (needed for temporal aggregations).
- `temporalExtendRange( keys:Seq[String], extendMin:Boolean=true, extendMax:Boolean=true )`
  Extend temporal range to min/maxDate according to TemporalQueryConfig
- `temporalContinuous2discrete`
  transforms a data frame with continuous time to a frame with discrete time
- `temporalRoundDiscreteTime`
  sets the discreteness of the time scale to milliseconds
