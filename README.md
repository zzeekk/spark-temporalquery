# spark-temporalquery
Implicit functions for querying interval data with Apache Spark/Scala.
Features:
- support for closed interval and half open intervals (closed-from, open-to)
- support for discrete (timestamp, integer) and continuous (double, float) interval axis datatype

Breaking changes in version 2.x:
- temporalRoundDiscreteTime is no longer included in temporalCleanupExtend. Add it separately if needed.
- temporalCombine is no longer included in temporalCleanupExtend. Add it separately if needed. Note that this affects also temporal*Join methods.
- superfluous parameter `keys:Seq[String] = Seq()` is removed from temporalCleanupExtend

## Usage
- add maven dependency to project
  repo: https://dl.bintray.com/zach/zzeekk-release
  artifact: ch.zzeekk, spark-temporalquery, 2.0.0

### temporal queries
TemporalQueryUtil provides implicit function on DataFrame to query temporal data with timestamp interval axis datatype.
```
  // this imports temporal* implicit functions on DataFrame
  import ch.zzeekk.spark.temporalquery.TemporalQueryUtil._
  // configure options for temporal query operations
  val intervalDef = ClosedInterval(Timestamp.valueOf("0001-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.MILLIS))
  implicit val tqc = TemporalQueryConfig( fromColName="valid_from", toColName="valid_to", intervalDef = intervalDef)
  // make SparkSession implicitly available
  implicit val sss = spark
  import sss.implicits._
  // prepare some DataFrames
  val dfLeft = Seq((0, Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999"), 4.2))
    .toDF("id", "valid_from", "valid_to","value_l")
  val dfRight = Seq((0, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-12-31 23:59:59.999"), 5))
    .toDF("id", "valid_from", "valid_to","value_r")
  // use temporal* functions
  dfLeft.temporalInnerJoin(dfRight, Seq("id"))
```

### linear queries
LinearGenericQueryUtil provides implicit function on DataFrame to query linear data with any numeric interval axis datatype.
The following shortcuts exists to use it with predefined datatypes:
- LinearFloatQueryUtil
- LinearDoubleQueryUtil

```
  // this imports linear* implicit functions on DataFrame
  import ch.zzeekk.spark.temporalquery.LinearDoubleQueryUtil._
  // configure options for linear query operations
  val intervalDef = ClosedFromOpenToInterval(0d, Double.MaxValue)
  implicit val lqc: LinearQueryConfig = LinearQueryConfig(fromColName="pos_from", toColName="pos_to", intervalDef = intervalDef)
  // make SparkSession implicitly available
  implicit val sss = TemporalTestUtils.session
  import sss.implicits._
  // prepare some DataFrames
  val dfLeft = Seq((0, 0.0, 100.0, 4.2))
    .toDF("id", "pos_from", "pos_to","value_l")
  val dfRight = Seq((0, 50.0, 200.0, 5))
    .toDF("id", "pos_from", "pos_to","value_r")
  // use linear* functions
  dfLeft.linearInnerJoin(dfRight, Seq("id"))
```

The following sections are written for temporal queries, but the library works in the same way with linear queries by exchanging temporal* vs. linear* in function names.

## Precondition

For temporal queries a time axis with datatype timestamp is needed. The axis can be configured as:
- ClosedInterval with different discrete step size, or
- ClosedFromOpenToInterval
via `TemporalQueryConfig.intervalDef`
The axis starts at `TemporalQueryConfig.intervalDef.minValue` and ends at `TemporalQueryConfig.intervalDef.maxValue`.

Before using the operations below you must ensure that your data satisfies the requirements of the chosen `intervalDef` configuration.
Moreover the data frame must not contain temporally overlapping entries or entries where validTo < validFrom as this will lead to confusing results.

### temporalCombine() to clean up data frames
You may use the method `temporalCleanupExtend` and `temporalCombine()` in order to clean up your data frame. For example
<table>
    <tr><th>Id</th><th>val</th><th>validFrom</th><th>validTo</th><th>comment</th></tr>
    <tr><td>1</td><td>2.72</td><td>2019-01-05 12:34:56.123456789</td><td>2019-02-01 02:34:56.1235</td><td>nanoseconds</td></tr>
    <tr><td>1</td><td>2.72</td><td>2019-02-01 01:00:00.0</td><td>2019-02-01 02:34:56.1245</td><td>overlaps with previous</td></tr>
    <tr><td>1</td><td>2.72</td><td>2019-02-10 00:00:0</td><td>2019-02-09 00:00:0</td><td>ends before it starts</td></tr>
    <tr><td>1</td><td>42.0</td><td>2019-01-01 00:00:0</td><td>2019-12-31 23:59:59.999</td><td>does not overlap because different value:<br />many-to-many relation</td></tr>
</table>
is cleaned up to
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
  Note: this function is not yet supported on intervalDef's other than type ClosedInterval. 
- `temporalCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)], rnkFilter:Boolean = true, extend: Boolean = true, fillGapsWithNull: Boolean = true )`
  Solve temporal overlaps by a prioritizing Records according to rnkExpressions and extend the temporal range of each key to be defined over the whole timeline. The resulting DataFrame has an additional column `_defined` which is false for extended ranges.
  - aggExpressions: Aggregates to be calculated on overlapping records (e.g. count)
  - rnkFilter: Flag if overlapping records should be tagged or filtered (default=filtered=true)
  - extend: If true and fillGapsWithNull=true, every key is extended with additional records with null values, so that for every key the whole timeline [minDate , maxDate] is covered (default=extend=true)
  - fillGapsWithNull: If true, gaps in history are filled with records with null values for every key (default=fillGapsWithNull=true)
  - Note: extend=true needs fillGapsWithNull=true in order to work
- `temporalCombine( keys:Seq[String] = Seq(), ignoreColNames:Seq[String] = Seq() )`
  Combines successive records if there are no changes on the non-technical attributes.
  - ignoreColName: A list of columns to be ignored in change detection
- `temporalUnifyRanges( keys:Seq[String] )`
  Unify temporal ranges in group of records defined by 'keys' (needed for temporal aggregations).
- `temporalExtendRange( keys:Seq[String], extendMin:Boolean=true, extendMax:Boolean=true )`
  Extend temporal range to min/maxDate according to TemporalQueryConfig
- `temporalContinuous2discrete`
  transforms a data frame with continuous time to a frame with discrete time
  Note: this function only works on intervalDef's of type ClosedInterval. 
- `temporalRoundDiscreteTime`
  sets the discreteness of the time scale to the discrete step size chosen in TemporalQueryConfig
  Note: this function only works on intervalDef's of type ClosedInterval. 
