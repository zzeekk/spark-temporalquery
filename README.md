# spark-temporalquery
Implicit functions for querying interval data with Apache Spark/Scala.
Features:
- support for closed interval and half open intervals (closed-from, open-to)
- support for discrete (timestamp, integer) and dense (double, float) interval axis datatype
- support for an arbitrary number of interval dimensions (1D linear, 2D bi-linear/bi-temporal, or N-dimensional via `GenericQueryUtil`)

Breaking changes in version 4.x:
- Spark-temporalquery is built and released only for Scala 2.13 with Spark 4.1.x.
  It is not built anymore for Scala 2.11, nor 2.12. Spark 3 is not supported.
- `Timestamp` is no longer treated as a special "temporal" axis type. It is now just another `Ordering` datatype for the interval axis, on par with `Double`/`Float`. The dedicated `TemporalQueryUtil` object together with its `TemporalClosedIntervalQueryConfig`/`TemporalHalfOpenIntervalQueryConfig` case classes has therefore been removed.
  Use `ch.zzeekk.spark.temporalquery.util.linear.TemporalClosedQueryUtil` (a `LinearGenericQueryUtil[Timestamp]`) instead, with its `LinearClosedIntervalQueryConfig`/`LinearHalfOpenIntervalQueryConfig` configuration case classes.
- As a consequence, the two separate implicit classes `TemporalDataFrameExtensions` and `LinearDataFrameExtensions` (with duplicated method sets) have been merged into a single `MultivariateRangeFrameExtensions` (`ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary`), which works uniformly regardless of the interval axis datatype and the number of dimensions.
  All operation methods have been renamed from the `temporalXyz`/`linearXyz` prefixes to `rangeXyz`, e.g. `temporalInnerJoin`/`linearInnerJoin` -> `rangeInnerJoin`, `temporalCombine` -> `rangeCombine`, `temporalCleanupExtend`/`linearCleanupExtend` -> `rangeCleanupExtend`.
- Classes have been reorganised into new packages: `interval` (`IntervalDef`, `ClosedInterval`, `HalfOpenInterval`), `axis` (`DiscreteAxisDef`, `DiscreteNumericAxis`, `DiscreteTimeAxis`), `multivarRange` (`MultivarRangeQueryConfig` and subclasses) and `util`/`util.linear`/`util.bilinear` for the concrete query-util objects. Adjust your imports accordingly.
- Added support for bi-dimensional (`BiLinearGenericQueryUtil`, e.g. `BiTemporalClosedIntervalQueryUtil`) and arbitrary N-dimensional (`GenericQueryUtil`) interval queries on the same DataFrame, based on the new dimension-aware `MultivarRangeQueryConfig`.

Breaking changes in version 3.x:
- Default lower horizon of TemporalClosedIntervalQueryConfig set to 1970-01-01 instead of 0001-01-01 for better compatibility.
  To instantiate the previous configuration use `TemporalClosedIntervalQueryConfig(intervalDef = ClosedInterval(Timestamp.valueOf("1970-01-01 00:00:00"), Timestamp.valueOf("9999-12-31 00:00:00"), DiscreteTimeAxis(ChronoUnit.MILLIS)))`
- Removed deprecated fields `TemporalClosedIntervalQueryConfig.min/maxDate` and `TemporalHalfOpenIntervalQueryConfig.min/maxDate`.
  Use `intervalDef.lower/upperHorizon` instead.

Breaking changes in version 2.x:
- temporalRoundDiscreteTime is no longer included in temporalCleanupExtend. Add it separately if needed.
- temporalCombine is no longer included in temporalCleanupExtend. Add it separately if needed. Note that this affects also temporal*Join methods.
- superfluous parameter `keys:Seq[String] = Nil` is removed from temporalCombine

## Usage
Spark-temporalquery releases are published on maven central.
To use it just add the following maven dependency for your Scala version to the project: 
```
<dependency>
  <groupId>ch.zzeekk.spark</groupId>
  <artifactId>spark-temporalquery_2.13</artifactId>
  <version>4.0.0</version>
</dependency>
```
See also [Builds](#builds) to review compatibility between Spark, Scala and Java.

All interval query operations are available implicit functions on DataFrame
by the implicit class `MultivariateRangeFrameExtensions`.

### temporal queries
`TemporalClosedQueryUtil` provides configuration for temporal data with a Timestamp interval axis.

```scala
import ch.zzeekk.spark.temporalquery.util.linear.TemporalClosedQueryUtil._
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import java.sql.Timestamp

// configure options: map from-column name to to-column name
implicit val tqc: LinearClosedIntervalQueryConfig = LinearClosedIntervalQueryConfig
  .withDefaultIntervalDef(fromColName = "valid_from", toColName = "valid_to")
// make SparkSession implicitly available
implicit val sss = spark

import sss.implicits._

// prepare some DataFrames
val dfLeft = Seq((0, Timestamp.valueOf("2017-12-10 00:00:00"), Timestamp.valueOf("2018-12-08 23:59:59.999"), 4.2))
  .toDF("id", "valid_from", "valid_to", "value_l")
val dfRight = Seq((0, Timestamp.valueOf("2018-01-01 00:00:00"), Timestamp.valueOf("2018-12-31 23:59:59.999"), 5))
  .toDF("id", "valid_from", "valid_to", "value_r")
// use multivarRange* functions
dfLeft.rangeInnerJoin(dfRight, Seq("id"))
```

### linear queries
`LinearGenericQueryUtil` provides configuration for data with a numeric interval axis.
The following concrete objects exist for predefined datatypes:
- `LinearDoubleQueryUtil`
- `LinearFloatQueryUtil`

```scala
import ch.zzeekk.spark.temporalquery.util.linear.LinearDoubleQueryUtil._
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.interval.HalfOpenInterval

// configure options: dimensionColNameMap maps from-column to to-column
implicit val lqc: LinearHalfOpenIntervalQueryConfig = LinearHalfOpenIntervalQueryConfig(
  dimensionColNameMap = Map("pos_from" -> "pos_to"),
  intervalDef = HalfOpenInterval(0d, Double.MaxValue)
)
// make SparkSession implicitly available
implicit val sss = session
import sss.implicits._
// prepare some DataFrames
val dfLeft = Seq((0, 0.0, 100.0, 4.2))
  .toDF("id", "pos_from", "pos_to", "value_l")
val dfRight = Seq((0, 50.0, 200.0, 5))
  .toDF("id", "pos_from", "pos_to", "value_r")
// use multivarRange* functions
dfLeft.rangeInnerJoin(dfRight, Seq("id"))
```

### bi-temporal and 2D range queries
`BiLinearGenericQueryUtil` provides configuration for data with two interval dimensions, e.g. a bi-temporal model with a known-time axis and a valid-time axis.
The following concrete objects exist for predefined datatypes:
- `BiTemporalClosedIntervalQueryUtil` (Timestamp, closed intervals)
- `BiTemporalHalfOpenIntervalQueryUtil` (Timestamp, half-open intervals)
- `BiLinearDoubleQueryUtil`
- `BiLinearFloatQueryUtil`

```scala
import ch.zzeekk.spark.temporalquery.util.bilinear.BiTemporalClosedIntervalQueryUtil._
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions

// configure options: two dimensions, each with its own from/to column pair
implicit val btqc: BiLinearClosedIntervalQueryConfig = BiLinearClosedIntervalQueryConfig
  .withDefaultIntervalDef(
    fstFromColName = "known_from", fstToColName = "known_to",
    sndFromColName = "valid_from", sndToColName = "valid_to"
  )
implicit val sss = spark
import sss.implicits._

dfLeft.rangeInnerJoin(dfRight, Seq("id"))
```

### N-dimensional range queries
`GenericQueryUtil` provides configuration for data with an arbitrary number of interval dimensions (not limited to 1 or 2 as with `LinearGenericQueryUtil`/`BiLinearGenericQueryUtil`). This is useful e.g. for hypercuboid-shaped data with more than two range dimensions.
The following concrete object exists for predefined datatypes:
- `GenericDoubleQueryUtil`
You can simply create a seven-dimensional query config with the following lines of code:
```scala
import ch.zzeekk.spark.temporalquery.util.GenericDoubleQueryUtil._

implicit val gqc: GenericHalfOpenIntervalQueryConfig = GenericHalfOpenIntervalQueryConfig
   .withDefaultIntervalDef(numDim = 7)
```

## Precondition

For temporal queries a time axis with datatype timestamp is needed. The axis can be configured as:
- ClosedInterval with different discrete step size, or
- HalfOpenInterval
via the `intervalDef` field of the query config (`LinearClosedIntervalQueryConfig`, `BiLinearClosedIntervalQueryConfig`, etc.).
The axis starts at `intervalDef.lowerHorizon` and ends at `intervalDef.upperHorizon`.

Before using the operations below you must ensure that your data satisfies the requirements of the chosen `intervalDef` configuration.
Moreover the data frame must not contain temporally overlapping entries or entries where validTo < validFrom as this will lead to confusing results.

### rangeCombine() to clean up data frames
You may use the method `rangeCleanupExtend` and `rangeCombine()` in order to clean up your data frame. For example
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
- `rangeInnerJoin( df2:DataFrame, keys:Seq[String] )`
  Inner Join of two interval datasets using a list of key-columns named the same as condition (using-join). "Inner join" means that the result for a given key contains only periods which are defined in both DataFrames.
- `rangeInnerJoin( df2:DataFrame, keyCondition:Column )`
  Inner Join of two interval datasets using a given expression as join condition.
- `rangeFullJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Nil, additionalJoinFilterCondition:Column = lit(true), doCleanupExtend:Boolean = true )`
  Full Outer Join of two interval datasets using a list of key-columns named the same as condition (using-join). "Outer join" means that the result for a given key contains all periods from DataFrame 1, 2 respectively, with null values for attributes of DataFrame 2, 1 respectively, where the period is missing from Data Frame 2, 1 respectively.
  - rnkExpressions: In case df1 or df2 are not a 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df1 or df2 are a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
  - doCleanupExtend: set to false if `rangeCleanupExtend` has already been applied to both input DataFrames (default=true).
- `rangeLeftJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Nil, additionalJoinFilterCondition:Column = lit(true), doCleanupExtend:Boolean = true )`
  Left Outer Join of two interval datasets using a list of key-columns named the same as condition (using-join). "Left join" means that the result for a given key contains all periods from DataFrame 1 with null values for attributes of DataFrame 2 where the period is missing from Data Frame 2.
  - rnkExpressions: In case df2 is not a 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df2 is a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
  - doCleanupExtend: set to false if `rangeCleanupExtend` has already been applied to df2 (default=true).
- `rangeRightJoin( df2:DataFrame, keys:Seq[String], rnkExpressions:Seq[Column] = Nil, additionalJoinFilterCondition:Column = lit(true), doCleanupExtend:Boolean = true )`
  Right Outer Join of two interval datasets using a list of key-columns named the same as condition (using-join). "Right join" means that the result for a given key contains all periods from DataFrame 2 with null values for attributes of DataFrame 1 where the period is missing from Data Frame 1.
  - rnkExpressions: In case df1 is not a 1-to-1 or many-to-1 mapping, this parameter is used to select a sub-dataFrame which constitutes a 1-1 mapping:
   by ordering for each key according to rnkExpressions and selecting the first row. In case df1 is a to-many relation you need to skip this cleaning by not setting the parameter rnkExpressions or by setting it to the empty sequence.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
  - doCleanupExtend: set to false if `rangeCleanupExtend` has already been applied to df1 (default=true).
- `rangeLeftAntiJoin( df2:DataFrame, joinColumns:Seq[String], additionalJoinFilterCondition:Column = lit(true))`
  Left Anti Join of two interval datasets using a list of key-columns named the same as condition (using-join). "Anti left join" means that the result contains all periods from DataFrame 1 which do not occur in DataFrame 2 for the given joinColumns.
  - additionalJoinFilterCondition: you can provide additional non-equi-join conditions which will be combined with the conditions generated from the list of keys.
- `rangeCleanupExtend( keys:Seq[String], rnkExpressions:Seq[Column], aggExpressions:Seq[(String,Column)] = Nil, rnkFilter:Boolean = true, extend:Boolean = true, fillGapsWithNull:Boolean = true )`
  Resolve interval overlaps by prioritizing records according to rnkExpressions and extend the range of each key to cover the whole configured horizon. The resulting DataFrame has an additional column `_defined` which is false for extended ranges.
  - aggExpressions: Aggregates to be calculated on overlapping records (e.g. count)
  - rnkFilter: Flag if overlapping records should be tagged or filtered (default=filtered=true)
  - extend: If true and fillGapsWithNull=true, every key is extended with additional records with null values so that for every key the whole horizon [lowerHorizon, upperHorizon] is covered (default=true)
  - fillGapsWithNull: If true, gaps in history are filled with records with null values for every key (default=true)
  - Note: extend=true needs fillGapsWithNull=true in order to work
- `rangeCombine( ignoreColNames:Seq[String] = Nil )`
  Combines successive records if there are no changes on the non-technical attributes.
  - ignoreColNames: A list of columns to be ignored in change detection
- `rangeUnifyRanges( keys:Seq[String], extend:Boolean = false, fillGapsWithNull:Boolean = false )`
  Unify interval ranges in a group of records defined by 'keys' by cutting records at overlap boundaries (needed for interval aggregations).
- `rangeExtendRange( keys:Seq[String] = Nil, extendMin:Boolean = true, extendMax:Boolean = true )`
  Extend interval range to lowerHorizon/upperHorizon according to the configured intervalDef.
- `rangeDense2discrete`
  Transforms a DataFrame with dense, half-open time intervals to discrete, closed intervals.
  Note: this function only works on intervalDef's of type ClosedInterval.
- `rangeRoundDiscreteTime`
  Sets the discreteness of the time scale to the discrete step size configured in the intervalDef.
  Note: this function only works on intervalDef's of type ClosedInterval.
- `toSvg( valueCol:String )`
  Renders the first two interval dimensions of the DataFrame as an SVG string. Each row becomes a rectangle whose horizontal extent maps to the first interval dimension and vertical extent to the second. Colour-encodes `valueCol`: numeric columns use a heat-map (blue→red), other types use a categorical palette, null values are grey.
  Note: this function requires at least two interval dimensions (i.e. a BiLinear or higher-dimensional config).

## Builds
Spark-temporalquery is built and released for Scala 2.13 with Spark 4.1.x.
We recommend to use Java 17 - 21.
See also https://spark.apache.org/docs/latest/#downloading. 

## Troubleshooting

### AnalysisException: Column ... is ambiguous.
On exception `org.apache.spark.sql.AnalysisException: Column ... are ambiguous. It's probably because you joined several Datasets together, and some of these Datasets are the same. ...` when using `multivarRange*Join` methods, try to use df.alias on both DataFrames before joining.
If temporal-query finds aliases it will use them in the join conditions.

The exception might remain. In these cases you can disable the check by setting Spark property `spark.sql.analyzer.failAmbiguousSelfJoin = false`.

