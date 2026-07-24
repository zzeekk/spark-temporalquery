package ch.zzeekk.spark.temporalquery.util

import ch.zzeekk.spark.temporalquery.Logging
import ch.zzeekk.spark.temporalquery.interval.{ClosedInterval, IntervalDef}
import ch.zzeekk.spark.temporalquery.multivarRange.{ClosedMultivarRangeQueryConfig, MultivarRangeQueryConfig, MultivarRangeQueryImpl}
import org.apache.spark.sql.functions.{col, greatest, least, lit}
import org.apache.spark.sql.{Column, DataFrame}
import org.slf4j.Logger

import scala.reflect.runtime.universe.TypeTag

object MultivariateRangeLibrary extends Logging {

  /**
   * Pimp-my-library pattern for Columns
   */
  implicit class MultivarRangeColumnExtensions(value: Column) {
    def isInMultivariateRange[T: Ordering: TypeTag](implicit mrqc: MultivarRangeQueryConfig[T, _]): Column =
      mrqc.isInIntervalExpr(List(value))
  }

  /**
   * Pimp-my-library pattern for DataFrame
   */
  implicit class MultivariateRangeFrameExtensions(df1: DataFrame) {

    /**
     * Implements an inner join of historical data over a list of equally named columns
     */
    def multivarRangeInnerJoin[T: Ordering: TypeTag](df2: DataFrame, keys: Seq[String])(implicit
        mrqc: MultivarRangeQueryConfig[T, _],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.joinIntervalsWithKeysImpl(df1, df2, keys)

    /**
     * Implements an inner join of historical data over an explicit join condition
     */
    def multivarRangeInnerJoin[T: Ordering: TypeTag](df2: DataFrame, keyCondition: Column)(implicit
        mrqc: MultivarRangeQueryConfig[T, _],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.joinIntervals(df1, df2, keys = Nil, joinType = "inner", keyCondition)

    /**
     * Implements a full outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df1 or df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in the other
     *   frame can occur. If df1 or df2 is to be joined as a one-to-many relation (allowing
     *   multiplication of records from df1/df2), set rnkExpressions = Nil to disable this
     *   deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the full join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to both input
     *   DataFrames (default = true)
     */
    def multivarRangeFullJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "full", doCleanupExtend)

    /**
     * Implements a left outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in df1 can
     *   occur. If df2 is to be joined as a one-to-many relation (allowing multiplication of records
     *   from df1), set rnkExpressions = Nil to disable this deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the left join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfRight (default = true)
     */
    def multivarRangeLeftJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions,
        additionalJoinFilterCondition, "left", doCleanupExtend)

    /**
     * Implements a right outer join of historical data over a list of equally named columns
     *
     * @param rnkExpressions
     *   : In case df1 or df2 does not have a temporal 1-1-mapping, i.e. keys :+ fromColName are not
     *   unique, rnkExpressions is used to select exactly one row per point in time. This
     *   corresponds to a join with the constraint that no multiplication of records in the other
     *   frame can occur. If df1 or df2 is to be joined as a one-to-many relation (allowing
     *   multiplication of records from df1/df2), set rnkExpressions = Nil to disable this
     *   deduplication.
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the right join
     * @param doCleanupExtend
     *   Can be set to false if the cleanupExtend operation has already been applied to input
     *   DataFrame dfLeft (default = true)
     */
    def multivarRangeRightJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        keys: Seq[String],
        rnkExpressions: Seq[Column] = Nil,
        additionalJoinFilterCondition: Column = lit(true),
        doCleanupExtend: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .outerJoinIntervalsWithKey(df1, df2, keys, rnkExpressions, additionalJoinFilterCondition, "right", doCleanupExtend)

    /**
     * Implements a left anti join of historical data over a list of equally named columns
     *
     * @param additionalJoinFilterCondition
     *   : additional non-equi join conditions for the left anti join
     *
     * Note: this function is not yet supported on intervalDef's other than type ClosedInterval.
     */
    def multivarRangeLeftAntiJoin[T: Ordering: TypeTag](
        df2: DataFrame,
        joinColumns: Seq[String],
        additionalJoinFilterCondition: Column = lit(true)
    )(implicit
        mrqc: MultivarRangeQueryConfig[T, ClosedInterval[T]],
        logger: Logger
    ): DataFrame =
      MultivarRangeQueryImpl.leftAntiJoinIntervals(df1, df2, joinColumns, additionalJoinFilterCondition)

    /**
     * Resolves temporal overlaps
     *
     * @param rnkExpressions
     *   : priority expressions for deduplication
     * @param aggExpressions
     *   : aggregations to compute during deduplication
     * @param rnkFilter
     *   : if false, overlapping sections are only marked with rnk>1 but not filtered out
     * @param extend
     *   : if true and fillGapsWithNull=true, rows with null values are added for each key so that
     *   the entire time axis [minDate , maxDate] is covered for all keys
     * @param fillGapsWithNull
     *   : if true, gaps in the history are filled with null rows. fillGapsWithNull must be set to
     *   true for extend=true to have any effect
     */
    def multivarRangeCleanupExtend[T: Ordering: TypeTag](
        keys: Seq[String],
        rnkExpressions: Seq[Column],
        aggExpressions: Seq[(String, Column)] = Nil,
        rnkFilter: Boolean = true,
        extend: Boolean = true,
        fillGapsWithNull: Boolean = true
    )(implicit mrqc: MultivarRangeQueryConfig[T, _], logger: Logger): DataFrame = MultivarRangeQueryImpl
      .cleanupExtendIntervals(df1, keys, rnkExpressions, aggExpressions, rnkFilter, mrqc, extend, fillGapsWithNull)

    /**
     * Combines consecutive records when there is no change in the non-technical columns. The
     * dataframe is first cleaned up via [[multivarRangeRoundDiscreteTime]], see its description.
     */
    def multivarRangeCombine[T: Ordering: TypeTag](ignoreColNames: Seq[String] = Nil)(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.combineMultivarRanges(df1, ignoreColNames, mrqc)

    /**
     * Cuts records into pieces at overlaps, so that at the start of each overlap all active records
     * are split
     */
    def multivarRangeUnifyRanges[T: Ordering: TypeTag](
        keys: Seq[String],
        extend: Boolean = false,
        fillGapsWithNull: Boolean = false
    )(implicit
        mrqc: MultivarRangeQueryConfig[T, _ <: IntervalDef[T]],
        logger: Logger
    ): DataFrame = MultivarRangeQueryImpl.unifyMultivarRanges(df1, keys, extend, fillGapsWithNull, mrqc)

    /**
     * Extends the history of the smallest value per key to minDate
     */
    def multivarRangeExtendRange[T: Ordering: TypeTag](
        keys: Seq[String] = Nil,
        extendMin: Boolean = true,
        extendMax: Boolean = true
    )(implicit
        mrqc: MultivarRangeQueryConfig[T, _]
    ): DataFrame = MultivarRangeQueryImpl.extendIntervalRanges(df1, keys, extendMin, extendMax)

    /**
     * Sets the discreteness of the time scale to milliseconds. Hereby the validity intervals may be
     * shortened on the lower bound and extended on the upper bound. To the lower bound ceiling is
     * applied whereas to the upper bound flooring. If the dataframe has a discreteness of
     * millisecond or coarser, then the only two changes are: If a timestamp lies outside of
     * [minDate , maxDate] it will be replaced by minDate, maxDate respectively. Rows for which the
     * validity ends before it starts, i.e. with toCol.before(fromCol), are removed.
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   temporal dataframe with a discreteness of milliseconds
     */
    def multivarRangeRoundDiscreteTime[T: Ordering: TypeTag](implicit clmrqc: ClosedMultivarRangeQueryConfig[T]): DataFrame =
      MultivarRangeQueryImpl.roundIntervalsToDiscreteTime(df1, clmrqc)

    /**
     * Transforms [[DataFrame]] with continuous time, half open time intervals [fromColName ,
     * toColName [, to discrete time ([fromColName , toColName])
     *
     * Note: This function needs TemporalQueryConfig with a ClosedInterval definition
     *
     * @return
     *   [[DataFrame]] with discrete time axis
     */
    def multivarRangeContinuous2discrete[T: Ordering: TypeTag](implicit clmrqc: ClosedMultivarRangeQueryConfig[T]): DataFrame =
      MultivarRangeQueryImpl.transformHalfOpenToClosedIntervals(df1, clmrqc)

    /**
     * Renders the first two interval dimensions of the DataFrame as an SVG string.
     *
     * Each row becomes a `<rect>` whose horizontal extent maps to the first interval dimension and
     * whose vertical extent maps to the second; higher dimensions are ignored. A bounding rectangle
     * (no fill, black border) frames the entire data space.
     *
     * Colour encoding of `valueCol`:
     *   - Numeric columns (Double, Float, Long, Int, …): HSL heat-map from H=240 (blue, minimum
     *     value) to H=0 (red, maximum value) through the full visible spectrum.
     *   - Other types: a categorical palette of ten distinct colours.
     *   - Null values: grey (#cccccc).
     *
     * For [[ClosedInterval]] dimensions the rendered rectangle extends to `successor(to)` so that
     * the last discrete step is fully covered visually; for [[HalfOpenInterval]] the `to` value is
     * used directly. Rectangles are outlined only for closed intervals.
     *
     * Both axes share the same scale so that the aspect ratio of the data space is preserved; the
     * longer axis fills up to 1024 px.
     *
     * @param valueCol
     *   name of the column whose value determines the rectangle fill colour
     */
    def toSvg[T: Ordering: TypeTag](valueCol: String)(implicit mrqc: MultivarRangeQueryConfig[T, _]): String = {
      require(1 < mrqc.numDimensions,
        s"toSvg works only for multi-dimensional data but, numDimensions=${mrqc.numDimensions}")

      val dim1 = mrqc.intervalDimensions.head
      val dim2 = mrqc.intervalDimensions(1)
      val isClosed = dim1.intDef.isInstanceOf[ClosedInterval[_]]

      val clampedDf = {
        val rawCols = mrqc.intervalDimensions.flatMap { dim =>
          Seq(
            s"_raw_${dim.fromColName}" -> col(dim.fromColName),
            s"_raw_${dim.toColName}"   -> col(dim.toColName)
          )
        }.toMap
        val clampCols = mrqc.intervalDimensions.flatMap { dim =>
          val lo = lit(dim.lowerHorizon)
          val hi = lit(dim.upperHorizon)
          Seq(
            dim.fromColName -> least(greatest(col(dim.fromColName), lo), hi),
            dim.toColName   -> least(greatest(col(dim.toColName), lo), hi)
          )
        }.toMap
        df1.withColumns(rawCols).withColumns(clampCols)
      }
      val rows = clampedDf.collect()
      if (rows.isEmpty) return """<svg xmlns="http://www.w3.org/2000/svg"/>"""

      val schema = clampedDf.schema
      val from1Idx = schema.fieldIndex(dim1.fromColName)
      val to1Idx = schema.fieldIndex(dim1.toColName)
      val from2Idx = schema.fieldIndex(dim2.fromColName)
      val to2Idx = schema.fieldIndex(dim2.toColName)
      val valueIdx = schema.fieldIndex(valueCol)
      val rawFrom1Idx = schema.fieldIndex(s"_raw_${dim1.fromColName}")
      val rawTo1Idx = schema.fieldIndex(s"_raw_${dim1.toColName}")
      val rawFrom2Idx = schema.fieldIndex(s"_raw_${dim2.fromColName}")
      val rawTo2Idx = schema.fieldIndex(s"_raw_${dim2.toColName}")

      // For a closed interval [from, to] the visual extent ends at successor(to): the "to" value
      // is the last *included* discrete step, so the rectangle must cover one full step beyond it.
      // For a half-open interval [from, to) the "to" value is already the exclusive upper bound.
      // We precompute a per-dimension converter (Any => Double) to avoid passing the existential
      // intDef type to a typed parameter — Scala 2 won't auto-upcast the wildcard.
      def toDoubleVia(intDef: Any): Any => Double = intDef match {
        case ci: ClosedInterval[T @unchecked] => (v: Any) => anyToDouble(ci.successor(v.asInstanceOf[T]))
        case _                                => anyToDouble
      }
      val visualTo1 = toDoubleVia(dim1.intDef)
      val visualTo2 = toDoubleVia(dim2.intDef)

      // (from1, visualTo1, from2, visualTo2, value, rawFrom1, rawTo1, rawFrom2, rawTo2)
      val rects = rows.map { row =>
        (anyToDouble(row(from1Idx)), visualTo1(row(to1Idx)),
          anyToDouble(row(from2Idx)), visualTo2(row(to2Idx)),
          row(valueIdx),
          row(rawFrom1Idx), row(rawTo1Idx), row(rawFrom2Idx), row(rawTo2Idx))
      }

      // lowerHorizon / upperHorizon are sentinel "infinity" values (e.g. 1970-01-01 / 9999-12-31).
      // We extract them via a pattern match on Any to avoid Scala 2 existential-type restrictions.
      def horizonsOf(intDef: Any): (Double, Double) = intDef match {
        case id: IntervalDef[_] => (anyToDouble(id.lowerHorizon), anyToDouble(id.upperHorizon))
        case _                  => (Double.NegativeInfinity,      Double.PositiveInfinity)
      }
      val (lowerH1, upperH1) = horizonsOf(dim1.intDef)
      val (lowerH2, upperH2) = horizonsOf(dim2.intDef)

      // Derive the viewable range from the non-sentinel ("real") values only, then pad by 10 %.
      // This prevents the plot from being dominated by intervals that span all the way to the
      // infinity sentinels, which would compress all the interesting data into a thin green strip.
      def realBounds(vals: Array[Double], lo: Double, hi: Double): (Double, Double) = {
        val real = vals.filterNot(v => v == lo || v == hi)
        if (real.isEmpty) (vals.min, vals.max)
        else {
          val rMin = real.min
          val rMax = real.max
          val pad = if (rMax == rMin) math.abs(rMin) * 0.1 + 1d else (rMax - rMin) * 0.1
          (rMin - pad, rMax + pad)
        }
      }
      val (viewMinX, viewMaxX) = realBounds(rects.map(_._1) ++ rects.map(_._2), lowerH1, upperH1)
      val (viewMinY, viewMaxY) = realBounds(rects.map(_._3) ++ rects.map(_._4), lowerH2, upperH2)

      val dataW = viewMaxX - viewMinX
      val dataH = viewMaxY - viewMinY

      val svgMax = 1024d
      val scale = if (dataW == 0 && dataH == 0) 1d
      else if (dataW == 0) svgMax / dataH
      else if (dataH == 0) svgMax / dataW
      else math.min(svgMax / dataW, svgMax / dataH)

      val svgW = math.ceil(dataW * scale).toInt max 1
      val svgH = math.ceil(dataH * scale).toInt max 1

      // Margins outside the data rectangle for axis labels.
      val marginLeft = 20
      val marginBottom = 18
      val totalW = marginLeft + svgW
      val totalH = svgH + marginBottom

      // Data rectangle occupies [marginLeft, totalW) × [0, svgH).
      // Coordinates that map outside this area are clipped by SVG's overflow:hidden.
      def sx(x: Double): Double = marginLeft + (x - viewMinX) * scale
      def sy(y: Double): Double = svgH - (y - viewMinY) * scale

      // Convert HSL (h∈[0,360), s∈[0,1], l∈[0,1]) to a hex colour string
      def hsl2hex(h: Double, s: Double, l: Double): String = {
        val c = (1d - math.abs(2 * l - 1)) * s
        val x = c * (1d - math.abs(h / 60 % 2 - 1))
        val m = l - c / 2
        val (r1, g1, b1) =
          if (h < 60) (c, x, 0d)
          else if (h < 120) (x, c, 0d)
          else if (h < 180) (0d, c, x)
          else if (h < 240) (0d, x, c)
          else if (h < 300) (x, 0d, c)
          else (c,              0d, x)
        f"#${((r1 + m) * 255).round}%02x${((g1 + m) * 255).round}%02x${((b1 + m) * 255).round}%02x"
      }

      val nonNullValues = rects.flatMap { case (_, _, _, _, v, _, _, _, _) => Option(v) }
      val isNumeric = nonNullValues.nonEmpty && nonNullValues.forall(_.isInstanceOf[java.lang.Number])

      val fillOf: Any => String = if (isNumeric) {
        val nums = nonNullValues.map(_.asInstanceOf[java.lang.Number].doubleValue())
        val minVal = nums.min
        val range = nums.max - minVal
        (v: Any) =>
          v match {
            case null                => "#cccccc"
            case n: java.lang.Number =>
              val t = if (range == 0) 0.5 else (n.doubleValue() - minVal) / range
              hsl2hex(240d * (1d - t), 1d, 0.5) // hue: 240 = blue, 0 = red
            case _ => "#cccccc"
          }
      } else {
        val palette = Array("#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
          "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf")
        val catMap = nonNullValues.distinct.zipWithIndex
          .map { case (v, i) => v -> palette(i % palette.length) }.toMap
        (v: Any) => if (v == null) "#cccccc" else catMap.getOrElse(v, "#808080")
      }

      val strokeAttr = if (isClosed) """ stroke="black" stroke-width="0.5"""" else ""

      val sb = new StringBuilder
      sb.append(s"""<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 $totalW $totalH">\n""")
      // Data bounding rectangle – positioned at (marginLeft, 0), sized svgW × svgH
      sb.append(s"""  <rect x="$marginLeft" y="0" width="$svgW" height="$svgH" fill="none" stroke="black" stroke-width="1"/>\n""")
      // Axis labels in the margin areas, outside the data rectangle
      val fs = 11
      val xCenter = marginLeft / 2 // centre of left margin for rotated Y labels
      val yLow = svgH * 3 / 4 // lower-half position for "from" label
      val yHigh = svgH / 4 // upper-half position for "to" label
      val yText = svgH + marginBottom - 4 // baseline in bottom margin
      sb.append(s"""  <text x="${marginLeft +
          2}" y="$yText" font-size="$fs" fill="#444" text-anchor="start">${dim1.fromColName}</text>\n""")
      sb.append(s"""  <text x="${totalW - 2}" y="$yText" font-size="$fs" fill="#444" text-anchor="end">${dim1.toColName}</text>\n""")
      sb.append(
        s"""  <text x="$xCenter" y="$yLow" font-size="$fs" fill="#444" text-anchor="middle" transform="rotate(-90,$xCenter,$yLow)">${dim2.fromColName}</text>\n"""
      )
      sb.append(
        s"""  <text x="$xCenter" y="$yHigh" font-size="$fs" fill="#444" text-anchor="middle" transform="rotate(-90,$xCenter,$yHigh)">${dim2.toColName}</text>\n"""
      )
      for ((from1, to1, from2, to2, value, rawF1, rawT1, rawF2, rawT2) <- rects) {
        sb.append(
          s"  <!-- ${dim1.fromColName}=$rawF1 ${dim1.toColName}=$rawT1 ${dim2.fromColName}=$rawF2 ${dim2.toColName}=$rawT2 value=$value -->\n"
        )
        val x = sx(from1)
        val y = sy(to2)
        val w = (to1 - from1) * scale
        val h = (to2 - from2) * scale
        sb.append(f"""  <rect x="$x%.2f" y="$y%.2f" width="$w%.2f" height="$h%.2f" fill="${fillOf(value)}"$strokeAttr/>\n""")
        // Intersect with the data area [marginLeft, marginLeft+svgW] × [0, svgH]
        val vx0 = math.max(x, marginLeft.toDouble)
        val vx1 = math.min(x + w, (marginLeft + svgW).toDouble)
        val vy0 = math.max(y, 0d)
        val vy1 = math.min(y + h, svgH.toDouble)
        val visW = vx1 - vx0
        val visH = vy1 - vy0
        if (visW > 0 && visH > 0) {
          val fontSize = math.min(visW, visH) / 2
          val cx = (vx0 + vx1) / 2
          val cy = (vy0 + vy1) / 2
          sb.append(
            f"""  <text x="$cx%.2f" y="$cy%.2f" font-size="$fontSize%.2f" fill="black" text-anchor="middle" dominant-baseline="middle">$value</text>\n"""
          )
        }
      }
      sb.append("</svg>")
      sb.toString()
    }

  }

}
