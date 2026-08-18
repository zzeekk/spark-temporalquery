package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util._
import ch.zzeekk.spark.temporalquery.util.bilinear.BiDatoralHalfOpenIntervalQueryUtil._
import ch.zzeekk.spark.temporalquery.{saveString2File, TestUtils}
import org.apache.spark.sql.functions.concat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class BiTemporalPresentation extends AnyFlatSpec with Matchers with TestUtils {

  import session.implicits._

  private implicit val mrqc: BiLinearHalfOpenIntervalQueryConfig = BiLinearHalfOpenIntervalQueryConfig
    .withDefaultIntervalDef(fstFromColName = "known_from", fstToColName = "known_to",
      sndFromColName = "valid_from", sndToColName = "valid_to")

  logger.info(s"BiTemporalQueryUtilTest: mrqc = $mrqc")

  "presentation" should "explain Bitemporality" in {
    val monthlyPremium = List(
      (0, "2022-11-13", "2023-10-01", "2023-01-01", doomsDateStr, 100),
      // each year 1st October: new insurance tariffs entered in database
      (0, "2023-10-01", doomsDateStr, "2023-01-01", "2024-01-01", 100),
      (0, "2023-10-01", "2024-10-01", "2024-01-01", doomsDateStr, 110),
      //
      (0, "2024-10-01", doomsDateStr, "2024-01-01", "2025-01-01", 110),
      (0, "2024-10-01", "2025-10-01", "2025-01-01", doomsDateStr, 120),
      //
      (0, "2025-10-01", doomsDateStr, "2025-01-01", "2026-01-01", 120),
      (0, "2025-10-01", "2026-02-01", "2026-01-01", doomsDateStr, 130),
      //
      (0, "2026-02-01", doomsDateStr, "2026-01-01", doomsDateStr, 145)
    ).map(makeRowsBiDatoral)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "premium")
      .orderBy("id", "premium", "known_from")
    logger.info(s"*** Data Frame monthlyPremium (${monthlyPremium.count()} rows) ***")
    monthlyPremium.printSchema()
    monthlyPremium.show(false)
    saveString2File("monthlyPremium.svg")(monthlyPremium.toSvg[Date]("premium"))

    val address = List(
      (0, "2022-11-13", "2025-09-01", "2023-01-01", doomsDateStr, "AG"),
      (0, "2025-09-01", doomsDateStr, "2023-01-01", "2025-08-01", "AG"),
      (0, "2025-09-01", doomsDateStr, "2025-08-01", doomsDateStr, "ZH")
    ).map(makeRowsBiDatoral)
      .toDF("id", "known_from", "known_to", "valid_from", "valid_to", "canton")
      .orderBy("id", "canton", "known_from")
    logger.info(s"*** Data Frame address (${address.count()} rows) ***")
    address.printSchema()
    address.show(false)
    saveString2File("address.svg")(address.toSvg[Date]("canton"))

    val cantonPremium = address.rangeInnerJoin[Date](df2 = monthlyPremium, keys = List("id"))
      .drop("id").rangeCombine[Date]().orderBy("canton", "premium", "known_from")
    cantonPremium.printSchema()
    logger.info(s"*** Data Frame cantonPremium (${cantonPremium.count()} rows) ***")
    cantonPremium.show(false)
    saveString2File("cantonPremium.svg")(
      cantonPremium.withColumn("canton_premium", concat($"canton", $"premium"))
        .toSvg[Date]("canton_premium")
    )

    cantonPremium.count() shouldBe 10L
  }

}
