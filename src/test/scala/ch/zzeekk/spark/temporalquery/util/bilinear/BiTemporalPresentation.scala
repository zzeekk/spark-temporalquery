package ch.zzeekk.spark.temporalquery.util.bilinear

import ch.zzeekk.spark.temporalquery.saveString2File
import ch.zzeekk.spark.temporalquery.util.MultivariateRangeLibrary.MultivariateRangeFrameExtensions
import ch.zzeekk.spark.temporalquery.util.bilinear.BiDatoralHalfOpenIntervalQueryUtil._
import org.apache.spark.sql.functions.concat
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.sql.Date

class BiTemporalPresentation extends AnyFlatSpec with Matchers with BiTemporalTestUtils {

  import session.implicits._

  private implicit val mrqc: BiLinearHalfOpenIntervalQueryConfig = BiLinearHalfOpenIntervalQueryConfig
    .withDefaultIntervalDef(fstFromColName = "known_from", fstToColName = "known_to",
      sndFromColName = "valid_from", sndToColName = "valid_to")
  logger.info(s"BiTemporalQueryUtilTest: mrqc = $mrqc")

  "presentation" should "explain Bitemporality" in {
    logger.info(s"*** Data Frame dfMonthlyPremium (${dfMonthlyPremium.count()} rows) ***")
    dfMonthlyPremium.printSchema()
    dfMonthlyPremium.show(false)
    saveString2File("dfMonthlyPremium.svg")(dfMonthlyPremium.toSvg[Date]("premium"))

    logger.info(s"*** Data Frame dfAddress (${dfAddress.count()} rows) ***")
    dfAddress.printSchema()
    dfAddress.show(false)
    saveString2File("dfAddress.svg")(dfAddress.toSvg[Date]("canton"))

    val cantonPremium = dfAddress.rangeInnerJoin[Date](df2 = dfMonthlyPremium, keys = List("id"))
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
