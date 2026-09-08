package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.8 part B — the `<string> -> <temporal>` CAST arms hard-coded ONE format per target, so
  * the commonest way a human writes a timestamp was rejected at script-execution time:
  *
  *   - `CAST('2025-01-10 14:30:00' AS TIMESTAMP)` raised (`ISO_ZONED_DATE_TIME` demands both the
  *     `T` separator AND a zone);
  *   - `CAST('2025/01/10' AS DATE)` raised (`ofPattern("yyyy-MM-dd")`);
  *   - `CAST('14:30' AS TIME)` raised (`ofPattern("HH:mm:ss")`).
  *
  * And the engine disagreed with ITSELF: #276/BIDC-4 taught the WHERE path to resolve a
  * space-separated literal against the mapped column's own `format`, so
  * `WHERE ts = '2025-01-10 14:30:00'` worked while the CAST of the same literal raised.
  *
  * The lead's OQ-3 ruling is "accept a small ordered format set per target". Painless has no
  * expression-level `try`/`catch` and `ofPattern`'s optional sections cannot express ISO's
  * variable-length fractional seconds, so the set is realised as a separator NORMALISATION in front
  * of a WIDER ISO formatter — one parse, and a strict superset of what parsed before.
  *
  * 🔴 EVERY expectation below was EXECUTED on a real Elasticsearch 8.18.3 via
  * `POST /_scripts/painless/_execute`, including the ISO forms that already worked (to prove the
  * widening is not a swap) and the inputs that must STILL fail. A Painless claim is only provable
  * by Elasticsearch.
  */
class TemporalCastFormatSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(Column("name", SQLTypes.Keyword), Column("ts", SQLTypes.Timestamp))
  )

  private def painlessOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head.painless(None)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a string cast to DATE" should "normalise the separator before parsing" in {
    // ES 8.18.3: "2025/01/10" -> 2025-01-10, and "2025-01-10" -> 2025-01-10 (unchanged behaviour).
    painlessOf("SELECT CAST('2025/01/10' AS DATE) FROM t") shouldBe
    """LocalDate.parse(("2025/01/10").replace("/", "-"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
    painlessOf("SELECT CAST('2025-01-10' AS DATE) FROM t") shouldBe
    """LocalDate.parse(("2025-01-10").replace("/", "-"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
  }

  it should "keep an ambiguous day-first layout a LOUD failure" in {
    // 🔴 The rewrite deliberately does NOT rescue `10/01/2025`: it becomes `10-01-2025`, which no
    // pattern accepts, so it still raises. Day-first and month-first are indistinguishable and
    // guessing between them is how a date silently becomes a different date. Verified on ES
    // 8.18.3: this script throws. Asserted on the EMISSION, since the engine's job here is to
    // pass the literal through unrescued.
    painlessOf("SELECT CAST('10/01/2025' AS DATE) FROM t") shouldBe
    """LocalDate.parse(("10/01/2025").replace("/", "-"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
  }

  "a string cast to TIME" should "use the ISO formatter rather than one hard-coded layout" in {
    // ES 8.18.3: "14:30:00" (as before), "14:30" and "14:30:00.123" (new) all parse.
    painlessOf("SELECT CAST('14:30:00' AS TIME) FROM t") shouldBe
    """LocalTime.parse("14:30:00", DateTimeFormatter.ISO_LOCAL_TIME)"""
  }

  "a string cast to DATETIME" should "accept the space separator" in {
    // ES 8.18.3: both forms -> 2025-01-10T14:30.
    painlessOf("SELECT CAST('2025-01-10 14:30:00' AS DATETIME) FROM t") shouldBe
    """LocalDateTime.parse(("2025-01-10 14:30:00").replace(" ", "T"), DateTimeFormatter.ISO_DATE_TIME)"""
  }

  "a string cast to TIMESTAMP" should "accept the space separator AND a missing zone" in {
    // 🔴 `ISO_ZONED_DATE_TIME` REQUIRES an offset, so the space form failed twice over. `withZone`
    // supplies UTC only when the text carried none -- MEASURED on ES 8.18.3:
    //   "2025-01-10 14:30:00"        -> 2025-01-10T14:30Z
    //   "2025-01-10T14:30:00Z"       -> 2025-01-10T14:30Z          (unchanged)
    //   "2025-01-10T14:30:00+01:00"  -> 2025-01-10T13:30Z          (the explicit offset WINS)
    //   "2025-01-10T14:30:00.123Z"   -> 2025-01-10T14:30:00.123Z   (fractional seconds survive)
    //   "…+01:00[Europe/Paris]"      -> kept as the zone region
    //   "not a date"                 -> still throws
    painlessOf("SELECT CAST('2025-01-10 14:30:00' AS TIMESTAMP) FROM t") shouldBe
    """ZonedDateTime.parse(("2025-01-10 14:30:00").replace(" ", "T"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z')))"""
  }

  "a temporal cast over a COLUMN" should "normalise the same way" in {
    // #306 made a column operand reach these arms at all; the normalisation must not be
    // literal-only, or the two operand kinds would disagree about which layouts are accepted.
    painlessOf("SELECT CAST(name AS DATE) FROM t") should include(""".replace("/", "-")""")
    painlessOf("SELECT CAST(name AS TIMESTAMP) FROM t") should include(""".replace(" ", "T")""")
    painlessOf("SELECT CAST(name AS TIMESTAMP) FROM t") should include("withZone(ZoneId.of('Z'))")
  }

  it should "stay null-guarded" in {
    // These four arms return from inside `ctx.addParam(...)`, so they never reach the end-of-method
    // wrapper and must guard themselves (#306). The normalisation must not have moved that guard:
    // `null.replace(...)` would be an NPE per document missing the field.
    painlessOf("SELECT CAST(name AS DATE) FROM t") should include("!= null")
    painlessOf("SELECT CAST(name AS TIMESTAMP) FROM t") should include("!= null")
  }
}
