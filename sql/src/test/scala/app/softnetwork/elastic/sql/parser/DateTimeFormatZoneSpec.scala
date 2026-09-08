package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.8 — `DATETIME_PARSE` and `DATETIME_FORMAT` appended `" XXX"` to the CALLER'S pattern.
  *
  * 🔴 Found by re-measuring record 6 on the PRODUCTION path. The record claimed those functions
  * emit uncompilable Painless; measured, that is true only of the context-free rendering, which no
  * shipped surface applies to them. What IS live is this, and it is worse in one direction —
  * MEASURED on a real Elasticsearch 8.18.3:
  *
  *   - `DATETIME_FORMAT(ts, 'yyyy')` emitted `ofPattern("yyyy XXX")` and returned `"2025 Z"`. A
  *     caller who asked for four characters got seven, silently, on the ordinary script-field path;
  *   - `DATETIME_PARSE('2025-01-10 10:00:00', 'yyyy-MM-dd HH:mm:ss')` emitted
  *     `ofPattern("yyyy-MM-dd HH:mm:ss XXX")`, which demands a space and an offset the caller's
  *     format never declared, and THREW. It "worked" only for text that happened to carry `
  *     +01:00`.
  *
  * The fix does not rewrite the caller's pattern. A parse that must yield a `ZonedDateTime` gets
  * the zone from `withZone`, which supplies one only when the text carried none — the same
  * mechanism `SQLTypeUtils`' `<string> -> TIMESTAMP` arm uses, deliberately shared rather than
  * re-derived.
  */
class DateTimeFormatZoneSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(Column("name", SQLTypes.Keyword), Column("ts", SQLTypes.Timestamp))
  )

  /** 🔴 Rendered WITH a `PainlessContext`, because that is the production path: the bridge builds
    * one for every script field and script sort (`bridge/package.scala:563` and `:584`), and the
    * extensions' materialized-view projection does the same. The context-FREE rendering of these
    * four functions is separately malformed (`local-21.8-date-parse-emits-malformed-painless.md`);
    * that is the deferred Part C, and asserting these claims through it would be measuring a
    * surface no query uses.
    */
  private def painlessOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "DATETIME_FORMAT" should "format with EXACTLY the pattern the caller asked for" in {
    // ES 8.18.3, same operand: before -> "2025 Z"; after -> "2025".
    val painless = painlessOf("SELECT DATETIME_FORMAT(ts, 'yyyy') FROM t")
    painless should include("""DateTimeFormatter.ofPattern("yyyy")""")
    painless should not include "XXX"
  }

  "DATETIME_PARSE" should "get its zone from the formatter, not from the caller's pattern" in {
    // ES 8.18.3: `ZonedDateTime.parse("2025-01-10 10:00:00", ofPattern("yyyy-MM-dd HH:mm:ss")
    // .withZone(ZoneId.of('Z')))` -> 2025-01-10T10:00Z. Before the fix the same statement threw.
    val painless = painlessOf("SELECT DATETIME_PARSE(name, 'yyyy-MM-dd HH:mm:ss') FROM t")
    painless should include("""DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")""")
    painless should include("withZone(ZoneId.of('Z'))")
    painless should not include "XXX"
  }

  it should "still honour an offset the caller DID declare" in {
    // `withZone` yields to a zone present in the text, so a caller who writes the offset into their
    // own format keeps it. ES 8.18.3: "2025-01-10 10:00:00 +01:00" -> 2025-01-10T09:00Z.
    val painless = painlessOf("SELECT DATETIME_PARSE(name, 'yyyy-MM-dd HH:mm:ss XXX') FROM t")
    painless should include("""ofPattern("yyyy-MM-dd HH:mm:ss XXX")""")
    painless should include("withZone(ZoneId.of('Z'))")
  }

  "the DATE-only pair" should "be untouched - they never carried the suffix" in {
    // `DATE_PARSE` yields a `LocalDate` and `DATE_FORMAT` formats one: no zone is involved, and
    // neither ever appended anything. Pinned so the fix is shown to be narrow.
    painlessOf("SELECT DATE_PARSE(name, 'yyyy-MM-dd') FROM t") should
    include("""LocalDate.parse(param1, DateTimeFormatter.ofPattern("yyyy-MM-dd"))""")
    painlessOf("SELECT DATE_PARSE(name, 'yyyy-MM-dd') FROM t") should not include "withZone"
    painlessOf("SELECT DATE_FORMAT(ts, 'yyyy') FROM t") should
    include("""DateTimeFormatter.ofPattern("yyyy")""")
  }

  "a literal Z in the caller's pattern" should "still become X" in {
    // Unrelated to the default-zone question and deliberately kept: to `ofPattern`, `Z` means
    // "zone NAME", which does not accept the `Z` ISO-8601 writes for UTC; `X` does.
    painlessOf("SELECT DATETIME_PARSE(name, 'yyyy-MM-ddZ') FROM t") should
    include("""ofPattern("yyyy-MM-ddX")""")
  }

  "the %f fractional-seconds format" should "emit a VARIABLE-width fraction, not a fixed one" in {
    // 🔴 `%f` was mapped to `SSS` under a comment reading "microseconds". MEASURED on real ES
    // 8.18.3, `S` is FIXED WIDTH in BOTH directions: `SSS` formats `.123456789` as `.123` and
    // REFUSES to parse `.123456`; `SSSSSS` refuses to parse `.123`. Optional sections do not
    // rescue it — `[.SSSSSS][.SSS]` formats as `.123456.123`.
    //
    // Picking a width would therefore NARROW one direction to widen the other — an "except" inside
    // the very rule the temporal CAST arms in this same story are justified by. `appendFraction`
    // is variable width, so the emission is a strict SUPERSET in both directions. Verified on ES
    // 8.18.3 with the emitted script: parses `.123` (as before), `.123456`, `.123456789` and NO
    // fraction at all; formats the value's real precision.
    val painless = painlessOf("SELECT DATETIME_PARSE(name, '%Y-%m-%d %H:%i:%s.%f') FROM t")
    painless should include("appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)")
    painless should not include "SSS"
    // The decimal point the caller wrote is absorbed INTO the fraction, which is what makes a
    // zero-nanosecond value format as `12:00:00` rather than `12:00:00.` — measured.
    painless should include("""appendPattern("yyyy-MM-dd HH:mm:ss")""")
  }

  it should "leave a format with no fraction on the plain ofPattern path" in {
    // The builder is emitted ONLY when a fraction is present, so nothing else moves.
    painlessOf("SELECT DATETIME_PARSE(name, '%Y-%m-%d %H:%i:%s') FROM t") should
    include("""DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")""")
    painlessOf("SELECT DATETIME_PARSE(name, '%Y-%m-%d %H:%i:%s') FROM t") should
    not include "DateTimeFormatterBuilder"
  }
}
