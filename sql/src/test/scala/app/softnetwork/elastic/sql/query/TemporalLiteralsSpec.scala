package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.TemporalLiterals.FieldFormat
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Issue #276 -- the AST-level normalisation of string literals compared against `date`-mapped
  * columns. The bridge emission is asserted in `TemporalLiteralQuerySpec` (bridge module) and the
  * core seam in `TemporalLiteralSearchSpec` (core module); this spec pins the RULES.
  */
class TemporalLiteralsSpec extends AnyFlatSpec with Matchers {

  private val default = FieldFormat(TemporalLiterals.DefaultDateFormat)
  private val custom = FieldFormat("yyyy-MM-dd HH:mm:ss")
  private val mixed = FieldFormat("yyyy-MM-dd HH:mm:ss||strict_date_optional_time")
  private val opaque = FieldFormat("basic_date_time||epoch_millis")
  private val nanos = FieldFormat("strict_date_optional_time_nanos||epoch_millis")

  private val field = "event_ts"

  private def norm(literal: String, format: FieldFormat = default): Either[String, Option[String]] =
    TemporalLiterals.normalizeLiteral(literal, field, format)

  // ---- FieldFormat -----------------------------------------------------------------------------

  "FieldFormat" should "classify Elasticsearch's default date format" in {
    default.alternatives shouldBe Seq("strict_date_optional_time", "epoch_millis")
    default.acceptsIsoOptionalTime shouldBe true
    default.fullyUnderstood shouldBe true
    default.customPatterns shouldBe empty
  }

  it should "classify a custom pattern as neither ISO-accepting nor fully understood" in {
    custom.acceptsIsoOptionalTime shouldBe false
    custom.fullyUnderstood shouldBe false
    custom.customPatterns shouldBe Seq("yyyy-MM-dd HH:mm:ss")
    custom.acceptsAsCustom("2026-06-04 00:00:00") shouldBe true
    custom.acceptsAsCustom("2026-06-04T00:00:00") shouldBe false
  }

  it should "classify a mixed list" in {
    mixed.acceptsIsoOptionalTime shouldBe true
    mixed.fullyUnderstood shouldBe false
    mixed.customPatterns shouldBe Seq("yyyy-MM-dd HH:mm:ss")
  }

  it should "treat an unrecognised built-in name as opaque (never a custom pattern)" in {
    opaque.acceptsIsoOptionalTime shouldBe false
    opaque.fullyUnderstood shouldBe false
    opaque.customPatterns shouldBe empty
  }

  it should "understand the date_nanos default format" in {
    nanos.acceptsIsoOptionalTime shouldBe true
    nanos.fullyUnderstood shouldBe true
  }

  it should "strip the Elasticsearch 7 java-time '8' prefix from a custom pattern" in {
    FieldFormat("8yyyy-MM-dd").acceptsAsCustom("2026-06-04") shouldBe true
  }

  it should "ignore an unparseable custom pattern instead of failing" in {
    FieldFormat("yyyy-MM-dd {").acceptsAsCustom("2026-06-04") shouldBe false
  }

  it should "read a column's format option, else fall back to the default" in {
    FieldFormat.of(Column("ts", SQLTypes.Date)) shouldBe default
    FieldFormat.of(
      Column("ts", SQLTypes.Date, options = ListMap("format" -> StringValue("yyyy-MM-dd HH:mm:ss")))
    ) shouldBe custom
  }

  // ---- normalizeLiteral ------------------------------------------------------------------------

  "normalizeLiteral" should "rewrite the SQL space form to the T form under the default format" in {
    norm("2026-06-04 00:00:00.000000") shouldBe Right(Some("2026-06-04T00:00:00.000000"))
    norm("2026-06-04 00:00:00") shouldBe Right(Some("2026-06-04T00:00:00"))
    norm("2026-06-04 00:00") shouldBe Right(Some("2026-06-04T00:00"))
    norm("2026-06-04 10") shouldBe Right(Some("2026-06-04T10"))
    norm("2026-06-04 00:00:00.123+02:00") shouldBe Right(Some("2026-06-04T00:00:00.123+02:00"))
    norm("2026-06-04 00:00:00+0200") shouldBe Right(Some("2026-06-04T00:00:00+0200"))
    norm("2026-06-04 00:00:00Z") shouldBe Right(Some("2026-06-04T00:00:00Z"))
    norm("2026-06-04 00:00:00z") shouldBe Right(Some("2026-06-04T00:00:00Z")) // zone upper-cased
    norm("2026-06-04 23:59:59,5") shouldBe Right(Some("2026-06-04T23:59:59,5"))
    norm("2026-06-04 00:00:00.123456789") shouldBe Right(Some("2026-06-04T00:00:00.123456789"))
  }

  it should "leave every ISO spelling untouched" in {
    Seq(
      "2026-06-04T00:00:00",
      "2026-06-04T00:00:00.000000",
      "2026-06-04T00:00:00Z",
      "2026-06-04T10:30:15.123456789+01:00",
      "2026-06-04T10",
      "2026-06-04",
      "2026-06",
      "2026-155",
      "2026-W23-4"
    ).foreach(literal => withClue(literal)(norm(literal) shouldBe Right(None)))
  }

  it should "leave epoch numbers and date math untouched" in {
    Seq(
      "1780531200000",
      "-1",
      "1780531200",
      "1780531200000.5",
      "now",
      "now-1d/d",
      "now+1h",
      "now+1M/M",
      "now/d",
      "2026-06-04||/M",
      "2026-06-04 00:00:00||+1d"
    ).foreach(literal => withClue(literal)(norm(literal) shouldBe Right(None)))
  }

  it should "reject an unparseable literal under the default format, naming the literal and the field" in {
    Seq(
      "not-a-date",
      "nowhere", // starts with `now` but is not date math
      "NOW-1d", // Elasticsearch date math is lower-case
      "2026-13-45 99:99:99",
      "2026-06-04 24:00:00",
      "2026-13-45T00:00:00", // T form with an invalid calendar value
      "2026-02-30", // date-only with an invalid calendar value
      "",
      "04/06/2026",
      "2026-06-04 00:00:00 UTC",
      "2026-06-04  00:00:00" // two spaces
    ).foreach { literal =>
      norm(literal) match {
        case Left(reason) =>
          withClue(literal) {
            reason should include(s"'$literal'")
            reason should include(s"'$field'")
            reason should include(TemporalLiterals.DefaultDateFormat)
            reason should not startWith "Internal parser error"
          }
        case other => fail(s"'$literal' should be rejected, got $other")
      }
    }
  }

  it should "bound and sanitise the literal echoed in the rejection" in {
    val long = "x" * 500
    norm(long) match {
      case Left(reason) =>
        reason should include("...")
        // the fixed wording plus at most MaxLiteralExcerpt characters of the literal
        reason.length should be < (TemporalLiterals.MaxLiteralExcerpt + 400)
        reason should not include long
      case other => fail(s"expected a rejection, got $other")
    }
    norm("bad\nliteral\u0007here") match {
      case Left(reason) =>
        reason should not include "\n"
        reason should not include "\u0007"
        reason should include("'bad literal here'")
      case other => fail(s"expected a rejection, got $other")
    }
    TemporalLiterals.excerpt("short") shouldBe "short"
  }

  it should "never reject under a custom or opaque format" in {
    norm("not-a-date", custom) shouldBe Right(None)
    norm("2026-06-04 00:00:00", custom) shouldBe Right(None) // parity: the custom pattern parses it
    norm("2026-06-04T00:00:00", custom) shouldBe Right(None) // not ours to fix: no ISO alternative
    norm("not-a-date", opaque) shouldBe Right(None)
    norm("2026-06-04 00:00:00", opaque) shouldBe Right(None)
  }

  it should "prefer parity with a custom alternative, and still fix the space form where ISO is accepted" in {
    norm("2026-06-04 00:00:00", mixed) shouldBe Right(None) // the custom pattern parses it
    norm("2026-06-04 00:00", mixed) shouldBe Right(Some("2026-06-04T00:00")) // custom needs seconds
    norm("garbage", mixed) shouldBe Right(None) // not fully understood => never rejected
  }

  it should "rewrite the space form under the date_nanos default format too" in {
    norm("2026-06-04 00:00:00.123456789", nanos) shouldBe Right(
      Some("2026-06-04T00:00:00.123456789")
    )
  }

  // ---- statement level ---------------------------------------------------------------------------

  private val schema: SchemaTable = SchemaTable(
    "events",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("event_ts", SQLTypes.Date),
      Column("created", SQLTypes.Timestamp),
      Column("t", SQLTypes.Time),
      Column("label", SQLTypes.Keyword),
      Column("amount", SQLTypes.Int),
      Column(
        "fmt_ts",
        SQLTypes.Date,
        options = ListMap("format" -> StringValue("yyyy-MM-dd HH:mm:ss"))
      )
    )
  )

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"$sql did not parse as a SingleSearch: $other")
  }

  private def whereSql(search: SingleSearch): String = search.where.map(_.sql.trim).getOrElse("")

  private def resolved(sql: String, table: SchemaTable = schema): SingleSearch =
    TemporalLiterals(single(sql), table) match {
      case Right(s)     => s
      case Left(reason) => fail(s"$sql was rejected: $reason")
    }

  private def untouched(sql: String, table: SchemaTable = schema): Unit = {
    val parsed = single(sql)
    TemporalLiterals(parsed, table) match {
      case Right(s)     => withClue(sql)(s should be theSameInstanceAs parsed)
      case Left(reason) => fail(s"$sql was rejected: $reason")
    }
  }

  "TemporalLiterals" should "rewrite a range comparison against a date column" in {
    val sql = "SELECT id FROM events WHERE event_ts >= '2026-06-04 00:00:00.000000'"
    TemporalLiterals.hasCandidates(single(sql)) shouldBe true
    whereSql(resolved(sql)) shouldBe "WHERE event_ts >= '2026-06-04T00:00:00.000000'"
  }

  it should "rewrite equality, BETWEEN and IN through AND/OR nesting and a table alias" in {
    val where = whereSql(
      resolved(
        "SELECT id FROM events e WHERE (e.event_ts = '2026-06-04 00:00:00' OR e.created BETWEEN " +
        "'2026-06-01 00:00:00' AND '2026-06-30 23:59:59') AND e.event_ts IN ('2026-06-04 00:00:00', " +
        "'2026-07-01 00:00:00') AND e.amount > 1"
      )
    )
    where should include("event_ts = '2026-06-04T00:00:00'")
    where should include("BETWEEN '2026-06-01T00:00:00' AND '2026-06-30T23:59:59'")
    where should include("IN ('2026-06-04T00:00:00','2026-07-01T00:00:00')")
    where should include("amount > 1")
    where should not include " 00:00:00'"
  }

  it should "rewrite the NOT / != / < spellings as well" in {
    whereSql(resolved("SELECT id FROM events WHERE NOT event_ts < '2026-06-04 00:00:00'")) should
    include("'2026-06-04T00:00:00'")
    whereSql(resolved("SELECT id FROM events WHERE event_ts != '2026-06-04 00:00:00'")) should
    include("'2026-06-04T00:00:00'")
    whereSql(resolved("SELECT id FROM events WHERE event_ts NOT IN ('2026-06-04 00:00:00')")) should
    include("'2026-06-04T00:00:00'")
  }

  it should "return the very same statement, and report no candidate, when nothing qualifies" in {
    val sql = "SELECT id FROM events WHERE amount > 10 AND label IS NOT NULL"
    TemporalLiterals.hasCandidates(single(sql)) shouldBe false
    untouched(sql)
    TemporalLiterals.hasCandidates(single("SELECT id FROM events")) shouldBe false
    TemporalLiterals.hasCandidates(
      single("SELECT id FROM events WHERE YEAR(event_ts) = 2026")
    ) shouldBe false
  }

  it should "leave keyword, TIME, LIKE, function-wrapped, unknown and custom-format columns untouched" in {
    untouched("SELECT id FROM events WHERE label = '2026-06-04 00:00:00'") // AC 3 negative control
    untouched("SELECT id FROM events WHERE t = '10:30:00'")
    untouched("SELECT id FROM events WHERE event_ts LIKE '2026-06-04%'")
    untouched("SELECT id FROM events WHERE event_ts RLIKE '2026.*'")
    untouched("SELECT id FROM events WHERE unknown_col >= '2026-06-04 00:00:00'")
    untouched("SELECT id FROM events WHERE fmt_ts >= '2026-06-04 00:00:00'") // AC 2 parity
    untouched("SELECT id FROM events WHERE event_ts >= '2026-06-04T00:00:00'") // already ISO
    untouched("SELECT id FROM events WHERE event_ts >= '1780531200000'") // AC 4 epoch millis
    untouched("SELECT id FROM events WHERE event_ts > 'now-1d/d'") // date math
  }

  it should "reject an unparseable literal against a date column, naming the literal and the field" in {
    def rejects(sql: String, literal: String, column: String): Unit =
      TemporalLiterals(single(sql), schema) match {
        case Left(reason) =>
          withClue(sql) {
            reason should include(s"'$literal'")
            reason should include(s"'$column'")
            reason should not startWith "Internal parser error"
          }
        case Right(s) => fail(s"$sql should have been rejected, got ${whereSql(s)}")
      }
    rejects("SELECT id FROM events WHERE event_ts >= 'not-a-date'", "not-a-date", "event_ts")
    rejects(
      "SELECT id FROM events WHERE created BETWEEN '2026-06-01 00:00:00' AND 'nope'",
      "nope",
      "created"
    )
    rejects(
      "SELECT id FROM events WHERE event_ts IN ('2026-06-04 00:00:00', '2026-06-04 25:00:00')",
      "2026-06-04 25:00:00",
      "event_ts"
    )
    rejects(
      "SELECT id FROM events WHERE amount > 1 AND (event_ts = '' OR label = 'x')",
      "",
      "event_ts"
    )
  }

  it should "leave an identifier qualified with a cross-index JOIN alias untouched" in {
    // `o` is the statement's own source; `c` is a JOIN source whose mapping is NOT the one in hand.
    // A `customers` schema with a `date` column of the same name must not be used for `c.event_ts`.
    val sql =
      "SELECT o.id FROM events o JOIN customers c ON o.id = c.id " +
      "WHERE o.event_ts >= '2026-06-04 00:00:00' AND c.event_ts >= '2026-06-04 00:00:00'"
    val where = whereSql(resolved(sql))
    where should include("o.event_ts >= '2026-06-04T00:00:00'")
    where should include("c.event_ts >= '2026-06-04 00:00:00'")
  }

  it should "resolve columns from a real Elasticsearch mapping, excluding date_nanos by construction" in {
    val json =
      """{"events":{"aliases":{},"mappings":{"properties":{
        |  "id":{"type":"keyword"},
        |  "event_ts":{"type":"date"},
        |  "fmt_ts":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"},
        |  "ts_nanos":{"type":"date_nanos"},
        |  "label":{"type":"keyword"},
        |  "items":{"type":"nested","properties":{"ts":{"type":"date"},"qty":{"type":"integer"}}}
        |}},"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}}""".stripMargin
    val table = Index("events", json).schema

    table.find("event_ts").map(_.dataType) shouldBe Some(SQLTypes.Date)
    table.find("fmt_ts").flatMap(_.options.get("format")) shouldBe Some(
      StringValue("yyyy-MM-dd HH:mm:ss")
    )
    // `date_nanos` is not a SQL type the mapping resolves to: it comes out as ANY, so it is never
    // a candidate. Pinned on purpose (AC 4: excluded, not covered).
    table.find("ts_nanos").map(_.dataType) shouldBe Some(SQLTypes.Any)
    table.find("items.ts").map(_.dataType) shouldBe Some(SQLTypes.Date)

    whereSql(
      resolved("SELECT id FROM events WHERE event_ts >= '2026-06-04 00:00:00'", table)
    ) shouldBe
    "WHERE event_ts >= '2026-06-04T00:00:00'"
    untouched("SELECT id FROM events WHERE ts_nanos >= '2026-06-04 00:00:00'", table)
    untouched("SELECT id FROM events WHERE fmt_ts >= '2026-06-04 00:00:00'", table)
    untouched("SELECT id FROM events WHERE label = '2026-06-04 00:00:00'", table)
  }

  it should "walk into an UNNEST nested criteria" in {
    val json =
      """{"events":{"aliases":{},"mappings":{"properties":{
        |  "id":{"type":"keyword"},
        |  "items":{"type":"nested","properties":{"ts":{"type":"date"},"qty":{"type":"integer"}}}
        |}},"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}}""".stripMargin
    val table = Index("events", json).schema
    val sql =
      "SELECT id FROM events e JOIN UNNEST(e.items) AS i WHERE i.ts >= '2026-06-04 00:00:00' AND i.qty > 0"
    TemporalLiterals.hasCandidates(single(sql)) shouldBe true
    val where = whereSql(resolved(sql, table))
    where should include("'2026-06-04T00:00:00'")
    where should not include "'2026-06-04 00:00:00'"
  }
}
