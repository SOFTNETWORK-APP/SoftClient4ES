/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
object TemporalLiteralsSpec {

  /** What the oracle expects of a literal: forwarded, rejected, or rewritten to `to`. */
  sealed trait Expected
  case object Verbatim extends Expected
  case object Reject extends Expected
  final case class Rewrite(to: String) extends Expected
}

class TemporalLiteralsSpec extends AnyFlatSpec with Matchers {

  import TemporalLiteralsSpec._

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

  it should "accept the ISO form under the non-strict date_optional_time but never reject under it" in {
    val lenient = FieldFormat("date_optional_time")
    lenient.acceptsIsoOptionalTime shouldBe true
    lenient.fullyUnderstood shouldBe false
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

  // ---- normalizeLiteral: the four-major oracle -------------------------------------------------
  //
  // Every row below was checked against the REAL Elasticsearch parsers of 6.8.23 (Joda), 7.17.29,
  // 8.18.3 and 9.0.3 (`DateFormatter.forPattern(spec).toDateMathParser().parse(...)`, the parser a
  // range/term value goes through). Contract: a `Reject` row FAILS on all four; a `Rewrite` row
  // produces a value every major accepts (a zone id after the time is 7+ only -- exactly as when
  // the user types the `T` form); a `Verbatim` row is a no-op, whatever Elasticsearch then says.
  // The rule may REJECT only what cannot be an ISO date at all or carries an invalid recognised
  // calendar/time component -- never a shape it merely does not model.

  private def check(format: FieldFormat, oracle: Seq[(String, Expected)]): Unit =
    oracle.foreach { case (literal, expected) =>
      withClue(s"'$literal' under '${format.spec}': ") {
        (norm(literal, format), expected) match {
          case (Right(None), Verbatim)       => succeed
          case (Right(Some(v)), Rewrite(to)) => v shouldBe to
          case (Left(reason), Reject) =>
            reason should include(s"'$literal'")
            reason should include(s"'$field'")
            reason should include(format.spec)
            reason should not startWith "Internal parser error"
          case (actual, _) => fail(s"expected $expected, got $actual")
        }
      }
    }

  private val strictOracle: Seq[(String, Expected)] = Seq(
    // -- the SQL space form is rewritten to the T form (fraction and zone kept, `z` upper-cased)
    "2026-06-04 10:30:15"           -> Rewrite("2026-06-04T10:30:15"),
    "2026-06-04 10:30:15.000000"    -> Rewrite("2026-06-04T10:30:15.000000"),
    "2026-06-04 00:00"              -> Rewrite("2026-06-04T00:00"),
    "2026-06-04 10"                 -> Rewrite("2026-06-04T10"),
    "2026-06-04 10:30:15.123+02:00" -> Rewrite("2026-06-04T10:30:15.123+02:00"),
    "2026-06-04 10:30:15+0100"      -> Rewrite("2026-06-04T10:30:15+0100"),
    "2026-06-04 10:30:15+01"        -> Rewrite("2026-06-04T10:30:15+01"),
    "2026-06-04 10:30:15+01:00:00"  -> Rewrite("2026-06-04T10:30:15+01:00:00"),
    "2026-06-04 10:30:15Z"          -> Rewrite("2026-06-04T10:30:15Z"),
    "2026-06-04 10:30:15z"          -> Rewrite("2026-06-04T10:30:15Z"),
    "2026-06-04 23:59:59,5"         -> Rewrite("2026-06-04T23:59:59,5"),
    "2026-06-04 10:30:15.123456789" -> Rewrite("2026-06-04T10:30:15.123456789"),
    "2026-06-04 10:30:15||+1d"      -> Rewrite("2026-06-04T10:30:15||+1d"),
    // -- zone shapes some majors reject are never produced by the rewrite: verbatim (R4-13)
    "2026-06-04 10:30:15UTC"      -> Verbatim, // zone id: 7+ only
    "2026-06-04 10:30:15GMT+1"    -> Verbatim, // 6.8 / 7.17 reject it
    "2026-06-04 10:30:15+010000"  -> Verbatim, // 7.17 rejects it
    "2026-06-04 10:30:15+0100:00" -> Verbatim, // every major rejects it
    "2026-06-04 10:30:15+01:0000" -> Verbatim, // every major rejects it
    // -- ISO forms Elasticsearch accepts: verbatim
    "2026-06-04"                      -> Verbatim,
    "2026-06"                         -> Verbatim,
    "2026"                            -> Verbatim,
    "2026-06-04T"                     -> Verbatim,
    "2026-06-04T10"                   -> Verbatim,
    "2026-06-04T10:30"                -> Verbatim,
    "2026-06-04T10:30:15"             -> Verbatim,
    "2026-06-04T10:30:15.1"           -> Verbatim,
    "2026-06-04T10:30:15.123456789"   -> Verbatim,
    "2026-06-04T10:30:15,5"           -> Verbatim,
    "2026-06-04T10:30:15Z"            -> Verbatim,
    "2026-06-04T10:30:15z"            -> Verbatim, // 6.8 only -- Elasticsearch decides
    "2026-06-04T10:30:15+01:00"       -> Verbatim,
    "2026-06-04T10:30:15+0100"        -> Verbatim,
    "2026-06-04T10:30:15+01"          -> Verbatim,
    "2026-06-04T10:30:15-05:30"       -> Verbatim,
    "2026-06-04T10:30:15+01:00:00"    -> Verbatim, // offset with seconds
    "2026-06-04T10:30:15UTC"          -> Verbatim, // zone id, 7+ only
    "2026-06-04T10:30:15Europe/Paris" -> Verbatim, // region id, 7+ only
    "-0001-06-04"                     -> Verbatim, // negative year
    // -- starts like a date but carries a shape the recogniser does not model: verbatim
    "2026-155"                       -> Verbatim, // ordinal date, 6.8 only
    "2026-W23-4"                     -> Verbatim, // week date, 6.8 only
    "+12026-06-04"                   -> Verbatim,
    "12026-06-04"                    -> Verbatim,
    "2026-6-4"                       -> Verbatim,
    "2026-06-04T1:02:03"             -> Verbatim,
    "2026-06-04t10:30:15"            -> Verbatim, // lower-case t, 6.8 only
    "2026-06-04Z"                    -> Verbatim,
    "2026-06-04+01:00"               -> Verbatim,
    "2026-06-04 10:30:15 UTC"        -> Verbatim, // a space before the zone: not ours
    "2026-06-04  10:30:15"           -> Verbatim, // two spaces: not ours
    "2026-06-04T10:30:15.1234567890" -> Verbatim,
    // -- numbers and date math: verbatim
    "1780531200000"   -> Verbatim,
    "1780531200"      -> Verbatim,
    "1.5"             -> Verbatim,
    "1780531200000.5" -> Verbatim,
    "-1"              -> Verbatim,
    "+1"              -> Verbatim, // 6.8 only
    "1e3"             -> Verbatim, // 6.8 only
    "now"             -> Verbatim,
    "now-1d/d"        -> Verbatim,
    "now+1h"          -> Verbatim,
    "now+1M/M"        -> Verbatim,
    "now/d"           -> Verbatim,
    "2026-06-04||/d"  -> Verbatim,
    // -- rejected: fails on every major, and the message names the literal and the field
    ""                    -> Reject,
    "not-a-date"          -> Reject,
    "nowhere"             -> Reject,
    "NOW-1d"              -> Reject, // date math is lower-case
    "now-1D"              -> Reject, // no such unit
    "04/06/2026"          -> Reject,
    "2026-02-30"          -> Reject,
    "2026-06-04T24:00:00" -> Reject,
    "2026-13-45T00:00:00" -> Reject,
    "2026-06-04 24:00:00" -> Reject,
    "2026-13-45 99:99:99" -> Reject
  )

  "normalizeLiteral" should "agree with the four Elasticsearch parsers under the default format" in {
    check(default, strictOracle)
  }

  it should "behave the same under strict_date_optional_time_nanos" in {
    check(nanos, strictOracle)
  }

  it should "never reject under the non-strict date_optional_time, but still fix the space form" in {
    check(
      FieldFormat("date_optional_time"),
      Seq(
        "2026-06-04 10:30:15" -> Rewrite("2026-06-04T10:30:15"),
        "2026-06-04 00:00:00" -> Rewrite("2026-06-04T00:00:00"),
        "2026-6-4"            -> Verbatim, // accepted by the lenient parser on every major
        "2026-06-04T1:02:03"  -> Verbatim,
        "12026-06-04"         -> Verbatim,
        "-0001-06-04"         -> Verbatim,
        "2026-06-04T10:30:15" -> Verbatim,
        "not-a-date"          -> Verbatim, // not fully understood => Elasticsearch decides
        "2026-02-30"          -> Verbatim,
        ""                    -> Verbatim
      )
    )
  }

  it should "never reject under a custom or opaque format" in {
    check(
      custom,
      Seq(
        "not-a-date"          -> Verbatim,
        "2026-06-04 00:00:00" -> Verbatim, // parity: the custom pattern parses it
        "2026-06-04T00:00:00" -> Verbatim // not ours to fix: no ISO alternative
      )
    )
    check(opaque, Seq("not-a-date" -> Verbatim, "2026-06-04 00:00:00" -> Verbatim))
  }

  it should "prefer parity with a custom alternative, and still fix the space form where ISO is accepted" in {
    check(
      mixed,
      Seq(
        "2026-06-04 00:00:00" -> Verbatim, // the custom pattern parses it
        "2026-06-04 00:00"    -> Rewrite("2026-06-04T00:00"), // custom needs seconds
        "garbage"             -> Verbatim // not fully understood => never rejected
      )
    )
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

    // 🔴 TIMESTAMP, not DATE, since story 21.5 / issue #306: an Elasticsearch `date` field is a
    // millisecond timestamp and `doc['f'].value` hands Painless a ZonedDateTime, so calling it a
    // calendar DATE was a lie that became executable once a column's mapped type reached
    // `SQLTypeUtils.coerce`. Only the SPELLING moves here — every temporal type still maps to the
    // ES type `date`, which is why the assertions below are untouched and the DDL round trip is
    // unaffected.
    table.find("event_ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
    table.find("fmt_ts").flatMap(_.options.get("format")) shouldBe Some(
      StringValue("yyyy-MM-dd HH:mm:ss")
    )
    // `date_nanos` is not a SQL type the mapping resolves to: it comes out as ANY, so it is never
    // a candidate. Pinned on purpose (AC 4: excluded, not covered).
    table.find("ts_nanos").map(_.dataType) shouldBe Some(SQLTypes.Any)
    table.find("items.ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)

    whereSql(
      resolved("SELECT id FROM events WHERE event_ts >= '2026-06-04 00:00:00'", table)
    ) shouldBe
    "WHERE event_ts >= '2026-06-04T00:00:00'"
    untouched("SELECT id FROM events WHERE ts_nanos >= '2026-06-04 00:00:00'", table)
    untouched("SELECT id FROM events WHERE fmt_ts >= '2026-06-04 00:00:00'", table)
    untouched("SELECT id FROM events WHERE label = '2026-06-04 00:00:00'", table)
  }

  it should "resolve the schema through an index alias over exactly one index, not over several" in {
    // `GET /<alias>` answers with the concrete index documents keyed by THEIR names (lead ruling on
    // review R4-5). One document: the alias resolves to it (the schema keeps the alias name); two
    // documents: ambiguous -- no mapping, so nothing is a candidate and the caller reports not found.
    val events =
      """{"aliases":{"events_alias":{},"multi_alias":{}},"mappings":{"properties":{
        |  "id":{"type":"keyword"},"event_ts":{"type":"date"},"label":{"type":"keyword"}}},
        |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin
    val archive =
      """{"aliases":{"multi_alias":{}},"mappings":{"properties":{
        |  "id":{"type":"keyword"},"event_ts":{"type":"date","format":"yyyy-MM-dd HH:mm:ss"}}},
        |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin
    val single = Index("events_alias", s"""{"events":$events}""")
    single.name shouldBe "events_alias"
    single.schema.find("event_ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp) // #306
    whereSql(
      resolved("SELECT id FROM events_alias WHERE event_ts >= '2026-06-04 00:00:00'", single.schema)
    ) shouldBe "WHERE event_ts >= '2026-06-04T00:00:00'"

    val multiRoot: com.fasterxml.jackson.databind.JsonNode =
      new com.fasterxml.jackson.databind.ObjectMapper()
        .readTree(s"""{"events":$events,"archive":$archive}""")
    Index.indexDocuments(multiRoot).map(_._1) shouldBe Seq("events", "archive")
    val multi = Index("multi_alias", multiRoot)
    multi.schema.columns shouldBe empty
    untouched("SELECT id FROM multi_alias WHERE event_ts >= '2026-06-04 00:00:00'", multi.schema)

    // a concrete index keyed by its own name is untouched by the rule
    Index("events", s"""{"events":$events}""").schema.find("event_ts").map(_.dataType) shouldBe
    Some(SQLTypes.Timestamp) // #306, see the note on the mapping test above

    // R4-18: a member WITHOUT mappings still counts -- otherwise a two-index alias would resolve
    // silently to the other member (the silent-wrong-answer mode this story removes)
    val mappingless =
      """{"aliases":{"partial_alias":{}},"settings":{"index":{"number_of_shards":"1"}}}"""
    val partialRoot: com.fasterxml.jackson.databind.JsonNode =
      new com.fasterxml.jackson.databind.ObjectMapper()
        .readTree(s"""{"events":$events,"no_mappings":$mappingless}""")
    Index.indexDocuments(partialRoot).map(_._1) shouldBe Seq("events", "no_mappings")
    Index("partial_alias", partialRoot).schema.columns shouldBe empty

    // R4-17: a resolved alias is not an alias of itself
    single.schema.aliases.keySet should not contain "events_alias"
    single.resolvedFrom shouldBe Some("events")
    Index("events", s"""{"events":$events}""").resolvedFrom shouldBe None
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
