package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.8 Part C — a date function over a date COLUMN was unusable in an ingest script.
  *
  * `CREATE TABLE t (created DATE, y INTEGER SCRIPT AS (YEAR(created)))` emitted
  * `ctx.created.get(ChronoField.YEAR)`. In an ingest script `ctx.<field>` is the RAW JSON value of
  * the document being indexed — the string `"2025-01-10"`, not a temporal object — so `String` has
  * no `get(ChronoField)`, the script threw, `ignore_failure: true` swallowed the throw, and the
  * computed column was simply ABSENT from the stored document. Measured on real Elasticsearch for
  * `YEAR`, `MONTH`, `DATE_TRUNC`, `DATE_ADD`/`DATE_SUB`, `DATE_FORMAT` and `DATE_DIFF` — and
  * `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` is a PUBLISHED example
  * (`documentation/sql/ddl_statements.md`, and the REPL testkit's `users` table).
  *
  * 🔴 Three separate things had to be true for that example to work, and only the first was known:
  *
  *   1. the OPERAND must be parsed into a temporal. Its runtime shape is decided by `instanceof`
  *      rather than guessed, because Elasticsearch accepts BOTH an ISO string and epoch millis
  *      into a `date` field (the lead's ruling). The previous code answered that question one way,
  *      hard-coded as `from = SQLTypes.BigInt`, with no test — and wrong for every documented
  *      example, which ingests strings;
  *   2. the ingest CLOCK was `ctx['_ingest']['timestamp']`, which is NULL. Verified on REAL
  *      indices, not `_simulate`, on ES 6.8.23, 7.17.29, 8.18.3 and 9.0.3 — so `CURRENT_DATE`,
  *      `CURRENT_TIMESTAMP`, `NOW` and `TODAY` inside `SCRIPT AS` have never worked on any
  *      supported version. `System.currentTimeMillis()` is whitelisted on all four, and is already
  *      the unit the surrounding `Instant.ofEpochMilli(...)` expects;
  *   3. every temporal value in a processor must be the SAME Java type. `ChronoUnit.between`
  *      refuses a `ZonedDateTime` paired with a `LocalDate`, and Elasticsearch refuses a
  *      `LocalDate` assigned back into `ctx.<field>` at all (`illegal_argument_exception:
  *      unexpected value type [class java.time.LocalDate]`). So a processor collapses to
  *      `ZonedDateTime` — the same collapse `SQLTypeUtils.runtimeType` already applies to a query.
  *
  * ⚠️ Every emission below was executed as a real ingest pipeline on ES 6.8.23, 7.17.29, 8.18.3
  * AND 9.0.3, against both document shapes. `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` stores
  * `age: 36` for `{"birthdate":"1990-05-20"}` and for `{"birthdate":643161600000}` on all four.
  *
  * KNOWN AND UNCHANGED: a document MISSING the source field still leaves the computed column
  * absent. `instanceof` on null throws and `ignore_failure: true` swallows it, which is the same
  * outcome — and the same mechanism — as before this story.
  */
class IngestTemporalSpec extends AnyFlatSpec with Matchers {

  private def source(sql: String, column: String): String =
    Parser(sql) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == column)
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$column] in [$sql]"))
      case other => fail(s"[$sql] expected a CreateTable, got $other")
    }

  private val family = List(
    "CREATE TABLE t (created DATE, c INTEGER SCRIPT AS (YEAR(created)))",
    "CREATE TABLE t (created DATE, c INTEGER SCRIPT AS (MONTH(created)))",
    "CREATE TABLE t (created DATE, c DATE SCRIPT AS (DATE_TRUNC(created, MONTH)))",
    "CREATE TABLE t (created DATE, c DATE SCRIPT AS (DATE_ADD(created, INTERVAL 1 DAY)))",
    "CREATE TABLE t (created DATE, c DATE SCRIPT AS (DATE_SUB(created, INTERVAL 1 DAY)))",
    "CREATE TABLE t (created DATE, c KEYWORD SCRIPT AS (DATE_FORMAT(created, '%Y')))",
    "CREATE TABLE t (created DATE, c INTEGER SCRIPT AS (DATE_DIFF(created, CURRENT_DATE, YEAR)))",
    "CREATE TABLE t (ts TIMESTAMP, c INTEGER SCRIPT AS (YEAR(ts)))",
    "CREATE TABLE t (ts TIMESTAMP, c TIMESTAMP SCRIPT AS (DATE_TRUNC(ts, MONTH)))"
  )

  behavior of "a date function over a date column in SCRIPT AS"

  it should "parse the raw value before applying the function" in {
    // The family, not the one case that was reported. Every member reaches `ctx.<field>` and every
    // member needs the same decision, so the guard is over the list rather than a single example.
    family.foreach { sql =>
      withClue(s"[$sql] -> ${source(sql, "c")} ") {
        source(sql, "c") should include("instanceof String")
      }
    }
  }

  it should "resolve BOTH shapes Elasticsearch accepts, not one of them" in {
    // The point of `instanceof`: an ISO string AND epoch millis. Asserting only the string branch
    // would pass with the old hard-coded epoch guess reinstated the other way round.
    family.foreach { sql =>
      val s = source(sql, "c")
      withClue(s"[$sql] -> $s ") {
        // The string branch parses (`LocalDate.parse` for a DATE, `ZonedDateTime.parse` for a
        // TIMESTAMP -- the declared type chooses, because `ZonedDateTime.parse` REFUSES a
        // date with no time), and the other branch reads epoch millis.
        s should include(".parse(")
        s should include("Instant.ofEpochMilli")
      }
    }
  }

  it should "yield one temporal type, so values pair and can be stored" in {
    // `ChronoUnit.between` refuses mixed types and Elasticsearch refuses a `LocalDate` in `ctx`.
    // A DATE parses through `LocalDate` and is then given the UTC start of day; nothing leaves a
    // bare `LocalDate` behind.
    family.foreach { sql =>
      val s = source(sql, "c")
      withClue(s"[$sql] -> $s ") {
        if (s.contains("LocalDate.parse")) s should include("atStartOfDay(ZoneId.of('Z'))")
        s should not include ".toLocalDate()"
      }
    }
  }

  it should "leave a non-temporal column byte-identical" in {
    // The scope: a `keyword` is a `String` in a query and in `ctx` alike.
    source(
      "CREATE TABLE t (zip_code KEYWORD, zip_n BIGINT SCRIPT AS (CAST(zip_code AS BIGINT)))",
      "zip_n"
    ) should not include "instanceof"
    source("CREATE TABLE t (a KEYWORD, b KEYWORD SCRIPT AS (UPPER(a)))", "b") should not include
    "instanceof"
  }

  behavior of "the ingest clock"

  it should "not read a key that is null on every supported Elasticsearch" in {
    // 🔴 Falsifiable both ways: the old accessor must be gone AND the new one present. Asserting
    // only its absence would pass with the clock deleted entirely.
    val s = source(
      "CREATE TABLE t (n INTEGER, c TIMESTAMP SCRIPT AS (CURRENT_TIMESTAMP))",
      "c"
    )
    s should not include "_ingest"
    s should include("System.currentTimeMillis()")
  }

  it should "keep the ZonedDateTime for CURRENT_DATE too" in {
    // Narrowing to a `LocalDate` here is what made `ChronoUnit.YEARS.between` refuse the pair in
    // the published DATE_DIFF example. A query is unaffected and still narrows -- asserted in
    // ParserSpec, whose query-side pins did not move.
    val s = source(
      "CREATE TABLE t (birthdate DATE, age INTEGER SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)))",
      "age"
    )
    s should not include ".toLocalDate()"
    s should include("ChronoUnit.YEARS.between")
  }

  it should "emit the published example exactly as it was executed" in {
    // The whole point, pinned end to end. This byte string was run as an ingest pipeline on ES
    // 6.8.23, 7.17.29, 8.18.3 and 9.0.3: `{"birthdate":"1990-05-20"}` and
    // `{"birthdate":643161600000}` both store `age: 36`.
    source(
      "CREATE TABLE t (birthdate DATE, age INTEGER SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)))",
      "age"
    ) shouldBe
    "def param1 = ctx.birthdate; " +
    "def param2 = (param1 instanceof String ? " +
    """LocalDate.parse((param1).replace("/", "-"), DateTimeFormatter.ofPattern("yyyy-MM-dd"))""" +
    ".atStartOfDay(ZoneId.of('Z')) : Instant.ofEpochMilli(param1).atZone(ZoneId.of('Z'))); " +
    "def param3 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), " +
    "ZoneId.of('Z')); " +
    "def param4 = Long.valueOf(ChronoUnit.YEARS.between(param2, param3)); " +
    "ctx.age = (param1 == null) ? null : param4"
  }
}
