package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

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
  *      rather than guessed, because Elasticsearch accepts BOTH an ISO string and epoch millis into
  *      a `date` field (the lead's ruling). The previous code answered that question one way,
  *      hard-coded as `from = SQLTypes.BigInt`, with no test — and wrong for every documented
  *      example, which ingests strings; 2. the ingest CLOCK was `ctx['_ingest']['timestamp']`,
  *      which is NULL. Verified on REAL indices, not `_simulate`, on ES 6.8.23, 7.17.29, 8.18.3 and
  *      9.0.3 — so `CURRENT_DATE`, `CURRENT_TIMESTAMP`, `NOW` and `TODAY` inside `SCRIPT AS` have
  *      never worked on any supported version. `System.currentTimeMillis()` is whitelisted on all
  *      four, and is already the unit the surrounding `Instant.ofEpochMilli(...)` expects; 3. every
  *      temporal value in a processor must be the SAME Java type. `ChronoUnit.between` refuses a
  *      `ZonedDateTime` paired with a `LocalDate`, and Elasticsearch refuses a `LocalDate` assigned
  *      back into `ctx.<field>` at all (`illegal_argument_exception: unexpected value type [class
  *      java.time.LocalDate]`). So a processor collapses to `ZonedDateTime` — the same collapse
  *      `SQLTypeUtils.runtimeType` already applies to a query.
  *
  * ⚠️ Every emission below was executed as a real ingest pipeline on ES 6.8.23, 7.17.29, 8.18.3 AND
  * 9.0.3, against both document shapes. `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` stores `age: 36`
  * for `{"birthdate":"1990-05-20"}` and for `{"birthdate":643161600000}` on all four.
  *
  * KNOWN AND UNCHANGED: a document MISSING the source field still leaves the computed column
  * absent. `instanceof` on null throws and `ignore_failure: true` swallows it, which is the same
  * outcome — and the same mechanism — as before this story.
  */
class IngestTemporalSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

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
    "CREATE TABLE t (ts TIMESTAMP, c TIMESTAMP SCRIPT AS (DATE_TRUNC(ts, MONTH)))",
    // Issue #368. Both were broken in a processor too -- and that is the venue where it hurts
    // most, because `ignore_failure: true` swallows the throw and the computed column is simply
    // ABSENT rather than the query failing loudly. Executed as real ingest pipelines on ES 6.8.23
    // and 8.18.3: before the fix the column never appeared, after it `LAST_DAY` stores the last
    // day of the month and `EPOCHDAY` the day count.
    "CREATE TABLE t (created DATE, c DATE SCRIPT AS (LAST_DAY(created)))",
    "CREATE TABLE t (created DATE, c BIGINT SCRIPT AS (EPOCHDAY(created)))"
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

  /** 🔴 Issue #384 — a COMPARISON inside a computed column must not be given the query path's
    * operand promotion.
    *
    * A query reconciles a `LocalDate` operand with a `ZonedDateTime` one by appending
    * `.atStartOfDay(ZoneId.of('Z'))` to whichever side is date-only. In a processor there is no
    * date-only side to fix: rule 3 above has already collapsed every temporal to a `ZonedDateTime`,
    * so appending it is `dynamic method [java.time.ZonedDateTime, atStartOfDay/1] not found` —
    * #368's rule, in the venue where it costs most. `ScriptProcessor` sets `ignore_failure = true`,
    * so the throw is swallowed and the document is simply indexed with the computed column ABSENT:
    * no error anywhere, and the defect is invisible to every query.
    *
    * MEASURED on real ES 8.18.3 through the DDL path, for both document shapes, when the guard was
    * missing. Found by review, not by the suite — which is why the assertion is on the SHAPE (a
    * method appended to a bound parameter) rather than on `atStartOfDay` as a substring: the
    * LEGITIMATE `LocalDate.parse(…).atStartOfDay(…)` of rule 4 appears in these very scripts.
    */
  /** 🔴 Issue #384, item 3 — in an ingest script EACH operand of a comparison is parsed EXACTLY
    * ONCE, and a parsed operand is never parsed again.
    *
    * Two pre-existing defects, both measured on real ES 8.18.3 through
    * `_ingest/pipeline/_simulate`, over `CASE WHEN <cmp> THEN 1 ELSE 0 END` and both document
    * shapes (a JSON string and epoch millis). **8 of 12 comparison shapes produced NO column** — 7
    * of them SILENTLY, `ignore_failure = true` swallowing the throw; the eighth (`LAST_DAY(d) >
    * ts`) emitted Painless that does not compile, so the pipeline was refused and the DDL failed
    * loudly:
    *
    *   - the LEFT operand was parsed TWICE whenever its chain had already parsed it, so
    *     `Instant.ofEpochMilli` was handed a `ZonedDateTime` — **5** shapes;
    *   - the RIGHT operand was never parsed, so `d > ts` compared a parsed `ZonedDateTime` with the
    *     raw `ctx.ts` — **2** shapes.
    *
    * (The 8th, `d > CAST('2025-01-01' AS DATE)`, is neither: the parameter shortcut in
    * `Expression.painless` discards the coercion. NOT fixed — see the release note.)
    *
    * One rule fixes both of the above: parse an operand exactly when it is still a BARE column read
    * (`Expression.parsesAsBareColumn`). 11 of the 12 now compute, and the VALUES were checked, not
    * merely their presence.
    *
    * The assertion is on the MECHANISM rather than on bytes: a parse may not take an operand that
    * is itself a parse. A byte pin over these would be long, fragile, and would not say why.
    */
  it should "parse each operand of a comparison exactly once" in {

    /** `def <name> = <expr>;` declarations, in order. */
    def declarations(script: String): Map[String, String] =
      script
        .split("; ")
        .toList
        .collect {
          case d if d.trim.startsWith("def ") =>
            val body = d.trim.stripPrefix("def ")
            val i = body.indexOf(" = ")
            body.substring(0, i) -> body.substring(i + 3)
        }
        .toMap

    val parse = "instanceof String"
    forAll(
      Table(
        "cmp",
        "d > ts",
        "ts > d",
        "CAST(d AS DATE) > ts",
        "ts > CAST(d AS DATE)",
        "DATE_TRUNC(d, MONTH) > ts",
        "ts > DATE_TRUNC(d, MONTH)",
        "LAST_DAY(d) > ts",
        "DATE_ADD(ts, INTERVAL 1 DAY) > ts",
        "CAST(d AS DATE) > CAST(ts AS TIMESTAMP)"
      )
    ) { cmp =>
      val script = source(
        "CREATE TABLE t (name KEYWORD, d DATE, ts TIMESTAMP, c INTEGER " +
        s"SCRIPT AS (CASE WHEN $cmp THEN 1 ELSE 0 END))",
        "c"
      )
      val decls = declarations(script)
      withClue(s"[$cmp] ->\n$script\n") {
        // BOTH sides are parsed -- exactly two parses, one per operand.
        parse.r.findAllMatchIn(script).size shouldBe 2
        // ...and neither parse takes an operand that is ALREADY a parse.
        """\((\w+) instanceof String""".r
          .findAllMatchIn(script)
          .map(_.group(1))
          .foreach { name =>
            withClue(s"operand [$name] is re-parsed; its declaration is [${decls
              .getOrElse(name, "<not a local>")}] ") {
              decls.get(name).exists(_.contains(parse)) shouldBe false
            }
          }
      }
    }
  }

  it should "never promote a comparison operand in a processor" in {
    forAll(
      Table(
        "ddl",
        "CREATE TABLE t (d DATE, ts TIMESTAMP, c INTEGER " +
        "SCRIPT AS (CASE WHEN ts > CAST(d AS DATE) THEN 1 ELSE 0 END))",
        "CREATE TABLE t (d DATE, ts TIMESTAMP, c INTEGER " +
        "SCRIPT AS (CASE WHEN CAST(d AS DATE) > ts THEN 1 ELSE 0 END))",
        "CREATE TABLE t (d DATE, ts TIMESTAMP, c INTEGER " +
        "SCRIPT AS (CASE WHEN DATE_TRUNC(CAST(d AS DATE), MONTH) > ts THEN 1 ELSE 0 END))"
      )
    ) { ddl =>
      val s = source(ddl, "c")
      withClue(s"[$ddl] ->\n$s\n") {
        s should not include regex("""param\d+\.atStartOfDay\(""")
        s should not include regex("""right\d+\.atStartOfDay\(""")
        s should not include regex("""left\d+\.atStartOfDay\(""")
      }
    }
  }

  /** 🔴 Issue #384, item 1 — a comparison a processor CANNOT express is refused at DDL, because
    * making these scripts run (item 3) would otherwise have made this one silently WRONG.
    *
    * In an ingest script every temporal collapses to a `ZonedDateTime` (rule 3 above), which ERASES
    * a `CAST(ts AS TIME)` rather than honouring it: `SCRIPT AS (CASE WHEN CAST(ts AS TIME) > ts …)`
    * emits `ts > ts` and stores a confident `0` for every document. MEASURED on 8.18.3 — and on
    * `origin/main` the very same column threw instead (the operand was parsed twice) and was simply
    * ABSENT. Absent is bad; a plausible wrong number is worse, so the statement is refused where
    * the user can still do something about it.
    */
  it should "refuse a comparison a processor cannot express" in {
    val rejected = Parser(
      "CREATE TABLE t (name KEYWORD, d DATE, ts TIMESTAMP, c INTEGER " +
      "SCRIPT AS (CASE WHEN CAST(ts AS TIME) > ts THEN 1 ELSE 0 END))"
    )
    withClue(s"$rejected ") {
      rejected.isLeft shouldBe true
      rejected.left.getOrElse("").toString should include("TIME")
      rejected.left.getOrElse("").toString should include("'c'")
    }

    /** 🔴 Found by review, and it is the finding that mattered most: the walk must reach a CASE
      * nested as a function ARGUMENT. `transformFunctions` walks the chain APPLIED TO an operand,
      * so it saw `CASE WHEN … END` but not `ABS(CASE WHEN … END)` — and item 3 had made that
      * wrapped form RUN, so it stored a confident `c = 0.0` where it used to be absent. One
      * function away from the shape the check did see.
      */
    forAll(
      Table(
        "wrapped",
        "ABS(CASE WHEN CAST(ts AS TIME) > ts THEN 1 ELSE 0 END)",
        "(CASE WHEN CAST(ts AS TIME) > ts THEN 1 ELSE 0 END) + 1"
      )
    ) { expr =>
      withClue(s"[$expr] ") {
        Parser(
          "CREATE TABLE t (name KEYWORD, d DATE, ts TIMESTAMP, c INTEGER " +
          s"SCRIPT AS ($expr))"
        ).isLeft shouldBe true
      }
    }
    // the same wrapping over a comparison that IS expressible stays accepted
    Parser(
      "CREATE TABLE t (name KEYWORD, d DATE, ts TIMESTAMP, c INTEGER " +
      "SCRIPT AS (ABS(CASE WHEN CAST(d AS DATE) > ts THEN 1 ELSE 0 END)))"
    ).isRight shouldBe true

    // ...and no OTHER comparison is refused by this rule. ⚠️ ACCEPTED, not necessarily WORKING:
    // this asserts the DDL parses, which is all this rule owns. `CAST(ts AS TIME) >
    // CAST('07:00:00' AS TIME)` is accepted here and still yields NO column at ingest — the
    // temporal collapse erases both casts. Measured, unchanged by this story, and deliberately not
    // claimed otherwise.
    forAll(
      Table(
        "cmp",
        "d > ts",
        "CAST(d AS DATE) > ts",
        "LAST_DAY(d) > ts",
        "YEAR(d) > 2000",
        "UPPER(name) = 'A'",
        "CAST(ts AS TIME) > CAST('07:00:00' AS TIME)"
      )
    ) { cmp =>
      withClue(s"[$cmp] ") {
        Parser(
          "CREATE TABLE t (name KEYWORD, d DATE, ts TIMESTAMP, c INTEGER " +
          s"SCRIPT AS (CASE WHEN $cmp THEN 1 ELSE 0 END))"
        ).isRight shouldBe true
      }
    }
  }

}
