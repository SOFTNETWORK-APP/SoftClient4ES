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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.PainlessOperandForm
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** A computed column may only read columns the table declares, with types the consuming function
  * can take.
  *
  * 🔴 This REVERSES the lead's OQ-4 ruling (overturned 2026-09-20: *"Existence and type otherwise
  * the painless will not be accurate."*). Under OQ-4 an unresolved operand was accepted and the
  * conversion simply did not fire. What that shipped, measured on real Elasticsearch 8.18.3:
  *
  *   - `YEAR(nosuch)` emits `ctx.nosuch.get(ChronoField.YEAR)` -- throws, `ignore_failure` swallows
  *     it, and the computed column is silently ABSENT from every document
  *   - an UNDECLARED column takes the `ZonedDateTime.parse` branch where a declared DATE takes
  *     `LocalDate.parse`, and `ZonedDateTime.parse` refuses a date-only value: `Text '2025-01-10'
  *     could not be parsed at index 10`
  *   - `YEAR(k)` over a KEYWORD emits `ctx.k.get(ChronoField.YEAR)` -- a String has no such method
  *   - `k + 1` over a KEYWORD emits `param1 + String.valueOf(1)`, string CONCATENATION into an
  *     INTEGER column: the silent one
  *
  * Every failure was at INGEST, long after the DDL was accepted.
  */
class ScriptReferenceValidationSpec
    extends AnyFlatSpec
    with Matchers
    with TableDrivenPropertyChecks {

  private def parse(sql: String): Either[String, CreateTable] = Parser(sql) match {
    case Right(ct: CreateTable) => Right(ct)
    case Left(err)              => Left(err.msg)
    case other                  => fail(s"[$sql] expected a CreateTable, got $other")
  }

  private def rejectionOf(sql: String): String = parse(sql) match {
    case Left(msg) => msg
    case Right(_)  => fail(s"[$sql] was ACCEPTED, expected a rejection")
  }

  "a reference to a column the table does not declare" should "be rejected" in {
    forAll(
      Table(
        ("ddl", "missing"),
        ("CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(nosuch)))", "nosuch"),
        ("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (n + nosuch))", "nosuch"),
        (
          "CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (CASE WHEN nosuch > 0 THEN 1 ELSE 0 END))",
          "nosuch"
        ),
        ("CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(name, nosuch)))", "nosuch"),
        (
          "CREATE TABLE t (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.other, MONTH)))",
          "meta.other"
        )
      )
    ) { (ddl, missing) =>
      withClue(s"[$ddl] ") {
        val msg = rejectionOf(ddl)
        msg should include(s"'$missing'")
        msg should include("does not exist")
      }
    }
  }

  "a column handed to a function that cannot take its declared type" should "be rejected" in {
    forAll(
      Table(
        ("ddl", "column", "declared"),
        ("CREATE TABLE t (k KEYWORD, c INTEGER SCRIPT AS (YEAR(k)))", "k", "KEYWORD"),
        ("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (YEAR(n)))", "n", "INT"),
        (
          "CREATE TABLE t (k KEYWORD, c TIMESTAMP SCRIPT AS (DATE_TRUNC(k, MONTH)))",
          "k",
          "KEYWORD"
        ),
        ("CREATE TABLE t (k KEYWORD, c INTEGER SCRIPT AS (k + 1))", "k", "KEYWORD")
      )
    ) { (ddl, column, declared) =>
      withClue(s"[$ddl] ") {
        val msg = rejectionOf(ddl)
        msg should include(s"'$column'")
        msg should include(declared)
      }
    }
  }

  /** SCHEMA-6 (A-30) -- an `ARRAY<X>` column handed to a NUMERIC or TEMPORAL function input is
    * refused by its DECLARED type, as 0.24.0 refused it on CREATE. An ingest processor reads
    * `ctx.<c>`, the whole `_source` value -- the list -- and not one element, so the element view
    * Painless has in a query cannot be honoured there: measured on ES 8.18.3, `c INT SCRIPT AS (arr
    * + 1)` stored NULL for `[5, 9]` and for `[5]`, and `YEAR(ad)` NULL for every document, the
    * processor's failure swallowed by `ignore_failure`.
    */
  "a NUMERIC or TEMPORAL function over an ARRAY<X> column" should "be refused on CREATE, naming the column" in {
    forAll(
      Table(
        ("ddl", "column", "declared"),
        ("CREATE TABLE t (arr ARRAY<INT>, c INT SCRIPT AS (arr + 1))", "arr", "ARRAY<INT>"),
        ("CREATE TABLE t (ad ARRAY<DATE>, cy INT SCRIPT AS (YEAR(ad)))", "ad", "ARRAY<DATE>")
      )
    ) { (ddl, column, declared) =>
      withClue(s"[$ddl] ") {
        val msg = rejectionOf(ddl)
        msg should include(s"'$column'")
        msg should include(declared)
      }
    }
  }

  it should "be accepted over the scalar twin" in {
    parse("CREATE TABLE t (n INT, c INT SCRIPT AS (n + 1))") shouldBe a[Right[_, _]]
    parse("CREATE TABLE t (d DATE, cy INT SCRIPT AS (YEAR(d)))") shouldBe a[Right[_, _]]
  }

  /** The ALTER half: `GatewayApi` merges the ALTER into the table it LOADS from Elasticsearch, then
    * re-validates the whole table. 0.24.0 read the `_meta` type `ARRAY<INT>` as ANY there and
    * accepted the column; it is now read back `ARRAY<INT>` and refused like the CREATE.
    */
  it should "be refused by validateScriptReferences on a table loaded from Elasticsearch and merged with ADD COLUMN" in {
    loadedArrayTable.find("arr").map(_.dataType) shouldBe Some(
      app.softnetwork.elastic.sql.`type`.SQLTypes
        .Array(app.softnetwork.elastic.sql.`type`.SQLTypes.Int)
    )
    forAll(
      Table(
        ("alter", "column", "declared"),
        ("ALTER TABLE t ADD COLUMN c INT SCRIPT AS (arr + 1)", "arr", "ARRAY<INT>"),
        ("ALTER TABLE t ADD COLUMN cy INT SCRIPT AS (YEAR(ad))", "ad", "ARRAY<DATE>")
      )
    ) { (alter, column, declared) =>
      withClue(s"[$alter] ") {
        validateScriptReferences(mergedWith(alter)) match {
          case Left(msg) =>
            msg should include(s"'$column'")
            msg should include(declared)
          case Right(_) => fail("ACCEPTED, expected a refusal")
        }
      }
    }
    validateScriptReferences(
      mergedWith("ALTER TABLE t ADD COLUMN c INT SCRIPT AS (n + 1)")
    ) shouldBe
    Right(())
    validateScriptReferences(
      mergedWith("ALTER TABLE t ADD COLUMN cy INT SCRIPT AS (YEAR(d))")
    ) shouldBe
    Right(())
  }

  /** The table `GatewayApi` LOADS from Elasticsearch before it merges an ALTER: `_meta` declares
    * the `ARRAY<X>` columns.
    */
  private lazy val loadedArrayTable: Schema = {
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    app.softnetwork.elastic.schema
      .Index(
        name = "t",
        mappings = mapper.readTree(
          """{"mappings": {
            |  "_meta": {"columns": {
            |    "arr": {"data_type": "ARRAY<INT>"},
            |    "arrk": {"data_type": "ARRAY<KEYWORD>"},
            |    "ad": {"data_type": "ARRAY<DATE>"},
            |    "n": {"data_type": "INT"},
            |    "d": {"data_type": "DATE"}
            |  }},
            |  "properties": {
            |    "id": {"type": "keyword"},
            |    "arr": {"type": "integer"},
            |    "arrk": {"type": "keyword"},
            |    "ad": {"type": "date"},
            |    "n": {"type": "integer"},
            |    "d": {"type": "date"}
            |  }
            |}}""".stripMargin
        ),
        settings = mapper.readTree("""{"settings": {}}""")
      )
      .schema
  }

  /** [[loadedArrayTable]] merged with each ALTER in turn, as `GatewayApi` merges one. */
  private def mergedWith(alters: String*): Schema =
    alters.foldLeft(loadedArrayTable) { (table, alter) =>
      Parser(alter) match {
        case Right(at: app.softnetwork.elastic.sql.query.AlterTable) => table.merge(at.statements)
        case other => fail(s"[$alter] is not an ALTER TABLE: $other")
      }
    }

  /** The ingest source of the computed column `column` of `table`. */
  private def processorSource(table: Schema, column: String): String =
    table.defaultPipeline.processors
      .collectFirst { case p: ScriptProcessor if p.column == column => p.source }
      .getOrElse(fail(s"no script processor for '$column'"))

  /** SCHEMA-6 (lead R-66) -- a computed column over an `ARRAY<X>` column keeps 0.24.0's verdict and
    * bytes: its processor reads `ctx.<c>`, the whole list, so D7's element view is off while the
    * script renders and while it is validated. The sources pinned are 0.24.0's (and 5d0c46b2's) for
    * the same CREATE, RENDERED by the processor census on both trees. Read as the element, the
    * first two had moved to an element cast (`Integer.parseInt(param1)`, `(long) param2`) that
    * fails on `["7"]` / `[5]` and stores NULL under `ignore_failure`, and the last three were
    * refused ("declared ARRAY<INT> and not INT").
    */
  private val wholeValueScripts = Table(
    ("script", "source"),
    (
      "c INT SCRIPT AS (CAST(arrk AS INT))",
      "def param1 = ctx.arrk; ctx.c = param1"
    ),
    (
      "c INT SCRIPT AS (CASE WHEN n > 0 THEN arr ELSE 0 END)",
      "def param1 = ctx.n; def param2 = ctx.arr; def param3 = param1 == null ? false : (param1 > 0); ctx.c = param3 ? param2 : 0"
    ),
    (
      "c BIGINT SCRIPT AS (CAST(arr AS BIGINT))",
      "def param1 = ctx.arr; ctx.c = param1"
    ),
    (
      "c VARCHAR SCRIPT AS (CAST(arr AS VARCHAR))",
      "def param1 = ctx.arr; ctx.c = (param1 != null ? String.valueOf(param1) : null)"
    ),
    (
      "c TIMESTAMP SCRIPT AS (CAST(ad AS TIMESTAMP))",
      "def param1 = ctx.ad; ctx.c = param1"
    )
  )

  "a computed column over an ARRAY<X> column" should "keep 0.24.0's verdict and processor on CREATE" in {
    forAll(wholeValueScripts) { (script, source) =>
      val ddl =
        s"CREATE TABLE t (n INT, d DATE, arr ARRAY<INT>, arrk ARRAY<KEYWORD>, ad ARRAY<DATE>, $script)"
      withClue(s"[$ddl] ") {
        parse(ddl) match {
          case Right(ct) => processorSource(ct.schema, "c") shouldBe source
          case Left(msg) => fail(s"REFUSED: $msg")
        }
      }
    }
  }

  it should "keep 0.24.0's verdict and processor on an ALTER of a table loaded from Elasticsearch" in {
    forAll(wholeValueScripts) { (script, source) =>
      withClue(s"[ADD COLUMN $script] ") {
        val merged = mergedWith(s"ALTER TABLE t ADD COLUMN $script")
        validateScriptReferences(merged) shouldBe Right(())
        processorSource(merged, "c") shouldBe source
      }
    }
  }

  it should "leave every later ALTER of a table that holds it accepted" in {
    forAll(wholeValueScripts) { (script, _) =>
      withClue(s"[holding $script, then ADD COLUMN z INT] ") {
        validateScriptReferences(
          mergedWith(s"ALTER TABLE t ADD COLUMN $script", "ALTER TABLE t ADD COLUMN z INT")
        ) shouldBe Right(())
      }
    }
  }

  /** The NULLIF type rule judges the declared types too, as on 0.24.0: an `ARRAY<INT>` beside a
    * number is refused, where the element view had accepted it and its processor compared the list
    * with `0` and wrote the list into an INT column.
    */
  it should "keep 0.24.0's NULLIF refusal" in {
    val msg = rejectionOf("CREATE TABLE t (arr ARRAY<INT>, c INT SCRIPT AS (NULLIF(arr, 0)))")
    msg should include("NULLIF(arr, 0) compares ARRAY<INT> with BIGINT")
  }

  /** The element view is off only while such a script renders or is validated: a query over the
    * same column still reads the element (D7), so the scope is restored after the validation.
    */
  it should "leave the element view of a query untouched" in {
    import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
    val arrayOfInt = SQLTypes.Array(SQLTypes.Int)
    SQLTypeUtils.validatingComputedColumns(SQLTypeUtils.operandType(arrayOfInt)) shouldBe arrayOfInt
    SQLTypeUtils.operandType(arrayOfInt) shouldBe SQLTypes.Int
    SQLTypeUtils.runtimeType(arrayOfInt) shouldBe SQLTypes.Int
  }

  /** 🔴 The half that matters more than the rejections. Over-rejecting valid DDL would be worse
    * than the defect, and several of these are non-obvious.
    */
  "valid DDL" should "keep being accepted" in {
    forAll(
      Table(
        ("ddl", "why"),
        ("CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(d)))", "the plain shape"),
        (
          "CREATE TABLE t (c INTEGER SCRIPT AS (n + 1), n INTEGER)",
          "a FORWARD reference -- validation runs on the RESOLVED table, so declaration order is irrelevant"
        ),
        (
          "CREATE TABLE t (n INTEGER, a INTEGER SCRIPT AS (n + 1), c INTEGER SCRIPT AS (a + 1))",
          "a computed column reading ANOTHER computed column"
        ),
        (
          "CREATE TABLE t (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.dt, MONTH)))",
          "a nested STRUCT path"
        ),
        (
          "CREATE TABLE t (name KEYWORD FIELDS (raw KEYWORD), c KEYWORD SCRIPT AS (UPPER(name.raw)))",
          "a MULTI-FIELD path"
        ),
        ("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (1 + 1))", "constants reference nothing"),
        (
          "CREATE TABLE t (n INTEGER, c TIMESTAMP SCRIPT AS (CURRENT_DATE))",
          "CURRENT_DATE is not a column"
        ),
        (
          "CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (n))",
          "a bare reference, no function to type-check"
        ),
        (
          "CREATE TABLE t (k KEYWORD, c KEYWORD SCRIPT AS (UPPER(k)))",
          "KEYWORD satisfies a VARCHAR input"
        ),
        (
          "CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (ABS(n)))",
          "INT satisfies a NUMERIC input"
        ),
        (
          "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(DATE_TRUNC(d, MONTH))))",
          "a nested chain -- the INNERMOST function is the consumer"
        ),
        (
          "CREATE TABLE t (ts TIMESTAMP, c INTEGER SCRIPT AS (YEAR(ts)))",
          "TIMESTAMP satisfies a TEMPORAL input"
        ),
        (
          "CREATE TABLE t (birthdate DATE, age INTEGER SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)))",
          "🔴 the PUBLISHED example -- DATE_DIFF declares DATETIME, and `matches(DATE, DATETIME)` is " +
          "FALSE. Checking with `matches` alone rejected this, and it stores `age` correctly on all " +
          "four majors. This row is why `acceptableOperand` also consults `canConvert`."
        ),
        (
          "CREATE TABLE t (profile STRUCT FIELDS (join_date DATE), c INTEGER SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, YEAR)))",
          "the same widening through a nested path"
        ),
        (
          "CREATE TABLE t (a KEYWORD, n INTEGER, c KEYWORD SCRIPT AS (CONCAT(a, n)))",
          "🔴 CONCAT coerces every argument (`String.valueOf`) and stores \"ORD-42\" on 8.18.3. " +
          "Checking CONCAT's declared VARCHAR input rejected the commonest computed column there is."
        ),
        (
          "CREATE TABLE t (a KEYWORD, d DATE, c KEYWORD SCRIPT AS (CONCAT(a, d)))",
          "the same, with a temporal argument"
        ),
        (
          "CREATE TABLE t (d DATE, c KEYWORD SCRIPT AS (DATE_PARSE(d, 'yyyy-MM-dd')))",
          "🔴 at INGEST `ctx.d` IS the raw JSON string -- re-parsing a date column that arrives in " +
          "a non-standard format is why the function exists"
        ),
        (
          "CREATE TABLE users_view (id INT, age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)) STORED)",
          "🔴 a STORED column emits NO processor, so its source columns deliberately need not " +
          "survive into the table that carries it -- that is the documented point of STORED"
        )
      )
    ) { (ddl, why) =>
      withClue(s"[$ddl] -- $why: ") {
        parse(ddl) match {
          case Right(_)  => succeed
          case Left(msg) => fail(s"REJECTED: $msg")
        }
      }
    }
  }

  /** 🔴 THE COMPLETENESS GUARD, and the reason this spec exists rather than a handful of rows.
    *
    * `ScriptReferences.of` walks the AST, and it can only see operands a function exposes through
    * `FunctionN.args`. A function shape carrying operands some other way would be INVISIBLE to it
    * and the reference inside would go unchecked -- the #318 failure mode, where enumerating by
    * type silently fails OPEN and no rejection test can notice.
    *
    * So the walk is asserted against a SECOND, independent derivation: the `ctx.<path>` reads in
    * the EMITTED processor source, which is what the script will actually read at ingest. If the
    * walk ever misses an operand shape, the emission still reads it and these disagree.
    *
    * (Literals are blanked first -- a `;`-bearing or `ctx.`-bearing string literal is data, not a
    * read. That is issue #373's lesson, reused here.)
    */
  "the AST walk" should "find exactly the columns the emission reads" in {
    val shapes = Table(
      "ddl",
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(d)))",
      "CREATE TABLE t (d DATE, n INTEGER, c INTEGER SCRIPT AS (YEAR(d) + n))",
      "CREATE TABLE t (a KEYWORD, b KEYWORD, c KEYWORD SCRIPT AS (CONCAT(a, b)))",
      "CREATE TABLE t (d DATE, n INTEGER, c INTEGER SCRIPT AS (CASE WHEN n > 0 THEN YEAR(d) ELSE 0 END))",
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(DATE_TRUNC(d, MONTH))))",
      "CREATE TABLE t (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.dt, MONTH)))",
      "CREATE TABLE t (a INTEGER, b INTEGER, e INTEGER, c INTEGER SCRIPT AS ((a + b) * e))",
      "CREATE TABLE t (k KEYWORD, c KEYWORD SCRIPT AS (COALESCE(k, 'x')))",
      "CREATE TABLE t (n INTEGER, lo INTEGER, hi INTEGER, c INTEGER SCRIPT AS (CASE WHEN n BETWEEN lo AND hi THEN 1 ELSE 0 END))",
      "CREATE TABLE t (d DATE, c KEYWORD SCRIPT AS (DATE_PARSE(d, 'yyyy-MM-dd')))",
      "CREATE TABLE t (a KEYWORD, n INTEGER, c KEYWORD SCRIPT AS (CONCAT(a, n)))",
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(DATE_TRUNC(DATE_TRUNC(d, MONTH), MONTH))))"
    )
    forAll(shapes) { ddl =>
      val ct = parse(ddl).fold(msg => fail(s"[$ddl] rejected: $msg"), identity)
      val column = ct.schema.columns.find(_.name == "c").getOrElse(fail(s"[$ddl] no column c"))
      val script = column.script.getOrElse(fail(s"[$ddl] no script"))

      val fromAst =
        script.expr.toSeq.flatMap(ScriptReferences.of).map(_._1.path).toSet

      // the independent derivation: what the emitted source actually reads, minus the target
      // 🔴 A trailing METHOD call must not be read as part of the path: `ctx.tm.get(ChronoField…)`
      // captures `tm.get`, and `ctx.d instanceof String` captures `d`. Drop the last segment
      // exactly when the character after the match opens a call.
      val blanked = PainlessOperandForm.withoutLiterals(script.source)
      val fromEmission =
        """ctx\.([A-Za-z0-9_?.]+)""".r
          .findAllMatchIn(blanked)
          .map { m =>
            val raw = m.group(1).replace("?.", ".")
            if (m.end < blanked.length && blanked.charAt(m.end) == '(')
              raw.split("\\.").dropRight(1).mkString(".")
            else raw
          }
          .filter(_.nonEmpty)
          .toSet - column.path

      withClue(s"[$ddl]\n  source = ${script.source}\n  ") {
        fromAst shouldBe fromEmission
      }
    }
  }

  /** 🔴 CHARACTERISATION of a KNOWN limit, not an endorsement. A string-typed input is not
    * type-checked, because the declared `inputType` does not say whether the emission coerces:
    * `CONCAT` and `UPPER` both declare VARCHAR, and `CONCAT(a, n)` WORKS (`String.valueOf`) while
    * `UPPER(n)` does not (`param1.toUpperCase()` on a number, measured absent on 8.18.3). Rejecting
    * on the declared type rejected both, including `CONCAT(name, ' ', id)`.
    *
    * Catching `UPPER(n)` needs the EMISSION's coercion behaviour rather than a declared type, which
    * is a larger change than this one. Recorded so the gap is visible and cannot be closed by
    * accident.
    */
  "a string function over a non-string column" should "be accepted, knowingly" in {
    parse("CREATE TABLE t (n INTEGER, c KEYWORD SCRIPT AS (UPPER(n)))") shouldBe a[Right[_, _]]
  }

  /** 🔴 CHARACTERISATION of a second known limit. `matches` and `canConvert` both relate the
    * temporal types as a FAMILY, so a TIME column satisfies every temporal function: `YEAR(tm)`
    * emits `ctx.tm.get(ChronoField.YEAR)` and the column is silently absent (measured on 8.18.3
    * with `tm: "10:15:30"`). The rule is family-level where this defect is member-level.
    */
  "a temporal function over a TIME column" should "be accepted, knowingly" in {
    parse("CREATE TABLE t (tm TIME, c INTEGER SCRIPT AS (YEAR(tm)))") shouldBe a[Right[_, _]]
  }

  /** 🔴 A `FunctionWithIdentifier` lists its own receiver in `args`, so the walk used to yield each
    * such node twice and was 2^depth: 16,384 pairs and 148 ms at depth 14, with one rejection
    * repeated 8 times for a 3-deep expression. Asserting the message COUNT is what pins that --
    * `should include` passes happily on a message repeated eight times.
    */
  "a reference nested through repeated calls" should "be reported once, not once per path" in {
    val msg = rejectionOf(
      "CREATE TABLE t (d DATE, c DATE SCRIPT AS (LAST_DAY(LAST_DAY(LAST_DAY(nosuch)))))"
    )
    withClue(s"[$msg] ")(msg.split("does not exist", -1).length - 1 shouldBe 1)
  }

  /** 🔴 The message count above is NOT enough: `validateScriptReferences` de-duplicates its errors,
    * so it stays 1 even while the walk itself doubles per level. This asserts the WALK, which is
    * where the cost is. Measured before the receiver exclusion: 2, 4, 8, 32 pairs at depths 1, 2,
    * 3, 5 -- one reference, visited 2^depth times.
    */
  it should "be visited once by the walk, not once per level" in {
    forAll(Table("depth", 1, 2, 3, 5)) { depth =>
      val inner = (1 to depth).foldLeft("d")((acc, _) => s"LAST_DAY($acc)")
      val ct = parse(s"CREATE TABLE t (d DATE, c DATE SCRIPT AS ($inner))")
        .fold(msg => fail(s"rejected: $msg"), identity)
      val pairs = ct.schema.columns
        .find(_.name == "c")
        .flatMap(_.script)
        .flatMap(_.expr)
        .map(ScriptReferences.of(_).size)
        .getOrElse(fail("no script"))
      withClue(s"depth=$depth ")(pairs shouldBe 1)
    }
  }

  "a non-regular table" should "not be validated against its own column list" in {
    // A materialized view's computed column legitimately reads a column of its SOURCE, which the
    // view never declares; at ingest that value is in the incoming document. MV deployment RENDERS
    // a stage's schema to DDL and runs the text, so rejecting these would break deployment.
    // `Table` here must be the SCHEMA one -- `TableDrivenPropertyChecks` shadows the name.
    val mv = app.softnetwork.elastic.sql.schema.Table(
      name = "v",
      columns = List(Column("c", app.softnetwork.elastic.sql.`type`.SQLTypes.Timestamp)),
      tableType = TableType.Enrichment
    )
    validateScriptReferences(mv) shouldBe Right(())
  }
}
