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

import app.softnetwork.elastic.sql.{Identifier, PainlessContext, PainlessContextType}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The computed columns core declared NUMERIC or TEMPORAL declare DuckDB's type (#418).
  *
  *   - DATEDIFF / DATE_DIFF / TIMESTAMPDIFF, every spelling: BIGINT (DuckDB `date_diff`);
  *   - ROUND: its operand's type (DuckDB `RoundFun`), but a BIGINT for a TINYINT, SMALLINT or INT
  *     rounded to a negative scale, NUMERIC over an operand of no known number type;
  *   - EXP / LOG / LOG10 / SQRT / POW: DOUBLE (DuckDB `numeric.cpp`);
  *   - DATE_TRUNC over an operand that reports a date: TIMESTAMP (DuckDB `date_trunc`), in a query
  *     only -- an ingest processor or a transform reads the type it read before.
  *
  * The type REPORTED: `describe`, the readers, set-operation slots, the GROUP BY key level, MIN /
  * MAX / SUM and a `CREATE TABLE … AS SELECT` read it. `outputType` -- what `coerce` and the
  * function's own Painless read -- is unchanged, so a script keeps its bytes, but for the
  * negative-scale ROUND, which renders DuckDB's form (and refuses a BIGINT overflow in a query).
  *
  * The oracle is DuckDB 1.5.5 (`typeof(...)`, run 2026-11-02): `round` over DOUBLE / INTEGER /
  * FLOAT / BIGINT = DOUBLE / INTEGER / FLOAT / BIGINT at every scale; `date_diff` BIGINT; `sqrt`,
  * `exp`, `log10`, `ln`, `pow` DOUBLE; `date_trunc('day', DATE)` / `(TIMESTAMP)` TIMESTAMP,
  * `(current_timestamp)` TIMESTAMP WITH TIME ZONE (core's TIMESTAMP is the UTC instant);
  * `round(423789293::INTEGER, -5)` = 423800000. A narrower integer rounded to a negative scale is a
  * BIGINT here, where DuckDB keeps its type and wraps what leaves it (`round(127::TINYINT, -1)` =
  * -126, `round(2147483647::INTEGER, -1)` = -2147483646, run 2026-11-03): only the label differs,
  * the value (130, 2147483650) always fits a BIGINT.
  */
class NumericWidthReportedTypeSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("tm", SQLTypes.Time),
      Column("s", SQLTypes.Keyword),
      Column("ti", SQLTypes.TinyInt),
      Column("sm", SQLTypes.SmallInt),
      Column("n", SQLTypes.Int),
      Column("big", SQLTypes.BigInt),
      Column("r", SQLTypes.Real),
      Column("x", SQLTypes.Double)
    )
  )

  private def search(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] is not one SELECT: $other")
    }

  /** The resolved statement and the identifier of its column `a`. */
  private def resolved(sql: String): (SingleSearch, Identifier) = {
    val s = search(sql)
    val r = s.update(Some(schema))
    val field = r.select.fields
      .find(_.fieldAlias.map(_.alias).contains("a"))
      .getOrElse(fail(s"no column `a` in [$sql]"))
    (s, field.identifier)
  }

  private def ctas(s: SingleSearch): Column =
    schema
      .mergeWithSearch(s)
      .columns
      .find(_.name == "a")
      .getOrElse(fail(s"no CTAS column `a` for [${s.sql}]"))

  /** `a` is reported, declared by a CTAS and mapped as `expected`. */
  private def assertDeclared(sql: String, expected: SQLType, es: String): Unit = {
    val (s, id) = resolved(sql)
    withClue(s"[$sql] ") {
      id.reportedType shouldBe expected
      val column = ctas(s)
      column.dataType shouldBe expected
      SQLTypeUtils.elasticType(column.dataType) shouldBe es
    }
  }

  /** The script field `a` rendered in a query. */
  private def painlessOf(sql: String): String = {
    val (s, _) = resolved(sql)
    val fields = s.update(Some(schema)).scriptFields.filterNot(_.isAggregation)
    withClue(s"[$sql] ") { fields.size shouldBe 1 }
    fields.head.painless(Some(PainlessContext(context = PainlessContextType.Query)))
  }

  /** The GROUP BY key level of `GROUP BY <expr>` (`value_type` of the scripted `terms`). */
  private def keyLevel(expr: String): Option[String] = {
    val s = search(s"SELECT $expr AS a, COUNT(*) AS c FROM t GROUP BY $expr").update(Some(schema))
    s.groupBy.getOrElse(fail("no GROUP BY")).buckets.head.scriptedKeyValueType
  }

  // -------------------------------------------------------------------------------------------
  // DATEDIFF family (D5)
  // -------------------------------------------------------------------------------------------

  private val spellings: List[String] = List(
    "DATEDIFF(%1$s, %2$s)",
    "DATE_DIFF(%1$s, %2$s, %3$s)",
    "DATEDIFF(%1$s, %2$s, %3$s)",
    "DATEDIFF(%3$s, %2$s, %1$s)",
    "DATE_DIFF(%3$s, %2$s, %1$s)",
    "TIMESTAMPDIFF(%3$s, %2$s, %1$s)",
    "TIMESTAMPDIFF(%1$s, %2$s, %3$s)"
  )

  behavior of "DATEDIFF / DATE_DIFF / TIMESTAMPDIFF"

  it should "report BIGINT in every spelling, over a DATE and a TIMESTAMP, DAY and HOUR, mapped long" in {
    for (
      spelling <- spellings;
      (a, b)   <- List(("d", "d2"), ("ts", "d2"), ("ts", "ts"));
      unit     <- List("DAY", "HOUR")
    ) {
      val sql = s"SELECT ${spelling.format(a, b, unit)} AS a FROM t"
      assertDeclared(sql, SQLTypes.BigInt, "long")
    }
  }

  it should "keep its outputType NUMERIC, so its Painless keeps its bytes" in {
    val (_, id) = resolved("SELECT DATEDIFF(d, d2) AS a FROM t")
    id.functions.headOption.map(_.out) shouldBe Some(SQLTypes.Numeric)
  }

  it should "be a long GROUP BY key level, MIN / MAX BIGINT and SUM BIGINT" in {
    keyLevel("DATEDIFF(d, d2)") shouldBe Some("long")
    keyLevel("TIMESTAMPDIFF(HOUR, ts, ts)") shouldBe Some("long")
    assertDeclared("SELECT MAX(DATEDIFF(d, d2)) AS a FROM t", SQLTypes.BigInt, "long")
    assertDeclared("SELECT MIN(DATEDIFF(d, d2)) AS a FROM t", SQLTypes.BigInt, "long")
    // SUM over a whole type is BIGINT (A-20): was DOUBLE over the NUMERIC chain
    assertDeclared("SELECT SUM(DATEDIFF(d, d2)) AS a FROM t", SQLTypes.BigInt, "long")
  }

  // DuckDB 1.5.5: `typeof(greatest(date_diff('day', d2, d), n))` BIGINT
  it should "make GREATEST / LEAST beside an INT a BIGINT, cast as one" in {
    for (fn <- List("GREATEST", "LEAST")) {
      val sql = s"SELECT $fn(DATEDIFF(d, d2), n) AS a FROM t"
      assertDeclared(sql, SQLTypes.BigInt, "long")
      withClue(s"[$sql] ") { painlessOf(sql) should include("(long)") }
    }
  }

  // the date-arithmetic ruling: a DATE plus whole days is a DATE (`ArithmeticExpression.operandOf`
  // reads the declared type)
  it should "make a DATE plus DATEDIFF a DATE" in {
    resolved("SELECT d + DATEDIFF(d, d2) AS a FROM t")._2.reportedType shouldBe SQLTypes.Date
  }

  // -------------------------------------------------------------------------------------------
  // ROUND (D7, D8, A-39)
  // -------------------------------------------------------------------------------------------

  behavior of "ROUND"

  private val scales: List[String] = List("", ", 0", ", 1")

  private val negativeScales: List[String] = List(", -1", ", -5")

  it should "report its operand's type at a scale of zero or more, mapped by it" in {
    for (
      (operand, expected, es) <- List(
        ("ti", SQLTypes.TinyInt, "byte"),
        ("sm", SQLTypes.SmallInt, "short"),
        ("n", SQLTypes.Int, "integer"),
        ("big", SQLTypes.BigInt, "long"),
        ("r", SQLTypes.Real, "float"),
        ("x", SQLTypes.Double, "double")
      );
      scale <- scales
    ) assertDeclared(s"SELECT ROUND($operand$scale) AS a FROM t", expected, es)
  }

  // R-67 -- to a negative scale a TINYINT, SMALLINT or INT is a BIGINT: its rounded value always
  // fits one (ROUND(2147483647, -1) = 2147483650), where its own type would overflow
  it should "report BIGINT for a narrower integer at a negative scale, its type otherwise" in {
    for (
      (operand, expected, es) <- List(
        ("ti", SQLTypes.BigInt, "long"),
        ("sm", SQLTypes.BigInt, "long"),
        ("n", SQLTypes.BigInt, "long"),
        ("big", SQLTypes.BigInt, "long"),
        ("r", SQLTypes.Real, "float"),
        ("x", SQLTypes.Double, "double")
      );
      scale <- negativeScales
    ) assertDeclared(s"SELECT ROUND($operand$scale) AS a FROM t", expected, es)
    assertDeclared("SELECT ROUND(CAST(2147483647 AS INT), -1) AS a FROM t", SQLTypes.BigInt, "long")
    assertDeclared("SELECT ROUND(CAST(127 AS TINYINT), -1) AS a FROM t", SQLTypes.BigInt, "long")
    // a negative scale over no known number type keeps NUMERIC
    resolved("SELECT ROUND(s, -1) AS a FROM t")._2.reportedType shouldBe SQLTypes.Numeric
  }

  it should "keep NUMERIC over an operand of no known number type, and its outputType NUMERIC" in {
    resolved("SELECT ROUND(s) AS a FROM t")._2.reportedType shouldBe SQLTypes.Numeric
    val (_, unresolved) = {
      val s = search("SELECT ROUND(n) AS a FROM t")
      (s, s.select.fields.head.identifier)
    }
    unresolved.reportedType shouldBe SQLTypes.Numeric
    resolved("SELECT ROUND(n) AS a FROM t")._2.functions.headOption.map(_.out) shouldBe Some(
      SQLTypes.Numeric
    )
  }

  it should "be a long key level over a whole operand, a double one over DOUBLE and REAL" in {
    keyLevel("ROUND(n)") shouldBe Some("long")
    keyLevel("ROUND(big, -1)") shouldBe Some("long")
    keyLevel("ROUND(n, -1)") shouldBe Some("long")
    keyLevel("ROUND(x)") shouldBe Some("double")
    keyLevel("ROUND(r, 1)") shouldBe Some("double")
  }

  it should "make MIN / MAX its type and SUM over a whole operand BIGINT" in {
    assertDeclared("SELECT MAX(ROUND(n)) AS a FROM t", SQLTypes.Int, "integer")
    assertDeclared("SELECT MIN(ROUND(x)) AS a FROM t", SQLTypes.Double, "double")
    assertDeclared("SELECT SUM(ROUND(n)) AS a FROM t", SQLTypes.BigInt, "long")
    assertDeclared("SELECT SUM(ROUND(x, 1)) AS a FROM t", SQLTypes.Double, "double")
    // R-67: a negative scale over an INT / TINYINT is a BIGINT, and so are its MIN / MAX / SUM
    assertDeclared("SELECT MAX(ROUND(n, -1)) AS a FROM t", SQLTypes.BigInt, "long")
    assertDeclared("SELECT MIN(ROUND(ti, -1)) AS a FROM t", SQLTypes.BigInt, "long")
    assertDeclared("SELECT SUM(ROUND(n, -1)) AS a FROM t", SQLTypes.BigInt, "long")
  }

  // D8 -- DuckDB's `RoundOperatorPrecision`: the magnitude divided by 10^-s, rounded, multiplied
  // back; `s >= 0` keeps its bytes
  it should "render a negative scale as DuckDB's form, a non-negative one as before" in {
    // the power of ten is a parameter (`param2`): 10^5 for -5, 10^2 for 2
    val negative = painlessOf("SELECT ROUND(x, -5) AS a FROM t")
    negative should include("Math.round(Math.abs(param1) / param2) * param2")
    negative should not include "Overflow"
    val positive = painlessOf("SELECT ROUND(x, 2) AS a FROM t")
    positive should include("Math.round(Math.abs(param1) * param2) / param2")
    painlessOf("SELECT ROUND(x) AS a FROM t") should include("Long.valueOf((long) (")
    val (s, _) = resolved("SELECT ROUND(x, -5) AS a FROM t")
    s.update(Some(schema)).scriptFields.head.identifier.functions.collectFirst {
      case r: app.softnetwork.elastic.sql.function.math.Round => r.args.last.painless(None)
    } shouldBe Some("Math.pow(10, 5)")
  }

  // A-39, R-67 -- a BIGINT rounded out of its range fails the query, where DuckDB v1.5.5 wraps it
  // (round(9223372036854775807::BIGINT, -1) = -9223372036854775800, run 2026-11-03), never copied.
  // A narrower integer is a BIGINT at a negative scale, which its rounded value always fits: no
  // test, nothing refused (ROUND(CAST(2147483647 AS INT), -1) answers 2147483650)
  it should "fail a BIGINT rounded out of its range, in a query only" in {
    val big = painlessOf("SELECT ROUND(big, -1) AS a FROM t")
    big should include("< -9.223372036854775808E18 || ")
    big should include(">= 9.223372036854775808E18")
    big should include("new IllegalArgumentException('Overflow in ROUND of integer')")
    painlessOf("SELECT ROUND(big, -19) AS a FROM t") should include(
      "new IllegalArgumentException('Overflow in ROUND of integer')"
    )
    for (
      sql <- List(
        "SELECT ROUND(ti, -1) AS a FROM t",
        "SELECT ROUND(sm, -1) AS a FROM t",
        "SELECT ROUND(n, -1) AS a FROM t",
        "SELECT ROUND(n, -5) AS a FROM t",
        "SELECT ROUND(CAST(2147483647 AS INT), -1) AS a FROM t",
        "SELECT ROUND(CAST(127 AS TINYINT), -1) AS a FROM t",
        // a DOUBLE and a REAL have no such range
        "SELECT ROUND(x, -1) AS a FROM t",
        "SELECT ROUND(r, -1) AS a FROM t"
      )
    ) withClue(s"[$sql] ") {
      val painless = painlessOf(sql)
      painless should not include "Overflow"
      painless should include("Math.round(Math.abs(")
    }
    // an ingest processor keeps the bytes it had (D8's form, no guard)
    val (s, _) = resolved("SELECT ROUND(big, -1) AS a FROM t")
    val field = s.update(Some(schema)).scriptFields.filterNot(_.isAggregation).head
    field.painless(
      Some(PainlessContext(context = PainlessContextType.Processor))
    ) should not include
    "Overflow"
  }

  // -------------------------------------------------------------------------------------------
  // EXP / LOG / LOG10 / SQRT / POW (A-35)
  // -------------------------------------------------------------------------------------------

  behavior of "EXP / LOG / LOG10 / SQRT / POW"

  it should "report DOUBLE whatever the operand, schema-less included, mapped double" in {
    for (
      fn      <- List("EXP(%s)", "LOG(%s)", "LOG10(%s)", "SQRT(%s)", "POW(%s, 2)", "POWER(%s, 3)");
      operand <- List("n", "big", "x", "r")
    ) assertDeclared(s"SELECT ${fn.format(operand)} AS a FROM t", SQLTypes.Double, "double")
    for (fn <- List("EXP(n)", "LOG(n)", "LOG10(n)", "SQRT(n)", "POW(n, 2)"))
      search(s"SELECT $fn AS a FROM t").select.fields.head.identifier.reportedType shouldBe
      SQLTypes.Double
  }

  it should "keep a double key level and its Painless" in {
    keyLevel("SQRT(n)") shouldBe Some("double")
    painlessOf("SELECT SQRT(x) AS a FROM t") should include("Math.sqrt(")
  }

  // -------------------------------------------------------------------------------------------
  // DATE_TRUNC (D15)
  // -------------------------------------------------------------------------------------------

  behavior of "DATE_TRUNC"

  private val dateOperands: List[String] = List(
    "d",
    "ts",
    "CURRENT_TIMESTAMP",
    "NOW()",
    "CURRENT_DATE",
    "CAST(ts AS DATE)",
    "DATE_PARSE(s, '%Y-%m-%d')",
    "LAST_DAY(ts)",
    "DATE_ADD(d, INTERVAL 1 DAY)"
  )

  private val units: List[String] = List("DAY", "WEEK", "MONTH", "QUARTER", "HOUR")

  it should "report TIMESTAMP over every operand that reports a date, at every unit, mapped date" in {
    for (operand <- dateOperands; unit <- units)
      assertDeclared(s"SELECT DATE_TRUNC($operand, $unit) AS a FROM t", SQLTypes.Timestamp, "date")
  }

  // Premise correction (T0-2, 2026-11-02): over a TIME column DATE_TRUNC already reports TIMESTAMP
  // on main (the column's runtime type, `Identifier.reportedType`'s fallback), and keeps it
  it should "keep the type it reported over a TIME and over an operand of no known type" in {
    resolved("SELECT DATE_TRUNC(tm, HOUR) AS a FROM t")._2.reportedType shouldBe SQLTypes.Timestamp
    resolved("SELECT DATE_TRUNC(s, DAY) AS a FROM t")._2.reportedType shouldBe SQLTypes.Temporal
  }

  it should "keep main's type while an ingest processor renders" in {
    val processor = Some(PainlessContext(context = PainlessContextType.Processor))
    for (
      (operand, main) <- List(
        "CAST(ts AS DATE)"  -> SQLTypes.Date,
        "LAST_DAY(ts)"      -> SQLTypes.Timestamp,
        "CURRENT_DATE"      -> SQLTypes.Temporal,
        "CURRENT_TIMESTAMP" -> SQLTypes.Temporal
      )
    ) {
      val (_, id) = resolved(s"SELECT DATE_TRUNC($operand, DAY) AS a FROM t")
      withClue(s"[$operand] ") {
        SQLTypeUtils.renderingIn(processor)(id.reportedType) shouldBe main
        id.reportedType shouldBe SQLTypes.Timestamp
      }
    }
  }

  // D12 (8): over a date-producing operand it was a DATE -- `… - d` was whole days, `… + 1` a DATE
  it should "make date arithmetic over it a TIMESTAMP's" in {
    resolved("SELECT DATE_TRUNC(CAST(ts AS DATE), DAY) - d AS a FROM t")._2.reportedType shouldBe
    SQLTypes.Double
    resolved("SELECT DATE_TRUNC(CAST(ts AS DATE), DAY) + 1 AS a FROM t")._2.reportedType shouldBe
    SQLTypes.Timestamp
    resolved(
      "SELECT COALESCE(DATE_TRUNC(CURRENT_DATE, DAY), d) AS a FROM t"
    )._2.reportedType shouldBe
    SQLTypes.Timestamp
  }

  it should "keep its outputType TEMPORAL, so its Painless keeps its bytes" in {
    resolved("SELECT DATE_TRUNC(d, MONTH) AS a FROM t")._2.functions.headOption.map(_.out) shouldBe
    Some(SQLTypes.Temporal)
  }

  // -------------------------------------------------------------------------------------------
  // One answer per scope (A-45): a node a query and an ingest processor both render
  // -------------------------------------------------------------------------------------------

  behavior of "a node rendered in a query and in an ingest processor"

  /** The select item `a` of the resolved statement: a NEW node per call. */
  private def fieldOf(sql: String): Field =
    search(sql)
      .update(Some(schema))
      .select
      .fields
      .find(_.fieldAlias.map(_.alias).contains("a"))
      .getOrElse(fail(s"no column `a` in [$sql]"))

  /** What `field` renders in a fresh context of `kind`, its parameters included; a throw is an
    * answer too.
    */
  private def rendered(field: Field, kind: PainlessContextType): String = {
    val context = PainlessContext(context = kind)
    scala.util.Try(field.painless(Some(context))).fold(t => s"THROW $t", body => s"$context$body")
  }

  private def processorType(field: Field): SQLType =
    SQLTypeUtils.renderingIn(Some(PainlessContext(context = PainlessContextType.Processor)))(
      field.identifier.reportedType
    )

  // the cells whose answer depends on the scope -- DATE_TRUNC over a date (D15), DATE_ADD over a
  // DATE, a conditional -- and a comparison and an IN whose literal is read beside DATE_TRUNC (a
  // TIMESTAMP in a query, a DATE in a processor: the literal's time of day is kept or dropped)
  private val scoped: List[String] = List(
    "DATE_TRUNC(CAST(ts AS DATE), DAY) + 1",
    "DATE_TRUNC(CAST(ts AS DATE), DAY) - d",
    "DATE_TRUNC(LAST_DAY(ts), DAY) + 1",
    "GREATEST(DATE_TRUNC(CAST(ts AS DATE), DAY), d)",
    "COALESCE(DATE_TRUNC(CAST(ts AS DATE), DAY), d)",
    "DATEDIFF(DATE_TRUNC(CAST(ts AS DATE), MONTH), d)",
    "DATE_ADD(d, INTERVAL 1 DAY) + 1",
    "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE d2 END",
    "CASE WHEN DATE_TRUNC(CAST(ts AS DATE), DAY) = '2024-01-01T10:00:00Z' THEN 1 ELSE 0 END",
    "CASE WHEN DATE_TRUNC(CAST(ts AS DATE), DAY) IN ('2024-01-01T10:00:00Z', '2024-01-02') " +
    "THEN 1 ELSE 0 END"
  )

  it should "answer what a fresh node answers in each scope, whichever scope renders first" in {
    val query = PainlessContextType.Query
    val processor = PainlessContextType.Processor
    for (e <- scoped) {
      val sql = s"SELECT $e AS a FROM t"
      val freshQuery = rendered(fieldOf(sql), query)
      val freshProcessor = rendered(fieldOf(sql), processor)
      val freshType = fieldOf(sql).identifier.reportedType
      val freshProcessorType = processorType(fieldOf(sql))
      withClue(s"[$e] query first: ") {
        val node = fieldOf(sql)
        node.identifier.reportedType shouldBe freshType
        rendered(node, query) shouldBe freshQuery
        rendered(node, processor) shouldBe freshProcessor
        processorType(node) shouldBe freshProcessorType
        node.identifier.reportedType shouldBe freshType
      }
      withClue(s"[$e] processor first: ") {
        val node = fieldOf(sql)
        processorType(node) shouldBe freshProcessorType
        rendered(node, processor) shouldBe freshProcessor
        node.identifier.reportedType shouldBe freshType
        rendered(node, query) shouldBe freshQuery
        processorType(node) shouldBe freshProcessorType
      }
    }
    // the scopes do read differently here, so the cells above can tell a shared answer apart
    val node = fieldOf("SELECT DATE_TRUNC(CAST(ts AS DATE), DAY) - d AS a FROM t")
    node.identifier.reportedType shouldBe SQLTypes.Double
    processorType(node) shouldBe SQLTypes.BigInt
  }

  // A-45 (1): outside a query a DATEDIFF, or a ROUND over an integer, moves a date by the
  // fractional number of days it was read as before it reported a width -- the processor's and
  // the transform's bytes are main's (an ALTER re-creates no processor); a query moves it by whole
  // days, a DATE
  it should "keep main's processor and transform bytes for a date moved by DATEDIFF or ROUND" in {
    val mainTail = " + Math.round(lv1 * 86400000.0)))).atZone(ZoneId.of('Z')))"
    for (e <- List("d + DATEDIFF(d, d2)", "d + ROUND(n)", "d - ROUND(n, 1)")) {
      val sql = s"SELECT $e AS a FROM t"
      withClue(s"[$e] ") {
        rendered(fieldOf(sql), PainlessContextType.Processor) should include(
          mainTail.replace(" + ", if (e.contains(" - ")) " - " else " + ")
        )
        rendered(fieldOf(sql), PainlessContextType.Query) should not include "Math.round(lv1 *"
        fieldOf(sql).identifier.reportedType shouldBe SQLTypes.Date
      }
    }
    // the transform scripts, byte for byte as main renders them
    val transform = PainlessContextType.Transform
    rendered(fieldOf("SELECT d + DATEDIFF(d, d2) AS a FROM t"), transform) shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); def param2 = (doc['d2'].size() " +
    "== 0 ? null : doc['d2'].value); def lv1 = (param2 == null || param1 == null) ? null : " +
    "Long.valueOf(ChronoUnit.DAYS.between(param2.toLocalDate(), param1.toLocalDate())); (param1 " +
    "== null || lv1 == null) ? null : (def)(Instant.ofEpochMilli(((long) (Math.floor(param1" +
    ".toInstant().toEpochMilli() / 86400000.0) * 86400000.0 + Math.round(lv1 * 86400000.0))))" +
    ".atZone(ZoneId.of('Z')))"
    rendered(fieldOf("SELECT d + ROUND(n) AS a FROM t"), transform) shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); def param2 = (doc['n'].size() " +
    "== 0 ? null : doc['n'].value); def param3 = Math.pow(10, 0); def lv1 = (param2 == null || " +
    "param3 == null) ? null : Long.valueOf((long) (Math.signum(param2) * Math.round(Math" +
    ".abs(param2) * param3) / param3)); (param1 == null || lv1 == null) ? null : (def)(Instant" +
    ".ofEpochMilli(((long) (Math.floor(param1.toInstant().toEpochMilli() / 86400000.0) * " +
    "86400000.0 + Math.round(lv1 * 86400000.0)))).atZone(ZoneId.of('Z')))"
    // the SCRIPT AS pipeline the parser builds
    for (e <- List("d + DATEDIFF(d, d2)", "d + ROUND(n)")) {
      val ddl = s"CREATE TABLE x (id KEYWORD, d DATE, d2 DATE, n INT, v DATE SCRIPT AS ($e))"
      val source = Parser(ddl) match {
        case Right(ct: CreateTable) =>
          ct.schema.columns.find(_.name == "v").flatMap(_.script).map(_.source)
        case other => fail(s"[$ddl] expected a CreateTable, got $other")
      }
      withClue(s"[$ddl] ") {
        source.getOrElse(fail("no script processor")) should include(
          "86400000.0 + Math.round(lv1 * 86400000.0)"
        )
      }
    }
    // a quoted number beside them is still the whole number it rounds to there (R-54)
    rendered(fieldOf("SELECT ROUND(n) + '1.5' AS a FROM t"), PainlessContextType.Processor) should
    endWith(" + 2L")
  }
}
