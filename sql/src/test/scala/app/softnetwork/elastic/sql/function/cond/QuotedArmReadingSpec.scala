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

package app.softnetwork.elastic.sql.function.cond

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, PainlessScript}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch, Update}
import app.softnetwork.elastic.sql.schema.{
  validateScriptReferences,
  Column,
  Schema,
  ScriptProcessor,
  Table => SchemaTable
}
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** A quoted argument of `COALESCE`, `GREATEST`, `LEAST` and a `CASE` result is read as the type of
  * the arguments beside it, as DuckDB 1.5.5 reads it (SCHEMA-5b, D1 - D5): the verdict, the
  * reported type and the rendering, in a query and in an ingest processor.
  */
class QuotedArmReadingSpec extends AnyFlatSpec with Matchers {

  private val table = SchemaTable(
    "t",
    columns = List(
      Column("ti", SQLTypes.TinyInt),
      Column("n", SQLTypes.Int),
      Column("l", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("r", SQLTypes.Real),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("b", SQLTypes.Boolean),
      Column("k", SQLTypes.Keyword),
      Column("g", SQLTypes.Keyword),
      Column("ab", SQLTypes("array<boolean>")),
      Column("an", SQLTypes("array<int>"))
    )
  )

  private def query = Some(PainlessContext(PainlessContextType.Query))

  private def resolved(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(table))
    case other                  => fail(s"not a single search: $other")
  }

  /** The verdict, the reported type and the query Painless of `SELECT <expr> AS r FROM t`. */
  private def select(expr: String): (Option[String], String, String) = {
    val s = resolved(s"SELECT $expr AS r FROM t")
    val field = s.select.fieldsWithComputedAliases.head
    (
      s.validateResolved().left.toOption,
      field.identifier.reportedType.typeId,
      field.painless(query)
    )
  }

  private def accepted(expr: String, reported: String): String = {
    val (verdict, t, painless) = select(expr)
    withClue(s"$expr: ") {
      verdict shouldBe None
      t shouldBe reported
    }
    painless
  }

  private val columns =
    "id KEYWORD, ti TINYINT, n INT, l BIGINT, x DOUBLE, r REAL, d DATE, ts TIMESTAMP, b BOOLEAN"

  /** The ingest processor of a computed column `c <decl> SCRIPT AS (<expr>)`. */
  private def processor(decl: String, expr: String): String =
    Parser(s"CREATE TABLE t ($columns, c $decl SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        ct.schema.defaultPipeline.processors
          .collectFirst { case p: ScriptProcessor if p.column == "c" => p.source }
          .getOrElse(fail(s"no processor for $expr"))
      case other => fail(s"$expr: $other")
    }

  /** The ingest processor of `UPDATE t SET c = <expr>`. */
  private def updateProcessor(expr: String): String =
    Parser(s"UPDATE t SET c = $expr WHERE n > 0") match {
      case Right(u: Update) =>
        u.customPipeline.processors
          .collectFirst { case p: ScriptProcessor => p.source }
          .getOrElse(fail(s"no processor for $expr"))
      case other => fail(s"$expr: $other")
    }

  /** The transform script of `SELECT <expr> AS r FROM t`. */
  private def transform(expr: String): String =
    resolved(s"SELECT $expr AS r FROM t").select.fieldsWithComputedAliases.head
      .painless(Some(PainlessContext(PainlessContextType.Transform)))

  behavior of "a quoted argument beside a number"

  it should "be read as the whole number it rounds to, half away from zero (R-54)" in {
    // main refused it: GREATEST requires numeric arguments ... but got VARCHAR for '1.5'
    val greatest = accepted("GREATEST(n, '1.5')", "INT")
    greatest should not include "\"1.5\""
    greatest should include("param1 >= 2 ? param1 : 2")
    accepted("LEAST('-2.5', n)", "INT") should include("-3")
    accepted("GREATEST(l, '0.5')", "BIGINT") should include("1L")
  }

  it should "take the operand's type, DuckDB's label (D2)" in {
    accepted("GREATEST(ti, '1.5')", "TINYINT") should include("((byte) 2)")
    accepted("COALESCE(x, '3')", "DOUBLE") should include("3.0")
    accepted("COALESCE(n, ' 2 ')", "INT") should include("param1 : 2")
  }

  it should "widen the node to BIGINT past the operand's whole range, never wrap (D2)" in {
    accepted("COALESCE(ti, '200')", "BIGINT") should include("200L")
    accepted("GREATEST(n, '9223372036854775807.4')", "BIGINT") should include(
      "9223372036854775807L"
    )
  }

  it should "answer the number in a COALESCE, ending it when the literal comes first (D3)" in {
    // main answered the text "3" on every row, reported VARCHAR
    accepted("COALESCE(n, '3')", "INT") shouldBe "(param1 != null ? param1 : 3)"
    val first = accepted("COALESCE('3', n)", "INT")
    first should not include "!= null"
    first should include("3")
  }

  it should "keep main's outcome for a text that reads as nothing (D11)" in {
    val (verdict, t, painless) = select("COALESCE(n, 'abc')")
    verdict shouldBe None
    t shouldBe "VARCHAR"
    painless shouldBe "(param1 != null ? param1 : \"abc\")"
    select("GREATEST(n, 'abc')")._1.getOrElse("") should include("requires numeric arguments")
    select("COALESCE(d, '3')")._2 shouldBe "VARCHAR"
  }

  it should "read a CASE result as the other results' type (D4, A-77)" in {
    val painless = accepted("CASE WHEN n > 0 THEN '1.5' ELSE n END", "INT")
    painless should not include "\"1.5\""
    painless should include("2")
  }

  behavior of "a quoted argument beside a date"

  it should "be read as the date it names, a date-time's date beside a DATE" in {
    // 2024-01-15 is epoch day 19737, its UTC midnight 1705276800000 ms
    accepted("GREATEST(d, '2024-01-15 10:30:00')", "DATE") should include("1705276800000L")
    accepted("COALESCE(d, '2024-01-15')", "DATE") should include("LocalDate.ofEpochDay(19737L)")
    accepted("LEAST(ts, '2024-01-15')", "TIMESTAMP") should include("1705276800000L")
  }

  behavior of "a BOOLEAN beside whole numbers (D5)"

  /** The whole query script of `SELECT <expr> AS r FROM t`: its declarations, then its value. */
  private def script(expr: String, reported: String): String = {
    accepted(expr, reported)
    val ctx = PainlessContext(PainlessContextType.Query)
    val painless =
      resolved(s"SELECT $expr AS r FROM t").select.fieldsWithComputedAliases.head
        .painless(Some(ctx))
    s"$ctx$painless"
  }

  it should "take their type and be converted to 1 / 0 in a query" in {
    // main refused GREATEST(b, 1) and answered COALESCE(b, 1)'s Boolean in a BIGINT column
    script("GREATEST(b, 1)", "BIGINT") should include("? 1L : 0L")
    script("LEAST(b, 1)", "BIGINT") should include("? 1L : 0L")
    script("COALESCE(b, 1)", "BIGINT") should include("? 1L : 0L")
    script("CASE WHEN n > 0 THEN b ELSE 1 END", "BIGINT") should include("? 1L : 0L")
  }

  it should "keep main's outcome beside a fractional argument, and alone" in {
    select("GREATEST(b, x)")._1.getOrElse("") should include("but got BOOLEAN for b")
    select("GREATEST(b, b)")._1.getOrElse("") should include("but got BOOLEAN for b")
    val (verdict, t, painless) = select("COALESCE(b, x)")
    verdict shouldBe None
    t shouldBe "DOUBLE"
    painless shouldBe "(param1 != null ? param1 : param2)"
  }

  behavior of "an ingest processor (D3 (5), R-107)"

  it should "render the value a literal is read as" in {
    // main: def param1 = ctx.n; ctx.c = (param1 != null ? param1 : "3")
    processor("INT", "COALESCE(n, '3')") shouldBe
    "def param1 = ctx.n; ctx.c = (param1 != null ? param1 : 3)"
    processor("INT", "GREATEST(n, '1.5')") should not include "\"1.5\""
    processor("DATE", "COALESCE(d, '2024-01-15 10:30:00')") should include("\"2024-01-15\"")
  }

  /** Does `source` call a method on a string literal (`"2024-01-15".atStartOfDay(…)`,
    * `("…").toString()`)? Painless refuses such a script when the pipeline is created.
    */
  private def callsOnStringLiteral(source: String): Boolean =
    "\"[^\"]*\"\\.[A-Za-z]".r.findFirstIn(source).isDefined ||
    "(^|[^A-Za-z0-9_])\\(\\s*\"[^\"]*\"\\s*\\)\\.[A-Za-z]".r.findFirstIn(source).isDefined

  it should "store a date read in a CASE result as its ISO text, never convert the text (A-89)" in {
    // before A-89: ... : "2024-01-15".atStartOfDay(ZoneId.of('Z')) -- a method call on a string
    // literal, so Painless refused a CREATE / ALTER that main and 0.24.0 accept. main: "2024-01-15"
    Seq(
      "CASE WHEN n > 0 THEN d ELSE '2024-01-15' END",
      "CASE WHEN n > 0 THEN '2024-01-15' ELSE d END",
      "CASE WHEN n > 0 THEN d ELSE '2024-01-15 10:30:00' END",
      "CASE WHEN n > 0 THEN '2024-01-15 10:30:00' ELSE d END"
    ).foreach { expr =>
      val source = processor("DATE", expr)
      withClue(s"$expr -> $source: ") {
        source should include("\"2024-01-15\"")
        callsOnStringLiteral(source) shouldBe false
      }
    }
    // beside a TIMESTAMP, the literal as written: main's bytes, the same instant (A-92; was the
    // ISO date-time "2024-01-15T00:00:00Z", which main's `yyyy-MM-dd` parse of a consumer refuses)
    val ts = processor("TIMESTAMP", "CASE WHEN n > 0 THEN ts ELSE '2024-01-15' END")
    withClue(ts) {
      ts should include(": \"2024-01-15\"")
      ts should not include "T00:00:00Z"
      callsOnStringLiteral(ts) shouldBe false
    }
    // toward another type (a cast over the CASE), the literal renders as on main
    val cast = processor("VARCHAR", "CAST(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END AS VARCHAR)")
    withClue(cast)(callsOnStringLiteral(cast) shouldBe false)
  }

  it should "keep a read date's text type outside a query, so a consumer parses it (A-92)" in {
    // before A-92 the node was a DATE outside a query while its value was still text:
    // `(param1 != null ? param1 : "2024-01-15").plus(1, ChronoUnit.DAYS)` failed every document
    // (`dynamic method [java.lang.String, plus/2] not found`) and the column was silently absent
    processor("DATE", "DATE_ADD(COALESCE(d, '2024-01-15'), INTERVAL 1 DAY)") shouldBe
    "def param1 = ctx.d; def param2 = LocalDate.parse(((param1 != null ? param1 : \"2024-01-15\"))" +
    ".replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "ctx.c = param2.plus(1, ChronoUnit.DAYS)"
    val overCase =
      processor("DATE", "DATE_ADD(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END, INTERVAL 1 DAY)")
    withClue(overCase) {
      overCase should include(": \"2024-01-15\"; def param5 = LocalDate.parse((param4)")
      callsOnStringLiteral(overCase) shouldBe false
    }
    // a TIMESTAMP reading renders the literal as written, and is tested as text: main's bytes
    processor("TIMESTAMP", "COALESCE(ts, '2024-01-15 10:30:00')") shouldBe
    "def param1 = ctx.ts; ctx.c = (param1 != null ? param1 : \"2024-01-15 10:30:00\")"
    processor("TIMESTAMP", "COALESCE('2024-01-15 10:30:00', ts)") shouldBe
    "def param1 = ctx.ts; ctx.c = (\"2024-01-15 10:30:00\" != null ? \"2024-01-15 10:30:00\" : param1)"
    processor("DATE", "DATE_ADD(COALESCE(ts, '2024-01-15'), INTERVAL 1 DAY)") should include(
      "(param1 != null ? param1 : \"2024-01-15\")"
    )
    // under a cast, the arm renders as on main (`CAST(CASE … END AS TIMESTAMP)` keeps main's bytes)
    processor(
      "TIMESTAMP",
      "CAST(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END AS TIMESTAMP)"
    ) should include(": \"2024-01-15\").replace(\" \", \"T\")")
    // a transform reads text too: no java.time value is built from the literal
    val transform =
      resolved(
        "SELECT COALESCE(d, '2024-01-15 10:30:00') AS r FROM t"
      ).select.fieldsWithComputedAliases.head
        .painless(Some(PainlessContext(PainlessContextType.Transform)))
    withClue(transform) {
      transform should include("\"2024-01-15\"")
      transform should not include "LocalDate.ofEpochDay"
    }
  }

  it should "be compared as its text outside a query, as 0.24.0 compares it (R1-1)" in {
    // before: the comparison read the node as a DATE and its literal as a date over the text a
    // processor holds -- `def param2 = LocalDate.parse(("2024-01-15")…); …
    // left1.isEqual(param2)` on the String left1: CREATE and ALTER were accepted, every document
    // failed and the column was silently absent. The strings below are main's (and 0.24.0's).
    val eq = "CASE WHEN COALESCE(d, '2024-01-15') = '2024-01-15' THEN 1 ELSE 0 END"
    val eqBytes = "def param1 = ctx.d; def left1 = (param1 != null ? param1 : \"2024-01-15\"); " +
      "def param2 = (left1 == null ? false : ((left1.compareTo(\"2024-01-15\") == 0))); " +
      "ctx.c = param2 ? 1 : 0"
    processor("INT", eq) shouldBe eqBytes
    updateProcessor(eq) shouldBe eqBytes
    processor(
      "INT",
      "CASE WHEN COALESCE(d, '2024-01-15') IN ('2024-01-15', '2024-02-01') THEN 1 ELSE 0 END"
    ) shouldBe "def param1 = ctx.d; def left1 = (param1 != null ? param1 : \"2024-01-15\"); " +
    "def param2 = (left1 == null ? false : (([\"2024-01-15\",\"2024-02-01\"].contains(left1)))); " +
    "ctx.c = param2 ? 1 : 0"
    processor("INT", "CASE COALESCE(d, '2024-01-15') WHEN '2024-01-15' THEN 1 ELSE 0 END") shouldBe
    "def param1 = ctx.d; def param2 = (param1 != null ? param1 : \"2024-01-15\"); " +
    "def param3 = \"2024-01-15\"; " +
    "ctx.c = param2 != null && param3 != null && param2.compareTo(param3) == 0 ? 1 : 0"
    processor("DATE", "NULLIF(COALESCE(d, '2024-01-15'), '2024-01-15')") shouldBe
    "def param1 = ctx.d; def nif1 = (param1 != null ? param1 : \"2024-01-15\"); " +
    "def param2 = nif1 == null || " +
    "(\"2024-01-15\" != null && nif1.compareTo(\"2024-01-15\") == 0) ? null : nif1; ctx.c = param2"
    // every operator over every such conditional, in a pipeline and in a transform: no java.time
    // value is built from a compared literal, none is compared with the text
    val operands = Seq(
      "COALESCE(d, '2024-01-15')",
      "COALESCE(d, '2024-01-15 10:30:00')",
      "COALESCE('2024-01-15', d)",
      "COALESCE(ts, '2024-01-15')",
      "COALESCE(ts, '2024-01-15 10:30:00')",
      "CASE WHEN n > 0 THEN d ELSE '2024-01-15' END",
      "CASE WHEN n > 0 THEN '2024-01-15' ELSE d END",
      "CASE WHEN n > 0 THEN ts ELSE '2024-01-15' END",
      "CASE WHEN n > 0 THEN ts ELSE '2024-01-15 10:30:00' END",
      "COALESCE(COALESCE(d, '2024-01-15'), d)"
    )
    // `NULLIF(COALESCE(d, '2024-01-15'), d)` is judged as its processor types it (A-96): a text
    // compared with a date, which a CREATE refuses as main does where it judges that NULLIF -- the
    // operand of a `CASE … WHEN`, the value of a NULLIF; inside a WHEN condition it is accepted, as
    // on main (below)
    val nullIfOperand = "NULLIF(COALESCE(d, '2024-01-15'), d)"
    val shapes: Seq[(String, String => String)] = Seq(
      "INT"  -> (o => s"CASE WHEN $o = '2024-01-15' THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN $o <> '2024-01-15' THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN $o < '2024-01-15' THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN '2024-01-15' = $o THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN $o IN ('2024-01-15', '2024-02-01') THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN $o NOT IN ('2024-01-15') THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE WHEN $o BETWEEN '2024-01-01' AND '2024-01-31' THEN 1 ELSE 0 END"),
      "INT"  -> (o => s"CASE $o WHEN '2024-01-15' THEN 1 ELSE 0 END"),
      "DATE" -> (o => s"NULLIF($o, '2024-01-15')")
    )
    val javaTimeOverText =
      "isEqual\\(|isBefore\\(|isAfter\\(|(LocalDate|ZonedDateTime)\\.parse\\(\\(\"20".r
    for {
      o             <- operands
      (decl, shape) <- shapes
      expr = shape(o)
      (venue, source) <- Seq("CREATE" -> processor(decl, expr), "TRANSFORM" -> transform(expr))
    } withClue(s"$venue $expr -> $source: ")(javaTimeOverText.findFirstIn(source) shouldBe None)
    for ((decl, shape) <- shapes) {
      val expr = shape(nullIfOperand)
      val transformed = transform(expr)
      withClue(s"TRANSFORM $expr -> $transformed: ") {
        javaTimeOverText.findFirstIn(transformed) shouldBe None
      }
      if (expr.startsWith("CASE WHEN")) {
        val source = processor(decl, expr)
        withClue(s"CREATE $expr -> $source: ")(javaTimeOverText.findFirstIn(source) shouldBe None)
      } else
        withClue(s"CREATE $expr: ") {
          Parser(s"CREATE TABLE t ($columns, c $decl SCRIPT AS ($expr))") match {
            case Left(e) =>
              e.msg shouldBe s"Column 'c': $nullIfOperand compares VARCHAR with TIMESTAMP: " +
                "comparing a temporal value with a string is not supported yet, so cast the string " +
                "to the column's type"
            case other => fail(s"accepted: $other")
          }
        }
    }
    // the arithmetic keeps reading the node as a DATE: it parses a text operand itself
    processor("BIGINT", "COALESCE(d, '2024-01-15') - d") should include("lv1 instanceof String")
    // in a query the node is a DATE (D1): nothing moves there
    accepted("COALESCE(d, '2024-01-15')", "DATE")
  }

  private val ddlColumns = "id KEYWORD, n INT, d DATE, d2 DATE, ts TIMESTAMP, k KEYWORD"

  /** `CREATE TABLE t (…, c <decl> SCRIPT AS (<expr>))`: the refusal, or the column's processor. */
  private def created(decl: String, expr: String): Either[String, String] =
    Parser(s"CREATE TABLE t ($ddlColumns, c $decl SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        Right(
          ct.schema.defaultPipeline.processors
            .collectFirst { case p: ScriptProcessor if p.column == "c" => p.source }
            .getOrElse(fail(s"no processor for $expr"))
        )
      case Left(e) => Left(e.msg)
      case other   => fail(s"$expr: $other")
    }

  /** `ALTER TABLE t ADD COLUMN c <decl> SCRIPT AS (<expr>)`, then an unrelated `ALTER TABLE t ADD
    * COLUMN z INT` of the table holding it: each verdict (an ALTER judges every computed column of
    * the table, R-70).
    */
  private def altered(decl: String, expr: String): (Either[String, Unit], Either[String, Unit]) = {
    def merged(base: Schema, alter: String): Schema = Parser(alter) match {
      case Right(at: AlterTable) => base.merge(at.statements)
      case other                 => fail(s"$alter: $other")
    }
    val base = Parser(s"CREATE TABLE t ($ddlColumns)") match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"base: $other")
    }
    val holding = merged(base, s"ALTER TABLE t ADD COLUMN c $decl SCRIPT AS ($expr)")
    val next = merged(holding, "ALTER TABLE t ADD COLUMN z INT")
    (validateScriptReferences(holding), validateScriptReferences(next))
  }

  it should "be judged in a computed column as its processor types it (A-96)" in {
    // before: the validation typed these nodes DATE / TIMESTAMP, while the processor it installs
    // types and renders them as text (A-92) -- `NULLIF(COALESCE(d, '2024-01-15'), k)` was refused
    // ('compares DATE with KEYWORD') on CREATE and on every later ALTER of a table holding it, which
    // main accepts, and `NULLIF(COALESCE(d, '2024-01-15'), d2)` was accepted over a processor
    // whose value is a String (`YEAR` called `.get(ChronoField.YEAR)` on it), which main refuses
    Seq(
      "COALESCE(d, '2024-01-15')",
      "COALESCE('2024-01-15', d)",
      "COALESCE(ts, '2024-01-15 10:30:00')",
      "COALESCE(d, ts, '2024-01-15 10:30:00')",
      "CASE WHEN n > 0 THEN d ELSE '2024-01-15' END",
      "CASE n WHEN 1 THEN d ELSE '2024-01-15' END",
      "COALESCE(COALESCE(d, '2024-01-15'), d)",
      "COALESCE(d, COALESCE(ts, '2024-01-15'))",
      "CASE WHEN n > 0 THEN COALESCE(d, '2024-01-15') ELSE d END",
      "NULLIF(COALESCE(d, '2024-01-15'), k)",
      "NULLIF(COALESCE(d, '2024-01-15'), '2024-01-15')",
      "NULLIF(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END, k)"
    ).foreach { expr =>
      val node =
        resolved(
          s"SELECT $expr AS r FROM t"
        ).select.fieldsWithComputedAliases.head.identifier.functions.head match {
          case p: PainlessScript => p
          case other             => fail(s"$expr: $other")
        }
      val validated = SQLTypeUtils.validatingComputedColumns(node.out)
      withClue(s"$expr: ") {
        validated shouldBe SQLTypeUtils.processorType(node)
        validated.isText shouldBe true
      }
    }
    // accepted, CREATE and ALTER, with main's bytes (0.24.0 compares the two texts)
    val accepted = Seq(
      "DATE" -> "NULLIF(COALESCE(d, '2024-01-15'), k)" ->
      ("def param1 = ctx.k; def param2 = ctx.d; def nif1 = (param2 != null ? param2 : " +
      "\"2024-01-15\"); def param3 = nif1 == null || (param1 != null && nif1.compareTo(param1) == 0)" +
      " ? null : nif1; ctx.c = param3"),
      "KEYWORD" -> "NULLIF(k, COALESCE(d, '2024-01-15'))" ->
      ("def param1 = ctx.k; def param2 = ctx.d; def nif1 = (param2 != null ? param2 : " +
      "\"2024-01-15\"); def param3 = param1 == null || (nif1 != null && param1.compareTo(nif1) == 0)" +
      " ? null : param1; ctx.c = param3"),
      "DATE" -> "NULLIF(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END, k)" ->
      ("def param1 = ctx.k; def param2 = ctx.n; def param3 = ctx.d; def param4 = param2 == null ? " +
      "false : (param2 > 0); def nif1 = param4 ? (param3 instanceof String ? (param3.length() > 10 " +
      "? ZonedDateTime.parse((param3).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME" +
      ".withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS)" +
      " : LocalDate.parse((param3).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(" +
      "\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(param3).atZone(" +
      "ZoneId.of('Z'))) : \"2024-01-15\"; def param5 = nif1 == null || (param1 != null && " +
      "nif1.compareTo(param1) == 0) ? null : nif1; ctx.c = param5")
    )
    accepted.foreach { case ((decl, expr), bytes) =>
      withClue(s"$expr: ") {
        created(decl, expr) shouldBe Right(bytes)
        altered(decl, expr) shouldBe ((Right(()), Right(())))
      }
    }
    // refused, CREATE and ALTER, with main's message
    val refusal =
      "Column 'c': NULLIF(COALESCE(d, '2024-01-15'), d2) compares VARCHAR with TIMESTAMP: " +
      "comparing a temporal value with a string is not supported yet, so cast the string to the " +
      "column's type"
    Seq(
      "DATE" -> "NULLIF(COALESCE(d, '2024-01-15'), d2)",
      "INT"  -> "YEAR(NULLIF(COALESCE(d, '2024-01-15'), d2))",
      "INT" -> ("CASE NULLIF(COALESCE(d, '2024-01-15'), d2) WHEN '2024-01-15' THEN 1 " +
      "WHEN d2 THEN 2 ELSE 0 END"),
      "DATE"   -> "NULLIF(NULLIF(COALESCE(d, '2024-01-15'), d2), '2024-01-15')",
      "BIGINT" -> "DATEDIFF(NULLIF(COALESCE(d, '2024-01-15'), d2), d2)"
    ).foreach { case (decl, expr) =>
      withClue(s"$expr: ") {
        created(decl, expr) shouldBe Left(refusal)
        altered(decl, expr)._1 shouldBe Left(refusal)
      }
    }
    // the arithmetic keeps reading the reported type, DATE, in both (A-96): unchanged
    created("BIGINT", "COALESCE(d, '2024-01-15') - d2").isRight shouldBe true
  }

  it should "convert a BOOLEAN beside whole numbers as Elasticsearch reads its _source" in {
    // main: def param1 = ctx.b; ctx.c = (param1 == null ? 1 : Math.max(param1, 1))
    val source = processor("BIGINT", "GREATEST(b, 1)")
    source should include("instanceof Boolean ? (boolean) param1 : 'true'.equals(param1)")
    source should include("? 1L : 0L")
    source should not include "Math.max(param1, 1)"
    // COALESCE keeps main's processor bytes (D5: converted in a query only)
    processor("BIGINT", "COALESCE(b, 1)") shouldBe
    "def param1 = ctx.b; ctx.c = (param1 != null ? param1 : 1)"
  }

  it should "keep the bytes of a conditional with no quoted argument" in {
    processor("INT", "COALESCE(n, 3)") should not include "\""
  }

  behavior of "the reading memo (A-45 (2), R1)"

  it should "read each scope as a fresh node does, whatever order they render in" in {
    val sql = "SELECT COALESCE(d, '2024-01-15 10:30:00') AS r FROM t"
    val fresh = resolved(sql).select.fieldsWithComputedAliases.head.painless(query)
    val flipped = resolved(sql).select.fieldsWithComputedAliases.head
    flipped.painless(Some(PainlessContext(PainlessContextType.Processor))) should include(
      "\"2024-01-15\""
    )
    flipped.painless(query) shouldBe fresh
    fresh should include("LocalDate.ofEpochDay(19737L)")
  }

  /** The verdict, the reported type and the query Painless of a resolved field's function, asked
    * first under a computed column's validation when `validatedFirst`.
    */
  private def judged(expr: String, validatedFirst: Boolean): (Option[String], String, String) = {
    val field = resolved(s"SELECT $expr AS r FROM t").select.fieldsWithComputedAliases.head
    val f = field.identifier.functions.head
    if (validatedFirst) {
      SQLTypeUtils.validatingComputedColumns(f.typeError)
      SQLTypeUtils.validatingComputedColumns(field.identifier.reportedType)
    }
    (f.typeError, field.identifier.reportedType.typeId, field.painless(query))
  }

  it should "never carry a computed column's validation into a query over an ARRAY arm (R0-5)" in {
    // the validation reads the WHOLE list (`readsWholeValues`), a query one element: both shared
    // the query slot of the memo, and the validation's answer leaked into the query
    Seq("GREATEST(ab, 1)", "COALESCE(an, '3')", "COALESCE(ab, 1)", "GREATEST(an, '1.5')").foreach {
      expr =>
        withClue(s"$expr: ") {
          judged(expr, validatedFirst = true) shouldBe judged(expr, validatedFirst = false)
        }
    }
    judged("GREATEST(ab, 1)", validatedFirst = true)._1 shouldBe None
    judged("COALESCE(an, '3')", validatedFirst = true)._2 shouldBe "INT"
  }

  it should "never carry a comparison's validation into a query, or the reverse (R0-4)" in {
    // the comparison memoised the type its operand is declared with, and the readings of its
    // literals: a conditional over an ARRAY arm is VARCHAR under the validation, INT in a query
    def validation(s: SingleSearch): Option[String] =
      SQLTypeUtils.validatingComputedColumns(s.validateResolved().left.toOption)
    def queried(s: SingleSearch): (Option[String], String) = {
      val ctx = PainlessContext(PainlessContextType.Query)
      val painless = s.select.fieldsWithComputedAliases.head.painless(Some(ctx))
      (s.validateResolved().left.toOption, s"$ctx$painless")
    }
    Seq(
      "CASE WHEN COALESCE(an, '3') = k THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') = '3' THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') IN ('3', '4') THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') BETWEEN '1' AND '5' THEN 1 ELSE 0 END",
      "CASE WHEN '3' = COALESCE(an, '3') THEN 1 ELSE 0 END",
      "NULLIF(COALESCE(an, '3'), '3')"
    ).foreach { expr =>
      val sql = s"SELECT $expr AS r FROM t"
      val (freshValidation, freshQuery) = (validation(resolved(sql)), queried(resolved(sql)))
      withClue(s"$expr, validated first: ") {
        val node = resolved(sql)
        validation(node) shouldBe freshValidation
        queried(node) shouldBe freshQuery
      }
      withClue(s"$expr, queried first: ") {
        val node = resolved(sql)
        queried(node) shouldBe freshQuery
        validation(node) shouldBe freshValidation
      }
    }
  }

  /** What `s` answers in one scope: `v` a computed column's validation (verdict, reported type),
    * `q` a query (verdict, reported type, Painless and its parameters), `p` an ingest processor.
    */
  private def askedIn(s: SingleSearch, scope: Char): String =
    Try {
      val field = s.select.fieldsWithComputedAliases.head
      scope match {
        case 'v' =>
          SQLTypeUtils.validatingComputedColumns(
            s"${s.validateResolved().left.toOption}|${field.identifier.reportedType.typeId}"
          )
        case 'q' =>
          val ctx = PainlessContext(PainlessContextType.Query)
          val painless = field.painless(Some(ctx))
          val reported = field.identifier.reportedType.typeId
          s"${s.validateResolved().left.toOption}|$reported|$ctx$painless"
        case _ =>
          val ctx = PainlessContext(PainlessContextType.Processor)
          val painless = field.painless(Some(ctx))
          s"$ctx$painless"
      }
    }.recover { case e => s"ERR:${e.getClass.getSimpleName}:${e.getMessage}" }.get

  it should "answer each scope as a fresh node does, in all six orders (A-95)" in {
    // one memo slot per scope -- a query, a computed column's validation, a script outside a query:
    // with the validation in the query slot, a validation first made the query render
    // `Long.parseLong("yes")` (CASE COALESCE(ab, 'true') WHEN 'yes') or refuse an arithmetic a
    // fresh query answers, and a query first refused `CASE COALESCE(an, '3') WHEN k` in the
    // validation (45 of these 180 orders leaked); computed live there instead, nested conditionals
    // made a CREATE TABLE exponential
    val shapes = Seq(
      "CASE WHEN COALESCE(an, '3') = k THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') = '3' THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') = n THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') IN ('3', '4') THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') BETWEEN k AND k THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') BETWEEN '1' AND '5' THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') > '2' THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(an, '3') <> k THEN 1 ELSE 0 END",
      "CASE WHEN k = COALESCE(an, '3') THEN 1 ELSE 0 END",
      "CASE WHEN '3' = COALESCE(an, '3') THEN 1 ELSE 0 END",
      "CASE COALESCE(an, '3') WHEN '3' THEN 1 ELSE 0 END",
      "CASE COALESCE(an, '3') WHEN k THEN 1 ELSE 0 END",
      "CASE COALESCE(an, '3') WHEN 3 THEN 1 ELSE 0 END",
      "CASE CASE WHEN n > 0 THEN an ELSE '3' END WHEN '3' THEN 1 ELSE 0 END",
      "CASE GREATEST(an, '1.5') WHEN '2' THEN 1 ELSE 0 END",
      "COALESCE(an, '3') + '1'",
      "'1' + COALESCE(an, '3')",
      "COALESCE(an, '3') * '2'",
      "CASE WHEN n > 0 THEN an ELSE '3' END + '1'",
      "COALESCE(an, '3') + 1",
      "NULLIF(COALESCE(an, '3'), '3')",
      "NULLIF(COALESCE(an, '3'), k)",
      "NULLIF(k, COALESCE(an, '3'))",
      "GREATEST(COALESCE(an, '3'), '2')",
      "COALESCE(COALESCE(an, '3'), '4')",
      "CASE WHEN n > 0 THEN COALESCE(an, '3') ELSE '4' END",
      "UPPER(COALESCE(an, '3'))",
      "CASE WHEN ab = k THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE(ab, 'true') = k THEN 1 ELSE 0 END",
      "CASE COALESCE(ab, 'true') WHEN 'yes' THEN 1 ELSE 0 END",
      // a read date: text outside a query only (R1-1)
      "CASE WHEN COALESCE(d, '2024-01-15') = '2024-01-15' THEN 1 ELSE 0 END",
      "CASE WHEN CASE WHEN n > 0 THEN ts ELSE '2024-01-15' END IN ('2024-01-15') THEN 1 ELSE 0 END",
      "CASE COALESCE(d, '2024-01-15 10:30:00') WHEN '2024-01-15' THEN 1 ELSE 0 END",
      "NULLIF(COALESCE(d, '2024-01-15'), '2024-01-15')",
      "COALESCE(COALESCE(d, '2024-01-15'), d) + 1"
    )
    shapes.foreach { expr =>
      val sql = s"SELECT $expr AS r FROM t"
      val fresh = "vqp".map(scope => scope -> askedIn(resolved(sql), scope)).toMap
      "vqp".permutations.foreach { order =>
        val node = resolved(sql)
        order.foreach { scope =>
          withClue(s"$expr, order $order, scope $scope: ") {
            askedIn(node, scope) shouldBe fresh(scope)
          }
        }
      }
    }
  }

  behavior of "a string function over a read conditional (R-109)"

  it should "read a non-text receiver as its text, and a conditional receiver as one value" in {
    // main: param2 ? param1 : "3".toUpperCase() -- the call on the last arm alone
    val upper = accepted("UPPER(CASE WHEN n > 0 THEN n ELSE '3' END)", "VARCHAR")
    upper should include("String.valueOf(lv1).toUpperCase()")
    upper should not include "3.toUpperCase()"
    // main failed on every row: dynamic method [java.lang.Long, toUpperCase/0] not found
    accepted("UPPER(n)", "VARCHAR") should include("String.valueOf(param1).toUpperCase()")
    accepted("LENGTH(n)", "BIGINT") should include("String.valueOf(param1).length()")
    // typed `def` beside its NULL guard: `(param1 == null) ? null : <int>` failed to compile on
    // Elasticsearch 8.18.3 ("Cannot cast from [int] to [java.lang.Object]")
    accepted("LENGTH(n)", "BIGINT") should include("((def)(String.valueOf(param1).length()))")
    accepted("UPPER(COALESCE(n, '3'))", "VARCHAR") should include(
      "String.valueOf((param1 != null ? param1 : 3)).toUpperCase()"
    )
    accepted("SUBSTRING(n, 1, 1)", "VARCHAR") should include("((String)String.valueOf(param1))")
    // a text conditional: one value -- main lowered the ELSE arm only
    accepted("LOWER(CASE WHEN n > 0 THEN 'A' ELSE 'B' END)", "VARCHAR") should include(
      "(param2 ? \"A\" : \"B\").toLowerCase()"
    )
    // a conditional that can be NULL is bound and guarded: never the text "null"
    accepted("UPPER(CASE WHEN n > 0 THEN k END)", "VARCHAR") should include(
      "(lv1 == null ? null : (def)(lv1.toUpperCase()))"
    )
  }

  it should "keep the bytes of a text receiver" in {
    accepted("UPPER(k)", "VARCHAR") shouldBe "(param1 == null) ? null : param1.toUpperCase()"
    accepted("UPPER(TRIM(k))", "VARCHAR") shouldBe
    "(param1 == null) ? null : param1.trim().toUpperCase()"
    accepted("LOWER(COALESCE(k, 'x'))", "VARCHAR") shouldBe
    "(param1 != null ? param1 : \"x\").toLowerCase()"
  }

  behavior of "a BOOLEAN cast to a temporal (R-110)"

  it should "be refused in every scope, as DuckDB refuses it" in {
    // main answered the Boolean in a DATE column, or failed on every row once its text was parsed
    Seq(
      "CAST(b AS DATE)"                   -> "DATE",
      "CAST(b AS TIMESTAMP)"              -> "TIMESTAMP",
      "CAST(b AS TIME)"                   -> "TIME",
      "TRY_CAST(b AS DATE)"               -> "DATE",
      "CAST(COALESCE(b, 'true') AS DATE)" -> "DATE"
    ).foreach { case (expr, target) =>
      select(expr)._1.getOrElse("") should include(s"cannot cast BOOLEAN to $target")
    }
    Parser(s"CREATE TABLE t ($columns, c DATE SCRIPT AS (CAST(b AS DATE)))") match {
      case Left(e) => e.msg should include("cannot cast BOOLEAN to DATE")
      case other   => fail(s"accepted: $other")
    }
    select("CAST(b AS BIGINT)")._1 shouldBe None
  }

  behavior of "a read conditional nested beside text in a query (A-97)"

  /** The statements a nested reading refused in a query (`Cannot mix values of type INT and
    * KEYWORD`), which 0.24.0 answered as text: each answers 0.24.0's VARCHAR again.
    */
  private val nestedBesideText = Seq(
    "COALESCE(COALESCE(n, '3'), k)",
    "COALESCE(k, COALESCE(n, '3'))",
    "COALESCE(COALESCE(n, '3'), g, k)",
    "CASE WHEN n > 0 THEN COALESCE(n, '3') ELSE k END",
    "CASE WHEN n > 0 THEN k ELSE COALESCE(n, '3') END",
    "COALESCE(CASE WHEN n > 0 THEN n ELSE '3' END, k)",
    "COALESCE(COALESCE(x, '1.5'), k)",
    "COALESCE(COALESCE(b, 'true'), k)",
    "COALESCE(COALESCE(d, '2024-01-15'), k)",
    "CASE WHEN n > 0 THEN COALESCE(d, '2024-01-15') ELSE k END",
    "COALESCE(COALESCE(n, '3'), CAST(n AS VARCHAR))",
    "COALESCE(COALESCE(n, '1.5'), k)",
    // read x read, a NULLIF's value, GREATEST over dates and numbers, a deeper nesting
    "COALESCE(COALESCE(n, '3'), COALESCE(d, '2024-01-15'))",
    "COALESCE(COALESCE(d, '2024-01-15'), COALESCE(n, '3'))",
    "COALESCE(NULLIF(COALESCE(n, '3'), k), g)",
    "COALESCE(GREATEST(d, '2024-01-15'), k)",
    "COALESCE(GREATEST(n, '3'), k)",
    "COALESCE(COALESCE(COALESCE(n, '3'), g), k)"
  )

  it should "keep 0.24.0's verdict and VARCHAR type, answering each read arm's value as text" in {
    nestedBesideText.foreach { expr =>
      val painless = accepted(expr, "VARCHAR")
      withClue(s"$expr: ") {
        // each read arm is converted to the text of its value: no Integer under a VARCHAR label
        painless should include("String.valueOf")
      }
    }
    // the inner node keeps its one reading: '1.5' is 2 beside the INT n, answered as '2'
    val n15 = accepted("COALESCE(COALESCE(n, '1.5'), k)", "VARCHAR")
    n15 should not include "1.5"
    n15 should include(": 2)")
    accepted("COALESCE(n, '1.5')", "INT")
  }

  it should "leave every other node as it was: no mix, a mix at 0.24.0's types, no read arm" in {
    // no mix at the read types: the read type stays (a BOOLEAN beside a number is an INT)
    accepted("COALESCE(COALESCE(n, '3'), x)", "DOUBLE")
    accepted("CASE WHEN n > 0 THEN COALESCE(n, '3') ELSE COALESCE(b, 'true') END", "INT")
    accepted("COALESCE(COALESCE(n, '3'), 'abc')", "VARCHAR")
    // still mixing with the read arm at VARCHAR: r4's refusal and message
    select("COALESCE(COALESCE(n, '3'), d)")._1 shouldBe Some(
      "Cannot mix values of type INT and DATE in COALESCE operator - an explicit cast is required"
    )
    select("COALESCE(COALESCE(n, '3'), x, k)")._1 shouldBe Some(
      "Cannot mix values of type INT and KEYWORD in COALESCE operator - an explicit cast is required"
    )
    // no read-moved arm: main's refusal
    select("COALESCE(COALESCE(n, 3), k)")._1 shouldBe Some(
      "Cannot mix values of type BIGINT and KEYWORD in COALESCE operator - an explicit cast is " +
      "required"
    )
    select("COALESCE(n, k)")._1 shouldBe Some(
      "Cannot mix values of type INT and KEYWORD in COALESCE operator - an explicit cast is required"
    )
    // a GREATEST over text stays refused, as on main
    select("GREATEST(COALESCE(n, '3'), k)")._1.getOrElse("") should include(
      "requires numeric arguments"
    )
  }

  it should "reach every consumer of the outer node as its VARCHAR" in {
    accepted("UPPER(COALESCE(COALESCE(n, '3'), k))", "VARCHAR")
    select("CASE WHEN COALESCE(COALESCE(n, '3'), k) = '1' THEN 1 ELSE 0 END")._1 shouldBe None
    resolved("SELECT g FROM t ORDER BY COALESCE(COALESCE(n, '3'), k)")
      .validateResolved() shouldBe Right(())
    resolved(
      "SELECT COALESCE(COALESCE(n, '3'), k) AS r FROM t GROUP BY COALESCE(COALESCE(n, '3'), k)"
    )
      .validateResolved() shouldBe Right(())
  }

  it should "keep the read types outside a query: a processor, a transform, a computed column" in {
    // `id`, a KEYWORD of the computed column's table
    val expr = "COALESCE(COALESCE(n, '3'), id)"
    val source = processor("KEYWORD", expr)
    source should not include "String.valueOf"
    source should not include "\"3\""
    transform("COALESCE(COALESCE(n, '3'), k)") should not include "String.valueOf"
    created("KEYWORD", expr) shouldBe a[Right[_, _]]
  }

  behavior of "COALESCE over a CASE arm (R-113)"

  it should "test the whole CASE for NULL, in a query and in a processor" in {
    // main tested the ELSE alone (`c ? a : b != null ? … : k`): NULL where c held and a was NULL
    val q = accepted("COALESCE(CASE WHEN n IS NULL THEN l ELSE n END, 7)", "BIGINT")
    q should startWith("((")
    q should include(") != null ?")
    val p = processor("BIGINT", "COALESCE(CASE WHEN n IS NULL THEN l ELSE n END, 7)")
    p should include(") != null ?")
    // a NULL ELSE: `null != null` is never tested alone
    accepted("COALESCE(CASE WHEN k = '3' THEN n ELSE NULL END, 7)", "BIGINT") should not include
    "null != null"
    // a CASE as a middle arm, and a read CASE beside a typed arm (r4 tested `3 != null`)
    accepted("COALESCE(ti, CASE WHEN n IS NULL THEN l ELSE n END, 7)", "BIGINT") should include(
      ") != null ?"
    )
    val p01 = accepted("COALESCE(CASE WHEN n > 0 THEN n ELSE '3' END, n)", "INT")
    p01 should include(": 3) != null ?")
    p01 should not include ": 3 != null"
    // the testkit's RUN rows (text arms)
    Seq(
      "COALESCE(CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE g END, g)",
      "COALESCE(CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE NULL END, g)",
      "COALESCE(CAST(ti AS VARCHAR), CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE g END, g)"
    ).foreach(e => accepted(e, "VARCHAR") should include(") != null ?"))
  }

  it should "keep the bytes of an arm that is no CASE" in {
    accepted("COALESCE(n, l)", "BIGINT") should startWith("(param")
    accepted("COALESCE(COALESCE(n, l), 7)", "BIGINT") should startWith("((param")
  }

  // ---- run 6 (A-98, A-99): pins P1 - P4 ----

  private val cdr = "COALESCE('2024-01-15', d)"
  private val caseD = "CASE WHEN n > 0 THEN d ELSE '2024-01-15' END"
  private val caseTs = "CASE WHEN n > 0 THEN ts ELSE '2024-01-15 10:30:00' END"
  private val simpleD = "CASE n WHEN 3 THEN d ELSE '2024-01-15' END"

  /** The verdict, the reported type and the query Painless -- the WHERE criteria if there is one,
    * else the first field -- with the declarations it registered.
    */
  private def rendered(sql: String): (Option[String], String, String) = {
    val s = resolved(sql)
    val c = PainlessContext(PainlessContextType.Query)
    val p = s.where.flatMap(_.criteria) match {
      case Some(criteria) => criteria.painless(Some(c))
      case None           => s.select.fieldsWithComputedAliases.head.painless(Some(c))
    }
    (
      s.validateResolved().left.toOption,
      s.select.fieldsWithComputedAliases.head.identifier.reportedType.typeId,
      s"$c$p"
    )
  }

  private def refusal(expr: String): String =
    select(expr)._1.getOrElse(fail(s"$expr: accepted"))

  behavior of "a literal-first COALESCE beside text in an equality (A-98 α, R-119)"

  it should "compare its literal's text, with main's verdict and type (P1)" in {
    Seq(
      s"NULLIF($cdr, k)"                                                        -> "VARCHAR",
      s"NULLIF(k, $cdr)"                                                        -> "KEYWORD",
      s"CASE WHEN NULLIF($cdr, k) = '2024-01-15' THEN 1 ELSE 0 END"             -> "BIGINT",
      s"COALESCE(NULLIF($cdr, k), k)"                                           -> "VARCHAR",
      s"CASE $cdr WHEN k THEN 1 ELSE 0 END"                                     -> "BIGINT",
      s"CASE WHEN $cdr = k THEN 1 ELSE 0 END"                                   -> "BIGINT",
      s"CASE k WHEN $cdr THEN 1 ELSE 0 END"                                     -> "BIGINT",
      s"CASE WHEN k = $cdr THEN 1 ELSE 0 END"                                   -> "BIGINT",
      s"CASE WHEN $cdr <> k THEN 1 ELSE 0 END"                                  -> "BIGINT",
      s"CASE WHEN $cdr != k THEN 1 ELSE 0 END"                                  -> "BIGINT",
      s"CASE WHEN NOT $cdr = k THEN 1 ELSE 0 END"                               -> "BIGINT",
      s"CASE WHEN $cdr = UPPER(k) THEN 1 ELSE 0 END"                            -> "BIGINT",
      "CASE WHEN COALESCE(NULL, '2024-01-15', d) = k THEN 1 ELSE 0 END"         -> "BIGINT",
      "NULLIF(COALESCE(NULL, '2024-01-15', d), k)"                              -> "VARCHAR",
      "CASE WHEN COALESCE('2024-01-15', '2024-01-16', d) = k THEN 1 ELSE 0 END" -> "BIGINT"
    ).foreach { case (e, t) =>
      val (verdict, reported, painless) = rendered(s"SELECT $e AS r FROM t")
      withClue(s"$e: ") {
        verdict shouldBe None
        reported shouldBe t
        painless should include("\"2024-01-15\"")
        painless should not include "LocalDate.ofEpochDay"
      }
    }
    val (verdict, _, where) = rendered(s"SELECT k FROM t WHERE $cdr = k")
    verdict shouldBe None
    where should include("(\"2024-01-15\" == param")
    // the NULLIF beside its own operand: alpha answers it, A-97 converts the read arm to its text
    select(s"COALESCE(NULLIF(k, $cdr), $cdr)")._1 shouldBe None
  }

  it should "keep the reading's verdict everywhere else (P1 controls)" in {
    // ordering keeps r5's refusal and message (main fails at runtime, RUN M6, R-119)
    Seq(s"$cdr < k", s"$cdr >= k").foreach { c =>
      refusal(s"CASE WHEN $c THEN 1 ELSE 0 END") should startWith(
        "Type mismatch: 'DATE' is not compatible with 'KEYWORD'"
      )
    }
    select(cdr)._2 shouldBe "DATE"
    refusal(s"CASE WHEN $cdr = n THEN 1 ELSE 0 END") should startWith(
      "Type mismatch: 'DATE' is not compatible with 'INT'"
    )
    refusal("CASE WHEN COALESCE('3', n) > k THEN 1 ELSE 0 END") should startWith(
      "Type mismatch: 'INT' is not compatible with 'KEYWORD'"
    )
    refusal("CASE WHEN COALESCE(d, '2024-01-15') = k THEN 1 ELSE 0 END") should startWith(
      "Type mismatch: 'DATE' is not compatible with 'KEYWORD'"
    )
    // one rendered operand: a CASE whose values do not all read the literal keeps the reading
    refusal(s"CASE $cdr WHEN k THEN 1 WHEN d THEN 2 ELSE 0 END") shouldBe
    "CASE WHEN conditions must be of the same type as the expression"
    Seq(
      s"CASE WHEN $cdr = '2024-01-15 00:00:00' THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE('3', n) = k THEN 1 ELSE 0 END",
      "CASE WHEN COALESCE('true', b) = k THEN 1 ELSE 0 END"
    ).foreach(e => rendered(s"SELECT $e AS r FROM t")._1 shouldBe None)
  }

  it should "refuse the classes main answers wrongly (P3)" in {
    Seq(
      "COALESCE('2024-01-15 10:30:00', d)",
      "COALESCE('2024-01-15', ts)",
      "COALESCE('2024-01-15 10:30:00', ts)"
    ).foreach { f =>
      refusal(s"CASE WHEN $f = k THEN 1 ELSE 0 END") should startWith("Type mismatch: ")
      refusal(s"NULLIF($f, k)") should include(
        "comparing a temporal value with a string is not supported yet"
      )
    }
    Seq(caseD, caseTs, simpleD).foreach { f =>
      refusal(s"NULLIF($f, k)") should include(" compares ")
      refusal(s"NULLIF(k, $f)") should include(" compares ")
      refusal(s"CASE WHEN NULLIF($f, k) = '2024-01-15' THEN 1 ELSE 0 END") should include(
        " compares "
      )
      refusal(s"CASE $f WHEN k THEN 1 ELSE 0 END") shouldBe
      "CASE WHEN conditions must be of the same type as the expression"
      refusal(s"CASE WHEN $f = k THEN 1 ELSE 0 END") should startWith("Type mismatch: ")
    }
  }

  behavior of "COALESCE(NULLIF(a, b), b) over a read-moved conditional (A-98 β)"

  /** A rendering with its parameter names erased. */
  private def shape(painless: String): String =
    painless.replaceAll("\\b(param|nif|lv|left|right)\\d+\\b", "P")

  it should "answer COALESCE(a, b), VARCHAR, rendered as COALESCE(a, b) is (P2)" in {
    Seq(
      s"COALESCE(NULLIF($caseD, k), k)"   -> s"COALESCE($caseD, k)",
      s"COALESCE(NULLIF($caseTs, k), k)"  -> s"COALESCE($caseTs, k)",
      s"COALESCE(NULLIF($simpleD, k), k)" -> s"COALESCE($simpleD, k)",
      "COALESCE(NULLIF(COALESCE(d, '2024-01-15'), k), k)" -> "COALESCE(COALESCE(d, '2024-01-15'), k)",
      s"COALESCE(NULLIF($caseD, k), k, g)"   -> s"COALESCE($caseD, k, g)",
      s"COALESCE(g, NULLIF($caseD, k), k)"   -> s"COALESCE(g, $caseD, k)",
      s"COALESCE(NULLIF(k, $caseD), $caseD)" -> s"COALESCE(k, $caseD)"
    ).foreach { case (beta, reference) =>
      val (verdict, reported, painless) = rendered(s"SELECT $beta AS r FROM t")
      withClue(s"$beta: ") {
        verdict shouldBe None
        reported shouldBe "VARCHAR"
        shape(painless) shouldBe shape(rendered(s"SELECT $reference AS r FROM t")._3)
      }
    }
    accepted(s"UPPER(COALESCE(NULLIF($caseD, k), k))", "VARCHAR")
  }

  it should "keep r5's verdict and message for every other NULLIF (P2 controls)" in {
    refusal(s"COALESCE(NULLIF($caseD, k), g)") should include(" compares DATE with KEYWORD")
    refusal("COALESCE(NULLIF(d, k), k)") shouldBe
    "Cannot mix values of type DATE and KEYWORD in COALESCE operator - an explicit cast is required"
    refusal("COALESCE(NULLIF(d, 'yesterday'), 'yesterday')") should startWith(
      "Cannot parse 'yesterday'"
    )
    accepted("COALESCE(NULLIF(n, 0), 0)", "BIGINT")
    // one pass: the inner NULLIF of a replaced arm is judged, and refused
    refusal("COALESCE(NULLIF(NULLIF(COALESCE(d, '2024-01-15'), k), k), k)") should include(
      " compares DATE with KEYWORD"
    )
  }

  behavior of "COALESCE over a GREATEST / LEAST holding a literal (A-99, R-116)"

  /** How many groups a primitive cast opens directly tested for NULL (`((long) …) != null`). */
  private def primitivesTested(painless: String): Int =
    "\\) != null".r.findAllMatchIn(painless).count { m =>
      var depth = 0
      var i = m.start
      var open = -1
      while (i >= 0 && open < 0) {
        painless.charAt(i) match {
          case ')' => depth += 1
          case '(' =>
            depth -= 1
            if (depth == 0) open = i
          case _ =>
        }
        i -= 1
      }
      open >= 0 && painless
        .substring(open)
        .matches("(?s)^\\(\\(\\s*(int|long|double|float|byte|short)\\).*")
    }

  private val keywordColumns = s"$columns, k KEYWORD, g KEYWORD"

  private def processorWithK(decl: String, expr: String): String =
    Parser(s"CREATE TABLE t ($keywordColumns, c $decl SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        ct.schema.defaultPipeline.processors
          .collectFirst { case p: ScriptProcessor if p.column == "c" => p.source }
          .getOrElse(fail(s"no processor for $expr"))
      case other => fail(s"$expr: $other")
    }

  it should "end at that arm in every scope: no primitive is tested for NULL (P4)" in {
    val rows = Seq(
      "COALESCE(GREATEST(n, 3), l)"    -> "BIGINT",
      "COALESCE(LEAST(x, 1.5), x)"     -> "DOUBLE",
      "COALESCE(GREATEST(n, 3), n, 7)" -> "BIGINT",
      "COALESCE(GREATEST(n, '2'), k)"  -> "VARCHAR",
      "COALESCE(GREATEST(n, '3'), n)"  -> "INT",
      "COALESCE(LEAST(x, '1.5'), x)"   -> "DOUBLE",
      "COALESCE(GREATEST(n, '3'), k)"  -> "VARCHAR"
    ) ++ (for {
      f <- Seq("GREATEST", "LEAST")
      t <- Seq("ti", "n", "x")
      x <- Seq("'3'", "'1.5'", "'200'")
    } yield s"COALESCE($f($t, $x), $t)" -> "DOUBLE")
    rows.foreach { case (e, decl) =>
      withClue(s"$e: ") {
        val (verdict, _, q) = rendered(s"SELECT $e AS r FROM t")
        verdict shouldBe None
        primitivesTested(q) shouldBe 0
        primitivesTested(processorWithK(decl, e)) shouldBe 0
        primitivesTested(updateProcessor(e)) shouldBe 0
        primitivesTested(transform(e)) shouldBe 0
      }
    }
    // the cut follows `NumericReducer.neverNull`: a processor ends at the reducer, a reducer with
    // no literal (or a NULL one) is still tested (#292)
    processorWithK("BIGINT", "COALESCE(GREATEST(n, 3), l)") should not include "!= null"
    rendered("SELECT COALESCE(GREATEST(n, l), x) AS r FROM t")._3 should include("!= null ?")
    rendered("SELECT COALESCE(GREATEST(n, NULL), l) AS r FROM t")._3 should include("!= null ?")
  }
}
