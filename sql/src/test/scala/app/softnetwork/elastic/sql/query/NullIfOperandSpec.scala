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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** Issue #382, the NULLIF addendum: `NULLIF` emitted two DIFFERENT scripts Elasticsearch refuses,
  * and a third that it accepts and answers WRONGLY. All three are pre-existing on `main`; all three
  * are one question asked of two operands — what does this argument RENDER, and may it be spliced
  * where it is spliced?
  *
  * '''N1 — the TYPE.''' `NullIf.argTypes` answered `_.out`, and a schema-resolved `Identifier.out`
  * reports the COLUMN's type, so `NULLIF(CAST(s AS BIGINT), 0)` over a KEYWORD column reported
  * `List(KEYWORD, BIGINT)`, `leastCommonSuperType` came out VARCHAR and the string arm's guard
  * reached the cluster:
  * {{{
  * def param3 = param2 == null || (0 != null && param2.compareTo(0) == 0) ? null : param2;
  *   class_cast_exception: Cannot cast from [int] to [java.lang.Object].   (ES 8.18.3)
  * }}}
  * Identical for `CAST`, `TRY_CAST` and `::`, and in the DDL, projection and predicate venues.
  *
  * '''N2 — the FORM.''' `arg0` is spliced RAW three times into `$arg0 == null || ($arg1 != null &&
  * $comparison) ? null : $arg0` and `arg1` twice, and `&&`, `||` and `==` all bind tighter than
  * `?:`. A compound rendering therefore re-associates across the template. MEASURED on ES 8.18.3:
  * {{{
  * NULLIF(UPPER(s), 'X')                          Cannot cast null to a primitive type [boolean].
  * NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)   n = 5 stored c = 1 where SQL says NULL, HTTP 200
  * }}}
  * 🔴 The second is why the repair cannot be narrowed to the arm that fails LOUDLY. The numeric arm
  * `$arg0 == $arg1 ? null : $arg0` re-associates just as badly; it merely happens to come out right
  * when the compound rendering is a `guard ? null : value`, which is luck, not a rule — and the
  * price of trusting that luck is a silent wrong answer in the #205 family. Both templates now read
  * their operands from ONE place, so they cannot disagree.
  *
  * ⚠️ Two shapes that already WORKED change bytes as a consequence, and both were executed on a
  * cluster before and after (`GatewayApiIntegrationSpec`):
  *   - `NULLIF(LENGTH(s), 0)` gains the bound local (it was correct, and evaluated twice);
  *   - `NULLIF(CAST(s AS BIGINT), n)` moves from the string arm to the numeric one, which is the
  *     arm its corrected types name and the one `NULLIF(n, 0)` has always used.
  *
  * `ScriptProcessor.source` is persisted in `_meta` and `IngestPipeline.diff` compares it, so a
  * stored computed column of either shape reports `ProcessorChanged` on the next ALTER.
  */
class NullIfOperandSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("s", SQLTypes.Keyword),
      Column("s2", SQLTypes.Keyword),
      Column("status", SQLTypes.Keyword),
      Column("n", SQLTypes.BigInt),
      Column("i", SQLTypes.Int),
      Column("d", SQLTypes.Date)
    )
  )

  private val ddlColumns = "s KEYWORD, s2 KEYWORD, status KEYWORD, n BIGINT, i INTEGER, d DATE"

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema))
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The ingest-processor source of a computed column, through the production seam. */
  private def processorOf(expr: String, dataType: String = "KEYWORD"): String =
    Parser(s"CREATE TABLE t ($ddlColumns, c $dataType SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == "c")
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$expr]"))
      case other => fail(s"[$expr] expected a CreateTable, got $other")
    }

  /** The first SELECT field rendered as one script. The body is rendered FIRST: it is what
    * registers the parameters, so an inline `s"$ctx${…painless(ctx)}"` prints an EMPTY prologue.
    */
  private def fieldOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT $expr AS c FROM t").select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  /** The WHERE criteria rendered as ONE script, prologue included. */
  private def predicateOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT s FROM t WHERE ISNOTNULL($expr)").where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$expr] has no WHERE"))
    s"$ctx$body"
  }

  private def everyVenue(expr: String, dataType: String = "KEYWORD"): Seq[(String, String)] =
    Seq(
      "computed column" -> processorOf(expr, dataType),
      "projection"      -> fieldOf(expr),
      "predicate"       -> predicateOf(expr)
    )

  /** How many times `needle` occurs in `haystack` -- the shape of an "evaluated once" assertion. */
  private def countOf(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

  // -- N1: the type an operand RENDERS decides the arm ---------------------------------------

  /** Every spelling of a cast that RENDERS a number, against a numeric literal. The guard the
    * string arm puts on that literal — `0 != null` — is what Elasticsearch refuses, so the
    * assertion is that the numeric arm is taken in EVERY venue, not just in DDL where it was filed.
    */
  it should "take the numeric arm when the operand RENDERS a number" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(TRY_CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(s::BIGINT, 0)", "BIGINT"),
        ("NULLIF(CAST(s AS DOUBLE), 0)", "DOUBLE"),
        ("NULLIF(CAST(s AS INTEGER), -1)", "INTEGER"),
        ("NULLIF(CAST(s AS BIGINT), n)", "BIGINT")
      )
    ) { (expr, dataType) =>
      everyVenue(expr, dataType).foreach { case (venue, emitted) =>
        withClue(s"[$venue] [$expr] -> [$emitted] ") {
          emitted should not include "compareTo"
          emitted should not include "!= null &&"
        }
      }
    }
  }

  /** The literal guard Painless refuses, named exactly as ES reported it. */
  it should "never guard a numeric literal against null" in {
    forAll(
      Table(
        ("expr", "dataType", "literal"),
        ("NULLIF(CAST(s AS BIGINT), 0)", "BIGINT", "0 != null"),
        ("NULLIF(CAST(s AS INTEGER), -1)", "INTEGER", "-1 != null"),
        ("NULLIF(CAST(s AS DOUBLE), 1.5)", "DOUBLE", "1.5 != null")
      )
    ) { (expr, dataType, literal) =>
      everyVenue(expr, dataType).foreach { case (venue, emitted) =>
        withClue(s"[$venue] [$expr] -> [$emitted] ")(emitted should not include literal)
      }
    }
  }

  /** 🔴 RECORDED, NOT FIXED — and deliberately so. `NULLIF(s, 0)` is not a rendering problem: the
    * operand really IS a KEYWORD and the supertype really IS a string, so no type derivation can
    * rescue it. It is a TYPE MISMATCH, `NullIf.validate()` already computes exactly the right
    * `Left` for it — and never fires, because validation runs at PARSE time, before a schema is
    * attached, when `s` has no type at all. Widening validation to a resolved statement is a
    * separate change, recorded on #382.
    *
    * Emitting the comparison WITHOUT the guard was measured and rejected: on ES 8.18.3 it turns the
    * compile error into a per-document runtime failure (`wrong_method_type_exception: cannot
    * convert MethodHandle(String,String)int to (Object,int)Object`), which is strictly worse.
    *
    * This assertion is a CHARACTERISATION: it reddens when the gap closes, which is when it should
    * be rewritten the way this round rewrote the one it inherited.
    */
  it should "still emit the string arm for a genuinely KEYWORD operand (recorded, #382)" in {
    val emitted = processorOf("NULLIF(s, 0)", "BIGINT")
    withClue(emitted)(emitted should include("0 != null"))
  }

  // -- N2: a compound operand is bound, once -------------------------------------------------

  /** The LOUD half. `UPPER(s)` renders `(param1 == null) ? null : param1.toUpperCase()`, which
    * re-associates in all three of the template's slots.
    */
  it should "bind a nullable function operand and evaluate it ONCE" in {
    everyVenue("NULLIF(UPPER(s), 'X')").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "toUpperCase()") shouldBe 1
        emitted should include("def nif1 = (param1 == null) ? null : param1.toUpperCase(); ")
        emitted should include(
          "nif1 == null || (\"X\" != null && nif1.compareTo(\"X\") == 0) ? null : nif1"
        )
      }
    }
  }

  /** The SILENT half, and the reason the repair reaches the numeric arm too. `NULLIF(CASE WHEN n >
    * 1 THEN 1 ELSE 2 END, 1)` emitted `param2 ? 1 : 2 == 1 ? null : param2 ? 1 : 2`, which Painless
    * reads as `param2 ? 1 : ((2 == 1) ? null : …)`: MEASURED on ES 8.18.3, `n = 5` stored `c = 1`
    * where SQL says NULL, HTTP 200.
    */
  it should "bind a compound operand on the NUMERIC arm too" in {
    val expr = "NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)"
    everyVenue(expr, "BIGINT").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "? 1 : 2") shouldBe 1
        emitted should include("def nif1 = param2 ? 1 : 2; ")
        emitted should include("nif1 == 1 ? null : nif1")
      }
    }
  }

  /** `arg1` is spliced twice and mis-associates the same way — `NULLIF(s, UPPER(s2))` emitted
    * `((param1 == null) ? null : param1.toUpperCase() != null && …)`, a null in a boolean slot.
    */
  it should "bind a compound SECOND argument" in {
    everyVenue("NULLIF(s, UPPER(s2))").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "toUpperCase()") shouldBe 1
        emitted should include("nif1 != null && ")
      }
    }
  }

  /** A NAME and a LITERAL are already safe operands, so neither is bound: the predicate is narrow
    * on purpose, and this is what keeps the working population byte-identical below.
    */
  it should "bind neither a name nor a literal" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(n, 0)", "BIGINT"),
        ("NULLIF(status, 'unknown')", "KEYWORD"),
        ("NULLIF(s, s2)", "KEYWORD"),
        ("NULLIF('a', 'b')", "KEYWORD")
      )
    ) { (expr, dataType) =>
      val emitted = processorOf(expr, dataType)
      withClue(s"[$expr] -> [$emitted] ")(emitted should not include "nif")
    }
  }

  // -- the shape of the whole script ----------------------------------------------------------

  /** Every `def` opens a STATEMENT, so it may only start the script or follow one; a hoisted local
    * that landed inside the expression would be a compile error, which is the failure mode the
    * `ArithmeticExpression` binding had twice. And a name declared twice is one Painless rejects.
    */
  it should "keep every declaration in the prologue, declared once" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(UPPER(s), 'X')", "KEYWORD"),
        ("NULLIF(TRIM(s), '')", "KEYWORD"),
        ("NULLIF(LENGTH(s), 0)", "BIGINT"),
        ("NULLIF(s, UPPER(s2))", "KEYWORD"),
        ("NULLIF(UPPER(s), LOWER(s2))", "KEYWORD"),
        ("NULLIF(TRY_CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)", "BIGINT")
      )
    ) { (expr, dataType) =>
      everyVenue(expr, dataType).foreach { case (venue, emitted) =>
        val offences = "def ".r
          .findAllMatchIn(emitted)
          .map(_.start)
          .filterNot(i =>
            i == 0 || emitted.startsWith("; ", i - 2) || emitted.startsWith("} ", i - 2)
          )
          .map(i => emitted.substring(math.max(0, i - 24), math.min(emitted.length, i + 12)))
          .toSeq
        val names = "def ([A-Za-z_][A-Za-z0-9_]*) =".r.findAllMatchIn(emitted).map(_.group(1)).toSeq
        withClue(s"[$venue] [$expr] -> [$emitted] ") {
          offences shouldBe empty
          names.groupBy(identity).collect { case (n, xs) if xs.size > 1 => n } shouldBe empty
        }
      }
    }
  }

  // -- byte stability of the population that already worked -----------------------------------

  /** The shapes the addendum listed as working, pinned BYTE-EXACT so the repair cannot move them
    * quietly. Measured against a `git clone --local` control at this branch's previous HEAD over a
    * 618-expression / 2,460-cell corpus; in the three venues production uses, the ONLY working
    * shapes that move are the two named in the class scaladoc.
    */
  it should "leave the working population byte-identical" in {
    forAll(
      Table(
        ("expr", "dataType", "expected"),
        (
          "NULLIF(n, 0)",
          "BIGINT",
          "def param1 = ctx.n; def param2 = param1 == 0 ? null : param1; ctx.c = param2"
        ),
        (
          "NULLIF(CAST(i AS BIGINT), 0)",
          "BIGINT",
          "def param1 = ctx.i; def param2 = (param1 != null ? (def)(((long) param1)) : null); " +
          "def param3 = param2 == 0 ? null : param2; ctx.c = param3"
        ),
        (
          "NULLIF(status, 'unknown')",
          "KEYWORD",
          "def param1 = ctx.status; def param2 = param1 == null || (\"unknown\" != null && " +
          "param1.compareTo(\"unknown\") == 0) ? null : param1; ctx.c = param2"
        )
      )
    ) { (expr, dataType, expected) =>
      withClue(s"[$expr] ")(processorOf(expr, dataType) shouldBe expected)
    }
  }

  /** The TEMPORAL pin the addendum singled out: `isEqual` is #373 item 2's fix and a `LocalTime`
    * has no such method, so this spelling must not drift with the type derivation.
    */
  it should "leave the temporal spellings byte-identical" in {
    fieldOf("NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param3 = LocalDate.parse((\"2025-01-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def param4 = param2 == null || (param3 != null && param2.isEqual(param3)) ? null : param2; param4"

    fieldOf("NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME))") should
    include("param2 == null || (param3 != null && param2.compareTo(param3) == 0) ? null : param2")
  }
}
