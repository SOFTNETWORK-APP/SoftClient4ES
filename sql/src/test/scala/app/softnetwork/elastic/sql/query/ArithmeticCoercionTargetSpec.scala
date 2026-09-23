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
import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** Issue #382, the lead's OQ-2 ruling: arithmetic over a CAST of a STRING column CONCATENATED
  * instead of computing. `SCRIPT AS (CAST(name AS BIGINT) + n)` stored **1257** where 132 was asked
  * for — HTTP 200, and a BIGINT mapping coerces that string happily.
  *
  * 🔴 The mechanism, measured by construction rather than from the two samples on the issue.
  * `ArithmeticExpression` derives its coercion TARGET from `FunctionN.argTypes = args.map(_.out)`,
  * and a schema-resolved `Identifier.out` reports the COLUMN's type:
  *
  * {{{
  * CAST(name AS BIGINT) + n     argTypes = List(KEYWORD, BIGINT)   out = VARCHAR
  *   left  out = KEYWORD   chainType = BIGINT   <- the column's type, not the chain's
  * }}}
  *
  * So `leastCommonSuperType` answered VARCHAR, `coerce`'s `(_, _: SQLLiteral)` arm wrapped BOTH
  * operands in `String.valueOf`, and Painless concatenated. The site's own scaladoc already stated
  * the rule it broke — *"Coerced FROM what the operand RENDERS, not from its column's type"* (#367)
  * — applied to the FROM and never to the TO. The repair is one derivation, `argTypes`, which is
  * the single input to `baseType` and therefore to `out`, so both renderings (`toPainless` for the
  * nullable path, `painless` for the other) are fixed by it.
  *
  * 🔴 It is NOT a DDL defect. The same `out` is the coercion target in a PREDICATE and in a
  * PROJECTION, which is why every row below is asserted in all three venues. Executed against real
  * Elasticsearch in `GatewayApiIntegrationSpec` ("compute, not concatenate, …"): an emission that
  * merely CHANGED is not evidence — the arithmetic has to produce the NUMBER.
  */
class ArithmeticCoercionTargetSpec
    extends AnyFlatSpec
    with Matchers
    with TableDrivenPropertyChecks {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("m", SQLTypes.BigInt),
      // a DOUBLE source, so a cast to INTEGER is a genuine NARROWING (issue #382, Ruling A)
      Column("x", SQLTypes.Double),
      Column("d", SQLTypes.Date)
    )
  )

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema))
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The ingest-processor source of a computed column, through the production seam. */
  private def processorOf(ddl: String, column: String = "c"): String =
    Parser(ddl) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == column)
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$column] in [$ddl]"))
      case other => fail(s"[$ddl] expected a CreateTable, got $other")
    }

  /** The WHERE criteria rendered as ONE script, prologue included. */
  private def predicateOf(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(sql).where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$sql] has no WHERE"))
    s"$ctx$body"
  }

  /** The first SELECT field rendered as one script. The body is rendered FIRST and bound: it is
    * what registers the parameters, so an inline `s"$ctx${…painless(ctx)}"` prints an EMPTY
    * prologue.
    */
  private def fieldOf(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(sql).select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  /** `String.valueOf` on an operand of a `+` is the fingerprint: it is how `coerce` widens to a
    * literal target, and two of them around a `+` is a Painless CONCATENATION.
    */
  private def concatenates(emitted: String): Boolean = emitted.contains("String.valueOf(")

  private val stringSourced = Table(
    ("venue", "emitted"),
    (
      "computed column",
      () =>
        processorOf(
          "CREATE TABLE t (name KEYWORD, m BIGINT, c BIGINT SCRIPT AS (CAST(name AS BIGINT) + m))"
        )
    ),
    (
      "computed column, safe cast",
      () =>
        processorOf(
          "CREATE TABLE t (name KEYWORD, m BIGINT, c BIGINT SCRIPT AS (TRY_CAST(name AS BIGINT) + m))"
        )
    ),
    ("predicate", () => predicateOf("SELECT name FROM t WHERE CAST(name AS BIGINT) + m > 130")),
    ("projection", () => fieldOf("SELECT CAST(name AS BIGINT) + m AS c FROM t")),
    (
      "literal right operand",
      () =>
        processorOf("CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (CAST(name AS BIGINT) + 1))")
    ),
    (
      "literal LEFT operand",
      () =>
        processorOf("CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (1 + CAST(name AS BIGINT)))")
    )
  )

  "arithmetic over a cast of a STRING column" should "coerce to what the operands RENDER" in {
    forAll(stringSourced) { (venue, emitted) =>
      val script = emitted()
      withClue(s"[$venue] -> [$script] ")(concatenates(script) shouldBe false)
    }
  }

  /** The control that makes the row above non-vacuous: a genuine widening to a string target must
    * STILL emit `String.valueOf`, or "no concatenation" would be satisfied by a coercion that
    * stopped working altogether.
    */
  it should "still widen to a string where a string really is the target" in {
    val emitted =
      processorOf("CREATE TABLE t (name KEYWORD, c KEYWORD SCRIPT AS (CONCAT(name, 'ab')))")
    withClue(emitted)(emitted should include("String.valueOf("))
  }

  /** 🔴 WHAT MOVES, AND WHAT DOES NOT — the honest statement of the repair's blast radius.
    *
    * An earlier version of this file claimed the repair "leaves arithmetic over a numeric source
    * byte-identical" and pinned two shapes that happen not to move. That is FALSE, and the lead has
    * ruled the new behaviour SHIPS (Ruling A), so the guard has to state what is true instead.
    *
    * MEASURED by a differential probe — the branch against a control whose only difference is
    * `argTypeOf` restored to `args.map(_.out)` — over 1,792 emissions (two cast spellings x four
    * source columns x four target types x four operators x four right-hand sides, in the DDL, the
    * predicate and the projection venue, plus the reversed-operand and no-cast orientations):
    *
    * {{{
    *   no cast at all                     128 rows, 0 moved
    *   cast to the column's OWN type      104 rows, 0 moved
    *   cast to a DIFFERENT type           596 rows moved
    * }}}
    *
    * So the rule is exactly this: a cast whose target DIFFERS from the column's declared type now
    * decides the arithmetic. In both directions, which is what makes it more than the string case
    * #382 was filed for:
    *
    * {{{
    *   x DOUBLE   CAST(x AS INTEGER) / 2   (lv1 / ((double) 2))  ->  (lv1 / 2)
    *   n INTEGER  CAST(n AS DOUBLE)  / 2   (lv1 / 2)             ->  (lv1 / ((double) 2))
    *   s KEYWORD  CAST(s AS BIGINT)  + m   String.valueOf(lv1)   ->  lv1          (the filed bug)
    *   n INTEGER  CAST(n AS KEYWORD) + 2   Long.parseLong(...)   ->  String.valueOf(...)
    * }}}
    *
    * The last line is a family the issue never named and the one to watch: a numeric column cast to
    * a string used to be silently parsed BACK to a number, so `CAST(n AS KEYWORD) + 2` computed
    * arithmetic. It now concatenates, which is self-consistent — the arithmetic follows the cast in
    * this direction too — and under `-`, `*` and `/` it becomes a runtime failure on a String
    * receiver instead of a silently un-done cast. Recorded on #382 for the lead; pinned below as
    * CHARACTERISATION so it cannot drift unnoticed either way.
    *
    * ⚠️ Release-note obligation, not a test one: `ScriptProcessor.source` is persisted in `_meta`
    * and `IngestPipeline.diff` compares it, so every STORED computed column of a moved shape
    * reports `ProcessorChanged` on the next ALTER.
    */
  it should "leave arithmetic with no cast, and an identity cast, byte-identical" in {
    forAll(
      Table(
        ("ddl", "expected"),
        // no cast: the coercion target is the column's type, which is also what it renders
        (
          "CREATE TABLE t (n INTEGER, m BIGINT, c BIGINT SCRIPT AS (n + m))",
          "def param1 = ctx.n; def param2 = ctx.m; " +
          "ctx.c = (param1 == null || param2 == null) ? null : (param1 + param2)"
        ),
        (
          "CREATE TABLE t (x DOUBLE, c DOUBLE SCRIPT AS (x / 2))",
          "def param1 = ctx.x; ctx.c = (param1 == null) ? null : (param1 / ((double) 2))"
        ),
        // an IDENTITY cast: `chainType` and `out` agree, so there is nothing to move
        (
          "CREATE TABLE t (n INTEGER, m BIGINT, c BIGINT SCRIPT AS (CAST(n AS INTEGER) + m))",
          "def param1 = ctx.n; def param2 = ctx.m; " +
          "ctx.c = (param1 == null || param2 == null) ? null : (param1 + param2)"
        ),
        (
          "CREATE TABLE t (x DOUBLE, c DOUBLE SCRIPT AS (CAST(x AS DOUBLE) / 2))",
          "def param1 = ctx.x; ctx.c = (param1 == null) ? null : (param1 / ((double) 2))"
        ),
        // ... and a function chain with no cast, which reads its own `out` and never did move
        (
          "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (LENGTH(name) + 1))",
          "def param1 = ctx.name; ctx.c = (param1 == null) ? null : param1.length() + 1"
        )
      )
    )((ddl, expected) => withClue(s"[$ddl] ")(processorOf(ddl) shouldBe expected))
  }

  /** 🔴 The NARROWING cast, pinned in all three venues — the semantic change the lead ruled ships.
    *
    * Executed on real Elasticsearch 8.18.3 in `GatewayApiIntegrationSpec` ("make a narrowing cast
    * decide the arithmetic, in BOTH venues"): `x = 5.0` stores `2.5` before and `2` after, and
    * `WHERE CAST(x AS INTEGER) / 2 > 2` returns a DIFFERENT SET of rows. A byte pin alone would
    * only say the emission moved; the cluster says which answer is right.
    *
    * All three venues, because `argTypes` feeds `baseType` feeds `out`, and `out` is the coercion
    * target in `toPainless` (the nullable path) AND in `painless` (the other) — one derivation,
    * three renderings, and a repair at a single local would have fixed only one of them.
    */
  it should "let a narrowing cast decide the coercion target, in every venue" in {
    forAll(
      Table(
        ("venue", "emitted", "expected"),
        (
          "computed column",
          () =>
            processorOf("CREATE TABLE t (x DOUBLE, c DOUBLE SCRIPT AS (CAST(x AS INTEGER) / 2))"),
          "def param1 = ctx.x; def lv1 = (param1 != null ? (def)(((int) param1)) : null); " +
          "ctx.c = (lv1 == null) ? null : (lv1 / 2)"
        ),
        (
          "projection",
          () => fieldOf("SELECT CAST(x AS INTEGER) / 2 AS c FROM t"),
          "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); " +
          "def lv1 = (param1 != null ? (def)(((int) param1)) : null); " +
          "(lv1 == null) ? null : (lv1 / 2)"
        ),
        (
          "predicate",
          () => predicateOf("SELECT name FROM t WHERE CAST(x AS INTEGER) / 2 > 2"),
          "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); " +
          "def lv1 = (param1 != null ? (def)(((int) param1)) : null); " +
          "def left2 = (lv1 == null) ? null : (lv1 / 2); " +
          "(left2 == null ? false : ((left2 > 2)))"
        )
      )
    )((venue, emitted, expected) => withClue(s"[$venue] ")(emitted() shouldBe expected))
  }

  /** The WIDENING direction, which the same derivation produces and which no other assertion
    * covers: an INTEGER column cast to DOUBLE now widens the literal it is divided by.
    */
  it should "let a widening cast decide the coercion target too" in {
    processorOf("CREATE TABLE t (n INTEGER, c DOUBLE SCRIPT AS (CAST(n AS DOUBLE) / 2))") shouldBe
    "def param1 = ctx.n; def lv1 = (param1 != null ? (def)(((double) param1)) : null); " +
    "ctx.c = (lv1 == null) ? null : (lv1 / ((double) 2))"
  }

  /** CHARACTERISATION, not an endorsement: a NUMERIC column cast to a STRING. Recorded on #382 for
    * the lead — it is the mirror image of the filed bug, the same rule produces it, and the lead
    * ruled only on the numeric-target direction.
    */
  it should "make a numeric column cast to a string CONCATENATE (recorded, #382)" in {
    processorOf("CREATE TABLE t (n INTEGER, c KEYWORD SCRIPT AS (CAST(n AS KEYWORD) + 2))") shouldBe
    "def param1 = ctx.n; " +
    "def lv1 = String.valueOf((param1 != null ? String.valueOf(param1) : null)); " +
    "ctx.c = (lv1 == null) ? null : (lv1 + String.valueOf(2))"
  }

  /** The type the expression REPORTS, not only the one it emits — the mechanism itself, asked of
    * the `ArithmeticExpression` rather than of the field that wraps it.
    *
    * 🔴 Asked of the FIELD it would be vacuous: the wrapping identifier answers `NUMERIC` before
    * and after, because `ArithmeticExpression.applyType` is the identity and the chain never folds
    * this type in. It is `baseType` — `leastCommonSuperType(argTypes)` — that read VARCHAR, and
    * that is what every `coerce` in both renderings takes as its target.
    */
  it should "report the type its operands RENDER, not the column's" in {
    forAll(
      Table(
        ("sql", "expected"),
        ("SELECT CAST(name AS BIGINT) + m AS c FROM t", SQLTypes.BigInt), // was VARCHAR
        ("SELECT CAST(n AS BIGINT) + m AS c FROM t", SQLTypes.BigInt) // the control: unchanged
      )
    ) { (sql, expected) =>
      val arithmetic = single(sql).select.fields.head.identifier.functions
        .collectFirst { case a: ArithmeticExpression =>
          a
        }
        .getOrElse(fail(s"[$sql] carries no ArithmeticExpression"))
      withClue(s"[$sql] -> ${arithmetic.baseType} ")(arithmetic.baseType shouldBe expected)
    }
  }
}
