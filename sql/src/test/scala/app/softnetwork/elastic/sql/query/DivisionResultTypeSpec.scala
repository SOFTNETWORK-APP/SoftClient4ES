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
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** The lead's ruling of 2026-09-23 on issue #382: **`/` decides its own result type.** A division
  * of numbers is floating-point whatever its operands are; the result type of a division is a
  * property of the OPERATOR, not of its arguments. `+`, `-`, `*` and `%` keep deriving theirs from
  * what the operands RENDER, which is the OQ-2 repair and is untouched here.
  *
  * 🔴 WHY, measured rather than argued — three separate defects, all on real Elasticsearch 8.18.3:
  *
  *   1. `SELECT n / m` over two INTEGER columns (7 and 2) answered `3`, while the SAME statement
  *      inside a JOIN answered `3.5`: a JOIN evaluates its SELECT list in DuckDB
  *      (`JoinPlanner.scala:1093` hands it `identifier.sql`) and DuckDB, like MySQL, treats `/` as
  *      always-decimal. The divergence therefore already existed on `main` for a plain `n / m`. 2.
  *      `CREATE TABLE u (n INTEGER, m INTEGER, c DOUBLE SCRIPT AS (n / m))` emitted `param1 /
  *      param2` with NO coercion at all, so a column the user DECLARED `DOUBLE` stored `3.0`. 3. A
  *      zero divisor was never the NULL the documentation promises. INTEGER division THREW (a
  *      search answered HTTP 400 "all shards failed"; an ingest processor swallowed it under
  *      `ignore_failure` and the column was simply ABSENT), and FLOATING division produced
  *      `Infinity` — which Elasticsearch REFUSES to index, so the WHOLE DOCUMENT was rejected with
  *      `[double] supports only finite values`. That third one is a data-loss bug that predates
  *      this ruling for any `double` column, and it is why the guard ships WITH the ruling rather
  *      than after it.
  *
  * The corresponding VALUES are executed against a real cluster in `GatewayApiIntegrationSpec`
  * ("divide as SQL divides", "index the document a zero divisor used to destroy"): an emission that
  * merely changed is not evidence.
  *
  * 🔴 Blast radius, MEASURED by a differential probe (five operators x thirteen operand shapes x
  * thirteen, in the computed-column, projection, predicate and sort venues plus the expression's
  * reported type — 5,070 cells) against a control clone at this branch's previous HEAD:
  *
  * {{{
  *   moved 726 of 5,070 cells, ALL of them `/`   (129 of the 169 division shapes)
  *       280  zero guard + the left operand widened to double   (integer / integer)
  *       280  zero guard only                                   (already floating)
  *         81  the expression's own type INT|BIGINT -> DOUBLE
  *         45  the left operand widened to double only           (provably non-zero divisor)
  *         40  ... of which 40 also gained the `(def)` on the live branch
  *   moved 0 of 4,056 cells for `+`, `-`, `*`, `%`
  * }}}
  *
  * The 40 unmoved division shapes are the ones that need nothing: every shape over the KEYWORD
  * column (a non-numeric fold, deliberately untouched) and every already-floating division by a
  * provably non-zero literal, such as `x / 2` and `CAST(n AS DOUBLE) / 2`.
  */
class DivisionResultTypeSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("m", SQLTypes.Int),
      Column("b", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("s", SQLTypes.Keyword)
    )
  )

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema))
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The ingest-processor source of a computed column, through the production seam. */
  private def processorOf(expr: String): String =
    Parser(
      s"CREATE TABLE t (n INTEGER, m INTEGER, b BIGINT, x DOUBLE, c DOUBLE SCRIPT AS ($expr))"
    ) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == "c")
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$expr]"))
      case other => fail(s"[$expr] expected a CreateTable, got $other")
    }

  private def fieldOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT $expr AS c FROM t").select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  private def predicateOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT n FROM t WHERE $expr > 2").where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$expr] has no WHERE"))
    s"$ctx$body"
  }

  private def arithmeticOf(expr: String): ArithmeticExpression =
    single(s"SELECT $expr AS c FROM t").select.fields.head.identifier.functions
      .collectFirst { case a: ArithmeticExpression => a }
      .getOrElse(fail(s"[$expr] carries no ArithmeticExpression"))

  /** 🔴 The mechanism itself, asked of the `ArithmeticExpression` rather than of the field that
    * wraps it. Asked of the FIELD it would be VACUOUS: the wrapping identifier answers `NUMERIC`
    * before and after, because `ArithmeticExpression.applyType` is the identity and the chain never
    * folds this type in. It is `baseType` that every `coerce` in both renderings takes as its
    * target — and, through `Table.mergeWithSearch`, what a CTAS would write if the chain propagated
    * it (it does not: a CTAS target for `SELECT n / m AS c` is `NUMERIC` before AND after this
    * change, measured; recorded here so nobody reads the row below as a mapping promise).
    */
  "a division of numbers" should "report DOUBLE whatever its operands are" in {
    forAll(
      Table(
        ("expression", "expected"),
        ("n / m", SQLTypes.Double: SQLType), // INTEGER / INTEGER -- was INT
        ("n / b", SQLTypes.Double: SQLType), // INTEGER / BIGINT  -- was BIGINT
        ("x / m", SQLTypes.Double: SQLType), // DOUBLE  / INTEGER -- already DOUBLE
        ("2 / 2", SQLTypes.Double: SQLType), // two literals      -- was BIGINT
        // the controls: every other operator still derives its type from the operands, and a
        // non-numeric fold is not a division of NUMBERS at all
        ("n + m", SQLTypes.Int: SQLType),
        ("n - m", SQLTypes.Int: SQLType),
        ("n * m", SQLTypes.Int: SQLType),
        ("n % m", SQLTypes.Int: SQLType),
        ("n % b", SQLTypes.BigInt: SQLType),
        ("s / 2", SQLTypes.Varchar: SQLType)
      )
    )((expr, expected) => withClue(s"[$expr] ")(arithmeticOf(expr).baseType shouldBe expected))
  }

  /** 🔴 The type alone is not enough, and that is the trap this row exists for: making `out` DOUBLE
    * leaves `param1 / param2` untouched, because a NULLABLE numeric operand deliberately skips its
    * coercion (a primitive cast over a null-guarded expression does not compile — see
    * `ArithmeticExpression.coercionSkipped`). Two `def`s holding `Integer` then divide as INTEGERS,
    * silently, exactly as before. The `(double)` therefore goes INSIDE the null guard, where the
    * operand is known non-null, and ONE side is enough because Painless promotes `double / def`.
    */
  it should "divide as floating point when BOTH operands are null-guarded integers" in {
    forAll(
      Table(
        ("venue", "emitted"),
        ("computed column", () => processorOf("n / m")),
        ("projection", () => fieldOf("n / m")),
        ("predicate", () => predicateOf("n / m"))
      )
    ) { (venue, emitted) =>
      val script = emitted()
      withClue(s"[$venue] -> [$script] ")(script should include("((double) param1) / param2"))
    }
  }

  /** The same three venues, pinned to the byte, so the guard above cannot be satisfied by an
    * emission that also broke something else.
    */
  it should "emit exactly this, in every venue" in {
    processorOf("n / m") shouldBe
    "def param1 = ctx.n; def param2 = ctx.m; " +
    "ctx.c = (param1 == null || param2 == null || param2 == 0) ? null : (((double) param1) / param2)"

    fieldOf("n / m") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['m'].size() == 0 ? null : doc['m'].value); " +
    "(param1 == null || param2 == null || param2 == 0) ? null : (((double) param1) / param2)"

    predicateOf("n / m") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['m'].size() == 0 ? null : doc['m'].value); " +
    "def left1 = (param1 == null || param2 == null || param2 == 0) ? null : " +
    "(((double) param1) / param2); (left1 == null ? false : ((left1 > 2)))"
  }

  /** 🔴 The widening is emitted ONLY where nothing else already carries it, and that restraint is
    * deliberate: `ScriptProcessor.source` is PERSISTED in `_meta` and `IngestPipeline.diff`
    * compares it, so an unconditional `(double)` would rewrite every stored computed column that
    * divides — including the ones that always answered correctly — and each would report
    * `ProcessorChanged` on the next ALTER. Only the shapes whose ANSWER changes move.
    */
  it should "not widen an operand that already renders floating point" in {
    forAll(
      Table(
        ("expression", "emitted"),
        // the DOUBLE column carries it: `x / m` divides as floating already
        (
          "x / m",
          "def param1 = ctx.x; def param2 = ctx.m; " +
          "ctx.c = (param1 == null || param2 == null || param2 == 0) ? null : (param1 / param2)"
        ),
        // the LITERAL carries it, because a non-nullable operand IS coerced to `out`
        ("n / 2", "def param1 = ctx.n; ctx.c = (param1 == null) ? null : (param1 / ((double) 2))"),
        // ... and so a division by a non-zero literal is byte-identical to before the ruling
        ("x / 2", "def param1 = ctx.x; ctx.c = (param1 == null) ? null : (param1 / ((double) 2))")
      )
    ) { (expr, expected) =>
      withClue(s"[$expr] ")(processorOf(expr) shouldBe expected)
    }
  }

  /** 🔴 A zero divisor is NULL, in every venue — the documented contract
    * (`documentation/sql/operators.md`: *"Engine returns NULL for invalid arithmetic"*), which was
    * never true. MEASURED on real Elasticsearch 8.18.3, for the same `n / m` with `m = 0`:
    *
    * {{{
    * integer   ingest (ignore_failure: true)  document indexed, column ABSENT
    * integer   search                         HTTP 400, "all shards failed", `/ by zero`
    * floating  ingest                         HTTP 400 -- the WHOLE DOCUMENT is REJECTED:
    *                                           "[double] supports only finite values, got [Infinity]"
    * floating  search                         the string "Infinity" in the result column
    * }}}
    *
    * So without the guard this ruling would have converted "one column missing" into "the document
    * is lost" on every bulk ingest with a zero divisor — and the floating rows show the loss was
    * already happening on `main` for any `double` column. The guard is part of the ruling, not a
    * follow-up.
    *
    * ⚠️ `%` is deliberately NOT guarded (the ruling is about `/` only): `n % 0` still throws, i.e.
    * HTTP 400 in a search and an absent column in an ingest processor. Recorded, not changed.
    */
  it should "answer NULL for a zero divisor rather than throwing or producing Infinity" in {
    forAll(
      Table(
        ("venue", "emitted"),
        ("computed column", () => processorOf("n / m")),
        ("projection", () => fieldOf("n / m")),
        ("predicate", () => predicateOf("n / m"))
      )
    ) { (venue, emitted) =>
      val script = emitted()
      withClue(s"[$venue] -> [$script] ")(script should include("|| param2 == 0) ? null :"))
    }
  }

  /** 🔴 ... and it costs NOTHING where it cannot fire. A divisor that is a non-zero numeric LITERAL
    * — `/ 2`, `/ 100`, the overwhelming majority of real division — is proved safe at emission time
    * and gets no guard at all. This is the row that keeps the one above from being satisfied by a
    * guard sprayed over every division.
    *
    * 🔴 The unwrap it depends on is not the obvious one: a numeric literal operand is NOT a
    * `NumericValue`. `TypeParser.identifierWithValue` is `(value ^^ functionAsIdentifier) >> cast`,
    * so `/ 2` arrives as a `GenericIdentifier` with an EMPTY name carrying `LongValue(2)` as its
    * only function. Matching `NumericValue` directly — written first — proved every divisor
    * un-provable and emitted `((double) 2) == 0` on every `/ 2` in the corpus.
    */
  it should "emit no guard at all for a provably non-zero literal divisor" in {
    forAll(
      Table(
        "expression",
        "n / 2",
        "x / 2",
        "n / 2.0",
        "CAST(n AS DOUBLE) / 2"
      )
    ) { expr =>
      val script = processorOf(expr)
      withClue(s"[$expr] -> [$script] ")(script should not include "== 0)")
    }
  }

  /** The non-nullable rendering — two literals, so `toPainless` falls through to `painless` and
    * there is no null guard to hang anything on.
    *
    * 🔴 The live branch is `(def)`-cast because Painless types `cond ? null : <primitive>` as
    * `Object`: without it `SELECT 10 / 0` answered `class_cast_exception: Cannot cast from [double]
    * to [java.lang.Object]` instead of NULL — MEASURED on 8.18.3, and the same trap story 21.8 hit
    * with a primitive method placed outside a null guard.
    */
  it should "divide two literals as floating point, and guard a literal zero" in {
    fieldOf("10 / 3") shouldBe "((double) 10) / ((double) 3)"
    fieldOf("10 / 0") shouldBe "((((double) 0) == 0) ? null : (def)(((double) 10) / ((double) 0)))"
  }

  /** 🔴 The ruling is `/` and ONLY `/`. Measured over the corpus: zero of the 4,056 `+`, `-`, `*`
    * and `%` cells moved. Pinned to the byte here so that widening the override to another operator
    * — the natural "while we are here" edit — reddens.
    */
  it should "leave every other operator byte-identical" in {
    forAll(
      Table(
        ("expression", "expected"),
        ("n + m", "(param1 == null || param2 == null) ? null : (param1 + param2)"),
        ("n - m", "(param1 == null || param2 == null) ? null : (param1 - param2)"),
        ("n * m", "(param1 == null || param2 == null) ? null : (param1 * param2)"),
        ("n % m", "(param1 == null || param2 == null) ? null : (param1 % param2)")
      )
    ) { (expr, expected) =>
      withClue(s"[$expr] ")(
        processorOf(expr) shouldBe s"def param1 = ctx.n; def param2 = ctx.m; ctx.c = $expected"
      )
    }
  }

  /** A fold that is not NUMERIC is not a division of NUMBERS, so the ruling does not reach it: a
    * KEYWORD operand keeps exactly the rendering it had, wrong as that rendering is. Widening the
    * override to every `/` would turn this into a runtime cast failure instead.
    */
  it should "leave a non-numeric operand untouched" in {
    fieldOf("s / 2") shouldBe
    "def param1 = (doc['s'].size() == 0 ? null : doc['s'].value); " +
    "(param1 == null) ? null : (param1 / String.valueOf(2))"
  }

  /** 🔴 CHARACTERISATION, and the reason `documentation/sql/operators.md` cannot promise the
    * truncation idiom every other engine spells `DIV` or `//`: **the grammar rejects an arithmetic
    * expression as the operand of a function, of a `CAST` or of a `CASE` branch.** Verified on a
    * clean clone at `origin/main` as well as here, so it is pre-existing and not a consequence of
    * this ruling. It is UNFILED, and cited by BEHAVIOUR rather than by an issue number on purpose:
    * the obvious candidate — #267, *"CAST accepts no bare literal operand"* — is a DIFFERENT defect
    * that was closed and really is fixed (`CAST('125' AS BIGINT)` parses today).
    *
    * 🔴 The rule is DIRECTIONAL, which is what a reader needs in order to predict which rewrite
    * works: `f(<arithmetic>)` is rejected, `<arithmetic> f(…)` is accepted. It is also not about
    * division — ANY arithmetic operand is refused, `+` included — and parenthesising does not help.
    *
    * What DOES work is computing the quotient into a column and casting THAT column, which is in
    * the documentation and EXECUTED in `GatewayApiIntegrationSpec`. This row exists so that the day
    * the grammar learns these operands, the doc is known to be stale.
    */
  it should "still REJECT an arithmetic operand inside a function, CAST or CASE (recorded)" in {
    forAll(
      Table(
        ("statement", "accepted"),
        ("SELECT CAST(n / m AS INTEGER) AS c FROM t", false),
        // not about DIVISION: any arithmetic operand is refused
        ("SELECT CAST(n + m AS INTEGER) AS c FROM t", false),
        // ... and parenthesising does not help
        ("SELECT CAST((n / m) AS INTEGER) AS c FROM t", false),
        ("SELECT FLOOR(n / m) AS c FROM t", false),
        ("SELECT ABS(n / m) AS c FROM t", false),
        ("SELECT COALESCE(n / m, 0) AS c FROM t", false),
        ("SELECT CASE WHEN n > 1 THEN n / m ELSE 0 END AS c FROM t", false),
        // 🔴 the positive controls, without which "everything is rejected" would satisfy the row:
        // the SAME functions over a plain operand parse, a cast INSIDE the division parses, and --
        // the direction that matters -- arithmetic OVER a function call is fine
        ("SELECT CAST(n AS INTEGER) / 2 AS c FROM t", true),
        ("SELECT FLOOR(x) / 2 AS c FROM t", true),
        ("SELECT ABS(n) AS c FROM t", true),
        ("SELECT n / NULLIF(m, 0) AS c FROM t", true)
      )
    )((sql, accepted) => withClue(s"[$sql] ")(Parser(sql).isRight shouldBe accepted))
  }
}
