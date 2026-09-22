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

  /** The other control: arithmetic over a cast of a NUMERIC column was always right, and the repair
    * must not move a byte of it.
    */
  it should "leave arithmetic over a numeric source byte-identical" in {
    forAll(
      Table(
        ("ddl", "expected"),
        (
          "CREATE TABLE t (n INTEGER, m BIGINT, c BIGINT SCRIPT AS (CAST(n AS BIGINT) + m))",
          "def param1 = ctx.n; def param2 = ctx.m; " +
          "def lv1 = (param1 != null ? (def)(((long) param1)) : null); " +
          "ctx.c = (lv1 == null || param2 == null) ? null : (lv1 + param2)"
        ),
        (
          "CREATE TABLE t (name KEYWORD, c BIGINT SCRIPT AS (LENGTH(name) + 1))",
          "def param1 = ctx.name; ctx.c = (param1 == null) ? null : param1.length() + 1"
        )
      )
    )((ddl, expected) => withClue(s"[$ddl] ")(processorOf(ddl) shouldBe expected))
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
