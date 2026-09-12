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
// 🔴 ALIASED: this spec lives in `…sql.query`, which declares its OWN `Table` (the FROM
// clause's). Scala 2.13 let the explicit import win; 2.12 does not, and only an explicit
// `++ 2.12.20 sql/Test/compile` sees it — no CI job compiles sql test sources on 2.12.
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story BIDC-8, AD-9 (review L-11) — a criteria rendered WITH a `PainlessContext` must be one
  * EXPRESSION, never a statement sequence.
  *
  * 🔴 Why this is a gate and not a style rule. `Criteria.painless` has three consumers and two of
  * them splice the rendering INTO a larger expression: `Criteria.painless`'s own `Predicate` arm
  * joins two renderings with `&&` / `||`, and a `CASE` condition captures one as `def paramN =
  * <rendering>;`. A rendering of the form `def left = X; left == null` is valid only at the TOP of
  * a script: spliced, it produces `def param1 = def left = ...` or `A && def left = ...`, which
  * Elasticsearch refuses at COMPILE time — a 400 on a statement the user typed correctly, and the
  * exact failure class AD-9 was opened for.
  *
  * The context is what makes the statement form unnecessary: `PainlessContext.bindLocal` hoists the
  * binding into the script PROLOGUE and hands back a bare name, so the criteria stays an
  * expression. A context-free rendering (`painless(None)`) has nowhere to hoist to and keeps the
  * statement form — that path is the HAVING / bucket-pipeline route, which is never spliced.
  *
  * REFUTES the reading that "no expectation moved" is evidence: the whole unit estate was green
  * while `WHERE UPPER(status) IS NULL` emitted a statement form under a context. This spec fails on
  * that, by construction, with no allow-list.
  */
class PainlessOperandFormSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("status", SQLTypes.Keyword),
      Column("other", SQLTypes.Keyword),
      Column("amount", SQLTypes.Double),
      Column("n", SQLTypes.Int),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  /** Every criteria shape that can carry a function-wrapped operand — which is what forces the
    * binding — plus the plain-column twin of each, so a fix that only guards the function case is
    * visible.
    */
  private val shapes: Seq[String] = Seq(
    "SELECT id FROM t WHERE UPPER(status) = 'A'",
    "SELECT id FROM t WHERE NOT UPPER(status) = 'A'",
    "SELECT id FROM t WHERE UPPER(status) = UPPER(other)",
    "SELECT id FROM t WHERE UPPER(status) <> 'A'",
    "SELECT id FROM t WHERE ABS(amount) > 10",
    "SELECT id FROM t WHERE UPPER(status) LIKE 'A%'",
    "SELECT id FROM t WHERE UPPER(status) NOT LIKE 'A%'",
    "SELECT id FROM t WHERE UPPER(status) IN ('A','B')",
    "SELECT id FROM t WHERE UPPER(status) NOT IN ('A','B')",
    "SELECT id FROM t WHERE ABS(amount) BETWEEN 1 AND 100",
    "SELECT id FROM t WHERE ABS(amount) NOT BETWEEN 1 AND 100",
    "SELECT id FROM t WHERE status IS NULL",
    "SELECT id FROM t WHERE status IS NOT NULL",
    "SELECT id FROM t WHERE UPPER(status) = 'A' AND ABS(amount) > 10",
    "SELECT id FROM t WHERE UPPER(status) = 'A' OR ABS(amount) > 10",
    "SELECT id FROM t WHERE ABS(amount) > 10 AND NOT UPPER(status) = 'A'"
  )

  private def renderedOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val criteria = ss
          .update(Some(schema))
          .where
          .flatMap(_.criteria)
          .getOrElse(fail(s"[$sql] has no WHERE criteria"))
        criteria.painless(Some(ctx))
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** A `;` that is NOT inside a string literal. Parentheses are irrelevant — Painless has no
    * bracketed statement sequence that could legitimately appear in an operand.
    */
  private def statementSeparators(rendered: String): Int = {
    var inString = false
    var i = 0
    var count = 0
    while (i < rendered.length) {
      val c = rendered.charAt(i)
      if (inString) {
        if (c == '\\') i += 1
        else if (c == '"') inString = false
      } else if (c == '"') inString = true
      else if (c == ';') count += 1
      i += 1
    }
    count
  }

  "a criteria rendered with a PainlessContext" should "be a single expression, never a statement sequence" in {
    val offenders = shapes.map(sql => sql -> renderedOf(sql)).collect {
      case (sql, rendered) if statementSeparators(rendered) > 0 => s"$sql\n    => $rendered"
    }
    withClue(
      s"${offenders.size} criteria rendered a STATEMENT SEQUENCE under a context; a `def x = …;` " +
      "binding belongs in the context prologue (PainlessContext.bindLocal), not in the operand:\n  " +
      offenders.mkString("\n  ") + "\n"
    )(offenders shouldBe empty)
  }

  it should "hoist every binding it needs into the context prologue instead" in {
    // Positive proof that the expression form is not achieved by dropping the binding: the shapes
    // whose operand is function-wrapped DO declare something, and it is in the prologue.
    Parser("SELECT id FROM t WHERE UPPER(status) = 'A'") match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val rendered = ss.update(Some(schema)).where.flatMap(_.criteria).get.painless(Some(ctx))
        ctx.nonEmpty shouldBe true
        ctx.toString should include("def ")
        rendered should not include "def "
      case other => fail(s"expected a SingleSearch, got $other")
    }
  }

  "a context-free rendering" should "keep the statement form, which is what HAVING consumes" in {
    // The counterpart of the gate above: `painless(None)` has no prologue to hoist into, and the
    // bucket-pipeline consumer never splices it into another expression. Pinning this keeps the
    // gate honest — it must be the CONTEXT that removes the statement, not a blanket ban.
    Parser("SELECT id FROM t WHERE UPPER(status) = 'A'") match {
      case Right(ss: SingleSearch) =>
        ss.update(Some(schema)).where.flatMap(_.criteria).get.painless(None) should include("def ")
      case other => fail(s"expected a SingleSearch, got $other")
    }
  }
}
