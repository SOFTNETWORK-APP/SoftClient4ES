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

  /** 🔴 Round 11 (M-3) — the CLASS axis, which is the axis BLOCKING-1 actually lived on.
    *
    * Round 10 added a gate over the CONSUMERS of `Predicate.not`. That is orthogonal to the defect
    * that blocked: `BetweenExpr` was a criteria CLASS with no `negated` override, and no
    * consumer-side check could ever see it. A class that can carry its own `NOT` and does not say
    * how to flip it silently falls back to an Elasticsearch `must_not`, which MATCHES a document
    * lacking the field — the semantics this story exists to remove.
    *
    * The rule has no allow-list: if the constructor declares a `maybeNot` field, the body must
    * override `negated`. Shown failing by deleting one override (measured: removing
    * `BetweenExpr.negated` reddens this with that class named).
    *
    * ⚠️ SCOPE, stated rather than implied. `MatchCriteria` and `MultiMatchCriteria` declare NO
    * `maybeNot` field — a full-text match has no negated spelling of its own — so they are outside
    * this rule by construction, and `NOT match(x) AGAINST ('y')` still takes the `must_not` route.
    * MEASURED live on ES 8.18.3: it matches a document that does not carry the field. That is the
    * same deviation the bare-column `NOT status = 'A'` route has, it is pinned in
    * `GatewayApiIntegrationSpec`, and it is NOT fixed here: giving `MatchCriteria` a `maybeNot`
    * changes its arity and its `.sql` render (which `MaterializedViewExtension` persists) and needs
    * a new bridge arm emitting `must_not(match) + exists(field)` — too much for the round that
    * closes this story, and the lead's call, not the dev's.
    */
  "every criteria class that can carry its own NOT" should "say how to flip it" in {
    def root: java.io.File = {
      var d = new java.io.File(".").getAbsoluteFile
      while (d != null && !new java.io.File(d, "build.sbt").isFile) d = d.getParentFile
      if (d == null) fail("could not locate the build root") else d
    }
    def scalaFilesUnder(dir: java.io.File): Seq[java.io.File] =
      if (!dir.isDirectory) Nil
      else
        Option(dir.listFiles).toSeq.flatten.flatMap { f =>
          if (f.isDirectory) scalaFilesUnder(f)
          else if (f.getName.endsWith(".scala")) Seq(f)
          else Nil
        }
    val sources = scalaFilesUnder(new java.io.File(root, "sql/src/main"))
    sources should not be empty
    val offenders = sources.flatMap { f =>
      val text = new String(java.nio.file.Files.readAllBytes(f.toPath), "UTF-8")
        .replaceAll("(?s)/\\*.*?\\*/", " ")
        .replaceAll("(?m)//.*$", " ")
      // Each top-level `case class` body, up to the next top-level definition.
      val starts = """(?m)^case class (\w+)\(""".r.findAllMatchIn(text).toSeq
      starts.zipWithIndex.flatMap { case (m, i) =>
        val end = if (i + 1 < starts.size) starts(i + 1).start else text.length
        val body = text.substring(m.start, end)
        val declaresMaybeNot =
          """(?m)^\s*(override\s+)?(val\s+)?maybeNot:\s*Option\[NOT\.type\]""".r
            .findFirstIn(body)
            .isDefined
        if (declaresMaybeNot && !body.contains("override def negated"))
          Some(s"${m.group(1)} (${f.getName})")
        else None
      }
    }
    withClue(
      "these criteria classes declare a `maybeNot` field but never override `negated`, so a " +
      "predicate's NOT over them falls back to an Elasticsearch `must_not`, which MATCHES a " +
      "document lacking the field:\n  " + offenders.mkString("\n  ") + "\n"
    )(offenders shouldBe empty)
  }

}
