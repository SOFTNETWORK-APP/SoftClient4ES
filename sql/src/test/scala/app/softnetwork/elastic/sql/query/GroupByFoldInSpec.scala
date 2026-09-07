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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #253's two lead fold-ins and the OFFSET ruling (2026-09-04).
  *
  *   - FOLD-IN 1: a ROW-INVARIANT literal is legal beside a `GROUP BY` (it cannot vary within a
  *     group, so it needs no bucket). What rejected it was `SingleSearch.validate()`'s
  *     non-aggregated-field check, not the grammar -- `SELECT 2 AS COL2 FROM t` has always parsed.
  *   - FOLD-IN 2: `GROUP BY <select alias>` resolves to the aliased field. It used to emit `terms {
  *     field: "<alias>" }` on a NON-EXISTENT field, which Elasticsearch answers with ZERO buckets
  *     and HTTP 200 -- a silent wrong answer in #253's own family.
  *   - `OFFSET` under a `GROUP BY` is rejected loudly, where it used to be silently dropped by the
  *     aggregation emission branch.
  *
  * Every "today" verdict in the comments was MEASURED against a parser compiled from `origin/main`
  * `8c426253` on 2026-09-07.
  */
class GroupByFoldInSpec extends AnyFlatSpec with Matchers {

  private def parsed(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(select: SelectStatement) =>
        select.statement match {
          case Some(s: SingleSearch) => s
          case other                 => fail(s"Not a SingleSearch: $other")
        }
      case Right(s: SingleSearch) => s
      case other                  => fail(s"Failed to parse '$sql': $other")
    }

  private def reasonOf(sql: String): String =
    Parser(sql).swap.getOrElse(fail(s"expected Left for [$sql]")).msg

  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  private def accepts(sql: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] reason=[${Parser(sql).swap.map(_.msg).getOrElse("")}] ") {
      Parser(sql).isRight shouldBe true
    }
    ()
  }

  private def bucketTriples(sql: String): Seq[(String, String, String)] =
    parsed(sql).buckets.map(b => (b.name, b.path, b.sourceBucket))

  // ---- FOLD-IN 1: a row-invariant literal that IS the group key ----
  //
  // 🔴 SCOPE, decided by MEASUREMENT during implementation (2026-09-07) and escalated to the lead.
  // FOLD-IN 1 has two sub-cases and only one of them works end to end:
  //
  //   (A) the constant IS the grouped item (`GROUP BY 2` naming `2 AS COL2`, or `GROUP BY COL2`).
  //       The bucket key carries the value, and the four Tableau corpus probes PD-7 names -- the
  //       ones "holding the published ceiling at 65/99" -- are all this shape. It is delivered
  //       here, end to end, and verified against real Elasticsearch.
  //
  //   (B) the constant is an EXTRA ungrouped column (`SELECT category, 2 AS flag ... GROUP BY
  //       category`). MEASURED on real ES 8.18: the statement parses, the constant is emitted as a
  //       `script_fields` entry, the aggregation path answers with `"size": 0`, and the column
  //       comes back **null** for every row. Accepting it would trade one loud rejection for a
  //       silent wrong answer, in the exact family this story exists to close, so it is NOT
  //       accepted here; the rows below pin the rejection so the decision is visible.
  //
  // Case (B) needs the constant PROJECTED into each aggregation row, which lives in `core`
  // (`ElasticConversion.jsonToRows` takes `fields: Seq[String]`, with no statement in scope) and
  // reaches ~10 call sites across `SearchApi` / `ScrollApi`. That is a design question for the
  // lead, not a drive-by: it is recorded in `docs/issues/local-21.3-grouped-select-literal.md`.

  "a constant that IS the group key" should "be accepted (FOLD-IN 1, case A)" in {
    // The 4-row Tableau corpus shape, and its alias spelling. Both were Left before this story
    // (the ordinal resolved onto the RAW literal identifier, whose bucket name was "" rather than
    // the alias, so the non-aggregated-field check rejected the SELECT item).
    accepts("SELECT SUM(1) AS COL, 2 AS COL2 FROM t GROUP BY 2")
    accepts("SELECT SUM(1) AS COL, 2 AS COL2 FROM t GROUP BY 2 HAVING COUNT(1) > 0")
    accepts("SELECT 2 AS COL2 FROM t GROUP BY COL2")
    accepts("SELECT 3 AS c FROM t GROUP BY 1")
  }

  it should "keep accepting a constant written directly as the group key" in {
    // Pre-existing on main and unchanged -- included so the constant-bucket work is pinned for the
    // non-integer spellings too (a string constant renders and re-parses as itself).
    accepts("SELECT 'x' AS lbl FROM t GROUP BY 'x'")
  }

  "an ungrouped constant beside a GROUP BY" should "stay rejected (FOLD-IN 1, case B)" in {
    // See the scope note above: accepted, this returns the column as NULL on every row (MEASURED
    // on real ES 8.18). The rejection is the SAME message these statements got before this story,
    // so nothing regresses -- what changes is that the reason is now recorded rather than assumed.
    rejects("SELECT category, 2 AS COL2 FROM t GROUP BY category", "Non-aggregated fields")
    rejects("SELECT category, 'x' AS lbl FROM t GROUP BY category", "Non-aggregated fields")
    rejects("SELECT SUM(amount) AS s, 2 AS COL2 FROM t GROUP BY country", "Non-aggregated fields")
  }

  it should "resolve an ordinal naming a literal EXACTLY ONCE" in {
    // The substituted identifier is itself ordinal-SHAPED (`name = ""`, `functions = [LongValue]`),
    // so a second derivation of `ordinalOf` on an already-substituted bucket would re-read the
    // literal as a position. `SELECT 3 AS c FROM t GROUP BY 1` is the probe: position 1, literal 3.
    // If resolution ran twice the bucket would re-point to position 3, which does not exist, and
    // the statement would be rejected as out of range.
    accepts("SELECT 3 AS c FROM t GROUP BY 1")
    val s = parsed("SELECT 3 AS c FROM t GROUP BY 1")
    s.buckets.map(_.name) shouldBe Seq("c")
  }

  it should "keep accepting the alias-of-a-literal spelling (the don't-regress pin)" in {
    // Right TODAY (measured) -- this pin proves the fix does not disturb it.
    accepts("SELECT 2 AS COL2, SUM(amount) AS s FROM t GROUP BY COL2")
  }

  it should "re-emit a constant bucket as its ALIAS, so the render re-parses to the same bucket" in {
    // 🔴 Found while implementing. A bucket resolved onto an integer constant renders through
    // `Identifier.sql` as the bare `2`, which the grammar reads back as POSITION 2. MEASURED
    // before the fix: `SELECT 2 AS COL2, SUM(amount) AS s FROM t GROUP BY COL2` rendered
    // `... GROUP BY 2` and re-parsed as `GROUP BY SUM(amount)` -- a DIFFERENT aggregation, and one
    // `MaterializedViewExtension` would persist and re-run. Story 21.1's AD-13 lesson, one clause
    // over: re-parse the RENDER, because every parse-only assertion is green either way.
    Seq(
      "SELECT 2 AS COL2, SUM(amount) AS s FROM t GROUP BY COL2",
      "SELECT 2 AS COL2 FROM t GROUP BY COL2",
      "SELECT 3 AS c FROM t GROUP BY 1",
      "SELECT SUM(1) AS COL, 2 AS COL2 FROM t GROUP BY 2"
    ).foreach { sql =>
      val rendered = Parser(sql).map(_.sql).getOrElse(fail(s"did not parse: $sql"))
      withClue(s"[$sql] rendered=[$rendered] ") {
        Parser(rendered).map(_.sql).getOrElse("") shouldBe rendered
        parsed(rendered).buckets.map(b => (b.name, b.path)) shouldBe
        parsed(sql).buckets.map(b => (b.name, b.path))
      }
    }
    // A NON-integer constant is unambiguous and keeps rendering as itself (unchanged behaviour).
    Parser("SELECT 'x' AS lbl FROM t GROUP BY 'x'").map(_.sql).getOrElse("") should
    include("GROUP BY 'x'")
  }

  it should "reject a constant bucket that has no alias to name it" in {
    // The residual of the rule above: with no explicit SELECT alias there is NO spelling that
    // re-emits the bucket (`SELECT 3 FROM t GROUP BY 1` would render `GROUP BY 3` and re-parse as
    // position 3, which is out of range). Rejected rather than shipped with a corrupt render --
    // and it was rejected before the constant classification landed too, so nothing regresses.
    rejects("SELECT 3 FROM t GROUP BY 1", "requires a SELECT alias")
  }

  it should "still reject an ungrouped COLUMN" in {
    rejects("SELECT category, country FROM t GROUP BY category", "Non-aggregated fields country")
  }

  it should "never treat a ROW-VARIANT pseudo-value as a constant" in {
    // `Bucket.shouldBeScripted`'s constant classification is an ALLOW-list, so a row-VARIANT value
    // can never be scripted as a constant bucket: `RANDOM` re-evaluates per document and `?` is an
    // unbound parameter.
    // ⚠️ The spelling is the bare `RANDOM`, not `RANDOM()`: MEASURED, `RANDOM()` does not parse at
    // all ("end of input expected"), so a `RANDOM()` row would have passed for the wrong reason.
    rejects("SELECT category, RANDOM AS r FROM t GROUP BY category", "Non-aggregated fields")
    rejects("SELECT category, ? AS p FROM t GROUP BY category", "Non-aggregated fields")
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT RANDOM AS r FROM t").select.fields.head.identifier
    ) shouldBe false
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT ? AS p FROM t").select.fields.head.identifier
    ) shouldBe false
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT 2 AS n FROM t").select.fields.head.identifier
    ) shouldBe true
  }

  // ---- FOLD-IN 2: GROUP BY <select alias> resolves to the aliased field ----

  "GROUP BY <select alias>" should "produce the SAME bucket as the explicit spelling" in {
    // Equivalence oracle (measured pre-fix: (pays, pays, pays) vs (pays, country, country)).
    bucketTriples("SELECT country AS pays FROM t GROUP BY pays") shouldBe
    bucketTriples("SELECT country AS pays FROM t GROUP BY country")
    bucketTriples("SELECT country AS pays FROM t GROUP BY pays") shouldBe
    Seq(("pays", "country", "country"))
  }

  it should "resolve an EXPRESSION alias to the aliased expression's bucket" in {
    bucketTriples("SELECT UPPER(country) AS c FROM t GROUP BY c") shouldBe
    bucketTriples("SELECT UPPER(country) AS c FROM t GROUP BY UPPER(country)")
  }

  it should "fix the aggregate-BEARING twin too (pre-existing, even without #253)" in {
    parsed("SELECT country AS pays, COUNT(*) AS cnt FROM t GROUP BY pays").buckets
      .map(_.sourceBucket) shouldBe Seq("country")
  }

  it should "let a PROJECTED column win over an alias spelled the same" in {
    // `SELECT country AS pays FROM t GROUP BY country` must never re-route through an alias.
    bucketTriples("SELECT country AS pays FROM t GROUP BY country") shouldBe
    Seq(("pays", "country", "country"))
    // And a column aliased to its own name is not an alias reference either.
    bucketTriples("SELECT country AS country FROM t GROUP BY country") shouldBe
    Seq(("country", "country", "country"))
  }

  it should "index the bucket under BOTH spellings so the SELECT field finds it" in {
    val s = parsed("SELECT country AS pays FROM t GROUP BY pays")
    s.bucketNames.keys.toSeq should contain("country")
  }

  it should "normalise the alias to the resolved column on render, with the render fixed point" in {
    val rendered = Parser("SELECT country AS pays FROM t GROUP BY pays")
      .map(_.sql)
      .getOrElse(fail("did not parse"))
    rendered should include("GROUP BY country")
    Parser(rendered).map(_.sql).getOrElse("") shouldBe rendered
  }

  it should "leave a bucket that matches no alias alone" in {
    bucketTriples("SELECT category FROM t GROUP BY category") shouldBe
    Seq(("category", "category", "category"))
  }

  // ---- OFFSET under GROUP BY: loud rejection ----

  "OFFSET under a GROUP BY" should "be rejected loudly, aggregate-free or not" in {
    // BOTH succeed today with the offset silently dropped (behaviour change -- release note).
    rejects("SELECT category FROM t GROUP BY category LIMIT 10 OFFSET 5", "OFFSET")
    rejects(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category LIMIT 10 OFFSET 5",
      "OFFSET"
    )
  }

  it should "leave LIMIT without OFFSET, and OFFSET without GROUP BY, untouched" in {
    accepts("SELECT category FROM t GROUP BY category LIMIT 10")
    accepts("SELECT category FROM t LIMIT 10 OFFSET 5")
  }
}
