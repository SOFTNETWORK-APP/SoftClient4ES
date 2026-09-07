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

import scala.collection.immutable.ListMap
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

  // ---- FOLD-IN 1: a row-invariant literal beside a GROUP BY ----
  //
  // Two shapes, both delivered, and they are delivered by different machinery:
  //
  //   (A) the constant IS the grouped item (`GROUP BY 2` naming `2 AS COL2`, or `GROUP BY COL2`).
  //       The bucket key carries the value. This needs NO validator change -- once `Bucket.update`
  //       resolves the position AND updates it, the bucket's `name` is the field's alias, which the
  //       existing non-aggregated-field check already excuses -- but it does need the bucket to be
  //       SCRIPTED (a constant has no field name to give) and to RE-EMIT its alias (its own render
  //       reads back as an ordinal).
  //
  //   (B) the constant is an EXTRA ungrouped column (`SELECT category, 2 AS flag ... GROUP BY
  //       category`). Accepting it is only half the job: an aggregation response has no hits, so
  //       the `script_fields` entry is never fetched under `"size": 0` and the column came back
  //       NULL on every row (MEASURED on real ES 8.18). `SingleSearch.rowInvariantProjection`
  //       carries the value from the AST and `SearchApi` merges it into each aggregation row.

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

  "an ungrouped constant beside a GROUP BY" should "be accepted (FOLD-IN 1, case B)" in {
    // Standard SQL: a constant does not vary within a group, so it needs no grouping. All Left on
    // `origin/main` with "Non-aggregated fields ... cannot be selected when GROUP BY is present".
    accepts("SELECT category, 2 AS COL2 FROM t GROUP BY category")
    accepts("SELECT category, 'x' AS lbl FROM t GROUP BY category")
    accepts("SELECT category, 2.5 AS ratio FROM t GROUP BY category")
    accepts("SELECT SUM(amount) AS s, 2 AS COL2 FROM t GROUP BY country")
    accepts("SELECT category, true AS b FROM t GROUP BY category")
    accepts("SELECT category, NULL AS n FROM t GROUP BY category")
    accepts("SELECT category, PI AS p FROM t GROUP BY category")
  }

  it should "carry the constant's VALUE, keyed by its output name, for the result side to project" in {
    // 🔴 Acceptance alone would be a NEW silent wrong answer: an aggregation response has no hits,
    // so the `script_fields` entry a constant is emitted as is never fetched under `"size": 0` and
    // the column came back NULL on every row (MEASURED on real ES 8.18). The value therefore comes
    // from the AST, and `SearchApi` merges it into each aggregation row.
    parsed("SELECT category, 2 AS COL2 FROM t GROUP BY category").rowInvariantProjection shouldBe
    ListMap("COL2" -> 2L)
    parsed("SELECT category, 'x' AS lbl FROM t GROUP BY category").rowInvariantProjection shouldBe
    ListMap("lbl" -> "x")
    parsed("SELECT category, 2.5 AS ratio FROM t GROUP BY category").rowInvariantProjection shouldBe
    ListMap("ratio" -> 2.5d)
    parsed("SELECT category, true AS b FROM t GROUP BY category").rowInvariantProjection shouldBe
    ListMap("b" -> true)

    // 🔴 An UN-ALIASED constant is keyed by the COMPUTED alias, because that is the column the
    // result side asks for. Keying it by the rendered name (`"2"`) left the requested `__c2` column
    // NULL and invented a bogus `2` column beside it -- measured end to end through the real
    // `rowNormalizer`. `Field.outputName` is the single definition both sides read.
    parsed("SELECT category, 2 FROM t GROUP BY category").rowInvariantProjection.keys.toSeq shouldBe
    Seq("__c2")
    parsed(
      "SELECT category, 'x' FROM t GROUP BY category"
    ).rowInvariantProjection.keys.toSeq shouldBe
    Seq("__c2")

    // A statement with no constant projects nothing, so the merge is a no-op by construction.
    parsed("SELECT category FROM t GROUP BY category").rowInvariantProjection shouldBe empty
  }

  it should "project SQL NULL as a PRESENT null column, and never a row-variant value" in {
    // `Null` is kept in the allow-list: it is a constant, and excluding it would be an exception
    // with no principle behind it. ⚠️ Recorded limitation: a projected `NULL` column is
    // indistinguishable downstream from a column that failed to project, because `rowNormalizer`
    // null-fills a missing requested column with exactly the same value. Every OTHER constant
    // carries a real value, so the ambiguity is confined to the one case where null is correct.
    val nulled = parsed("SELECT category, NULL AS n FROM t GROUP BY category")
    nulled.rowInvariantProjection.keys.toSeq shouldBe Seq("n")
    Option(nulled.rowInvariantProjection("n")) shouldBe None

    // Row-VARIANT and unbound shapes are excluded: a projected `?` would be a NULL column by
    // another route, which is exactly what this fix exists to prevent. (Asserted WITHOUT a GROUP
    // BY, because with one these statements are rejected -- see the test below -- and there would
    // be no AST to inspect.)
    parsed("SELECT category, RANDOM AS r FROM t").rowInvariantProjection shouldBe empty
    parsed("SELECT category, ? AS p FROM t").rowInvariantProjection shouldBe empty
  }

  it should "still declare a constant whose name an aggregation also claims" in {
    // Precedence is settled downstream, not here: the constants SEED the aggregation recursion, so
    // anything Elasticsearch computes overwrites them (asserted in `ElasticConversionSpec`). The
    // AST layer therefore just reports what the statement declares.
    parsed(
      "SELECT category, 2 AS m, MAX(amount) AS m FROM t GROUP BY category"
    ).rowInvariantProjection shouldBe ListMap("m" -> 2L)
  }

  it should "reject a GROUP BY over an unbound query parameter" in {
    // Scripting every nameless bucket is right, but `params.paramValue` is never bound: Painless
    // reads the missing key as null and Elasticsearch answers ZERO groups with HTTP 200. A silent
    // empty result is worse than the loud rejection.
    rejects("SELECT ? AS p FROM t GROUP BY p", "query parameter")
  }

  it should "still reject a ROW-VARIANT pseudo-value beside a GROUP BY" in {
    // The classification must not leak: `RANDOM` re-evaluates per document and `?` is unbound, so
    // neither is a constant a group can be said to carry.
    rejects("SELECT category, RANDOM AS r FROM t GROUP BY category", "Non-aggregated fields")
    rejects("SELECT category, ? AS p FROM t GROUP BY category", "Non-aggregated fields")
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
      "SELECT SUM(1) AS COL, 2 AS COL2 FROM t GROUP BY 2",
      // 🔴 The ORDER BY half: a sort resolved onto a bare numeric literal rendered as that literal
      // and re-parsed as a POSITION. `... GROUP BY flag ORDER BY flag` became
      // `ORDER BY 2` -> `ORDER BY SUM(amount)` (a bucket sort silently becoming a metric sort), and
      // `... GROUP BY 1 ORDER BY 1` rendered a statement that no longer parses at all.
      "SELECT 2 AS flag, SUM(amount) AS s FROM t GROUP BY flag ORDER BY flag ASC",
      "SELECT 2 AS flag FROM t GROUP BY 1 ORDER BY 1 ASC",
      "SELECT 2 AS flag FROM t GROUP BY flag ORDER BY flag DESC"
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

  it should "classify row-invariance on the AST, as an allow-list" in {
    // ⚠️ The spelling is the bare `RANDOM`, not `RANDOM()`: MEASURED, `RANDOM()` does not parse at
    // all ("end of input expected"), so a `RANDOM()` row would pass for the wrong reason.
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT RANDOM AS r FROM t").select.fields.head.identifier
    ) shouldBe false
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT ? AS p FROM t").select.fields.head.identifier
    ) shouldBe false
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT 2 AS n FROM t").select.fields.head.identifier
    ) shouldBe true
    // A real column is not a constant, however it is spelled.
    SingleSearch.isRowInvariantLiteral(
      parsed("SELECT category FROM t").select.fields.head.identifier
    ) shouldBe false
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

  it should "re-emit the alias the bucket was resolved through, and re-parse to the same bucket" in {
    // The render keeps the spelling the statement used (`GROUP BY pays`), NOT the resolved column:
    // the alias is what `Bucket.aliasItem` reads back to the same SELECT item, it is what the user
    // wrote, and it matches what `origin/main` rendered -- so an existing MATERIALIZED VIEW whose
    // definition groups by an alias does not see its stored render change and rebuild.
    val rendered = Parser("SELECT country AS pays FROM t GROUP BY pays")
      .map(_.sql)
      .getOrElse(fail("did not parse"))
    rendered should include("GROUP BY pays")
    Parser(rendered).map(_.sql).getOrElse("") shouldBe rendered
    bucketTriples(rendered) shouldBe bucketTriples("SELECT country AS pays FROM t GROUP BY country")
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
