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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.operator.{ALL, ANY, DIFF, GT, NE, NOT}
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.2 — WHERE subqueries: grammar, AST, validation, correlation, the render fixed point
  * WITH its rendered text, and the neighbour productions the new alternation order could move.
  */
class WhereSubquerySpec extends AnyFlatSpec with Matchers {

  private def parse(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case Right(other) => fail(s"[$sql] expected SingleSearch, got ${other.getClass.getSimpleName}")
    case Left(e)      => fail(s"[$sql] rejected: ${e.msg}")
  }

  private def reasonOf(sql: String): String =
    Parser(sql).swap.toOption.map(_.msg).getOrElse("<parsed>")

  private def whereOf(sql: String): Criteria =
    parse(sql).where.flatMap(_.criteria).getOrElse(fail(s"[$sql] no WHERE"))

  /** `ParserTotalitySpec.rejects` (private there): no throw, THEN `Left`, THEN NOT the boundary
    * catch, THEN the reasons.
    *
    * 🔴 The third assertion is what keeps every case FALSIFIABLE. Since `Parser.apply` carries a
    * `NonFatal` boundary catch (#250), a restored `throw` still yields a `Left` whose message
    * CONTAINS the same words — `noException` + `isLeft` + `include` all stay green. Only the
    * `InternalParseFailure` prefix tells the two apart.
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(
      s"[$sql] msg=[$msg] - this rejection must come from an `err(...)` in the grammar or from a " +
      "`validate()`, never from `Parser.apply`'s NonFatal boundary catch. "
    ) {
      msg should not startWith Parser.InternalParseFailure
    }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  // ── grammar + AST ────────────────────────────────────────────────────────────────────────────

  "IN (SELECT ...)" should "build an InSubquery whose body is the inner SingleSearch" in {
    val s = parse(
      "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')"
    )
    val in = s.where.flatMap(_.criteria).get.asInstanceOf[InSubquery]
    in.identifier.name shouldBe "customer_id"
    in.maybeNot shouldBe None
    in.inner.map(_.from.tables.head.name) shouldBe Some("customers")
    in.correlatedRefs shouldBe Nil
    s.hasWhereSubqueries shouldBe true
    s.whereSubqueries should have size 1
    // AD-8 — an uncorrelated WHERE subquery executes in core at every venue: PASSTHROUGH.
    s.relationalClosureRequired shouldBe false
    relationalClosureRequired(s) shouldBe false
    s.returnsRows shouldBe true // the outer statement's SHAPE is unchanged
  }

  it should "parse a body whose LAST clause is WHERE or HAVING (story 22.1's AD-2b scanner)" in {
    // Without the depth-aware `whereCriteria` these are `Left("Unbalanced parentheses")`: the inner
    // clause swallows the subquery's own `)`.
    parse(
      "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')"
    )
    parse("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE x = 1)")
    parse("SELECT id FROM t WHERE a IN (SELECT a FROM u GROUP BY a HAVING COUNT(*) > 1)")
    parse("SELECT id FROM t WHERE a IN (SELECT a FROM u WHERE (b = 1 OR c = 2) AND d = 3)")
    // and the top level keeps its contract: an unmatched OPENING paren is still loud
    rejects("SELECT a FROM t WHERE (b = 1", "Unbalanced parentheses")
    // a stray `)` stays a rejection; its wording is `phrase`'s and is never pinned
    rejects("SELECT a FROM t WHERE a = 1)")
  }

  it should "carry NOT in the spelling the grammar accepts" in {
    whereOf("SELECT id FROM t WHERE a NOT IN (SELECT a FROM u)")
      .asInstanceOf[InSubquery]
      .maybeNot shouldBe Some(NOT)
    // MEASURED at Task 0 on `origin/main`: the prefix form `WHERE NOT a IN (…)` does NOT parse for
    // a literal list either (`end of input expected`), so this story neither gains nor loses it.
    rejects("SELECT a FROM t WHERE NOT a IN (1, 2)")
    rejects("SELECT a FROM t WHERE NOT a IN (SELECT a FROM u)")
  }

  "EXISTS (SELECT ...)" should "build an ExistsSubquery, negated by NOT" in {
    whereOf("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u WHERE x = 1)") shouldBe
    a[ExistsSubquery]
    whereOf("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u)")
      .asInstanceOf[ExistsSubquery]
      .maybeNot shouldBe Some(NOT)
  }

  "<op> (SELECT ...)" should "build a ScalarSubquery for every comparison operator" in {
    Seq("=", "<>", "!=", ">=", ">", "<=", "<").foreach { op =>
      withClue(op) {
        whereOf(s"SELECT id FROM t WHERE amount $op (SELECT AVG(amount) FROM t)") shouldBe
        a[ScalarSubquery]
      }
    }
    whereOf("SELECT id FROM t WHERE amount > (SELECT AVG(amount) FROM t)")
      .asInstanceOf[ScalarSubquery]
      .operator shouldBe GT
  }

  "Quantified forms" should "reduce = ANY / = SOME to IN and <> ALL / != ALL to NOT IN (PD-3)" in {
    val in = whereOf("SELECT id FROM t WHERE a IN (SELECT a FROM u)").asInstanceOf[InSubquery]
    whereOf("SELECT id FROM t WHERE a = ANY (SELECT a FROM u)") shouldBe in
    whereOf("SELECT id FROM t WHERE a = SOME (SELECT a FROM u)") shouldBe in
    whereOf("SELECT id FROM t WHERE a <> ALL (SELECT a FROM u)")
      .asInstanceOf[InSubquery]
      .maybeNot shouldBe Some(NOT)
    whereOf("SELECT id FROM t WHERE a != ALL (SELECT a FROM u)")
      .asInstanceOf[InSubquery]
      .maybeNot shouldBe Some(NOT)
  }

  /** 🔴 Lead ruling OQ-4 (2026-09-14) REVERSES PD-1: these SHIP. The `err` naming a MIN/MAX rewrite
    * is deleted, not retargeted.
    */
  it should "build a QuantifiedSubquery for the ten remaining combinations" in {
    val cases = Seq(
      ">"  -> ANY,
      ">=" -> ANY,
      "<"  -> ANY,
      "<=" -> ANY,
      ">"  -> ALL,
      ">=" -> ALL,
      "<"  -> ALL,
      "<=" -> ALL
    )
    cases.foreach { case (op, q) =>
      val c = whereOf(s"SELECT id FROM t WHERE a $op $q (SELECT a FROM u)")
      withClue(s"$op $q: ") {
        c shouldBe a[QuantifiedSubquery]
        c.asInstanceOf[QuantifiedSubquery].quantifier shouldBe q
        c.asInstanceOf[QuantifiedSubquery].universal shouldBe (q == ALL)
      }
    }
    whereOf("SELECT id FROM t WHERE a = ALL (SELECT a FROM u)")
      .asInstanceOf[QuantifiedSubquery]
      .universal shouldBe true
    whereOf("SELECT id FROM t WHERE a <> ANY (SELECT a FROM u)")
      .asInstanceOf[QuantifiedSubquery]
      .operator shouldBe NE
    whereOf("SELECT id FROM t WHERE a != ANY (SELECT a FROM u)")
      .asInstanceOf[QuantifiedSubquery]
      .operator shouldBe DIFF
  }

  it should "canonicalise SOME to ANY so the two spellings share one node" in {
    whereOf("SELECT id FROM t WHERE a > SOME (SELECT a FROM u)") shouldBe
    whereOf("SELECT id FROM t WHERE a > ANY (SELECT a FROM u)")
  }

  it should "not reserve ANY / SOME: a column named any or some still parses" in {
    parse("SELECT id FROM t WHERE any = 1")
    parse("SELECT some FROM t WHERE some > 1")
    parse("SELECT a FROM t WHERE a = any")
  }

  // ── validation (AC 3) ────────────────────────────────────────────────────────────────────────

  "Validation" should "require exactly one projected column in IN, quantified and scalar position" in {
    rejects("SELECT id FROM t WHERE a IN (SELECT a, b FROM u)", "exactly one column", "got 2")
    rejects("SELECT id FROM t WHERE a IN (SELECT * FROM u)", "exactly one column, not *")
    rejects("SELECT id FROM t WHERE a > (SELECT a, b FROM u LIMIT 1)", "exactly one column")
    rejects("SELECT id FROM t WHERE a > ANY (SELECT a, b FROM u)", "exactly one column")
    parse("SELECT id FROM t WHERE EXISTS (SELECT a, b FROM u)") // EXISTS projects anything
    parse("SELECT id FROM t WHERE EXISTS (SELECT * FROM u)")
  }

  it should "require a single-row scalar body: metric-only or LIMIT 1" in {
    parse("SELECT id FROM t WHERE a > (SELECT MAX(a) FROM u)")
    parse("SELECT id FROM t WHERE a > (SELECT a FROM u ORDER BY a DESC LIMIT 1)")
    rejects("SELECT id FROM t WHERE a > (SELECT a FROM u)", "must return a single row")
    rejects("SELECT id FROM t WHERE a > (SELECT MAX(a) FROM u GROUP BY b)", "single row")
    // a QUANTIFIED body is a LIST, so the single-row rule must NOT apply to it
    parse("SELECT id FROM t WHERE a > ANY (SELECT a FROM u)")
  }

  it should "decline UNION ALL and FROM-less bodies (PD-7)" in {
    rejects(
      "SELECT id FROM t WHERE a IN (SELECT a FROM u UNION ALL SELECT a FROM v)",
      "UNION ALL inside a WHERE subquery"
    )
    rejects("SELECT id FROM t WHERE a IN (SELECT 1)", "must read a table")
    rejects("SELECT id FROM t WHERE EXISTS (SELECT 1)", "must read a table")
  }

  it should "validate the body one level down" in {
    // `Parser.apply` validates the TOP level only, so the node's own validate() must run the body's
    rejects("SELECT id FROM t WHERE a IN (SELECT a, b FROM u GROUP BY a)", "Non-aggregated")
  }

  it should "reject an aggregate as the left operand (aggregates are not allowed in WHERE)" in {
    rejects(
      "SELECT id FROM t WHERE COUNT(a) IN (SELECT a FROM u)",
      "Aggregate functions are not allowed in WHERE"
    )
  }

  it should "decline a subquery in HAVING, CASE and JOIN ON (AD-6)" in {
    rejects(
      "SELECT a, COUNT(*) AS n FROM t GROUP BY a HAVING n IN (SELECT n FROM u)",
      "not supported in HAVING"
    )
    rejects(
      "SELECT CASE WHEN a IN (SELECT a FROM u) THEN 1 ELSE 0 END AS f FROM t",
      "CASE WHEN condition"
    )
    // the EXISTING `On` rule already refuses it — no new arm was added, and the substring below is
    // what tells a later change that a DIFFERENT rule started firing
    rejects(
      "SELECT o.id FROM orders o JOIN customers c ON o.cid IN (SELECT id FROM x)",
      "ON clause",
      "equality"
    )
  }

  it should "decline a subquery in a materialized view and in a watcher input" in {
    rejects(
      "CREATE MATERIALIZED VIEW v AS SELECT a FROM t WHERE a IN (SELECT a FROM u)",
      "cannot run the inner query"
    )
    rejects(
      """CREATE WATCHER my_watcher AS
        | EVERY 5 MINUTES
        | FROM t WHERE a IN (SELECT a FROM u) WITHIN 2 MINUTES
        | ALWAYS DO
        | log_action AS LOG "Watcher triggered" AT INFO
        | END""".stripMargin,
      "cannot carry a WHERE subquery"
    )
  }

  // ── correlation (AC 4) ───────────────────────────────────────────────────────────────────────

  /** 🔴 Story 22.3b — THE FLIP. These three statements were `Left`s carrying "Correlated subquery …
    * story 22.3" until this story deleted `SubqueryCriteria.commonChecks`' correlated arm. They now
    * PARSE, and `relationalClosureRequired` routes them to the relational engine; every venue
    * WITHOUT the engine refuses them at `SearchApi.resolveWithSchema` / `CoreDqlExtension`
    * (`RelationalClosureGuardSpec`, `CoreDqlExtensionSpec`, `ReplGatewayIntegrationSpec` 6d).
    *
    * The contract that did NOT change is the one that matters: a correlated subquery is still never
    * EXECUTED as if it were self-contained.
    */
  "A correlated subquery" should "parse and route to the relational engine (story 22.3b)" in {
    val routed = Seq(
      "SELECT o.id FROM orders o WHERE o.cid IN " +
      "(SELECT c.id FROM customers c WHERE c.region = o.region)",
      "SELECT o.id FROM orders o WHERE EXISTS (SELECT 1 FROM customers c WHERE c.id = o.cid)",
      // the outer INDEX name is a correlation name too
      "SELECT id FROM orders WHERE cid IN (SELECT id FROM customers WHERE region = orders.region)",
      // a nested body's reference to the OUTERMOST alias — DEEP
      "SELECT o.id FROM orders o WHERE o.cid IN (SELECT c.id FROM customers c WHERE c.k IN " +
      "(SELECT k FROM z WHERE z.r = o.region))"
    )
    routed.foreach { sql =>
      withClue(s"[$sql] ") {
        val s = parse(sql)
        s.hasCorrelatedSubqueries shouldBe true
        s.relationalClosureRequired shouldBe true
        s.from.relationalClosureRequired shouldBe false // the FROM half is untouched
        relationalClosureRequired(s) shouldBe true // the package fn reads the SingleSearch val
      }
    }
  }

  it should "honour shadowing: an alias the inner statement declares is the INNER one" in {
    val s = parse(
      "SELECT o.id FROM orders o WHERE o.cid IN (SELECT o.id FROM customers o WHERE o.r = 'EU')"
    )
    // the control that keeps the flip above non-vacuous: shadowing is NOT correlation, so this
    // statement must stay ES-native.
    s.hasCorrelatedSubqueries shouldBe false
    s.relationalClosureRequired shouldBe false
  }

  it should "keep an UNcorrelated WHERE subquery off the engine (story 22.2's PASSTHROUGH)" in {
    val s = parse(
      "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')"
    )
    s.hasWhereSubqueries shouldBe true
    s.hasCorrelatedSubqueries shouldBe false
    s.relationalClosureRequired shouldBe false
  }

  /** 🔴 Story 22.3 (PD-2) — a derived body reading an ENCLOSING correlation name is SQL:1999
    * `LATERAL`, refused at every venue and on every Elasticsearch major because the refusal lives
    * in the `sql` module.
    *
    * The FROM/JOIN-position arm shipped with story 22.1; the second row is story 22.3's OWN
    * extension of the walk to a derived table nested inside a WHERE-subquery BODY. Before it, that
    * row was caught by the correlated arm this story DELETED — so without the extension the
    * deletion would have turned a loud rejection into an accepted statement.
    */
  "A LATERAL-shaped derived table" should "be rejected by validate() with the ANSI message" in {
    rejects(
      "SELECT c.id FROM customers c JOIN (SELECT o.cid FROM orders o WHERE o.cid = c.id) d ON d.cid = c.id",
      "LATERAL is not supported",
      "derived table cannot reference an outer alias",
      "'c.id'",
      "derived table 'd'"
    )
    rejects(
      "SELECT c.id FROM customers c WHERE c.id IN " +
      "(SELECT x FROM (SELECT o.cid AS x FROM orders o WHERE o.cid = c.id) d)",
      "LATERAL is not supported",
      "derived table cannot reference an outer alias",
      "derived table 'd'"
    )
  }

  it should "leave a derived body that names ONLY its own sources alone" in {
    parse("SELECT d.total FROM (SELECT amount AS total FROM t) d WHERE d.total > 1")
    ()
  }

  /** The canonical render of every correlated form, MEASURED (story 22.3 Task 0 row 6) — the text
    * and the fixed point, so a render that could not be re-read fails here rather than at a
    * customer's cluster. `= ANY` canonicalises to `IN`, as story 22.2 already pinned.
    */
  private val correlatedRenders = Seq(
    "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)" ->
    "SELECT c.id FROM customers AS c WHERE EXISTS (SELECT 1 FROM orders AS o WHERE o.customer_id = c.id)",
    "SELECT c.id FROM customers c WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)" ->
    "SELECT c.id FROM customers AS c WHERE NOT EXISTS (SELECT 1 FROM orders AS o WHERE o.customer_id = c.id)",
    "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.customer_id FROM orders o WHERE o.amount > c.credit_limit)" ->
    "SELECT c.id FROM customers AS c WHERE c.id IN (SELECT o.customer_id FROM orders AS o WHERE o.amount > c.credit_limit)",
    "SELECT c.id FROM customers c WHERE c.id NOT IN (SELECT o.customer_id FROM orders o WHERE o.amount > c.credit_limit)" ->
    "SELECT c.id FROM customers AS c WHERE c.id NOT IN (SELECT o.customer_id FROM orders AS o WHERE o.amount > c.credit_limit)",
    "SELECT c.id FROM customers c WHERE c.credit_limit > (SELECT AVG(o.amount) FROM orders o WHERE o.customer_id = c.id)" ->
    "SELECT c.id FROM customers AS c WHERE c.credit_limit > (SELECT AVG(o.amount) FROM orders AS o WHERE o.customer_id = c.id)",
    "SELECT c.id FROM customers c WHERE c.id = ANY (SELECT o.customer_id FROM orders o WHERE o.amount > c.credit_limit)" ->
    "SELECT c.id FROM customers AS c WHERE c.id IN (SELECT o.customer_id FROM orders AS o WHERE o.amount > c.credit_limit)",
    "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.cid FROM orders o WHERE o.amount > (SELECT AVG(r.amount) FROM refunds r WHERE r.cid = c.id))" ->
    "SELECT c.id FROM customers AS c WHERE c.id IN (SELECT o.cid FROM orders AS o WHERE o.amount > (SELECT AVG(r.amount) FROM refunds AS r WHERE r.cid = c.id))"
  )

  correlatedRenders.foreach { case (in, text) =>
    it should s"[22.3b] parse, route and round-trip [$in]" in {
      val stmt = parse(in)
      stmt.hasCorrelatedSubqueries shouldBe true
      stmt.relationalClosureRequired shouldBe true
      stmt.sql shouldBe text
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  it should "assume a BARE name is the inner column at parse time (PD-2)" in {
    val s = parse("SELECT id FROM orders WHERE cid IN (SELECT id FROM customers WHERE r = 'EU')")
    s.whereSubqueries.head.correlatedRefs shouldBe Nil
  }

  // ── render: the fixed point AND the text (constraint 2) ──────────────────────────────────────

  private val renders = Seq(
    "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')" ->
    "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU')",
    "SELECT id FROM t WHERE a NOT IN (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a NOT IN (SELECT a FROM u)",
    "SELECT id FROM t WHERE a = ANY (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a IN (SELECT a FROM u)",
    "SELECT id FROM t WHERE a = SOME (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a IN (SELECT a FROM u)",
    "SELECT id FROM t WHERE a <> ALL (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a NOT IN (SELECT a FROM u)",
    "SELECT id FROM t WHERE a > ANY (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a > ANY (SELECT a FROM u)",
    "SELECT id FROM t WHERE a > SOME (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a > ANY (SELECT a FROM u)",
    "SELECT id FROM t WHERE a <= ALL (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a <= ALL (SELECT a FROM u)",
    "SELECT id FROM t WHERE a = ALL (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a = ALL (SELECT a FROM u)",
    "SELECT id FROM t WHERE a <> ANY (SELECT a FROM u)" ->
    "SELECT id FROM t WHERE a <> ANY (SELECT a FROM u)",
    "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE x = 1)" ->
    "SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u WHERE x = 1)",
    "SELECT id FROM t WHERE amount > (SELECT AVG(amount) FROM t)" ->
    "SELECT id FROM t WHERE amount > (SELECT AVG(amount) FROM t)",
    "SELECT id FROM t WHERE a = 1 AND b IN (SELECT b FROM u) OR EXISTS (SELECT 1 FROM v)" ->
    "SELECT id FROM t WHERE a = 1 AND b IN (SELECT b FROM u) OR EXISTS (SELECT 1 FROM v)",
    "SELECT id FROM t WHERE a IN (SELECT a FROM u WHERE b IN (SELECT b FROM v))" ->
    "SELECT id FROM t WHERE a IN (SELECT a FROM u WHERE b IN (SELECT b FROM v))",
    "DELETE FROM t WHERE id IN (SELECT id FROM u WHERE flag = true)" ->
    "DELETE FROM t WHERE id IN (SELECT id FROM u WHERE flag = true)"
  )

  renders.foreach { case (in, text) =>
    it should s"render [$in] as [$text] and re-parse to an EQUAL ast" in {
      val stmt = Parser(in).toOption.getOrElse(fail(s"[$in] rejected: ${reasonOf(in)}"))
      stmt.sql shouldBe text
      // 🔴 `== Right(stmt)`, never `isRight`: a LOSSY render satisfies the fixed point on both
      // sides. The TEXT assertion above and this one only bite together.
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  // ── neighbour pins (Task 0 row 6; the alternation order is what could move them) ─────────────

  /** Every render here was MEASURED on the branch base before the grammar changed and is asserted
    * byte-for-byte, so a subquery production that started winning one of these inputs fails here
    * rather than in a customer's query.
    */
  private val neighbours = Seq(
    "SELECT a FROM t WHERE a = (b + 1)"           -> "SELECT a FROM t WHERE a = (b + 1)",
    "SELECT a FROM t WHERE (a = 1 AND b = 2)"     -> "SELECT a FROM t WHERE (a = 1 AND b = 2)",
    "SELECT a FROM t WHERE a IN ('x', 'y')"       -> "SELECT a FROM t WHERE a IN ('x','y')",
    "SELECT a FROM t WHERE a IN (1, 2)"           -> "SELECT a FROM t WHERE a IN (1,2)",
    "SELECT a FROM t WHERE a IN (1.5, 2.5)"       -> "SELECT a FROM t WHERE a IN (1.5,2.5)",
    "SELECT a FROM t WHERE a NOT IN (1, 2)"       -> "SELECT a FROM t WHERE a NOT IN (1,2)",
    "SELECT a FROM t WHERE a = 1 AND b IN (2, 3)" -> "SELECT a FROM t WHERE a = 1 AND b IN (2,3)",
    "SELECT a FROM t WHERE a = b"                 -> "SELECT a FROM t WHERE a = b",
    "SELECT a FROM t WHERE a > 1"                 -> "SELECT a FROM t WHERE a > 1",
    "SELECT a FROM t WHERE exists_flag = true"    -> "SELECT a FROM t WHERE exists_flag = true",
    "SELECT a FROM t WHERE a = any"               -> "SELECT a FROM t WHERE a = any",
    "SELECT a FROM t WHERE any = 1"               -> "SELECT a FROM t WHERE any = 1",
    "SELECT some FROM t WHERE some > 1"           -> "SELECT some FROM t WHERE some > 1",
    "SELECT a FROM t WHERE ABS(a) > 1"            -> "SELECT a FROM t WHERE ABS(a) > 1",
    "SELECT a FROM t WHERE MATCH(title) AGAINST('x')" ->
    "SELECT a FROM t WHERE MATCH (title) AGAINST ('x')",
    "SELECT a FROM t WHERE a BETWEEN 1 AND 2" -> "SELECT a FROM t WHERE a BETWEEN 1 AND 2",
    "SELECT a FROM t WHERE id = 1 AND CHILD(x = 2 AND y = 3 AND z = 4)" ->
    "SELECT a FROM t WHERE id = 1 AND CHILD(x = 2 AND y = 3 AND z = 4)",
    "SELECT a FROM t WHERE CASE WHEN a = 1 THEN 1 ELSE 0 END = 1" ->
    "SELECT a FROM t WHERE CASE WHEN a = 1 THEN 1 ELSE 0 END = 1"
  )

  "Neighbouring productions" should "not move" in {
    neighbours.foreach { case (sql, expected) =>
      val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
      withClue(s"[$sql] ") { stmt.sql shouldBe expected }
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
  }
}
