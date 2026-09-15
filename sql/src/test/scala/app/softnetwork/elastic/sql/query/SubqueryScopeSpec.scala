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

import app.softnetwork.elastic.sql.{GenericIdentifier, Identifier}
import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.2 — the correlation detector itself, on parsed scopes.
  *
  * `WhereSubquerySpec` pins what the PARSER does with a correlated statement; this pins the walk
  * story 22.3 consumes as THE detector, so a change that keeps the rejections but loses the
  * shadowing rule or the nested walk fails here.
  */
class SubqueryScopeSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] $other")
  }

  /** The correlated statements cannot be obtained through `Parser` (they are rejected at
    * `validate()`), so the body and the outer scope are taken apart the way `update` sees them: the
    * body is parsed on its own — where its qualifiers resolve against NOTHING — and measured
    * against the outer statement's names.
    */
  private def refs(body: String, outer: String): Seq[String] =
    SubqueryScope.correlatedReferences(single(body), single(outer)).map(_.name)

  "correlationNames" should "carry every alias, index name and UNNEST alias of the FROM" in {
    val s = single("SELECT o.id FROM orders o")
    SubqueryScope.correlationNames(s) should contain allOf ("orders", "o")
  }

  /** 🔴 REGRESSION PIN (story 22.3, found by the independent review). `correlationNames` used to
    * read `from.tableAliases` directly, and that `ListMap` is keyed by TABLE, so it has ALREADY
    * collapsed two sources sharing an `aliasKey`. MEASURED on `FROM orders o JOIN UNNEST(o.orders)
    * AS i`: `tableAliases` is `ListMap(orders -> i)`, alias `o` was GONE, and a subquery body's
    * `o.region` was therefore NOT reported as correlated — the statement did not route, and
    * Elasticsearch read `o.region` as an object path: zero rows, HTTP 200.
    *
    * The fix is that `correlationNames` now derives from `scopeOf`, which reads the LOSSLESS
    * `aliasesToTable`. Same class as story BIDC-8 / softclient4es-arrow#144.
    */
  it should "keep BOTH aliases when two sources share an alias-map key (the collapse trap)" in {
    val unnest = single("SELECT o.id FROM orders o JOIN UNNEST(o.orders) AS i")
    unnest.from.tableAliases.keySet should have size 1 // the collapse, still there by design
    SubqueryScope.correlationNames(unnest) should contain allOf ("o", "i", "orders")
    val selfJoin = single("SELECT a.id FROM orders a JOIN orders b ON a.id = b.id")
    SubqueryScope.correlationNames(selfJoin) should contain allOf ("a", "b", "orders")
    // and the consequence that matters: the reference IS reported as correlated
    refs(
      "SELECT id FROM customers WHERE region = o.region",
      "SELECT o.id FROM orders o JOIN UNNEST(o.orders) AS i"
    ) shouldBe Seq("o.region")
  }

  "A qualified reference to an outer name" should "be reported" in {
    refs(
      "SELECT c.id FROM customers c WHERE c.region = o.region",
      "SELECT id FROM orders o"
    ) shouldBe
    Seq("o.region")
    // the outer INDEX name is a correlation name too
    refs(
      "SELECT id FROM customers WHERE region = orders.region",
      "SELECT id FROM orders"
    ) shouldBe Seq("orders.region")
  }

  it should "be silent when the inner statement declares the same name (innermost wins)" in {
    refs("SELECT o.id FROM customers o WHERE o.region = 'EU'", "SELECT id FROM orders o") shouldBe
    Nil
  }

  it should "be silent for a bare name (SQL resolves it innermost-first — PD-2)" in {
    refs("SELECT id FROM customers WHERE region = 'EU'", "SELECT id FROM orders o") shouldBe Nil
  }

  it should "be silent for a dotted path whose head is in NEITHER scope (an object field)" in {
    refs("SELECT id FROM customers WHERE address.city = 'X'", "SELECT id FROM orders o") shouldBe
    Nil
  }

  "A body nested one level deeper" should "still see the OUTERMOST scope" in {
    refs(
      "SELECT c.id FROM customers c WHERE c.k IN (SELECT k FROM z WHERE z.r = o.region)",
      "SELECT id FROM orders o"
    ) shouldBe Seq("o.region")
  }

  "The bare-name message" should "name the two indices and the remedy that makes it EXECUTABLE" in {
    // Story 22.3b: a QUALIFIED outer reference now routes to the relational engine, so the ONE
    // correlation shape still refused in core is the BARE one — and the remedy is to qualify it.
    val bare = SubqueryScope.bareCorrelatedMessage("vip", "orders", "customers")
    bare should include("'vip' is not a column of 'orders'")
    bare should include("is a column of 'customers'")
    bare should include("must be QUALIFIED")
  }

  // ══ Story 22.3 (AD-1) — the scope model ══════════════════════════════════════════════════════

  import SubqueryScope._

  /** 🔴 An UNVALIDATED parse (lead ruling OQ-8). Several rows below are statements `validate()`
    * REFUSES — a LATERAL-shaped derived body — so `Parser.apply` cannot feed them, and a test-side
    * re-implementation of the reader would drift from `apply`'s. `Parser.single`'s action still
    * runs `.update()`, so `correlatedRefs` and every resolved qualifier are populated.
    */
  private def outerOf(sql: String): SingleSearch = Parser.parseUnvalidated(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] $other")
  }

  private def firstBody(s: SingleSearch): SingleSearch =
    s.whereSubqueries.headOption
      .flatMap(_.inner)
      .getOrElse(fail(s"[${s.sql}] no WHERE-subquery body"))

  /** The RAW correlated shape — `tableAlias` and `table` both empty, the qualifier still in the
    * name — which is exactly what `GenericIdentifier.update` leaves behind for an unresolved
    * qualifier (story 22.2 AD-3).
    */
  private def ref(name: String): Identifier = GenericIdentifier(name)

  behavior of "SubqueryScope.scopeOf / chain"

  it should "speak the alias-map KEY language for every source" in {
    val s = outerOf(
      "SELECT o.id FROM orders o JOIN \"prod_eu\".orders p ON o.cid = p.id " +
      "JOIN UNNEST(o.items) AS i WHERE o.x IN (SELECT y FROM z)"
    )
    val sc = scopeOf(s)
    sc.sources.map(_.alias) shouldBe Seq("o", "p", "i")
    // story 21.2 AD-6: an AMBIGUOUS bare name keys by the QUALIFIED reference
    sc.sources.collect { case ps: PlainSource => ps.key } should contain("prod_eu.orders")
    sc.sources.collect { case ps: PlainSource => ps.key }.foreach { k =>
      withClue(s"key [$k] must be an alias-map key ") {
        s.from.tableAliases.keySet should contain(k)
      }
    }
    sc.names should contain allOf ("o", "orders", "p", "prod_eu.orders", "i")
  }

  it should "build the chain innermost first" in {
    val outer =
      outerOf(
        "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cid = c.id)"
      )
    val ch = chain(firstBody(outer), Seq(outer))
    ch.map(_.depth) shouldBe Seq(0, 1)
    ch.head.sources.map(_.alias) shouldBe Seq("o")
    ch(1).sources.map(_.alias) shouldBe Seq("c")
  }

  behavior of "SubqueryScope.resolve - qualified names, innermost first"

  it should "resolve an inner-declared alias at depth 0 and an outer one at depth 1 (correlated)" in {
    val outer = outerOf(
      "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)"
    )
    val body = firstBody(outer)
    val ch = chain(body, Seq(outer))
    resolve(ref("c.id"), ch) shouldBe Resolved(1, PlainSource("customers", "c"), "id")
    // the inner operand: `Identifier.update` already set tableAlias = Some("o")
    val inner =
      body.referencedIdentifiers.find(_.tableAlias.contains("o")).getOrElse(fail("no o.*"))
    resolve(inner, ch) shouldBe Resolved(0, PlainSource("orders", "o"), "customer_id")
  }

  it should "let an inner alias SHADOW the outer (innermost wins) - and agree with the detector" in {
    val outer = outerOf(
      "SELECT o.id FROM orders o WHERE o.cid IN (SELECT o.id FROM customers o WHERE o.region = 'EU')"
    )
    outer.whereSubqueries.head.correlatedRefs shouldBe Nil // the story-22.2 row
    val body = firstBody(outer)
    val region = body.referencedIdentifiers.find(_.name == "region").getOrElse(fail("no region"))
    resolve(region, chain(body, Seq(outer))) should matchPattern {
      case Resolved(0, PlainSource("customers", "o"), "region") =>
    }
  }

  it should "resolve two levels out (depth 2) and agree with the detector at the outermost update" in {
    val outer = outerOf(
      "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.cid FROM orders o WHERE o.amount > " +
      "(SELECT AVG(r.amount) FROM refunds r WHERE r.cid = c.id))"
    )
    val middle = firstBody(outer)
    val innermost = firstBody(middle)
    resolve(ref("c.id"), chain(innermost, Seq(middle, outer))) shouldBe
    Resolved(2, PlainSource("customers", "c"), "id")
    // AD-2's agreement property: every reference the DETECTOR calls correlated resolves OUTWARD.
    // The two cannot drift without reddening this.
    correlatedReferences(middle, outer).foreach { id =>
      withClue(s"[${id.name}] ") {
        resolve(id, chain(middle, Seq(outer))) should matchPattern {
          case Resolved(d, _, _) if d >= 1 =>
        }
      }
    }
    outer.hasCorrelatedSubqueries shouldBe true // DEEP
    // 🔴 MEASURED, and it corrects the spec: correlation is RELATIVE TO A SCOPE CHAIN. Seen on its
    // own, the middle statement declares `o`/`orders` and its innermost body reads `c.id` — a name
    // the middle does not declare EITHER, so the middle's own node records nothing and
    // `middle.hasCorrelatedSubqueries` is FALSE. Nothing is lost: story 22.2's walk accumulates
    // scopes downward, so the reference IS reported on the OUTERMOST node, which is where routing
    // is decided. ⚠️ A consumer (the story-22.3b planner) that asks an ISOLATED body this question
    // gets the wrong answer — it must read the top-level node's `correlatedRefs`, or walk with the
    // scopes accumulated, exactly as `correlatedReferences` does.
    middle.hasCorrelatedSubqueries shouldBe false
    // 🔴 story 22.2's walk accumulates scopes, so the INNERMOST body's `c.id` is reported on the
    // TOP-LEVEL node: `correlatedRefs` means "this subtree reads outside its own body" (AD-2).
    outer.correlatedSubqueries.map(_.correlatedRefs.map(_.name)) shouldBe Seq(Seq("c.id"))
    // and a MIDDLE-only correlation is reported outward too — the middle body cannot run
    // ES-natively either, which is exactly why the whole statement must route.
    val midOnly = outerOf(
      "SELECT c.id FROM customers c WHERE c.id IN (SELECT o.cid FROM orders o WHERE o.amount > " +
      "(SELECT AVG(r.amount) FROM refunds r WHERE r.cid = o.cid))"
    )
    midOnly.hasCorrelatedSubqueries shouldBe true
    midOnly.whereSubqueries.head.correlatedRefs.map(_.name) shouldBe Seq("o.cid")
  }

  /** The CONVERSE of the agreement property above, and the one that can actually FAIL: every
    * reference the RESOLVER places in an enclosing scope must have been reported by the DETECTOR.
    * The forward direction alone is satisfied by a detector that reports nothing.
    */
  it should "report EVERY reference the resolver resolves outward (detector completeness)" in {
    val shapes = Seq(
      "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.cid = c.id)",
      "SELECT o.id FROM orders o JOIN UNNEST(o.orders) AS i WHERE o.id IN " +
      "(SELECT cid FROM customers WHERE name = o.region)",
      "SELECT a.id FROM orders a JOIN orders b ON a.id = b.id WHERE a.id IN " +
      "(SELECT cid FROM customers WHERE r = b.region)",
      "SELECT id FROM \"prod_eu\".orders p WHERE id IN (SELECT cid FROM customers WHERE r = p.region)"
    )
    shapes.foreach { sql =>
      val outer = outerOf(sql)
      val body = firstBody(outer)
      val ch = chain(body, Seq(outer))
      val reported = correlatedReferences(body, outer).map(_.name).toSet
      body.referencedIdentifiers.foreach { id =>
        resolve(id, ch) match {
          case Resolved(d, _, _) if d >= 1 =>
            withClue(
              s"[$sql] '${id.name}' resolves at depth $d but the detector did not report it "
            ) {
              reported should contain(id.name)
            }
          case _ => ()
        }
      }
    }
  }

  it should "answer Unresolved for a head in no scope (an object path at parse time)" in {
    val outer =
      outerOf("SELECT id FROM orders address WHERE id IN (SELECT id FROM t WHERE zip.code = 'X')")
    resolve(ref("zip.code"), chain(firstBody(outer), Seq(outer))) shouldBe Unresolved
  }

  behavior of "SubqueryScope.resolve - un-qualified names (story 22.4 AD-4, subsumed)"

  private val w5005 = outerOf(
    "SELECT country AS country, sum(amount) AS \"SUM(amount)\" FROM bi_events " +
    "JOIN (SELECT country AS country__, sum(amount) AS mme_inner__ FROM bi_events GROUP BY country " +
    "ORDER BY sum(amount) DESC LIMIT 10) AS series_limit ON country = country__ " +
    "GROUP BY country ORDER BY \"SUM(amount)\" DESC LIMIT 10000"
  )

  it should "row 1: send a name exactly one derived table projects to that derived table" in {
    resolve(ref("country__"), Seq(scopeOf(w5005))) shouldBe
    Resolved(0, DerivedSource("series_limit", Some(Seq("country__", "mme_inner__"))), "country__")
  }

  it should "row 2: send a name no derived table projects to the SOLE plain index" in {
    resolve(ref("country"), Seq(scopeOf(w5005))) shouldBe
    Resolved(0, PlainSource("bi_events", "bi_events"), "country")
    resolve(ref("amount"), Seq(scopeOf(w5005))) should matchPattern {
      case Resolved(0, PlainSource("bi_events", _), "amount") =>
    }
  }

  it should "row 3: stay Ambiguous between two PLAIN legs, and beside an OPAQUE derived leg" in {
    resolve(
      ref("total"),
      Seq(scopeOf(outerOf("SELECT total FROM orders o JOIN customers c ON o.cid = c.id")))
    ) shouldBe Ambiguous
    resolve(
      ref("total"),
      Seq(scopeOf(outerOf("SELECT total FROM orders o JOIN (SELECT * FROM x) AS d ON o.id = d.id")))
    ) shouldBe Ambiguous
    // the control that makes the `opaque` clause non-vacuous: once the derived leg's projection IS
    // known, `total` cannot be its column, so the sole plain leg owns it.
    resolve(
      ref("total"),
      Seq(scopeOf(outerOf("SELECT total FROM orders o JOIN (SELECT a FROM x) AS d ON o.id = d.a")))
    ) shouldBe Resolved(0, PlainSource("orders", "o"), "total")
  }

  it should "row 4: resolve BOTH operands of ON country = country to the SAME source" in {
    // the planner's same-alias guard case: a self-comparison must not be read as a join key, and
    // it is the RESOLVER's job to make both operands land on one source so the guard can see it.
    val s = outerOf(
      "SELECT country FROM bi_events JOIN (SELECT country, SUM(amount) AS s FROM bi_events " +
      "GROUP BY country) AS d ON country = country"
    )
    resolve(ref("country"), Seq(scopeOf(s))) should matchPattern {
      case Resolved(0, DerivedSource("d", _), "country") =>
    }
  }

  it should "let a LONE source own every bare name, opaque or not (story 22.4's fourth rule)" in {
    resolve(ref("zzz"), Seq(scopeOf(outerOf("SELECT a FROM (SELECT * FROM x) AS d")))) should
    matchPattern { case Resolved(0, DerivedSource("d", None), "zzz") => }
  }

  /** 🔴 REGRESSION PIN — the THIRD narrowing of the same story-22.3 retirement. `resolve`'s
    * scaladoc already promised that an `UnnestSource` *"takes part in NO un-qualified rule"*, but
    * rule (0) (*"a lone source owns every bare name"*) counted EVERY source, and `scopeOf` emits
    * the UNNEST as a source of its own — where the retired planner helper counted `TableInfo`s and
    * an UNNEST is PART OF one. MEASURED on this branch before the fix: `FROM (SELECT * FROM x) d
    * JOIN UNNEST(d.items) i` answered `Ambiguous` for every bare name, while the SAME statement
    * without the UNNEST resolved to `d`. Code and its own prose disagreed.
    */
  it should "keep an UNNEST source out of the LONE-source rule, as the contract says" in {
    val opaqueBesideUnnest =
      outerOf("SELECT a FROM (SELECT * FROM x) d JOIN UNNEST(d.items) i")
    resolve(ref("a"), Seq(scopeOf(opaqueBesideUnnest))) shouldBe
    Resolved(0, DerivedSource("d", None), "a")
    // the control that says the UNNEST was the whole difference: the same shape without it
    resolve(ref("a"), Seq(scopeOf(outerOf("SELECT a FROM (SELECT * FROM x) d")))) shouldBe
    Resolved(0, DerivedSource("d", None), "a")

    // a bare name that IS the unnested column, projected by the derived table: it resolves to the
    // derived source, and that is the right owner — `d.items` is the ARRAY, `i` is the alias of
    // its ELEMENT. (The element's own columns are not representable: `UnnestSource.projection` is
    // `None` by construction, since nothing but a mapping could know them.)
    val unnested = outerOf("SELECT items FROM (SELECT items FROM x) d JOIN UNNEST(d.items) i")
    resolve(ref("items"), Seq(scopeOf(unnested))) shouldBe
    Resolved(0, DerivedSource("d", Some(Seq("items"))), "items")

    // 🔴 THE OTHER DIRECTION — dropping the UNNEST from the COUNT never invents an owner: with two
    // real sources beside it, a bare name is still refused.
    resolve(
      ref("a"),
      Seq(
        scopeOf(
          outerOf("SELECT a FROM orders o JOIN customers c ON o.id = c.id JOIN UNNEST(o.items) i")
        )
      )
    ) shouldBe Ambiguous
    resolve(
      ref("a"),
      Seq(
        scopeOf(
          outerOf(
            "SELECT a FROM orders o JOIN (SELECT * FROM x) d ON o.id = d.id JOIN UNNEST(o.items) i"
          )
        )
      )
    ) shouldBe Ambiguous

    // and the UNNEST is still a SOURCE everywhere else: a correlation name (story 22.2's collapse
    // pin depends on it) and a qualified target.
    correlationNames(opaqueBesideUnnest) should contain("i")
    scopeOf(opaqueBesideUnnest).byName("i") shouldBe Some(UnnestSource("i", "items"))
  }

  it should "never resolve a bare name OUTWARD (PD-2: assumed inner; the seam re-checks)" in {
    val outer =
      outerOf(
        "SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders WHERE vip = true)"
      )
    resolve(ref("vip"), chain(firstBody(outer), Seq(outer))) should matchPattern {
      case Resolved(0, PlainSource("orders", "orders"), "vip") =>
    }
  }

  /** 🔴 REGRESSION PIN — story 22.3 retired the planner-local `resolveUnqualified`, which compared
    * projections with `equalsIgnoreCase`, in favour of this resolver, which compared them with
    * `Seq.contains`. MEASURED on `origin/main` at `36f0884e`, both halves of the narrowing:
    *   - beside TWO plain legs, `Total` became `Ambiguous` (the reported *"Ambiguous column"*);
    *   - beside ONE plain leg it silently resolved to the PLAIN leg — the worse half, a wrong leg
    *     with no error at all.
    *
    * SQL identifiers are case-insensitive; a QUOTED one is matched the same way here, deliberately
    * (see `projects`' scaladoc: the projection side carries no quoting information, so an exact
    * rule could only be applied to one half). Nothing is rewritten — `Resolved.column` keeps the
    * caller's own spelling, pinned below.
    */
  it should "match a derived projection case-INSENSITIVELY, and keep the caller's spelling" in {
    val onePlain = outerOf(
      "SELECT Total FROM bi_events JOIN (SELECT SUM(amount) AS total FROM bi_events) AS d " +
      "ON bi_events.id = d.total"
    )
    resolve(ref("Total"), Seq(scopeOf(onePlain))) shouldBe
    Resolved(0, DerivedSource("d", Some(Seq("total"))), "Total")
    // a QUALIFIER is an identifier too: `D.total` names the source written `d`
    resolve(ref("D.total"), Seq(scopeOf(onePlain))) shouldBe
    Resolved(0, DerivedSource("d", Some(Seq("total"))), "total")
    // the control, unchanged: a name the derived table does NOT project still falls to the plain leg
    resolve(ref("other"), Seq(scopeOf(onePlain))) shouldBe
    Resolved(0, PlainSource("bi_events", "bi_events"), "other")

    val twoPlain = outerOf(
      "SELECT Total FROM orders o JOIN customers c ON o.id = c.id " +
      "JOIN (SELECT SUM(x) AS total FROM t) d ON o.id = d.total"
    )
    resolve(ref("Total"), Seq(scopeOf(twoPlain))) shouldBe
    Resolved(0, DerivedSource("d", Some(Seq("total"))), "Total")
    // 🔴 the other direction: a genuinely ambiguous bare name is STILL refused — this fix widens
    // the MATCH, never the rule that follows it
    resolve(ref("nope"), Seq(scopeOf(twoPlain))) shouldBe Ambiguous
  }

  behavior of "SubqueryScope.lateralReferences"

  it should "report a FROM-derived, a JOIN-derived and a WHERE-nested derived body reading an outer alias" in {
    lateralReferences(
      outerOf(
        "SELECT c.id FROM customers c JOIN (SELECT o.cid FROM orders o WHERE o.cid = c.id) d ON d.cid = c.id"
      )
    ).map(_.name) shouldBe Seq("c.id")
    lateralReferences(
      outerOf(
        "SELECT d.x FROM (SELECT o.cid AS x FROM orders o WHERE o.cid = customers.id) d, customers"
      )
    ).map(_.name) shouldBe Seq("customers.id")
    // 🔴 story 22.3's OWN extension of the walk. Before it, this shape was caught by the CORRELATED
    // arm of `SubqueryCriteria.commonChecks` — which 22.3b deletes — so without the extension the
    // deletion would have turned a loud rejection into an accepted statement.
    lateralReferences(
      outerOf(
        "SELECT c.id FROM customers c WHERE c.id IN (SELECT x FROM (SELECT o.cid AS x FROM orders o WHERE o.cid = c.id) d)"
      )
    ).map(_.name) shouldBe Seq("c.id")
    lateralReferences(
      outerOf("SELECT d.total FROM (SELECT amount AS total FROM t) d WHERE d.total > 1")
    ) shouldBe Nil
  }

  /** 🔴 REGRESSION PIN — the FALSE POSITIVE that made story 22.3's own AC 11 unreachable.
    *
    * The walk `lateralOffenders` used ACCUMULATED the enclosing names as it descended, so a derived
    * body's OWN alias was added to the "outer" set one level up and a correlated WHERE subquery
    * living entirely INSIDE the body was reported as LATERAL. MEASURED on `origin/main` at
    * `36f0884e`: the statement below was rejected with *"LATERAL is not supported: … 'o.id' inside
    * derived table 'd' reads the enclosing FROM"* — except `o` is declared by `d`'s body, not by
    * the enclosing FROM.
    *
    * Both directions are pinned here: this shape resolves to NOTHING, and every genuine LATERAL
    * shape above and below still reports its offender.
    */
  it should "NOT fire for a correlated subquery INSIDE the derived body (its own alias)" in {
    val ac11 =
      "SELECT d.id FROM (SELECT o.id FROM orders o WHERE EXISTS " +
      "(SELECT 1 FROM returns r WHERE r.oid = o.id)) d"
    lateralReferences(outerOf(ac11)) shouldBe Nil
    // …and the statement therefore PARSES, which is the acceptance criterion this closes.
    Parser(ac11) should matchPattern { case Right(_: SingleSearch) => }
    // it still needs the relational engine — the shape is accepted, not executed ES-natively
    Parser(ac11).toOption.collect { case s: SingleSearch => s.relationalClosureRequired } shouldBe
    Some(true)
  }

  /** The two shapes that must NOT be let through by the boundary above — the derived table is still
    * the boundary, at every depth.
    */
  it should "still fire when the reference escapes the derived table, at any depth" in {
    // a nested derived table two levels in, reading the OUTERMOST alias: attributed to the INNER
    // derived table, as the pairing promises
    lateralOffenders(
      outerOf(
        "SELECT d.y FROM customers c, (SELECT y FROM " +
        "(SELECT o.cid AS y FROM orders o WHERE o.cid = c.id) e) d"
      )
    ).map { case (alias, id) => alias -> id.name } shouldBe Seq("e" -> "c.id")
    // a WHERE subquery inside the body reading the DERIVED TABLE'S OWN name is still LATERAL: `d`
    // is declared by the ENCLOSING FROM, not by the body
    lateralReferences(
      outerOf("SELECT d.x FROM (SELECT x FROM t WHERE y IN (SELECT z FROM u WHERE u.k = d.k)) d")
    ).map(_.name) shouldBe Seq("d.k")
  }

  it should "name the derived table whose body reads the outer alias" in {
    val msg = SubqueryScope.lateralMessage(ref("c.id"), "d")
    msg should include("derived table cannot reference an outer alias")
    msg should include("'c.id'")
    msg should include("derived table 'd'")
    // 🔴 LATERAL must be in the first 120 characters: `GatewayApi.excerpt` caps a rejection reason
    // at 200 and keeps head(120) + "..." + tail(77), so anything in the middle never reaches the
    // user. MEASURED against real ES 8.18 — the previous wording lost exactly this word.
    msg should startWith("LATERAL is not supported")
    msg.indexOf("derived table cannot reference an outer alias") should be < 120
  }

  behavior of "SubqueryScope.unresolvedMessage"

  it should "name every scope it searched, innermost first" in {
    val outer =
      outerOf(
        "SELECT id FROM customers c WHERE id IN (SELECT cid FROM orders o WHERE zip.code = 'X')"
      )
    val body = firstBody(outer)
    val msg = SubqueryScope.unresolvedMessage(ref("zip.code"), chain(body, Seq(outer)), body.sql)
    msg should include("names no source in scope")
    msg should include("Scopes searched (innermost first)")
    msg should include("[0] o=orders")
    msg should include("[1] c=customers")
  }
}
