package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.Alias
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.1 — derived tables in FROM and JOIN: grammar, AST shape, column scope, the LATERAL
  * refusal, the render fixed point WITH its rendered text, and the neighbour productions that must
  * not move.
  *
  * Every rendered string below was MEASURED against the implementation, never guessed.
  */
class DerivedTableSpec extends AnyFlatSpec with Matchers {

  private def parse(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case Right(other) => fail(s"[$sql] expected SingleSearch, got ${other.getClass.getSimpleName}")
    case Left(e)      => fail(s"[$sql] rejected: ${e.msg}")
  }

  private def reasonOf(sql: String): String = Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  /** `ParserTotalitySpec`'s `rejects`, copied rather than shared (it is private there): no throw,
    * THEN `Left`, THEN not the boundary catch, THEN the reason.
    *
    * 🔴 The third assertion is what makes every case falsifiable. Once `Parser.apply` carries a
    * `NonFatal` boundary catch (#250) a restored `throw` still yields a `Left` CONTAINING the same
    * reason, so `isLeft` + `include` alone can never go red for it.
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  // ── grammar + AST ──────────────────────────────────────────────────────────────────────────

  "FROM (SELECT ...) AS alias" should "build a Table whose derived table IS its name" in {
    val s = parse("SELECT COL FROM (SELECT 1 AS COL) AS SUBQUERY")
    val t = s.from.mainTable
    t.name shouldBe "SUBQUERY"
    t.tableAlias shouldBe None
    t.parts shouldBe Nil
    t.derived.map(_.alias) shouldBe Some(Alias("SUBQUERY"))
    t.derived.map(_.query.getClass.getSimpleName) shouldBe Some("FromlessSelect")
    t.derived.flatMap(_.outputNames) shouldBe Some(Seq("COL"))
    s.from.tableAliases shouldBe scala.collection.immutable.ListMap("SUBQUERY" -> "SUBQUERY")
    s.from.aliasesToTable shouldBe scala.collection.immutable.ListMap("SUBQUERY" -> "SUBQUERY")
    s.from.hasDerivedTables shouldBe true
    s.from.enrichmentRequired shouldBe false
    s.from.relationalClosureRequired shouldBe true
    s.relationalClosureRequired shouldBe true
    relationalClosureRequired(s) shouldBe true
    derivedTablesPresent(s) shouldBe true
    // 🔴 the ALIAS, never an index — see the guards in core (AD-5)
    s.sources shouldBe Seq("SUBQUERY")
  }

  it should "accept the alias without AS, quoted and backticked" in {
    parse("SELECT a FROM (SELECT a FROM t) d").from.mainTable.name shouldBe "d"
    parse("""SELECT a FROM (SELECT a FROM t) "d"""").from.mainTable.name shouldBe "d"
    parse("SELECT a FROM (SELECT a FROM t) `d`").from.mainTable.name shouldBe "d"
  }

  it should "take a SingleSearch, a UNION ALL and a FROM-less body" in {
    parse("SELECT a FROM (SELECT a FROM t WHERE a > 1) d").from.mainTable.derived
      .map(_.query.getClass.getSimpleName) shouldBe Some("SingleSearch")
    parse("SELECT a FROM (SELECT a FROM t UNION ALL SELECT a FROM u) d").from.mainTable.derived
      .map(_.query.getClass.getSimpleName) shouldBe Some("MultiSearch")
    parse("SELECT n FROM (SELECT 1 AS n) d").from.mainTable.derived
      .map(_.query.getClass.getSimpleName) shouldBe Some("FromlessSelect")
  }

  /** 🔴 AD-2b's own row. Without the depth-aware `whereCriteria` these two are rejected with
    * "Unbalanced parentheses": the inner clause's bare `end` swallows the body's own `)`.
    */
  it should "accept a body whose LAST clause is WHERE or HAVING (AD-2b)" in {
    parse("SELECT a FROM (SELECT a FROM t WHERE x = 1) d")
    parse("SELECT a FROM (SELECT a, COUNT(*) AS c FROM t GROUP BY a HAVING c > 1) d")
  }

  "JOIN (SELECT ...) AS alias ON ..." should "put the derived table on StandardJoin.source" in {
    val s = parse(
      "SELECT o.id, d.total FROM orders o JOIN " +
      "(SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid) AS d ON o.id = d.cid"
    )
    val sj = s.from.mainTable.joins.head.asInstanceOf[StandardJoin]
    sj.source shouldBe a[DerivedTable]
    sj.source.name shouldBe "d"
    sj.alias shouldBe None
    sj.parts shouldBe Nil
    s.from.joinSourceKeys should contain("d")
    s.from.tableAliases("d") shouldBe "d"
    s.from.aliasesToTable("d") shouldBe "d"
    // `enrichmentRequired` keeps its meaning — "a JOIN leg exists" — with no edit
    s.from.enrichmentRequired shouldBe true
    s.from.relationalClosureRequired shouldBe true
    s.from.derivedTables.keySet shouldBe Set("d")
  }

  Seq("INNER JOIN", "LEFT JOIN", "LEFT OUTER JOIN", "RIGHT JOIN", "FULL OUTER JOIN").foreach { jt =>
    it should s"accept every join type: $jt" in {
      parse(
        s"SELECT o.id FROM orders o $jt (SELECT cid FROM x) d ON o.id = d.cid"
      ).from.mainTable.joins.head
        .asInstanceOf[StandardJoin]
        .source shouldBe a[DerivedTable]
    }
  }

  it should "accept CROSS JOIN (SELECT ...) d without ON (story 21.2 AD-7)" in {
    parse("SELECT o.id FROM orders o CROSS JOIN (SELECT cid FROM x) d")
    ()
  }

  it should "nest to any depth and mix with UNNEST and a comma list" in {
    parse("SELECT a FROM (SELECT a FROM (SELECT a FROM (SELECT a FROM t) x) y) z")
    parse("SELECT a FROM t, (SELECT a FROM u) d")
    parse("SELECT a FROM (SELECT a, items FROM t) d JOIN UNNEST(d.items) i")
    ()
  }

  // ── column scope (AD-4) ────────────────────────────────────────────────────────────────────

  "An outer reference" should "resolve against the derived table's output names" in {
    val s = parse("SELECT d.total FROM (SELECT amount AS total FROM t) d WHERE d.total > 1")
    val id = s.select.fields.head.identifier
    id.name shouldBe "total"
    id.table shouldBe Some("d")
    id.tableAlias shouldBe Some("d")
  }

  it should "be rejected when the derived table does not project it (qualified)" in {
    rejects(
      "SELECT d.amount FROM (SELECT amount AS total FROM t) d",
      "Column 'amount' is not projected by derived table 'd'",
      "it projects: total"
    )
  }

  it should "be rejected when un-qualified over a single derived source" in {
    rejects(
      "SELECT nope FROM (SELECT 1 AS COL) AS d",
      "Column 'nope' is not projected by derived table 'd'"
    )
  }

  it should "be checked in a JOIN ON clause too, and accept struct access into a projection" in {
    rejects(
      "SELECT o.id FROM orders o JOIN (SELECT cid FROM x) d ON o.id = d.nope",
      "Column 'nope' is not projected by derived table 'd'"
    )
    // the HEAD segment `items` IS projected — a dotted remainder is struct access, not a column
    parse("SELECT d.items.name FROM (SELECT items FROM t) d")
    ()
  }

  it should "enforce the AD-1 invariant on a programmatic construction" in {
    val dt = DerivedTable(parse("SELECT a FROM t"), Alias("d"))
    Table("d", derived = Some(dt)).validate() shouldBe Right(())
    Table("x", derived = Some(dt)).validate().isLeft shouldBe true
    Table("d", Some(Alias("y")), derived = Some(dt)).validate().isLeft shouldBe true
    // 🔴 The JOIN row needs an ON, or it is UNFALSIFIABLE: with `on = None` and
    // `joinType = None` the NEXT arm of `StandardJoin.validate` answers "requires an ON clause"
    // whether or not the AD-1 arm exists. The message is pinned for the same reason.
    val on = parse("SELECT a FROM t JOIN u ON t.a = u.a").from.mainTable.joins.head
      .asInstanceOf[StandardJoin]
      .on
    on should not be empty
    StandardJoin(dt, None, on, alias = Some(Alias("y"))).validate() match {
      case Left(msg) => msg should include("A derived table owns its alias")
      case Right(_)  => fail("a derived JOIN source carrying its own alias must be rejected")
    }
    StandardJoin(dt, None, on).validate() shouldBe Right(())
  }

  it should "not trip on an outer SELECT alias, a literal, an ordinal, * or COUNT(*)" in {
    parse("SELECT COL AS c FROM (SELECT 1 AS COL) AS d ORDER BY c")
    parse("SELECT 2 AS two FROM (SELECT 1 AS COL) AS d")
    parse("SELECT COL FROM (SELECT 1 AS COL) AS d ORDER BY 1")
    parse("SELECT * FROM (SELECT a FROM t) d")
    parse("SELECT COUNT(*) AS n FROM (SELECT a FROM t) d")
    ()
  }

  it should "treat a SELECT * body as opaque (PD-3)" in {
    val s = parse("SELECT MAX(1) AS TblMax FROM (SELECT * FROM bi_events bi_events) bi_events")
    s.from.mainTable.derived.flatMap(_.outputNames) shouldBe None
    // never checked, because nothing here knows the mapping — a wrong name fails loudly in the
    // relational engine's binder instead of being guessed at
    parse("SELECT anything FROM (SELECT * FROM t) d")
    // a QUALIFIED star must be opaque too. `Identifier.update` normalises `e.*` to `name = "*"`
    // with `tableAlias = Some("e")`, so `projected`'s `name == "*"` test sees it — MEASURED. Were
    // it ever to keep the dotted spelling, the projection would read `Seq("e.*")` and EVERY outer
    // reference would be rejected, which is why this is pinned rather than assumed.
    val q = parse("SELECT anything FROM (SELECT e.* FROM t e) d")
    q.from.mainTable.derived.flatMap(_.outputNames) shouldBe None
    ()
  }

  it should "reject a duplicate correlation name — including the key-collapsing shape" in {
    rejects(
      "SELECT a FROM t d JOIN (SELECT a FROM u) d ON d.a = d.a",
      "Alias 'd' is used by more than one source in FROM"
    )
    // 🔴 the shape `tableAliases` CANNOT see: the ListMap has already collapsed the key, so alias
    // `b` is silently gone by the time anyone reads the map. Tableau aliases a derived table with
    // the inner table's OWN name, so this is the default spelling, not a corner case.
    rejects(
      "SELECT b.amount FROM bi_events b JOIN (SELECT category FROM bi_events) bi_events " +
      "ON b.category = bi_events.category",
      "Alias 'bi_events' is used by more than one source in FROM"
    )
  }

  // ── the LATERAL refusal (amendment from story 22.3's review) ───────────────────────────────

  "A derived body reading an enclosing alias" should "be rejected as LATERAL, in JOIN position" in {
    rejects(
      "SELECT c.id FROM customers c JOIN (SELECT o.cid FROM orders o WHERE o.cid = c.id) d " +
      "ON d.cid = c.id",
      // 🔴 story 22.3 moved "LATERAL" to the FRONT of this message: `GatewayApi.excerpt` caps a
      // rejection reason at 200 characters and elides the MIDDLE, so the old wording reached the
      // REPL user without the word that names the construct. Contract unchanged, order fixed.
      "LATERAL is not supported",
      "derived table cannot reference an outer alias",
      "'c.id'",
      "derived table 'd'"
    )
  }

  it should "be rejected in FROM position, against a later comma-list table" in {
    rejects(
      "SELECT d.x FROM (SELECT o.cid AS x FROM orders o WHERE o.cid = customers.id) d, customers",
      // 🔴 story 22.3 moved "LATERAL" to the FRONT of this message: `GatewayApi.excerpt` caps a
      // rejection reason at 200 characters and elides the MIDDLE, so the old wording reached the
      // REPL user without the word that names the construct. Contract unchanged, order fixed.
      "LATERAL is not supported",
      "derived table cannot reference an outer alias"
    )
  }

  it should "not fire for a body naming only its OWN sources" in {
    parse("SELECT d.total FROM (SELECT amount AS total FROM t WHERE t.amount > 1) d")
    ()
  }

  /** 🔴 REGRESSION PIN — the FALSE POSITIVE. The LATERAL walk accumulated the enclosing names as it
    * descended, so a derived body's OWN alias counted as "enclosing" for a WHERE subquery nested
    * inside that body. MEASURED on `origin/main` at `36f0884e`, this statement was rejected with
    * *"LATERAL is not supported: … 'o.id' inside derived table 'd' reads the enclosing FROM"* — and
    * `o` is declared by `d`'s body. It is an ordinary correlated subquery, entirely inside `d`.
    *
    * The two rows ABOVE are the other direction: a reference that genuinely escapes the derived
    * table is still refused, with the same message.
    */
  it should "not fire for a correlated WHERE subquery INSIDE the derived body" in {
    val s = parse(
      "SELECT d.id FROM (SELECT o.id FROM orders o WHERE EXISTS " +
      "(SELECT 1 FROM returns r WHERE r.oid = o.id)) d"
    )
    s.relationalClosureRequired shouldBe true
    ()
  }

  // ── derived-table column scope is CASE-INSENSITIVE (story 22.3 regression) ──────────────────

  /** 🔴 REGRESSION PIN — `SubqueryScope.resolve` narrowed derived-projection matching from
    * `equalsIgnoreCase` to `Seq.contains` when it replaced the planner's own helper, and
    * `derivedScopeCheck` reads that resolution. An SQL identifier is case-insensitive.
    */
  "A derived-table column reference" should "match the projection case-insensitively" in {
    parse("SELECT d.Total FROM (SELECT amount AS total FROM t) d")
    parse("SELECT D.total FROM (SELECT amount AS total FROM t) d")
    parse("SELECT Total FROM (SELECT amount AS total FROM t) d")
    // the other direction: a name the derived table really does not project is STILL refused
    rejects(
      "SELECT d.nope FROM (SELECT amount AS total FROM t) d",
      "Column 'nope' is not projected by derived table 'd'",
      "it projects: total"
    )
  }

  /** 🔴 REGRESSION PIN — an UNNEST leg used to change the answer for a bare name, because
    * `SubqueryScope.resolve`'s lone-source rule counted it as a source (the retired planner helper
    * counted `TableInfo`s, and an UNNEST is part of one). The statement-level consequence: a
    * derived table beside an UNNEST stopped being scope-checked at all, so the two rows below
    * DISAGREED with their UNNEST-free twins — one accepted a column nothing projects.
    */
  "An UNNEST leg" should "not change what a bare name resolves to" in {
    // accepted twin: an OPAQUE body accepts every reference, UNNEST or not
    parse("SELECT a FROM (SELECT * FROM x) d")
    parse("SELECT a FROM (SELECT * FROM x) d JOIN UNNEST(d.items) i")
    // rejected twin: the UNNEST form used to be silently accepted (the name resolved to nothing,
    // so nothing checked it)
    val reason = "Column 'a' is not projected by derived table 'd'"
    rejects("SELECT a FROM (SELECT b FROM x) d", reason, "it projects: b")
    rejects("SELECT a FROM (SELECT b FROM x) d JOIN UNNEST(d.items) i", reason, "it projects: b")
    // and the UNNEST alias itself still resolves as a qualifier
    parse("SELECT i.name FROM (SELECT items FROM x) d JOIN UNNEST(d.items) i")
    ()
  }

  // ── rejections that MUST be ours (AD-3 / AD-6) ─────────────────────────────────────────────

  "A derived table" should "require an alias (PD-1)" in {
    rejects("SELECT * FROM (SELECT * FROM t)", "A derived table requires an alias")
    rejects(
      """SELECT * FROM (SELECT * FROM "elastic"."bi_events") WHERE ROWNUM <= 1""",
      "requires an alias"
    )
  }

  it should "require a SELECT body — FROM (t) x stays rejected, with OUR message" in {
    rejects("SELECT a FROM (t) x", "A derived table body must be a SELECT")
    rejects("SELECT a FROM (SHOW TABLES) x", "A derived table body must be a SELECT")
  }

  it should "be refused by DELETE and by a watcher input" in {
    rejects(
      "DELETE FROM (SELECT id FROM t) d WHERE id = 1",
      "DELETE cannot target a derived table"
    )
    rejects(
      """CREATE WATCHER my_watcher AS
        | EVERY 5 MINUTES
        | FROM (SELECT a FROM t) d WITHIN 2 MINUTES
        | ALWAYS DO
        | log_action AS LOG "Watcher triggered" AT INFO
        | END""".stripMargin,
      "A watcher input cannot search a derived table"
    )
  }

  it should "be refused by CREATE MATERIALIZED VIEW (AD-6)" in {
    rejects(
      "CREATE MATERIALIZED VIEW v AS SELECT a FROM (SELECT a FROM t) d",
      "MATERIALIZED VIEW over a derived table"
    )
  }

  it should "validate its body (an inner rule is not skipped one level down)" in {
    rejects("SELECT a FROM (SELECT a, b FROM t GROUP BY a) d", "GROUP BY")
  }

  // ── render: fixed point AND text ───────────────────────────────────────────────────────────

  private val renders = Seq(
    "SELECT COL FROM (SELECT 1 AS COL) AS SUBQUERY" ->
    "SELECT COL FROM (SELECT 1 AS COL) AS SUBQUERY",
    "SELECT `COL` FROM (SELECT 1 AS `COL`) AS `SUBQUERY`" ->
    """SELECT "COL" FROM (SELECT 1 AS "COL") AS "SUBQUERY"""",
    "SELECT a FROM (SELECT a FROM t) d" ->
    "SELECT a FROM (SELECT a FROM t) AS d",
    "SELECT MAX(1) AS TblMax FROM ( SELECT * FROM `elastic`.`bi_events` `bi_events` ) `bi_events`" ->
    """SELECT MAX(1) AS TblMax FROM (SELECT * FROM "elastic"."bi_events" AS "bi_events") AS "bi_events"""",
    "SELECT o.id FROM orders o LEFT JOIN (SELECT cid FROM x) d ON o.id = d.cid" ->
    "SELECT o.id FROM orders AS o  LEFT JOIN (SELECT cid FROM x) AS d ON o.id = d.cid",
    "SELECT a FROM (SELECT a FROM (SELECT a FROM t) x) y" ->
    "SELECT a FROM (SELECT a FROM (SELECT a FROM t) AS x) AS y",
    "SELECT a FROM (SELECT a FROM t UNION ALL SELECT a FROM u) d" ->
    "SELECT a FROM (SELECT a FROM t UNION ALL SELECT a FROM u) AS d",
    "SELECT a FROM (SELECT a FROM t WHERE x = 1) d" ->
    "SELECT a FROM (SELECT a FROM t WHERE x = 1) AS d",
    "SELECT a FROM t, (SELECT a FROM u) d" ->
    "SELECT a FROM t,(SELECT a FROM u) AS d",
    "SELECT a FROM (SELECT a, items FROM t) d JOIN UNNEST(d.items) i" ->
    "SELECT a FROM (SELECT a, items FROM t) AS d JOIN UNNEST(d.items) AS i"
  )

  renders.foreach { case (in, text) =>
    it should s"render [$in] as its canonical text and re-parse to an equal AST" in {
      val stmt = Parser(in).toOption.getOrElse(fail(s"[$in] rejected: ${reasonOf(in)}"))
      stmt.sql shouldBe text
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  // ── neighbour pins ─────────────────────────────────────────────────────────────────────────

  "Neighbouring productions" should "not move" in {
    Seq(
      "SELECT (a + 1) AS s FROM t",
      "SELECT a FROM t WHERE (a = 1 AND b = 2)",
      "SELECT a FROM t JOIN UNNEST(t.items) i",
      "SELECT a FROM t1, t2",
      """SELECT category FROM "elastic"."bi_events" "bi_events""""
    ).foreach { sql =>
      val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
  }
}
