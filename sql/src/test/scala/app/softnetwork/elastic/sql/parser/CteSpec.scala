package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{Alias, NamePart}
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story 22.5 — non-recursive CTEs: the grammar, the substitution into story 22.1's derived tables,
  * column scope through 22.1/22.3's check, the render fixed point WITH rendered-text pins, the
  * rejections that must be OURS, and the neighbours (the DDL `WITH` families, `with` and
  * `recursive` as identifiers) that must not move.
  */
class CteSpec extends AnyFlatSpec with Matchers {

  private def parse(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case Right(other) => fail(s"[$sql] expected SingleSearch, got ${other.getClass.getSimpleName}")
    case Left(e)      => fail(s"[$sql] rejected: ${e.msg}")
  }

  private def reasonOf(sql: String): String = Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  /** Story 21.4's falsifiable rejection idiom. The `not startWith InternalParseFailure` assertion
    * is the load-bearing half: once `Parser.apply` carries a `NonFatal` boundary catch, a restored
    * `throw` still yields a `Left` whose message CONTAINS the same reason, so `isLeft` +
    * `include(...)` alone cannot tell a grammar rejection from an internal fault.
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  private val witness =
    "WITH monthly AS (SELECT category, SUM(amount) AS total FROM bi_events GROUP BY category) SELECT * FROM monthly"

  // -- grammar + AST ---------------------------------------------------------------------------

  "The corpus witness superset.flightsql.w6.006" should "parse into a marked derived table" in {
    val s = parse(witness)
    s.ctes.map(_.name) shouldBe Seq(NamePart("monthly", quoted = false))
    val t = s.from.mainTable
    t.name shouldBe "monthly"
    t.tableAlias shouldBe None
    t.parts shouldBe Nil
    val d = t.derived.getOrElse(fail("the CTE reference must be a derived table"))
    d.alias shouldBe Alias("monthly")
    d.cte shouldBe Some(NamePart("monthly", quoted = false))
    // ONE body value, shared by the WITH entry and the reference — the identity a materialise-once
    // optimisation would key on, and what makes "executed twice" an EXECUTOR choice, not an AST one.
    (d.query eq s.ctes.head.query) shouldBe true
    d.outputNames shouldBe Some(Seq("category", "total"))
    s.from.hasDerivedTables shouldBe true
    s.from.enrichmentRequired shouldBe false
    s.relationalClosureRequired shouldBe true
    relationalClosureRequired(s) shouldBe true
    ctesPresent(s) shouldBe true
    // The ALIAS, never an index (story 22.1 AD-1): nothing in core may hand this to Elasticsearch.
    s.sources shouldBe Seq("monthly")
    s.sql shouldBe witness // byte-identical render
    Parser(s.sql) shouldBe Right(s)
    s.validate() shouldBe Right(())
  }

  it should "keep a reference's own alias and normalise an alias equal to the CTE name" in {
    val aliased = parse("WITH m AS (SELECT a FROM t) SELECT q.a FROM m q")
    aliased.from.mainTable.name shouldBe "q"
    aliased.from.mainTable.derived.map(_.alias) shouldBe Some(Alias("q"))
    aliased.from.mainTable.derived.flatMap(_.cte) shouldBe Some(NamePart("m", quoted = false))
    aliased.from.tableAliases shouldBe ListMap("q" -> "q")
    aliased.from.derivedTables.keySet shouldBe Set("q")
    val id = aliased.select.fields.head.identifier
    id.name shouldBe "a"
    id.tableAlias shouldBe Some("q")
    // `table` is deliberately NOT asserted. It keeps the FIRST pass's key (`m`, the plain table the
    // reference was before substitution) because `Identifier.update` re-derives `table` only when
    // `parts.size > 1` — MEASURED, and accepted as a residual (lead ruling OQ-5) because every
    // consumer keys on `tableAlias`. `SubqueryScope.resolve` reads `tableAlias` FIRST, which is why
    // the aliased scope rejection below fires correctly.

    // An alias that merely repeats the CTE name is not information: the canonical form makes the
    // three spellings ONE AST, which is what lets the render be a fixed point for all of them.
    parse("WITH m AS (SELECT a FROM t) SELECT a FROM m AS m") shouldBe
    parse("WITH m AS (SELECT a FROM t) SELECT a FROM m")
    parse("""WITH "m" AS (SELECT a FROM t) SELECT a FROM "m" AS m""") shouldBe
    parse("""WITH "m" AS (SELECT a FROM t) SELECT a FROM "m"""")
  }

  it should "substitute a JOIN-position reference onto StandardJoin.source" in {
    val s = parse(
      "WITH d AS (SELECT cid, SUM(amount) AS total FROM orders GROUP BY cid) " +
      "SELECT o.id, d.total FROM orders o JOIN d ON o.id = d.cid"
    )
    val sj = s.from.mainTable.joins.head.asInstanceOf[StandardJoin]
    sj.source shouldBe a[DerivedTable]
    sj.source.asInstanceOf[DerivedTable].cte shouldBe Some(NamePart("d", quoted = false))
    // Story 22.1's invariant: a derived JOIN source OWNS its alias.
    sj.alias shouldBe None
    sj.parts shouldBe Nil
    s.from.enrichmentRequired shouldBe true
    s.from.derivedTables.keySet shouldBe Set("d")
    s.validate() shouldBe Right(())
  }

  it should "substitute EARLIER CTEs into a later body, recursively through literal derived bodies" in {
    val s = parse("WITH a AS (SELECT x FROM t), b AS (SELECT x FROM a WHERE x > 1) SELECT * FROM b")
    val bBody = s.ctes(1).query.asInstanceOf[SingleSearch]
    bBody.from.mainTable.derived.flatMap(_.cte) shouldBe Some(NamePart("a", quoted = false))
    // TRUE is what makes story 22.4's `planDerivedLeg` take its NESTED arm for this leg.
    bBody.from.relationalClosureRequired shouldBe true

    val nested = parse("WITH a AS (SELECT x FROM t) SELECT * FROM (SELECT x FROM a) d")
    // The OUTER source is a LITERAL derived table (no marker) whose BODY holds the CTE reference.
    nested.from.mainTable.derived.map(_.cte) shouldBe Some(None)
    nested.from.mainTable.derived
      .map(_.query)
      .collect { case i: SingleSearch => i }
      .flatMap(_.from.mainTable.derived.flatMap(_.cte)) shouldBe
    Some(NamePart("a", quoted = false))
  }

  it should "reference a CTE twice under two aliases, and leave an unreferenced CTE alone" in {
    val twice = parse("WITH m AS (SELECT k, n FROM t) SELECT a.k FROM m a JOIN m b ON a.k = b.k")
    twice.from.tables.map(_.name) shouldBe Seq("a")
    twice.from.joinSourceKeys shouldBe Set("b")
    twice.from.derivedTables.keySet shouldBe Set("a", "b")
    val bodies = twice.from.derivedTables.values.map(_.query).toSeq
    withClue("inline semantics: two legs, ONE body value ") {
      (bodies.head eq bodies(1)) shouldBe true
    }

    val unref = parse("WITH u AS (SELECT 1 AS x) SELECT a FROM t")
    unref.ctes should have size 1
    unref.from.hasDerivedTables shouldBe false
    // A WITH clause is the engine's ALWAYS: the arrow regex classifier keys on the leading token
    // and cannot count references, and the anti-drift property asserts the two agree.
    unref.relationalClosureRequired shouldBe true
    ctesPresent(unref) shouldBe true
  }

  it should "shadow an index of the same name, and never substitute a QUALIFIED reference" in {
    parse(
      "WITH bi_events AS (SELECT 1 AS x) SELECT x FROM bi_events"
    ).from.mainTable.derived shouldBe defined
    val qualified = parse("""WITH monthly AS (SELECT 1 AS x) SELECT a FROM "sch".monthly""")
    qualified.from.mainTable.derived shouldBe None
    qualified.from.mainTable.parts.size shouldBe 2
  }

  it should "attach the WITH list to the FIRST branch of a UNION ALL and substitute into every branch" in {
    Parser("WITH m AS (SELECT a FROM t) SELECT a FROM m UNION ALL SELECT a FROM m") match {
      case Right(ms: MultiSearch) =>
        ms.requests.head.ctes should have size 1
        ms.requests(1).ctes shouldBe Nil
        ms.requests.forall(_.from.hasDerivedTables) shouldBe true
        relationalClosureRequired(ms) shouldBe true
        ctesPresent(ms) shouldBe true
        ms.sql shouldBe "WITH m AS (SELECT a FROM t) SELECT a FROM m UNION ALL SELECT a FROM m"
        Parser(ms.sql) shouldBe Right(ms)
        ms.validate() shouldBe Right(())
      case other => fail(s"expected MultiSearch, got $other")
    }
  }

  it should "take a FROM-less body and a UNION ALL body" in {
    parse("WITH one AS (SELECT 1 AS x) SELECT x FROM one").ctes.head.query shouldBe a[
      FromlessSelect
    ]
    parse(
      "WITH u AS (SELECT a FROM t UNION ALL SELECT a FROM v) SELECT a FROM u"
    ).ctes.head.query shouldBe a[MultiSearch]
  }

  // -- column scope ----------------------------------------------------------------------------

  "An outer reference to a CTE" should "resolve against the CTE's projection and reject an un-projected column" in {
    parse("WITH m AS (SELECT amount AS total FROM t) SELECT m.total FROM m WHERE m.total > 1")
    rejects(
      "WITH m AS (SELECT amount AS total FROM t) SELECT m.amount FROM m",
      "Column 'amount' is not projected by derived table 'm'",
      "it projects: total"
    )
    // The ALIASED reference is checked too, and this is exactly where the stale `Identifier.table`
    // residual would have hidden the statement from the check: `SubqueryScope.resolve` reads
    // `tableAlias` first, which is what makes the aliased form reachable without a new fallback.
    rejects(
      "WITH m AS (SELECT amount AS total FROM t) SELECT q.amount FROM m q",
      "Column 'amount' is not projected by derived table 'q'"
    )
    // Chained CTEs stay checkable: `SELECT * FROM <one derived source>` forwards that source's
    // projection instead of going opaque.
    rejects(
      "WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT b.nope FROM b",
      "Column 'nope' is not projected by derived table 'b'",
      "it projects: x"
    )
    parse("WITH a AS (SELECT x FROM t), b AS (SELECT * FROM a) SELECT b.x FROM b")
    // `*` is never a derived-table reference, so it is never scope-checked.
    parse("WITH m AS (SELECT amount AS total FROM t) SELECT * FROM m")
  }

  it should "stay OPAQUE for a select list that is not exactly the bare star" in {
    // The AD-7 guard: `SELECT *, 1 AS extra FROM a` must NOT be typed without `extra`, or a legal
    // `b.extra` would be rejected. Opaque means the scope check declines, i.e. BOTH names pass.
    parse("WITH a AS (SELECT x FROM t), b AS (SELECT *, 1 AS extra FROM a) SELECT b.extra FROM b")
    parse("WITH a AS (SELECT x FROM t), b AS (SELECT *, 1 AS extra FROM a) SELECT b.nope FROM b")
  }

  it should "reject a duplicate correlation name through story 22.1's sequence check" in {
    rejects(
      "WITH m AS (SELECT k FROM t) SELECT k FROM m JOIN m ON m.k = m.k",
      "Alias 'm' is used by more than one source"
    )
  }

  // -- rejections that MUST be ours ------------------------------------------------------------

  "A WITH clause" should "refuse RECURSIVE, a column list, a duplicate, a self reference and a forward reference" in {
    rejects(
      "WITH RECURSIVE a AS (SELECT 1 AS x) SELECT * FROM a",
      "WITH RECURSIVE is not supported"
    )
    rejects(
      "WITH a (x, y) AS (SELECT 1 AS x, 2 AS y) SELECT * FROM a",
      "CTE 'a' declares a column list",
      "alias the columns"
    )
    rejects(
      "WITH a AS (SELECT 1 AS x), a AS (SELECT 2 AS x) SELECT * FROM a",
      "CTE 'a' is defined more than once"
    )
    rejects("WITH a AS (SELECT x FROM a) SELECT * FROM a", "CTE 'a' references itself")
    rejects(
      "WITH a AS (SELECT x FROM b), b AS (SELECT x FROM t) SELECT * FROM a",
      "CTE 'a' references CTE 'b', which is defined later"
    )
    rejects(
      """WITH "s"."a" AS (SELECT 1 AS x) SELECT * FROM a""",
      "A CTE name must be a single unqualified name"
    )
    // `cteBody`'s OWN err: `derivedTableBodyInner` carries none, and `FromParser.derivedTable`'s
    // names the wrong construct.
    rejects("WITH a AS (t) SELECT * FROM a", "A CTE body must be a SELECT")
  }

  it should "descend into an embedded WHERE-subquery body (the CTE must not be read as an INDEX)" in {
    // Story 22.2 landed first, so the `Right` branch is LIVE: a parsed `IN (SELECT …)` whose walk
    // is EMPTY, or whose inner main table is not MARKED, is the forgotten-descent signal — the
    // silent-wrong-answer mode (`c` read as an index) this pin exists to catch.
    val s = parse("WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE k IN (SELECT id FROM c)")
    val embedded = CteSubstitution.embeddedStatements(s)
    embedded should not be empty
    embedded.collect { case i: SingleSearch => i }.foreach { inner =>
      inner.from.mainTable.derived.flatMap(_.cte) shouldBe Some(NamePart("c", quoted = false))
    }
    s.validate() shouldBe Right(())
    s.sql shouldBe "WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE k IN (SELECT id FROM c)"
    Parser(s.sql) shouldBe Right(s)
  }

  it should "descend into a subquery nested under OR, under NOT IN and under a relation wrapper" in {
    // 🔴 A criteria tree is NOT a list of conjuncts: a node under `OR`, under a relation wrapper or
    // behind a `NOT` arrives wrapped, and a walk that dispatches on the node's TYPE alone never
    // sees it. Each of these shapes must still MARK the inner reference.
    Seq(
      "WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE a = 1 OR k IN (SELECT id FROM c)",
      "WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE k NOT IN (SELECT id FROM c)",
      "WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE EXISTS (SELECT id FROM c)",
      "WITH c AS (SELECT id FROM t) SELECT * FROM u WHERE NESTED(k.a = 1 AND k.b IN (SELECT id FROM c))"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val s = parse(sql)
        val inners = CteSubstitution.embeddedStatements(s).collect { case i: SingleSearch => i }
        inners should not be empty
        inners.foreach(
          _.from.mainTable.derived.flatMap(_.cte) shouldBe Some(NamePart("c", quoted = false))
        )
        Parser(s.sql) shouldBe Right(s)
      }
    }
  }

  it should "be a loud parse error everywhere but the top of a SELECT" in {
    Seq(
      "SELECT a FROM (WITH x AS (SELECT a FROM t) SELECT a FROM x) d",
      "CREATE TABLE tgt AS WITH x AS (SELECT a FROM t) SELECT a FROM x",
      "CREATE MATERIALIZED VIEW mv AS WITH x AS (SELECT a FROM t) SELECT a FROM x",
      "INSERT INTO tgt WITH x AS (SELECT a FROM t) SELECT a FROM x",
      "WITH SELECT a FROM t",
      "WITH a () AS (SELECT 1 AS x) SELECT * FROM a"
    ).foreach(sql => rejects(sql)) // the message is grammar-internal: NEVER pin it
  }

  it should "validate an UNREFERENCED body and refuse an un-substituted programmatic reference" in {
    // Nothing in the FROM tree points at an unreferenced CTE, so without the `ctes` arm in
    // `validate()` its own GROUP BY rule would be skipped entirely.
    rejects("WITH a AS (SELECT a, b FROM t GROUP BY a) SELECT 1 FROM t", "Non-aggregated fields")

    val body = Parser("SELECT x FROM t") match {
      case Right(s: DqlStatement) => s
      case other                  => fail(s"unexpected $other")
    }
    val cte = Cte(NamePart("a", quoted = false), body)
    SingleSearch(from = From(Seq(Table("a"))), where = None, ctes = Seq(cte)).validate() match {
      case Left(msg) => msg should include("CTE 'a' is referenced but was not substituted")
      case Right(_)  => fail("a bare reference named like a CTE must not validate")
    }
    // The converse, or the arm above would pass for a statement that simply names nothing.
    SingleSearch(from = From(Seq(Table("u"))), where = None, ctes = Seq(cte))
      .validate() shouldBe Right(())
  }

  // -- render: the fixed point AND the text ----------------------------------------------------

  private val renders = Seq(
    witness -> witness,
    "with m as (select a from t) select a from m" ->
    "WITH m AS (SELECT a FROM t) SELECT a FROM m",
    "WITH m AS (SELECT a FROM t) SELECT q.a FROM m q" ->
    "WITH m AS (SELECT a FROM t) SELECT q.a FROM m AS q",
    "WITH `My CTE` AS (SELECT a FROM t) SELECT a FROM `My CTE`" ->
    """WITH "My CTE" AS (SELECT a FROM t) SELECT a FROM "My CTE"""",
    """WITH "My CTE" AS (SELECT a FROM t) SELECT a FROM "My CTE"""" ->
    """WITH "My CTE" AS (SELECT a FROM t) SELECT a FROM "My CTE"""",
    "WITH a AS (SELECT x FROM t), b AS (SELECT x FROM a WHERE x > 1) SELECT * FROM b" ->
    "WITH a AS (SELECT x FROM t), b AS (SELECT x FROM a WHERE x > 1) SELECT * FROM b",
    "WITH d AS (SELECT cid FROM x) SELECT o.id FROM orders o LEFT JOIN d ON o.id = d.cid" ->
    "WITH d AS (SELECT cid FROM x) SELECT o.id FROM orders AS o  LEFT JOIN d ON o.id = d.cid",
    "WITH one AS (SELECT 1 AS x) SELECT x FROM one" ->
    "WITH one AS (SELECT 1 AS x) SELECT x FROM one",
    "WITH u AS (SELECT 1 AS x) SELECT a FROM t" ->
    "WITH u AS (SELECT 1 AS x) SELECT a FROM t"
  )

  renders.foreach { case (in, text) =>
    it should s"render [$in] as [$text] and re-parse to an EQUAL AST" in {
      val stmt = Parser(in).toOption.getOrElse(fail(s"[$in] rejected: ${reasonOf(in)}"))
      // Both halves. The fixed point alone is satisfied by a LOSSY render (it is lossy on both
      // sides); the TEXT alone does not prove the render re-parses to the same statement. The
      // render is what `MaterializedViewExtension` persists and re-runs.
      stmt.sql shouldBe text
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  // -- neighbours: the productions this story must not move ------------------------------------

  "Neighbouring productions" should "keep `with` and `recursive` as ordinary identifiers" in {
    // Story 22.5 reserves NOTHING: rejecting a name that parses today is a breaking change and
    // belongs to whoever schedules one. These four are the no-narrowing pin.
    Seq(
      "SELECT * FROM t with",
      "SELECT with FROM t",
      "SELECT * FROM t recursive",
      "SELECT recursive FROM t"
    ).foreach { sql =>
      val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
    // ... and the QUOTED spelling is the documented escape hatch for a CTE named `recursive`,
    // because `keyword("RECURSIVE")` cannot match a `"`.
    val escaped = parse("""WITH "recursive" AS (SELECT 1 AS x) SELECT x FROM "recursive"""")
    escaped.ctes.map(_.name) shouldBe Seq(NamePart("recursive", quoted = true))
    // ... as is any position but the first.
    parse("WITH a AS (SELECT 1 AS x), recursive AS (SELECT 2 AS y) SELECT y FROM recursive")
  }

  it should "leave the pipeline `WITH PROCESSORS` family exactly where it was" in {
    // Copied from `PipelineRoundTripIdentitySpec:117`, reduced to one processor. This family gets
    // the TEXT fixed point only: a pipeline statement's AST is NOT a fixed point on `origin/main`
    // either. MEASURED against a control worktree at unmodified `de8f7594` — `Parser(s.sql) ==
    // Right(s)` is `false` there while `Parser(s.sql).map(_.sql) == Right(s.sql)` is `true`. It is
    // pre-existing, unowned and untouched by this story; the TEXT assertion is what this story
    // could actually have broken.
    val sql =
      """CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS """ +
      """(RENAME (field = "old_name", target_field = "new_name", ignore_failure = true))"""
    val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
    Parser(stmt.sql).toOption.map(_.sql) shouldBe Some(stmt.sql)
  }

  it should "leave every other DDL `WITH` family exactly where it was" in {
    // One statement per remaining `keyword("WITH")` family in the grammar, each copied from an
    // existing fixture rather than invented (ParserSpec's materialized-view rows, WatcherSpec:198
    // / :288, QuotedDmlDdlNameSpec:545).
    Seq(
      "CREATE MATERIALIZED VIEW mv WITH (delay = '1s') AS SELECT id FROM orders",
      "CREATE OR REPLACE MATERIALIZED VIEW mv REFRESH EVERY 60 SECONDS WITH (delay = '1s') AS SELECT id FROM orders",
      "REFRESH MATERIALIZED VIEW mv WITH SCHEDULE NOW",
      "CREATE OR REPLACE WATCHER my_watcher AS EVERY 5 MINUTES FROM my_index WITHIN 2 MINUTES " +
      "WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' " +
      "WITH PARAMS (threshold = 10) RETURNS TRUE DO log_action AS LOG 'x' AT INFO END",
      "CREATE OR REPLACE WATCHER w2 AS EVERY 5 MINUTES WITH INPUT (keys = [\"v1\",\"v2\"]) " +
      "NEVER DO a AS LOG 'x' END",
      "CREATE OR REPLACE WATCHER my_watch AS EVERY 5 MINUTES WITH INPUTS my_input AS " +
      "FROM t WITHIN 2 MINUTES ALWAYS DO my_action AS LOG 'x' END",
      // ... and the derived-table / WHERE-subquery neighbours story 22.5 builds on.
      "SELECT a FROM (SELECT a FROM t) d",
      "SELECT a FROM (SELECT a FROM t WHERE x = 1) d",
      "SELECT * FROM u WHERE k IN (SELECT id FROM t)"
    ).foreach { sql =>
      val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
  }
}
