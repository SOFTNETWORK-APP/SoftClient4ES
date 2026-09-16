package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.6 — set operators: grammar, flat AST + derived precedence tree, the `EXCEPT`
  * disambiguation, the render fixed point WITH rendered-text pins, OUR rejections, and the
  * neighbouring productions that must not move.
  */
class SetOperatorSpec extends AnyFlatSpec with Matchers {

  private def parse(sql: String): MultiSearch = Parser(sql) match {
    case Right(m: MultiSearch) => m
    case Right(other) => fail(s"[$sql] expected MultiSearch, got ${other.getClass.getSimpleName}")
    case Left(e)      => fail(s"[$sql] rejected: ${e.msg}")
  }

  private def reasonOf(sql: String): String = Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  /** `ParserTotalitySpec`'s `rejects`, copied rather than shared (it is private there): not
    * throwing, THEN `Left`, THEN not the boundary catch, THEN the reason. The third assertion is
    * what makes each case falsifiable once `Parser.apply` carries a `NonFatal` boundary catch —
    * without it a restored `throw` still yields a `Left` CONTAINING the same reason (story 21.4).
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should not startWith Parser.InternalParseFailure }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  // ── every spelling maps to its own token ──────────────────────────────────────────────────
  private val spellings: Seq[(String, SetOperator)] = Seq(
    "UNION ALL"      -> UNION,
    "UNION  ALL"     -> UNION,
    "union all"      -> UNION,
    "UNION"          -> UNION_DISTINCT,
    "UNION DISTINCT" -> UNION_DISTINCT,
    "union distinct" -> UNION_DISTINCT,
    "INTERSECT"      -> INTERSECT,
    "INTERSECT ALL"  -> INTERSECT_ALL,
    "intersect  all" -> INTERSECT_ALL,
    "EXCEPT"         -> EXCEPT,
    "EXCEPT ALL"     -> EXCEPT_ALL
  )

  spellings.foreach { case (op, token) =>
    it should s"parse [$op] as $token between two branches" in {
      val m = parse(s"SELECT a FROM x $op SELECT b FROM y")
      m.requests.map(_.sql) shouldBe Seq("SELECT a FROM x", "SELECT b FROM y")
      // Canonical form: a UNION ALL-only list is STORED as Nil.
      m.operators shouldBe (if (token == UNION) Nil else Seq(token))
      m.resolvedOperators shouldBe Seq(token)
      m.isUnionAllOnly shouldBe (token == UNION)
      m.relationalClosureRequired shouldBe (token != UNION)
      relationalClosureRequired(m) shouldBe (token != UNION)
      setOperationsPresent(m) shouldBe (token != UNION)
    }
  }

  it should "distinguish every spelling from every other (no two spellings share a token)" in {
    // Non-vacuity of the table above: it must actually exercise all six tokens, or the ordered
    // alternation could commit UNION_DISTINCT on the prefix of UNION ALL and stay green.
    spellings.map(_._2).distinct should contain theSameElementsAs Seq(
      UNION,
      UNION_DISTINCT,
      INTERSECT,
      INTERSECT_ALL,
      EXCEPT,
      EXCEPT_ALL
    )
  }

  "UNION ALL" should "keep the legacy AST shape: operators default to Nil ⇒ every operator is UNION" in {
    val parsed = parse("SELECT a FROM x UNION ALL SELECT b FROM y")
    val legacy = MultiSearch(parsed.requests)
    legacy.operators shouldBe Nil
    legacy.resolvedOperators shouldBe Seq(UNION)
    legacy.isUnionAllOnly shouldBe true
    legacy.sql shouldBe "SELECT a FROM x UNION ALL SELECT b FROM y"
    // The canonical form is what makes the two constructions EQUAL...
    legacy shouldBe parsed
    // ...and the legacy construction a fixed point.
    Parser(legacy.sql) shouldBe Right(legacy)
  }

  it should "store a MIXED list verbatim (a UNION ALL b UNION c is not UNION ALL-only)" in {
    val m = parse("SELECT a FROM x UNION ALL SELECT b FROM y UNION SELECT c FROM z")
    m.operators shouldBe Seq(UNION, UNION_DISTINCT)
    m.isUnionAllOnly shouldBe false
  }

  it should "accept an unparenthesised ORDER BY / LIMIT on a NON-last branch (only the last is ambiguous)" in {
    val m = parse("SELECT a FROM x LIMIT 5 UNION SELECT b FROM y")
    m.requests.head.limit.map(_.limit) shouldBe Some(5)
    m.requests(1).limit shouldBe None
  }

  "A lone parenthesised SELECT" should "parse as the SingleSearch and render bare (a deliberate widening)" in {
    Parser("(SELECT a FROM t)") shouldBe Parser("SELECT a FROM t")
    Parser("(SELECT a FROM t)").toOption.get.sql shouldBe "SELECT a FROM t"
  }

  // ── precedence + associativity (the derived tree) ──────────────────────────────────────────
  it should "bind INTERSECT tighter than UNION: a UNION b INTERSECT c = a UNION (b INTERSECT c)" in {
    parse("SELECT a FROM x UNION SELECT a FROM y INTERSECT SELECT a FROM z").tree shouldBe
    SetOpBranch(SetOpLeaf(0), UNION_DISTINCT, SetOpBranch(SetOpLeaf(1), INTERSECT, SetOpLeaf(2)))
  }

  it should "be left-associative within a level: a EXCEPT b EXCEPT c = (a EXCEPT b) EXCEPT c" in {
    parse("SELECT a FROM x EXCEPT SELECT a FROM y EXCEPT SELECT a FROM z").tree shouldBe
    SetOpBranch(SetOpBranch(SetOpLeaf(0), EXCEPT, SetOpLeaf(1)), EXCEPT, SetOpLeaf(2))
  }

  it should "group consecutive INTERSECTs left to right before the outer operators" in {
    parse(
      "SELECT a FROM w UNION ALL SELECT a FROM x INTERSECT SELECT a FROM y " +
      "INTERSECT ALL SELECT a FROM z EXCEPT SELECT a FROM v"
    ).tree shouldBe SetOpBranch(
      SetOpBranch(
        SetOpLeaf(0),
        UNION,
        SetOpBranch(SetOpBranch(SetOpLeaf(1), INTERSECT, SetOpLeaf(2)), INTERSECT_ALL, SetOpLeaf(3))
      ),
      EXCEPT,
      SetOpLeaf(4)
    )
  }

  it should "give a two-branch UNION ALL the same tree as any other operator (Nil is expanded)" in {
    parse("SELECT a FROM x UNION ALL SELECT a FROM y").tree shouldBe
    SetOpBranch(SetOpLeaf(0), UNION, SetOpLeaf(1))
  }

  // ── EXCEPT: projection exclusion vs set operator, pinned BOTH ways and combined ────────────
  "EXCEPT" should "stay the SELECT * EXCEPT(cols) projection exclusion before FROM" in {
    Parser("SELECT * EXCEPT(a) FROM t") match {
      case Right(s: SingleSearch) =>
        s.select.except.map(_.fields.map(_.outputName)) shouldBe Some(Seq("a"))
      case other => fail(s"expected SingleSearch, got $other")
    }
  }

  it should "be the set operator after a complete branch" in {
    parse("SELECT a FROM t EXCEPT SELECT a FROM u").operators shouldBe Seq(EXCEPT)
    // no space before the parenthesis: still the set operator, because the branch is complete
    parse("SELECT a FROM t EXCEPT(SELECT a FROM u)").operators shouldBe Seq(EXCEPT)
  }

  it should "be both in one statement" in {
    val m = parse("SELECT * EXCEPT(a) FROM t EXCEPT SELECT * FROM u")
    m.requests.head.select.except shouldBe defined
    m.operators shouldBe Seq(EXCEPT)
  }

  // ── parenthesised branches ────────────────────────────────────────────────────────────────
  "A parenthesised branch" should "parse and not be recorded (renders bare for UNION ALL)" in {
    val m = parse("(SELECT a FROM t) UNION ALL (SELECT a FROM u)")
    m.sql shouldBe "SELECT a FROM t UNION ALL SELECT a FROM u"
    parse("(SELECT a FROM t ORDER BY a LIMIT 3) UNION ALL SELECT a FROM u").requests.head.limit
      .map(_.limit) shouldBe Some(3)
  }

  it should "carry a WHERE-terminated body (the depth-aware whereCriteria landed with 22.1/22.2)" in {
    val m = parse("(SELECT a FROM t WHERE a = 1) UNION ALL (SELECT a FROM u)")
    m.requests.head.where shouldBe defined
    m.requests should have size 2
  }

  it should "be the spelling for a per-branch ORDER BY / LIMIT under UNION / INTERSECT / EXCEPT" in {
    val m = parse("SELECT a FROM t UNION (SELECT a FROM u ORDER BY a LIMIT 3)")
    m.requests(1).limit.map(_.limit) shouldBe Some(3)
    m.requests(1).orderBy shouldBe defined
  }

  // ── rejections that MUST be ours ──────────────────────────────────────────────────────────
  "A trailing ORDER BY or LIMIT on the last bare branch" should
  "be rejected for UNION / INTERSECT / EXCEPT, naming the rule and the derived-table remedy" in {
    Seq("UNION", "UNION DISTINCT", "INTERSECT", "EXCEPT", "EXCEPT ALL").foreach { op =>
      rejects(
        s"SELECT a FROM t $op SELECT a FROM u ORDER BY a",
        "applies to that branch only",
        "derived table"
      )
      rejects(s"SELECT a FROM t $op SELECT a FROM u LIMIT 5", "applies to that branch only")
    }
  }

  it should "stay bound to the last branch for UNION ALL (today's behaviour, byte-for-byte)" in {
    val m = parse("SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a LIMIT 5")
    m.requests(1).limit.map(_.limit) shouldBe Some(5)
    m.requests.head.limit shouldBe None
    m.sql shouldBe "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a ASC LIMIT 5"
  }

  "A set operation wrapped in parentheses" should
  "be rejected naming the derived-table spelling — as a branch AND as a whole" in {
    rejects(
      "(SELECT a FROM t UNION SELECT a FROM u) INTERSECT SELECT a FROM v",
      "wrapped in parentheses is not supported",
      "derived table"
    )
    rejects(
      "(SELECT a FROM t UNION SELECT a FROM u)",
      "wrapped in parentheses is not supported"
    )
    rejects(
      "SELECT d.a FROM ((SELECT a FROM t UNION SELECT a FROM u)) d",
      "wrapped in parentheses is not supported"
    )
  }

  "intersect" should "be a reserved word (the story's one narrowing)" in {
    rejects("SELECT intersect FROM t")
    rejects("SELECT a FROM t intersect")
    // the quoted escape hatch (story 21.1), exactly as for `union`
    Parser("""SELECT "intersect" FROM t""").isRight shouldBe true
  }

  // ── validation ─────────────────────────────────────────────────────────────────────────────
  "Branch arity" should "be validated between declaring branches, skipping SELECT *" in {
    rejects(
      "SELECT a, b FROM t UNION ALL SELECT a FROM u",
      "same number of columns",
      "branch 1 projects 2 (a, b)",
      "branch 2 projects 1 (a)"
    )
    rejects("SELECT a FROM t INTERSECT SELECT a, b, c FROM u", "branch 2 projects 3")
    // opaque: a bare SELECT * is never guessed
    Parser("SELECT * FROM t UNION SELECT a, b FROM u").isRight shouldBe true
    Parser("SELECT * FROM t UNION ALL SELECT * FROM u").isRight shouldBe true
  }

  "Branch types" should "be validated where both sides are typeable" in {
    rejects("SELECT 1 AS n FROM t UNION SELECT 'a' AS n FROM u", "compatible types at column 1")
    rejects(
      "SELECT CAST(x AS BIGINT) AS v FROM t UNION ALL SELECT CAST(y AS VARCHAR) AS v FROM u",
      "compatible types at column 1"
    )
    // numeric family
    Parser("SELECT 1 AS n FROM t UNION SELECT 2.5 AS n FROM u").isRight shouldBe true
    // a bare column is `Any` at parse time — the core seam re-checks with schemas attached
    Parser("SELECT a FROM t UNION SELECT 'x' FROM u").isRight shouldBe true
  }

  it should "validate a set operation INSIDE a derived table one level down" in {
    rejects(
      "SELECT d.a FROM (SELECT a, b FROM t UNION SELECT a FROM u) d",
      "same number of columns"
    )
  }

  "A WITH clause on a non-first branch" should "be refused (only a programmatic construction can build it)" in {
    val body = Parser("WITH x AS (SELECT a FROM t) SELECT a FROM x").toOption.get
      .asInstanceOf[SingleSearch]
    val plain = Parser("SELECT a FROM y").toOption.get.asInstanceOf[SingleSearch]
    MultiSearch(Seq(plain, body), operators = Seq(UNION_DISTINCT))
      .validate()
      .swap
      .toOption
      .getOrElse("") should include("must precede the FIRST branch")
    // the legal placement validates
    MultiSearch(Seq(body, plain), operators = Seq(UNION_DISTINCT)).validate() shouldBe Right(())
  }

  "An operator list of the wrong length" should "be refused" in {
    val a = Parser("SELECT a FROM t").toOption.get.asInstanceOf[SingleSearch]
    val b = Parser("SELECT a FROM u").toOption.get.asInstanceOf[SingleSearch]
    MultiSearch(Seq(a, b), operators = Seq(UNION_DISTINCT, EXCEPT))
      .validate()
      .swap
      .toOption
      .getOrElse("") should include("needs 1 operators")
  }

  "A MATERIALIZED VIEW over a set operation" should "be refused at parse time" in {
    rejects(
      "CREATE MATERIALIZED VIEW mv AS SELECT a FROM t UNION SELECT a FROM u",
      "MATERIALIZED VIEW over a set operation is not supported"
    )
    // ...including UNION ALL: `CreateMaterializedView.search` THREW for every MultiSearch body
    rejects(
      "CREATE MATERIALIZED VIEW mv AS SELECT a FROM t UNION ALL SELECT a FROM u",
      "MATERIALIZED VIEW over a set operation is not supported"
    )
  }

  // ── render: fixed point AND text ───────────────────────────────────────────────────────────
  private val renders = Seq(
    "SELECT a FROM x UNION ALL SELECT b FROM y" ->
    "SELECT a FROM x UNION ALL SELECT b FROM y",
    "(SELECT a FROM x) UNION ALL (SELECT b FROM y)" ->
    "SELECT a FROM x UNION ALL SELECT b FROM y",
    "SELECT a FROM x UNION SELECT b FROM y" ->
    "(SELECT a FROM x) UNION (SELECT b FROM y)",
    "SELECT a FROM x UNION DISTINCT SELECT b FROM y" ->
    "(SELECT a FROM x) UNION (SELECT b FROM y)",
    "SELECT a FROM x INTERSECT ALL SELECT b FROM y EXCEPT SELECT c FROM z" ->
    "(SELECT a FROM x) INTERSECT ALL (SELECT b FROM y) EXCEPT (SELECT c FROM z)",
    "SELECT a FROM x UNION SELECT b FROM y INTERSECT SELECT c FROM z" ->
    "(SELECT a FROM x) UNION (SELECT b FROM y) INTERSECT (SELECT c FROM z)",
    "SELECT a FROM x UNION (SELECT b FROM y ORDER BY b ASC LIMIT 3)" ->
    "(SELECT a FROM x) UNION (SELECT b FROM y ORDER BY b ASC LIMIT 3)",
    "SELECT a FROM x UNION ALL SELECT b FROM y UNION SELECT c FROM z" ->
    "(SELECT a FROM x) UNION ALL (SELECT b FROM y) UNION (SELECT c FROM z)",
    "SELECT * EXCEPT(a) FROM t EXCEPT SELECT * FROM u" ->
    "(SELECT * except(a) FROM t) EXCEPT (SELECT * FROM u)",
    "SELECT d.a FROM (SELECT a FROM t UNION SELECT a FROM u) AS d" ->
    "SELECT d.a FROM ((SELECT a FROM t) UNION (SELECT a FROM u)) AS d"
  )

  renders.foreach { case (in, text) =>
    it should s"render [$in] as [$text] and re-parse to an equal AST" in {
      val stmt = Parser(in).toOption.getOrElse(fail(s"[$in] rejected: ${reasonOf(in)}"))
      stmt.sql shouldBe text
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  /** 🔴 An OMITTED sort direction is not an AST fixed point, and that is PRE-EXISTING and
    * statement-wide, not a set-operator property: MEASURED against a control on this same tree,
    * `SELECT b FROM y ORDER BY b LIMIT 3` renders `... ORDER BY b ASC LIMIT 3` and re-parses to a
    * DIFFERENT AST with an identical render. The row above pins the explicit-`ASC` spelling as a
    * true fixed point; this one pins what actually matters for a render that gets PERSISTED and
    * re-run (`MaterializedViewExtension`) — the render is IDEMPOTENT.
    */
  it should "render an omitted sort direction idempotently inside a parenthesised branch" in {
    val in = "SELECT a FROM x UNION (SELECT b FROM y ORDER BY b LIMIT 3)"
    val text = "(SELECT a FROM x) UNION (SELECT b FROM y ORDER BY b ASC LIMIT 3)"
    val stmt = Parser(in).toOption.getOrElse(fail(s"[$in] rejected: ${reasonOf(in)}"))
    stmt.sql shouldBe text
    val reparsed = Parser(stmt.sql).toOption.getOrElse(fail(s"[$text] rejected"))
    reparsed.sql shouldBe text
    // and the explicit spelling IS a fixed point, so nothing but the direction default differs
    Parser(text) shouldBe Right(reparsed)
  }

  // ── 22.5 interplay: the WITH prefix is hoisted out of the parenthesised render ──────────────
  "A set operation whose first branch carries a WITH clause" should
  "hoist the WITH prefix ONCE, ahead of the first parenthesised branch, and stay a fixed point" in {
    val flat =
      Parser("WITH x AS (SELECT a FROM t) SELECT a FROM x UNION ALL SELECT a FROM y").toOption
        .getOrElse(fail("UNION ALL + WITH rejected"))
    // UNION ALL renders flat, exactly as before story 22.6
    flat.sql shouldBe "WITH x AS (SELECT a FROM t) SELECT a FROM x UNION ALL SELECT a FROM y"
    Parser(flat.sql) shouldBe Right(flat)

    val grouped =
      Parser("WITH x AS (SELECT a FROM t) SELECT a FROM x UNION SELECT a FROM y").toOption
        .getOrElse(fail("UNION + WITH rejected"))
    grouped.sql shouldBe
    "WITH x AS (SELECT a FROM t) (SELECT a FROM x) UNION (SELECT a FROM y)"
    // 🔴 The obligation this story owns: `(WITH x AS (…) SELECT …) UNION (…)` cannot parse,
    // because a branch is a `single` and `single` has no WITH. Hoisting is what keeps the render
    // a fixed point — and `MaterializedViewExtension` persists renders and runs them.
    Parser(grouped.sql) shouldBe Right(grouped)
    grouped.asInstanceOf[MultiSearch].relationalClosureRequired shouldBe true
  }

  it should "route a UNION ALL to the engine when a BRANCH carries a WITH clause" in {
    // The operator list alone says ES-native; the branch member says otherwise, and the statement
    // member is the ONE thing every venue reads.
    val m = parse("WITH x AS (SELECT a FROM t) SELECT a FROM x UNION ALL SELECT a FROM y")
    m.isUnionAllOnly shouldBe true
    m.relationalClosureRequired shouldBe true
    relationalClosureRequired(m) shouldBe true
    // ...and it is NOT reported as a set operation: the shape the analyst wrote is the WITH clause
    setOperationsPresent(m) shouldBe false
  }

  it should "accept a CTE body that is itself a set operation" in {
    val stmt = Parser("WITH u AS (SELECT a FROM t UNION SELECT a FROM v) SELECT a FROM u").toOption
      .getOrElse(fail("CTE body as a set operation rejected"))
    Parser(stmt.sql) shouldBe Right(stmt)
    relationalClosureRequired(stmt) shouldBe true
  }

  // ── neighbour pins ─────────────────────────────────────────────────────────────────────────
  "Neighbouring productions" should "not move" in {
    Seq(
      "SELECT * EXCEPT(a, b) FROM t WHERE c = 1",
      "SELECT COUNT(DISTINCT a) AS n FROM t",
      "SELECT a FROM t WHERE a IN (1, 2) AND b = 'union'",
      "SELECT a FROM t1, t2",
      "SELECT a FROM t JOIN UNNEST(t.items) i",
      "SELECT d.a FROM (SELECT a FROM t) d",
      "SELECT a FROM t WHERE a IN (SELECT a FROM u)"
    ).foreach { sql =>
      val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
    // FROM-less branches are not operands (documented limitation)
    rejects("SELECT 1 UNION SELECT 2")
  }
}
