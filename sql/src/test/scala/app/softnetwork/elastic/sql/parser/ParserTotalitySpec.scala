package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{Criteria, ElasticRelation, SingleSearch}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

/** Issue #250 - `Parser.apply` is typed `Either[ParserError, Statement]` and must honour it.
  *
  * Five production call sites match on that Either with no `try` of their own:
  * `SQLImplicits.queryToStatement` (an implicit conversion), `IndicesApi` x3 (update/delete/insert
  * by query) and the `searchAs` macro (where a throw is a raw scalac crash). Two more wrap it
  * (`GatewayApi.run`, `PipelineApi.pipeline`) - which is why a test that only asserts `isLeft`
  * passes vacuously. Every case here asserts NOT-THROWING as its own assertion, first.
  *
  * Every `rejects` input below was MEASURED throwing, or silently mis-parsing, on the unmodified
  * tree at ac54a079.
  */
class ParserTotalitySpec extends AnyFlatSpec with Matchers {

  private def reasonOf(sql: String): String =
    Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  /** Asserts a GRAMMAR rejection, in the order that matters: not-throwing, THEN Left, THEN that it
    * came from an `err(...)` and NOT from the boundary catch, THEN the reason.
    *
    * 🔴 The third assertion is what makes this helper a GATE rather than a formality. Without it
    * every case here is UNFALSIFIABLE: `Parser.apply` wraps its whole body in `catch { case
    * NonFatal(e) => Left(...) }`, so re-introducing `throw ValidationError("Unbalanced
    * parentheses")` in `extractSubTokens` would still satisfy not-throwing, still satisfy `isLeft`,
    * and still produce a message CONTAINING "Unbalanced parentheses" - all three of the original
    * assertions pass while the defect this story exists to fix is back. Requiring the message NOT
    * to carry the `InternalParseFailure` label discriminates the two routes.
    *
    * Verified by falsification, not by reasoning: one `throw` was temporarily restored and this
    * suite was observed going RED (see the story's Dev Agent Record).
    */
  private def rejects(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(
      s"[$sql] msg=[$msg] - this rejection must come from an `err(...)` in the grammar, not from " +
      "`Parser.apply`'s NonFatal boundary catch. A `throw` restored anywhere under `parser/` " +
      "would land here. "
    ) {
      msg should not startWith Parser.InternalParseFailure
    }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  /** The mirror of `rejects` for the ONE route that legitimately goes through the boundary catch: a
    * throw raised in the AST, below the grammar, by `single`'s `.update()` action.
    */
  private def rejectsInternally(sql: String, reasons: String*): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should startWith(Parser.InternalParseFailure) }
    reasons.foreach(r => withClue(s"[$sql] msg=[$msg] ") { msg should include(r) })
    ()
  }

  behavior of "Parser.apply totality (#250)"

  // --- site 3: extractSubTokens ------------------------------------------------------------

  // The measured repro: jOOQ renders `name RLIKE 'Jo.*'` as `(name like_regex 'Jo.*')`, and
  // `like_regex` matches no operator, so `whereCriteria` stops after the bare `(` - the only
  // `crash` row of the Epic 19 census probe.
  it should "reject the jOOQ like_regex rendering instead of throwing" in {
    rejects("SELECT id FROM emp WHERE (name like_regex 'Jo.*')", "Unbalanced parentheses")
  }

  it should "reject an unclosed WHERE parenthesis instead of throwing" in {
    rejects("SELECT a FROM t WHERE (b = 1", "Unbalanced parentheses")
  }

  // j1/j2 - the ONLY rows that prove the `err` survives `ddlStatement | dqlStatement |
  // dmlStatement` when the winning leg is the THIRD one. `ddl` and `dql` both fail at offset ~0
  // with a Failure and `Failure.append(Error)` keeps whichever got further (Parsers.scala:195-198).
  it should "reject a malformed WHERE reached from the DML leg instead of throwing" in {
    rejects("DELETE FROM t WHERE (a = 1", "Unbalanced parentheses")
    rejects("UPDATE t SET a = 1 WHERE (b = 2", "Unbalanced parentheses")
  }

  // --- site 2: processSubTokens, through all three of its callers ---------------------------

  it should "reject an empty sub-expression instead of throwing" in {
    rejects("SELECT a FROM t WHERE ()", "Empty sub-expression")
  }

  it should "reject an empty sub-expression in HAVING instead of throwing" in {
    rejects("SELECT a FROM t HAVING ()", "Empty sub-expression")
  }

  it should "reject an empty sub-expression in CASE WHEN instead of throwing" in {
    rejects("SELECT CASE WHEN () THEN 'x' ELSE 'y' END AS c FROM t", "Empty sub-expression")
  }

  // Measured: this reaches site 2 through `on`, NOT site 4. Both rows are kept on purpose.
  it should "reject an empty ON clause instead of throwing" in {
    rejects("SELECT a FROM t JOIN u ON ()", "Empty sub-expression")
  }

  // --- site 1: processTokensHelper's invalid stack -----------------------------------------

  it should "reject a leading predicate operator instead of throwing" in {
    rejects("SELECT a FROM t WHERE AND a = 1", "Invalid stack state for predicate creation")
  }

  // --- site 4: FromParser.on -----------------------------------------------------------------

  // `On(criteria: Criteria)` is not optional (query/From.scala), so an ON whose criteria resolve
  // to None used to `throw new Exception`. MEASURED: the reachable shape is a DANGLING OPERATOR,
  // not `ON ()` - which dies earlier, in processSubTokens.
  it should "reject an ON clause whose criteria resolve to nothing instead of throwing" in {
    rejects("SELECT a FROM t JOIN u ON t.id = u.id AND", "ON clause requires criteria")
  }

  // --- OQ-5: the dangling AND / OR, which used to DROP the clause silently -------------------

  // MEASURED on the unmodified tree: a dangling AND/OR made `processTokens` return None, and
  // `Where(None)` renders as no clause at all, so `DELETE FROM orders WHERE id = 1 AND` parsed as
  // `DELETE FROM orders` and emptied the index. Same #213 data-loss family as the `phrase` change.
  // Lead ruling 2026-09-05: fold the fix into this story. `where` runs only after the literal
  // WHERE matched and `whereCriteria` is `rep1`, so `None` always means "a WHERE was written and
  // nothing usable came of it" - no valid statement can be lost.
  it should "reject a dangling AND in a SELECT instead of dropping the WHERE" in {
    rejects("SELECT a FROM t WHERE a = 1 AND", "WHERE clause requires criteria")
    rejects("SELECT a FROM t WHERE a = 1 OR", "WHERE clause requires criteria")
  }

  it should "reject a dangling AND in a DELETE instead of emptying the index" in {
    rejects("DELETE FROM orders WHERE id = 1 AND", "WHERE clause requires criteria")
  }

  it should "reject a dangling AND in an UPDATE instead of updating every document" in {
    rejects("UPDATE orders SET a = 1 WHERE id = 1 AND", "WHERE clause requires criteria")
  }

  it should "reject a dangling AND in a HAVING instead of dropping the clause" in {
    rejects(
      "SELECT COUNT(a) AS n FROM t GROUP BY b HAVING n > 1 AND",
      "HAVING clause requires criteria"
    )
  }

  // --- OQ-6: the stray closing parenthesis, which used to be swallowed ----------------------

  // MEASURED on the unmodified tree: `SELECT a FROM t WHERE a = 1)` parsed as `... WHERE a = 1`,
  // the `)` consumed by `whereCriteria` and then ignored by processTokensHelper's EndDelimiter
  // arm. A closing delimiter reaching that scan is unmatched by construction - a balanced group is
  // consumed whole by `extractSubTokens` - so rejecting it cannot lose a valid statement.
  it should "reject a stray closing parenthesis instead of swallowing it" in {
    rejects("SELECT a FROM t WHERE a = 1)", "Unbalanced parentheses")
    rejects("SELECT a FROM t WHERE a = 1))", "Unbalanced parentheses")
    rejects("SELECT a FROM t HAVING COUNT(a) > 1)", "Unbalanced parentheses")
  }

  // --- an `err` competing with a sibling alternative that consumed FURTHER --------------------

  // `Error` is NOT unconditionally dominant in scala-parser-combinators 1.1.2:
  // `Error.append(a) = this` (Parsers.scala:211) short-circuits everything nested inside and to the
  // RIGHT of it, but `Failure.append(alt) = if (alt.next.pos < next.pos) this else alt`
  // (Parsers.scala:195-198) keeps whichever got FURTHER - so an `err` raised EARLIER in the input
  // than a preceding sibling's `Failure` is discarded. All five converted sites sit deep inside a
  // clause, and `statement = ddlStatement | dqlStatement | dmlStatement` whose legs are largely
  // prefix-disjoint, which is why this is low risk. That is an argument, so it was MEASURED
  // (2026-09-05) rather than asserted, on shapes where an EARLIER leg genuinely consumes far into
  // the input before our `err` fires.
  it should "surface the grammar err even when an earlier alternative consumed far into the input" in {
    // CREATE MATERIALIZED VIEW / CREATE TABLE ... AS / INSERT ... SELECT all embed
    // `searchStatement`, so the DDL or DML leg has consumed 25-45 characters before the WHERE is
    // reached. Measured: our reason survives verbatim in every one.
    rejects("CREATE MATERIALIZED VIEW v AS SELECT a FROM t WHERE (b = 1", "Unbalanced parentheses")
    rejects(
      "CREATE MATERIALIZED VIEW v AS SELECT a FROM t WHERE a = 1 AND",
      "WHERE clause requires criteria"
    )
    rejects("INSERT INTO u SELECT a FROM t WHERE (b = 1", "Unbalanced parentheses")
    rejects("CREATE TABLE x AS SELECT a FROM t WHERE (b = 1", "Unbalanced parentheses")
    // `rep1sep(single, union)`: the err is raised in the FIRST leg, with a whole second leg to its
    // right that never gets to run.
    rejects("SELECT a FROM t WHERE (b = 1 UNION ALL SELECT c FROM u", "Unbalanced parentheses")
    rejects(
      "SELECT a FROM t WHERE a = 1 AND UNION ALL SELECT c FROM u",
      "WHERE clause requires criteria"
    )
  }

  // ...and the ONE measured shape where a sibling's failure DOES win, pinned as known-and-accepted.
  // `CREATE WATCHER w AS ...` is malformed twice over - the watcher grammar wants `AT` where `AS`
  // stands - and the parser reports only the FIRST blocker it met, which here is the watcher leg's
  // own regex failure rather than our WHERE `err`. That costs nothing this story owns: the result
  // is still a `Left` and still NOT the boundary catch, so totality holds; only the REASON TEXT is
  // a different leg's. Deliberately asserted on the CONTRACT, never on that text - a
  // grammar-internal `string matching regex ...` message is never-pin.
  it should "stay a grammar rejection when a sibling leg's failure masks the err" in {
    val sql = "CREATE WATCHER w AS SELECT a FROM t WHERE (b = 1"
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    withClue(s"[$sql] msg=[${reasonOf(sql)}] ") {
      reasonOf(sql) should not startWith Parser.InternalParseFailure
    }
  }

  // --- N-ARY relation predicates: the review finding, now FIXED at its source ----------------

  // `NESTED`/`CHILD`/`PARENT` used to take a strictly BINARY `predicate`
  // (`criteria ~ (and|or) ~ not.? ~ criteria`), so THREE OR MORE criteria could not match it and
  // fell through to the `X.regex ~ start.? ~ criteria ~ end.?` form - whose `start.?` swallowed the
  // `(` while its `end.?` never fired. MEASURED before the fix: not a syntax error but a SILENT
  // WRONG ANSWER -
  //   `WHERE id = 1 AND child(a = 2 AND b = 3 AND c = 4)`
  //     parsed as `WHERE id = 1 AND CHILD(a = 2) AND b = 3 AND c = 4`
  // with `b` and `c` escaping onto the PARENT document, and with `OR` the operator itself spanning
  // the relation boundary.
  //
  // 🔴 Identifiers here are DOTTED ON PURPOSE (`child.a`, not `a`). `has_child.type` /
  // `has_parent.parent_type` are derived from the FIRST criterion's leading name segment
  // (`query/Where.scala` `rtype`), so `child(a = 1 AND b = 2)` emits the nonsense `"type": "a"`.
  // An unqualified acceptance row would be certifying that nonsense as correct.
  it should "parse a relation predicate with three or more criteria" in {
    Seq(
      "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.a = 2 AND child.b = 3 AND child.c = 4)",
      "SELECT * FROM Table WHERE a = 1 AND child(child.x = 1 OR child.y = 2 OR child.z = 3)",
      "SELECT * FROM Table WHERE parent(parent.a = 1 AND parent.b = 2 AND parent.c = 3)",
      "SELECT * FROM Table WHERE child(child.a = 1 AND child.b = 2 OR child.c = 3)",
      // a parenthesised sub-group INSIDE the relation - `relationGroup` matches a `(` with its own
      // `)` and re-emits both, so `processTokens` rebuilds the group exactly as at top level
      "SELECT * FROM Table WHERE child(child.a = 1 AND (child.b = 2 OR child.c = 3))",
      "SELECT * FROM Table WHERE a = 1 AND child(child.x = 1 AND child.y = 2 AND child.z = 3) OR b = 2",
      // NESTED is only meaningful once `JOIN UNNEST` has declared the path - see the KNOWN
      // PRE-EXISTING DEFECT test further down for what happens when it has not.
      "SELECT * FROM Table JOIN UNNEST(Table.nested) AS nested WHERE nested(nested.a = 1 AND nested.b = 2 AND nested.c = 3)"
    ).foreach(sql => withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true })
  }

  // 🔴 THE CLAIM, STATED EXECUTABLY. The whole correctness argument for this design is that the
  // relation body is reduced by `processTokens` - the same function `where`/`having`/`on` use - so
  // that `CHILD(X)` contains exactly the criteria tree `WHERE X` produces. Asserting a couple of
  // all-AND and all-OR shapes could not prove it: those are association-INSENSITIVE, so they show
  // that nesting exists, not that it is the RIGHT nesting.
  //
  // This property compares the two trees STRUCTURALLY (`==`, not rendered text) across shapes that
  // do distinguish associativity and precedence. It also guards the hand-copied alternation: if
  // someone adds an alternative to `whereCriteria` and forgets `relationTokens`, a shape using it
  // stops being equivalent and this test fails - which is the only thing keeping the two in step.
  it should "reduce a relation body to exactly the tree the same expression produces at top level" in {
    def whereCriteriaOf(sql: String): Option[Criteria] =
      Parser(sql).toOption
        .collect { case s: SingleSearch => s }
        .flatMap(_.where)
        .flatMap(_.criteria)

    Seq(
      "child.a = 1",
      "child.a = 1 AND child.b = 2",
      "child.a = 1 AND child.b = 2 AND child.c = 3",
      "child.a = 1 OR child.b = 2 OR child.c = 3",
      "child.a = 1 AND child.b = 2 OR child.c = 3",
      "child.a = 1 OR child.b = 2 AND child.c = 3",
      "(child.a = 1 OR child.b = 2) AND child.c = 3",
      "child.a = 1 AND (child.b = 2 OR child.c = 3)",
      "child.a = 1 AND NOT child.b = 2 AND NOT child.c = 3",
      "NOT child.a = 1"
    ).foreach { shape =>
      val top = whereCriteriaOf(s"SELECT * FROM t WHERE $shape")
      val inRelation = whereCriteriaOf(s"SELECT * FROM t WHERE child($shape)").collect {
        case r: ElasticRelation => r.criteria
      }
      withClue(s"[$shape] top=[$top] inRelation=[$inRelation] ") {
        top shouldBe defined
        inRelation shouldBe defined
        inRelation shouldBe top
      }
    }
  }

  // Every AST `.sql` must re-parse to an EQUAL AST (project_ast_render_roundtrip_family).
  // ⚠️ NOT is deliberately absent here: `NOT c = 3` renders as `c NOT = 3`, which does NOT re-parse.
  // That render defect is PRE-EXISTING and identical at top level (`WHERE NOT a = 1` renders
  // `a NOT = 1` on `main` too), but 3-criteria relations were rejected before, so this commit makes
  // it newly REACHABLE inside a relation. Recorded in
  // docs/issues/local-21.4-not-render-asymmetry.md; deliberately not pinned, because pinning a
  // broken round-trip reads as a contract.
  it should "round-trip an N-ary relation predicate through its own render" in {
    Seq(
      "SELECT * FROM Table WHERE child(child.a = 2 AND child.b = 3 AND child.c = 4)",
      "SELECT * FROM Table WHERE child(child.x = 1 OR child.y = 2 OR child.z = 3)",
      "SELECT * FROM Table WHERE child(child.a = 1 AND (child.b = 2 OR child.c = 3))",
      "SELECT * FROM Table WHERE parent(parent.a = 1 AND parent.b = 2 AND parent.c = 3)",
      "SELECT * FROM Table WHERE nested nested.a = 1"
    ).foreach { sql =>
      val parsed = Parser(sql)
      withClue(s"[$sql] ") { parsed.isRight shouldBe true }
      val rendered = parsed.toOption.get.sql
      withClue(s"[$sql] rendered=[$rendered] ") { Parser(rendered) shouldBe parsed }
    }
  }

  // The regression BOUNDARY. The one- and two-criteria forms always worked (two matched the binary
  // `predicate`); they are the single thing most likely to break when the arity is generalised.
  it should "still accept a relation predicate with one or two criteria" in {
    Seq(
      "SELECT * FROM Table WHERE child(child.a = 1 AND child.b = 2)",
      "SELECT * FROM Table WHERE parent(parent.a = 1 AND parent.b = 2)",
      "SELECT * FROM Table WHERE child(child.a = 1)",
      "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.identifier3 = 3)",
      "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.identifier2 > 2 OR child.identifier3 = 3)",
      // the paren-LESS form, which is what `nestedCriteria` / `childCriteria` are now for
      "SELECT * FROM Table WHERE child child.a = 1"
    ).foreach(sql => withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true })
  }

  // 🔴 The KNOWN GAP recorded earlier in this story is CLOSED. The old
  // `X.regex ~ start.? ~ criteria ~ end.?` form let a `(` be consumed with NO matching `)` and
  // nothing noticed, so `WHERE child(a = 1 AND b = 2` parsed as `CHILD(a = 1) AND b = 2`. The
  // optional delimiters are gone: an opening parenthesis now commits to the predicate form, which
  // requires the closing one.
  //
  // The reason text is a grammar-internal `phrase` message (`end of input expected`) and is NOT
  // pinned - it is not ours and it is unstable. That makes the rejection half weak on its own, so
  // the discriminating companion is the POSITIVE control below it: the same statements WITH the
  // closing parenthesis must still parse. Together they say "the paren is what decides", which a
  // test that merely observed a Left could not.
  it should "reject an unmatched opening parenthesis in a relation predicate" in {
    Seq(
      "SELECT * FROM Table WHERE child(child.a = 1 AND child.b = 2",
      "SELECT * FROM Table WHERE child(child.a = 1",
      "SELECT * FROM Table WHERE nested(nested.a = 1 AND nested.b = 2 AND nested.c = 3"
    ).foreach { sql =>
      withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
      withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
      withClue(s"[$sql] msg=[${reasonOf(sql)}] ") {
        reasonOf(sql) should not startWith Parser.InternalParseFailure
      }
      // the positive control: the SAME statement closed parses
      withClue(s"[$sql)] ") { Parser(s"$sql)").isRight shouldBe true }
    }
  }

  // Balanced, but empty. `relationTokens` is `rep1`, so it fails inside the repetition rather than
  // reaching `relationCriteria`'s `Right(None)` arm - a different path from the one above, hence a
  // test of its own rather than a row in it.
  it should "reject an empty relation predicate" in {
    Seq(
      "SELECT * FROM Table WHERE child()",
      "SELECT * FROM Table WHERE nested()",
      "SELECT * FROM Table WHERE parent()"
    ).foreach { sql =>
      withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
      withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
      withClue(s"[$sql] msg=[${reasonOf(sql)}] ") {
        reasonOf(sql) should not startWith Parser.InternalParseFailure
      }
    }
  }

  // 🔴 NEWLY REACHABLE degenerate token streams. `relationTokens` accepts `(or | and)` as a
  // standalone element in any position and lets `allCriteria` fire twice in a row, so streams that
  // could never previously reach `processTokens` from a relation body now can. In a story whose
  // subject is totality, that surface must be exercised - with the full contract, because
  // `noException` + `isLeft` alone would pass even if the code threw and the boundary catch caught
  // it. Messages are MEASURED, and they are our own `err` literals, so pinning them is safe.
  it should "reject a degenerate relation body without throwing" in {
    rejects("SELECT * FROM Table WHERE child(AND)", "Invalid stack state for predicate creation")
    rejects("SELECT * FROM Table WHERE child(OR OR)", "Invalid stack state for predicate creation")
    rejects(
      "SELECT * FROM Table WHERE child(AND child.a = 1)",
      "Invalid stack state for predicate creation"
    )
    rejects(
      "SELECT * FROM Table WHERE child(child.a = 1 child.b = 2)",
      "Invalid stack state for predicate creation"
    )
    // reaches `relationCriteria`'s `Right(None)` arm, which is what makes its `relation` parameter
    // load-bearing rather than decorative - the message NAMES the relation
    rejects("SELECT * FROM Table WHERE child(child.a = 1 AND)", "CHILD clause requires criteria")
    rejects("SELECT * FROM Table WHERE nested(nested.a = 1 OR)", "NESTED clause requires criteria")
    rejects("SELECT * FROM Table WHERE parent(parent.a = 1 AND)", "PARENT clause requires criteria")
  }

  // `err` inside `relationCriteria` is non-backtracking, so it is worth proving it fires only AFTER
  // `child(` has committed. A column literally NAMED `child`/`nested`/`parent` must still work:
  // `Child.regex` matches, `start` then fails, and that is a `Failure` (not an `Error`), so the
  // alternation falls through to `criteria` as it always did.
  it should "still treat a column named like a relation as an ordinary column" in {
    Seq(
      "SELECT * FROM Table WHERE child = 1",
      "SELECT * FROM Table WHERE child = 1 AND a = 2",
      "SELECT * FROM Table WHERE nested = 1",
      "SELECT * FROM Table WHERE parent > 3"
    ).foreach(sql => withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true })
  }

  // 🔴 KNOWN PRE-EXISTING DEFECT, pinned so this story cannot be read as certifying it.
  // `NESTED(...)` WITHOUT a `JOIN UNNEST(...) AS <alias>` to declare the path emits
  // `{"match_all":{}}` - the criteria are DISCARDED and every document matches
  // (`bridge/.../ElasticBridge.scala`, `buildNestedTrees(criteria.nestedElements)` is `Nil`).
  // Measured identical at arity 1, 2 and 3, so it is NOT caused by this commit - but arity >= 3
  // used to be rejected, so this commit makes it newly REACHABLE. The statement parses; only the
  // emitted query is wrong, which is why the bridge specs assert the DECLARED-path spelling.
  // Recorded in docs/issues/local-21.4-nested-without-declared-path.md.
  // When that defect is fixed this test is DELETED, not retargeted - it pins a defect.
  it should "parse an undeclared-path NESTED, which is a known pre-existing emission defect" in {
    Parser(
      "SELECT * FROM Table WHERE nested(nested.a = 1 AND nested.b = 2 AND nested.c = 3)"
    ).isRight shouldBe true
  }

  // --- the residual the grammar cannot own (boundary catch) ---------------------------------

  // `single` calls `.update()` inside its action, so the AST-update pass runs during the PARSE.
  // Two sites CRASH there, MEASURED: `SingleSearch.bucketNames` (query/package.scala:125) with a
  // bare `select.fields(n - 1)` INDEX - no `throw` token, which is why the source scan below
  // cannot see it - and `Bucket.update` (query/GroupBy.scala:76, IllegalArgumentException, reached
  // only by `GROUP BY -1`, because bucketNames' `\d+` regex matches the `1` inside `-1`).
  //
  // Ordinal-bucket SEMANTICS are story 21.3 / #253. When 21.3 lands it rejects these in
  // `validate()`, which returns a `Left` WITHOUT reaching the boundary catch, so the
  // `InternalParseFailure` assertions below go red. RETARGET them to 21.3's message; do NOT delete
  // the tests. The `noException` + `isLeft` halves are this story's contract and must NEVER be
  // relaxed, whatever the reason string becomes.
  it should "not let an AST-side throw escape (GROUP BY ordinal out of range)" in {
    rejectsInternally("SELECT a, b FROM t GROUP BY 0", "IndexOutOfBounds")
    rejectsInternally("SELECT a, b FROM t GROUP BY 9")
  }

  it should "not let an AST-side throw escape (non-positive GROUP BY ordinal)" in {
    rejectsInternally("SELECT a, b FROM t GROUP BY -1", "IllegalArgument")
  }

  // 🔴 THIS ROW IS NOT AN ENDORSEMENT - `SELECT city2 FROM t GROUP BY city2` is a VALID query.
  // `city2` is an ordinary column, not an ordinal, but `SingleSearch.bucketNames`
  // (query/package.scala:120-132) detects an "ordinal" by running `\d+` over the RENDERED column
  // name, guarded only by "the name contains no space" - so it matches the `2` in `city2` and
  // indexes `select.fields(2 - 1)` on a ONE-column SELECT. Every column whose name contains a
  // digit (`city2`, `q4`, `field_2`) is a candidate.
  //
  // On `main` this CRASHES `Parser.apply` with IndexOutOfBoundsException: 1. 21.4 only stops the
  // crash escaping; the SEMANTICS are story 21.3 / #253's defect 2 and PD-4 forbids fixing them
  // here. Local record: docs/issues/local-21.4-group-by-digit-suffixed-column.md.
  // When 21.3 lands, this query must PARSE - at which point this test is deleted, not retargeted.
  it should "not let an AST-side throw escape for a VALID query (21.3 defect 2, not endorsed)" in {
    rejectsInternally("SELECT city2 FROM t GROUP BY city2", "IndexOutOfBounds")
  }

  // --- the prologue, which is NOT part of the grammar ----------------------------------------

  // MEASURED: `normalize` dereferences `query.length`, so `Parser(null)` throws an NPE BEFORE
  // `parse` runs. An NPE is NonFatal, so `ElasticResult.attempt` caught it at the two `core`
  // boundaries - which is exactly why removing those wrappers is only safe while the `try` covers
  // the WHOLE method body. Degenerate-but-non-null inputs already returned Left and must keep
  // doing so.
  it should "not let a null or degenerate input escape" in {
    rejectsInternally(null.asInstanceOf[String], "NullPointerException")
    Seq("", "   ", ";", ";;;").foreach { s =>
      withClue(s"[$s] ") { noException should be thrownBy Parser(s) }
      withClue(s"[$s] ") { Parser(s).isLeft shouldBe true }
      // a degenerate-but-non-null input is a GRAMMAR rejection, never the boundary catch
      withClue(s"[$s] msg=[${reasonOf(s)}] ") {
        reasonOf(s) should not startWith Parser.InternalParseFailure
      }
    }
  }

  // --- the cause is carried, and ONLY on the internal-fault route ----------------------------

  // AD-10. `ElasticResult.attempt` used to supply the Throwable cause at the two `core`
  // boundaries; folding that route away would have deleted the only stack trace an internal
  // parser fault ever produces. A grammar rejection keeps `cause = None` - a user's syntax error
  // has no interesting stack, and attaching one puts a parser internal in front of a BI user.
  it should "carry the cause on an internal fault and not on a grammar rejection" in {
    Parser("SELECT a, b FROM t GROUP BY 0").swap.toOption.flatMap(_.cause) shouldBe defined
    Parser("SELECT a FROM t WHERE (b = 1").swap.toOption.flatMap(_.cause) shouldBe empty
    Parser("SELECT * FRM users").swap.toOption.flatMap(_.cause) shouldBe empty
  }

  // --- no regression on the happy path -------------------------------------------------------

  // The grouped-predicate surface `processTokens` exists for. ParserSpec asserts these ASTs in
  // full; this is a cheap local guard that the `^^` -> `>>` conversions did not change what the
  // grammar ACCEPTS, only what it rejects.
  it should "still accept every grouped-predicate shape" in {
    val accepted = Seq(
      "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR identifier3 = 3",
      "SELECT * FROM Table WHERE identifier1 = 1 AND (identifier2 > 2 OR identifier3 = 3)",
      "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR (identifier3 = 3 AND identifier4 = 4)",
      "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.identifier2 > 2 OR child.identifier3 = 3)",
      "SELECT * FROM Table WHERE identifier1 = 1 AND parent(parent.identifier2 > 2 OR parent.identifier3 = 3)",
      "SELECT * FROM Table WHERE identifier IN ('val1','val2','val3')",
      // ON criteria must be equality or an AND of equalities - a PRE-EXISTING `validate()`
      // restriction, unrelated to #250 (`ON t.x > 1` is rejected on the unmodified tree too).
      "SELECT a FROM t JOIN u ON (t.id = u.id) WHERE (a = 1)",
      "SELECT a FROM t JOIN u ON (t.id = u.id AND t.x = u.x) WHERE (a = 1)",
      "SELECT a FROM t JOIN u ON t.id = u.id WHERE a = 1",
      "SELECT CASE WHEN (a = 1) THEN 'x' ELSE 'y' END AS c FROM t",
      "SELECT CASE WHEN a = 1 THEN 'x' ELSE 'y' END AS c FROM t",
      "DELETE FROM Table WHERE (identifier1 = 1 AND identifier2 > 2)",
      "UPDATE Table SET identifier1 = 1 WHERE (identifier2 > 2 OR identifier3 = 3)",
      "SELECT identifier, COUNT(identifier2) AS ct FROM Table WHERE identifier2 IS NOT NULL GROUP BY identifier HAVING COUNT(identifier2) > 1"
    )
    accepted.foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  // --- the generic gate ----------------------------------------------------------------------

  // MEASURED on the unmodified tree: these seeds' prefixes threw 49 times across sites 1-3 AND
  // site 4 (46 x "Unbalanced parentheses", 3 x "ON clause requires criteria"). Seed 6 - the
  // PAREN-FREE join - is what reaches site 4: its `ON t.id = u.id AND` prefix makes processTokens
  // return None. Seeds 8/9 are the DML leg. Token-boundary prefixes only (a character-level sweep
  // costs ~40x more parse time for no extra shapes).
  //
  // HONEST LIMIT: this sweep reaches sites 1-4. It does NOT reach site 5 (a defensive arm believed
  // unreachable) and it CANNOT reach the AST surface - an out-of-range ordinal is never a prefix
  // of a valid statement. Those are covered by the GROUP BY cases above and by the source scan.
  it should "never throw on any token prefix of a valid parenthesised statement" in {
    val seeds = Seq(
      "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR identifier3 = 3",
      "SELECT * FROM Table WHERE identifier1 = 1 AND (identifier2 > 2 OR identifier3 = 3)",
      "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR (identifier3 = 3 AND identifier4 = 4)",
      "SELECT identifier, COUNT(identifier2) AS ct FROM Table WHERE identifier2 IS NOT NULL GROUP BY identifier HAVING COUNT(identifier2) > 1",
      "SELECT a FROM t JOIN u ON (t.id = u.id AND t.x > 1) WHERE (a = 1)",
      "SELECT a FROM t JOIN u ON t.id = u.id AND t.x > 1 WHERE a = 1",
      "SELECT CASE WHEN (a = 1) THEN 'x' ELSE 'y' END AS c FROM t",
      "DELETE FROM Table WHERE (identifier1 = 1 AND identifier2 > 2)",
      "UPDATE Table SET identifier1 = 1 WHERE (identifier2 > 2 OR identifier3 = 3)",
      "SELECT a, b, c FROM t GROUP BY 2 ORDER BY 1"
    )
    seeds.foreach { seed =>
      val tokens = seed.split(" ").toList
      tokens.indices.foreach { i =>
        val prefix = tokens.take(i + 1).mkString(" ")
        withClue(s"[$prefix] ") { noException should be thrownBy Parser(prefix) }
        // 🔴 `noException` ALONE is vacuous here - `Parser.apply`'s NonFatal catch guarantees it
        // unconditionally, so the sweep would stay green with every `err` conversion reverted.
        // A prefix may legitimately PARSE (`SELECT * FROM Table` is a prefix of seed 1), so the
        // discriminating property is not `isLeft`: it is that no prefix reaches the boundary
        // catch. Every rejection here must come from the grammar.
        withClue(s"[$prefix] msg=[${reasonOf(prefix)}] ") {
          reasonOf(prefix) should not startWith Parser.InternalParseFailure
        }
      }
    }
  }

  // --- anti-drift guard: no `throw` may return to the parser package -------------------------

  private val parserSourceCandidates = Seq(
    new File("sql/src/main/scala/app/softnetwork/elastic/sql/parser"),
    new File("src/main/scala/app/softnetwork/elastic/sql/parser")
  )

  private def parserSourceRoot: Option[File] = parserSourceCandidates.find(_.isDirectory)

  private def scalaFiles(dir: File): Seq[File] = {
    val (dirs, files) =
      Option(dir.listFiles).getOrElse(Array.empty[File]).toSeq.partition(_.isDirectory)
    files.filter(_.getName.endsWith(".scala")) ++ dirs.flatMap(scalaFiles)
  }

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  behavior of "the parser package"

  // NOT `assume`: a cancelled ScalaTest is not a failure and `sbt test` still exits 0, so an
  // `assume`-guarded scan would let "the guard passes" and "the guard scanned nothing" both hold.
  // Assert the precondition once, unconditionally, and name the working directory in the clue.
  it should "have its sources on disk for the anti-drift scan" in {
    withClue(s"working directory = ${new File(".").getAbsolutePath} - ") {
      parserSourceRoot shouldBe defined
    }
  }

  it should "have found more than one source file to scan" in {
    // Guards the guard: an empty file list makes `offenders shouldBe empty` pass vacuously.
    scalaFiles(parserSourceRoot.get).size should be > 5
  }

  it should "contain no throw expression (#250 anti-drift)" in {
    // Matches `throw`, plus `sys.error` and `???` - throws under another spelling. Scaladoc and
    // comment lines are skipped two ways: a line whose first non-space chars are `*` or `/*`, and
    // everything from the first `//` onward. MEASURED against the real tree BEFORE the fix: 22
    // files, exactly the 5 known sites, zero false positives; the 4 prose mentions are correctly
    // excluded. Cutting at `//` can only ever cause a false NEGATIVE (a `throw` written after a
    // `//` inside a string literal on the same line), which no line in this tree has.
    //
    // KNOWN BLIND SPOTS, recorded so nobody mistakes this for a proof:
    //   - a bare INDEX access. `SingleSearch.bucketNames` (query/package.scala:125) crashes with
    //     `select.fields(n - 1)` and contains no `throw` token at all - NO grep-based guard can
    //     ever see it. This is not a gap to be closed by a better regex; it is why the GROUP BY
    //     cases above exercise the AST surface by INPUT instead.
    //   - a non-exhaustive `match` (a MatchError carries no `throw` token).
    //   - `require` / `assert`, `.get` on an empty Option, `.head` on Nil.
    //   - a single-line block comment: `val x = 1 /* throw */` would be a FALSE POSITIVE
    //     (`isCommentLine` only inspects the line's start). No such line exists in this package
    //     today; if one appears, fix the line, not the guard.
    val throwToken = """\bthrow\b|\bsys\.error\(|\?\?\?""".r
    def codeOf(line: String): String = {
      val i = line.indexOf("//")
      if (i < 0) line else line.substring(0, i)
    }
    def isCommentLine(line: String): Boolean = {
      val s = line.trim
      s.startsWith("*") || s.startsWith("/*")
    }
    val offenders =
      scalaFiles(parserSourceRoot.get).flatMap { f =>
        read(f).linesIterator.zipWithIndex.collect {
          case (line, i)
              if !isCommentLine(line) && throwToken.findFirstIn(codeOf(line)).isDefined =>
            s"${f.getPath}:${i + 1}: ${line.trim}"
        }
      }
    withClue(
      "A `throw` inside the parser package escapes `Parser.apply`'s Either (#250). " +
      "Use `err(...)` in a `>>` continuation, or return an Either from the helper and let the " +
      "caller emit `err`. See Parser.scala's `alterTable`.\n" +
      "SCOPE, so this clue is not over-trusted: it scans `parser/**` ONLY. Throws that are " +
      "reachable during a parse but live elsewhere - `query/GroupBy.scala`, `query/package.scala`, " +
      "`query/Where.scala`, all pulled in by `single`'s `.update()` action - are NOT covered here, " +
      "and one of the real crashers is a bare index with no `throw` token at all. Those are " +
      "covered by input instead (the GROUP BY cases above) and belong to story 21.3.\n" +
      "Offenders:\n" + offenders.mkString("\n") + "\n"
    ) {
      offenders shouldBe empty
    }
  }
}
