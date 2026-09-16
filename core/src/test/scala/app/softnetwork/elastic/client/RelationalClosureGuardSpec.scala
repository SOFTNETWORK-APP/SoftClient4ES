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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  ctesPresent,
  relationalClosureRequired,
  setOperationsPresent,
  SearchStatement,
  SingleSearch
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

/** Story 22.1 — the DIRECT client API seam (`SearchApi.resolveWithSchema`) and the sweep of every
  * real `Parser(` dispatch site in core.
  *
  * Docker-free: `NopeClientApi` answers everything benignly, so a statement that is NOT refused
  * comes back `ElasticSuccess`. That is what makes the assertions falsifiable — the pre-story
  * behaviour of the JOIN row below was a MEASURED `ElasticSuccess` over index list `[orders]` with
  * a `match_all` body.
  *
  * 🔴 The 13 `Parser(` grep hits in this repository are SEVEN real dispatch sites: the four
  * remaining hits are comments (`sql/package.scala`, `parser/time/package.scala`,
  * `query/TemporalLiterals.scala`, `query/package.scala`) or unrelated APIs (Jackson's
  * `createParser` in `file/package.scala`, JLine's `.parser` in `repl/Repl.scala`). The seven are
  * `SQLImplicits`, `IndicesApi` ×3 (update / delete / insert by query), `GatewayApi.run`,
  * `PipelineApi.pipeline` and the `searchAs` macro — the last two covered by `CoreDqlExtensionSpec`
  * / `SQLQueryValidatorSpec`.
  */
class RelationalClosureGuardSpec extends AnyFlatSpec with Matchers {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private def client(): ElasticClientApi = new NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  private implicit val context: ConversionContext = NativeContext

  private def searchStatement(sql: String): SearchStatement = Parser(sql) match {
    case Right(s: SearchStatement) => s
    case Right(other)              => fail(s"[$sql] expected a search statement, got $other")
    case Left(e)                   => fail(s"[$sql] rejected: ${e.msg}")
  }

  private def refusalOf(res: ElasticResult[_]): ElasticError = res match {
    case ElasticFailure(e) => e
    case ElasticSuccess(v) => fail(s"expected a refusal, got ElasticSuccess($v)")
  }

  private val DerivedSelect = "SELECT COL FROM (SELECT 1 AS COL) AS d"
  private val JoinSelect = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id"
  private val CorrelatedSelect =
    "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)"

  /** Story 22.5's corpus witness, `superset.flightsql.w6.006`, verbatim. */
  private val CteSelect =
    "WITH monthly AS (SELECT category, SUM(amount) AS total FROM bi_events GROUP BY category) " +
    "SELECT * FROM monthly"

  // ---- the ONE seam ------------------------------------------------------------------------

  behavior of "SearchApi.resolveWithSchema (the direct-API seam)"

  it should "refuse a derived-table statement before any router sees it" in {
    val err = refusalOf(client().search(searchStatement(DerivedSelect)))
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("search")
    err.message should include(RelationalClosureGuard.ExtensionJar)
    err.message should include("derived table")
  }

  /** 🔴 The #157 residual on the raw client API, closed by this story on the lead's ruling.
    *
    * MEASURED on `origin/main` before the change: this returned `ElasticSuccess(ElasticResponse(…,
    * {"query": {"match_all": {}}}, …))` over index list `[orders]` — the JOIN leg silently dropped,
    * HTTP 200, wrong answer. No sibling production code calls this API (every one of them goes
    * through `gateway.run`), which is why it survived #157. ⚠️ Behaviour change, release-noted.
    */
  it should "refuse a cross-index JOIN on the direct API (the pre-existing #157 residual)" in {
    val err = refusalOf(client().search(searchStatement(JoinSelect)))
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("search")
    err.message should include(RelationalClosureGuard.ExtensionJar)
    err.message should include("cross-index JOIN")
  }

  /** The controls. `NopeClientApi` refuses every real search (it has no cluster), so the assertion
    * is that the statement got PAST this guard and failed for the client's own reason — never that
    * it succeeded, which would be unobservable here. A guard that widened too far reddens these.
    */
  it should "leave a plain statement alone" in {
    val err = refusalOf(client().search(searchStatement("SELECT a FROM t LIMIT 5")))
    err.message should not include RelationalClosureGuard.ExtensionJar
  }

  /** Story 22.2 (AD-8) — an UNCORRELATED WHERE subquery is PASSTHROUGH: it executes ES-natively in
    * core at every venue, so it must NOT trip this guard.
    *
    * The assertion is falsifiable in BOTH directions here. The refusal must not be the closure one;
    * and the message it IS must name the SUBQUERY, which proves phase one actually ran — the
    * resolver executed the inner statement through this same (cluster-less) client and reported its
    * failure. A seam that skipped the rewrite would report the OUTER statement's failure instead.
    */
  it should "let an uncorrelated WHERE subquery through the guard, and RUN phase one" in {
    val sql = "SELECT id FROM orders WHERE cid IN (SELECT id FROM customers)"
    relationalClosureRequired(searchStatement(sql)) shouldBe false
    val err = refusalOf(client().search(searchStatement(sql)))
    err.message should not include RelationalClosureGuard.ExtensionJar
    err.message should include("Subquery")
  }

  it should "leave a JOIN UNNEST alone — it is not a cross-index JOIN" in {
    val err = refusalOf(
      client().search(
        searchStatement("SELECT o.id, oi.q FROM orders o JOIN UNNEST(o.items) AS oi LIMIT 5")
      )
    )
    err.message should not include RelationalClosureGuard.ExtensionJar
  }

  it should "refuse a UNION ALL whose leg carries a derived table" in {
    val err = refusalOf(
      client().search(searchStatement(s"$DerivedSelect UNION ALL SELECT a FROM t"))
    )
    err.statusCode shouldBe Some(400)
  }

  /** 🔴 AC 6 in full. `search` is only ONE of the paths that cross the seam; `scroll`,
    * `searchAsync` and `searchWithInnerHits` each call `resolveWithSchema` at their OWN call site
    * (`ScrollApi:305`, `SearchApi:715/:741`, `SearchApi:1268/:1279`). Without a row each, deleting
    * the guard line would redden the `search` rows alone — and `scroll` is the documented path for
    * large extractions (#238), i.e. the one an embedder is most likely to call directly.
    */
  it should "refuse a derived-table statement on searchAsync too" in {
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val err = refusalOf(
      Await.result(client().searchAsync(searchStatement(DerivedSelect)), 10.seconds)
    )
    err.statusCode shouldBe Some(400)
    err.message should include(RelationalClosureGuard.ExtensionJar)
  }

  it should "refuse a derived-table statement on scroll too" in {
    import akka.stream.scaladsl.Sink
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val system: akka.actor.ActorSystem = akka.actor.ActorSystem("closure-guard-scroll")
    try {
      val thrown = intercept[Throwable] {
        Await.result(
          client().scroll(searchStatement(DerivedSelect)).runWith(Sink.seq),
          20.seconds
        )
      }
      // the refusal reaches the stream as a failure carrying OUR message, never as an empty stream
      // (an empty stream is what a silently-executed wrong query would look like)
      Option(thrown.getMessage).getOrElse("") should include(RelationalClosureGuard.ExtensionJar)
    } finally {
      Await.result(system.terminate(), 20.seconds)
      ()
    }
  }

  it should "refuse a derived-table statement on searchWithInnerHits too" in {
    import org.json4s.DefaultFormats
    implicit val formats: org.json4s.Formats = DefaultFormats
    val select = app.softnetwork.elastic.sql.query.SelectStatement(DerivedSelect)
    select.statement.map(_.getClass.getSimpleName) shouldBe Some("SingleSearch")
    val err =
      refusalOf(client().searchWithInnerHits[Map[String, Any], Map[String, Any]](select, "x"))
    err.statusCode shouldBe Some(400)
    err.message should include(RelationalClosureGuard.ExtensionJar)
  }

  // ---- the message ------------------------------------------------------------------------

  behavior of "RelationalClosureGuard"

  it should "name the DERIVED shape when both shapes are present" in {
    val stmt = Parser(
      "SELECT o.id FROM orders o JOIN (SELECT cid FROM orders) AS d ON o.id = d.cid"
    ).toOption.getOrElse(fail("rejected"))
    stmt.asInstanceOf[SingleSearch].from.enrichmentRequired shouldBe true
    RelationalClosureGuard.shapeOf(stmt) should include("derived table")
  }

  /** Story 22.3b — the correlated shape is reported FIRST when several are present, for the same
    * reason the derived table outranks the JOIN: it carries the narrowest remedy.
    */
  it should "name the CORRELATED shape, and prefer it over the others" in {
    val correlated = searchStatement(CorrelatedSelect)
    relationalClosureRequired(correlated) shouldBe true
    RelationalClosureGuard.shapeOf(correlated) should include("A correlated subquery")
    val both = searchStatement(
      "SELECT o.id FROM orders o JOIN customers c ON o.cid = c.id " +
      "WHERE EXISTS (SELECT 1 FROM refunds r WHERE r.oid = o.id)"
    )
    RelationalClosureGuard.shapeOf(both) should include("A correlated subquery")
  }

  it should "refuse a correlated statement at the seam, before phase one ever runs" in {
    val err = refusalOf(client().search(searchStatement(CorrelatedSelect)))
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("search")
    err.message should include("A correlated subquery")
    err.message should include(RelationalClosureGuard.ExtensionJar)
    // 🔴 falsifiable in the right direction: the resolver would have reported the INNER statement's
    // own failure ("Subquery …", as the uncorrelated row above asserts). Seeing the closure message
    // instead is what proves the guard ran FIRST.
    err.message should not include "Subquery"
  }

  it should "give DELETE ... WHERE EXISTS (correlated) the same refusal" in {
    val err = refusalOf(
      client()
        .asInstanceOf[IndicesApi]
        .deleteByQuery(
          "orders",
          "DELETE FROM orders WHERE EXISTS (SELECT 1 FROM refunds r WHERE r.oid = orders.id)"
        )
    )
    err.statusCode shouldBe Some(400)
    err.message should include("A correlated subquery")
  }

  /** Story 22.5 — the CTE shape is reported FIRST, and the test that matters is the one below it: a
    * CTE reference IS a derived table, so WITHOUT the arm this statement is refused as "a derived
    * table (subquery in FROM/JOIN)" — a construct the analyst never wrote.
    */
  it should "name the CTE shape, and prefer it over the derived table it is made of" in {
    val cte = searchStatement(CteSelect)
    relationalClosureRequired(cte) shouldBe true
    ctesPresent(cte) shouldBe true
    RelationalClosureGuard.shapeOf(cte) should include("WITH clause")
    // The falsifiable half: it must NOT fall through to the derived-table wording.
    RelationalClosureGuard.shapeOf(cte) should not include "derived table"
    // ... and the plain derived table must still get its own name (the arm did not swallow it).
    RelationalClosureGuard.shapeOf(searchStatement(DerivedSelect)) should include("derived table")
  }

  it should "refuse a CTE statement on the direct API at the seam" in {
    val err = refusalOf(client().search(searchStatement(CteSelect)))
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("search")
    err.message should include("WITH clause")
    err.message should include(RelationalClosureGuard.ExtensionJar)
  }

  it should "refuse a CTE statement even when NO CTE is referenced" in {
    // The classifier cannot count references, and the AST predicate must agree with it.
    val err =
      refusalOf(client().search(searchStatement("WITH u AS (SELECT 1 AS x) SELECT a FROM t")))
    err.statusCode shouldBe Some(400)
    err.message should include("WITH clause")
  }

  /** Story 22.5, Task 0 row 13. `IndicesApi.parseQueryForDeletion` ALREADY sniffed a leading `WITH`
    * as SQL before this story, so a CTE statement handed to `deleteByQuery` used to die on a lexer
    * error. It now PARSES, and both routes must still refuse it — never a delete-by-query over an
    * index named after the CTE's alias.
    */
  it should "refuse a CTE statement handed to deleteByQuery, on both routes" in {
    val sql = "WITH c AS (SELECT id FROM t) SELECT * FROM c"
    // alias != index: the SingleSearch arm's index check fires first (loud, 400).
    val mismatch = refusalOf(client().asInstanceOf[IndicesApi].deleteByQuery("t", sql))
    mismatch.statusCode shouldBe Some(400)
    // alias == index: the arm reaches `resolveDmlWithSchema` and the seam's closure term refuses.
    val seam = refusalOf(client().asInstanceOf[IndicesApi].deleteByQuery("c", sql))
    seam.statusCode shouldBe Some(400)
    seam.message should include("WITH clause")
  }

  // ---- story 22.6: set operators -----------------------------------------------------------

  /** 🔴 The silent-wrong-answer mode this family invites: a de-duplicating `UNION` executed as
    * `UNION ALL` returns duplicates with HTTP 200. The seam refuses it BEFORE any branch is
    * resolved, and the message names the set operation rather than one of its branches.
    */
  it should "name the SET OPERATION shape, and prefer it over the branches' own shapes" in {
    val distinctUnion = searchStatement("SELECT a FROM t UNION SELECT a FROM u")
    setOperationsPresent(distinctUnion) shouldBe true
    relationalClosureRequired(distinctUnion) shouldBe true
    RelationalClosureGuard.shapeOf(distinctUnion) should include("A set operation")
    // Falsifiable half: without the arm it falls through to "A cross-index JOIN", the shape the
    // analyst did NOT write.
    RelationalClosureGuard.shapeOf(distinctUnion) should not include "cross-index JOIN"
    // A set operation whose branches ALSO carry a derived table is still named for the operator.
    val both = searchStatement(s"SELECT a FROM t UNION $DerivedSelect")
    RelationalClosureGuard.shapeOf(both) should include("A set operation")
    RelationalClosureGuard.shapeOf(both) should not include "derived table"
  }

  it should "refuse a distinct UNION at the seam, never execute it as UNION ALL" in {
    val err = refusalOf(client().search(searchStatement("SELECT a FROM t UNION SELECT a FROM u")))
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("search")
    err.message should include("A set operation")
    err.message should include(RelationalClosureGuard.ExtensionJar)
  }

  it should "refuse INTERSECT and EXCEPT at the seam too" in {
    Seq("INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL", "UNION DISTINCT").foreach { op =>
      val err = refusalOf(client().search(searchStatement(s"SELECT a FROM t $op SELECT a FROM u")))
      withClue(s"[$op] ") {
        err.statusCode shouldBe Some(400)
        err.message should include("A set operation")
      }
    }
  }

  /** The CONTROL that makes the four rows above mean something: `UNION ALL` must still get PAST
    * this guard. Without it, a guard that refused every `MultiSearch` would look identical.
    */
  it should "leave a plain UNION ALL alone (it is Elasticsearch's own msearch)" in {
    val unionAll = searchStatement("SELECT a FROM t LIMIT 5 UNION ALL SELECT a FROM u LIMIT 5")
    setOperationsPresent(unionAll) shouldBe false
    relationalClosureRequired(unionAll) shouldBe false
    val err = refusalOf(client().search(unionAll))
    err.message should not include RelationalClosureGuard.ExtensionJar
  }

  it should "refuse a set operation on searchAsync and on scroll too" in {
    import akka.stream.scaladsl.Sink
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val ec: scala.concurrent.ExecutionContext = scala.concurrent.ExecutionContext.global
    val stmt = searchStatement("SELECT a FROM t EXCEPT SELECT a FROM u")
    refusalOf(Await.result(client().searchAsync(stmt), 10.seconds)).message should include(
      "A set operation"
    )
    implicit val system: akka.actor.ActorSystem = akka.actor.ActorSystem("closure-guard-setop")
    try {
      // 🔴 `ScrollApi.scroll` refuses EVERY MultiSearch with its own pre-existing
      // UnsupportedOperationException, so the assertion here is only that it is LOUD — never an
      // empty stream, which is what a silently-executed wrong query would look like.
      val thrown = intercept[Throwable] {
        Await.result(client().scroll(stmt).runWith(Sink.seq), 20.seconds)
      }
      Option(thrown.getMessage).getOrElse("") should not be empty
    } finally {
      Await.result(system.terminate(), 20.seconds)
      ()
    }
  }

  /** Story 22.6 AD-4's second half: at parse time a bare column is `SQLTypes.Any` and passes, so
    * the check must RE-RUN once schemas are attached. `NopeClientApi` attaches none, so this row
    * pins the LITERAL case the parse-time half already rejects plus the seam's own wiring; the
    * schema-attached case is covered by `TemporalLiteralSearchSpec`'s stub-schema idiom.
    */
  it should "reject a typeable branch mismatch before any request is built" in {
    Parser("SELECT 1 AS n FROM t UNION ALL SELECT 'a' AS n FROM u").swap.toOption
      .map(_.msg)
      .getOrElse("") should include("compatible types at column 1")
  }

  it should "refuse an INSERT ... SELECT carrying a set operation" in {
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val system: akka.actor.ActorSystem = akka.actor.ActorSystem("closure-guard-setop-ins")
    try {
      val res = Await.result(
        client()
          .asInstanceOf[IndicesApi]
          .insertByQuery("idx", "INSERT INTO idx SELECT a FROM t EXCEPT SELECT a FROM u"),
        10.seconds
      )
      refusalOf(res).statusCode should not be None
    } finally {
      Await.result(system.terminate(), 10.seconds)
      ()
    }
  }

  it should "say the statement was refused rather than executed against the first index (PD-2)" in {
    val msg = RelationalClosureGuard.rejection(searchStatement(JoinSelect)).message
    msg should include("refused rather than executed against the first index it names")
  }

  // ---- the dispatch-site sweep -------------------------------------------------------------

  behavior of "every real Parser( dispatch site in core"

  /** `parseQueryForUpdate` parses ONLY strings that `startsWith("UPDATE")`; anything else is
    * treated as a raw JSON body, so a SELECT string never reaches its `SingleSearch` arm. And
    * `UPDATE` itself takes `identRef` and rejects any `FROM`, so no derived table can enter an
    * UPDATE at all. Both halves are pinned, because a sweep test written as `updateByQuery(idx,
    * "SELECT …")` would be VACUOUS.
    */
  it should "refuse an UPDATE that names a FROM at all, and never parse a SELECT body" in {
    val c = client().asInstanceOf[IndicesApi]
    refusalOf(
      c.updateByQuery("idx", "UPDATE idx SET a = 1 FROM (SELECT id FROM t) d")
    ).statusCode shouldBe Some(400)
    // not SQL to this site — it is an invalid JSON body, never a parsed statement
    refusalOf(c.updateByQuery("idx", DerivedSelect)).statusCode should not be None
  }

  it should "refuse a DELETE targeting a derived table (the grammar's own err)" in {
    val err = refusalOf(
      client()
        .asInstanceOf[IndicesApi]
        .deleteByQuery("idx", "DELETE FROM (SELECT id FROM t) d WHERE id = 1")
    )
    err.message should include("DELETE cannot target a derived table")
  }

  it should "refuse an INSERT ... SELECT carrying a derived table" in {
    import scala.concurrent.Await
    import scala.concurrent.duration._
    implicit val system: akka.actor.ActorSystem = akka.actor.ActorSystem("closure-guard-spec")
    try {
      val res = Await.result(
        client().asInstanceOf[IndicesApi].insertByQuery("idx", s"INSERT INTO idx $DerivedSelect"),
        10.seconds
      )
      refusalOf(res).statusCode should not be None
    } finally {
      Await.result(system.terminate(), 10.seconds)
      ()
    }
  }

  it should "keep the pipeline DDL site loud for a SELECT" in {
    refusalOf(client().asInstanceOf[PipelineApi].pipeline(DerivedSelect)).message should include(
      "Unsupported pipeline DDL statement"
    )
  }
}
