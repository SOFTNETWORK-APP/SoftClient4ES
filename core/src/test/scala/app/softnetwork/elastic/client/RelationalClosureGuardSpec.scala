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
import app.softnetwork.elastic.sql.query.{relationalClosureRequired, SearchStatement, SingleSearch}
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
