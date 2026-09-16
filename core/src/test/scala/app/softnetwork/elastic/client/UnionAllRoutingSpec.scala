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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.sql.query.{SearchStatement, SelectStatement}
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext}

/** Story 22.6 AD-6 — how a `UNION ALL` reaches Elasticsearch.
  *
  * An `_msearch` sends every leg as its own ONE-SHOT request, so a row-shaped leg with no bounding
  * `LIMIT` came back with Elasticsearch's default 10 hits and HTTP 200 — issue #209's family, one
  * venue over. Such a statement is now executed per leg through `search(leg)`, which routes each
  * leg through scroll/PIT exactly as that leg executes on its own. EVERY OTHER SHAPE keeps the
  * single `_msearch`, and that is what the first rows pin.
  *
  * Docker-free: the routing is observed at the CLIENT seams (`executeMultiSearch` / `scroll`),
  * never inferred from a result.
  */
class UnionAllRoutingSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)
  private implicit val system: ActorSystem = ActorSystem("union-all-routing-spec")
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val context: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 20.seconds)
    ()
  }

  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = testLogger

    val msearchCalls = new AtomicInteger(0)
    val scrollCalls = new AtomicInteger(0)
    @volatile var lastMultiQuery: Option[ElasticQueries] = None
    @volatile var scrolledStatements: Seq[SearchStatement] = Seq.empty

    /** An EMPTY but well-formed Elasticsearch response.
      *
      * `NopeClientApi` answers `None`, which core reports as "Failed to execute …" — so with the
      * stub's own answer every routing row below would end in a failure and both counters would
      * read zero for a reason that has nothing to do with routing. Answering an empty hit set lets
      * each leg complete, which is what makes "leg 2 was scrolled" observable at all: the per-leg
      * fold stops at the FIRST failing leg.
      */
    private def emptyResponse: ElasticResult[Option[JsonNode]] = {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val root = mapper.createObjectNode()
      val hits = root.putObject("hits")
      hits.putArray("hits")
      hits.putObject("total").put("value", 0)
      ElasticResult.success(Some(root))
    }

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = emptyResponse

    override private[client] def executeSingleSearchAsync(
      elasticQuery: ElasticQuery
    )(implicit ec: ExecutionContext): scala.concurrent.Future[ElasticResult[Option[JsonNode]]] =
      scala.concurrent.Future.successful(emptyResponse)

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] = {
      msearchCalls.incrementAndGet()
      lastMultiQuery = Some(elasticQueries)
      emptyResponse
    }

    // `searchAsync` reaches the ASYNC seam, never the sync one — a counter on `executeMultiSearch`
    // alone reads zero for every async row and would make the async pins vacuous.
    override private[client] def executeMultiSearchAsync(
      elasticQueries: ElasticQueries
    )(implicit ec: ExecutionContext): scala.concurrent.Future[ElasticResult[Option[JsonNode]]] =
      scala.concurrent.Future.successful {
        msearchCalls.incrementAndGet()
        lastMultiQuery = Some(elasticQueries)
        emptyResponse
      }

    override def scroll(statement: SearchStatement, config: ScrollConfig)(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[(ListMap[String, Any], ScrollMetrics), NotUsed] = {
      scrollCalls.incrementAndGet()
      scrolledStatements = scrolledStatements :+ statement
      Source.empty
    }
  }

  /** Every routing row asserts the statement actually EXECUTED. Without this a parse rejection or a
    * seam refusal would leave both counters at zero and satisfy half the assertions below for
    * entirely the wrong reason.
    */
  private def run(sql: String): RecordingClient = {
    val client = new RecordingClient
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] was refused: ${error.message}")
    }
    client
  }

  private def runAsync(sql: String): RecordingClient = {
    val client = new RecordingClient
    Await.result(client.searchAsync(SelectStatement(sql)), 20.seconds) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] was refused: ${error.message}")
    }
    client
  }

  // ── the fast path: ONE msearch, byte-identical to the baseline ─────────────────────────────

  /** 🔴 What this byte pin can and cannot see, stated so it is not read as more than it is.
    *
    * `ElasticQueries.multiQuery` is what Elasticsearch RECEIVES (`ElasticMultiSearchRequest.query`
    * is called by no production path — its own scaladoc says so). But the SQL-to-DSL rendering
    * lives in the bridge, and `core` does not depend on it, so through a core-only client every leg
    * renders as `{"query": {"match_all": {}}}`. The body pin therefore proves the SHAPE of the
    * request (one entry PER LEG, newline-joined) rather than the query inside it; the
    * discriminating half is `sqlQuery` — the statement, per leg, in order — plus the call counts.
    * The literal below was recorded from a CONTROL worktree at `origin/main` before this story.
    */
  "A UNION ALL whose legs are all bounded" should
  "emit ONE _msearch whose body is byte-identical to the pre-story baseline" in {
    val sql = "SELECT id, name FROM dql_users WHERE age > 30 LIMIT 100 " +
      "UNION ALL SELECT id, name FROM dql_users WHERE age <= 30 LIMIT 100"
    val client = run(sql)
    client.msearchCalls.get() shouldBe 1
    client.scrollCalls.get() shouldBe 0
    val queries = client.lastMultiQuery.getOrElse(fail("no _msearch was emitted"))
    queries.multiQuery shouldBe "{\"query\": {\"match_all\": {}}}\n{\"query\": {\"match_all\": {}}}"
    queries.queries should have size 2
    queries.queries.map(_.indices) shouldBe Seq(Seq("dql_users"), Seq("dql_users"))
    queries.sqlQuery shouldBe sql
  }

  it should "stay one-shot for aggregation-shaped legs (no rows to page)" in {
    val client = run(
      "SELECT category, COUNT(*) AS n FROM a GROUP BY category " +
      "UNION ALL SELECT category, COUNT(*) AS n FROM b GROUP BY category"
    )
    client.msearchCalls.get() shouldBe 1
    client.scrollCalls.get() shouldBe 0
  }

  // ── the per-leg route ──────────────────────────────────────────────────────────────────────

  "A UNION ALL with a row-shaped un-LIMITed leg" should
  "never emit an _msearch, and scroll each leg in order" in {
    val client = run("SELECT a FROM x UNION ALL SELECT a FROM y")
    client.msearchCalls.get() shouldBe 0
    client.scrollCalls.get() shouldBe 2
    client.scrolledStatements.map(_.sql) shouldBe Seq("SELECT a FROM x", "SELECT a FROM y")
  }

  it should "route the MIXED shape per leg too — one bounded leg, one paged" in {
    val client = run("SELECT a FROM x LIMIT 5 UNION ALL SELECT a FROM y")
    client.msearchCalls.get() shouldBe 0
    // the bounded leg goes one-shot through `search(leg)`, the un-LIMITed one through scroll
    client.scrollCalls.get() shouldBe 1
    client.scrolledStatements.map(_.sql) shouldBe Seq("SELECT a FROM y")
  }

  it should "route the same way on searchAsync" in {
    val paged = runAsync("SELECT a FROM x UNION ALL SELECT a FROM y")
    paged.msearchCalls.get() shouldBe 0
    paged.scrollCalls.get() shouldBe 2

    val bounded = runAsync("SELECT a FROM x LIMIT 5 UNION ALL SELECT a FROM y LIMIT 5")
    bounded.msearchCalls.get() shouldBe 1
    bounded.scrollCalls.get() shouldBe 0
  }

  // ── the seam guard runs FIRST ──────────────────────────────────────────────────────────────

  "A non-UNION ALL set operation" should "be refused before either route is taken" in {
    val client = new RecordingClient
    client.search(SelectStatement("SELECT a FROM x UNION SELECT a FROM y")) match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.message should include("A set operation")
        error.message should include(RelationalClosureGuard.ExtensionJar)
      case ElasticSuccess(other) => fail(s"expected a refusal, got $other")
    }
    // 🔴 The falsifiable half: neither seam is touched. Executing it as an `_msearch` would be
    // executing a de-duplicating UNION as UNION ALL — duplicates, HTTP 200.
    client.msearchCalls.get() shouldBe 0
    client.scrollCalls.get() shouldBe 0
  }
}
