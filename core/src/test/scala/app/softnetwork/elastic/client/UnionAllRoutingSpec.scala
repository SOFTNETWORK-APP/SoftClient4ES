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
import app.softnetwork.elastic.sql.query.{SearchStatement, SelectStatement, SingleSearch}
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

    /** 🔴 The two lines that make the byte pin below DISCRIMINATE. `core` does not depend on the
      * bridge, so with the real conversion every leg renders `{"query": {"match_all": {}}}`
      * whatever the statement and the pin proves only that SOMETHING was emitted per leg. Rendering
      * each leg's own SQL into the emitted body — the idiom `TemporalLiteralSearchSpec` already
      * uses — makes `multiQuery` carry each leg's CONTENT, so the pin can tell a correct `_msearch`
      * from a wrong one.
      */
    override private[client] implicit def singleSearchToJsonQuery(
      sqlSearch: app.softnetwork.elastic.sql.query.SingleSearch
    )(implicit
      timestamp: Long,
      contextType: app.softnetwork.elastic.sql.PainlessContextType =
        app.softnetwork.elastic.sql.PainlessContextType.Query
    ): String =
      new com.fasterxml.jackson.databind.ObjectMapper()
        .createObjectNode()
        .put("sql", sqlSearch.sql)
        .toString

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
      val n = scrollCalls.incrementAndGet()
      scrolledStatements = scrolledStatements :+ statement
      // 🔴 One row per leg, carrying THAT leg's own columns, and every VALUE NAMES ITS OWN
      // COLUMN (`category@2`). Three defects need that much:
      //   * `Source.empty` made the row-shape assertion loop over zero rows and pass for any
      //     behaviour at all;
      //   * one column per leg cannot see a REORDERED projection (`SELECT category, tag` beside
      //     `SELECT tag, category`), where the row shape agrees and the VALUES are swapped;
      //   * a value that does not identify its column cannot tell "re-keyed correctly" from
      //     "re-keyed onto the wrong column" — the oracle has to be independent of the mapping
      //     under test.
      // A `SELECT *` leg answers in `_source` order with columns the first branch never declared,
      // which is exactly what Elasticsearch returns for an opaque projection.
      val names: Seq[String] = statement match {
        case sel: SingleSearch =>
          val fields = sel.select.fieldsWithComputedAliases
          if (fields.size == 1 && fields.head.identifier.identifierName == "*")
            Seq("id", "category", "tag", "amount")
          else fields.map(_.outputName)
        case _ => Seq(s"leg$n")
      }
      val row = ListMap(names.map(k => k -> (s"$k@$n": Any)): _*)
      Source.single((row, ScrollMetrics()))
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
    // 🔴 One entry PER LEG, newline-joined, each carrying that leg's OWN statement — so a body
    // that dropped a leg, duplicated one, or reordered them reddens here.
    queries.multiQuery shouldBe
    "{\"sql\":\"SELECT id, name FROM dql_users WHERE age > 30 LIMIT 100\"}\n" +
    "{\"sql\":\"SELECT id, name FROM dql_users WHERE age <= 30 LIMIT 100\"}"
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

  /** 🔴 THE ROW CONTRACT. The routes must agree, or the presence of a `LIMIT` decides what a caller
    * gets — and the first attempt at making them agree was itself a wrong answer.
    *
    * The contract is [[ElasticConversion.rowNormalizer]] over the FIRST branch's output names: a
    * BY-NAME lookup that null-fills a miss and appends an extra. It is the function the one-shot
    * `_msearch` path already applies, and the lead's ruling for this story.
    *
    * An earlier draft re-keyed POSITIONALLY. The two rows below are the shapes that exposed it,
    * both measured on real ES 8.18 by the independent review — and note that neither is exotic: one
    * is the same two columns written in a different order, the other is `SELECT *`.
    */
  "The per-leg route" should "bind every value to its own column when a branch REORDERS the projection" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement("SELECT category, tag FROM l UNION ALL SELECT tag, category FROM r")
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.scrollCalls.get() shouldBe 2
    response.results should have size 2
    // Every row keyed by the first branch's names, in its order …
    response.results.map(_.keys.toSeq) shouldBe Seq(Seq("category", "tag"), Seq("category", "tag"))
    // 🔴 … and each VALUE under the column it names. A positional re-key reads
    // `category -> tag@2` here: same keys, same order, opposite binding, HTTP 200.
    response.results.map(_("category")) shouldBe Seq("category@1", "category@2")
    response.results.map(_("tag")) shouldBe Seq("tag@1", "tag@2")
  }

  it should "not mis-key or DROP columns when a branch is an opaque SELECT *" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement("SELECT category, tag FROM l UNION ALL SELECT * FROM r")
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.scrollCalls.get() shouldBe 2
    response.results should have size 2
    val starRow = response.results(1)
    // 🔴 `MultiSearch.declared` is `None` for `SELECT *` — arity and type checks exempt it on
    // purpose — so the leg's own names are unknown to `extractOutputFieldNames` and its row
    // arrives in `_source` order. A positional re-key put the ID under `category` and TRUNCATED
    // the row to the first branch's width, losing two columns. HTTP 200 both ways.
    starRow("category") shouldBe "category@2"
    starRow("tag") shouldBe "tag@2"
    starRow("id") shouldBe "id@2"
    starRow("amount") shouldBe "amount@2"
    // the first branch's columns lead; whatever the opaque leg carried beyond them follows
    starRow.keys.toSeq.take(2) shouldBe Seq("category", "tag")
  }

  it should "null-fill a column a branch does not declare" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement("SELECT a AS x FROM t UNION ALL SELECT b AS y FROM u")
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.msearchCalls.get() shouldBe 0
    client.scrollCalls.get() shouldBe 2
    response.results should have size 2
    response.results.head shouldBe ListMap[String, Any]("x" -> "x@1")
    // 🔴 Recorded rather than asserted-away, and MEASURED on real ES 8.18 rather than assumed:
    // where the branches AGREE on a column name every route gives the same answer (the two rows
    // above), and where they disagree — an alias per branch — this route null-fills the missing
    // name and keeps the branch's own column as an extra:
    //
    //   SELECT id AS x FROM l UNION ALL SELECT id AS y FROM r
    //     one-shot : {x -> null, y -> L_id_1} … {x -> null, y -> R_id_2}
    //     per-leg  : {x -> L_id_1}            … {x -> null, y -> R_id_2}
    //
    // The routes therefore still differ on BRANCH 1 of this one shape, because the one-shot
    // `_msearch` route never applies a leg's own alias mapping and loses branch 1's value under
    // its own declared name. That is PRE-EXISTING and outside this story (the review recorded it
    // as an observation); the per-leg answer is the better of the two, and propagating the
    // one-shot defect to make the two agree would be aligning to a bug.
    response.results(1) shouldBe ListMap[String, Any]("x" -> null, "y" -> "y@2")
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
