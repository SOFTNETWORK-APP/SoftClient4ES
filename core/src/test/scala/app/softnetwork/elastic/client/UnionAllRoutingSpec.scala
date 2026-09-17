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

    /** The `_msearch` answer: ONE response per leg, each carrying a single hit whose `_source` is
      * keyed by THAT leg's SOURCE field names and valued `"<source field>@<leg>"`.
      *
      * 🔴 `_source`, not output names, and that is what makes the alias mapping load-bearing: the
      * rename from `id` to `x` is `jsonNodeToMap`'s, driven by the leg's `fieldAliases`. A stub
      * that answered `{"x": …}` directly would be green whichever alias map the parse used, which
      * is exactly the defect issue #354 case 2 reports. The leg's SQL is recovered from the body
      * the stub itself rendered above, so this needs no out-of-band channel.
      */
    private def legHitsResponse(elasticQueries: ElasticQueries): ElasticResult[Option[JsonNode]] = {
      val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
      val root = mapper.createObjectNode()
      val responses = root.putArray("responses")
      elasticQueries.queries.zipWithIndex.foreach { case (q, i) =>
        val leg = i + 1
        val sql = mapper.readTree(q.query).path("sql").asText()
        val sourceNames: Seq[String] =
          app.softnetwork.elastic.sql.parser.Parser(sql) match {
            case Right(sel: SingleSearch) =>
              val fields = sel.select.fieldsWithComputedAliases
              if (fields.size == 1 && fields.head.identifier.identifierName == "*")
                Seq("id", "category", "tag", "amount")
              else fields.map(_.identifier.identifierName)
            case _ => Seq(s"leg$leg")
          }
        val response = responses.addObject()
        val hits = response.putObject("hits")
        hits.putObject("total").put("value", sourceNames.size)
        val hitsArray = hits.putArray("hits")
        val source = hitsArray.addObject().putObject("_source")
        sourceNames.foreach(n => source.put(n, s"$n@$leg"))
      }
      ElasticResult.success(Some(root))
    }

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] = {
      msearchCalls.incrementAndGet()
      lastMultiQuery = Some(elasticQueries)
      legHitsResponse(elasticQueries)
    }

    // `searchAsync` reaches the ASYNC seam, never the sync one — a counter on `executeMultiSearch`
    // alone reads zero for every async row and would make the async pins vacuous.
    override private[client] def executeMultiSearchAsync(
      elasticQueries: ElasticQueries
    )(implicit ec: ExecutionContext): scala.concurrent.Future[ElasticResult[Option[JsonNode]]] =
      scala.concurrent.Future.successful {
        msearchCalls.incrementAndGet()
        lastMultiQuery = Some(elasticQueries)
        legHitsResponse(elasticQueries)
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

  /** Counts how many times the per-statement `SingleSearch` seam is entered (issue #355). The
    * `MultiSearch` seam resolves every leg once; anything beyond that is the per-leg fold resolving
    * what it was already handed.
    */
  private class ResolveCountingClient extends RecordingClient {
    val singleResolutions = new AtomicInteger(0)

    override private[client] def resolveWithSchema(
      single: SingleSearch
    ): ElasticResult[SingleSearch] = {
      singleResolutions.incrementAndGet()
      super.resolveWithSchema(single)
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
    * gets — and they must agree on what SQL says, which is where issue #354 comes in.
    *
    * SQL-92 §7.10 matches set-operation branches BY ORDINAL POSITION: same degree, i-th column
    * type-compatible with i-th column, and the result takes the FIRST branch's names. Names play no
    * part in the matching. `CORRESPONDING` — the optional clause that asks for name-based matching
    * — is the proof, because nobody adds an opt-in for the behaviour they already have.
    *
    * Story 22.6 shipped one BY-NAME contract for all three routes, which made them agree with each
    * other but not with SQL; #354 is the remaining half, and the rows below are its three measured
    * shapes. Every value NAMES ITS OWN COLUMN (`tag@2`), so the oracle is independent of the
    * mapping under test: a value that lands under the wrong name says so.
    *
    * 🔴 The trap the fix had to avoid, recorded because a 22.6 draft fell into it: "position" means
    * the index into the branch's DECLARED projection, never an index into whatever order the row
    * map enumerates. Deriving it from `row.keys` put values under the wrong column names on real ES
    * 8.18 — HTTP 200, and harder to detect than the raggedness it replaced.
    */
  "The per-leg route" should "match a REORDERED branch POSITIONALLY, as SQL-92 does" in {
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
    // … and column i holds column i OF ITS OWN BRANCH. Branch 2 declared `tag` first, so `tag@2`
    // is what the result's FIRST column holds — reordering the projection is how an analyst aligns
    // two differently-named schemas, and a by-name lookup silently undid it.
    response.results.map(_("category")) shouldBe Seq("category@1", "tag@2")
    response.results.map(_("tag")) shouldBe Seq("tag@1", "category@2")
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
    // purpose — so the leg DECLARES no projection and there is no position to match. Its row
    // arrives in `_source` order; a positional `zip` over that order put the ID under `category`
    // and TRUNCATED the row to the first branch's width, losing two columns. HTTP 200 both ways.
    // An opaque branch therefore keeps the by-name match, which is the only one it admits.
    starRow("category") shouldBe "category@2"
    starRow("tag") shouldBe "tag@2"
    starRow("id") shouldBe "id@2"
    starRow("amount") shouldBe "amount@2"
    // the first branch's columns lead; whatever the opaque leg carried beyond them follows
    starRow.keys.toSeq.take(2) shouldBe Seq("category", "tag")
  }

  /** Issue #354, the shape the analyst sees most: an alias per branch. The first branch names the
    * result, so branch 2's single column becomes `x` — where a by-name lookup answered `{x -> null,
    * y -> …}` and, on the one-shot route, did it to branch 1's OWN rows too.
    */
  it should "give a branch that aliases its column differently the FIRST branch's name" in {
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
    // 🔴 not `{x -> null, y -> "y@2"}`: the column the analyst asked for holds branch 2's value,
    // and the branch's own alias does not leak into the result as a second column.
    response.results(1) shouldBe ListMap[String, Any]("x" -> "y@2")
  }

  /** Issue #354 case 1 — the shape BOTH parse-time guards admit (same degree, same types) and that
    * still came back with a NULL: a branch may project the same column twice, and positionally that
    * is perfectly well defined. A row map cannot hold two `a` keys, so the leg's row carries one —
    * and both result columns read it.
    */
  it should "feed BOTH result columns from a branch that projects one column twice" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement("SELECT a, b FROM x UNION ALL SELECT a, a FROM y")
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.scrollCalls.get() shouldBe 2
    response.results should have size 2
    response.results.head shouldBe ListMap[String, Any]("a" -> "a@1", "b" -> "b@1")
    // column 2 was NULL before #354 — branch 2 never names a column `b`, and nothing looked at
    // what it DID name column 2.
    response.results(1) shouldBe ListMap[String, Any]("a" -> "a@2", "b" -> "a@2")
  }

  /** 🔴 THE OTHER ROUTE, on the shape that exposed it. A bounded `UNION ALL` is ONE `_msearch`, and
    * every leg of its response used to be parsed with the FIRST branch's alias map — but
    * `MultiSearch.fieldAliases` merges the branches' maps keyed by SOURCE field, so `id AS x` and
    * `id AS y` collapse to ONE entry and the survivor renames BOTH legs' `id`. MEASURED on real ES
    * 8.18: every row, branch 1's own included, came back `{x -> null, y -> …}`.
    *
    * The fixture is the mechanism: the stub answers each leg with a `_source` keyed by that leg's
    * SOURCE field, valued `"<source>@<leg>"`, which is exactly what Elasticsearch returns and what
    * makes the alias mapping load-bearing. A stub that answered output names would have been green
    * against the defect.
    */
  "The one-shot _msearch route" should "apply each leg's OWN alias map, then the first branch's names" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement("SELECT id AS x FROM l LIMIT 5 UNION ALL SELECT id AS y FROM r LIMIT 5")
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.msearchCalls.get() shouldBe 1
    client.scrollCalls.get() shouldBe 0
    response.results shouldBe Seq(
      ListMap[String, Any]("x" -> "id@1"),
      ListMap[String, Any]("x" -> "id@2")
    )
  }

  it should "match a REORDERED branch positionally too — the routes agree" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement(
        "SELECT category, tag FROM l LIMIT 5 UNION ALL SELECT tag, category FROM r LIMIT 5"
      )
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.msearchCalls.get() shouldBe 1
    response.results shouldBe Seq(
      ListMap[String, Any]("category" -> "category@1", "tag" -> "tag@1"),
      ListMap[String, Any]("category" -> "tag@2", "tag"      -> "category@2")
    )
  }

  /** 🔴 #354 case 2 SURVIVING BEHIND AN OPAQUE FIRST BRANCH. With no names to match against there
    * is nothing to RENAME to — but that is not a licence to hand every leg the MERGED alias map,
    * which is keyed by the SOURCE field and therefore keeps ONE of `x`/`y` and renames both later
    * legs with it. Each leg keeps its OWN projection, which is exactly what it would answer alone,
    * so the one-shot route and the per-leg route agree here too.
    */
  it should "still give each leg its OWN alias map when the FIRST branch is SELECT *" in {
    val client = new RecordingClient
    val response = client.search(
      SelectStatement(
        "SELECT * FROM t LIMIT 5 UNION ALL SELECT id AS x FROM l LIMIT 5 " +
        "UNION ALL SELECT id AS y FROM r LIMIT 5"
      )
    ) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.msearchCalls.get() shouldBe 1
    response.results should have size 3
    // the opaque branch keeps what Elasticsearch returned …
    response.results.head.keys.toSeq shouldBe Seq("id", "category", "tag", "amount")
    // … and each aliasing branch keeps ITS OWN column, not the other's. Before the fix both read
    // `{y -> …}` (or both `{x -> …}`, whichever alias the merge happened to keep).
    response.results(1) shouldBe ListMap[String, Any]("x" -> "id@2")
    response.results(2) shouldBe ListMap[String, Any]("y" -> "id@3")
  }

  /** Issue #355 — the per-leg route re-entered `search(leg)`, which resolves the leg a SECOND time.
    * `resolveWithSchema` is not a pure check: a leg carrying a WHERE subquery has its inner
    * statement EXECUTED by `SubqueryResolver`, uncached.
    *
    * 🔴 Counting inner searches would be VACUOUS here: phase one rewrites the subquery into
    * literals, so the second pass finds nothing left to execute and the count stays at 1 either
    * way. The assertion is therefore on the mechanism — how many times the per-statement seam was
    * entered at all (measured: reinstating `search(leg)` makes it 5).
    */
  "A UNION ALL executed per leg" should "resolve each leg ONCE — the seam's resolution is reused" in {
    val client = new ResolveCountingClient
    client.search(
      SelectStatement("SELECT a FROM x WHERE a IN (SELECT b FROM y) UNION ALL SELECT a FROM z")
    ) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.scrollCalls.get() shouldBe 2
    // TWO from the `MultiSearch` seam (one per leg) plus ONE for the inner statement
    // `SubqueryResolver` executes — and NONE from the fold. Re-entering `search(leg)` makes it 5
    // (measured), which is the whole of issue #355's second half.
    client.singleResolutions.get() shouldBe 3
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
