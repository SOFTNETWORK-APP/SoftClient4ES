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

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.language.implicitConversions

/** Issue #276 -- the core seam: `SearchApi.search` / `searchAsync` / `ScrollApi.scroll` resolve the
  * temporal literals of a statement against the mapped `date` columns BEFORE rendering the
  * Elasticsearch query. Network-free: a `NopeClientApi` records the `ElasticQuery` it would have
  * sent and counts schema lookups; the schema cache is seeded through the public `updateSchema`.
  *
  * Covered entry points (AC 5): `search` (single + UNION ALL), `searchAsync`, `scroll`. The
  * schema-absent path (no schema, several sources, wildcard source) is asserted verbatim, and the
  * lookup is asserted CACHED (issue #306 made it unconditional for a single concrete index; it is
  * still SKIPPED for a multi-source or wildcard FROM, asserted below).
  */
class TemporalLiteralSearchSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  implicit val system: ActorSystem = ActorSystem("temporal-literal-search-spec")
  implicit val ec: ExecutionContext = system.dispatcher
  implicit val context: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  private val schema: Schema = Table(
    "events",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("event_ts", SQLTypes.Date),
      Column("label", SQLTypes.Keyword),
      Column("amount", SQLTypes.Int)
    )
  )

  /** Records the query the client would send, and how many schema lookups it made.
    *
    * `NopeClientApi` renders every statement as `match_all`; this client renders the SQL of the
    * statement that REACHED the conversion instead (as a JSON document, so `validateJson` accepts
    * it) -- which is exactly what the assertions below need to see. Statements carry a `LIMIT` so
    * the one-shot path is taken (#209 routes an un-LIMITed row query through scroll).
    */
  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = testLogger

    /** Set to 0 by a test to expire the negative cache immediately. */
    @volatile var missTtlMs: Long = 5 * 60 * 1000L
    override protected def temporalLiteralSchemaMissTtlMs: Long = missTtlMs

    override private[client] implicit def singleSearchToJsonQuery(
      sqlSearch: SingleSearch
    )(implicit
      timestamp: Long,
      contextType: PainlessContextType = PainlessContextType.Query
    ): String = new ObjectMapper().createObjectNode().put("sql", sqlSearch.sql).toString

    @volatile var lastQuery: Option[ElasticQuery] = None
    @volatile var lastMultiQuery: Option[ElasticQueries] = None
    @volatile var schemaLookups: Int = 0

    override def loadSchema(index: String): ElasticResult[Schema] = {
      schemaLookups += 1
      super.loadSchema(index)
    }

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = {
      lastQuery = Some(elasticQuery)
      ElasticResult.success(None)
    }

    override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] = Future {
      lastQuery = Some(elasticQuery)
      ElasticResult.success(None)
    }

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] = {
      lastMultiQuery = Some(elasticQueries)
      ElasticResult.success(None)
    }
  }

  private def seeded(): RecordingClient = {
    val client = new RecordingClient
    client.updateSchema("events", schema)
    client
  }

  private val spaceForm = "2026-06-04 00:00:00.000000"
  private val isoForm = "2026-06-04T00:00:00.000000"

  "search" should "render the T-separated literal for a date column when the schema is known" in {
    val client = seeded()
    client.search(SelectStatement(s"SELECT id FROM events WHERE event_ts >= '$spaceForm' LIMIT 5"))
    val query = client.lastQuery.getOrElse(fail("no query was rendered")).query
    query should include(isoForm)
    query should not include spaceForm
    client.schemaLookups shouldBe 1
  }

  it should "leave a keyword column compared to a date-shaped string untouched" in {
    val client = seeded()
    client.search(SelectStatement(s"SELECT id FROM events WHERE label = '$spaceForm' LIMIT 5"))
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
  }

  it should "forward the literal verbatim when the schema cannot be loaded, and remember the miss" in {
    val client = new RecordingClient // no seed: getIndex and getTemplate both answer None
    client.search(SelectStatement(s"SELECT id FROM events WHERE event_ts >= '$spaceForm' LIMIT 5"))
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.schemaLookups shouldBe 1
    // the failed lookup is remembered: a second statement on the same source costs no round trip
    client.search(SelectStatement(s"SELECT id FROM events WHERE event_ts < '$spaceForm' LIMIT 5"))
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.schemaLookups shouldBe 1
    // ... until the miss expires and the schema is available
    client.updateSchema("events", schema)
    client.missTtlMs = 0L
    client.search(SelectStatement(s"SELECT id FROM events WHERE event_ts < '$spaceForm' LIMIT 5"))
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(isoForm)
  }

  it should "remember only 404 misses, never a transient failure" in {
    val client = new RecordingClient {
      override def loadSchema(index: String): ElasticResult[Schema] =
        if (index.startsWith("flaky")) {
          schemaLookups += 1
          ElasticResult.failure(
            ElasticError(
              message = "cluster hiccup",
              statusCode = Some(503),
              index = Some(index),
              operation = Some("loadSchema")
            )
          )
        } else super.loadSchema(index)
    }
    val statement =
      SelectStatement(s"SELECT id FROM flaky_events WHERE event_ts >= '$spaceForm' LIMIT 5")
    client.search(statement)
    client.search(statement)
    client.schemaLookups shouldBe 2 // a 503 is retried on the next statement
    client.temporalLiteralSchemaMissCount shouldBe 0
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
  }

  it should "bound the negative cache: purge expired misses above the threshold, clear it above the cap" in {
    val expiring = new RecordingClient
    expiring.missTtlMs = 0L // every miss is expired at once
    (1 to 300).foreach { i =>
      expiring.search(
        SelectStatement(s"SELECT id FROM unknown_$i WHERE event_ts >= '$spaceForm' LIMIT 5")
      )
    }
    expiring.temporalLiteralSchemaMissCount should be < 257 // the purge ran at least once

    val flooding = new RecordingClient // default TTL: nothing expires, only the cap can act
    (1 to 1100).foreach { i =>
      flooding.search(
        SelectStatement(s"SELECT id FROM probe_$i WHERE event_ts >= '$spaceForm' LIMIT 5")
      )
    }
    flooding.temporalLiteralSchemaMissCount should be <= 1024
  }

  it should "resolve the schema through an alias over one index, and treat an alias over several as unknown" in {
    val events =
      """{"aliases":{"events_alias":{},"multi_alias":{}},"mappings":{"properties":{
        |  "id":{"type":"keyword"},"event_ts":{"type":"date"}}},
        |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin
    val archive = events.replace("\"events_alias\":{},", "")
    val client = new RecordingClient {
      // what `GET /<alias>` answers: the concrete index documents keyed by THEIR names
      override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
        index match {
          case "events_alias" => ElasticResult.success(Some(s"""{"events":$events}"""))
          case "multi_alias" =>
            ElasticResult.success(Some(s"""{"events":$events,"archive":$archive}"""))
          case _ => ElasticResult.success(None)
        }
    }
    client.search(
      SelectStatement(s"SELECT id FROM events_alias WHERE event_ts >= '$spaceForm' LIMIT 5")
    )
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(isoForm)
    client.temporalLiteralSchemaMissCount shouldBe 0 // a resolved alias is never a miss
    client.getIndex("multi_alias") shouldBe ElasticSuccess(None) // ambiguous: not found
    val before = client.schemaLookups
    client.search(
      SelectStatement(s"SELECT id FROM multi_alias WHERE event_ts >= '$spaceForm' LIMIT 5")
    )
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.schemaLookups shouldBe (before + 1)
    client.temporalLiteralSchemaMissCount shouldBe 1
    // R4-19: the ambiguous alias is BOUNDED -- a second statement costs no further lookup
    client.search(
      SelectStatement(s"SELECT id FROM multi_alias WHERE event_ts < '$spaceForm' LIMIT 5")
    )
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.schemaLookups shouldBe (before + 1)
  }

  it should "drop a cached alias schema when the concrete index it resolved from is invalidated" in {
    // R4-21: without this an ALTER TABLE on the concrete index would leave a stale mapping (hence
    // a stale date `format`) reachable through the alias for the rest of the TTL.
    val events =
      """{"aliases":{"events_alias":{}},"mappings":{"properties":{
        |  "id":{"type":"keyword"},"event_ts":{"type":"date"}}},
        |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin
    var fetches = 0
    val client = new RecordingClient {
      override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
        if (index == "events_alias") {
          fetches += 1
          ElasticResult.success(Some(s"""{"events":$events}"""))
        } else ElasticResult.success(None)
    }
    val statement =
      SelectStatement(s"SELECT id FROM events_alias WHERE event_ts >= '$spaceForm' LIMIT 5")
    client.search(statement)
    client.search(statement)
    fetches shouldBe 1 // cached
    client.invalidateSchema("events") // the CONCRETE index, not the alias
    client.search(statement)
    fetches shouldBe 2 // the alias entry went with it
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(isoForm)
  }

  // 🔴 RETARGETED by issue #306, not relaxed. This used to assert `schemaLookups shouldBe 0` for a
  // statement with no temporal candidate — a real BIDC-4 performance guarantee, and one that #306
  // had to give up: the schema is now attached to the AST for EVERY statement over a single
  // concrete index, because `GenericIdentifier.baseType` needs it and a cast over a column emitted
  // no conversion at all without it.
  //
  // The cost is stated rather than hidden: ONE lookup per index per cache TTL, served from
  // `loadSchema`'s 5-minute cache. The test now pins that cost — the lookup happens ONCE and is
  // then cached — so a regression to a per-statement fetch still fails here.
  it should "look the schema up even when the WHERE carries no candidate literal" in {
    val client = seeded()
    client.search(SelectStatement("SELECT id FROM events WHERE amount > 10 LIMIT 5"))
    client.lastQuery shouldBe defined
    client.schemaLookups shouldBe 1
    client.search(SelectStatement("SELECT id FROM events LIMIT 5"))
    // `schemaLookups` counts CALLS, and this fixture overrides `loadSchema`, so it is one per
    // statement by construction — it cannot show the COST.
    client.schemaLookups shouldBe 2
  }

  it should "cost ONE round trip per index per TTL, not one per statement" in {
    // The number the #306 guarantee is actually about. `schemaLookups` above counts calls; this
    // counts real fetches through the production cache, using the same local-override pattern as
    // the alias test above (an unseeded client, so the first statement genuinely misses).
    var fetches = 0
    val client = new RecordingClient {
      override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
        fetches += 1
        ElasticResult.success(
          Some(
            """{"mappings":{"properties":{"id":{"type":"keyword"},"event_ts":{"type":"date"},
              |"label":{"type":"keyword"},"amount":{"type":"integer"}}},
              |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin
          )
        )
      }
    }
    client.search(SelectStatement("SELECT id FROM events WHERE amount > 10 LIMIT 5"))
    client.search(SelectStatement("SELECT id FROM events LIMIT 5"))
    client.search(SelectStatement("SELECT id FROM events WHERE amount > 20 LIMIT 5"))
    fetches shouldBe 1 // three statements, ONE round trip - a per-statement regression fails here
  }

  it should "forward the literal verbatim over several sources or a wildcard source" in {
    val client = seeded()
    client.search(
      SelectStatement(s"SELECT id FROM events, archive WHERE event_ts >= '$spaceForm' LIMIT 5")
    )
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.search(SelectStatement(s"SELECT id FROM events* WHERE event_ts >= '$spaceForm' LIMIT 5"))
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(spaceForm)
    client.schemaLookups shouldBe 0
  }

  it should "reject an unparseable literal against a date column with a 400 naming the literal and the field" in {
    val client = seeded()
    client.search(
      SelectStatement("SELECT id FROM events WHERE event_ts >= 'not-a-date' LIMIT 5")
    ) match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("search")
        error.index shouldBe Some("events")
        error.message should include("'not-a-date'")
        error.message should include("'event_ts'")
        error.message should not include "search_phase_execution_exception"
      case ElasticSuccess(other) => fail(s"expected a rejection, got $other")
    }
    client.lastQuery shouldBe None // never reached the client
  }

  it should "resolve every request of a UNION ALL" in {
    val client = seeded()
    client.search(
      SelectStatement(
        s"SELECT id FROM events WHERE event_ts >= '$spaceForm' UNION ALL " +
        "SELECT id FROM events WHERE event_ts < '2026-06-01 00:00:00'"
      )
    )
    val multi = client.lastMultiQuery.getOrElse(fail("no multi-search was rendered")).multiQuery
    multi should include(isoForm)
    multi should include("2026-06-01T00:00:00")
    multi should not include spaceForm
  }

  "searchAsync" should "render the T-separated literal as well" in {
    val client = seeded()
    Await.result(
      client.searchAsync(
        SelectStatement(s"SELECT id FROM events WHERE event_ts >= '$spaceForm' LIMIT 5")
      ),
      10.seconds
    )
    client.lastQuery.getOrElse(fail("no query was rendered")).query should include(isoForm)
  }

  it should "reject an unparseable literal with a 400 without touching the client" in {
    val client = seeded()
    Await.result(
      client.searchAsync(SelectStatement("SELECT id FROM events WHERE event_ts = 'nope' LIMIT 5")),
      10.seconds
    ) match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.message should include("'nope'")
      case ElasticSuccess(other) => fail(s"expected a rejection, got $other")
    }
    client.lastQuery shouldBe None
  }

  /** RETARGETED, not deleted (issue #222 D-1, lead ruling 2026-09-06). This pinned the CONTRACT "an
    * unparseable literal is a 400 that reaches the caller", through the lens of the day: a
    * `Source.failed`, observable only by materialising the stream. That lens was the defect — BOTH
    * consumers of `scroll` wrap the Source into an `ElasticSuccess`, so `GatewayApi.run` answered
    * SUCCESS on the un-LIMITed row route and the rejection surfaced only if somebody ran the
    * stream. `scroll` now THROWS the same `ElasticError`, exactly as a client module's
    * extended-stats refusal does, and `run` reports `ElasticFailure(400)` (see
    * `GatewayRefusalBoundarySpec`). The contract is unchanged and now holds one hop earlier.
    */
  "scroll" should "throw the same 400 for an unparseable literal, before any stream exists" in {
    val client = seeded()
    val error = the[ElasticError] thrownBy client.scroll(
      SelectStatement("SELECT id FROM events WHERE event_ts >= 'nope'")
    )
    error.statusCode shouldBe Some(400)
    error.message should include("'nope'")
    error.message should include("'event_ts'")
    // The rejection is pre-execution: nothing was sent.
    client.lastQuery shouldBe None
  }
}
