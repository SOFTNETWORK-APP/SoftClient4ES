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
  * lookup is asserted SKIPPED when the WHERE carries no candidate literal.
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

  it should "not look the schema up at all when the WHERE carries no candidate literal" in {
    val client = seeded()
    client.search(SelectStatement("SELECT id FROM events WHERE amount > 10 LIMIT 5"))
    client.lastQuery shouldBe defined
    client.schemaLookups shouldBe 0
    client.search(SelectStatement("SELECT id FROM events LIMIT 5"))
    client.schemaLookups shouldBe 0
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

  "scroll" should "fail the stream with the same 400 for an unparseable literal" in {
    val client = seeded()
    val stream = client.scroll(SelectStatement("SELECT id FROM events WHERE event_ts >= 'nope'"))
    val error = the[ElasticError] thrownBy Await.result(stream.runWith(Sink.seq), 10.seconds)
    error.statusCode shouldBe Some(400)
    error.message should include("'nope'")
    error.message should include("'event_ts'")
  }
}
