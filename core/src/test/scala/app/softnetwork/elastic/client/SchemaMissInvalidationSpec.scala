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
import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table, TableAlias}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/** A remembered 404 schema miss is forgotten wherever an elasticsql DDL statement refreshes or
  * drops a schema-cache entry -- so "SELECT before the table exists, CREATE TABLE, SELECT" types
  * the second SELECT instead of running it with no schema for up to five minutes.
  *
  * Network-free. The client double keeps a set of "existing" indices and answers `GET <index>` the
  * way Elasticsearch does: a concrete name it does not hold is a 404, and so is a comma list with a
  * missing concrete member; a `*` part expands over what it holds. Its query renderer writes the
  * SQL of the statement that reached the conversion, so the OBSERVABLE is the temporal literal: the
  * space-separated spelling is normalised to the `T` form only when the schema was attached.
  */
class SchemaMissInvalidationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  implicit val system: ActorSystem = ActorSystem("schema-miss-invalidation-spec")
  implicit val context: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  private val spaceForm = "2026-06-04 00:00:00"
  private val isoForm = "2026-06-04T00:00:00"

  private def document(aliases: String = ""): String =
    s"""{"aliases":{$aliases},"mappings":{"properties":{
       |  "id":{"type":"keyword"},"event_ts":{"type":"date"}}},
       |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin

  private val schema: Schema = Table(
    "t",
    columns = List(Column("id", SQLTypes.Keyword), Column("event_ts", SQLTypes.Date))
  )

  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = testLogger

    /** The indices this "cluster" holds. */
    @volatile var indices: Set[String] = Set.empty

    /** Overrides for `GET <name>` (an alias), consulted first. */
    @volatile var getIndexOverride: PartialFunction[String, Option[String]] =
      PartialFunction.empty

    @volatile var cacheTtlMs: Long = 5 * 60 * 1000L
    override protected def schemaCacheTtlMs: Long = cacheTtlMs

    /** Fixed, so a row that shortens the CACHE's TTL still holds its misses for their full TTL. */
    override protected def schemaMissTtlMs: Long = 5 * 60 * 1000L

    @volatile var lastQuery: Option[ElasticQuery] = None
    @volatile var lookups: List[String] = Nil

    override private[client] implicit def singleSearchToJsonQuery(
      sqlSearch: SingleSearch
    )(implicit
      timestamp: Long,
      contextType: PainlessContextType = PainlessContextType.Query
    ): String = new ObjectMapper().createObjectNode().put("sql", sqlSearch.sql).toString

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = {
      lastQuery = Some(elasticQuery)
      ElasticResult.success(None)
    }

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      lookups = lookups :+ index
      if (getIndexOverride.isDefinedAt(index)) ElasticResult.success(getIndexOverride(index))
      else {
        val parts = index.split(",").map(_.trim).toList
        val members = parts.flatMap { part =>
          if (part.contains("*")) indices.filter(IndicesApi.patternMatches(part, _)).toList
          else if (indices.contains(part)) List(part)
          else Nil
        }.distinct
        val missingConcrete = parts.exists(p => !p.contains("*") && !indices.contains(p))
        if (members.isEmpty || missingConcrete) ElasticResult.success(None)
        else
          ElasticResult.success(
            Some(members.map(m => s""""$m":${document()}""").mkString("{", ",", "}"))
          )
      }
    }

    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[TableAlias]
    ): ElasticResult[Boolean] = {
      indices += index
      ElasticResult.success(true)
    }

    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
      indices -= index
      ElasticResult.success(true)
    }
  }

  /** Runs the observable statement over `from` through `api` and answers whether it was TYPED. */
  private def typed(api: SearchApi, recorder: RecordingClient, from: String): Boolean = {
    recorder.lastQuery = None
    api.search(
      SelectStatement(s"SELECT id FROM $from WHERE event_ts >= '$spaceForm' LIMIT 5")
    )
    val query = recorder.lastQuery.getOrElse(fail(s"no query was rendered for $from")).query
    if (query.contains(isoForm)) true
    else if (query.contains(spaceForm)) false
    else fail(s"neither literal spelling in $query")
  }

  private def typed(client: RecordingClient, from: String): Boolean = typed(client, client, from)

  "createIndex" should "forget a miss on the index it creates: the next statement is typed" in {
    val client = new RecordingClient
    typed(client, "t") shouldBe false
    client.schemaMissCount shouldBe 1
    val before = client.lookups.size
    typed(client, "t") shouldBe false // the miss stands: no lookup at all
    client.lookups.size shouldBe before

    client.createIndex("t") shouldBe ElasticSuccess(true)
    client.schemaMissCount shouldBe 0
    typed(client, "t") shouldBe true
  }

  it should "forget a miss on a PATTERN the new index matches" in {
    val client = new RecordingClient
    typed(client, "logs-*") shouldBe false
    client.schemaMissCount shouldBe 1

    client.createIndex("logs-1") shouldBe ElasticSuccess(true)
    client.schemaMissCount shouldBe 0
    typed(client, "logs-*") shouldBe true
  }

  it should "forget a miss on a LIST naming the new index" in {
    val client = new RecordingClient
    client.indices = Set("a")
    typed(client, "a, b") shouldBe false // `b` is missing: the list is a 404
    client.schemaMissCount shouldBe 1

    client.createIndex("b") shouldBe ElasticSuccess(true)
    client.schemaMissCount shouldBe 0
    typed(client, "a, b") shouldBe true
  }

  it should "keep the misses the new index does not answer" in {
    val client = new RecordingClient
    Seq("u", "metrics-*", "x, y", "t2").foreach(typed(client, _))
    client.schemaMissCount shouldBe 4

    client.createIndex("t") shouldBe ElasticSuccess(true)
    client.schemaMissCount shouldBe 4 // `t2` is not `t`, `metrics-*` and `x,y` do not match it
    val before = client.lookups.size
    typed(client, "u") shouldBe false
    typed(client, "metrics-*") shouldBe false
    client.lookups.size shouldBe before // still remembered: no lookup
  }

  it should "not forget anything when the index was not created" in {
    val client = new RecordingClient {
      override private[client] def executeCreateIndex(
        index: String,
        settings: String,
        mappings: Option[String],
        aliases: Seq[TableAlias]
      ): ElasticResult[Boolean] = ElasticResult.success(false)
    }
    typed(client, "t") shouldBe false
    client.createIndex("t") shouldBe ElasticSuccess(false)
    client.schemaMissCount shouldBe 1
  }

  "every DDL site that refreshes or drops a schema-cache entry" should "forget the misses it makes stale" in {
    // updateSchema = ALTER TABLE; invalidateSchema = DROP TABLE and the REPL `refresh <index>`;
    // deleteIndex reaches invalidateSchema; invalidateAllSchemas = the REPL `refresh`.
    val sites: Seq[(String, RecordingClient => Unit)] = Seq(
      "updateSchema"         -> (c => c.updateSchema("t", schema)),
      "invalidateSchema"     -> (c => c.invalidateSchema("t")),
      "deleteIndex"          -> (c => c.deleteIndex("t") shouldBe ElasticSuccess(true)),
      "invalidateAllSchemas" -> (c => c.invalidateAllSchemas())
    )
    sites.foreach { case (site, ddl) =>
      withClue(s"$site: ") {
        val client = new RecordingClient
        typed(client, "t") shouldBe false
        typed(client, "t*") shouldBe false
        typed(client, "s, t") shouldBe false
        typed(client, "unrelated") shouldBe false
        client.schemaMissCount shouldBe 4
        client.indices = Set("s", "t") // the index exists by the time the DDL refreshes it
        ddl(client)
        if (site == "invalidateAllSchemas") client.schemaMissCount shouldBe 0
        else client.schemaMissCount shouldBe 1 // `unrelated` stays
        if (site != "deleteIndex") {
          typed(client, "t") shouldBe true
          typed(client, "t*") shouldBe true
          typed(client, "s, t") shouldBe true
        }
      }
    }
  }

  it should "forget a miss an ALIAS answered, when the index it resolved from is refreshed" in {
    val client = new RecordingClient
    client.indices = Set("events", "archive")
    client.cacheTtlMs = 0L // the alias entry expires at once, so the next statement re-reads it
    client.getIndexOverride = { case "events_alias" =>
      Some(s"""{"events":${document(""""events_alias":{}""")}}""")
    }
    typed(client, "events_alias") shouldBe true // resolved from `events`
    client.schemaMissCount shouldBe 0
    client.getIndexOverride = { case "events_alias" => // now over two indices: ambiguous
      Some(
        s"""{"events":${document(""""events_alias":{}""")},"archive":${document(
          """"events_alias":{}"""
        )}}"""
      )
    }
    typed(client, "events_alias") shouldBe false
    client.schemaMissCount shouldBe 1

    client.invalidateSchema("events") // the CONCRETE index: drops the alias entry, and its miss
    client.schemaMissCount shouldBe 0
  }

  "a partitioned CREATE TABLE through the gateway" should
  "forget a miss on the table's name and on the patterns it matches" in {
    // the template answers the table's lookup (`fetchSchemaFromES` falls back to it)
    val client = new RecordingClient {
      override private[client] def executeVersion(): ElasticResult[String] =
        ElasticResult.success("8.18.3")
      override private[client] def executeCreatePipeline(
        pipelineName: String,
        pipelineDefinition: String
      ): ElasticResult[Boolean] = ElasticResult.success(true)
      override private[client] def executeCreateComposableTemplate(
        templateName: String,
        templateDefinition: String
      ): ElasticResult[Boolean] = ElasticResult.success(true)
    }
    typed(client, "p") shouldBe false
    typed(client, "p*") shouldBe false
    typed(client, "unrelated") shouldBe false
    client.schemaMissCount shouldBe 3
    Await.result(
      client.run(
        "CREATE TABLE p (id KEYWORD, event_ts TIMESTAMP, PRIMARY KEY (id)) PARTITION BY event_ts (MONTH)"
      ),
      10.seconds
    ) shouldBe a[ElasticSuccess[_]]
    client.schemaMissCount shouldBe 1 // `unrelated` stays
  }

  "ElasticClientDelegator" should "forget the miss where it is held: in the delegate" in {
    wrapperRows(rec => new ElasticClientDelegator { override val delegate: ElasticClientApi = rec })
  }

  "MetricsElasticClient" should "forget the miss where it is held: in the delegate" in {
    wrapperRows(rec => new MetricsElasticClient(rec, new MetricsCollector()))
  }

  /** Every statement through a wrapper is resolved by its delegate (`search` / `run` delegate), so
    * the miss lives in the DELEGATE's map; every DDL entry point of the wrapper must reach it.
    */
  private def wrapperRows(wrap: RecordingClient => ElasticClientApi): Unit = {
    val sites: Seq[(String, ElasticClientApi => Unit)] = Seq(
      "createIndex"          -> (w => w.createIndex("t") shouldBe ElasticSuccess(true)),
      "updateSchema"         -> (w => w.updateSchema("t", schema)),
      "invalidateSchema"     -> (w => w.invalidateSchema("t")),
      "invalidateAllSchemas" -> (w => w.invalidateAllSchemas())
    )
    sites.foreach { case (site, ddl) =>
      withClue(s"$site: ") {
        val recorder = new RecordingClient
        val wrapper = wrap(recorder)
        typed(wrapper, recorder, "t") shouldBe false
        recorder.schemaMissCount shouldBe 1 // held by the delegate...
        wrapper.schemaMissCount shouldBe 0 // ...never by the wrapper
        if (site != "createIndex") recorder.indices = Set("t")
        ddl(wrapper)
        recorder.schemaMissCount shouldBe 0
        typed(wrapper, recorder, "t") shouldBe true
      }
    }
  }
}
