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
import app.softnetwork.elastic.client.result._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** Story 20.9 / issue #251 — FROM-less SELECT handshake, Docker-free half.
  *
  * Covers routing + row assembly + connection-check semantics on `NopeClientApi`-derived fixtures.
  * Value truth lives in the testkit integration specs (semantics execute on real ES); here the
  * stubs RECORD and the tests ASSERT — never a matcher inside a stub.
  */
class FromlessSelectGatewaySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("fromless-select-gateway")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(5.seconds))

  private val mapper = new ObjectMapper()

  private val minimalMappingJson = """{"properties": {"name": {"type": "keyword"}}}"""

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def rows(result: ElasticResult[QueryResult]): Seq[ListMap[String, Any]] =
    result match {
      case ElasticSuccess(QueryRows(r, _)) => r
      case other                           => fail(s"expected QueryRows, got $other")
    }

  // ── Fixture B: a NopeClientApi whose search answers from a response QUEUE and which
  //    RECORDS index-admin calls. NopeClientApi returns benign no-op successes by default
  //    (NOT throws — corrected fact), so only the handshake-relevant members are overridden.
  private class StubHandshakeClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    val responses: mutable.Queue[String] = mutable.Queue.empty
    val created: mutable.Buffer[String] = mutable.Buffer.empty
    var createdMappings: Option[String] = None // pins AD-9: the keyword mapping MUST be passed
    val seeded: mutable.Buffer[(String, String, String)] = mutable.Buffer.empty
    val refreshed: mutable.Buffer[String] = mutable.Buffer.empty
    var existing: Boolean = false

    def respond(fieldsJson: String): Unit =
      responses.enqueue(
        s"""{"hits":{"total":{"value":1},"hits":[{"_index":"x","_id":"1",
           |"_source":{"dummy":"dummy"},"fields":$fieldsJson}]}}""".stripMargin
      )

    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
      ElasticResult.success(existing)
    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[app.softnetwork.elastic.sql.schema.TableAlias]
    ): ElasticResult[Boolean] = {
      created += index; createdMappings = mappings; existing = true; ElasticResult.success(true)
    }
    override private[client] def executeIndex(
      index: String,
      id: String,
      source: String,
      wait: Boolean
    ): ElasticResult[Boolean] = { seeded += ((index, id, source)); ElasticResult.success(true) }
    override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
      refreshed += index; ElasticResult.success(true)
    }
    override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] =
      Future.successful(
        if (responses.isEmpty) ElasticResult.success(None)
        else ElasticResult.success(Some(mapper.readTree(responses.dequeue())))
      )
  }

  // ── (A) nothing answers locally: bare NopeClientApi => the handshake FAILS (AC 4) ──
  "GatewayApi.run(SELECT 1)" should "never be answered engine-side (bare no-op client fails)" in {
    val client = new NopeClientApi {
      override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    }
    client.run("SELECT 1").futureValue match {
      case ElasticFailure(_) => succeed // ensure or search failed — no local answer exists
      case other             => fail(s"a FROM-less SELECT must not succeed without ES: $other")
    }
  }

  // ── (B) row assembly, unwrap, naming, LIMIT semantics (AC 1, 7) ────────────
  it should "assemble one unwrapped row under PD-2 names" in {
    val client = new StubHandshakeClient
    client.respond("""{"__c1":[1]}""")
    rows(client.run("SELECT 1").futureValue) shouldBe Seq(ListMap("1" -> 1))
    // ensure-flow ran exactly once and in order, with the keyword mapping (AD-9 — not dynamic).
    // The mapping reaches executeCreateIndex CONVERTED per ES version (MappingConverter may add
    // the ES-6 `_doc` type wrapper), so pin presence + content, not byte equality.
    client.created shouldBe Seq(GatewayApi.HandshakeIndex)
    client.createdMappings should not be empty
    client.createdMappings.get should include(""""dummy"""")
    client.createdMappings.get should include(""""keyword"""")
    client.seeded.map(_._2) shouldBe Seq(GatewayApi.HandshakeDocId)
    client.refreshed shouldBe Seq(GatewayApi.HandshakeIndex)

    client.respond("""{"x":[1],"s":["ok"]}""")
    rows(client.run("SELECT 1 AS x, 'ok' AS s").futureValue) shouldBe
    Seq(ListMap("x" -> 1, "s" -> "ok"))
    // memoized: no further create/seed/refresh
    client.created.size shouldBe 1
  }

  it should "honour LIMIT engine-side while still executing the round-trip" in {
    val client = new StubHandshakeClient
    client.respond("""{"__c1":[1]}""")
    rows(client.run("SELECT 1 LIMIT 100").futureValue).size shouldBe 1
    client.respond("""{"__c1":[1]}""")
    rows(client.run("SELECT 1 LIMIT 0").futureValue) shouldBe Seq.empty
    client.responses.isEmpty shouldBe true // LIMIT 0 still consumed a search (AD-8)
  }

  it should "run a multi-statement handshake batch and return the last result" in {
    val client = new StubHandshakeClient
    client.respond("""{"__c1":[1]}"""); client.respond("""{"__c1":[2]}""")
    rows(client.run("SELECT 1; SELECT 2").futureValue) shouldBe Seq(ListMap("2" -> 2))
  }

  // ── (C) connection-check semantics (AC 4) ──────────────────────────────────
  it should "fail with the propagated client error when the cluster is unreachable" in {
    val client = new StubHandshakeClient {
      override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
        ElasticResult.failure(
          ElasticError(message = "Connection refused: localhost/127.0.0.1:9200")
        )
    }
    client.run("SELECT 1").futureValue match {
      case ElasticFailure(error) =>
        error.message should include("Connection refused")
      // NOT pinned: statusCode (None is legitimate here — project_elastic_error_status_semantics)
      case other => fail(s"expected connection failure, got $other")
    }
  }

  it should "append the pre-creation guidance when the exists probe itself is denied (403)" in {
    // Read-only BI account, index never created: ES security can 403 the EXISTS action itself —
    // the OQ-6 guidance must reach that failure too (original status preserved, never invented).
    val client = new StubHandshakeClient {
      override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
        ElasticResult.failure(
          ElasticError(message = "security_exception: action denied", statusCode = Some(403))
        )
    }
    client.run("SELECT 1").futureValue match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(403) // the ORIGINAL status — no invention
        error.message should include("security_exception")
        error.message should include(GatewayApi.HandshakeIndex) // the guidance names the index
      case other => fail(s"expected guided 403, got $other")
    }
  }

  it should "append the pre-creation guidance when the create itself is denied (403)" in {
    val client = new StubHandshakeClient {
      override private[client] def executeCreateIndex(
        index: String,
        settings: String,
        mappings: Option[String],
        aliases: Seq[app.softnetwork.elastic.sql.schema.TableAlias]
      ): ElasticResult[Boolean] =
        ElasticResult.failure(
          ElasticError(message = "security_exception: create denied", statusCode = Some(403))
        )
    }
    client.run("SELECT 1").futureValue match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(403)
        error.message should include("security_exception")
        error.message should include(GatewayApi.HandshakeIndex)
      case other => fail(s"expected guided 403, got $other")
    }
  }

  // ── rejection path (AC 3, 12) — 20.4-independent ───────────────────────────
  it should "reject column references with a 400 and a named reason" in {
    val client = new StubHandshakeClient
    client.run("SELECT col").futureValue match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.message should include("Column reference 'col' requires a FROM clause")
      case other => fail(s"expected failure, got $other")
    }
  }

  // ── SHOW TABLES invisibility (AC 6), Docker-free half ──────────────────────
  it should "never list the handshake index in SHOW TABLES" in {
    val client = new StubHandshakeClient {
      override private[client] def executeGetAllMappings(
        indices: Seq[String]
      ): ElasticResult[Map[String, String]] =
        ElasticResult.success(
          Map(
            GatewayApi.HandshakeIndex -> minimalMappingJson,
            "orders"                  -> minimalMappingJson
          )
        )
    }
    rows(client.run("SHOW TABLES").futureValue).map(_("name")) shouldBe Seq("orders")
  }
}
