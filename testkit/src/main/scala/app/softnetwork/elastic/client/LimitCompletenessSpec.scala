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
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}
import scala.language.implicitConversions

case class LimitRow(id: String, amount: Int)

/** Regression test for issue #224: a `SELECT` with an explicit `LIMIT` above the index's
  * `index.max_result_window` (ES default 10,000) failed outright, while the SAME query with no
  * `LIMIT` succeeded and returned every row — the one-shot search path issued a single search with
  * `size = LIMIT`, which Elasticsearch rejects whenever `from + size` exceeds the window.
  *
  * Row queries whose LIMIT window (`offset + limit`) exceeds the ES default window are now routed
  * through the scroll path bounded at `maxDocuments = offset + limit` (#209's routing, extended).
  * This spec asserts the asymmetry directly on a multi-shard index: LIMIT above the window and no
  * LIMIT both return their full expected row counts, small LIMITs keep their one-shot bound, and
  * ORDER BY + OFFSET stay exact through the scroll routing. On an index tuned BELOW the routing
  * threshold the one-shot rejection remains — the spec asserts it now names `max_result_window`
  * instead of surfacing an opaque failure.
  */
trait LimitCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "limit_completeness"

  /** 12,000 docs — above the ES default `index.max_result_window` of 10,000, so an explicit `LIMIT
    * 11000` can only be served by paging past the window. Zero-padded ids make ORDER BY + OFFSET
    * content exact oracles.
    */
  private val totalDocs = 12000

  /** A second index tuned BELOW the scroll-routing threshold: `max_result_window = 100`, so a
    * one-shot `LIMIT 250` (well under 10,000) is still rejected by Elasticsearch — the error must
    * be actionable, naming `max_result_window`.
    */
  private val loweredIndex = "limit_window_lowered"

  private val loweredDocs = 300

  private def indexDocs(indexName: String, count: Int, settings: String): Unit = {
    val mapping =
      """{
        |  "properties": {
        |    "id":     { "type": "keyword" },
        |    "amount": { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(indexName, settings = settings).get shouldBe true
    client.setMapping(indexName, mapping).get shouldBe true

    val docs = (1 to count).map { i =>
      s"""{"id":"id_${"%05d".format(i)}","amount":$i}"""
    }.toList

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = indexName,
      logEvery = 10000
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }

    client.refresh(indexName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    indexDocs(index, totalDocs, """{"number_of_shards": 3, "number_of_replicas": 0}""")
    indexDocs(
      loweredIndex,
      loweredDocs,
      """{"number_of_shards": 1, "number_of_replicas": 0, "index.max_result_window": 100}"""
    )
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    client.deleteIndex(loweredIndex)
    super.afterAll()
  }

  "SELECT with LIMIT above index.max_result_window" should "return exactly LIMIT rows" in {
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_completeness LIMIT 11000"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 11000
        rows.map(_.id).toSet should have size 11000

        log.info(s"✓ ${rows.size} rows from $index with LIMIT 11000 (window 10000)")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "the same SELECT without LIMIT" should "return every row (the #224 asymmetry pair)" in {
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_completeness"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size totalDocs.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "SELECT with LIMIT above the window through searchAsync" should "return exactly LIMIT rows" in {
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val context: ConversionContext = NativeContext
    Await.result(
      client.searchAsync(
        SelectStatement("SELECT id, amount FROM limit_completeness LIMIT 11000")
      ),
      Duration.Inf
    ) match {
      case ElasticSuccess(response) =>
        response.results should have size 11000

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "ORDER BY + OFFSET with a LIMIT window above the ES window" should "stay exact" in {
    // offset (1000) + limit (10500) = 11500 > 10000 → scroll-routed; ORDER BY id with
    // zero-padded ids makes the returned slice an exact oracle.
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_completeness ORDER BY id ASC LIMIT 10500 OFFSET 1000"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 10500
        rows.head.id shouldBe "id_01001"
        rows.last.id shouldBe "id_11500"
        rows.map(_.id) shouldBe sorted

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "SELECT with a small LIMIT" should "keep its one-shot bound" in {
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_completeness LIMIT 42"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 42

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "SELECT with LIMIT exactly at the window" should "succeed one-shot (boundary, not routed)" in {
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_completeness LIMIT 10000"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 10000

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "a one-shot rejection on an index tuned below the routing threshold" should
  "name max_result_window" in {
    client.searchAs[LimitRow](
      "SELECT id, amount FROM limit_window_lowered LIMIT 250"
    ) match {
      case ElasticSuccess(rows) =>
        fail(s"Expected a max_result_window rejection, got ${rows.size} rows")

      case ElasticFailure(error) =>
        error.message should include("max_result_window")
        error.message should include("LIMIT")

        log.info(s"✓ Actionable rejection: ${error.message}")
    }
  }
}
