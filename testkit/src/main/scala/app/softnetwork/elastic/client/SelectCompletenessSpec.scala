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

import scala.language.implicitConversions

case class SelectRow(category: String, amount: Int, rnum: Option[Long] = None)

/** Regression test for issue #209: a row query with no `LIMIT` must return EVERY matching row.
  *
  * Same silent failure class as #205/#207, on the base hits: the one-shot search path emitted no
  * top-level `size`, so Elasticsearch returned its default 10 hits with HTTP 200 and no truncation
  * flag — a 45-row index reported 10 arbitrary rows. Un-LIMITed row queries are now routed through
  * the scroll path (which pages completely); this spec asserts full row counts on a multi-shard
  * index across the projection shapes that route: plain columns, `SELECT *`, script-field-only, and
  * window-enriched. An explicit LIMIT must keep its one-shot bound.
  */
trait SelectCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "select_completeness"

  /** 45 docs — far above the ES default of 10 hits. Category `cat_i` holds 3 docs with amounts
    * i*100 + 1..3, so row counts and contents are exact oracles.
    */
  private val categories = 15

  private val docsPerCategory = 3

  private val totalDocs = categories * docsPerCategory

  override def beforeAll(): Unit = {
    super.beforeAll()

    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "category": { "type": "keyword" },
        |    "amount":   { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(index, settings = settings).get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = (for {
      c <- 1 to categories
      d <- 1 to docsPerCategory
    } yield {
      val category = f"cat_$c%02d"
      s"""{"id":"${category}_$d","category":"$category","amount":${c * 100 + d}}"""
    }).toList

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = index,
      logEvery = 1000
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }

    client.refresh(index)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    super.afterAll()
  }

  "SELECT without LIMIT" should "return every row" in {
    client.searchAs[SelectRow](
      "SELECT category, amount FROM select_completeness"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size totalDocs.toLong
        rows.map(r => (r.category, r.amount)).toSet should have size totalDocs.toLong

        log.info(s"✓ ${rows.size} rows from $index without LIMIT")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "SELECT * without LIMIT" should "return every row" in {
    // searchAsUnchecked: the searchAs macro rejects SELECT * at compile time by design.
    client.searchAsUnchecked[SelectRow](
      SelectStatement("SELECT * FROM select_completeness")
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size totalDocs.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "script-field-only SELECT without LIMIT" should "return every row" in {
    // Raw `search` (no entity conversion): script_fields values come back wrapped in
    // Elasticsearch's per-field array on EVERY path (pre-existing — breaks searchAs
    // conversion of a script field to a scalar; tracked separately from #209), so this
    // asserts row completeness only.
    implicit val context: ConversionContext = NativeContext
    client.search(SelectStatement("SELECT UPPER(category) AS n FROM select_completeness")) match {
      case ElasticSuccess(response) =>
        response.results should have size totalDocs.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "window-enriched SELECT without LIMIT" should "return every base row" in {
    // Base-row completeness only — per-partition window coverage is #207's
    // WindowPartitionCompletenessSpec.
    client.searchAs[SelectRow](
      """SELECT
           category,
           amount,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rnum
         FROM select_completeness"""
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size totalDocs.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "SELECT with LIMIT" should "still bound the returned rows" in {
    client.searchAs[SelectRow](
      "SELECT category, amount FROM select_completeness LIMIT 7"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 7

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }
}
