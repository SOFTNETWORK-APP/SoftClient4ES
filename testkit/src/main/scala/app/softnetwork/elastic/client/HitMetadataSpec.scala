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
import app.softnetwork.elastic.client.spi.ElasticClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContext}
import scala.concurrent.duration._
import scala.language.implicitConversions

/** Result rows must never surface Elasticsearch hit metadata: `_index`, `_score` and `_sort` are
  * gone for good, and `_id` only appears when `elastic.include-document-id` is enabled (disabled by
  * default).
  *
  * `_id` is still carried internally through parsing — the ranking-window enrichment matches base
  * rows to their per-partition ordinals by document id — so this spec also pins exact ROW_NUMBER
  * ordinals on both the one-shot (LIMIT) and scroll-routed (no LIMIT, #209) paths to prove the
  * egress strip does not starve that lookup.
  */
trait HitMetadataSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  /** Both clients are instantiated straight from the SPI rather than through
    * [[ElasticClientFactory]]: the factory caches clients per cluster URL, so a second `create`
    * against the same cluster returns the first client regardless of configuration. The flag is
    * pinned explicitly on BOTH clients so an ambient `ELASTIC_INCLUDE_DOCUMENT_ID` in the
    * environment cannot flip the assertions.
    */
  private def spiClient(includeDocumentId: Boolean): ElasticClientApi =
    java.util.ServiceLoader
      .load(classOf[ElasticClientSpi])
      .iterator()
      .next()
      .client(
        ConfigFactory
          .parseString(s"elastic.include-document-id = $includeDocumentId")
          .withFallback(elasticConfig)
      )

  lazy val client: ElasticClientApi = spiClient(includeDocumentId = false)

  lazy val clientWithDocumentId: ElasticClientApi = spiClient(includeDocumentId = true)

  private val index = "hit_metadata"

  private val forbiddenKeys = Set("_index", "_score", "_sort")

  /** Category `cat_i` holds 4 docs with amounts i*100 + 1..4 — exact per-partition oracles. */
  private val categories = 5

  private val docsPerCategory = 4

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

    client.refresh(index).get shouldBe true
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    system.terminate()
    super.afterAll()
  }

  private def rowsOf(
    api: ElasticClientApi,
    sql: String
  ): Seq[ListMap[String, Any]] =
    api.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}")
    }

  "SELECT with LIMIT (one-shot path)" should "surface no hit metadata" in {
    val rows = rowsOf(client, s"SELECT id, category, amount FROM $index ORDER BY amount LIMIT 5")
    rows should have size 5
    rows.foreach { row =>
      row.keySet shouldBe Set("id", "category", "amount")
    }
  }

  "SELECT without LIMIT (scroll-routed path)" should "surface no hit metadata" in {
    val rows = rowsOf(client, s"SELECT id, category, amount FROM $index ORDER BY amount")
    rows should have size totalDocs.toLong
    rows.foreach { row =>
      row.keySet shouldBe Set("id", "category", "amount")
    }
  }

  "window-enriched SELECT" should "keep exact ordinals while surfacing no hit metadata" in {
    // One-shot (LIMIT ≤ max_result_window) — enrichResponseWithWindowValues egress
    val oneShot = rowsOf(
      client,
      s"""SELECT
            category,
            amount,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rnum
          FROM $index LIMIT $totalDocs"""
    )
    // Scroll-routed (no LIMIT, #209) — scrollWithWindowEnrichment egress
    val scrolled = rowsOf(
      client,
      s"""SELECT
            category,
            amount,
            ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rnum
          FROM $index"""
    )

    Seq("one-shot" -> oneShot, "scroll-routed" -> scrolled).foreach { case (path, rows) =>
      withClue(s"$path path: ") {
        rows should have size totalDocs.toLong
        rows.foreach { row =>
          row.keySet shouldBe Set("category", "amount", "rnum")
        }
        // The ordinal lookup keys on the internally carried `_id`: highest amount per
        // category must rank 1, next 2, … — an off ordinal means the strip starved it.
        rows.foreach { row =>
          val amount = row("amount").toString.toInt
          val expectedRank = docsPerCategory - (amount % 100) + 1
          row("rnum").toString.toLong shouldBe expectedRank.toLong
        }
      }
    }
  }

  "searchAsync" should "surface no hit metadata on the asynchronous path" in {
    implicit val ec: ExecutionContext = system.dispatcher
    val rows = Await.result(
      client.searchAsync(SelectStatement(s"SELECT id, category FROM $index LIMIT 5")),
      30.seconds
    ) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}")
    }
    rows should have size 5
    rows.foreach { row =>
      row.keySet shouldBe Set("id", "category")
    }
  }

  "explicit SELECT _id" should "surface the document id even when disabled" in {
    val rows = rowsOf(client, s"SELECT _id, id FROM $index LIMIT 5")
    rows should have size 5
    rows.foreach { row =>
      row.keySet shouldBe Set("_id", "id")
      row("_id") shouldBe row("id")
    }
  }

  "UNION ALL" should "surface no hit metadata" in {
    val rows = rowsOf(
      client,
      s"""SELECT id, category FROM $index WHERE category = 'cat_01'
          UNION ALL
          SELECT id, category FROM $index WHERE category = 'cat_02'"""
    )
    rows should have size (2L * docsPerCategory)
    rows.foreach { row =>
      row.keySet shouldBe Set("id", "category")
    }
  }

  "GROUP BY aggregation" should "surface no hit metadata" in {
    val rows = rowsOf(
      client,
      s"SELECT category, COUNT(*) AS cnt FROM $index GROUP BY category"
    )
    rows should have size categories.toLong
    rows.foreach { row =>
      (row.keySet & (forbiddenKeys + "_id")) shouldBe empty
    }
  }

  "include-document-id = true" should "surface _id — and only _id — on every path" in {
    // One-shot
    val oneShot =
      rowsOf(clientWithDocumentId, s"SELECT id, category FROM $index ORDER BY amount LIMIT 5")
    oneShot should have size 5
    // Scroll-routed
    val scrolled = rowsOf(clientWithDocumentId, s"SELECT id, category FROM $index")
    scrolled should have size totalDocs.toLong

    (oneShot ++ scrolled).foreach { row =>
      row.keySet shouldBe Set("id", "category", "_id")
      // Documents are bulk-indexed with idKey = "id", so the surfaced `_id` must match it
      row("_id") shouldBe row("id")
    }
  }
}
