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
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

case class CategoryCount(category: String, cnt: Long)

/** Regression test for issue #205: `GROUP BY` with no `LIMIT` must return EVERY group.
  *
  * The failure mode this guards against is SILENT: without an explicit `size` on the `terms`
  * aggregation, Elasticsearch returns its default 10 buckets with HTTP 200 and no truncation flag,
  * so a 37-category index silently reports 10 plausible-looking groups. Every fixture in the other
  * suites has cardinality <= 10, which is exactly why this went unnoticed — this spec asserts group
  * count AND total doc coverage at cardinality > 10, on a multi-shard index.
  */
trait GroupByCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "group_by_completeness"

  /** 37 categories — far above the ES `terms` default of 10 buckets. Category `cat_i` holds exactly
    * `i` docs, so both the group count and every per-group count are exact oracles.
    */
  private val categories = 37

  private val totalDocs = (1 to categories).sum

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
      d <- 1 to c
    } yield {
      val category = f"cat_$c%02d"
      s"""{"id":"${category}_$d","category":"$category","amount":$d}"""
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

  "GROUP BY without LIMIT" should "return every group and account for every document" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong

        val counts = rows.map(r => r.category -> r.cnt).toMap
        counts should have size categories.toLong
        (1 to categories).foreach { c =>
          counts(f"cat_$c%02d") shouldBe c.toLong
        }

        rows.map(_.cnt).sum shouldBe totalDocs.toLong

        log.info(s"✓ ${rows.size} groups covering ${rows.map(_.cnt).sum} docs from $index")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "GROUP BY with LIMIT" should "still bound the number of groups" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category LIMIT 5"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 5

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "GROUP BY with ORDER BY on the bucket key and LIMIT" should "return the exact top groups" in {
    // Key ordering is exact on a multi-shard index; ordering by the metric (ORDER BY cnt DESC)
    // is NOT — the pushed-down terms top-N is shard-approximate by design (doc_count_error),
    // so a metric-ordered assertion here would flake on routing skew.
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category ORDER BY category DESC LIMIT 3"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(r => r.category -> r.cnt) shouldBe (0 until 3).map { i =>
          val c = categories - i
          f"cat_$c%02d" -> c.toLong
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }
}
