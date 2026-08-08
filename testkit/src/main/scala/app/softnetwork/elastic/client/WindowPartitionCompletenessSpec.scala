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

case class PartitionRow(
  category: String,
  amount: Int,
  rnum: Option[Long] = None,
  top_amount: Option[Int] = None
)

/** Regression test for issue #207: a window `PARTITION BY` must cover EVERY partition.
  *
  * Same silent failure class as #205, on the window path: the partition `terms` aggregation had no
  * explicit `size`, so Elasticsearch computed only its default 10 partition buckets — with more
  * than 10 distinct partition values, rows in partitions 11+ silently got no window value. Every
  * other window fixture has <= 5 partitions, which is why this went unnoticed — this spec asserts
  * window-value coverage across 15 partitions, on a multi-shard index.
  */
trait WindowPartitionCompletenessSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "window_partition_completeness"

  /** 15 partitions — above the ES `terms` default of 10 buckets. Category `cat_i` holds 3 docs
    * with amounts i*100 + 1..3, so every per-partition window value is an exact oracle.
    */
  private val categories = 15

  private val docsPerCategory = 3

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

  // Both queries carry an explicit statement LIMIT: without one, the non-scroll search
  // path emits no top-level `size` and Elasticsearch returns its default 10 base hits —
  // a separate, pre-existing truncation this spec must not conflate with the partition
  // coverage under test (the statement LIMIT does NOT shrink the partition terms).

  "ROW_NUMBER over more than 10 partitions" should "rank rows in every partition" in {
    client.searchAs[PartitionRow](
      """SELECT
           category,
           amount,
           ROW_NUMBER() OVER (PARTITION BY category ORDER BY amount DESC) AS rnum
         FROM window_partition_completeness
         LIMIT 100"""
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size (categories * docsPerCategory).toLong

        val ranked = rows.filter(_.rnum.isDefined)
        ranked.map(_.category).distinct should have size categories.toLong

        ranked.groupBy(_.category).foreach { case (category, group) =>
          group.flatMap(_.rnum).sorted shouldBe (1L to docsPerCategory.toLong)
          val c = category.stripPrefix("cat_").toInt
          group.find(_.rnum.contains(1L)).map(_.amount) shouldBe Some(c * 100 + docsPerCategory)
        }

        log.info(s"✓ ${ranked.map(_.category).distinct.size} partitions ranked from $index")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "FIRST_VALUE over more than 10 partitions" should "enrich rows in every partition" in {
    client.searchAs[PartitionRow](
      """SELECT
           category,
           amount,
           FIRST_VALUE(amount) OVER (PARTITION BY category ORDER BY amount DESC) AS top_amount
         FROM window_partition_completeness
         LIMIT 100"""
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size (categories * docsPerCategory).toLong

        rows.foreach { row =>
          val c = row.category.stripPrefix("cat_").toInt
          withClue(s"row ${row.category}/${row.amount}: ") {
            row.top_amount shouldBe Some(c * 100 + docsPerCategory)
          }
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }
}
