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
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

/** Regression test for issue #197: a paged extraction with no `ORDER BY` must return every row
  * exactly once on a MULTI-SHARD index.
  *
  * The failure mode this guards against is SILENT: a `_doc` sort without a cross-shard tiebreaker
  * (i.e. without a PIT on ES >= 7.12) returns HTTP 200 on every page and simply loses the rows
  * that tie at page boundaries — `_doc` values collide across shards by construction. A
  * single-shard fixture can never catch it, and the default test index template pins
  * `number_of_shards` to 1, so this spec creates its index with explicit multi-shard settings.
  */
trait ScrollCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "scroll_completeness"

  /** 3 shards x 1000 docs with pages of 100: every page boundary lands on `_doc` values that
    * exist in all three shards, so a missing tiebreaker loses rows immediately.
    */
  private val totalDocs = 3000

  override def beforeAll(): Unit = {
    super.beforeAll()

    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":    { "type": "keyword" },
        |    "value": { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(index, settings = settings).get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = (1 to totalDocs).map(i => s"""{"id":"$i","value":$i}""").toList

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

  "scrolling without ORDER BY over a multi-shard index" should "return every row exactly once" in {
    val source = client.scroll(
      SelectStatement(s"SELECT id, value FROM $index"),
      ScrollConfig(scrollSize = 100)
    )

    val rows = Await.result(source.runWith(Sink.seq), 5.minutes)

    rows should have size totalDocs.toLong

    val ids = rows.map(_._1("id").toString)
    ids.toSet should have size totalDocs.toLong

    log.info(s"✓ ${rows.size} rows, ${ids.toSet.size} distinct ids from $index (3 shards)")
  }

  it should "return every row exactly once with a small final page" in {
    // 3000 % 7 != 0 — exercises the partial-last-page path as well
    val source = client.scroll(
      SelectStatement(s"SELECT id FROM $index"),
      ScrollConfig(scrollSize = 7)
    )

    val rows = Await.result(source.runWith(Sink.seq), 5.minutes)

    rows should have size totalDocs.toLong
    rows.map(_._1("id").toString).toSet should have size totalDocs.toLong
  }
}
