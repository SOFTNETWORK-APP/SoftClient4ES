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

/** Integration tests for Story 14.1: `NULLS FIRST` / `NULLS LAST` on `ORDER BY`. Verifies that the
  * bridge's `missing` parameter actually controls null placement when executed against a live ES
  * instance.
  *
  * Fixture: 4 documents in the `sort_nulls` index with `bonus` set to (100, null, 50, missing). The
  * null and missing cases both translate to "no value" in ES; the `missing` sort parameter applies
  * to both.
  */
trait SortNullsSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "name":  { "type": "keyword" },
        |    "bonus": { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex("sort_nulls", mappings = None, aliases = Nil).get shouldBe true
    client.setMapping("sort_nulls", mapping).get shouldBe true

    val docs = List(
      """{"id":"1","name":"alice","bonus":100}""",
      """{"id":"2","name":"bob","bonus":null}""",
      """{"id":"3","name":"carol","bonus":50}""",
      """{"id":"4","name":"dave"}"""
    )

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = "sort_nulls",
      logEvery = 5
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }

    client.refresh("sort_nulls")
  }

  override def afterAll(): Unit = {
    client.deleteIndex("sort_nulls")
    super.afterAll()
  }

  "ORDER BY bonus DESC NULLS LAST" should "place non-null bonuses before null bonuses" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus DESC NULLS LAST"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val nonNullFirst = rows.takeWhile(_.bonus.isDefined)
        val nullsLast = rows.dropWhile(_.bonus.isDefined)
        nonNullFirst.map(_.name) shouldBe Seq("alice", "carol")
        nullsLast.map(_.name) should contain theSameElementsAs Seq("bob", "dave")
        log.info(s"NULLS LAST order: ${rows.map(_.name).mkString(", ")}")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "place null bonuses before non-null bonuses with NULLS FIRST" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus DESC NULLS FIRST"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val nullsFirst = rows.takeWhile(_.bonus.isEmpty)
        val nonNulls = rows.dropWhile(_.bonus.isEmpty)
        nullsFirst.map(_.name) should contain theSameElementsAs Seq("bob", "dave")
        nonNulls.map(_.name) shouldBe Seq("alice", "carol")
        log.info(s"NULLS FIRST order: ${rows.map(_.name).mkString(", ")}")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "default to nulls last for ASC when ordering is unspecified" in {
    val results = client.searchAs[SortNullsRow](
      "SELECT name, bonus FROM sort_nulls ORDER BY bonus ASC"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        rows.take(2).map(_.name) shouldBe Seq("carol", "alice")
        rows.drop(2).flatMap(_.bonus) shouldBe empty

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

}

case class SortNullsRow(name: String, bonus: Option[Int] = None)
