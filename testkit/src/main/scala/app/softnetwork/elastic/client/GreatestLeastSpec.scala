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

/** Integration tests for Story 14.2: `GREATEST` / `LEAST` over numeric columns. Verifies the
  * nested-ternary Painless script emitted by the bridge handles ANSI null-skipping semantics
  * correctly when executed against a live ES instance.
  *
  * Fixture: 4 documents in the `greatest_least` index with `(a, b, c)` set to:
  *   - alpha: 10, 20, 30 — none null, none missing
  *   - missing_b: 10, _, 30 — `b` field missing
  *   - null_a: null, 20, 30 — `a` explicitly null
  *   - all_null: null, null, null — every value null
  *
  * Both null and missing translate to "no value" in ES; the ternary tree skips both.
  */
trait GreatestLeastSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "name": { "type": "keyword" },
        |    "a":    { "type": "integer" },
        |    "b":    { "type": "integer" },
        |    "c":    { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex("greatest_least", mappings = None, aliases = Nil).get shouldBe true
    client.setMapping("greatest_least", mapping).get shouldBe true

    val docs = List(
      """{"id":"1","name":"alpha","a":10,"b":20,"c":30}""",
      """{"id":"2","name":"missing_b","a":10,"c":30}""",
      """{"id":"3","name":"null_a","a":null,"b":20,"c":30}""",
      """{"id":"4","name":"all_null","a":null,"b":null,"c":null}"""
    )

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = "greatest_least",
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

    client.refresh("greatest_least")
  }

  override def afterAll(): Unit = {
    client.deleteIndex("greatest_least")
    super.afterAll()
  }

  // ES script_fields wraps every value in an array (even scalars). The case class
  // binds `g`/`l` as `Option[List[java.lang.Integer]]` to faithfully reflect that
  // wire shape (a single-element array, or `[null]` when the Painless script
  // collapses to null). The helper below normalizes that to a plain `Option[Int]`
  // so the row assertions stay readable.
  private def scalar(v: Option[List[java.lang.Integer]]): Option[Int] =
    v.flatMap(_.headOption).flatMap(Option(_)).map(_.intValue)

  "GREATEST(a, b, c)" should "return the largest non-null value across three columns" in {
    val results = client.searchAs[GreatestLeastRow](
      "SELECT name, GREATEST(a, b, c) AS g FROM greatest_least"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val byName = rows.map(r => r.name -> scalar(r.g)).toMap
        byName("alpha") shouldBe Some(30)
        byName("missing_b") shouldBe Some(30)
        byName("null_a") shouldBe Some(30)
        byName("all_null") shouldBe None
        log.info(s"GREATEST result: $byName")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  "LEAST(a, b, c)" should "return the smallest non-null value across three columns" in {
    val results = client.searchAs[GreatestLeastRowLeast](
      "SELECT name, LEAST(a, b, c) AS l FROM greatest_least"
    )

    results match {
      case ElasticSuccess(rows) =>
        rows should have size 4
        val byName = rows.map(r => r.name -> scalar(r.l)).toMap
        byName("alpha") shouldBe Some(10)
        byName("missing_b") shouldBe Some(10)
        byName("null_a") shouldBe Some(20)
        byName("all_null") shouldBe None
        log.info(s"LEAST result: $byName")

      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

}

case class GreatestLeastRow(name: String, g: Option[List[java.lang.Integer]] = None)
case class GreatestLeastRowLeast(name: String, l: Option[List[java.lang.Integer]] = None)
