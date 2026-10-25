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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** Elasticsearch 6 only (7+ rejects a typed mapping): an index whose single mapping type is not
  * named `_doc` -- `doc`, what Logstash and Beats 6.x create -- is typed like any other.
  *
  * Its mapping loaded no column before, so a statement over it ran untyped: the SQL spelling of a
  * timestamp literal was forwarded verbatim and refused by the default date format. The observable
  * is the ANSWER of that statement, over an index created outside elasticsql (raw `PUT`).
  */
trait NonDocMappingTypeSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "non_doc_mapping_type"

  private val timestamps: Seq[String] = Seq(
    "2026-06-01T00:00:00",
    "2026-06-02T12:00:00",
    "2026-06-03T23:59:59",
    "2026-06-04T00:00:00",
    "2026-06-04T00:00:01",
    "2026-06-05T10:00:00",
    "2026-06-10T00:00:00",
    "2026-07-01T00:00:00"
  )

  private def http(method: String, path: String, body: String, contentType: String): Int = {
    val conn = new java.net.URL(elasticURL + path)
      .openConnection()
      .asInstanceOf[java.net.HttpURLConnection]
    conn.setRequestMethod(method)
    conn.setDoOutput(true)
    conn.setRequestProperty("Content-Type", contentType)
    val os = conn.getOutputStream
    os.write(body.getBytes("UTF-8"))
    os.close()
    conn.getResponseCode
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    http(
      "PUT",
      s"/$index",
      """{"settings":{"number_of_shards":1,"number_of_replicas":0},
        |"mappings":{"doc":{"properties":{"id":{"type":"keyword"},"event_ts":{"type":"date"}}}}}""".stripMargin,
      "application/json"
    ) shouldBe 200
    val bulk = timestamps.zipWithIndex
      .map { case (ts, i) =>
        s"""{"index":{"_index":"$index","_type":"doc","_id":"e${i + 1}"}}\n""" +
          s"""{"id":"e${i + 1}","event_ts":"$ts"}"""
      }
      .mkString("", "\n", "\n")
    http("POST", "/_bulk?refresh=true", bulk, "application/x-ndjson") shouldBe 200
  }

  override def afterAll(): Unit = {
    Try(client.deleteIndex(index))
    super.afterAll()
  }

  private def ids(sql: String): Either[String, Set[String]] =
    Try {
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(QueryRows(rs, _))             => Right(rs)
        case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
        case ElasticSuccess(QueryStream(stream, _)) =>
          Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
        case ElasticSuccess(other) => Left(s"unexpected result $other")
        case ElasticFailure(error) => Left(error.message)
      }
    }.toEither.left
      .map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")
      .joinRight
      .map(_.flatMap(_.get("id").map(String.valueOf)).toSet)

  "an index whose mapping type is named doc" should "be typed: its columns are loaded" in {
    ids(s"SELECT id FROM $index WHERE event_ts >= '2026-06-04 00:00:00'") shouldBe
    Right(Set("e4", "e5", "e6", "e7", "e8"))
  }
}
