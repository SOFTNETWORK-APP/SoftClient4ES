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
import app.softnetwork.elastic.client.rest.RestHighLevelClientApi
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext}

/** SoftClient4ES#241 / #217 — end-to-end proof on the real `scrollClassic` loop, without Docker.
  *
  * A stub HTTP endpoint serves a page that Elasticsearch would consider a success but whose body
  * core's converter rejects. Before this story the loop turned that into an empty page, which its
  * `results.isEmpty` terminator read as end-of-stream: the consumer received ZERO rows and a
  * SUCCESSFUL stream. Now the stream fails.
  *
  * It also pins AD-S1-2: on a FIRST-page failure the just-opened scroll context is released. The
  * stream-level recovery cannot do it — `scrollIdOpt` is still `None` there — so a BI host retrying
  * a poisoned query used to accumulate contexts toward `search.max_open_scroll_contexts`.
  */
class RestHighLevelClientScrollStreamFailureSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("scroll-stream-failure-spec")
  implicit private val ec: ExecutionContext = system.dispatcher
  implicit private val conversionContext: ConversionContext = NativeContext

  private val searchCalls = new AtomicInteger(0)
  private val clearedScrollIds = mutable.ArrayBuffer.empty[String]

  /** Pages served to `POST /{index}/_search`, in order. */
  private var pages: List[String] = Nil

  private val freePort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private val server: HttpServer = {
    val srv = HttpServer.create(new InetSocketAddress("127.0.0.1", freePort), 0)
    srv.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val path = exchange.getRequestURI.getPath
          val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          val response =
            if (path.endsWith("/_search/scroll") && exchange.getRequestMethod == "DELETE") {
              clearedScrollIds.synchronized {
                clearedScrollIds ++= """"([^"]+)"""".r
                  .findAllMatchIn(body)
                  .map(_.group(1))
                  .filterNot(_ == "scroll_id")
              }
              """{"succeeded":true,"num_freed":1}"""
            } else {
              val idx = searchCalls.getAndIncrement()
              pages.lift(idx).getOrElse(emptyPage)
            }
          val bytes = response.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json; charset=UTF-8")
          exchange.sendResponseHeaders(200, bytes.length.toLong)
          exchange.getResponseBody.write(bytes)
          exchange.close()
        }
      }
    )
    srv.start()
    srv
  }

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.parseString(
      s"""elastic {
         |  credentials { scheme = "http", host = "127.0.0.1", port = $freePort }
         |}""".stripMargin
    )
  }

  private val shards = """"_shards":{"total":1,"successful":1,"failed":0}"""

  private def page(scrollId: String, ids: Seq[String], error: Boolean = false): String = {
    val hits = ids
      .map(id => s"""{"_index":"people","_type":"_doc","_id":"$id","_source":{"name":"n$id"}}""")
      .mkString(",")
    val errorPart = if (error) """"error":{"reason":"boom"},""" else ""
    s"""{"_scroll_id":"$scrollId",$shards,$errorPart"hits":{"total":${ids.size},"hits":[$hits]}}"""
  }

  private def emptyPage: String = page("c-last", Seq.empty)

  private def rows(): Seq[ListMap[String, Any]] = {
    val source = client.scrollClassic(
      ElasticQuery("""{"query":{"match_all":{}}}""", Seq("people")),
      ListMap.empty,
      ListMap.empty[String, SQLAggregation],
      ScrollConfig(retryConfig = RetryConfig(maxRetries = 0))
    )
    Await.result(source.runWith(Sink.seq), Duration(30, TimeUnit.SECONDS))
  }

  override def beforeAll(): Unit = {
    searchCalls.set(0)
    clearedScrollIds.clear()
  }

  override def afterAll(): Unit = {
    server.stop(0)
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  "scrollClassic" should {

    "stream every row of a well-formed scroll and end cleanly on the empty page" in {
      searchCalls.set(0)
      clearedScrollIds.clear()
      pages = List(page("c1", Seq("1", "2")), page("c1", Seq("3")), emptyPage)

      rows().map(_.getOrElse("name", "")) shouldBe Seq("n1", "n2", "n3")
      // the exhausted context is released rather than left to expire — a scroll cursor names the
      // context that opened it, so the id cleared is the one carried into the last continuation
      clearedScrollIds should contain("c1")
    }

    "FAIL the stream on a first page the converter rejects — never complete with fewer rows" in {
      searchCalls.set(0)
      clearedScrollIds.clear()
      pages = List(page("c1", Seq("1", "2"), error = true))

      val ex = intercept[IllegalStateException](rows())
      ex.getMessage should include("Failed to parse scroll page")
      // AD-S1-2: the context opened by the initial search is released even though the
      // stream-level recovery still sees `scrollIdOpt == None`
      clearedScrollIds should contain("c1")
    }

    "FAIL the stream on a continuation page the converter rejects" in {
      searchCalls.set(0)
      clearedScrollIds.clear()
      pages = List(page("c1", Seq("1")), page("c2", Seq("2"), error = true))

      intercept[IllegalStateException](rows())
      // the advanced cursor is released too, not only the spent one
      clearedScrollIds should contain("c2")
    }
  }
}
