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
import app.softnetwork.elastic.client.rest.{RestHighLevelClientApi, RestHighLevelClientHelpers}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.SQLAggregation
import ch.qos.logback.classic.{Level, Logger => LogbackLogger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.LoggerFactory

import java.net.{InetSocketAddress, ServerSocket}
import java.nio.charset.StandardCharsets
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.concurrent.Await
import scala.concurrent.duration.Duration
import scala.jdk.CollectionConverters._
import scala.util.Try

/** A scroll CONTINUATION page that Elasticsearch rejects with a 4xx, on the real `scrollClassic`
  * loop, without Docker.
  *
  * The rejected page fails with Elasticsearch's status and reason, and the stream-level recovery
  * still runs for it: it releases the scroll context the spent cursor names, and it logs the
  * failure at ERROR. That recovery matches `Exception`, so the page's failure must stay one.
  */
class RestHighLevelClientScrollPageRejectionSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("scroll-page-rejection-spec")
  implicit private val conversionContext: ConversionContext = NativeContext

  private val searchCalls = new AtomicInteger(0)
  private val clearedScrollIds = mutable.ArrayBuffer.empty[String]

  private val freePort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private val rejection =
    """{"error":{"root_cause":[{"type":"illegal_argument_exception","reason":"the continuation's root cause"}],"type":"search_phase_execution_exception","reason":"all shards failed"},"status":400}"""

  private val firstPage =
    """{"_scroll_id":"c1","_shards":{"total":1,"successful":1,"failed":0},"hits":{"total":2,"hits":[{"_index":"people","_type":"_doc","_id":"1","_source":{"name":"n1"}}]}}"""

  /** The `GET /` answer a 7.x high-level client reads before its first call. */
  private val infoResponse =
    """{"name":"n","cluster_name":"c","cluster_uuid":"u","version":{"number":"7.17.29","build_flavor":"default","build_type":"docker","build_hash":"h","build_date":"2025-01-01T00:00:00.000Z","build_snapshot":false,"lucene_version":"8.11.3","minimum_wire_compatibility_version":"6.8.0","minimum_index_compatibility_version":"6.0.0-beta1"},"tagline":"You Know, for Search"}"""

  private val server: HttpServer = {
    val srv = HttpServer.create(new InetSocketAddress("127.0.0.1", freePort), 0)
    srv.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          val path = exchange.getRequestURI.getPath
          val body = new String(exchange.getRequestBody.readAllBytes(), StandardCharsets.UTF_8)
          val (status, response) =
            if (path.endsWith("/_search/scroll") && exchange.getRequestMethod == "DELETE") {
              clearedScrollIds.synchronized {
                clearedScrollIds ++= """"([^"]+)"""".r
                  .findAllMatchIn(body)
                  .map(_.group(1))
                  .filterNot(_ == "scroll_id")
              }
              (200, """{"succeeded":true,"num_freed":1}""")
            } else if (path.endsWith("/_search/scroll")) {
              searchCalls.incrementAndGet()
              (400, rejection)
            } else if (path.endsWith("/_search")) {
              searchCalls.incrementAndGet()
              (200, firstPage)
            } else (200, infoResponse)
          val bytes = response.getBytes(StandardCharsets.UTF_8)
          exchange.getResponseHeaders.add("Content-Type", "application/json; charset=UTF-8")
          exchange.getResponseHeaders.add("X-Elastic-Product", "Elasticsearch")
          exchange.sendResponseHeaders(status, bytes.length.toLong)
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

  /** The client's logger is `LoggerFactory getLogger getClass.getName` of its anonymous subclass
    * above, so it lives under this package.
    */
  private val clientLoggerPackage = "app.softnetwork.elastic.client"

  /** Run the scroll once; return its failure and the ERROR lines the client logged meanwhile. */
  private def rejectedContinuation(): (Throwable, Seq[String]) = {
    searchCalls.set(0)
    clearedScrollIds.synchronized(clearedScrollIds.clear())
    val logger = LoggerFactory.getLogger(clientLoggerPackage) match {
      case l: LogbackLogger => l
      case other =>
        fail(s"'$clientLoggerPackage' is not a logback logger (${other.getClass.getName})")
    }
    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    val outcome =
      try {
        Try(
          Await.result(
            client
              .scrollClassic(
                ElasticQuery("""{"query":{"match_all":{}}}""", Seq("people")),
                ListMap.empty,
                ListMap.empty[String, SQLAggregation],
                ScrollConfig(retryConfig = RetryConfig(maxRetries = 0))
              )
              .runWith(Sink.seq),
            Duration(30, TimeUnit.SECONDS)
          )
        )
      } finally logger.detachAppender(appender)
    val errors = appender.synchronized(
      appender.list.asScala.filter(_.getLevel == Level.ERROR).map(_.getFormattedMessage).toVector
    )
    appender.stop()
    // the fixture is what it claims to be: a first page, then the rejected continuation
    searchCalls.get shouldBe 2
    val failure = outcome.failed.getOrElse(fail(s"the stream did not fail: $outcome"))
    (failure, errors)
  }

  private def assertRejection(failure: Throwable): Unit = failure match {
    case rejected: RestHighLevelClientHelpers.SearchPageFailure =>
      rejected.error.statusCode shouldBe Some(400)
      rejected.getMessage shouldBe
      "[search_phase_execution_exception] all shards failed: the continuation's root cause"
    case other => fail(s"not the page rejection: $other")
  }

  override def afterAll(): Unit = {
    server.stop(0)
    system.terminate()
  }

  "a scroll continuation page Elasticsearch rejects with a 4xx" should {

    "release the scroll context the spent cursor names" in {
      val (failure, _) = rejectedContinuation()
      assertRejection(failure)
      clearedScrollIds.synchronized(clearedScrollIds.toList) should contain("c1")
    }

    "log the failure at ERROR" in {
      val (failure, errors) = rejectedContinuation()
      assertRejection(failure)
      errors.filter(_.startsWith("Scroll failed after retries")) should have size 1
    }
  }
}
