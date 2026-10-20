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
import app.softnetwork.elastic.client.jest.JestClientApi
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.google.gson.Gson
import com.typesafe.config.{Config, ConfigFactory}
import io.searchbox.action.Action
import io.searchbox.client.{JestClient, JestResult, JestResultHandler}
import io.searchbox.core.{ClearScroll, Search, SearchScroll}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** The three ES 6 Jest page doors read `_shards`: a page on which some shards failed fails the
  * stream, exactly as the ES 6 REST doors do, instead of reading the surviving shards' hits as the
  * whole page.
  *
  * Elasticsearch answers such a page with HTTP 200, and Jest's `isSucceeded` is the HTTP status, so
  * before this the classic scroll (initial page and continuation) and `search_after` returned the
  * partial page with no error. Now:
  *   - the initial scroll page releases its context and fails with `Initial scroll failed: …`;
  *   - a continuation page releases the cursor and fails, without a retry (a re-polled cursor skips
  *     rows), with `Scroll continuation failed: …`;
  *   - a `search_after` page fails with `Search after failed: …`.
  *
  * No Docker: a stub `JestClient` answers each action with a canned 200 body and records every
  * `ClearScroll`.
  */
class JestScrollShardFailureSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("jest-scroll-shard-failure-spec")
  implicit private val conversionContext: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 30.seconds)
    super.afterAll()
  }

  private val failedShards =
    """"_shards":{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[""" +
    """{"shard":1,"index":"t","node":"n1","reason":{"type":"script_exception",""" +
    """"reason":"shard boom"}}]}"""

  private val healthyShards = """"_shards":{"total":3,"successful":3,"skipped":0,"failed":0}"""

  private def page(scrollId: Option[String], shards: String, ids: String*): String = {
    val cursor = scrollId.map(id => s""""_scroll_id":"$id",""").getOrElse("")
    val hits = ids
      .map(id => s"""{"_index":"t","_id":"$id","_source":{"id":"$id"},"sort":["$id"]}""")
      .mkString(",")
    s"""{$cursor"took":1,"timed_out":false,$shards,"hits":{"total":${ids.size},"hits":[$hits]}}"""
  }

  /** Answers the n-th `Search` with `searches(n)` (the last one repeated), every `SearchScroll`
    * with `scroll`, and records the body of every `ClearScroll`.
    */
  private class StubJest(searches: Seq[String], scroll: String) extends JestClient {
    private val gson = new Gson()
    private val searchCount = new AtomicInteger(0)
    val cleared = new ConcurrentLinkedQueue[String]()

    override def execute[T <: JestResult](action: Action[T]): T = {
      val body = action match {
        case _: Search =>
          searches(math.min(searchCount.getAndIncrement(), searches.size - 1))
        case _: SearchScroll => scroll
        case clear: ClearScroll =>
          cleared.add(String.valueOf(clear.getData(gson)))
          """{"succeeded":true,"num_freed":1}"""
        case other => throw new IllegalStateException(s"unexpected action $other")
      }
      action.createNewElasticSearchResult(body, 200, "OK", gson)
    }

    override def executeAsync[T <: JestResult](
      action: Action[T],
      handler: JestResultHandler[_ >: T]
    ): Unit = throw new UnsupportedOperationException("executeAsync")

    override def shutdownClient(): Unit = ()

    override def setServers(servers: java.util.Set[String]): Unit = ()

    override def close(): Unit = ()
  }

  private def clientOf(stub: StubJest): JestClientApi = new JestClientApi {
    override lazy val config: Config = ConfigFactory.load()
    override def apply(): JestClient = stub
  }

  private val query =
    ElasticQuery("""{"query":{"match_all":{}},"sort":[{"id":"asc"}]}""", Seq("t"))

  /** No retry and no backoff: the initial page's `IOException` is retriable, and a retried page
    * would answer the same failed shards.
    */
  private val config =
    ScrollConfig(scrollSize = 10, retryConfig = RetryConfig(maxRetries = 0, initialDelay = 1.milli))

  private val noAggregations = ListMap.empty[String, SQLAggregation]

  private def classic(stub: StubJest): Try[Seq[ListMap[String, Any]]] =
    Try(
      Await.result(
        clientOf(stub)
          .scrollClassic(query, ListMap.empty, noAggregations, config)
          .runWith(Sink.seq),
        30.seconds
      )
    )

  private def searchAfter(stub: StubJest): Try[Seq[ListMap[String, Any]]] =
    Try(
      Await.result(
        clientOf(stub).searchAfter(query, ListMap.empty, config, hasSorts = true).runWith(Sink.seq),
        30.seconds
      )
    )

  private def failureOf(result: Try[Seq[ListMap[String, Any]]]): Throwable = result match {
    case Failure(e) => e
    case Success(rows) =>
      fail(s"a page on which a shard failed answered ${rows.size} row(s) with no error: $rows")
  }

  "the classic scroll" should {

    "fail the initial page whose shards failed, and release its context" in {
      val stub =
        new StubJest(Seq(page(Some("s1"), failedShards, "a")), page(Some("s2"), healthyShards))
      val e = failureOf(classic(stub))
      e.getMessage should include("Initial scroll failed: shard boom")
      stub.cleared.asScala.exists(_.contains("s1")) shouldBe true
    }

    "fail a continuation page whose shards failed, release the cursor, and not retry" in {
      val stub =
        new StubJest(
          Seq(page(Some("s1"), healthyShards, "a")),
          page(Some("s2"), failedShards, "b")
        )
      val e = failureOf(classic(stub))
      e shouldBe an[IllegalStateException]
      e.getMessage should include("Scroll continuation failed: shard boom")
      isRetriableError(e) shouldBe false
      stub.cleared.asScala.exists(_.contains("s1")) shouldBe true
    }

    "answer every row when no shard failed" in {
      val stub =
        new StubJest(
          Seq(page(Some("s1"), healthyShards, "a", "b")),
          page(Some("s2"), healthyShards)
        )
      classic(stub).get.map(_("id")) shouldBe Seq("a", "b")
    }
  }

  "search_after" should {

    "fail a page whose shards failed" in {
      val stub = new StubJest(Seq(page(None, failedShards, "a")), page(None, healthyShards))
      failureOf(searchAfter(stub)).getMessage should include("Search after failed: shard boom")
    }

    "answer every row when no shard failed" in {
      val stub = new StubJest(
        Seq(page(None, healthyShards, "a", "b"), page(None, healthyShards)),
        page(None, healthyShards)
      )
      searchAfter(stub).get.map(_("id")) shouldBe Seq("a", "b")
    }
  }
}
