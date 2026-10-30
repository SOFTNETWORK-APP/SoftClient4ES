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

import app.softnetwork.elastic.client.jest.JestClientApi
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap

/** SoftClient4ES#241 / #217 — ES 6 Jest page extractors.
  *
  * Both extractors used to LOG a conversion failure and return `Seq.empty`, which every paging loop
  * reads as end-of-stream: the caller got a silently truncated result set presented as a success.
  * Jest's PIT path is a `NotImplementedError`, so `search_after` — and therefore `extractHitsOnly`
  * — is this client's only deep-paging alternative to the classic scroll.
  *
  * No Docker: `JestClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network.
  */
class JestScrollPageFailureSpec extends AnyWordSpec with Matchers {

  implicit private val conversionContext: ConversionContext = NativeContext

  private val client: JestClientApi = new JestClientApi {
    override lazy val config: Config = ConfigFactory.load()
  }

  private val goodPage =
    """{"_scroll_id":"c1","hits":{"hits":[
      |{"_id":"1","_source":{"name":"a"}},
      |{"_id":"2","_source":{"name":"b"}},
      |{"_id":"3","_source":{"name":"c"}}]}}""".stripMargin

  /** Core's `parseResponse` rejects any response carrying `error`. The page still carries a hit, so
    * the pre-fix behaviour (empty page) was indistinguishable from a last page.
    */
  private val brokenPage =
    """{"_scroll_id":"c1","error":{"reason":"boom"},
      |"hits":{"hits":[{"_id":"1","_source":{"name":"a"}}]}}""".stripMargin

  /** A truncated body — what a dropped connection or a mangled proxy response looks like. */
  private val malformedPage = """{"_scroll_id":"c1","hits":{"hits":[{"_id":"1","_sou"""

  private val noAggregations = ListMap.empty[String, SQLAggregation]

  "extractAllResults" should {

    "return one row per raw hit on a good page" in {
      client
        .extractAllResults(goodPage, ListMap.empty, noAggregations, retainDocumentId = false)
        .size shouldBe 3
    }

    "FAIL the page — never return an empty one — when the conversion fails" in {
      val ex = intercept[IllegalStateException](
        client.extractAllResults(
          brokenPage,
          ListMap.empty,
          noAggregations,
          retainDocumentId = false
        )
      )
      ex.getMessage should include("Failed to parse scroll page")
      ex.getCause should not be null
    }

    "FAIL the page on a malformed body" in {
      intercept[IllegalStateException](
        client.extractAllResults(
          malformedPage,
          ListMap.empty,
          noAggregations,
          retainDocumentId = false
        )
      )
    }

    "raise a NON-retriable failure (AD-S1-1)" in {
      // An IOException would be retried by `retryWithBackoff` and, on the continuation arm,
      // re-poll a spent scroll cursor — which SKIPS rows.
      val ex = intercept[IllegalStateException](
        client.extractAllResults(
          brokenPage,
          ListMap.empty,
          noAggregations,
          retainDocumentId = false
        )
      )
      isRetriableError(ex) shouldBe false
    }
  }

  "both extractors" should {

    "read a page in its statement's schema context: the rows and classes core's parse gives (SCHEMA-2, pages)" in {
      val fixture = SchemaPageFixture
      val context = fixture.context("6.8.23")
      val scroll = client.extractAllResults(
        fixture.page,
        fixture.fieldAliases,
        noAggregations,
        retainDocumentId = false
      )(context)
      val searchAfter =
        client.extractHitsOnly(fixture.page, fixture.fieldAliases, retainDocumentId = false)(
          context
        )
      Seq(scroll, searchAfter).foreach { rows =>
        rows shouldBe fixture.expected
        fixture.classes(rows) shouldBe fixture.classes(fixture.expected)
      }
    }
  }

  "extractHitsOnly" should {

    "return one row per raw hit on a good page" in {
      client.extractHitsOnly(goodPage, ListMap.empty, retainDocumentId = false).size shouldBe 3
    }

    "FAIL the page when the conversion fails" in {
      val ex = intercept[IllegalStateException](
        client.extractHitsOnly(brokenPage, ListMap.empty, retainDocumentId = false)
      )
      ex.getMessage should include("Failed to parse search_after page")
      isRetriableError(ex) shouldBe false
    }
  }

  "statusOf" should {

    "read Elasticsearch's status from a failed page, and only from an HTTP error answer" in {
      val reason = """{"root_cause":[{"type":"illegal_argument_exception"}]}"""
      def failed(code: Int): io.searchbox.client.JestResult = {
        val result = new io.searchbox.client.JestResult(new com.google.gson.Gson())
        result.setSucceeded(false)
        result.setResponseCode(code)
        result.setErrorMessage(reason)
        result
      }
      val page = client.pageFailure("Search after failed", failed(400))
      // the message is the one the page always threw, and the retry policy does not move
      page.getMessage shouldBe s"Search after failed: $reason"
      isRetriableError(page) shouldBe true
      client.statusOf(page) shouldBe Some(400)
      client.statusOf(client.pageFailure("Scroll failed", failed(503))) shouldBe Some(503)
      // no HTTP response: no status, as before
      client.statusOf(client.pageFailure("Scroll failed", failed(0))) shouldBe None
    }
  }

  /** A page on which ONE of 3 shards failed with `errType`, as Elasticsearch answers it with HTTP
    * 200, read as the paging doors read it.
    */
  private def oneOfThreeShardsFailed(errType: String): com.fasterxml.jackson.databind.JsonNode =
    app.softnetwork.elastic.sql.serialization.JacksonConfig.objectMapper.readTree(
      """{"_shards":{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[{"shard":0,""" +
      s""""index":"idx","node":"n1","reason":{"type":"$errType","reason":"compile error",""" +
      """"lang":"painless","caused_by":{"type":"illegal_argument_exception",""" +
      """"reason":"cannot cast"}}}]},"hits":{"hits":[]}}"""
    )

  private def searchAfterPage(tree: com.fasterxml.jackson.databind.JsonNode): Option[Exception] =
    client.failedShardsPage("Search after failed", tree)(new java.io.IOException(_))

  "failedShardsPage" should {

    "answer core's shard-failure error and status when every failure is the caller's" in {
      val failure = searchAfterPage(oneOfThreeShardsFailed("script_exception")).get
      failure shouldBe a[jest.JestClientHelpers.SearchPageFailure]
      failure.getMessage shouldBe
      "Search after failed: 1 of 3 shards failed: script_exception: compile error " +
      "(caused by illegal_argument_exception: cannot cast)"
      failure.asInstanceOf[jest.JestClientHelpers.SearchPageFailure].error.operation shouldBe
      Some("search")
      client.statusOf(failure) shouldBe Some(400)
      client.statusOf(new java.util.concurrent.CompletionException(failure)) shouldBe Some(400)
      isRetriableError(failure) shouldBe false
    }

    "keep the door's own exception, text and retry for any other failure" in {
      val failure = searchAfterPage(oneOfThreeShardsFailed("circuit_breaking_exception")).get
      failure shouldBe an[java.io.IOException]
      failure should not be a[jest.JestClientHelpers.PageFailure]
      failure.getMessage shouldBe "Search after failed: compile error"
      client.statusOf(failure) shouldBe None
      isRetriableError(failure) shouldBe true
    }

    "answer nothing when no shard failed" in {
      searchAfterPage(
        app.softnetwork.elastic.sql.serialization.JacksonConfig.objectMapper.readTree(
          """{"_shards":{"total":3,"successful":3,"skipped":0,"failed":0},"hits":{"hits":[]}}"""
        )
      ) shouldBe None
    }
  }
}
