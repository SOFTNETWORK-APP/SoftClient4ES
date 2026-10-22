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

import app.softnetwork.elastic.client.rest.{
  RestHighLevelClientCompanion,
  RestHighLevelClientHelpers
}
import com.typesafe.config.ConfigFactory
import org.elasticsearch.rest.RestStatus
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import java.util.concurrent.CompletionException
import scala.util.Try

/** SoftClient4ES#184 — proves the ES 6/7 `statusOf` extractor against hand-built real exception
  * instances, without Docker: `ElasticClientCompanion` creates the underlying client lazily, and
  * `apply()` is never called here.
  */
class RestHighLevelClientStatusOfSpec extends AnyWordSpec with Matchers {

  private case class Probe(config: ElasticConfig)
      extends RestHighLevelClientCompanion
      with RestHighLevelClientHelpers {
    override def elasticConfig: ElasticConfig = config
  }

  private val probe = Probe(ElasticConfig(ConfigFactory.load()))

  /** A real `ResponseException` carrying `status`, for a request on `uri`, with `body` as its
    * response entity. `Response`'s constructor is package-private, so it is reached reflectively;
    * nothing is sent.
    */
  private def responseException(
    status: Int,
    uri: String = "/idx/_search",
    body: Option[String] = None
  ): org.elasticsearch.client.ResponseException = {
    val constructor = classOf[org.elasticsearch.client.Response].getDeclaredConstructor(
      classOf[org.apache.http.RequestLine],
      classOf[org.apache.http.HttpHost],
      classOf[org.apache.http.HttpResponse]
    )
    constructor.setAccessible(true)
    val httpResponse = new org.apache.http.message.BasicHttpResponse(
      new org.apache.http.message.BasicStatusLine(
        org.apache.http.HttpVersion.HTTP_1_1,
        status,
        org.apache.http.impl.EnglishReasonPhraseCatalog.INSTANCE
          .getReason(status, java.util.Locale.ROOT)
      )
    )
    body.foreach(b =>
      httpResponse.setEntity(
        new org.apache.http.entity.StringEntity(
          b,
          org.apache.http.entity.ContentType.APPLICATION_JSON
        )
      )
    )
    val response = constructor.newInstance(
      new org.apache.http.message.BasicRequestLine(
        "POST",
        uri,
        org.apache.http.HttpVersion.HTTP_1_1
      ),
      new org.apache.http.HttpHost("localhost", 9200),
      httpResponse
    )
    new org.elasticsearch.client.ResponseException(response)
  }

  /** The body Elasticsearch 6.8 answers for a search whose script fails on every shard. */
  private val scriptFailureBody =
    """{"error":{"root_cause":[{"type":"script_exception","reason":"runtime error","script_stack":["java.time.LocalDate.toEpochDay(LocalDate.java)"],"script":"...","lang":"painless"}],"type":"search_phase_execution_exception","reason":"all shards failed","phase":"query","grouped":true,"failed_shards":[{"shard":0,"index":"idx","node":"n","reason":{"type":"script_exception","reason":"runtime error","lang":"painless","caused_by":{"type":"class_cast_exception","reason":"java.lang.String cannot be cast to java.lang.Number"}}}]},"status":400}"""

  "statusOf" should {

    "map ElasticsearchStatusException to its RestStatus" in {
      val ex = new org.elasticsearch.ElasticsearchStatusException(
        "no such index [absent]",
        RestStatus.NOT_FOUND
      )
      probe.statusOf(ex) shouldBe Some(404)
    }

    "unwrap a CompletionException first" in {
      val ex = new org.elasticsearch.ElasticsearchStatusException("gone", RestStatus.CONFLICT)
      probe.statusOf(new CompletionException(ex)) shouldBe Some(409)
    }

    "return None for a plain transport failure" in {
      probe.statusOf(new java.net.ConnectException("Connection refused")) shouldBe None
    }

    "read the status of the failure a rejected search page throws" in {
      val response = responseException(400)
      // the fixture is what it claims to be: it carries the status line
      Try(response.getResponse.getStatusLine.getStatusCode).toOption shouldBe Some(400)
      val rejected = probe.pageRejection(response)
      rejected.error.statusCode shouldBe Some(400)
      // the one arm that reads it: the failure carries its error's status
      probe.statusOf(rejected) shouldBe Some(400)
    }

    "fall through to the core default for a framework throwable" in {
      probe.statusOf(
        app.softnetwork.elastic.client.result.ElasticError("boom", statusCode = Some(403))
      ) shouldBe Some(403)
    }
  }

  "a rejected search page" should {

    "answer Elasticsearch's status, type, reason, root cause and why a shard failed" in {
      val rejected = probe.pageRejection(responseException(400, body = Some(scriptFailureBody)))
      // led by the type and reason, as the ES 8 / ES 9 client reports them; then the root cause,
      // then why the shard failed (`failed_shards[0].reason.caused_by`), as the ES 8 / ES 9 client
      // appends it
      rejected.error.message shouldBe
      "[search_phase_execution_exception] all shards failed: runtime error: java.lang.String cannot be cast to java.lang.Number"
      rejected.getMessage shouldBe rejected.error.message
      rejected.error.statusCode shouldBe Some(400)
      rejected.error.operation shouldBe Some("search")
      // no exception class of the client, no script stack, no request
      rejected.getMessage should not include "Exception"
      rejected.getMessage should not include "LocalDate"
      rejected.getMessage should not include "method ["
    }

    "name the index of an index error, as Elasticsearch 7 names it in the reason" in {
      // measured: Elasticsearch 6.8.23 names the missing index beside its reason only
      probe
        .pageRejection(
          responseException(
            404,
            body = Some(
              """{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index","resource.type":"index_or_alias","resource.id":"absent_idx","index_uuid":"_na_","index":"absent_idx"}],"type":"index_not_found_exception","reason":"no such index","resource.type":"index_or_alias","resource.id":"absent_idx","index_uuid":"_na_","index":"absent_idx"},"status":404}"""
            )
          )
        )
        .getMessage shouldBe "[index_not_found_exception] no such index [absent_idx]"
      // Elasticsearch 7+ names it in the reason: it is not named twice
      probe
        .pageRejection(
          responseException(
            404,
            body = Some(
              """{"error":{"root_cause":[{"type":"index_not_found_exception","reason":"no such index [absent_idx]","index_uuid":"_na_","index":"absent_idx"}],"type":"index_not_found_exception","reason":"no such index [absent_idx]","index_uuid":"_na_","index":"absent_idx"},"status":404}"""
            )
          )
        )
        .getMessage shouldBe "[index_not_found_exception] no such index [absent_idx]"
    }

    "carry the reason Elasticsearch 7.17 gives for a function outside its domain" in {
      // measured: `SELECT ASIN(n)` over n outside [-1,1]; the reason is under `caused_by` only
      val body =
        """{"error":{"root_cause":[{"type":"script_exception","reason":"runtime error","script_stack":["..."],"script":"...","lang":"painless"}],"type":"search_phase_execution_exception","reason":"all shards failed","phase":"query","grouped":true,"failed_shards":[{"shard":0,"index":"idx","node":"n","reason":{"type":"script_exception","reason":"runtime error","script_stack":["..."],"script":"...","lang":"painless","caused_by":{"type":"illegal_argument_exception","reason":"ASIN is undefined outside [-1,1]"}}}]},"status":400}"""
      probe.pageRejection(responseException(400, body = Some(body))).getMessage shouldBe
      "[search_phase_execution_exception] all shards failed: runtime error: ASIN is undefined outside [-1,1]"
    }

    "add the root cause only when it says something else, and fall back to the status line" in {
      def messageOf(body: String): String =
        probe.pageRejection(responseException(400, body = Some(body))).getMessage
      messageOf(
        """{"error":{"type":"parsing_exception","reason":"top reason"},"status":400}"""
      ) shouldBe "[parsing_exception] top reason"
      messageOf(
        """{"error":{"root_cause":[{"type":"parsing_exception","reason":"top reason"}],"type":"parsing_exception","reason":"top reason"},"status":400}"""
      ) shouldBe "[parsing_exception] top reason"
      messageOf(
        """{"error":{"root_cause":[{"type":"x_exception","reason":"root only"}]},"status":400}"""
      ) shouldBe "root only"
      probe.pageRejection(responseException(400)).getMessage shouldBe "HTTP/1.1 400 Bad Request"
      probe
        .pageRejection(responseException(400, body = Some("<html>proxy refused</html>")))
        .getMessage shouldBe "HTTP/1.1 400 Bad Request"
    }

    "never carry the request URI or its query string" in {
      val scrollId = "DnF1ZXJ5VGhlbkZldGNoBQAAAAAAAAA"
      val uri = s"/_search/scroll?scroll=1m&scroll_id=$scrollId"
      // a textual `error` echoes the request line: it is not read as a reason
      val echoing =
        s"""{"error":"Incorrect HTTP method for uri [$uri] and method [POST]","status":405}"""
      Seq(
        responseException(400, uri = uri),
        responseException(400, uri = uri, body = Some(scriptFailureBody)),
        responseException(405, uri = uri, body = Some(echoing))
      ).foreach { response =>
        // the fixture is what it claims to be: the exception's own message carries the URI
        response.getMessage should include(scrollId)
        val rejected = probe.pageRejection(response)
        rejected.getMessage should not include scrollId
        rejected.getMessage should not include "scroll="
        rejected.toString should not include scrollId
        rejected.getCause shouldBe null
        rejected.error.cause shouldBe None
      }
    }

    "stay out of the retry" in {
      val response = responseException(400, body = Some(scriptFailureBody))
      // control: the exception it replaces is an IOException, which the retry policy retries
      isRetriableError(response) shouldBe true
      val rejected = probe.pageRejection(response)
      // an Exception, as the page's failure always was, so every `Exception` arm still takes it
      rejected shouldBe an[Exception]
      rejected should not be a[java.io.IOException]
      isRetriableError(rejected) shouldBe false
    }

    "be a 4xx except 408 and 429: a 408, a 429 and a 5xx keep the original exception, retried" in {
      Seq(400, 401, 403, 404, 405, 409).foreach { status =>
        withClue(s"$status: ")(probe.isNonRetriable4xx(responseException(status)) shouldBe true)
      }
      Seq(408, 429, 500, 502, 503, 504).foreach { status =>
        withClue(s"$status: ") {
          probe.isNonRetriable4xx(responseException(status)) shouldBe false
          isRetriableError(responseException(status)) shouldBe true
        }
      }
    }
  }

  /** A page on which ONE of 3 shards failed with `errType`, as Elasticsearch answers it with HTTP
    * 200, read as the paging doors read it.
    */
  private def oneOfThreeShardsFailed(errType: String): com.fasterxml.jackson.databind.JsonNode =
    app.softnetwork.elastic.sql.serialization.JacksonConfig.objectMapper.readTree(
      """{"_shards":{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[{"shard":0,""" +
      s""""index":"idx","node":"n1","reason":{"type":"$errType","reason":"runtime error",""" +
      """"lang":"painless","caused_by":{"type":"date_time_parse_exception",""" +
      """"reason":"Text 'x' could not be parsed at index 0"}}}]},"hits":{"hits":[]}}"""
    )

  private def searchAfterPage(tree: com.fasterxml.jackson.databind.JsonNode): Option[Exception] =
    probe.failedShardsPage("Search after failed", tree)(new java.io.IOException(_))

  "failedShardsPage" should {

    "answer core's shard-failure error and status when every failure is the caller's" in {
      val failure = searchAfterPage(oneOfThreeShardsFailed("script_exception")).get
      failure shouldBe a[RestHighLevelClientHelpers.SearchPageFailure]
      failure.getMessage shouldBe
      "Search after failed: 1 of 3 shards failed: script_exception: runtime error " +
      "(caused by date_time_parse_exception: Text 'x' could not be parsed at index 0)"
      failure.asInstanceOf[RestHighLevelClientHelpers.SearchPageFailure].error.operation shouldBe
      Some("search")
      probe.statusOf(failure) shouldBe Some(400)
      probe.statusOf(new CompletionException(failure)) shouldBe Some(400)
      isRetriableError(failure) shouldBe false
    }

    "keep the door's own exception, text and retry for any other failure" in {
      val failure = searchAfterPage(oneOfThreeShardsFailed("circuit_breaking_exception")).get
      failure shouldBe an[java.io.IOException]
      failure.getMessage shouldBe "Search after failed: runtime error"
      probe.statusOf(failure) shouldBe None
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
