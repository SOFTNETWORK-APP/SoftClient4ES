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

  /** A real `ResponseException` carrying `status`. `Response`'s constructor is package-private, so
    * it is reached reflectively; nothing is sent.
    */
  private def responseException(status: Int): org.elasticsearch.client.ResponseException = {
    val constructor = classOf[org.elasticsearch.client.Response].getDeclaredConstructor(
      classOf[org.apache.http.RequestLine],
      classOf[org.apache.http.HttpHost],
      classOf[org.apache.http.HttpResponse]
    )
    constructor.setAccessible(true)
    val response = constructor.newInstance(
      new org.apache.http.message.BasicRequestLine(
        "POST",
        "/idx/_search",
        org.apache.http.HttpVersion.HTTP_1_1
      ),
      new org.apache.http.HttpHost("localhost", 9200),
      new org.apache.http.message.BasicHttpResponse(
        new org.apache.http.message.BasicStatusLine(
          org.apache.http.HttpVersion.HTTP_1_1,
          status,
          "Bad Request"
        )
      )
    )
    new org.elasticsearch.client.ResponseException(response)
  }

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

    "read the status of the 4xx wrapper a search page throws, from its cause" in {
      val wrapped = new IllegalStateException("m", responseException(400))
      // the fixture is what it claims to be: the cause carries the status line
      Try(
        wrapped.getCause
          .asInstanceOf[org.elasticsearch.client.ResponseException]
          .getResponse
          .getStatusLine
          .getStatusCode
      ).toOption shouldBe Some(400)
      probe.statusOf(wrapped) shouldBe Some(400)
    }

    "fall through to the core default for a framework throwable" in {
      probe.statusOf(
        app.softnetwork.elastic.client.result.ElasticError("boom", statusCode = Some(403))
      ) shouldBe Some(403)
    }
  }
}
