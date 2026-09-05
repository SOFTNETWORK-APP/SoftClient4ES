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

import app.softnetwork.elastic.client.rest.RestHighLevelClientApi
import app.softnetwork.elastic.sql.query.SQLAggregation
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap

/** SoftClient4ES#241 / #217 — ES 6 REST page extractors.
  *
  * Both extractors used to LOG a conversion failure and return `Seq.empty`, which every paging loop
  * reads as end-of-stream: the caller got a silently truncated result set presented as a success.
  * `extractHitsOnly` matters twice here — ES 6 has no PIT, so `search_after` is this client's only
  * deep-paging alternative to the classic scroll.
  *
  * No Docker: `ElasticClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network.
  */
class RestHighLevelClientScrollPageFailureSpec extends AnyWordSpec with Matchers {

  private val jackson = new ObjectMapper()

  implicit private val conversionContext: ConversionContext = NativeContext

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.load()
  }

  private def tree(json: String): JsonNode = jackson.readTree(json)

  private val goodPage = tree(
    """{"_scroll_id":"c1","hits":{"hits":[
      |{"_id":"1","_source":{"name":"a"}},
      |{"_id":"2","_source":{"name":"b"}},
      |{"_id":"3","_source":{"name":"c"}}]}}""".stripMargin
  )

  /** Core's `parseSingleSearchResponse` rejects any response carrying `error`. The page still
    * carries a hit, so the pre-fix behaviour (empty page) was indistinguishable from a last page.
    */
  private val brokenPage = tree(
    """{"_scroll_id":"c1","error":{"reason":"boom"},
      |"hits":{"hits":[{"_id":"1","_source":{"name":"a"}}]}}""".stripMargin
  )

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
}
