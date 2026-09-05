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

import app.softnetwork.elastic.client.java.JavaClientApi
import app.softnetwork.elastic.sql.query.SQLAggregation
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.json.JsonData
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** SoftClient4ES#241 / #217 — ES 8 Java-client page extractor.
  *
  * `extractAllResults` (classic scroll, hits + aggregations) used to LOG a conversion failure and
  * return `Seq.empty`, which the paging loops read as end-of-stream: the caller got a silently
  * truncated result set presented as a success. `extractHitsOnly` was already hardened by #238 and
  * is deliberately NOT touched here (AC 8).
  *
  * The extractor takes the typed `SearchResponse[ObjectNode]`, so the pages are built through the
  * Java client's own builders. The failing page is an aggregation-shaped one whose `buckets` object
  * holds a scalar instead of a bucket — core's `ElasticConversion` casts bucket values to
  * `ObjectNode`, so the conversion fails the way a genuinely unexpected response shape would.
  *
  * No Docker: `ElasticClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network.
  */
class JavaClientScrollPageFailureSpec extends AnyWordSpec with Matchers {

  private val jackson = new ObjectMapper()
  private val jsonpMapper = new JacksonJsonpMapper(jackson)

  implicit private val conversionContext: ConversionContext = NativeContext

  private val client: JavaClientApi = new JavaClientApi {
    override def config: Config = ConfigFactory.load()
  }

  private def hit(id: String, name: String): Hit[ObjectNode] = {
    val source = jackson.createObjectNode()
    source.put("name", name)
    Hit.of[ObjectNode](h => h.index("people").id(id).source(source))
  }

  private def response(
    hits: Seq[Hit[ObjectNode]],
    aggregations: Map[String, Aggregate] = Map.empty
  ): SearchResponse[ObjectNode] =
    SearchResponse.of[ObjectNode] { b =>
      b.took(1L)
        .timedOut(false)
        .shards(s => s.total(1).successful(1).failed(0))
        .hits(h => h.hits(hits.asJava))
        .aggregations(aggregations.asJava)
    }

  private val goodPage = response(Seq(hit("1", "a"), hit("2", "b"), hit("3", "c")))

  /** A bucketed aggregation whose named bucket holds a scalar: core casts bucket values to
    * `ObjectNode`, so the page converts to a `Failure` rather than to rows.
    */
  private val brokenPage = response(
    hits = Seq.empty,
    aggregations = Map(
      "byCategory" -> Aggregate.of(
        _._custom(
          "filters",
          JsonData.of(jackson.readTree("""{"buckets":{"a":42}}"""), jsonpMapper)
        )
      )
    )
  )

  private val noAggregations = ListMap.empty[String, SQLAggregation]

  "extractAllResults" should {

    "return one row per raw hit on a good page" in {
      client
        .extractAllResults(Left(goodPage), ListMap.empty, noAggregations, retainDocumentId = false)
        .size shouldBe 3
    }

    "FAIL the page — never return an empty one — when the conversion fails" in {
      val ex = intercept[IllegalStateException](
        client
          .extractAllResults(
            Left(brokenPage),
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
        client
          .extractAllResults(
            Left(brokenPage),
            ListMap.empty,
            noAggregations,
            retainDocumentId = false
          )
      )
      isRetriableError(ex) shouldBe false
    }
  }
}
