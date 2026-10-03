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
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.transform.{
  BucketScriptTransformAggregation,
  Delay,
  Frequency,
  MaxTransformAggregation,
  MinTransformAggregation,
  TermsGroupBy,
  TransformBucketSelectorConfig,
  TransformConfig,
  TransformDest,
  TransformPivot,
  TransformSource
}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.{Config, ConfigFactory}
import org.elasticsearch.client.transform.PutTransformRequest
import org.elasticsearch.common.Strings
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.collection.immutable.ListMap

/** The ES 7 REST client builds a transform pivot with the client library's builders, one match arm
  * per `TransformAggregation` (ES 8 / ES 9 send the pivot as JSON). A materialized view's per-group
  * calculation (`MAX(a) - MIN(b) AS d`, a `BucketScriptTransformAggregation`) had no arm: the
  * request failed client-side, so a view with a SELECT calculation never deployed on ES 7.
  *
  * No Docker: `ElasticClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network.
  */
class RestHighLevelClientTransformPivotSpec extends AnyWordSpec with Matchers {

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.load()
  }

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  implicit private val context: PainlessContextType = PainlessContextType.Transform

  private val mapper = new ObjectMapper()

  private val calculation = BucketScriptTransformAggregation(
    expression = "MAX(a) - MIN(b)",
    bucketsPath = ListMap("max_a" -> "max_a", "min_b" -> "min_b"),
    script = "params.max_a - params.min_b",
    params = Nil
  )

  private def view(d: BucketScriptTransformAggregation): TransformConfig =
    TransformConfig(
      id = "v_d",
      viewName = "v",
      source = TransformSource(Seq("src"), None),
      dest = TransformDest("v_d"),
      pivot = Some(
        TransformPivot(
          groupBy = Map("g" -> TermsGroupBy("g")),
          aggregations = ListMap(
            "max_a" -> MaxTransformAggregation("a"),
            "min_b" -> MinTransformAggregation("b"),
            "d"     -> d
          ),
          bucketSelector = Some(
            TransformBucketSelectorConfig(bucketsPath = Map("d" -> "d"), script = "params.d > -5")
          )
        )
      ),
      delay = Delay.Default,
      frequency = Frequency.Default
    )

  /** The body the client sends: `PutTransformRequest` renders the converted config. */
  private def requestBody(config: TransformConfig): JsonNode =
    mapper.readTree(
      Strings.toString(new PutTransformRequest(client.convertToElasticTransformConfig(config)))
    )

  "the ES 7 transform request" should {

    "carry the calculation as a bucket_script under its name" in {
      val aggregations = requestBody(view(calculation)).path("pivot").path("aggregations")
      val bucketScript = aggregations.path("d").path("bucket_script")

      bucketScript.isObject shouldBe true
      bucketScript.path("buckets_path").size() shouldBe 2
      bucketScript.path("buckets_path").path("max_a").asText() shouldBe "max_a"
      bucketScript.path("buckets_path").path("min_b").asText() shouldBe "min_b"
      bucketScript.path("script").path("source").asText() shouldBe "params.max_a - params.min_b"
      bucketScript.path("script").has("params") shouldBe false

      // the operands it reads, and the HAVING that reads it by its name
      aggregations.path("max_a").path("max").path("field").asText() shouldBe "a"
      aggregations.path("min_b").path("min").path("field").asText() shouldBe "b"
      aggregations
        .path("having_filter")
        .path("bucket_selector")
        .path("buckets_path")
        .path("d")
        .asText() shouldBe "d"
    }

    "refuse a calculation whose script reads a parameter the transform does not bind" in {
      val unbound = view(calculation.copy(params = Seq("__now__")))
      val ex = intercept[UnsupportedOperationException](
        client.convertToElasticTransformConfig(unbound)
      )
      ex.getMessage should include("calculation d (MAX(a) - MIN(b))")
      ex.getMessage should include("__now__")
    }
  }
}
