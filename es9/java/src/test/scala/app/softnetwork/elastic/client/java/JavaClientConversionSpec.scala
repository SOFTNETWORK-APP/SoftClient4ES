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

package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.client.ElasticConfig
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import co.elastic.clients.elasticsearch._types.aggregations.Aggregate
import co.elastic.clients.elasticsearch.core.{MsearchResponse, SearchResponse}
import co.elastic.clients.elasticsearch.core.msearch.MultiSearchResponseItem
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.json.JsonData
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import _root_.java.util.{Collections, List => JList}

/** Pins the envelope contract of the single-parse hits path (softclient4es-arrow#160).
  *
  * The row parser (`ElasticConversion.parseSimpleHits`) reads exactly `_id`, `_source`,
  * `inner_hits` and `fields` per hit — `hitsToResponseNode` must provide those and may omit
  * everything else. A hit carrying `inner_hits` or `fields` must fall back to a full-fidelity node,
  * and `convertToTree` must produce the same tree `convertToJson` round-trips to.
  */
class JavaClientConversionSpec extends AnyWordSpec with Matchers {

  private object Companion extends JavaClientCompanion with JavaClientConversion {
    override def elasticConfig: ElasticConfig = ElasticConfig(ConfigFactory.load())
    def envelope(hits: JList[Hit[ObjectNode]]): ObjectNode = hitsToResponseNode(hits)
    def tree(hit: Hit[ObjectNode]): JsonNode = convertToTree(hit)
    def json(hit: Hit[ObjectNode]): String = convertToJson(hit)
    def searchTree(response: SearchResponse[ObjectNode]): JsonNode = searchResponseToTree(response)
    def msearchTree(response: MsearchResponse[ObjectNode]): JsonNode =
      msearchResponseToTree(response)
    def searchJson(response: SearchResponse[ObjectNode]): String = convertToJson(response)
  }

  private val mapper = JacksonConfig.objectMapper

  private def sourceNode(): ObjectNode = {
    val node = mapper.createObjectNode()
    node.put("name", "a")
    node.put("amount", 1.5)
    node
  }

  private def hitOf(
    id: Option[String],
    source: Option[ObjectNode],
    fields: Map[String, JsonData] = Map.empty
  ): Hit[ObjectNode] =
    Hit.of[ObjectNode] { builder =>
      builder.index("idx")
      id.foreach(builder.id)
      source.foreach(builder.source)
      fields.foreach { case (name, value) =>
        builder.fields(Collections.singletonMap(name, value))
      }
      builder
    }

  "hitsToResponseNode" should {

    "build the minimal envelope and re-parent the _source node untouched" in {
      val src = sourceNode()
      val envelope = Companion.envelope(Collections.singletonList(hitOf(Some("1"), Some(src))))

      val hits = envelope.path("hits").path("hits")
      hits.isArray shouldBe true
      hits.size() shouldBe 1
      val hitNode = hits.get(0)
      hitNode.path("_id").asText() shouldBe "1"
      hitNode.get("_source") should be theSameInstanceAs src
      hitNode.has("_index") shouldBe false
    }

    "omit _id when the hit has none" in {
      val hitNode = Companion
        .envelope(Collections.singletonList(hitOf(None, Some(sourceNode()))))
        .path("hits")
        .path("hits")
        .get(0)
      hitNode.has("_id") shouldBe false
      hitNode.has("_source") shouldBe true
    }

    "omit _source when the hit has none" in {
      val hitNode = Companion
        .envelope(Collections.singletonList(hitOf(Some("2"), None)))
        .path("hits")
        .path("hits")
        .get(0)
      hitNode.path("_id").asText() shouldBe "2"
      hitNode.has("_source") shouldBe false
    }

    "produce an empty hits array for no hits" in {
      val hits = Companion
        .envelope(Collections.emptyList[Hit[ObjectNode]]())
        .path("hits")
        .path("hits")
      hits.isArray shouldBe true
      hits.size() shouldBe 0
    }

    "preserve explicit nulls in _source" in {
      val src = mapper.createObjectNode()
      src.putNull("maybe")
      val hitNode = Companion
        .envelope(Collections.singletonList(hitOf(Some("3"), Some(src))))
        .path("hits")
        .path("hits")
        .get(0)
      hitNode.path("_source").get("maybe").isNull shouldBe true
    }

    "fall back to a full-fidelity hit node when fields are present" in {
      val src = sourceNode()
      val hit = hitOf(Some("4"), Some(src), fields = Map("f" -> JsonData.of("v")))
      val hitNode = Companion
        .envelope(Collections.singletonList(hit))
        .path("hits")
        .path("hits")
        .get(0)
      hitNode.path("_id").asText() shouldBe "4"
      hitNode.path("_index").asText() shouldBe "idx"
      hitNode.path("fields").path("f").asText() shouldBe "v"
      hitNode.path("_source") shouldBe src
      hitNode.path("_source") shouldNot be theSameInstanceAs src
    }
  }

  "convertToTree" should {
    "produce the same tree convertToJson round-trips to" in {
      val hit = hitOf(Some("5"), Some(sourceNode()), fields = Map("f" -> JsonData.of("v")))
      Companion.tree(hit) shouldBe mapper.readTree(Companion.json(hit))
    }
  }

  private def searchResponseOf(
    hits: JList[Hit[ObjectNode]],
    aggregations: Map[String, Aggregate] = Map.empty
  ): SearchResponse[ObjectNode] =
    SearchResponse.of[ObjectNode] { builder =>
      builder
        .took(1)
        .timedOut(false)
        .shards(s => s.total(1).successful(1).failed(0))
        .hits(h => h.hits(hits))
      aggregations.foreach { case (name, aggregate) => builder.aggregations(name, aggregate) }
      builder
    }

  "searchResponseToTree" should {

    "re-parent hits through the minimal envelope when the response has no aggregations" in {
      val src = sourceNode()
      val tree = Companion.searchTree(
        searchResponseOf(Collections.singletonList(hitOf(Some("1"), Some(src))))
      )
      val hitNode = tree.path("hits").path("hits").get(0)
      hitNode.path("_id").asText() shouldBe "1"
      hitNode.get("_source") should be theSameInstanceAs src
      tree.has("took") shouldBe false
    }

    "keep the exact convertToJson shape when aggregations are present" in {
      val response = searchResponseOf(
        Collections.singletonList(hitOf(Some("1"), Some(sourceNode()))),
        aggregations = Map("avg_price" -> Aggregate.of(a => a.avg(v => v.value(2.5))))
      )
      // normalize both sides through the same parse: the token-level tree may carry
      // LongNode/IntNode artifacts that are value-equal but not node-class-equal
      mapper.readTree(Companion.searchTree(response).toString) shouldBe
      mapper.readTree(Companion.searchJson(response))
    }
  }

  "msearchResponseToTree" should {

    "rebuild the responses array with per-item envelopes and full-fidelity failures" in {
      val src = sourceNode()
      val response = MsearchResponse.of[ObjectNode] { builder =>
        builder
          .took(1)
          .responses(
            MultiSearchResponseItem.of[ObjectNode](item =>
              item.result(r =>
                r.took(1)
                  .timedOut(false)
                  .shards(s => s.total(1).successful(1).failed(0))
                  .hits(h => h.hits(Collections.singletonList(hitOf(Some("1"), Some(src)))))
                  .status(200)
              )
            ),
            MultiSearchResponseItem.of[ObjectNode](item =>
              item.failure(f =>
                f.error(e => e.`type`("search_phase_execution_exception").reason("boom"))
                  .status(500)
              )
            )
          )
      }

      val tree = Companion.msearchTree(response)
      val responses = tree.path("responses")
      responses.isArray shouldBe true
      responses.size() shouldBe 2

      val first = responses.get(0)
      first.path("hits").path("hits").get(0).get("_source") should be theSameInstanceAs src

      val second = responses.get(1)
      second.path("error").path("reason").asText() shouldBe "boom"
      second.path("status").asInt() shouldBe 500
    }
  }
}
