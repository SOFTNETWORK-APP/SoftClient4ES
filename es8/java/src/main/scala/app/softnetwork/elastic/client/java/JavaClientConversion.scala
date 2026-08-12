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

import app.softnetwork.elastic.sql.serialization.JacksonConfig
import co.elastic.clients.elasticsearch.core.{MsearchResponse, SearchResponse}
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.json.JsonpSerializable
import co.elastic.clients.json.jackson.{JacksonJsonpGenerator, JacksonJsonpMapper}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.util.TokenBuffer

import java.io.{IOException, StringWriter}
import scala.util.Try

trait JavaClientConversion { _: JavaClientCompanion =>
  private[this] val jsonpMapper = new JacksonJsonpMapper(JacksonConfig.objectMapper)

  /** Convert any Elasticsearch response to JSON string */
  protected def convertToJson[T <: JsonpSerializable](response: T): String = {
    val stringWriter = new StringWriter()
    val generator = jsonpMapper.jsonProvider().createGenerator(stringWriter)
    try {
      response.serialize(generator, jsonpMapper)
      generator.flush()
      stringWriter.toString
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to convert response to JSON: ${ex.getMessage}", ex)
        throw new IOException("Failed to serialize Elasticsearch response", ex)
    } finally {
      Try(generator.close()).failed.foreach { ex =>
        logger.warn(s"Failed to close JSON generator: ${ex.getMessage}")
      }
    }
  }

  /** Convert any Elasticsearch response to a Jackson tree without going through a JSON string.
    *
    * The response is serialized into a [[TokenBuffer]] — a token-level copy with no character
    * writing, no string escaping and no re-parsing — and the tree is read back from those tokens.
    * On the scroll hot path the string round-trip (serialize to `StringWriter`, then
    * `mapper.readTree`) was measured at ~19% of the sidecar CPU during JOIN leg extraction
    * (softclient4es-arrow#160), dominated by per-character string writing and re-parsing costs that
    * both scale with the number and width of the selected columns.
    */
  protected def convertToTree[T <: JsonpSerializable](response: T): JsonNode = {
    val buffer = new TokenBuffer(JacksonConfig.objectMapper, false)
    val generator = new JacksonJsonpGenerator(buffer)
    try {
      response.serialize(generator, jsonpMapper)
      generator.flush()
      val parser = buffer.asParser()
      try {
        val tree: JsonNode = JacksonConfig.objectMapper.readTree(parser)
        tree
      } finally {
        Try(parser.close()).failed.foreach { ex =>
          logger.warn(s"Failed to close token-buffer parser: ${ex.getMessage}")
        }
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to convert response to a Jackson tree: ${ex.getMessage}", ex)
        throw new IOException("Failed to convert Elasticsearch response to a Jackson tree", ex)
    } finally {
      Try(generator.close()).failed.foreach { ex =>
        logger.warn(s"Failed to close JSON generator: ${ex.getMessage}")
      }
    }
  }

  /** Build the minimal response-envelope tree the row parser consumes, from hits whose `_source`
    * was already parsed as a Jackson tree by the transport (document type [[ObjectNode]]).
    *
    * This is the single-parse hits path (softclient4es-arrow#160): each `_source` node is
    * re-parented into the envelope untouched — no serialization, no re-parse. The row parser reads
    * exactly `_id`, `_source`, `inner_hits` and `fields` per hit, so hits carrying `inner_hits` or
    * `fields` (UNNEST legs, script fields) fall back to a whole-hit [[convertToTree]] to keep full
    * shape fidelity — still token-level, never a string.
    */
  protected def hitsToResponseNode(hits: _root_.java.util.List[Hit[ObjectNode]]): ObjectNode = {
    val root = JacksonConfig.objectMapper.createObjectNode()
    val hitsArray = root.putObject("hits").putArray("hits")
    hits.forEach { hit =>
      if (hit.innerHits().isEmpty && hit.fields().isEmpty) {
        val hitNode = hitsArray.addObject()
        Option(hit.id()).foreach(id => hitNode.put("_id", id))
        Option(hit.source()).foreach(source => hitNode.set[JsonNode]("_source", source))
      } else {
        hitsArray.add(convertToTree(hit))
      }
    }
    root
  }

  /** Response tree of a one-shot search (#228), single-parse.
    *
    * Hits-only responses — the common row-shaped case — re-parent the `_source` trees the transport
    * already parsed (see [[hitsToResponseNode]]); aggregation-bearing responses serialize the whole
    * envelope once at token level via [[convertToTree]], never through a String.
    */
  protected def searchResponseToTree(response: SearchResponse[ObjectNode]): JsonNode =
    if (response.aggregations() != null && !response.aggregations().isEmpty)
      convertToTree(response)
    else hitsToResponseNode(response.hits().hits())

  /** Response tree of a multi search (#228), single-parse.
    *
    * The `responses` array is rebuilt item by item with the same policy as
    * [[searchResponseToTree]]; a failed item keeps full fidelity so core still sees its `error`
    * object.
    */
  protected def msearchResponseToTree(response: MsearchResponse[ObjectNode]): JsonNode = {
    val root = JacksonConfig.objectMapper.createObjectNode()
    val responses = root.putArray("responses")
    response.responses().forEach { item =>
      if (item.isResult) {
        val result = item.result()
        if (result.aggregations() != null && !result.aggregations().isEmpty) {
          responses.add(convertToTree(result))
        } else {
          responses.add(hitsToResponseNode(result.hits().hits()))
        }
      } else {
        responses.add(convertToTree(item))
      }
    }
    root
  }
}
