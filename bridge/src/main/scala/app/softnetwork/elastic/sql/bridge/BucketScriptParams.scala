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

package app.softnetwork.elastic.sql.bridge

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import com.sksamuel.elastic4s.requests.searches.aggs.{AbstractAggregation, Aggregation}
import com.sksamuel.elastic4s.requests.searches.aggs.pipeline.BucketScriptPipelineAgg

import scala.jdk.CollectionConverters._

/** The PARAMS of a `bucket_script`'s script, which the elastic4s body builder drops.
  *
  * 🔴 elastic4s writes a `bucket_script` as `"script": "<source>"` -- the bare source string
  * (`BucketScriptPipelineAggBuilder`, the same in elastic4s 6.7, 7.17, 8.18 and 9.0) -- while a
  * `bucket_selector` gets the whole script object. So a per-group calculation reading the request
  * clock, `CURRENT_DATE - MAX(d)` or `NOW() - MAX(ts)`, was sent WITHOUT the `__now__` its script
  * reads, and Elasticsearch failed it; the same clock in a HAVING worked. MEASURED on 8.18.3.
  *
  * The script is sent as the object Elasticsearch documents -- `source`, `lang`, `params` -- for
  * exactly the `bucket_script`s that carry params, so the source stays the SAME text from one
  * request to the next and Elasticsearch compiles it once: inlining the clock instead would compile
  * a new script on every request, against the cluster's compilation rate limit. Every other body is
  * returned untouched, unparsed.
  */
object BucketScriptParams {

  private val mapper = new ObjectMapper()

  /** The params of every `bucket_script` of `aggs` that carries some, by aggregation name, at any
    * depth.
    */
  private def withParams(aggs: Seq[AbstractAggregation]): Map[String, Map[String, Any]] =
    aggs.flatMap {
      case script: BucketScriptPipelineAgg if script.script.params.nonEmpty =>
        Seq(script.name -> script.script.params)
      case parent: Aggregation => withParams(parent.subaggs).toSeq
      case _                   => Nil
    }.toMap

  /** `body`, the JSON of `search`, with the params of its `bucket_script`s bound. */
  def bind(body: String, search: SearchRequest): String = {
    val params = withParams(search.aggs)
    if (params.isEmpty) body
    else {
      val root = mapper.readTree(body)
      bind(root, params)
      mapper.writeValueAsString(root)
    }
  }

  private def bind(node: JsonNode, params: Map[String, Map[String, Any]]): Unit =
    node match {
      case obj: ObjectNode =>
        obj.fields().asScala.toList.foreach { entry =>
          (params.get(entry.getKey), entry.getValue) match {
            case (Some(values), agg: ObjectNode) if agg.has("bucket_script") =>
              agg.get("bucket_script") match {
                case bucketScript: ObjectNode if bucketScript.path("script").isTextual =>
                  val script = mapper.createObjectNode()
                  script.put("source", bucketScript.get("script").asText())
                  script.put("lang", "painless")
                  val bound = script.putObject("params")
                  values.foreach { case (name, value) =>
                    bound.set[JsonNode](name, mapper.valueToTree[JsonNode](value))
                  }
                  bucketScript.set[JsonNode]("script", script)
                case _ =>
              }
            case _ =>
          }
          bind(entry.getValue, params)
        }
      case array: ArrayNode => array.elements().asScala.foreach(bind(_, params))
      case _                =>
    }
}
