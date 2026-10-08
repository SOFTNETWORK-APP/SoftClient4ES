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
import app.softnetwork.elastic.sql.query.SingleSearch
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import com.sksamuel.elastic4s.requests.searches.aggs.{
  AbstractAggregation,
  Aggregation,
  CardinalityAggregation,
  KeyedFiltersAggregation,
  SumAggregation,
  ValueCountAggregation
}
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
  * a new script on every request, against the cluster's compilation rate limit.
  *
  * The same builder never writes a `gap_policy` either (and elastic4s 7.17 to 9.0 knows only `skip`
  * and `insert_zeros`), so the `keep_values` of a whole-table calculation is written here too
  * ([[keepingValues]]). Every other body is returned untouched, unparsed.
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

  /** The `bucket_script`s of the whole-table bucket (issue #413) whose every operand is a count or
    * a sum -- `_count`, `value_count`, `cardinality`, `sum` -- by name.
    *
    * Over ZERO documents Elasticsearch takes every `buckets_path` but `_count` for a gap, and its
    * default gap policy skips the calculation: `COUNT(x) * 2` answered NULL where SQL answers 0.
    * `keep_values` hands the script the value instead -- 0, or 0.0 for a sum (core's `SUM` over no
    * value, never NULL) -- and these four metrics are never `NaN` or infinite, so the script never
    * reads one. A calculation over a `MIN`, a `MAX`, an `AVG`... keeps the default: there
    * `keep_values` would hand the script `NaN`, which a cast turns into a value --
    * `DATEDIFF(MAX(d), MIN(d))` reads `(long) NaN`, 0, and answered 0 days over no document instead
    * of NULL (MEASURED on 8.18.3) -- while skipping answers NULL, as SQL does.
    *
    * 🔴 Elasticsearch 7.17 and later only: 6.8 refuses the policy (`Invalid gap policy:
    * [keep_values], accepted values: [insert_zeros, skip]`), and its bridge does not send it.
    */
  private def keepingValues(aggs: Seq[AbstractAggregation]): Set[String] =
    aggs
      .collectFirst {
        case wholeTable: KeyedFiltersAggregation
            if wholeTable.name == SingleSearch.WholeTableHavingAgg =>
          wholeTable.subaggs
      }
      .map { subaggs =>
        val counts: Set[String] = subaggs.collect {
          case count: ValueCountAggregation     => count.name
          case distinct: CardinalityAggregation => distinct.name
          case sum: SumAggregation              => sum.name
        }.toSet
        subaggs.collect {
          case script: BucketScriptPipelineAgg
              if script.bucketsPaths.values.forall(path => path == "_count" || counts(path)) =>
            script.name
        }.toSet
      }
      .getOrElse(Set.empty)

  /** `body`, the JSON of `search`, with the params of its `bucket_script`s bound and the gap policy
    * of its whole-table calculations written.
    */
  def bind(body: String, search: SearchRequest): String = {
    val params = withParams(search.aggs)
    val keepValues = keepingValues(search.aggs)
    if (params.isEmpty && keepValues.isEmpty) body
    else {
      val root = mapper.readTree(body)
      if (params.nonEmpty) bind(root, params)
      if (keepValues.nonEmpty)
        root.path("aggs").path(SingleSearch.WholeTableHavingAgg).path("aggs") match {
          case wholeTable: ObjectNode =>
            keepValues.foreach { name =>
              wholeTable.path(name).path("bucket_script") match {
                case bucketScript: ObjectNode => bucketScript.put("gap_policy", "keep_values")
                case _                        =>
              }
            }
          case _ =>
        }
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
