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

package app.softnetwork.elastic.client.rest

import app.softnetwork.elastic.sql.bridge.{ScriptedExtendedStatsAggregation, SearchBodySerializer}
import com.sksamuel.elastic4s.requests.searches.SearchRequest
import com.sksamuel.elastic4s.requests.searches.aggs.{AbstractAggregation, Aggregation}

/** The ES 7 search-body serializer (issue #222): **UNWRAP**, then serialise with elastic4s's own
  * one-argument builder.
  *
  * Since elastic4s **7.17.26** (`nl.gn0s1s`, the series/7.x backport elastic4s#4105 of #4100) the
  * library's `ExtendedStatsAggregationBuilder` emits `agg.script` natively, so this module computes
  * the statistic over the transform like ES 8 / ES 9 do — it no longer refuses. What it does NOT
  * have is the two-argument `SearchBodyBuilderFn.apply(request, customAggregation)` seam the ES 8 /
  * ES 9 modules render through: on the 7.x line both `SearchBodyBuilderFn.apply` and
  * `AggregationBuilderFn.apply` are one-argument, and no custom handler can be injected.
  *
  * That is the whole reason a third behaviour exists beside *render* (ES 8 / ES 9) and *refuse*
  * (ES 6). The version-agnostic bridge template binds a transform-bearing `extended_stats` to
  * [[ScriptedExtendedStatsAggregation]] — a type elastic4s does not know — so handing it to the
  * 7.x builder would raise `NotImplementedError`. Here the marker is simply substituted back for
  * the `ExtendedStatsAggregation` it wraps, throughout the aggregation tree, and the resulting
  * request is serialised by the stock builder, which now renders the script itself.
  *
  * The template stays version-agnostic: it knows only that a marker exists, never which majors can
  * render it. The unwrap lives HERE rather than on `SearchBodySerializer` because it is not shared
  * behaviour — ES 8 / ES 9 must NOT unwrap (their pinned elastic4s, 8.18.2 / 9.0.0, still drops the
  * script; they need the marker to reach their handler) and ES 6 must not either (it refuses).
  * One major needs it today, so it is written once, where it is true (Rule of Three).
  *
  * Watch item (spec AD-S3-3 / AD-S3-4): when an 8.x / 9.x release carrying elastic4s#4106 / #4100
  * exists and those pins move, the ES 8 / ES 9 handler retires the same way and the marker can be
  * dropped from the template altogether.
  */
object RestHighLevelClientSearchBodySerializer extends SearchBodySerializer {

  val ElasticsearchMajor: Int = 7

  /** The request with every [[ScriptedExtendedStatsAggregation]] replaced by the
    * `ExtendedStatsAggregation` it wraps — at the root and at every depth (the windowed bind sits
    * under a partition bucket). Any other request is returned untouched, so a body without a marker
    * is byte-identical to what the stock builder produced before this story.
    */
  private[client] def unwrap(search: SearchRequest): SearchRequest =
    if (!SearchBodySerializer.hasTransformExtendedStats(search)) search
    else search.aggregations(unwrapAll(search.aggs))

  private def unwrapAll(aggs: Iterable[AbstractAggregation]): Seq[AbstractAggregation] =
    aggs.toSeq.map {
      case marker: ScriptedExtendedStatsAggregation => marker.inner
      case agg: Aggregation if agg.subaggs.nonEmpty => agg.subAggregations(unwrapAll(agg.subaggs))
      case other                                    => other
    }

  override def serialize(search: SearchRequest): String =
    SearchBodySerializer.Default.serialize(unwrap(search))
}
