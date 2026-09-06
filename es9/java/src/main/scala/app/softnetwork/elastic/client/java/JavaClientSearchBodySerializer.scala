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

import app.softnetwork.elastic.sql.bridge.{ScriptedExtendedStatsAggregation, SearchBodySerializer}
import com.sksamuel.elastic4s.handlers.script.ScriptBuilderFn
import com.sksamuel.elastic4s.json.{XContentBuilder, XContentFactory}
import com.sksamuel.elastic4s.requests.searches.aggs.{
  AbstractAggregation,
  AggMetaDataFn,
  SubAggsBuilderFn
}
import com.sksamuel.elastic4s.requests.searches.{
  defaultCustomAggregationHandler,
  SearchBodyBuilderFn,
  SearchRequest
}

/** The ES 8 / ES 9 search-body serializer (issue #222): elastic4s's two-argument
  * `SearchBodyBuilderFn.apply(request, customAggregation)` with a handler for the bridge's
  * [[ScriptedExtendedStatsAggregation]] marker -- an `extended_stats` over a TRANSFORMED expression
  * (`STDDEV(YEAR(createdAt))`, `VARIANCE(ABS(salary))`, ...).
  *
  * elastic4s's own `ExtendedStatsAggregationBuilder` never emits `agg.script` (the only metric
  * builder of ten with the omission; fixed upstream once in elastic4s#2700 onto a dead branch,
  * re-submitted as elastic4s#4100). Its typed `case agg: ExtendedStatsAggregation` arm in
  * `AggregationBuilderFn` also runs BEFORE the custom handler, which is why the bridge binds the
  * shape to a marker type the library does not know: the handler below is consulted for exactly
  * that type and nothing else. Every other aggregation goes through the library's own builders, so
  * this serializer is byte-identical to the one-argument default for any request without a marker.
  *
  * The handler renders what `ExtendedStatsAggregationBuilder` renders -- `field`, `sigma`,
  * `missing` -- PLUS the `script` (through the same `ScriptBuilderFn` the Stats / Max / Avg
  * builders use), then the sub-aggregations and metadata. Watch item (spec AD-S3-3): when
  * elastic4s#4100 ships and `Versions.elastic84s` / `elastic94s` move past it, the marker can be
  * bound back to the library type and this handler deleted.
  *
  * Kept byte-identical between the ES 8 and ES 9 modules (their elastic4s builders are identical).
  */
object JavaClientSearchBodySerializer extends SearchBodySerializer {

  private val scriptedExtendedStats: PartialFunction[AbstractAggregation, XContentBuilder] = {
    case agg: ScriptedExtendedStatsAggregation => extendedStatsWithScript(agg)
  }

  /** Our one case, then elastic4s's own fallback. The `orElse` is not decoration: this partial
    * function REPLACES `defaultCustomAggregationHandler`, whose only job is to fail an unknown
    * aggregation with a `NotImplementedError` naming the class. Ours alone would raise a bare
    * `MatchError` instead -- a strictly worse diagnostic for anything the library cannot build.
    *
    * Declared AFTER `scriptedExtendedStats`: a `val` reading a `val` defined below it sees `null`,
    * and the object's initialiser then dies with `ExceptionInInitializerError` on first use.
    */
  private val handler: PartialFunction[AbstractAggregation, XContentBuilder] =
    scriptedExtendedStats orElse defaultCustomAggregationHandler

  private def extendedStatsWithScript(agg: ScriptedExtendedStatsAggregation): XContentBuilder = {
    val inner = agg.inner
    val builder = XContentFactory.jsonBuilder()
    builder.startObject("extended_stats")
    inner.field.foreach(builder.field("field", _))
    inner.sigma.foreach(builder.field("sigma", _))
    inner.missing.foreach(builder.autofield("missing", _))
    // The one line elastic4s's ExtendedStatsAggregationBuilder lacks (elastic4s#4100).
    inner.script.foreach(script => builder.rawField("script", ScriptBuilderFn(script)))
    builder.endObject()
    // Sub-aggregations and metadata are siblings of the aggregation type, as Elasticsearch
    // expects them (`"aggs"` / `"meta"` beside `"extended_stats"`). The bridge never attaches either
    // to an extended_stats, so the rendered shape stays identical to the default builder's.
    SubAggsBuilderFn(inner, builder, handler)
    AggMetaDataFn(inner, builder)
    builder.endObject()
  }

  override def serialize(search: SearchRequest): String =
    SearchBodyBuilderFn(search, handler).string
}
