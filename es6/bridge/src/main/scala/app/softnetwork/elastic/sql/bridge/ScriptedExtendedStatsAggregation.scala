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

import com.sksamuel.elastic4s.searches.aggs.{
  AbstractAggregation,
  Aggregation,
  ExtendedStatsAggregation
}

/** An `extended_stats` aggregation over a TRANSFORMED expression -- `STDDEV(YEAR(createdAt))`,
  * `VARIANCE(ABS(salary))` and the rest of the family (issue #222). Hand-maintained ES 6 twin of
  * the bridge template's marker (elastic4s 6.7.8 package names; same contract).
  *
  * elastic4s's own `ExtendedStatsAggregationBuilder` never emits `agg.script`, so an
  * `ExtendedStatsAggregation` carrying a script silently serialises as the statistic of the RAW
  * field -- or as `extended_stats: {}` when there is no raw field to fall back on. The bridge
  * therefore binds a transform-bearing extended_stats to this marker instead of to the library type
  * it wraps. The 6.x line has no script-emitting builder and no customisation seam, so the marker
  * is never rendered here: [[SearchBodySerializer.Default]] refuses it with a named message, and
  * the ES 6 client modules (REST and Jest) refuse it with an `ElasticError` naming their major.
  * Left to elastic4s, it would still fail loudly (`AggregationBuilderFn`'s `NotImplementedError`)
  * -- it can never leave as silently-wrong JSON.
  */
final case class ScriptedExtendedStatsAggregation(inner: ExtendedStatsAggregation)
    extends Aggregation {

  require(
    inner.script.isDefined,
    "ScriptedExtendedStatsAggregation wraps an extended_stats that carries a script"
  )

  type T = ScriptedExtendedStatsAggregation

  override def name: String = inner.name

  override def metadata: Map[String, AnyRef] = inner.metadata

  override def subaggs: Seq[AbstractAggregation] = inner.subaggs

  override def subAggregations(aggs: Iterable[AbstractAggregation]): T =
    copy(inner = inner.subAggregations(aggs))

  override def metadata(map: Map[String, AnyRef]): T = copy(inner = inner.metadata(map))
}

object ScriptedExtendedStatsAggregation {

  /** True when `aggs`, or any aggregation nested below them, is a
    * [[ScriptedExtendedStatsAggregation]] -- the shape discriminator behind
    * `hasTransformExtendedStats` (issue #222). Both binds are covered by construction: the plain
    * `STDDEV(f(x))` metric and the windowed `STDDEV(f(x)) OVER (PARTITION BY ...)` metric are the
    * same marker, one at the root and one under a partition bucket.
    */
  def existsIn(aggs: Iterable[AbstractAggregation]): Boolean =
    aggs.exists {
      case _: ScriptedExtendedStatsAggregation => true
      case a: Aggregation                      => existsIn(a.subaggs)
      case _                                   => false
    }
}
