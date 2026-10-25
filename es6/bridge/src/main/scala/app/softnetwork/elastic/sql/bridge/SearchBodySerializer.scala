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

import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.searches.aggs.{AbstractAggregation, Aggregation}

/** The ONE place a bridge `SearchRequest` becomes a JSON body (issue #222). Hand-maintained ES 6
  * twin of the bridge template's seam (elastic4s 6.7.8 package names; same contract).
  *
  * The ES 6 client modules (REST and Jest) put an implicit `SearchBodySerializer` in scope of the
  * `SingleSearch` conversions (`requestToElasticSearchRequest`, `sqlQueryToAggregations`), and
  * every serialisation door consumes it. Without one, [[SearchBodySerializer.Default]] applies.
  */
trait SearchBodySerializer {

  /** The JSON body of `search`, exactly as the transport sends it. */
  def serialize(search: SearchRequest): String
}

object SearchBodySerializer {

  /** True when the request carries an `extended_stats` over a transformed expression -- any
    * [[ScriptedExtendedStatsAggregation]] anywhere in its aggregation tree (plain or windowed
    * bind).
    */
  def hasTransformExtendedStats(search: SearchRequest): Boolean =
    ScriptedExtendedStatsAggregation.existsIn(search.aggs)

  /** The version-agnostic refusal the Default serializer raises (issue #222). */
  val TransformExtendedStatsUnsupported: String =
    "STDDEV/VARIANCE over a transformed expression cannot be serialised by the default " +
    "Elasticsearch body builder: the underlying elastic4s builder drops the aggregation script " +
    "(elastic4s#4100), so the statistic would silently be computed over the raw field. " +
    "Aggregate over a raw field, or use Elasticsearch 8+."

  /** The refusal an ES-major-aware client module raises for the same shape, naming its major. */
  def transformExtendedStatsUnsupportedOn(major: Int): String =
    s"STDDEV/VARIANCE over a transformed expression is not supported on Elasticsearch $major: " +
    "the underlying elastic4s builder drops the aggregation script (elastic4s#4100), so the " +
    "statistic would silently be computed over the raw field. Aggregate over a raw field, or use " +
    "Elasticsearch 8+."

  /** The GROUP BY key whose `terms` filter pairs a pattern with a list of two or more values -- the
    * one key-filter shape Elasticsearch 6.8 rejects outright (see [[TermsIncludeExclude]]).
    */
  def unmixableTermsFilter(search: SearchRequest): Option[String] =
    TermsIncludeExclude.unmixableIn(search.aggs)

  /** Why that shape cannot be sent, and what to write instead. The key NAME is quoted; values never
    * are -- they are data, and the gateway logs every refusal.
    */
  private def unmixableTermsFilterReason(key: String, venue: String): String =
    s"HAVING cannot filter the GROUP BY key '$key' $venue: it combines a pattern (LIKE, RLIKE or " +
    "MATCH) with a list of several values, and Elasticsearch before 7.11 applies a key filter's " +
    "kept and removed terms together only when both are patterns or both are value lists. Write " +
    "the value list as a single RLIKE pattern, escaping any regular-expression metacharacter a " +
    "value carries (for example NOT RLIKE 'b|c', and 'a\\.b' for the value a.b), or use " +
    "Elasticsearch 7.11 or later."

  /** The version-agnostic refusal the Default serializer raises for that shape. */
  def unmixableTermsFilterUnsupported(key: String): String =
    unmixableTermsFilterReason(key, "with the default Elasticsearch body builder")

  /** The refusal an ES-major-aware client module raises for the same shape, naming its major. */
  def unmixableTermsFilterOn(major: Int, key: String): String =
    unmixableTermsFilterReason(key, s"on Elasticsearch $major")

  /** The metadata key under which a bridge level carries the refusal of its own key (see
    * `ElasticAggregation.dateFormatOfDocValueUnsupportedOn`). Every serializer raises it before any
    * JSON exists, so it is never sent.
    */
  val Refusal: String = "softclient4es_refusal"

  /** The refusal a level of `search` carries, anywhere in its aggregation tree. */
  def refusedLevel(search: SearchRequest): Option[String] = refusedIn(search.aggs)

  private def refusedIn(aggs: Iterable[AbstractAggregation]): Option[String] =
    aggs.iterator
      .map {
        case agg: Aggregation =>
          agg.metadata.get(Refusal).map(_.toString).orElse(refusedIn(agg.subaggs))
        case _ => None
      }
      .collectFirst { case Some(message) => message }

  /** Today's behaviour -- the one-argument `SearchBodyBuilderFn` -- guarded against the shapes it
    * cannot render honestly. A transform-bearing extended_stats has no script-emitting builder on
    * this line, so the request is REFUSED, loudly and by name, instead of leaving as the statistic
    * of the wrong field; and a GROUP BY key filter pairing a pattern with several values is refused
    * by name instead of reaching Elasticsearch 6.8's own "Cannot mix" error.
    */
  object Default extends SearchBodySerializer {
    override def serialize(search: SearchRequest): String = {
      if (hasTransformExtendedStats(search))
        throw new UnsupportedOperationException(TransformExtendedStatsUnsupported)
      unmixableTermsFilter(search).foreach { key =>
        throw new UnsupportedOperationException(unmixableTermsFilterUnsupported(key))
      }
      refusedLevel(search).foreach { message =>
        throw new UnsupportedOperationException(message)
      }
      SearchBodyBuilderFn(search).string()
    }
  }
}
