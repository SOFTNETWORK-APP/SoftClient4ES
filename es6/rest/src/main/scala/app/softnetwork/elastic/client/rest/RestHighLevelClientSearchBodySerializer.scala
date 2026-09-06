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

import app.softnetwork.elastic.client.result.ElasticError
import app.softnetwork.elastic.sql.bridge.SearchBodySerializer
import com.sksamuel.elastic4s.searches.SearchRequest

/** The ES 6 (REST) search-body serializer (issue #222): the default one-argument elastic4s builder, with a
  * LOUD refusal of `STDDEV` / `VARIANCE` (any of the `extended_stats` family) over a transformed
  * expression -- plain or windowed -- BEFORE any JSON exists.
  *
  * elastic4s 6.7.8 has neither a script-emitting `ExtendedStatsAggregationBuilder` nor the
  * `customAggregation` seam the ES 8 / ES 9 modules use to render one, so this module cannot
  * compute the statistic over the transform; it used to compute it silently over the RAW field.
  * The refusal is an `ElasticError` with status 400 so it reaches the caller as an honest failure.
  * The 6.x elastic4s line is unmaintained: there is no upstream path (spec R-1b), so unlike the
  * ES 7 module this refusal is permanent.
  */
object RestHighLevelClientSearchBodySerializer extends SearchBodySerializer {

  val ElasticsearchMajor: Int = 6

  /** The named error a transform-bearing extended_stats is refused with on this module. */
  val TransformExtendedStatsUnsupported: String =
    SearchBodySerializer.transformExtendedStatsUnsupportedOn(ElasticsearchMajor)

  override def serialize(search: SearchRequest): String = {
    if (SearchBodySerializer.hasTransformExtendedStats(search))
      throw ElasticError(
        message = TransformExtendedStatsUnsupported,
        statusCode = Some(400),
        operation = Some("search")
      )
    SearchBodySerializer.Default.serialize(search)
  }
}
