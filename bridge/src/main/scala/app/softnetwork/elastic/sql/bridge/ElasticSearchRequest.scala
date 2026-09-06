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

import app.softnetwork.elastic.sql.query.{Bucket, Criteria, Except, Field, FieldSort}
import com.sksamuel.elastic4s.requests.searches.SearchRequest

case class ElasticSearchRequest(
  sql: String,
  fields: Seq[Field],
  except: Option[Except],
  sources: Seq[String],
  criteria: Option[Criteria],
  limit: Option[Int],
  offset: Option[Int],
  search: SearchRequest,
  buckets: Seq[Bucket] = Seq.empty,
  having: Option[Criteria] = None,
  sorts: Seq[FieldSort] = Seq.empty,
  // The body serializer the client module injected through the SingleSearch conversion (issue
  // #222); Default = the one-argument elastic4s builder, refusing a transform-bearing extended_stats.
  serializer: SearchBodySerializer = SearchBodySerializer.Default
) {
  def minScore(score: Option[Double]): ElasticSearchRequest = {
    score match {
      case Some(s) => this.copy(search = search minScore s)
      case _       => this
    }
  }

  /** True when this request carries `STDDEV` / `VARIANCE` (any of the `extended_stats` family) over
    * a TRANSFORMED expression -- plain (`STDDEV(YEAR(x))`) or windowed (`... OVER (PARTITION BY
    * ...)`). The shape a client module must either render with its script or refuse (issue #222).
    */
  def hasTransformExtendedStats: Boolean = SearchBodySerializer.hasTransformExtendedStats(search)

  def query: String =
    serializer.serialize(search).replace("\"version\":true,", "") /*FIXME*/
}
