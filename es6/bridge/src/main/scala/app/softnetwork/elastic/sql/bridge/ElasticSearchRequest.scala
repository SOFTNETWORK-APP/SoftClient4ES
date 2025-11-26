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

import app.softnetwork.elastic.sql.query.{Bucket, Criteria, Except, Field, FieldSort, Limit}
import com.sksamuel.elastic4s.searches.SearchRequest
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn

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
  sorts: Seq[FieldSort] = Seq.empty
) {
  def minScore(score: Option[Double]): ElasticSearchRequest = {
    score match {
      case Some(s) => this.copy(search = search minScore s)
      case _       => this
    }
  }

  def query: String =
    SearchBodyBuilderFn(search).string.replace("\"version\":true,", "") /*FIXME*/
}
