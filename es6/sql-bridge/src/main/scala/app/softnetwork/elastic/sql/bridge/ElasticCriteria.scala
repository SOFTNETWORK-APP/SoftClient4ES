/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.sql.query.Criteria
import com.sksamuel.elastic4s.searches.queries.Query

case class ElasticCriteria(criteria: Criteria) {

  def asQuery(group: Boolean = true, innerHitsNames: Set[String] = Set.empty): Query = {
    val query = criteria.boolQuery.copy(group = group)
    query
      .filter(criteria.asFilter(Option(query)))
      .unfilteredMatchCriteria()
      .query(innerHitsNames, Option(query))
  }
}
