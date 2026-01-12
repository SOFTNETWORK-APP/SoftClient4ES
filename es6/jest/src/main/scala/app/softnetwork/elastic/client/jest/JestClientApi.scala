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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql.bridge._
import io.searchbox.action.BulkableAction
import io.searchbox.core._
import org.json4s.Formats

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

/** Created by smanciot on 20/05/2021.
  */
trait JestClientApi
    extends ElasticClientApi
    with JestIndicesApi
    with JestAliasApi
    with JestSettingsApi
    with JestMappingApi
    with JestRefreshApi
    with JestFlushApi
    with JestCountApi
    with JestIndexApi
    with JestUpdateApi
    with JestDeleteApi
    with JestGetApi
    with JestSearchApi
    with JestScrollApi
    with JestBulkApi
    with JestVersionApi
    with JestPipelineApi
    with JestTemplateApi
    with JestClientCompanion

object JestClientApi extends SerializationApi {

  implicit def requestToSearch(elasticSelect: ElasticSearchRequest): Search = {
    import elasticSelect._
    val search = new Search.Builder(query)
    for (source <- sources) search.addIndex(source)
    search.build()
  }

  implicit class SearchElasticQuery(elasticQuery: ElasticQuery) {
    def search: (Search, String) = {
      import elasticQuery._
      val _search = new Search.Builder(query)
      for (indice <- indices) _search.addIndex(indice)
      for (t      <- types) _search.addType(t)
      (_search.build(), query)
    }
  }

  implicit class SearchResults(searchResult: SearchResult) {
    def apply[M: Manifest]()(implicit formats: Formats): List[M] = {
      searchResult.getSourceAsStringList.asScala.map(source => serialization.read[M](source)).toList
    }
  }

  implicit class JestBulkAction(bulkableAction: BulkableAction[DocumentResult]) {
    def index: String = bulkableAction.getIndex
  }
}
