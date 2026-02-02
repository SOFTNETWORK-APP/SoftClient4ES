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

import app.softnetwork.elastic.client.{ElasticQueries, ElasticQuery, SearchApi, SerializationApi}
import app.softnetwork.elastic.client.result.ElasticResult
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.bridge.ElasticSearchRequest
import app.softnetwork.elastic.sql.query.SingleSearch
import io.searchbox.core.{MultiSearch, Search, SearchResult}
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

trait JestSearchApi extends SearchApi with JestClientHelpers {
  _: JestClientCompanion =>

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
      searchResult.getSourceAsStringList.asScala
        .map(source => SerializationApi.serialization.read[M](source))
        .toList
    }
  }

  private[client] implicit def singleSearchToJsonQuery(
    sqlSearch: SingleSearch
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  override def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]] =
    executeJestAction(
      operation = "executeSingleSearch",
      index = Some(elasticQuery.indices.mkString(",")),
      retryable = true
    ) {
      elasticQuery.search._1
    }(result =>
      if (result.isSucceeded) {
        Some(result.getJsonString)
      } else {
        None
      }
    )

  private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]] =
    executeJestAction(
      operation = "executeMultiSearch",
      index = Some(
        elasticQueries.queries
          .flatMap(_.indices)
          .distinct
          .mkString(",")
      ),
      retryable = true
    ) {
      new MultiSearch.Builder(
        elasticQueries.queries.map(_.search._1).asJava
      ).build()
    }(result =>
      if (result.isSucceeded) {
        Some(result.getJsonString)
      } else {
        None
      }
    )

  override def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] =
    executeAsyncJestAction(
      operation = "executeSingleSearchAsync",
      index = Some(elasticQuery.indices.mkString(",")),
      retryable = true
    ) {
      elasticQuery.search._1
    }(result =>
      if (result.isSucceeded) {
        Some(result.getJsonString)
      } else {
        None
      }
    )

  override private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    executeAsyncJestAction(
      operation = "executeMultiSearchAsync",
      index = Some(
        elasticQueries.queries
          .flatMap(_.indices)
          .distinct
          .mkString(",")
      ),
      retryable = true
    ) {
      new MultiSearch.Builder(
        elasticQueries.queries.map(_.search._1).asJava
      ).build()
    }(result =>
      if (result.isSucceeded) {
        Some(result.getJsonString)
      } else {
        None
      }
    )

}
