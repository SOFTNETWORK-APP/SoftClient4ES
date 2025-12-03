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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.sql.macros.SQLQueryMacros
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import com.google.gson.{Gson, JsonElement, JsonObject, JsonParser}
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.experimental.macros
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

//format:off
/** Elasticsearch search API with unified error handling via ElasticResult.
  *
  * @example
  * {{{
  *   class MyClient extends SearchApi {
  *     // Implementation of abstract methods
  *   }
  *
  *   val client = new MyClient()
  *   val result = client.searchAs[User]("SELECT * FROM users WHERE age > 30")
  * }}}
  */
//format:on
trait SearchApi extends ElasticConversion with ElasticClientHelpers {

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Search for documents / aggregations matching the SQL query.
    *
    * @param sql
    *   the SQL query to execute
    * @return
    *   the Elasticsearch response
    */
  def search(sql: SQLQuery): ElasticResult[ElasticResponse] = {
    sql.request match {
      case Some(Left(single)) =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*),
          sql = Some(sql.query)
        )
        if (single.windowFunctions.exists(_.isWindowing) && single.groupBy.isEmpty)
          searchWithWindowEnrichment(sql, single)
        else
          singleSearch(elasticQuery, single.fieldAliases, single.sqlAggregations)

      case Some(Right(multiple)) =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList,
          sql = Some(sql.query)
        )
        multiSearch(elasticQueries, multiple.fieldAliases, multiple.sqlAggregations)

      case None =>
        logger.error(
          s"❌ Failed to execute search for query \n${sql.query}"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"SQL query does not contain a valid search request\n${sql.query}",
            operation = Some("search")
          )
        )
    }
  }

  /** Search for documents / aggregations matching the Elasticsearch query.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   the Elasticsearch response
    */
  def singleSearch(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] = {
    validateJson("search", elasticQuery.query) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid query: ${error.message}",
            statusCode = Some(400),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("search")
          )
        )
      case None => // continue
    }

    val sql = elasticQuery.sql
    val query = elasticQuery.query
    val indices = elasticQuery.indices.mkString(",")

    logger.info(
      s"🔍 Searching with query \n$elasticQuery\nin indices '$indices'"
    )

    executeSingleSearch(elasticQuery) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed search for query \n$elasticQuery\nin indices '$indices'"
        )
        val aggs = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
        ElasticResult.fromTry(parseResponse(response, fieldAliases, aggs)) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed search results for query \n$elasticQuery\nin indices '$indices'"
            )
            ElasticResult.success(
              ElasticResponse(
                sql,
                query,
                success.value,
                fieldAliases,
                aggs
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse search results for query \n${sql
                .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("search"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute search for query \n$elasticQuery\nin indices '$indices'",
            index = Some(indices),
            operation = Some("search")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute search for query \n${sql
            .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("search"),
            index = Some(elasticQuery.indices.mkString(","))
          )
        )
    }

  }

  /** Multi-search with Elasticsearch queries.
    *
    * @param elasticQueries
    *   Elasticsearch queries
    * @param fieldAliases
    *   field aliases
    * @param aggregations
    *   SQL aggregations
    * @return
    *   the combined Elasticsearch response
    */
  def multiSearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] = {
    elasticQueries.queries.flatMap { elasticQuery =>
      validateJson("search", elasticQuery.query).map(error =>
        elasticQuery.indices.mkString(",") -> error.message
      )
    } match {
      case Nil => // continue
      case errors =>
        return ElasticResult.failure(
          ElasticError(
            message = s"Invalid queries: ${errors.map(_._2).mkString(",")}",
            statusCode = Some(400),
            index = Some(errors.map(_._1).mkString(",")),
            operation = Some("multiSearch")
          )
        )
    }

    val query = elasticQueries.queries.map(_.query).mkString("\n")
    val sql = elasticQueries.sql.orElse(
      Option(elasticQueries.queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
    )

    logger.debug(
      s"🔍 Multi-searching with query \n$elasticQueries"
    )

    executeMultiSearch(elasticQueries) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed multi-search for query \n$elasticQueries"
        )
        val aggs = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
        ElasticResult.fromTry(parseResponse(response, fieldAliases, aggs)) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed multi-search results for query '$elasticQueries'"
            )
            ElasticResult.success(
              ElasticResponse(
                sql,
                query,
                success.value,
                fieldAliases,
                aggs
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse multi-search results for query \n$elasticQueries\n -> ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("multiSearch")
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute multi-search for query \n$elasticQueries",
            operation = Some("multiSearch")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute multi-search for query \n$elasticQueries\n -> ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("multiSearch")
          )
        )
    }
  }

  // ========================================================================
  // ASYNCHRONOUS SEARCH METHODS
  // ========================================================================

  /** Asynchronous search for documents / aggregations matching the SQL query.
    *
    * @param sqlQuery
    *   the SQL query
    * @return
    *   a Future containing the Elasticsearch response
    */
  def searchAsync(
    sqlQuery: SQLQuery
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    sqlQuery.request match {
      case Some(Left(single)) =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*)
        )
        singleSearchAsync(elasticQuery, single.fieldAliases, single.sqlAggregations)

      case Some(Right(multiple)) =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multiSearchAsync(elasticQueries, multiple.fieldAliases, multiple.sqlAggregations)

      case None =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${sqlQuery.query}'"
        )
        Future.successful(
          ElasticResult.failure(
            ElasticError(
              message = s"SQL query does not contain a valid search request: ${sqlQuery.query}",
              operation = Some("searchAsync")
            )
          )
        )
    }
  }

  /** Asynchronous search for documents / aggregations matching the Elasticsearch query.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   a Future containing the Elasticsearch response
    */
  def singleSearchAsync(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val sql = elasticQuery.sql
    val query = elasticQuery.query
    val indices = elasticQuery.indices.mkString(",")
    executeSingleSearchAsync(elasticQuery).flatMap {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query \n$elasticQuery\nin indices '$indices'"
        )
        val aggs = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
        ElasticResult.fromTry(parseResponse(response, fieldAliases, aggs)) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed search results for query \n$elasticQuery\nin indices '$indices'"
            )
            Future.successful(
              ElasticResult.success(
                ElasticResponse(
                  sql,
                  query,
                  success.value,
                  fieldAliases,
                  aggs
                )
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse search results for query \n${sql
                .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
            )
            Future.successful(
              ElasticResult.failure(
                error.copy(
                  operation = Some("searchAsync"),
                  index = Some(indices)
                )
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute asynchronous search for query \n$elasticQuery\nin indices '$indices'",
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("searchAsync")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticResult.failure(error))
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query \n${sql
            .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
        )
        Future.successful(
          ElasticResult.failure(
            error.copy(
              operation = Some("searchAsync"),
              index = Some(elasticQuery.indices.mkString(","))
            )
          )
        )
    }
  }

  /** Asynchronous multi-search with Elasticsearch queries.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   a Future containing the combined Elasticsearch response
    */
  def multiSearchAsync(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val query = elasticQueries.queries.map(_.query).mkString("\n")
    val sql = elasticQueries.sql.orElse(
      Option(elasticQueries.queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
    )

    executeMultiSearchAsync(elasticQueries).flatMap {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed asynchronous multi-search for query \n$elasticQueries"
        )
        val aggs = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
        ElasticResult.fromTry(parseResponse(response, fieldAliases, aggs)) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed multi-search results for query '$elasticQueries'"
            )
            Future.successful(
              ElasticResult.success(
                ElasticResponse(
                  sql,
                  query,
                  success.value,
                  fieldAliases,
                  aggs
                )
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse multi-search results for query \n$elasticQueries\n -> ${error.message}"
            )
            Future.successful(
              ElasticResult.failure(
                error.copy(
                  operation = Some("multiSearchAsync")
                )
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute asynchronous multi-search for query \n$elasticQueries",
            operation = Some("multiSearchAsync")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticResult.failure(error))
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous multi-search for query \n$elasticQueries\n -> ${error.message}"
        )
        Future.successful(
          ElasticResult.failure(
            error.copy(
              operation = Some("multiSearchAsync")
            )
          )
        )
    }
  }

  // ========================================================================
  // SEARCH METHODS WITH CONVERSION
  // ========================================================================

  /** Searches and converts results into typed entities from an SQL query.
    *
    * @note
    *   This method uses compile-time macros to validate the SQL query against the type U.
    *
    * @param query
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def searchAs[U](
    query: String
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] =
    macro SQLQueryMacros.searchAsImpl[U]

  /** Searches and converts results into typed entities from an SQL query.
    *
    * @note
    *   This method is a variant of searchAs without compile-time SQL validation.
    *
    * @param sqlQuery
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def searchAsUnchecked[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    for {
      response <- search(sqlQuery)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  /** Searches and converts results into typed entities.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def singleSearchAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    formats: Formats
  ): ElasticResult[Seq[U]] = {
    for {
      response <- singleSearch(elasticQuery, fieldAliases, aggregations)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  /** Multi-search with conversion to typed entities.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the queries
    */
  def multisearchAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    for {
      response <- multiSearch(elasticQueries, fieldAliases, aggregations)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  // ========================================================================
  // ASYNCHRONOUS SEARCH METHODS WITH CONVERSION
  // ========================================================================

  /** Asynchronous search with conversion to typed entities.
    *
    * @note
    *   This method uses compile-time macros to validate the SQL query against the type U.
    *
    * @param query
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def searchAsyncAs[U](
    query: String
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] =
    macro SQLQueryMacros.searchAsyncAsImpl[U]

  /** Asynchronous search with conversion to typed entities.
    *
    * @note
    *   This method is a variant of searchAsyncAs without compile-time SQL validation.
    *
    * @param sqlQuery
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def searchAsyncAsUnchecked[U](
    sqlQuery: SQLQuery
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    searchAsync(sqlQuery).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${sqlQuery.query}': ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query '${sqlQuery.query}'"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  /** Asynchronous search with conversion to typed entities.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def singleSearchAsyncAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    singleSearchAsync(elasticQuery, fieldAliases, aggregations).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${elasticQuery.query}': ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query '${elasticQuery.query}'"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  /** Asynchronous multi-search with conversion to typed entities.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def multiSearchAsyncAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    multiSearchAsync(elasticQueries, fieldAliases, aggregations).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous multi-search with ${elasticQueries.queries.size} queries: ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous multi-search with ${elasticQueries.queries.size} queries"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  // ========================================================================
  // SEARCH METHODS WITH INNER HITS
  // ========================================================================

  @deprecated("Use `search` instead.", "v0.10")
  /** Search with inner hits from an SQL query.
    *
    * @deprecated
    *   Use `search` instead.
    * @param sql
    *   the SQL query
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   tuples (main entity, inner hits)
    */
  def searchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    sql: SQLQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    sql.request match {
      case Some(Left(single)) =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*)
        )
        singleSearchWithInnerHits[U, I](elasticQuery, innerField)

      case Some(Right(multiple)) =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multisearchWithInnerHits[U, I](elasticQueries, innerField)

      case None =>
        logger.error(
          s"❌ Failed to execute search with inner hits for query '${sql.query}'"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"SQL query does not contain a valid search request: ${sql.query}",
            operation = Some("searchWithInnerHits")
          )
        )
    }
  }

  @deprecated("Use `search` instead.", "v0.10")
  /** Search with inner hits from an Elasticsearch query.
    *
    * @deprecated
    *   Use `search` instead.
    * @param elasticQuery
    *   the Elasticsearch query
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   tuples (main entity, inner hits)
    */
  def singleSearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQuery: ElasticQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    validateJson("search", elasticQuery.query) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid query: ${error.message}",
            statusCode = Some(400),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("singleSearchWithInnerHits")
          )
        )
      case None => // continue
    }

    logger.debug(
      s"🔍 Searching inner hits with query '${elasticQuery.query}' in indices '${elasticQuery.indices
        .mkString(",")}'"
    )

    executeSingleSearch(elasticQuery) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed search with inner hits in indices '${elasticQuery.indices.mkString(",")}'"
        )
        ElasticResult.attempt {
          JsonParser.parseString(response).getAsJsonObject
        } match {
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse Elasticsearch response for search with inner hits in indices '${elasticQuery.indices
                .mkString(",")}': ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("singleSearchWithInnerHits"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
          case ElasticSuccess(parsedResponse) =>
            ElasticResult.attempt(parseInnerHits[U, I](parsedResponse, innerField))
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute search with inner hits in indices '${elasticQuery.indices.mkString(",")}'",
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("singleSearchWithInnerHits")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute search with inner hits in indices '${elasticQuery.indices
            .mkString(",")}': ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("singleSearchWithInnerHits"),
            index = Some(elasticQuery.indices.mkString(","))
          )
        )
    }
  }

  @deprecated("Use `multisearch` instead.", "v0.10")
  /** Multisearch with inner hits from Elasticsearch queries.
    *
    * @deprecated
    *   Use `multisearch` instead.
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   a sequence of results with inner hits
    */
  def multisearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQueries: ElasticQueries,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    elasticQueries.queries.flatMap { elasticQuery =>
      validateJson("search", elasticQuery.query).map(error =>
        elasticQuery.indices.mkString(",") -> error.message
      )
    } match {
      case Nil => // continue
      case errors =>
        return ElasticResult.failure(
          ElasticError(
            message = s"Invalid queries: ${errors.map(_._2).mkString(",")}",
            statusCode = Some(400),
            index = Some(errors.map(_._1).mkString(",")),
            operation = Some("multisearchWithInnerHits")
          )
        )
    }

    logger.debug(
      s"🔍 Multi-searching inner hits with ${elasticQueries.queries.size} queries"
    )

    executeMultiSearch(elasticQueries) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed multi-search inner hits with ${elasticQueries.queries.size} queries"
        )
        ElasticResult.attempt {
          JsonParser.parseString(response).getAsJsonObject
        } match {
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse Elasticsearch response for multi-search inner hits with ${elasticQueries.queries.size} queries: ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("multisearchWithInnerHits")
              )
            )
          case ElasticSuccess(parsedResponse) =>
            ElasticResult.attempt(parseInnerHits[U, I](parsedResponse, innerField))
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute multi-search inner hits with ${elasticQueries.queries.size} queries",
            operation = Some("multisearchWithInnerHits")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute multi-search inner hits with ${elasticQueries.queries.size} queries: ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("multisearchWithInnerHits")
          )
        )
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]]

  private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]]

  private[client] def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]]

  private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]]

  // ================================================================================
  // IMPLICIT CONVERSIONS
  // ================================================================================

  /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query serialization.
    *
    * @param sqlSearch
    *   the SQL search request to convert
    * @return
    *   JSON string representation of the query
    */
  private[client] implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String

  private def parseInnerHits[M: Manifest: ClassTag, I: Manifest: ClassTag](
    searchResult: JsonObject,
    innerField: String
  )(implicit formats: Formats): Seq[(M, Seq[I])] = {
    val mManifest = implicitly[Manifest[M]]
    val iManifest = implicitly[Manifest[I]]
    val mClass = classTag[M].runtimeClass
    val iClass = classTag[I].runtimeClass

    logger.info(
      s"🔍 Processing inner hits with types: M=${mClass.getSimpleName}, I=${iClass.getSimpleName}"
    )

    def innerHits(result: JsonElement) = {
      result.getAsJsonObject
        .get("inner_hits")
        .getAsJsonObject
        .get(innerField)
        .getAsJsonObject
        .get("hits")
        .getAsJsonObject
        .get("hits")
        .getAsJsonArray
        .iterator()
    }

    val gson = new Gson()
    val results = searchResult.get("hits").getAsJsonObject.get("hits").getAsJsonArray.iterator()

    (for (result <- results.asScala)
      yield (
        result match {
          case obj: JsonObject =>
            Try {
              val source = gson.toJson(obj.get("_source"))
              logger.debug(
                s"Deserializing main entity ${mClass.getSimpleName} from source: $source"
              )
              serialization.read[M](source)(formats, mManifest)
            } match {
              case Success(s) => s
              case Failure(f) =>
                logger.error(s"❌ Failed to deserialize main entity: ${f.getMessage}", f)
                throw f
            }
          case _ => serialization.read[M](result.getAsString)(formats, mManifest)
        },
        (for (innerHit <- innerHits(result).asScala) yield innerHit match {
          case obj: JsonObject =>
            Try {
              val source = gson.toJson(obj.get("_source"))
              logger.debug(
                s"Deserializing inner hit entity ${iClass.getSimpleName} from source: $source"
              )
              serialization.read[I](source)(formats, iManifest)
            } match {
              case Success(s) => s
              case Failure(f) =>
                logger.error(s"❌ Failed to deserialize inner hit entity: ${f.getMessage}")
                throw f
            }
          case _ => serialization.read[I](innerHit.getAsString)(formats, iManifest)
        }).toList
      )).toList
  }

  // ========================================================================
  // PRIVATE HELPERS
  // ========================================================================

  /** Converts an Elasticsearch response to typed entities.
    *
    * @param response
    *   the Elasticsearch response
    * @tparam U
    *   the type of entities to convert to
    * @return
    *   ElasticResult containing the entities or an error
    */
  private def convertToEntities[U](
    response: ElasticResponse
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    val results = ElasticResult.fromTry(convertTo[U](response))
    results
      .fold(
        onFailure = error => {
          logger.error(
            s"❌ Conversion to entities failed: ${error.message} with query \n${response.query}\n and results:\n ${response.results}"
          )
          ElasticResult.failure(
            ElasticError(
              message = s"Failed to convert search results to ${m.runtimeClass.getSimpleName}",
              cause = error.cause,
              operation = Some("convertToEntities")
            )
          )
        },
        onSuccess = entities => ElasticResult.success(entities)
      )
  }

  // ========================================================================
  // WINDOW FUNCTION SEARCH
  // ========================================================================

  /** Search with window function enrichment
    *
    * Strategy:
    *   1. Execute aggregation query to compute window values 2. Execute main query (without window
    *      functions) 3. Enrich results with window values
    */
  private def searchWithWindowEnrichment(
    sql: SQLQuery,
    request: SQLSearchRequest
  ): ElasticResult[ElasticResponse] = {

    logger.info(s"🪟 Detected ${request.windowFunctions.size} window functions")

    for {
      // Step 1: Execute window aggregations
      windowCache <- executeWindowAggregations(request)

      // Step 2: Execute base query (without window functions)
      baseResponse <- executeBaseQuery(sql, request)

      // Step 3: Enrich results
      enrichedResponse <- enrichResponseWithWindowValues(baseResponse, windowCache, request)

    } yield enrichedResponse
  }

  // ========================================================================
  // WINDOW AGGREGATION EXECUTION
  // ========================================================================

  /** Execute aggregation queries for all window functions Returns a cache of partition key ->
    * window values
    */
  protected def executeWindowAggregations(
    request: SQLSearchRequest
  ): ElasticResult[WindowCache] = {

    // Build aggregation request
    val aggRequest = buildWindowAggregationRequest(request)
    val sql = aggRequest.sql

    logger.info(
      s"🔍 Executing window aggregation query:\n$sql"
    )

    // Execute aggregation using existing search infrastructure
    val elasticQuery = ElasticQuery(
      aggRequest,
      collection.immutable.Seq(aggRequest.sources: _*),
      sql = Some(sql)
    )

    for {
      // Use singleSearch to execute aggregation
      aggResponse <- singleSearch(
        elasticQuery,
        aggRequest.fieldAliases,
        aggRequest.sqlAggregations
      )

      // Parse aggregation results into cache
      cache <- parseWindowAggregationsToCache(aggResponse, request)

    } yield cache
  }

  /** Build aggregation request for window functions
    */
  private def buildWindowAggregationRequest(
    request: SQLSearchRequest
  ): SQLSearchRequest = {

    // Create modified request with:
    // - Only window buckets in GROUP BY
    // - Only window aggregations in SELECT
    // - No LIMIT (need all partitions)
    // - Same WHERE clause (to match base query filtering)
    request
      .copy(
        select = request.select.copy(fields = request.windowFields),
        groupBy = None, //request.groupBy.map(_.copy(buckets = request.windowBuckets)),
        orderBy = None, // Not needed for aggregations
        limit = None // Need all buckets
      )
      .update()
  }

  /** Parse aggregation response into window cache Uses your existing
    * ElasticConversion.parseResponse
    */
  private def parseWindowAggregationsToCache(
    response: ElasticResponse,
    request: SQLSearchRequest
  ): ElasticResult[WindowCache] = {

    logger.info(
      s"🔍 Parsing window aggregations to cache for query \n${response.sql.getOrElse(response.query)}"
    )

    val aggRows = response.results

    logger.info(s"✅ Parsed ${aggRows.size} aggregation buckets")

    // Build cache: partition key -> window values
    val cache = aggRows.map { row =>
      val partitionKey = extractPartitionKey(row, request)
      val windowValues = extractWindowValues(row, response.aggregations)

      partitionKey -> windowValues
    }.toMap

    ElasticResult.success(WindowCache(cache))
  }

  // ========================================================================
  // BASE QUERY EXECUTION
  // ========================================================================

  /** Execute base query without window functions
    */
  private def executeBaseQuery(
    sql: SQLQuery,
    request: SQLSearchRequest
  ): ElasticResult[ElasticResponse] = {

    val baseQuery = createBaseQuery(sql, request)

    logger.info(s"🔍 Executing base query without window functions ${baseQuery.sql}")

    singleSearch(
      ElasticQuery(
        baseQuery,
        collection.immutable.Seq(baseQuery.sources: _*),
        sql = Some(baseQuery.sql)
      ),
      baseQuery.fieldAliases,
      baseQuery.sqlAggregations
    )
  }

  /** Create base query by removing window functions from SELECT
    */
  protected def createBaseQuery(
    sql: SQLQuery,
    request: SQLSearchRequest
  ): SQLSearchRequest = {

    // Remove window function fields from SELECT
    val baseFields = request.select.fields.filterNot(_.identifier.hasWindow)

    // Create modified request
    val baseRequest = request
      .copy(
        select = request.select.copy(fields = baseFields)
      )
      .copy(score = sql.score)
      .update()

    baseRequest
  }

  /** Extract partition key from aggregation row
    */
  private def extractPartitionKey(
    row: Map[String, Any],
    request: SQLSearchRequest
  ): PartitionKey = {

    // Get all partition fields from window functions
    val partitionFields = request.windowFunctions
      .flatMap(_.partitionBy)
      .map(_.aliasOrName)
      .distinct

    if (partitionFields.isEmpty) {
      return PartitionKey(Map("__global__" -> true))
    }

    val keyValues = partitionFields.flatMap { field =>
      row.get(field).map(field -> _)
    }.toMap

    PartitionKey(keyValues)
  }

  /** Extract window function values from aggregation row
    */
  private def extractWindowValues(
    row: Map[String, Any],
    aggregations: Map[String, ClientAggregation]
  ): WindowValues = {

    val values = extractAggregationValues(row, aggregations)

    WindowValues(values)
  }

  // ========================================================================
  // RESULT ENRICHMENT
  // ========================================================================

  /** Enrich response with window values
    */
  private def enrichResponseWithWindowValues(
    response: ElasticResponse,
    cache: WindowCache,
    request: SQLSearchRequest
  ): ElasticResult[ElasticResponse] = {

    val baseRows = response.results
    // Enrich each row
    val enrichedRows = baseRows.map { row =>
      enrichDocumentWithWindowValues(row, cache, request)
    }

    ElasticResult.success(response.copy(results = enrichedRows))
  }

  /** Enrich a single document with window values
    */
  protected def enrichDocumentWithWindowValues(
    doc: Map[String, Any],
    cache: WindowCache,
    request: SQLSearchRequest
  ): Map[String, Any] = {

    if (request.windowFunctions.isEmpty) {
      return doc
    }

    // Build partition key from document
    val partitionKey = extractPartitionKey(doc, request)

    // Lookup window values
    cache.get(partitionKey) match {
      case Some(windowValues) =>
        // Merge document with window values
        doc ++ windowValues.values

      case None =>
        logger.warn(s"⚠️ No window values found for partition: ${partitionKey.values}")

        // Add null values for missing window functions
        val nullValues = request.windowFunctions.map { wf =>
          wf.identifier.aliasOrName -> null
        }.toMap

        doc ++ nullValues
    }
  }

  // ========================================================================
  // HELPER CASE CLASSES
  // ========================================================================

  /** Partition key for window function cache
    */
  protected case class PartitionKey(values: Map[String, Any]) {
    override def hashCode(): Int = values.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case other: PartitionKey => values == other.values
      case _                   => false
    }
  }

  /** Window function values for a partition
    */
  protected case class WindowValues(values: Map[String, Any])

  /** Cache of partition key -> window values
    */
  protected case class WindowCache(cache: Map[PartitionKey, WindowValues]) {
    def get(key: PartitionKey): Option[WindowValues] = cache.get(key)
    def size: Int = cache.size
  }
}
