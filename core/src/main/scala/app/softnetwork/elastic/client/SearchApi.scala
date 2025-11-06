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
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import com.google.gson.{Gson, JsonElement, JsonObject, JsonParser}
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
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
          collection.immutable.Seq(single.sources: _*)
        )
        singleSearch(elasticQuery, single.fieldAliases, single.sqlAggregations)

      case Some(Right(multiple)) =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multiSearch(elasticQueries, multiple.fieldAliases, multiple.sqlAggregations)

      case None =>
        logger.error(
          s"❌ Failed to execute search for query '${sql.query}'"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"SQL query does not contain a valid search request: ${sql.query}",
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

    logger.debug(
      s"Searching with query '${elasticQuery.query}' in indices '${elasticQuery.indices.mkString(",")}'"
    )

    executeSingleSearch(elasticQuery) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed search in indices '${elasticQuery.indices.mkString(",")}'"
        )
        ElasticResult.success(
          ElasticResponse(
            elasticQuery.query,
            response,
            fieldAliases,
            aggregations.map(kv => kv._1 -> kv._2)
          )
        )
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute search in indices '${elasticQuery.indices.mkString(",")}'",
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("search")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute search in indices '${elasticQuery.indices.mkString(",")}': ${error.message}"
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

    logger.debug(
      s"Multi-searching with ${elasticQueries.queries.size} queries"
    )

    executeMultiSearch(elasticQueries) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed multi-search with ${elasticQueries.queries.size} queries"
        )
        ElasticResult.success(
          ElasticResponse(
            elasticQueries.queries.map(_.query).mkString("\n"),
            response,
            fieldAliases,
            aggregations.map(kv => kv._1 -> kv._2)
          )
        )
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute multi-search with ${elasticQueries.queries.size} queries",
            operation = Some("multiSearch")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute multi-search with ${elasticQueries.queries.size} queries: ${error.message}"
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
    executeSingleSearchAsync(elasticQuery).flatMap {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query '${elasticQuery.query}'"
        )
        Future.successful(
          ElasticResult.success(
            ElasticResponse(
              elasticQuery.query,
              response,
              fieldAliases,
              aggregations.map(kv => kv._1 -> kv._2)
            )
          )
        )
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute asynchronous search for query '${elasticQuery.query}'",
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("searchAsync")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticResult.failure(error))
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${elasticQuery.query}': ${error.message}"
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
    executeMultiSearchAsync(elasticQueries).flatMap {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed asynchronous multi-search with ${elasticQueries.queries.size} queries"
        )
        Future.successful(
          ElasticResult.success(
            ElasticResponse(
              elasticQueries.queries.map(_.query).mkString("\n"),
              response,
              fieldAliases,
              aggregations.map(kv => kv._1 -> kv._2)
            )
          )
        )
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute asynchronous multi-search with ${elasticQueries.queries.size} queries",
            operation = Some("multiSearchAsync")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticResult.failure(error))
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous multi-search with ${elasticQueries.queries.size} queries: ${error.message}"
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
    * @param sqlQuery
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def searchAs[U](
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
    * @param sqlQuery
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def searchAsyncAs[U](
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
    * @param sqlQuery
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
          new JsonParser().parse(response).getAsJsonObject
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
          new JsonParser().parse(response).getAsJsonObject
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
        onFailure = error =>
          ElasticResult.failure(
            ElasticError(
              message = s"Failed to convert search results to ${m.runtimeClass.getSimpleName}",
              cause = error.cause,
              operation = Some("convertToEntities")
            )
          ),
        onSuccess = entities => ElasticResult.success(entities)
      )
  }

}
