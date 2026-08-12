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

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.function.aggregate.{PercentileAgg, RankingWindow}
import app.softnetwork.elastic.sql.macros.SQLQueryMacros
import app.softnetwork.elastic.sql.query.{
  Limit,
  MultiSearch,
  SQLAggregation,
  SearchStatement,
  SelectStatement,
  SingleSearch
}
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.json4s.Formats

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
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

  /** Extract output field names from a SingleSearch in SQL SELECT order. For each field, uses the
    * alias if present, otherwise the source field name. Returns empty Seq for SELECT * queries.
    */
  protected def extractOutputFieldNames(single: SingleSearch): Seq[String] = {
    val fields = single.select.fieldsWithComputedAliases
    if (fields.size == 1 && fields.head.identifier.identifierName == "*") Seq.empty
    else fields.map(f => f.fieldAlias.map(_.alias).getOrElse(f.sourceField))
  }

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Search for documents / aggregations matching the SQL query.
    *
    * @param statement
    *   the SQL query to execute
    * @return
    *   the Elasticsearch response
    */
  def search(
    statement: SearchStatement
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    val query = statement.sql
    statement match {
      case select: SelectStatement =>
        select.statement match {
          case Some(statement: SingleSearch) =>
            search(statement.copy(score = select.score))
          case Some(statement: MultiSearch) =>
            search(statement)
          case None =>
            logger.error(
              s"❌ Failed to execute search for query \n${statement.sql}"
            )
            ElasticResult.failure(
              ElasticError(
                message = s"SQL query does not contain a valid search request\n$query",
                operation = Some("search")
              )
            )
        }
      case single: SingleSearch =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*),
          sql = Some(query),
          explodeNested = single.explodeNested
        )
        this match {
          case scrollApi: ScrollApi if single.returnsRows && requiresScrollPaging(single.limit) =>
            // A row query is data-bound, not time-bound: every page request below
            // carries its own timeout, so the stream always terminates.
            Await.result(
              scrollRows(scrollApi, single, elasticQuery),
              Duration.Inf
            )
          case _ =>
            if (single.windowRowQuery)
              searchWithWindowEnrichment(single)
            else
              singleSearch(
                elasticQuery,
                single.fieldAliases,
                single.sqlAggregations,
                extractOutputFieldNames(single),
                single.nestedHitsMappings
              )
        }

      case multiple: MultiSearch =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*),
              explodeNested = multiple.explodeNested
            )
          }.toList,
          sql = Some(query),
          explodeNested = multiple.explodeNested
        )
        multiSearch(
          elasticQueries,
          multiple.fieldAliases,
          multiple.sqlAggregations,
          multiple.requests.headOption.map(extractOutputFieldNames).getOrElse(Seq.empty),
          multiple.requests.headOption.map(_.nestedHitsMappings).getOrElse(Map.empty)
        )

      case _ =>
        logger.error(
          s"❌ Failed to execute search for query \n${statement.sql}"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"SQL query does not contain a valid search request\n$query",
            operation = Some("search")
          )
        )
    }
  }

  /** Convert SELECT aggregations to ClientAggregations, applying percentile coalescing: columns
    * sharing a value column / `cont` flag / partition become delegates of the first (owner) column
    * and read their value from the owner's shared ES `percentiles` response node. Mirrors the
    * bridge's percent-merge (both call [[PercentileAgg.coalescePlan]] on the same SELECT-ordered
    * items, so they always pick the same owner).
    */
  private def toClientAggregations(
    aggregations: ListMap[String, SQLAggregation]
  ): ListMap[String, ClientAggregation] = {
    val aggs0 = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
    val percentileItems = aggregations.toSeq.collect {
      case (name, sa) if sa.aggType.isInstanceOf[PercentileAgg] =>
        name -> sa.aggType.asInstanceOf[PercentileAgg]
    }
    if (percentileItems.size < 2) aggs0
    else {
      val plan = PercentileAgg.coalescePlan(percentileItems)
      aggs0.map { case (name, ca) =>
        name -> (if (plan.isDelegate(name)) ca.copy(sourceAgg = plan.ownerOf.get(name)) else ca)
      }
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] =
    singleSearchInternal(elasticQuery, fieldAliases, aggregations, fields, nestedHits)

  /** [[singleSearch]] with an explicit document-id retention decision. `retainDocumentId = true` is
    * reserved for the window-enrichment base query, which matches rows to their ranking ordinals by
    * document id AFTER parsing (the id is stripped again after enrichment).
    */
  private[client] def singleSearchInternal(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    retainDocumentId: Boolean = false
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
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
        val aggs = toClientAggregations(aggregations)
        ElasticResult.fromTry(
          parseResponseTree(
            response,
            fieldAliases,
            aggs,
            fields,
            nestedHits,
            elasticQuery.explodeNested,
            retainDocumentId
          )
        ) match {
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
          enrichMaxResultWindowError(error).copy(
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
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
        val aggs = toClientAggregations(aggregations)
        ElasticResult.fromTry(
          parseResponseTree(
            response,
            fieldAliases,
            aggs,
            fields,
            nestedHits,
            elasticQueries.explodeNested
          )
        ) match {
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
          enrichMaxResultWindowError(error).copy(
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
    statement: SearchStatement
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    statement match {
      case select: SelectStatement =>
        select.statement match {
          case Some(statement: SingleSearch) =>
            searchAsync(statement.copy(score = select.score))
          case Some(statement: MultiSearch) =>
            searchAsync(statement)
          case None =>
            logger.error(
              s"❌ Failed to execute asynchronous search for query '${statement.sql}'"
            )
            Future.successful(
              ElasticResult.failure(
                ElasticError(
                  message = s"SQL query does not contain a valid search request: ${statement.sql}",
                  operation = Some("searchAsync")
                )
              )
            )
        }

      case single: SingleSearch =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*)
        )
        this match {
          case scrollApi: ScrollApi if single.returnsRows && requiresScrollPaging(single.limit) =>
            scrollRows(scrollApi, single, elasticQuery)
          case _ =>
            if (single.windowRowQuery)
              Future.successful(searchWithWindowEnrichment(single))
            else
              singleSearchAsync(
                elasticQuery,
                single.fieldAliases,
                single.sqlAggregations,
                extractOutputFieldNames(single),
                single.nestedHitsMappings
              )
        }

      case multiple: MultiSearch =>
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multiSearchAsync(
          elasticQueries,
          multiple.fieldAliases,
          multiple.sqlAggregations,
          multiple.requests.headOption.map(extractOutputFieldNames).getOrElse(Seq.empty),
          multiple.requests.headOption.map(_.nestedHitsMappings).getOrElse(Map.empty)
        )

      case _ =>
        val query = statement.sql
        logger.error(
          s"❌ Failed to execute asynchronous search for query '$query'"
        )
        Future.successful(
          ElasticResult.failure(
            ElasticError(
              message = s"SQL query does not contain a valid search request: $query",
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val sql = elasticQuery.sql
    val query = elasticQuery.query
    val indices = elasticQuery.indices.mkString(",")
    executeSingleSearchAsync(elasticQuery)
      .flatMap {
        case ElasticSuccess(Some(response)) =>
          logger.info(
            s"✅ Successfully executed asynchronous search for query \n$elasticQuery\nin indices '$indices'"
          )
          val aggs = toClientAggregations(aggregations)
          ElasticResult.fromTry(
            parseResponseTree(
              response,
              fieldAliases,
              aggs,
              fields,
              nestedHits,
              elasticQuery.explodeNested
            )
          ) match {
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
              enrichMaxResultWindowError(error).copy(
                operation = Some("searchAsync"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
          )
      }
      .recover {
        // Issue #224 — some client implementations surface an execution failure as a FAILED future
        // rather than an ElasticFailure; without this recover the raw Throwable propagates to the
        // consumer, which typically flattens it into an opaque generic error. Honor the
        // ElasticResult contract here (and translate a max_result_window rejection on the way).
        case t: Throwable =>
          logger.error(
            s"❌ Failed to execute asynchronous search for query \n${sql
              .getOrElse(query)}\nin indices '$indices' -> ${t.getMessage}"
          )
          ElasticResult.failure(
            enrichMaxResultWindowError(
              ElasticError(
                message = s"Failed to execute search: ${t.getMessage}",
                cause = Some(t),
                operation = Some("searchAsync"),
                index = Some(indices)
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val query = elasticQueries.queries.map(_.query).mkString("\n")
    val sql = elasticQueries.sql.orElse(
      Option(elasticQueries.queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
    )

    executeMultiSearchAsync(elasticQueries)
      .flatMap {
        case ElasticSuccess(Some(response)) =>
          logger.info(
            s"✅ Successfully executed asynchronous multi-search for query \n$elasticQueries"
          )
          val aggs = toClientAggregations(aggregations)
          ElasticResult.fromTry(
            parseResponseTree(
              response,
              fieldAliases,
              aggs,
              fields,
              nestedHits,
              elasticQueries.explodeNested
            )
          ) match {
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
              enrichMaxResultWindowError(error).copy(
                operation = Some("multiSearchAsync")
              )
            )
          )
      }
      .recover {
        // Issue #224 — same contract repair as singleSearchAsync: a client implementation may fail
        // the future instead of returning an ElasticFailure.
        case t: Throwable =>
          logger.error(
            s"❌ Failed to execute asynchronous multi-search for query \n$elasticQueries\n -> ${t.getMessage}"
          )
          ElasticResult.failure(
            enrichMaxResultWindowError(
              ElasticError(
                message = s"Failed to execute multi-search: ${t.getMessage}",
                cause = Some(t),
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
    sqlQuery: SelectStatement
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
    for {
      response <- search(sqlQuery.withoutNestedExplosion)
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    formats: Formats
  ): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
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
    sqlQuery: SelectStatement
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
    searchAsync(sqlQuery.withoutNestedExplosion).flatMap {
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
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
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
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
    sql: SelectStatement,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    sql.statement match {
      case Some(single: SingleSearch) =>
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*)
        )
        singleSearchWithInnerHits[U, I](elasticQuery, innerField)

      case Some(multiple: MultiSearch) =>
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
        ElasticResult.attempt(parseInnerHits[U, I](response, innerField)) match {
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
          case success => success
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
        ElasticResult.attempt(parseInnerHits[U, I](response, innerField)) match {
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse Elasticsearch response for multi-search inner hits with ${elasticQueries.queries.size} queries: ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("multisearchWithInnerHits")
              )
            )
          case success => success
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

  /** Execute the search and hand back the response as an already-parsed Jackson tree (#228).
    *
    * The tree contract kills the historical double parse: implementations must materialize the
    * Elasticsearch response as the Jackson tree core consumes — parsing the raw response bytes
    * exactly once, or re-parenting `_source` trees the transport already parsed — never by
    * serializing a typed response back to a JSON String for core to re-parse.
    */
  private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[JsonNode]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[JsonNode]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[JsonNode]]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[JsonNode]]]

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
  private[client] implicit def singleSearchToJsonQuery(sqlSearch: SingleSearch)(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String

  private def parseInnerHits[M: Manifest: ClassTag, I: Manifest: ClassTag](
    searchResult: JsonNode,
    innerField: String
  )(implicit formats: Formats): Seq[(M, Seq[I])] = {
    val mManifest = implicitly[Manifest[M]]
    val iManifest = implicitly[Manifest[I]]
    val mClass = classTag[M].runtimeClass
    val iClass = classTag[I].runtimeClass

    logger.info(
      s"🔍 Processing inner hits with types: M=${mClass.getSimpleName}, I=${iClass.getSimpleName}"
    )

    def innerHits(result: JsonNode): Iterator[JsonNode] = {
      val hits = result
        .path("inner_hits")
        .path(innerField)
        .path("hits")
        .path("hits")
      if (!hits.isArray) {
        throw new IllegalStateException(
          s"No inner hits found for field '$innerField' in search response"
        )
      }
      hits.elements().asScala
    }

    val hits = searchResult.path("hits").path("hits")
    if (!hits.isArray) {
      throw new IllegalStateException("No hits found in search response")
    }

    (for (result <- hits.elements().asScala)
      yield (
        result match {
          case obj if obj.isObject =>
            Try {
              val source = mapper.writeValueAsString(obj.get("_source"))
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
          case other => serialization.read[M](other.asText())(formats, mManifest)
        },
        (for (innerHit <- innerHits(result)) yield innerHit match {
          case obj if obj.isObject =>
            Try {
              val source = mapper.writeValueAsString(obj.get("_source"))
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
          case other => serialization.read[I](other.asText())(formats, iManifest)
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
    * {{{
    * Strategy:
    *   1. Execute aggregation query to compute window values
    *   2. Execute main query (without window functions)
    *   3. Enrich results with window values
    * }}}
    */
  private def searchWithWindowEnrichment(
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[ElasticResponse] = {

    logger.info(s"🪟 Detected ${request.windowFunctions.size} window functions")

    for {
      // Step 1: Execute window aggregations
      windowCache <- executeWindowAggregations(request)

      // Step 2: Execute base query (without window functions)
      baseResponse <- executeBaseQuery(request)

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
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[WindowCache] = {

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
        aggRequest.sqlAggregations,
        extractOutputFieldNames(aggRequest),
        aggRequest.nestedHitsMappings
      )

      // Parse aggregation results into cache
      cache <- parseWindowAggregationsToCache(aggResponse, request)

    } yield cache
  }

  /** Build aggregation request for window functions
    */
  private def buildWindowAggregationRequest(
    request: SingleSearch
  ): SingleSearch = {

    // Create modified request with:
    // - Only window buckets in GROUP BY
    // - Only window aggregations in SELECT
    // - No LIMIT (need all partitions)
    // - Same WHERE clause (to match base query filtering)
    request
      .copy(
        select = request.select.copy(fields = request.windowFields.map(_.update(request))),
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
    request: SingleSearch
  ): ElasticResult[WindowCache] = {

    logger.info(
      s"🔍 Parsing window aggregations to cache for query \n${response.sql.getOrElse(response.query)}"
    )

    val aggRows = response.results

    logger.info(s"✅ Parsed ${aggRows.size} aggregation buckets")

    // Ranking-style windows in the original request, paired with their
    // SELECT-field alias (which is the key under which the top_hits
    // sub-aggregation surfaces in the parsed agg row). Ranking windows have
    // an empty positional identifier, so we can't recover the alias from
    // the AST alone — pull it from the field that wraps them.
    val rankingWindows: Seq[(String, RankingWindow)] =
      request.windowFields.flatMap { f =>
        f.identifier.windows.collect { case r: RankingWindow =>
          f.fieldAlias.map(_.alias).getOrElse(f.sourceField) -> r
        }
      }

    val cache = aggRows.map { row =>
      val partitionKey = extractPartitionKey(row, request)
      val windowValues = extractWindowValues(row, response.aggregations)
      val rankings = extractRankings(row, rankingWindows)
      partitionKey -> windowValues.copy(rankings = rankings)
    }

    ElasticResult.success(WindowCache(ListMap(cache: _*)))
  }

  /** Read each ranking window's top_hits results from the parsed aggregation row, compute ordinals
    * via the window's `assignOrdinals` (per its tie rule), and return a `fieldAlias -> (rowId ->
    * rank)` map per partition.
    *
    * Each `(name, rw)` pair carries the SELECT-field alias (the key under which the top_hits
    * sub-agg surfaces in `row`) and the ranking window itself.
    */
  /** Resolve an OVER ORDER BY column value from a top_hits inner-source map. The inner-source map
    * keeps nested objects un-flattened, so a dotted column (e.g. `address.salary`) is not a
    * top-level key. Fall back to walking the dotted path into nested maps. This is purely additive
    * — a flat column hits the direct lookup and behaves exactly as before.
    */
  private def resolveSortKey(h: Map[String, Any], col: String): Any =
    h.get(col) match {
      case Some(v) => v
      case None =>
        col
          .split('.')
          .foldLeft(Option[Any](h)) {
            case (Some(m: Map[_, _]), part) =>
              m.asInstanceOf[Map[String, Any]].get(part)
            case _ => None
          }
          .orNull
    }

  private def extractRankings(
    row: ListMap[String, Any],
    rankingWindows: Seq[(String, RankingWindow)]
  ): Map[String, Map[String, Long]] = {
    if (rankingWindows.isEmpty) Map.empty
    else {
      rankingWindows.flatMap { case (name, rw) =>
        val orderByCols: Seq[String] =
          rw.orderBy.toSeq.flatMap(_.sorts.map(_.field.name))
        val hits: Seq[Map[String, Any]] = row.get(name) match {
          case Some(l: List[_]) =>
            l.collect { case m: Map[_, _] =>
              m.asInstanceOf[Map[String, Any]]
            }
          case _ => Seq.empty
        }
        if (hits.isEmpty) None
        else {
          val ordered: Seq[(String, Seq[Any])] = hits.map { h =>
            val rowId = h.getOrElse("_id", "").toString
            val key = orderByCols.map(c => resolveSortKey(h, c))
            rowId -> key
          }
          Some(name -> rw.assignOrdinals(ordered).toMap)
        }
      }.toMap
    }
  }

  // ========================================================================
  // BASE QUERY EXECUTION
  // ========================================================================

  /** Execute base query without window functions
    */
  private def executeBaseQuery(
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[ElasticResponse] = {

    val baseQuery = createBaseQuery(request)

    logger.info(s"🔍 Executing base query without window functions ${baseQuery.sql}")

    // Retain `_id` on the parsed rows: the enrichment step matches each base row to its
    // ranking ordinals by document id. The id is stripped after enrichment.
    singleSearchInternal(
      ElasticQuery(
        baseQuery,
        collection.immutable.Seq(baseQuery.sources: _*),
        sql = Some(baseQuery.sql)
      ),
      baseQuery.fieldAliases,
      baseQuery.sqlAggregations,
      extractOutputFieldNames(baseQuery),
      baseQuery.nestedHitsMappings,
      retainDocumentId = true
    )
  }

  /** Create base query by removing window functions from SELECT
    */
  protected def createBaseQuery(
    request: SingleSearch
  ): SingleSearch = {

    // Remove window function fields from SELECT
    val baseFields = request.select.fields.filterNot(_.identifier.hasWindow)

    // Create modified request
    val baseRequest = request
      .copy(
        select = request.select.copy(fields = baseFields)
      )
      .update()

    baseRequest
  }

  /** Extract partition key from aggregation row
    */
  private def extractPartitionKey(
    row: ListMap[String, Any],
    request: SingleSearch
  ): PartitionKey = {

    // Get all partition fields from window functions
    val partitionFields = request.windowFunctions
      .flatMap(_.partitionBy)
      .map(_.aliasOrName)
      .distinct

    if (partitionFields.isEmpty) {
      return PartitionKey(ListMap("__global__" -> true))
    }

    val keyValues = partitionFields.flatMap { field =>
      row.get(field).map(field -> _)
    }

    PartitionKey(ListMap(keyValues: _*))
  }

  /** Extract window function values from aggregation row
    */
  private def extractWindowValues(
    row: ListMap[String, Any],
    aggregations: ListMap[String, ClientAggregation]
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
    request: SingleSearch
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {

    val baseRows = response.results
    val outputFields = extractOutputFieldNames(request)

    // Determine ONCE whether the document ID stays in the output — never per row
    val shouldKeepDocumentId = keepsDocumentId(outputFields)

    // Built once for the whole result set — never per row (#229)
    val normalizeOutputRow = rowNormalizer(outputFields)

    // Enrich each row with window values, then normalize field order. The base rows carry
    // their `_id` (see singleSearchInternal with retainDocumentId = true) for the ordinal
    // lookup — strip it on the way out unless the document-id column is enabled or `_id`
    // is selected. Only window-enriched rows ever pay this per-row strip.
    val enrichedRows = baseRows.map { row =>
      val enriched = enrichDocumentWithWindowValues(row, cache, request)
      val normalized = normalizeOutputRow(enriched)
      if (shouldKeepDocumentId) normalized
      else normalized - ElasticConversion.DocumentIdField
    }

    ElasticResult.success(response.copy(results = enrichedRows))
  }

  /** Enrich a single document with window values
    */
  protected def enrichDocumentWithWindowValues(
    doc: ListMap[String, Any],
    cache: WindowCache,
    request: SingleSearch
  ): ListMap[String, Any] = {

    if (request.windowFunctions.isEmpty) {
      return doc
    }

    // Build partition key from document
    val partitionKey = extractPartitionKey(doc, request)
    val rowId = doc.get("_id").map(_.toString).getOrElse("")

    val rankingAliases: Seq[String] =
      request.windowFields.flatMap { f =>
        f.identifier.windows.collect { case _: RankingWindow =>
          f.fieldAlias.map(_.alias).getOrElse(f.sourceField)
        }
      }

    // Lookup window values
    cache.get(partitionKey) match {
      case Some(windowValues) =>
        // Aggregation-style windows: merge the per-partition scalars.
        val withScalars = doc ++ windowValues.values

        // Ranking-style windows: look up the ordinal by row _id and inject
        // it under the SELECT-field alias. Rows that the top_hits sub-agg
        // didn't return (e.g. when the LIMIT push-down kept only top-N per
        // partition) receive null.
        if (rankingAliases.isEmpty) withScalars
        else {
          val rankEntries = rankingAliases.map { name =>
            val value = windowValues.rankings
              .get(name)
              .flatMap(_.get(rowId))
              .map(Long.box(_): Any)
              .orNull
            name -> value
          }
          withScalars ++ ListMap(rankEntries: _*)
        }

      case None =>
        logger.warn(s"⚠️ No window values found for partition: ${partitionKey.values}")

        // Add null values for missing window functions. Aggregation-style
        // windows key off their own alias/name; ranking windows have an empty
        // positional identifier, so their null must be injected under the
        // SELECT-field alias (mirrors the Some-branch).
        val aggNulls = request.windowFunctions.collect {
          case wf if !wf.isInstanceOf[RankingWindow] =>
            wf.identifier.aliasOrName -> (null: Any)
        }
        val rankingNulls = rankingAliases.map(_ -> (null: Any))

        doc ++ ListMap(aggNulls: _*) ++ ListMap(rankingNulls: _*)
    }
  }

  // ========================================================================
  // HELPER CASE CLASSES
  // ========================================================================

  /** Partition key for window function cache
    */
  protected case class PartitionKey(values: ListMap[String, Any]) {
    override def hashCode(): Int = values.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case other: PartitionKey => values == other.values
      case _                   => false
    }
  }

  /** Window function values for a partition.
    *
    * `values` carries the existing per-partition scalars (aggregation-style windows:
    * SUM/COUNT/MIN/MAX/AVG, plus FIRST_VALUE/LAST_VALUE/ARRAY_AGG).
    *
    * `rankings` carries the per-row ordinals computed Scala-side from each ranking window's
    * top_hits sub-aggregation: a map `windowFunction.aliasOrName → (rowId → rank)`. The base-row
    * enrichment step looks up the ordinal by `doc._id` for each ranking window.
    */
  protected case class WindowValues(
    values: ListMap[String, Any],
    rankings: Map[String, Map[String, Long]] = Map.empty
  )

  /** Cache of partition key -> window values
    */
  protected case class WindowCache(cache: ListMap[PartitionKey, WindowValues]) {
    def get(key: PartitionKey): Option[WindowValues] = cache.get(key)
    def size: Int = cache.size
  }

  // ========================================================================
  // ROW COMPLETENESS — SCROLL ROUTING (issues #209 / #224)
  // ========================================================================

  /** True when a row-shaped query must page through scroll instead of the one-shot search:
    *
    *   - no `LIMIT` at all (#209): a one-shot search cannot honor "no LIMIT means every row" — with
    *     no `size` Elasticsearch returns its default 10 hits;
    *   - an explicit `LIMIT` whose window (`offset + limit`) exceeds
    *     [[SearchApi.DefaultMaxResultWindow]] (#224): Elasticsearch rejects a one-shot search
    *     whenever `from + size > index.max_result_window` (default 10,000), so the SAME query would
    *     fail with `LIMIT 20000` yet succeed with no LIMIT. The window is per-index and not probed
    *     (a multi-index query has no single window anyway); on an index tuned HIGHER the query
    *     pages unnecessarily but stays correct — and a >10k one-shot response is better paged
    *     regardless.
    */
  private def requiresScrollPaging(limit: Option[Limit]): Boolean =
    limit match {
      case None => true
      case Some(l) =>
        l.limit.toLong + l.offset.map(_.offset.toLong).getOrElse(0L) >
          SearchApi.DefaultMaxResultWindow
    }

  /** Issue #224 — a one-shot search whose `from + size` exceeds `index.max_result_window` is
    * rejected by Elasticsearch with an `illegal_argument_exception` that names neither the SQL
    * `LIMIT` that produced the `size` nor the remedy — and downstream consumers often flatten it
    * further. An index tuned BELOW the routing threshold of [[SearchApi.DefaultMaxResultWindow]]
    * can still surface the rejection despite the scroll routing, so translate it into an actionable
    * message here; every other error passes through unchanged.
    */
  private def enrichMaxResultWindowError(error: ElasticError): ElasticError = {
    def mentionsWindow(message: String): Boolean =
      message != null &&
      (message.contains("max_result_window") || message.contains("Result window is too large"))
    // The REST high-level clients (ES 6/7) surface the per-shard root cause as SUPPRESSED
    // exceptions on an "all shards failed" wrapper, so the scan walks both chains (bounded —
    // exception graphs can be cyclic in theory).
    def throwableMentionsWindow(t: Throwable, depth: Int = 10): Boolean =
      t != null && depth > 0 &&
      (mentionsWindow(t.getMessage) ||
      t.getSuppressed.exists(s => throwableMentionsWindow(s, depth - 1)) ||
      throwableMentionsWindow(t.getCause, depth - 1))
    if (mentionsWindow(error.message) || error.cause.exists(t => throwableMentionsWindow(t)))
      error.copy(
        message =
          "LIMIT/OFFSET exceeds the index's `index.max_result_window` for a single search: " +
          "lower LIMIT/OFFSET so `offset + limit` fits within the window, raise " +
          "`index.max_result_window` on the index, or drop the LIMIT to page through every row. " +
          s"Elasticsearch said: ${error.message}",
        statusCode = error.statusCode.orElse(Some(400))
      )
    else error
  }

  /** Collect the rows of a row-shaped query through the scroll path.
    *
    * The scroll path pages completely (PIT / search_after) and already handles window enrichment
    * and script fields, so `search` / `searchAsync` route here every row query that
    * [[requiresScrollPaging]] flags: with no LIMIT the stream is unbounded (every matching row,
    * #209); with an explicit LIMIT above the one-shot window (#224) the stream is bounded by
    * `maxDocuments = offset + limit` (enforced by ScrollApi's `.take`) and the first `offset` rows
    * are dropped client-side — scroll contexts reject `from`, and per-page `size` is the scroll
    * batch size, so the statement's LIMIT is stripped before translation. Aggregation-shaped
    * queries must never be routed — their result is the aggregation itself, already bounded by an
    * explicit `terms` size.
    */
  private[client] def scrollRows(
    scrollApi: ScrollApi,
    single: SingleSearch,
    elasticQuery: ElasticQuery
  )(implicit context: ConversionContext): Future[ElasticResult[ElasticResponse]] = {
    implicit val system: ActorSystem = SearchApi.scrollRoutingSystem
    implicit val ec: ExecutionContext = system.dispatcher
    val sql = elasticQuery.sql.orElse(Option(single.sql))
    val offset = single.limit.flatMap(_.offset).map(_.offset.toLong).getOrElse(0L)
    val maxDocuments = single.limit.map(l => offset + l.limit.toLong)
    val statement = if (single.limit.isEmpty) single else single.copy(limit = None)
    logger.info(
      s"▶ Row query ${maxDocuments.fold("without LIMIT")(max => s"with LIMIT window $max above ${SearchApi.DefaultMaxResultWindow}")} — routing through scroll for row completeness:\n${sql
        .getOrElse(elasticQuery.query)}"
    )
    scrollApi
      .scroll(statement, ScrollConfig(maxDocuments = maxDocuments))
      .map(_._1)
      .drop(offset)
      .runWith(Sink.seq)
      .map { rows =>
        logger.info(s"✅ Scroll-routed search returned ${rows.size} rows")
        ElasticResult.success(
          ElasticResponse(
            sql,
            elasticQuery.query,
            rows,
            single.fieldAliases,
            ListMap.empty
          )
        )
      }
      .recover { case t =>
        logger.error(
          s"❌ Scroll-routed search failed for query \n${sql.getOrElse(elasticQuery.query)} -> ${t.getMessage}"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"Scroll-routed search failed: ${t.getMessage}",
            cause = Some(t),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("searchAsync")
          )
        )
      }
  }

}

object SearchApi {

  /** Elasticsearch's default `index.max_result_window`: the ceiling on `from + size` for a one-shot
    * search, identical across ES 6/7/8/9. Row queries whose explicit LIMIT window exceeds it are
    * routed through scroll (#224) — see [[SearchApi.requiresScrollPaging]]. The actual per-index
    * setting is deliberately NOT probed; an index tuned higher just pages, an index tuned lower
    * keeps its (translated) one-shot rejection below this threshold.
    */
  val DefaultMaxResultWindow: Long = 10000L

  /** JVM-shared materializer for [[SearchApi.scrollRows]]. Daemonic so an un-terminated system can
    * never keep the JVM alive — clients don't own it, so no `close()` reaches it.
    */
  private[client] lazy val scrollRoutingSystem: ActorSystem =
    ActorSystem(
      "softclient4es-scroll-routing",
      ConfigFactory.parseString("akka.daemonic = on")
    )
}
