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

package app.softnetwork.elastic.client.metrics

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.{
  ElasticClientApi,
  ElasticClientDelegator,
  ElasticQueries,
  ElasticQuery,
  ElasticResponse,
  SingleValueAggregateResult
}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery}
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success}

/** Decorator that adds metrics to an existing Elasticsearch client.
  *
  * @param delegate
  *   - The Elasticsearch client to decorate
  * @param metricsCollector
  *   - The Metrics Collector
  */
class MetricsElasticClient(
  val delegate: ElasticClientApi,
  val metricsCollector: MetricsCollector
) extends ElasticClientDelegator
    with MetricsApi {

  // Helper for measuring operations
  private def measureAsync[T](operation: String, index: Option[String] = None)(
    block: => Future[T]
  )(implicit ec: ExecutionContext): Future[T] = {
    val startTime = System.currentTimeMillis()
    block.transform {
      case Success(result) =>
        val duration = System.currentTimeMillis() - startTime
        metricsCollector.recordOperation(operation, duration, success = true, index)
        Success(result)
      case Failure(ex) =>
        val duration = System.currentTimeMillis() - startTime
        metricsCollector.recordOperation(operation, duration, success = false, index)
        Failure(ex)
    }
  }

  private def measureResult[T](operation: String, index: Option[String] = None)(
    block: => ElasticResult[T]
  ): ElasticResult[T] = {
    val startTime = System.currentTimeMillis()
    val result = block
    val duration = System.currentTimeMillis() - startTime
    metricsCollector.recordOperation(operation, duration, success = result.isSuccess, index)
    result
  }

  // ==================== VersionApi ====================

  override def version: ElasticResult[String] =
    measureResult("version") {
      delegate.version
    }

  // ==================== IndicesApi ====================

  override def createIndex(index: String, settings: String): ElasticResult[Boolean] = {
    measureResult("createIndex", Some(index)) {
      delegate.createIndex(index, settings)
    }
  }

  override def deleteIndex(index: String): ElasticResult[Boolean] = {
    measureResult("deleteIndex", Some(index)) {
      delegate.deleteIndex(index)
    }
  }

  override def closeIndex(index: String): ElasticResult[Boolean] = {
    measureResult("closeIndex", Some(index)) {
      delegate.closeIndex(index)
    }
  }

  override def openIndex(index: String): ElasticResult[Boolean] = {
    measureResult("openIndex", Some(index)) {
      delegate.openIndex(index)
    }
  }

  override def reindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean
  ): ElasticResult[(Boolean, Option[Long])] = {
    measureResult("reindex", Some(s"$sourceIndex->$targetIndex")) {
      delegate.reindex(sourceIndex, targetIndex, refresh)
    }
  }

  override def indexExists(index: String): ElasticResult[Boolean] = {
    measureResult("indexExists", Some(index)) {
      delegate.indexExists(index)
    }
  }

  // ==================== AliasApi ====================

  override def addAlias(index: String, alias: String): ElasticResult[Boolean] = {
    measureResult("addAlias", Some(index)) {
      delegate.addAlias(index, alias)
    }
  }

  override def removeAlias(index: String, alias: String): ElasticResult[Boolean] = {
    measureResult("removeAlias", Some(index)) {
      delegate.removeAlias(index, alias)
    }
  }

  /** Check if an alias exists.
    *
    * @param alias
    *   the name of the alias to check
    * @return
    *   ElasticSuccess(true) if it exists, ElasticSuccess(false) otherwise, ElasticFailure in case
    *   of error
    * @example
    * {{{
    * aliasExists("my-alias") match {
    *   case ElasticSuccess(true)  => println("Alias exists")
    *   case ElasticSuccess(false) => println("Alias does not exist")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    */
  override def aliasExists(alias: String): ElasticResult[Boolean] =
    measureResult("aliasExists") {
      delegate.aliasExists(alias)
    }

  /** Retrieve all aliases from an index.
    *
    * @param index
    *   the index name
    * @return
    *   ElasticResult with the list of aliases
    * @example
    * {{{
    * getAliases("my-index") match {
    *   case ElasticSuccess(aliases) => println(s"Aliases: ${aliases.mkString(", ")}")
    *   case ElasticFailure(error)   => println(s"Error: ${error.message}")
    * }
    *
    * }}}
    */
  override def getAliases(index: String): ElasticResult[Set[String]] =
    measureResult("getAliases", Some(index)) {
      delegate.getAliases(index)
    }

  /** Atomic swap of an alias between two indexes.
    *
    * This operation is atomic: the alias is removed from oldIndex and added to newIndex in a single
    * query, thus avoiding any period when the alias does not exist. This is the recommended
    * operation for zero-downtime deployments.
    *
    * @param oldIndex
    *   the current index pointed to by the alias
    * @param newIndex
    *   the new index that should point to the alias
    * @param alias
    *   the name of the alias to swap
    * @return
    *   ElasticSuccess(true) if swapped, ElasticFailure otherwise
    * @example
    * {{{
    * // Zero-downtime deployment
    * swapAlias(oldIndex = "products-v1", newIndex = "products-v2", alias = "products") match {
    *   case ElasticSuccess(_)     => println("✅ Alias swapped, new version deployed")
    *   case ElasticFailure(error) => println(s"❌ Error: ${error.message}")
    * }
    * }}}
    * @note
    *   This operation is atomic and therefore preferable to removeAlias + addAlias
    */
  override def swapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] =
    measureResult("swapAlias", Some(s"$oldIndex->$newIndex")) {
      delegate.swapAlias(oldIndex, newIndex, alias)
    }

  // ==================== SettingsApi ====================

  override def updateSettings(index: String, settings: String): ElasticResult[Boolean] = {
    measureResult("updateSettings", Some(index)) {
      delegate.updateSettings(index, settings)
    }
  }

  override def loadSettings(index: String): ElasticResult[String] = {
    measureResult("loadSettings", Some(index)) {
      delegate.loadSettings(index)
    }
  }

  /** Toggle the refresh interval of an index.
    *
    * @param index
    *   - the name of the index
    * @param enable
    *   - true to enable the refresh interval, false to disable it
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  override def toggleRefresh(index: String, enable: Boolean): ElasticResult[Boolean] =
    measureResult("toggleRefresh", Some(index)) {
      delegate.toggleRefresh(index, enable)
    }

  /** Set the number of replicas for an index.
    *
    * @param index
    *   - the name of the index
    * @param replicas
    *   - the number of replicas to set
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  override def setReplicas(index: String, replicas: Int): ElasticResult[Boolean] =
    measureResult("setReplicas", Some(index)) {
      delegate.setReplicas(index, replicas)
    }

  // ==================== MappingApi ====================

  override def setMapping(index: String, mapping: String): ElasticResult[Boolean] = {
    measureResult("setMapping", Some(index)) {
      delegate.setMapping(index, mapping)
    }
  }

  override def getMapping(index: String): ElasticResult[String] = {
    measureResult("getMapping", Some(index)) {
      delegate.getMapping(index)
    }
  }

  /** Get the mapping properties of an index.
    *
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  override def getMappingProperties(index: String): ElasticResult[String] =
    measureResult("getMappingProperties", Some(index)) {
      delegate.getMappingProperties(index)
    }

  /** Check if the mapping of an index is different from the provided mapping.
    *
    * @param index
    *   - the name of the index to check
    * @param mapping
    *   - the mapping to compare with the current mapping of the index
    * @return
    *   true if the mapping is different, false otherwise
    */
  override def shouldUpdateMapping(index: String, mapping: String): ElasticResult[Boolean] =
    measureResult("shouldUpdateMapping", Some(index)) {
      delegate.shouldUpdateMapping(index, mapping)
    }

  /** Update the mapping of an index to a new mapping.
    *
    * This method handles three scenarios:
    *   1. Index doesn't exist: Create it with the new mapping 2. Index exists but mapping is
    *      outdated: Migrate to new mapping 3. Index exists and mapping is current: Do nothing
    *
    * @param index
    *   - the name of the index to migrate
    * @param mapping
    *   - the new mapping to set on the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the mapping was created or updated successfully, false otherwise
    */
  override def updateMapping(
    index: String,
    mapping: String,
    settings: String
  ): ElasticResult[Boolean] =
    measureResult("updateMapping", Some(index)) {
      delegate.updateMapping(index, mapping, settings)
    }

  // ==================== RefreshApi ====================

  override def refresh(index: String): ElasticResult[Boolean] = {
    measureResult("refresh", Some(index)) {
      delegate.refresh(index)
    }
  }

  // ==================== FlushApi ====================

  override def flush(index: String, force: Boolean, wait: Boolean): ElasticResult[Boolean] = {
    measureResult("flush", Some(index)) {
      delegate.flush(index, force, wait)
    }
  }

  // ==================== IndexApi ====================

  override def index(index: String, id: String, source: String): ElasticResult[Boolean] = {
    measureResult("index", Some(index)) {
      delegate.index(index, id, source)
    }
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = {
    measureAsync("indexAsync", Some(index)) {
      delegate.indexAsync(index, id, source)
    }
  }

  /** Index an entity in the given index.
    *
    * @param entity
    *   - the entity to index
    * @param id
    *   - the id of the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  override def indexAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean] =
    measureResult("indexAs", index) {
      delegate.indexAs(entity, id, index, maybeType)
    }

  /** Index an entity in the given index asynchronously.
    *
    * @param entity
    *   - the entity to index
    * @param id
    *   - the id of the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  override def indexAsyncAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit
    u: ClassTag[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Boolean]] =
    measureAsync("indexAsyncAs", index) {
      delegate.indexAsyncAs(entity, id, index, maybeType)
    }

  // ==================== UpdateApi ====================

  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): ElasticResult[Boolean] = {
    measureResult("update", Some(index)) {
      delegate.update(index, id, source, upsert)
    }
  }

  override def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = {
    measureAsync("updateAsync", Some(index)) {
      delegate.updateAsync(index, id, source, upsert)
    }
  }

  /** Update an entity in the given index.
    *
    * @param entity
    *   - the entity to update
    * @param id
    *   - the id of the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  override def updateAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String],
    maybeType: Option[String],
    upsert: Boolean
  )(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean] =
    measureResult("updateAs", index) {
      delegate.updateAs(entity, id, index, maybeType, upsert)
    }

  /** Update an entity in the given index asynchronously.
    *
    * @param entity
    *   - the entity to update
    * @param id
    *   - the id of the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  override def updateAsyncAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String],
    maybeType: Option[String],
    upsert: Boolean
  )(implicit
    u: ClassTag[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Boolean]] =
    measureAsync("updateAsyncAs", index) {
      delegate.updateAsyncAs(entity, id, index, maybeType, upsert)
    }

  // ==================== DeleteApi ====================

  override def delete(id: String, index: String): ElasticResult[Boolean] = {
    measureResult("delete", Some(index)) {
      delegate.delete(id, index)
    }
  }

  override def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = {
    measureAsync("deleteAsync", Some(index)) {
      delegate.deleteAsync(id, index)
    }
  }

  // ==================== GetApi ====================

  /** Get a document by its id from the given index.
    *
    * @param id
    *   - the id of the document to get
    * @param index
    *   - the name of the index to get the document from
    * @return
    *   an Option containing the document as a JSON string if it was found, None otherwise
    */
  override def get(id: String, index: String): ElasticResult[Option[String]] =
    measureResult("get", Some(index)) {
      delegate.get(id, index)
    }

  override def getAs[U <: AnyRef](
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Option[U]] = {
    measureResult("get", index) {
      delegate.getAs[U](id, index, maybeType)
    }
  }

  /** Get a document by its id from the given index asynchronously.
    *
    * @param id
    *   - the id of the document to get
    * @param index
    *   - the name of the index to get the document from
    * @return
    *   a Future that completes with an Option containing the document as a JSON string if it was
    *   found, None otherwise
    */
  override def getAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    measureAsync("getAsync", Some(index)) {
      delegate.getAsync(id, index)
    }

  /** Get an entity by its id from the given index asynchronously.
    *
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with an Option containing the entity if it was found, None otherwise
    */
  override def getAsyncAs[U <: AnyRef](
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Option[U]]] =
    measureAsync("getAsyncAs", index) {
      delegate.getAsyncAs[U](id, index, maybeType)
    }

  // ==================== CountApi ====================

  override def count(query: ElasticQuery): ElasticResult[Option[Double]] = {
    measureResult("count", Some(query.indices.mkString(","))) {
      delegate.count(query)
    }
  }

  /** Count the number of documents matching the given JSON query asynchronously.
    *
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  override def countAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]] =
    measureAsync("countAsync", Some(query.indices.mkString(","))) {
      delegate.countAsync(query)
    }

  // ==================== AggregateApi =================

  /** Aggregate the results of the given SQL query.
    *
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  override def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[collection.Seq[SingleValueAggregateResult]]] =
    measureAsync("aggregate") {
      delegate.aggregate(sqlQuery)
    }

  // ==================== SearchApi ====================

  /** Search for documents / aggregations matching the SQL query.
    *
    * @param sql
    *   the SQL query to execute
    * @return
    *   the Elasticsearch response
    */
  override def search(sql: SQLQuery): ElasticResult[ElasticResponse] =
    measureResult("search") {
      delegate.search(sql)
    }

  /** Asynchronous search for documents / aggregations matching the SQL query.
    *
    * @param sqlQuery
    *   the SQL query
    * @return
    *   a Future containing the Elasticsearch response
    */
  override def searchAsync(
    sqlQuery: SQLQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]] =
    measureAsync("searchAsync") {
      delegate.searchAsync(sqlQuery)
    }

  /** Searches and converts results into typed entities from an SQL query.
    *
    * @param sqlQuery
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  override def searchAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] =
    measureResult("searchAs") {
      delegate.searchAs[U](sqlQuery)
    }

  /** Asynchronous search with conversion to typed entities.
    *
    * @param sqlQuery
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  override def searchAsyncAs[U](sqlQuery: SQLQuery)(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] =
    measureAsync("searchAsyncAs") {
      delegate.searchAsyncAs[U](sqlQuery)
    }

  override def singleSearch(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] = {
    measureResult("search", Some(elasticQuery.indices.mkString(","))) {
      delegate.singleSearch(elasticQuery, fieldAliases, aggregations)
    }
  }

  override def multiSearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] = {
    measureResult("multisearch") {
      delegate.multiSearch(elasticQueries, fieldAliases, aggregations)
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
  override def singleSearchAsync(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]] =
    measureAsync("searchAsync", Some(elasticQuery.indices.mkString(","))) {
      delegate.singleSearchAsync(elasticQuery, fieldAliases, aggregations).asInstanceOf
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
  override def multiSearchAsync(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]] =
    measureAsync("multisearchAsync") {
      delegate.multiSearchAsync(elasticQueries, fieldAliases, aggregations).asInstanceOf
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
  override def singleSearchAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] =
    measureResult("searchAs", Some(elasticQuery.indices.mkString(","))) {
      delegate.singleSearchAs(elasticQuery, fieldAliases, aggregations)
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
  override def multisearchAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] =
    measureResult("multisearchAs") {
      delegate.multisearchAs(elasticQueries, fieldAliases, aggregations)
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
  override def singleSearchAsyncAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] =
    measureAsync("searchAsyncAs", Some(elasticQuery.indices.mkString(","))) {
      delegate.singleSearchAsyncAs(elasticQuery, fieldAliases, aggregations)
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
  override def multiSearchAsyncAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] =
    measureAsync("multisearchAsyncAs") {
      delegate.multiSearchAsyncAs(elasticQueries, fieldAliases, aggregations)
    }

  override def searchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    sql: SQLQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] =
    measureResult("searchWithInnerHits") {
      delegate.searchWithInnerHits[U, I](sql, innerField)
    }

  override def singleSearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQuery: ElasticQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] =
    measureResult("searchWithInnerHits", Some(elasticQuery.indices.mkString(","))) {
      delegate.singleSearchWithInnerHits[U, I](elasticQuery, innerField)
    }

  override def multisearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQueries: ElasticQueries,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] =
    measureResult("multisearchWithInnerHits") {
      delegate.multisearchWithInnerHits[U, I](elasticQueries, innerField)
    }

  // ==================== ScrollApi ====================

  /** Create a scrolling source with automatic strategy selection
    */
  override def scroll(sql: SQLQuery, config: ScrollConfig)(implicit
    system: ActorSystem
  ): Source[(Map[String, Any], ScrollMetrics), NotUsed] = {
    // Note: For streams, we measure at the beginning but not every element
    val startTime = System.currentTimeMillis()
    val source = delegate.scroll(sql, config)

    source.watchTermination() { (_, done) =>
      done.onComplete { result =>
        val duration = System.currentTimeMillis() - startTime
        val success = result.isSuccess
        metricsCollector.recordOperation(
          "scroll",
          duration,
          success
        )
      }(system.dispatcher)
      NotUsed
    }

  }

  /** Typed scroll source
    */
  override def scrollAs[T](sql: SQLQuery, config: ScrollConfig)(implicit
    system: ActorSystem,
    m: Manifest[T],
    formats: Formats
  ): Source[(T, ScrollMetrics), NotUsed] = {
    // Note: For streams, we measure at the beginning but not every element
    val startTime = System.currentTimeMillis()
    val source = delegate.scrollAs[T](sql, config)

    source.watchTermination() { (_, done) =>
      done.onComplete { result =>
        val duration = System.currentTimeMillis() - startTime
        val success = result.isSuccess
        metricsCollector.recordOperation(
          "scrollAs",
          duration,
          success
        )
      }(system.dispatcher)
      NotUsed
    }
  }

  // ==================== BulkApi ====================

  override def bulkWithResult[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None,
    callbacks: BulkCallbacks = BulkCallbacks.default
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {
    implicit val ec: ExecutionContext = system.dispatcher
    measureAsync("bulkWithResult", Some(bulkOptions.index)) {
      delegate.bulkWithResult(
        items,
        toDocument,
        idKey,
        suffixDateKey,
        suffixDatePattern,
        update,
        delete,
        parentIdKey,
        callbacks
      )
    }
  }

  override def bulkSource[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Source[Either[FailedDocument, SuccessfulDocument], NotUsed] = {
    val startTime = System.currentTimeMillis()
    val source = delegate.bulkSource(
      items,
      toDocument,
      idKey,
      suffixDateKey,
      suffixDatePattern,
      update,
      delete,
      parentIdKey
    )
    source.watchTermination() { (_, done) =>
      done.onComplete { result =>
        val duration = System.currentTimeMillis() - startTime
        val success = result.isSuccess
        metricsCollector.recordOperation(
          "bulkSource",
          duration,
          success,
          Some(bulkOptions.index)
        )
      }(system.dispatcher)
      NotUsed
    }
  }

  override def bulk[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): ElasticResult[BulkResult] = {
    measureResult("bulk", Some(bulkOptions.index)) {
      delegate.bulk(
        items,
        toDocument,
        idKey,
        suffixDateKey,
        suffixDatePattern,
        update,
        delete,
        parentIdKey
      )
    }
  }

  // ==================== MetricsApi (délégation) ====================

  override def recordOperation(
    operation: String,
    duration: Long,
    success: Boolean,
    index: Option[String]
  ): Unit = {
    metricsCollector.recordOperation(operation, duration, success, index)
  }

  override def getMetrics: OperationMetrics = metricsCollector.getMetrics

  override def getMetricsByOperation(operation: String): Option[OperationMetrics] = {
    metricsCollector.getMetricsByOperation(operation)
  }

  override def getMetricsByIndex(index: String): Option[OperationMetrics] = {
    metricsCollector.getMetricsByIndex(index)
  }

  override def getAggregatedMetrics: AggregatedMetrics = {
    metricsCollector.getAggregatedMetrics
  }

  override def resetMetrics(): Unit = {
    metricsCollector.resetMetrics()
  }
}
