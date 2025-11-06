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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import com.typesafe.config.Config
import org.json4s.Formats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.reflect.{classTag, ClassTag}

trait ElasticClientDelegator extends ElasticClientApi with BulkTypes {

  def delegate: ElasticClientApi

  // Delegate the logger to the underlying client
  protected lazy val logger: Logger = LoggerFactory getLogger getClass.getName

  // Delegate config to the underlying client
  override lazy val config: Config = delegate.config

  // ==================== Closeable ====================

  override def close(): Unit = delegate.close()

  // ==================== VersionApi ====================

  /** Get Elasticsearch version.
    *
    * @return
    *   the Elasticsearch version
    */
  override def version: ElasticResult[String] =
    delegate.version

  override private[client] def executeVersion(): ElasticResult[String] =
    delegate.executeVersion()

  // ==================== IndicesApi ====================

  /** Create an index with the provided name and settings.
    *
    * @param index
    *   - the name of the index to create
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the index was created successfully, false otherwise
    */
  override def createIndex(index: String, settings: String): ElasticResult[Boolean] =
    delegate.createIndex(index, settings)

  /** Delete an index with the provided name.
    *
    * @param index
    *   - the name of the index to delete
    * @return
    *   true if the index was deleted successfully, false otherwise
    */
  override def deleteIndex(index: String): ElasticResult[Boolean] =
    delegate.deleteIndex(index)

  /** Close an index with the provided name.
    *
    * @param index
    *   - the name of the index to close
    * @return
    *   true if the index was closed successfully, false otherwise
    */
  override def closeIndex(index: String): ElasticResult[Boolean] =
    delegate.closeIndex(index)

  /** Open an index with the provided name.
    *
    * @param index
    *   - the name of the index to open
    * @return
    *   true if the index was opened successfully, false otherwise
    */
  override def openIndex(index: String): ElasticResult[Boolean] =
    delegate.openIndex(index)

  /** Reindex from source index to target index.
    *
    * @param sourceIndex
    *   - the name of the source index
    * @param targetIndex
    *   - the name of the target index
    * @param refresh
    *   - true to refresh the target index after reindexing, false otherwise
    * @return
    *   true and the number of documents re indexed if the reindexing was successful, false
    *   otherwise
    */
  override def reindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean
  ): ElasticResult[(Boolean, Option[Long])] =
    delegate.reindex(sourceIndex, targetIndex, refresh)

  /** Check if an index exists.
    *
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  override def indexExists(index: String): ElasticResult[Boolean] =
    delegate.indexExists(index)

  override private[client] def executeCreateIndex(
    index: String,
    settings: String
  ): ElasticResult[Boolean] =
    delegate.createIndex(index, settings)

  override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
    delegate.deleteIndex(index)

  override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
    delegate.closeIndex(index)

  override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
    delegate.openIndex(index)

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean
  ): ElasticResult[(Boolean, Option[Long])] =
    delegate.reindex(sourceIndex, targetIndex, refresh)

  override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
    delegate.indexExists(index)

  // ==================== AliasApi ====================

  /** Add an alias to an index.
    *
    * This operation:
    *   1. Validates the index and alias names 2. Checks that the index exists 3. Adds the alias
    *
    * @param index
    *   the index name
    * @param alias
    *   the alias name to add
    * @return
    *   ElasticSuccess(true) if added, ElasticFailure otherwise
    * @example
    * {{{
    * addAlias("my-index-2024", "my-index-current") match {
    *   case ElasticSuccess(_)     => println("Alias added")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    * @note
    *   An alias can point to multiple indexes (useful for searches)
    * @note
    *   An index can have multiple aliases
    */
  override def addAlias(index: String, alias: String): ElasticResult[Boolean] =
    delegate.addAlias(index, alias)

  /** Remove an alias from an index.
    *
    * @param index
    *   the name of the index
    * @param alias
    *   the name of the alias to remove
    * @return
    *   ElasticSuccess(true) if removed, ElasticFailure otherwise
    * @example
    * {{{
    * removeAlias("my-index-2024", "my-index-current") match {
    *   case ElasticSuccess(_)     => println("Alias removed")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    * @note
    *   If the alias does not exist, Elasticsearch returns a 404 error
    */
  override def removeAlias(index: String, alias: String): ElasticResult[Boolean] =
    delegate.removeAlias(index, alias)

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
    delegate.aliasExists(alias)

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
    delegate.getAliases(index)

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
    delegate.swapAlias(oldIndex, newIndex, alias)

  override private[client] def executeAddAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] =
    delegate.addAlias(index, alias)

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] =
    delegate.removeAlias(index, alias)

  override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] =
    delegate.aliasExists(alias)

  override private[client] def executeGetAliases(index: String): ElasticResult[String] =
    delegate.executeGetAliases(index)

  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] =
    delegate.swapAlias(oldIndex, newIndex, alias)

  // ==================== SettingsApi ====================

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
    delegate.toggleRefresh(index, enable)

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
    delegate.setReplicas(index, replicas)

  /** Update index settings.
    *
    * @param index
    *   - the name of the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  override def updateSettings(index: String, settings: String): ElasticResult[Boolean] =
    delegate.updateSettings(index, settings)

  /** Load the settings of an index.
    *
    * @param index
    *   - the name of the index to load the settings for
    * @return
    *   the settings of the index as a JSON string
    */
  override def loadSettings(index: String): ElasticResult[String] =
    delegate.loadSettings(index)

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean] =
    delegate.updateSettings(index, settings)

  override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
    delegate.loadSettings(index)
  }

  // ==================== MappingApi ====================

  /** Set the mapping of an index.
    *
    * @param index
    *   - the name of the index to set the mapping for
    * @param mapping
    *   - the mapping to set on the index
    * @return
    *   true if the mapping was set successfully, false otherwise
    */
  override def setMapping(index: String, mapping: String): ElasticResult[Boolean] =
    delegate.setMapping(index, mapping)

  /** Get the mapping of an index.
    *
    * @param index
    *   - the name of the index to get the mapping for
    * @return
    *   the mapping of the index as a JSON string
    */
  override def getMapping(index: String): ElasticResult[String] =
    delegate.getMapping(index)

  /** Get the mapping properties of an index.
    *
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  override def getMappingProperties(index: String): ElasticResult[String] =
    delegate.getMappingProperties(index)

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
    delegate.shouldUpdateMapping(index, mapping)

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
    delegate.updateMapping(index, mapping, settings)

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): ElasticResult[Boolean] =
    delegate.setMapping(index, mapping)

  override private[client] def executeGetMapping(index: String): ElasticResult[String] = {
    delegate.getMapping(index)
  }

  // ==================== RefreshApi ====================

  /** Refresh the index to make sure all documents are indexed and searchable.
    *
    * @param index
    *   - the name of the index to refresh
    * @return
    *   true if the index was refreshed successfully, false otherwise
    */
  override def refresh(index: String): ElasticResult[Boolean] = delegate.refresh(index)

  override private[client] def executeRefresh(index: String): ElasticResult[Boolean] =
    delegate.executeRefresh(index)

  // ==================== FlushApi ====================

  /** Flush the index to make sure all operations are written to disk.
    *
    * @param index
    *   - the name of the index to flush
    * @param force
    *   - true to force the flush, false otherwise
    * @param wait
    *   - true to wait for the flush to complete, false otherwise
    * @return
    *   true if the index was flushed successfully, false otherwise
    */
  override def flush(index: String, force: Boolean, wait: Boolean): ElasticResult[Boolean] =
    delegate.flush(index, force, wait)

  override private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    delegate.executeFlush(index, force, wait)

  // ==================== IndexApi ====================

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
    delegate.indexAs(entity, id, index, maybeType)

  /** Index an entity in the given index.
    *
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  override def index(index: String, id: String, source: String): ElasticResult[Boolean] =
    delegate.index(index, id, source)

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
    delegate.indexAsyncAs(entity, id, index, maybeType)

  /** Index an entity in the given index asynchronously.
    *
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = delegate.indexAsync(index, id, source)
  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String
  ): ElasticResult[Boolean] =
    delegate.executeIndex(index, id, source)

  override private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    delegate.executeIndexAsync(index, id, source)

  // ==================== UpdateApi ====================

  /** Update an entity in the given index.
    *
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): ElasticResult[Boolean] =
    delegate.update(index, id, source, upsert)

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
    delegate.updateAs(entity, id, index, maybeType, upsert)

  /** Update an entity in the given index asynchronously.
    *
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  override def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    delegate.updateAsync(index, id, source, upsert)

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
    delegate.updateAsyncAs(entity, id, index, maybeType, upsert)

  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): ElasticResult[Boolean] =
    delegate.executeUpdate(index, id, source, upsert)

  override private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    delegate.executeUpdateAsync(index, id, source, upsert)

  // ==================== DeleteApi ====================

  /** Delete an entity from the given index.
    *
    * @param id
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  override def delete(id: String, index: String): ElasticResult[Boolean] =
    delegate.delete(id, index)

  /** Delete an entity from the given index asynchronously.
    *
    * @param id
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  override def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    delegate.deleteAsync(id, index)

  override private[client] def executeDelete(
    index: String,
    id: String
  ): ElasticResult[Boolean] =
    delegate.executeDelete(index, id)

  override private[client] def executeDeleteAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    delegate.executeDeleteAsync(index, id)

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
    delegate.get(id, index)

  /** Get an entity by its id from the given index.
    *
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   an Option containing the entity if it was found, None otherwise
    */
  override def getAs[U <: AnyRef](id: String, index: Option[String], maybeType: Option[String])(
    implicit
    m: Manifest[U],
    formats: Formats
  ): ElasticResult[Option[U]] =
    delegate.getAs(id, index, maybeType)

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
    delegate.getAsync(id, index)

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
    delegate.getAsyncAs(id, index, maybeType)

  override private[client] def executeGet(
    index: String,
    id: String
  ): ElasticResult[Option[String]] =
    delegate.executeGet(index, id)

  override private[client] def executeGetAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    delegate.executeGetAsync(index, id)

  // ==================== CountApi ====================

  /** Count the number of documents matching the given JSON query.
    *
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  override def count(query: ElasticQuery): ElasticResult[Option[Double]] =
    delegate.count(query)

  /** Count the number of documents matching the given JSON query asynchronously.
    *
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  override def countAsync(query: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[Double]]] =
    delegate.countAsync(query)

  override private[client] def executeCount(query: ElasticQuery): ElasticResult[Option[Double]] =
    delegate.executeCount(query)

  override private[client] def executeCountAsync(query: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[Double]]] =
    delegate.executeCountAsync(query)

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
    delegate.aggregate(sqlQuery)

  // ==================== SearchApi ====================

  /** Search for documents / aggregations matching the SQL query.
    *
    * @param sql
    *   the SQL query to execute
    * @return
    *   the Elasticsearch response
    */
  override def search(sql: SQLQuery): ElasticResult[ElasticResponse] = delegate.search(sql)

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
  override def singleSearch(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] =
    delegate.singleSearch(elasticQuery, fieldAliases, aggregations)

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
  override def multiSearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResult[ElasticResponse] =
    delegate.multiSearch(elasticQueries, fieldAliases, aggregations)

  /** Asynchronous search for documents / aggregations matching the SQL query.
    *
    * @param sqlQuery
    *   the SQL query
    * @return
    *   a Future containing the Elasticsearch response
    */
  override def searchAsync(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[ElasticResponse]] = delegate.searchAsync(sqlQuery)

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
    delegate.singleSearchAsync(elasticQuery, fieldAliases, aggregations)

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
    delegate.multiSearchAsync(elasticQueries, fieldAliases, aggregations)

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
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = delegate.searchAs(sqlQuery)

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
    delegate.singleSearchAs(elasticQuery, fieldAliases, aggregations)

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
    delegate.multisearchAs(elasticQueries, fieldAliases, aggregations)

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
  ): Future[ElasticResult[Seq[U]]] = delegate.searchAsyncAs(sqlQuery)

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
    delegate.singleSearchAsyncAs(elasticQuery, fieldAliases, aggregations)

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
    delegate.multiSearchAsyncAs(elasticQueries, fieldAliases, aggregations)

  override def searchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    sql: SQLQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] =
    delegate.searchWithInnerHits[U, I](sql, innerField)

  override def singleSearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQuery: ElasticQuery,
    innerField: String
  )(implicit formats: Formats): ElasticResult[Seq[(U, Seq[I])]] =
    delegate.singleSearchWithInnerHits[U, I](elasticQuery, innerField)

  override def multisearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQueries: ElasticQueries,
    innerField: String
  )(implicit formats: Formats): ElasticResult[Seq[(U, Seq[I])]] =
    delegate.multisearchWithInnerHits[U, I](elasticQueries, innerField)

  override private[client] implicit def sqlSearchRequestToJsonQuery(
    sqlSearch: SQLSearchRequest
  ): String =
    delegate.sqlSearchRequestToJsonQuery(sqlSearch)

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]] =
    delegate.executeSingleSearch(elasticQuery)

  override private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]] =
    delegate.executeMultiSearch(elasticQueries)

  override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    delegate.executeSingleSearchAsync(elasticQuery)

  override private[client] def executeMultiSearchAsync(elasticQueries: ElasticQueries)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    delegate.executeMultiSearchAsync(elasticQueries)

  // ==================== ScrollApi ====================

  /** Create a scrolling source with automatic strategy selection
    */
  override def scroll(sql: SQLQuery, config: ScrollConfig)(implicit
    system: ActorSystem
  ): Source[(Map[String, Any], ScrollMetrics), NotUsed] = delegate.scroll(sql, config)

  /** Typed scroll source
    */
  override def scrollAs[T](sql: SQLQuery, config: ScrollConfig)(implicit
    system: ActorSystem,
    m: Manifest[T],
    formats: Formats
  ): Source[(T, ScrollMetrics), NotUsed] = delegate.scrollAs(sql, config)

  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    delegate.scrollClassic(elasticQuery, fieldAliases, aggregations, config)
  }

  override private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    delegate.searchAfter(elasticQuery, fieldAliases, config, hasSorts)
  }

  override private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[JSONResults, JSONResults],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem) = {
    delegate.pitSearchAfter(elasticQuery, fieldAliases, config, hasSorts)
  }

  // ==================== BulkApi ====================

  /** Bulk with detailed results (successes + failures).
    *
    * This method provides:
    *
    *   - List of successfully indexed documents
    *   - List of failed documents with error details
    *   - Performance metrics
    *   - Configurable automatic retry
    *
    * @param items
    *   : documents to index
    * @param toDocument
    *   : JSON transformation function
    * @param indexKey
    *   : key for the index field
    * @param idKey
    *   : key for the id field
    * @param suffixDateKey
    *   : key for the date field to suffix the index
    * @param suffixDatePattern
    *   : date pattern for the suffix
    * @param update
    *   : true for upsert, false for index
    * @param delete
    *   : true for delete
    * @param parentIdKey
    *   : key for the parent field
    * @param callbacks
    *   : callbacks for events
    * @param bulkOptions
    *   : configuration options
    * @return
    *   Future with detailed results
    */
  override def bulkWithResult[D](
    items: Iterator[D],
    toDocument: D => String,
    indexKey: Option[String],
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String],
    callbacks: BulkCallbacks
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] =
    delegate.bulkWithResult(
      items,
      toDocument,
      indexKey,
      idKey,
      suffixDateKey,
      suffixDatePattern,
      update,
      delete,
      parentIdKey,
      callbacks
    )

  /** Source: Akka Streams, which provides real-time results.
    *
    * Each emitted item is either:
    *   - Right(id) for success
    *   - Left(failed) for failure
    *
    * @example
    * {{{
    *   bulkSource(items, toDocument)
    *     .runWith(Sink.foreach {
    *       case Right(id)    => println(s"✅ Success: $id")
    *       case Left(failed) => println(s"❌ Failed: ${failed.id}")
    *     })
    * }}}
    * @param items
    *   the documents to index
    * @param toDocument
    *   JSON transformation function
    * @param indexKey
    *   key for the index field
    * @param idKey
    *   key for the id field
    * @param suffixDateKey
    *   date field key to suffix the index
    * @param suffixDatePattern
    *   date pattern for the suffix
    * @param update
    *   true for upsert, false for index
    * @param delete
    *   true to delete
    * @param parentIdKey
    *   parent field key
    * @param bulkOptions
    *   configuration options
    * @return
    *   Source outputting Right(id) or Left(failed)
    */
  override def bulkSource[D](
    items: Iterator[D],
    toDocument: D => String,
    indexKey: Option[String],
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String]
  )(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Source[Either[FailedDocument, SuccessfulDocument], NotUsed] = delegate.bulkSource(
    items,
    toDocument,
    indexKey,
    idKey,
    suffixDateKey,
    suffixDatePattern,
    update,
    delete,
    parentIdKey
  )

  /** Backward compatible API (old signature).
    *
    * @deprecated
    *   Use `bulkWithResult` to get failure details
    */
  override def bulk[D](
    items: Iterator[D],
    toDocument: D => String,
    indexKey: Option[String],
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String]
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): ElasticResult[BulkResult] = delegate
    .bulk(
      items,
      toDocument,
      indexKey,
      idKey,
      suffixDateKey,
      suffixDatePattern,
      update,
      delete,
      parentIdKey
    )

  override private[client] def toBulkAction(bulkItem: BulkItem): BulkActionType =
    delegate.toBulkAction(bulkItem).asInstanceOf[BulkActionType]

  override private[client] implicit def toBulkElasticAction(a: BulkActionType): BulkElasticAction =
    delegate.toBulkElasticAction(a.asInstanceOf)

  /** Basic flow for executing a bulk action. This method must be implemented by concrete classes
    * depending on the Elasticsearch version and client used.
    *
    * @param bulkOptions
    *   configuration options
    * @return
    *   Flow transforming bulk actions into results
    */
  override private[client] def bulkFlow(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed] =
    delegate.bulkFlow(bulkOptions, system).asInstanceOf

  /** Convert a BulkResultType into individual results. This method must extract the successes and
    * failures from the ES response.
    *
    * @param result
    *   raw result from the bulk
    * @return
    *   sequence of Right(id) for success or Left(failed) for failure
    */
  override private[client] def extractBulkResults(
    result: BulkResultType,
    originalBatch: Seq[BulkItem]
  ): Seq[Either[FailedDocument, SuccessfulDocument]] =
    delegate.extractBulkResults(result.asInstanceOf, originalBatch)

  /** Conversion BulkActionType -> BulkItem */
  override private[client] def actionToBulkItem(action: BulkActionType): BulkItem =
    delegate.actionToBulkItem(action.asInstanceOf)
}
