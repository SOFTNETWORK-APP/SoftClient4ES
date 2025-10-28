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
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLSearchRequest}
import com.typesafe.config.Config
import org.json4s.Formats
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait ElasticClientDelegator extends ElasticClientApi with BulkTypes {

  def delegate: ElasticClientApi

  // Delegate the logger to the underlying client
  protected lazy val logger: Logger = LoggerFactory getLogger getClass.getName

  // Delegate config to the underlying client
  override lazy val config: Config = delegate.config

  // ==================== Closeable ====================

  override def close(): Unit = delegate.close()

  // ==================== IndicesApi ====================

  override def createIndex(index: String, settings: String): Boolean = {
    delegate.createIndex(index, settings)
  }

  override def deleteIndex(index: String): Boolean = {
    delegate.deleteIndex(index)
  }

  override def closeIndex(index: String): Boolean = {
    delegate.closeIndex(index)
  }

  override def openIndex(index: String): Boolean = {
    delegate.openIndex(index)
  }

  override def reindex(sourceIndex: String, targetIndex: String, refresh: Boolean): Boolean = {
    delegate.reindex(sourceIndex, targetIndex, refresh)
  }

  override def indexExists(index: String): Boolean = {
    delegate.indexExists(index)
  }

  // ==================== AliasApi ====================

  override def addAlias(index: String, alias: String): Boolean = {
    delegate.addAlias(index, alias)
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    delegate.removeAlias(index, alias)
  }

  // ==================== SettingsApi ====================

  override def updateSettings(index: String, settings: String): Boolean = {
    delegate.updateSettings(index, settings)
  }

  override def loadSettings(index: String): String = {
    delegate.loadSettings(index)
  }

  // ==================== MappingApi ====================

  override def setMapping(index: String, mapping: String): Boolean = {
    delegate.setMapping(index, mapping)
  }

  override def getMapping(index: String): String = {
    delegate.getMapping(index)
  }

  override def getMappingProperties(index: String): String = {
    delegate.getMappingProperties(index)
  }

  // ==================== RefreshApi ====================

  override def refresh(index: String): Boolean = {
    delegate.refresh(index)
  }

  // ==================== FlushApi ====================

  override def flush(index: String, force: Boolean, wait: Boolean): Boolean = {
    delegate.flush(index, force, wait)
  }

  // ==================== IndexApi ====================

  override def index(index: String, id: String, source: String): Boolean = {
    delegate.index(index, id, source)
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    delegate.indexAsync(index, id, source)
  }

  // ==================== UpdateApi ====================

  override def update(index: String, id: String, source: String, upsert: Boolean): Boolean = {
    delegate.update(index, id, source, upsert)
  }

  override def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    delegate.updateAsync(index, id, source, upsert)
  }

  // ==================== DeleteApi ====================

  override def delete(id: String, index: String): Boolean = {
    delegate.delete(id, index)
  }

  override def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    delegate.deleteAsync(id, index)
  }

  // ==================== GetApi ====================

  override def get[U <: AnyRef](
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    delegate.get[U](id, index, maybeType)
  }

  // ==================== CountApi ====================

  override def count(query: ElasticQuery): Option[Double] = {
    delegate.count(query)
  }

  // ==================== SearchApi ====================

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): JSONResults =
    delegate.sqlSearchRequestToJsonQuery(sqlSearch)

  override def search(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    delegate.search(elasticQuery, fieldAliases, aggregations)
  }

  override def multisearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    delegate.multisearch(elasticQueries, fieldAliases, aggregations)
  }

  override def searchWithInnerHits[U, I](elasticQuery: ElasticQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    delegate.searchWithInnerHits[U, I](elasticQuery, innerField)
  }

  override def multisearchWithInnerHits[U, I](elasticQueries: ElasticQueries, innerField: String)(
    implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    delegate.multisearchWithInnerHits[U, I](elasticQueries, innerField)
  }

  // ==================== ScrollApi ====================

  override def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    delegate.scrollClassic(elasticQuery, fieldAliases, aggregations, config)
  }

  override def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    delegate.searchAfter(elasticQuery, fieldAliases, config, hasSorts)
  }

  // ==================== BulkApi ====================

  override def toBulkAction(bulkItem: BulkItem): BulkActionType =
    delegate.toBulkAction(bulkItem).asInstanceOf[BulkActionType]

  override implicit def toBulkElasticAction(a: BulkActionType): BulkElasticAction =
    delegate.toBulkElasticAction(a.asInstanceOf)

  override implicit def toBulkElasticResult(r: BulkResultType): BulkElasticResult =
    delegate.toBulkElasticResult(r.asInstanceOf)

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed] = {
    // Pour les flows, on délègue directement
    delegate.bulk.asInstanceOf[Flow[Seq[BulkActionType], BulkResultType, NotUsed]]
  }

  override def bulkResult: Flow[BulkResultType, Set[String], NotUsed] = {
    delegate.bulkResult.asInstanceOf[Flow[BulkResultType, Set[String], NotUsed]]
  }

  override def bulk[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String]
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Set[String] = {
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
