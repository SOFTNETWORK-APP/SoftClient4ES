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
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client.{
  ElasticClientApi,
  ElasticClientDelegator,
  ElasticQueries,
  ElasticQuery,
  ElasticResponse,
  ElasticResult,
  JSONResults
}
import app.softnetwork.elastic.client.bulk.BulkOptions
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLSearchRequest}
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future}
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
  private def measure[T](operation: String, index: Option[String] = None)(block: => T): T = {
    val startTime = System.currentTimeMillis()
    try {
      val result = block
      val duration = System.currentTimeMillis() - startTime
      result match {
        case opt: Option[_] if opt.isEmpty =>
          metricsCollector.recordOperation(operation, duration, success = false, index)
        case b: Boolean =>
          metricsCollector.recordOperation(operation, duration, success = b, index)
        case e: ElasticResult[_] =>
          metricsCollector.recordOperation(operation, duration, success = e.success, index)
        case _ =>
          metricsCollector.recordOperation(operation, duration, success = true, index)
      }
      result
    } catch {
      case ex: Exception =>
        val duration = System.currentTimeMillis() - startTime
        metricsCollector.recordOperation(operation, duration, success = false, index)
        throw ex
    }
  }

  private def measureBoolean(operation: String, index: Option[String] = None)(
    block: => Boolean
  ): Boolean = {
    val startTime = System.currentTimeMillis()
    val result =
      try {
        block
      } catch {
        case ex: Exception =>
          val duration = System.currentTimeMillis() - startTime
          metricsCollector.recordOperation(operation, duration, success = false, index)
          throw ex
      }
    val duration = System.currentTimeMillis() - startTime
    metricsCollector.recordOperation(operation, duration, success = result, index)
    result
  }

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

  // ==================== IndicesApi ====================

  override def createIndex(index: String, settings: String): Boolean = {
    measureBoolean("createIndex", Some(index)) {
      delegate.createIndex(index, settings)
    }
  }

  override def deleteIndex(index: String): Boolean = {
    measureBoolean("deleteIndex", Some(index)) {
      delegate.deleteIndex(index)
    }
  }

  override def closeIndex(index: String): Boolean = {
    measureBoolean("closeIndex", Some(index)) {
      delegate.closeIndex(index)
    }
  }

  override def openIndex(index: String): Boolean = {
    measureBoolean("openIndex", Some(index)) {
      delegate.openIndex(index)
    }
  }

  override def reindex(sourceIndex: String, targetIndex: String, refresh: Boolean): Boolean = {
    measureBoolean("reindex", Some(s"$sourceIndex->$targetIndex")) {
      delegate.reindex(sourceIndex, targetIndex, refresh)
    }
  }

  override def indexExists(index: String): Boolean = {
    measureBoolean("indexExists", Some(index)) {
      delegate.indexExists(index)
    }
  }

  // ==================== AliasApi ====================

  override def addAlias(index: String, alias: String): Boolean = {
    measureBoolean("addAlias", Some(index)) {
      delegate.addAlias(index, alias)
    }
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    measureBoolean("removeAlias", Some(index)) {
      delegate.removeAlias(index, alias)
    }
  }

  // ==================== SettingsApi ====================

  override def updateSettings(index: String, settings: String): Boolean = {
    measureBoolean("updateSettings", Some(index)) {
      delegate.updateSettings(index, settings)
    }
  }

  override def loadSettings(index: String): String = {
    measure("loadSettings", Some(index)) {
      delegate.loadSettings(index)
    }
  }

  // ==================== MappingApi ====================

  override def setMapping(index: String, mapping: String): Boolean = {
    measureBoolean("setMapping", Some(index)) {
      delegate.setMapping(index, mapping)
    }
  }

  override def getMapping(index: String): String = {
    measure("getMapping", Some(index)) {
      delegate.getMapping(index)
    }
  }

  // ==================== RefreshApi ====================

  override def refresh(index: String): Boolean = {
    measureBoolean("refresh", Some(index)) {
      delegate.refresh(index)
    }
  }

  // ==================== FlushApi ====================

  override def flush(index: String, force: Boolean, wait: Boolean): Boolean = {
    measureBoolean("flush", Some(index)) {
      delegate.flush(index, force, wait)
    }
  }

  // ==================== IndexApi ====================

  override def index(index: String, id: String, source: String): Boolean = {
    measureBoolean("index", Some(index)) {
      delegate.index(index, id, source)
    }
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    measureAsync("indexAsync", Some(index)) {
      delegate.indexAsync(index, id, source)
    }
  }

  // ==================== UpdateApi ====================

  override def update(index: String, id: String, source: String, upsert: Boolean): Boolean = {
    measureBoolean("update", Some(index)) {
      delegate.update(index, id, source, upsert)
    }
  }

  override def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    measureAsync("updateAsync", Some(index)) {
      delegate.updateAsync(index, id, source, upsert)
    }
  }

  // ==================== DeleteApi ====================

  override def delete(id: String, index: String): Boolean = {
    measureBoolean("delete", Some(index)) {
      delegate.delete(id, index)
    }
  }

  override def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    measureAsync("deleteAsync", Some(index)) {
      delegate.deleteAsync(id, index)
    }
  }

  // ==================== GetApi ====================

  override def get[U <: AnyRef](
    id: String,
    index: Option[String],
    maybeType: Option[String]
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    measure("get", index) {
      delegate.get[U](id, index, maybeType)
    }
  }

  // ==================== CountApi ====================

  override def count(query: ElasticQuery): Option[Double] = {
    measure("count", Some(query.indices.mkString(","))) {
      delegate.count(query)
    }
  }

  // ==================== SearchApi ====================

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): JSONResults =
    delegate.sqlSearchRequestToJsonQuery(sqlSearch)

  override def search(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    measure("search", Some(elasticQuery.indices.mkString(","))) {
      delegate.search(elasticQuery, fieldAliases, aggregations)
    }
  }

  override def multisearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    measure("multisearch") {
      delegate.multisearch(elasticQueries, fieldAliases, aggregations)
    }
  }

  override def searchWithInnerHits[U, I](elasticQuery: ElasticQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    measure("searchWithInnerHits", Some(elasticQuery.indices.mkString(","))) {
      delegate.searchWithInnerHits[U, I](elasticQuery, innerField)
    }
  }

  override def multisearchWithInnerHits[U, I](elasticQueries: ElasticQueries, innerField: String)(
    implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    measure("multisearchWithInnerHits") {
      delegate.multisearchWithInnerHits[U, I](elasticQueries, innerField)
    }
  }

  // ==================== ScrollApi ====================

  override def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    // Note: For streams, we measure at the beginning but not every element
    val startTime = System.currentTimeMillis()
    val source = delegate.scrollClassic(elasticQuery, fieldAliases, aggregations, config)

    source.watchTermination() { (_, done) =>
      done.onComplete { result =>
        val duration = System.currentTimeMillis() - startTime
        val success = result.isSuccess
        metricsCollector.recordOperation(
          "scrollClassic",
          duration,
          success,
          Some(elasticQuery.indices.mkString(","))
        )
      }(system.dispatcher)
      NotUsed
    }
  }

  override def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    val startTime = System.currentTimeMillis()
    val source = delegate.searchAfter(elasticQuery, fieldAliases, config, hasSorts)

    source.watchTermination() { (_, done) =>
      done.onComplete { result =>
        val duration = System.currentTimeMillis() - startTime
        val success = result.isSuccess
        metricsCollector.recordOperation(
          "searchAfter",
          duration,
          success,
          Some(elasticQuery.indices.mkString(","))
        )
      }(system.dispatcher)
      NotUsed
    }
  }

  // ==================== BulkApi ====================

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
    measure("bulk", Some(bulkOptions.index)) {
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
