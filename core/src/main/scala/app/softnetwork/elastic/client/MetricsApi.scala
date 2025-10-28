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
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLSearchRequest}
import org.json4s.Formats

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import scala.concurrent.{ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions
import scala.util.{Failure, Success}

trait MetricsApi {

  /** Records an operation with its metrics.
    *
    * This method is called automatically by the decorator, but can also be used manually for custom
    * operations.
    *
    * @param operation
    *   Operation name (e.g., "search", "index")
    * @param duration
    *   Operation duration in milliseconds
    * @param success
    *   Indicates if the operation succeeded
    * @param index
    *   Elasticsearch index involved (optional)
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Manual recording
    * val start = System.currentTimeMillis()
    * try {
    *   // Custom operation
    *   customElasticsearchOperation()
    *   val duration = System.currentTimeMillis() - start
    *   client.recordOperation("customOp", duration, success = true, Some("myindex"))
    * } catch {
    *   case e: Exception =>
    *     val duration = System.currentTimeMillis() - start
    *     client.recordOperation("customOp", duration, success = false, Some("myindex"))
    * }
    * }}}
    */
  def recordOperation(
    operation: String,
    duration: Long,
    success: Boolean,
    index: Option[String] = None
  ): Unit

  /** Retrieves aggregated global metrics.
    *
    * @return
    *   Global metrics for all operations
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Perform operations
    * client.createIndex("test")
    * client.index("test", "1", """{"data": "value"}""")
    *
    * // Retrieve metrics
    * val metrics = client.getMetrics
    *
    * println(s"""
    *   |Total operations: ${metrics.totalOperations}
    *   |Successful: ${metrics.successCount}
    *   |Failed: ${metrics.failureCount}
    *   |Success rate: ${metrics.successRate}%
    *   |Failure rate: ${metrics.failureRate}%
    *   |Average duration: ${metrics.averageDuration}ms
    *   |Min duration: ${metrics.minDuration}ms
    *   |Max duration: ${metrics.maxDuration}ms
    *   |Last execution: ${new Date(metrics.lastExecutionTime)}
    * """.stripMargin)
    * }}}
    */
  def getMetrics: OperationMetrics

  /** Retrieves metrics for a specific operation.
    *
    * @param operation
    *   Operation name
    * @return
    *   Operation metrics, or None if no data
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Perform multiple searches
    * (1 to 100).foreach { i =>
    *   val query = ElasticQuery(indices = Seq("products"))
    *   client.search(query, Map.empty, Map.empty)
    * }
    *
    * // Analyze search performance
    * client.getMetricsByOperation("search").foreach { metrics =>
    *   println(s"""
    *     |=== Search Performance ===
    *     |Total searches: ${metrics.totalOperations}
    *     |Success rate: ${metrics.successRate}%
    *     |Average latency: ${metrics.averageDuration}ms
    *     |Fastest: ${metrics.minDuration}ms
    *     |Slowest: ${metrics.maxDuration}ms
    *     |
    *     |Performance grade: ${
    *       if (metrics.averageDuration < 100) "Excellent"
    *       else if (metrics.averageDuration < 500) "Good"
    *       else if (metrics.averageDuration < 1000) "Average"
    *       else "Needs optimization"
    *     }
    *   """.stripMargin)
    * }
    * }}}
    */
  def getMetricsByOperation(operation: String): Option[OperationMetrics]

  /** Retrieves metrics for a specific index.
    *
    * @param index
    *   Index name
    * @return
    *   Index metrics, or None if no data
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Operations on different indexes
    * client.index("products", "1", """{"name": "Product 1"}""")
    * client.index("products", "2", """{"name": "Product 2"}""")
    * client.index("orders", "1", """{"total": 100}""")
    *
    * // Metrics by index
    * client.getMetricsByIndex("products").foreach { metrics =>
    *   println(s"""
    *     |Products Index:
    *     |  Operations: ${metrics.totalOperations}
    *     |  Avg duration: ${metrics.averageDuration}ms
    *   """.stripMargin)
    * }
    *
    * client.getMetricsByIndex("orders").foreach { metrics =>
    *   println(s"""
    *     |Orders Index:
    *     |  Operations: ${metrics.totalOperations}
    *     |  Avg duration: ${metrics.averageDuration}ms
    *   """.stripMargin)
    * }
    *
    * // Compare performance
    * val productsPerf = client.getMetricsByIndex("products").map(_.averageDuration).getOrElse(0.0)
    * val ordersPerf = client.getMetricsByIndex("orders").map(_.averageDuration).getOrElse(0.0)
    *
    * if (productsPerf > ordersPerf * 2) {
    *   println("⚠️  Products index is significantly slower than orders")
    * }
    * }}}
    */
  def getMetricsByIndex(index: String): Option[OperationMetrics]

  /** Retrieves all aggregated metrics with details by operation and by index.
    *
    * @return
    *   Complete metrics with breakdowns
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Perform various operations
    * client.createIndex("test1")
    * client.createIndex("test2")
    * client.index("test1", "1", """{"data": 1}""")
    * client.index("test2", "1", """{"data": 2}""")
    * client.search(ElasticQuery(Seq("test1")), Map.empty, Map.empty)
    *
    * // Complete report
    * val aggregated = client.getAggregatedMetrics
    *
    * println(s"""
    *   |=== Global Report ===
    *   |Total operations: ${aggregated.totalOperations}
    *   |Success rate: ${aggregated.successRate}%
    *   |Average duration: ${aggregated.averageDuration}ms
    *   |
    *   |=== By Operation ===
    *   |${aggregated.operationMetrics.map { case (op, m) =>
    *     s"$op: ${m.totalOperations} ops, ${m.averageDuration}ms avg, ${m.successRate}% success"
    *   }.mkString("\n")}
    *   |
    *   |=== By Index ===
    *   |${aggregated.indexMetrics.map { case (idx, m) =>
    *     s"$idx: ${m.totalOperations} ops, ${m.averageDuration}ms avg"
    *   }.mkString("\n")}
    * """.stripMargin)
    * }}}
    */
  def getAggregatedMetrics: AggregatedMetrics

  /** Resets all collected metrics.
    *
    * Useful for starting a new measurement period or after a test.
    *
    * @example
    * {{{
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Warmup phase
    * (1 to 100).foreach { i =>
    *   client.index("test", s"$i", s"""{"value": $i}""")
    * }
    *
    * // Reset before real measurements
    * client.resetMetrics()
    *
    * // Real measurements
    * val start = System.currentTimeMillis()
    * (1 to 1000).foreach { i =>
    *   client.index("test", s"real_$i", s"""{"value": $i}""")
    * }
    * val end = System.currentTimeMillis()
    *
    * // Analyze clean metrics
    * val metrics = client.getMetrics
    * println(s"Pure indexing performance: ${metrics.averageDuration}ms per doc")
    * }}}
    */
  def resetMetrics(): Unit
}

case class OperationMetrics(
  operation: String,
  totalOperations: Long,
  successCount: Long,
  failureCount: Long,
  totalDuration: Long,
  minDuration: Long,
  maxDuration: Long,
  lastExecutionTime: Long
) {
  def averageDuration: Double =
    if (totalOperations > 0) totalDuration.toDouble / totalOperations else 0.0

  def successRate: Double =
    if (totalOperations > 0) (successCount.toDouble / totalOperations) * 100 else 0.0

  def failureRate: Double = 100.0 - successRate
}

case class AggregatedMetrics(
  totalOperations: Long,
  successCount: Long,
  failureCount: Long,
  totalDuration: Long,
  operationMetrics: Map[String, OperationMetrics],
  indexMetrics: Map[String, OperationMetrics]
) {
  def averageDuration: Double =
    if (totalOperations > 0) totalDuration.toDouble / totalOperations else 0.0

  def successRate: Double =
    if (totalOperations > 0) (successCount.toDouble / totalOperations) * 100 else 0.0
}

class MetricsCollector extends MetricsApi {

  private val metrics = new ConcurrentHashMap[String, MetricAccumulator]()
  private val indexMetrics = new ConcurrentHashMap[String, MetricAccumulator]()

  private class MetricAccumulator {
    val totalOps = new AtomicLong(0)
    val successOps = new AtomicLong(0)
    val failureOps = new AtomicLong(0)
    val totalDuration = new AtomicLong(0)
    val minDuration = new AtomicLong(Long.MaxValue)
    val maxDuration = new AtomicLong(Long.MinValue)
    val lastExecution = new AtomicLong(0)

    /** Records an operation with its duration and success status. Thread-safe implementation using
      * atomic operations.
      *
      * @param duration
      *   Operation duration in milliseconds
      * @param success
      *   Whether the operation succeeded
      */
    def record(duration: Long, success: Boolean): Unit = {
      // Update counters
      totalOps.incrementAndGet()
      if (success) successOps.incrementAndGet() else failureOps.incrementAndGet()
      totalDuration.addAndGet(duration)
      lastExecution.set(System.currentTimeMillis())

      // Update min duration using atomic operation
      minDuration.updateAndGet(current => Math.min(current, duration))

      // Update max duration using atomic operation
      maxDuration.updateAndGet(current => Math.max(current, duration))
    }

    /** Converts accumulated metrics to an OperationMetrics object.
      *
      * @param operation
      *   The operation name
      * @return
      *   OperationMetrics snapshot
      */
    def toMetrics(operation: String): OperationMetrics = {
      val min = minDuration.get()
      val max = maxDuration.get()

      OperationMetrics(
        operation = operation,
        totalOperations = totalOps.get(),
        successCount = successOps.get(),
        failureCount = failureOps.get(),
        totalDuration = totalDuration.get(),
        minDuration = if (min == Long.MaxValue) 0 else min,
        maxDuration = if (max == Long.MinValue) 0 else max,
        lastExecutionTime = lastExecution.get()
      )
    }

    /** Resets all metrics to initial values. Useful for testing or periodic metric resets.
      */
    def reset(): Unit = {
      totalOps.set(0)
      successOps.set(0)
      failureOps.set(0)
      totalDuration.set(0)
      minDuration.set(Long.MaxValue)
      maxDuration.set(Long.MinValue)
      lastExecution.set(0)
    }
  }

  override def recordOperation(
    operation: String,
    duration: Long,
    success: Boolean,
    index: Option[String] = None
  ): Unit = {
    // Record operation metrics
    val accumulator = metrics.computeIfAbsent(operation, _ => new MetricAccumulator())
    accumulator.record(duration, success)

    // Record index metrics if provided
    index.foreach { idx =>
      val idxAccumulator = indexMetrics.computeIfAbsent(idx, _ => new MetricAccumulator())
      idxAccumulator.record(duration, success)
    }
  }

  override def getMetrics: OperationMetrics = {
    val allMetrics = metrics.asScala.values.toSeq

    if (allMetrics.isEmpty) {
      OperationMetrics("all", 0, 0, 0, 0, 0, 0, 0)
    } else {
      OperationMetrics(
        operation = "all",
        totalOperations = allMetrics.map(_.totalOps.get()).sum,
        successCount = allMetrics.map(_.successOps.get()).sum,
        failureCount = allMetrics.map(_.failureOps.get()).sum,
        totalDuration = allMetrics.map(_.totalDuration.get()).sum,
        minDuration =
          allMetrics.map(_.minDuration.get()).filter(_ != Long.MaxValue).minOption.getOrElse(0),
        maxDuration = allMetrics.map(_.maxDuration.get()).max,
        lastExecutionTime = allMetrics.map(_.lastExecution.get()).max
      )
    }
  }

  override def getMetricsByOperation(operation: String): Option[OperationMetrics] = {
    Option(metrics.get(operation)).map(_.toMetrics(operation))
  }

  override def getMetricsByIndex(index: String): Option[OperationMetrics] = {
    Option(indexMetrics.get(index)).map(_.toMetrics(index))
  }

  override def getAggregatedMetrics: AggregatedMetrics = {
    val globalMetrics = getMetrics
    AggregatedMetrics(
      totalOperations = globalMetrics.totalOperations,
      successCount = globalMetrics.successCount,
      failureCount = globalMetrics.failureCount,
      totalDuration = globalMetrics.totalDuration,
      operationMetrics = metrics.asScala.map { case (op, acc) =>
        op -> acc.toMetrics(op)
      }.toMap,
      indexMetrics = indexMetrics.asScala.map { case (idx, acc) =>
        idx -> acc.toMetrics(idx)
      }.toMap
    )
  }

  override def resetMetrics(): Unit = {
    metrics.clear()
    indexMetrics.clear()
  }
}

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

  // Delegate the logger to the underlying client
  // protected lazy val logger: Logger = LoggerFactory getLogger getClass.getName

  // Delegate config to the underlying client
  //override def config: Config = delegate.config

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
