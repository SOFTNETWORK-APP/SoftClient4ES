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

import scala.language.implicitConversions

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
