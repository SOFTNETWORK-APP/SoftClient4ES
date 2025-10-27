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

package app.softnetwork.elastic.client.spi

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.{
  ElasticClientApi,
  ElasticConfig,
  MetricsCollector,
  MetricsElasticClient,
  MonitoredElasticClient
}
import com.typesafe.config.Config

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._

object ElasticClientFactory {

  private[this] val factories: ServiceLoader[ElasticClientSpi] =
    ServiceLoader.load(classOf[ElasticClientSpi])

  private[this] def clients(config: Config): Seq[ElasticClientApi] = {
    factories
      .iterator()
      .asScala
      .map(_.client(config))
      .toSeq
  }

  private[this] def client(config: Config): ElasticClientApi = {
    clients(config).head
  }

  /** Creates an Elasticsearch client with optional metrics and monitoring.
    *
    * The returned client type depends on configuration:
    *   - If metrics.enabled = false: Base client without metrics
    *   - If metrics.enabled = true and monitoring.enabled = false: Client with metrics
    *   - If metrics.enabled = true and monitoring.enabled = true: Client with metrics and
    *     monitoring
    *
    * @param config
    *   Typesafe configuration
    * @param system
    *   Akka ActorSystem for asynchronous operations
    * @return
    *   Configured Elasticsearch client
    *
    * @example
    * {{{
    * implicit val system: ActorSystem = ActorSystem("my-system")
    * val config = ConfigFactory.load()
    *
    * // Client created according to configuration
    * val client = ElasticClientFactory.create(config)
    *
    * // Normal usage
    * client.createIndex("products")
    * client.index("products", "123", """{"name": "Product"}""")
    *
    * // Access metrics if enabled
    * client match {
    *   case metricsClient: MetricsElasticClient =>
    *     val metrics = metricsClient.getMetrics
    *     println(s"Operations: ${metrics.totalOperations}")
    *   case _ => println("Metrics not enabled")
    * }
    * }}}
    */
  def create(config: Config)(implicit system: ActorSystem): ElasticClientApi = {
    val baseClient = client(config)

    val elasticConfig = ElasticConfig(config)

    if (elasticConfig.metrics.enabled) {
      val metricsCollector = new MetricsCollector()
      val monitoringConfig = elasticConfig.metrics.monitoring
      if (monitoringConfig.enabled) {
        // Start automatic monitoring
        new MonitoredElasticClient(client(config), metricsCollector, monitoringConfig)
      } else {
        new MetricsElasticClient(baseClient, metricsCollector)
      }
    } else {
      baseClient
    }
  }

  /** Creates a client with explicitly enabled metrics, regardless of configuration.
    *
    * @param config
    *   Typesafe configuration
    * @return
    *   MetricsElasticClient with active metrics collection
    *
    * @example
    * {{{
    * val config = ConfigFactory.load()
    * val client = ElasticClientFactory.createWithMetrics(config)
    *
    * // Operations
    * client.createIndex("logs")
    * client.index("logs", "1", """{"message": "test"}""")
    *
    * // Metrics always available
    * val metrics = client.getMetrics
    * println(s"Total ops: ${metrics.totalOperations}")
    *
    * // Metrics by operation
    * client.getMetricsByOperation("index").foreach { m =>
    *   println(s"Index ops: ${m.totalOperations}")
    *   println(s"Avg duration: ${m.averageDuration}ms")
    * }
    *
    * // Metrics by index
    * client.getMetricsByIndex("logs").foreach { m =>
    *   println(s"Logs ops: ${m.totalOperations}")
    * }
    * }}}
    */
  def createWithMetrics(config: Config): MetricsElasticClient = {
    val metricsCollector = new MetricsCollector()
    new MetricsElasticClient(client(config), metricsCollector)
  }

  /** Creates a client with a custom metrics collector. Useful for sharing a collector between
    * multiple clients.
    *
    * @param config
    *   Typesafe configuration
    * @param metricsCollector
    *   Custom metrics collector
    * @return
    *   MetricsElasticClient using the provided collector
    *
    * @example
    * {{{
    * val config = ConfigFactory.load()
    * val sharedCollector = new MetricsCollector()
    *
    * // Multiple clients sharing the same collector
    * val client1 = ElasticClientFactory.createWithCustomMetrics(config, sharedCollector)
    * val client2 = ElasticClientFactory.createWithCustomMetrics(config, sharedCollector)
    *
    * // Operations on both clients
    * client1.createIndex("index1")
    * client2.createIndex("index2")
    *
    * // Global metrics for both clients
    * val metrics = sharedCollector.getMetrics
    * println(s"Total ops across all clients: ${metrics.totalOperations}")
    * }}}
    */
  def createWithCustomMetrics(
    config: Config,
    metricsCollector: MetricsCollector
  ): MetricsElasticClient = {
    new MetricsElasticClient(client(config), metricsCollector)
  }

  /** Creates a client with automatic monitoring and alerting.
    *
    * Monitoring generates periodic reports and triggers alerts when configured thresholds are
    * exceeded.
    *
    * @param config
    *   Typesafe configuration
    * @param system
    *   Akka ActorSystem for monitoring scheduling
    * @return
    *   MonitoredElasticClient with active monitoring
    *
    * @example
    * {{{
    * implicit val system: ActorSystem = ActorSystem("monitoring-system")
    * val config = ConfigFactory.load()
    *
    * val client = ElasticClientFactory.createWithMonitoring(config)
    *
    * // Monitoring starts automatically
    * // Logs every 30s (according to config):
    * // === Elasticsearch Metrics ===
    * // Total Operations: 150
    * // Success Rate: 98.5%
    * // Average Duration: 45ms
    * // =============================
    *
    * // Normal operations
    * client.createIndex("monitored-index")
    *
    * // If failure rate > threshold, automatic alert:
    * // ⚠️  HIGH FAILURE RATE: 15.0%
    *
    * // Stop monitoring gracefully
    * client.shutdown()
    * system.terminate()
    * }}}
    */
  def createWithMonitoring(
    config: Config
  )(implicit system: ActorSystem): MonitoredElasticClient = {
    val elasticConfig = ElasticConfig(config)

    val metricsCollector = new MetricsCollector()
    new MonitoredElasticClient(client(config), metricsCollector, elasticConfig.metrics.monitoring)
  }

}
