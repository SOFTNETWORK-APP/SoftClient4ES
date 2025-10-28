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

import app.softnetwork.elastic.client._
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import java.util.ServiceLoader
import java.util.concurrent.ConcurrentHashMap
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** Factory for creating Elasticsearch clients with optional metrics and monitoring.
  *
  * This factory uses the Service Provider Interface (SPI) pattern to load Elasticsearch client
  * implementations and provides caching to avoid creating multiple instances for the same
  * configuration.
  *
  * Thread-safe implementation using ConcurrentHashMap.
  */
object ElasticClientFactory {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private[this] val factories: ServiceLoader[ElasticClientSpi] =
    ServiceLoader.load(classOf[ElasticClientSpi])

  // Use String key (URL) instead of Config for reliable caching
  private[this] val clientsByUrl = new ConcurrentHashMap[String, ElasticClientApi]()
  private[this] val metricsClientsByUrl = new ConcurrentHashMap[String, MetricsElasticClient]()
  private[this] val monitoredClientsByUrl = new ConcurrentHashMap[String, MonitoredElasticClient]()

  // Shutdown hook to close all clients
  sys.addShutdownHook {
    logger.info("JVM shutdown detected, closing all Elasticsearch clients")
    shutdown()
  }

  /** Gets or creates a base Elasticsearch client for the given configuration.
    *
    * @param config
    *   Typesafe configuration
    * @return
    *   Base Elasticsearch client
    */
  private[this] def getOrCreateBaseClient(config: Config): ElasticClientApi = {
    val elasticConfig = ElasticConfig(config)
    val url = elasticConfig.credentials.url

    clientsByUrl.computeIfAbsent(
      url,
      _ => {
        logger.info(s"Creating new Elasticsearch client for URL: $url")
        factories
          .iterator()
          .asScala
          .map(_.client(config))
          .toSeq
          .headOption
          .getOrElse(throw new IllegalStateException("No ElasticClientSpi implementation found"))
      }
    )
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
    * @return
    *   Configured Elasticsearch client
    *
    * @example
    * {{{
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
  def create(config: Config = ConfigFactory.load()): ElasticClientApi = {
    val elasticConfig = ElasticConfig(config)

    if (elasticConfig.metrics.enabled) {
      val monitoringConfig = elasticConfig.metrics.monitoring
      if (monitoringConfig.enabled) {
        createWithMonitoring(config)
      } else {
        createWithMetrics(config)
      }
    } else {
      getOrCreateBaseClient(config)
    }
  }

  /** Creates a client with explicitly enabled metrics, regardless of configuration.
    *
    * Uses caching: multiple calls with the same URL return the same instance.
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
    * }}}
    */
  def createWithMetrics(config: Config = ConfigFactory.load()): MetricsElasticClient = {
    val elasticConfig = ElasticConfig(config)
    val url = elasticConfig.credentials.url

    metricsClientsByUrl.computeIfAbsent(
      url,
      _ => {
        logger.info(s"Creating new MetricsElasticClient for URL: $url")
        val baseClient = getOrCreateBaseClient(config)
        val metricsCollector = new MetricsCollector()
        new MetricsElasticClient(baseClient, metricsCollector)
      }
    )
  }

  /** Creates a client with a custom metrics collector.
    *
    * Useful for sharing a collector between multiple clients or for testing. Does NOT use caching -
    * always creates a new client instance.
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
    config: Config = ConfigFactory.load(),
    metricsCollector: MetricsCollector
  ): MetricsElasticClient = {
    logger.info("Creating new MetricsElasticClient with custom collector")
    val baseClient = getOrCreateBaseClient(config)
    new MetricsElasticClient(baseClient, metricsCollector)
  }

  /** Creates a client with automatic monitoring and alerting.
    *
    * Monitoring generates periodic reports and triggers alerts when configured thresholds are
    * exceeded. Uses caching: multiple calls with the same URL return the same instance.
    *
    * @param config
    *   Typesafe configuration
    * @return
    *   MonitoredElasticClient with active monitoring
    *
    * @example
    * {{{
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
    * }}}
    */
  def createWithMonitoring(config: Config = ConfigFactory.load()): MonitoredElasticClient = {
    val elasticConfig = ElasticConfig(config)
    val url = elasticConfig.credentials.url

    monitoredClientsByUrl.computeIfAbsent(
      url,
      _ => {
        logger.info(s"Creating new MonitoredElasticClient for URL: $url")
        val baseClient = getOrCreateBaseClient(config)
        val metricsCollector = new MetricsCollector()
        val monitoringConfig = elasticConfig.metrics.monitoring
        new MonitoredElasticClient(baseClient, metricsCollector, monitoringConfig)
      }
    )
  }

  /** Creates a monitored client with a custom metrics collector.
    *
    * Does NOT use caching - always creates a new client instance.
    *
    * @param config
    *   Typesafe configuration
    * @param metricsCollector
    *   Custom metrics collector
    * @return
    *   MonitoredElasticClient using the provided collector
    */
  def createMonitoredWithCustomMetrics(
    config: Config = ConfigFactory.load(),
    metricsCollector: MetricsCollector
  ): MonitoredElasticClient = {
    logger.info("Creating new MonitoredElasticClient with custom collector")
    val elasticConfig = ElasticConfig(config)
    val baseClient = getOrCreateBaseClient(config)
    val monitoringConfig = elasticConfig.metrics.monitoring
    new MonitoredElasticClient(baseClient, metricsCollector, monitoringConfig)
  }

  /** Shuts down all cached clients.
    *
    * This method should be called when the application terminates. It's automatically called via a
    * JVM shutdown hook.
    */
  def shutdown(): Unit = {
    logger.info("Shutting down all Elasticsearch clients")

    // Shutdown monitored clients first (they have schedulers)
    monitoredClientsByUrl.values().asScala.foreach { client =>
      try {
        client.shutdown()
      } catch {
        case NonFatal(ex) =>
          logger.error(s"Error shutting down monitored client: ${ex.getMessage}", ex)
      }
    }

    // Clear caches
    monitoredClientsByUrl.clear()
    metricsClientsByUrl.clear()

    // Shutdown base clients
    clientsByUrl.values().asScala.foreach { client =>
      try {
        client.close()
      } catch {
        case NonFatal(ex) =>
          logger.error(s"Error shutting down base client: ${ex.getMessage}", ex)
      }
    }

    clientsByUrl.clear()

    logger.info("All Elasticsearch clients shut down")
  }

  /** Clears all caches without shutting down clients.
    *
    * Useful for testing. Use with caution in production.
    */
  def clearCache(): Unit = {
    logger.warn("Clearing Elasticsearch client cache")
    clientsByUrl.clear()
    metricsClientsByUrl.clear()
    monitoredClientsByUrl.clear()
  }

  /** Gets statistics about cached clients.
    *
    * @return
    *   Map with cache statistics
    */
  def getCacheStats: Map[String, Int] = {
    Map(
      "baseClients"      -> clientsByUrl.size(),
      "metricsClients"   -> metricsClientsByUrl.size(),
      "monitoredClients" -> monitoredClientsByUrl.size()
    )
  }
}
