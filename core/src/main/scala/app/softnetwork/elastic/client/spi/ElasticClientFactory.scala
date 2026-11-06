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
import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import app.softnetwork.elastic.client.monitoring.MonitoredElasticClient
import com.typesafe.config.{Config, ConfigFactory}
import org.slf4j.{Logger, LoggerFactory}

import java.util.ServiceLoader
import scala.jdk.CollectionConverters._
import scala.sys.ShutdownHookThread

/** Factory for creating Elasticsearch clients with optional metrics and monitoring.
  *
  * This factory uses the Service Provider Interface (SPI) pattern to load Elasticsearch client
  * implementations, and caches the clients it builds:
  *
  *   - Two callers share a cached client only when their effective `elastic.*` configurations are
  *     equal (credentials, timeouts, scroll and schema-cache settings, every `elastic.*` value): a
  *     caller never runs with another caller's identity.
  *   - Call `close()` once per `create...()`: it releases this caller's share and never closes a
  *     client, or stops a monitor, that another caller still holds.
  *   - A client nobody holds stays cached for 10 minutes, so a caller that comes back reuses it;
  *     after that it is closed at the next client creation, or by [[shutdown]].
  *
  * Thread-safe.
  */
object ElasticClientFactory {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  // #258: resolve providers against the classloader that loaded this library, never the thread
  // context classloader. This val is latched at class initialisation, so a single-arg load froze
  // whichever TCCL the first caller happened to carry - under a host-owned blind loader (Tableau,
  // plugin containers, app servers) that meant a permanent and misleading "No ElasticClientSpi
  // implementation found" although the provider sat right beside this class on the classpath.
  private[this] val factories: ServiceLoader[ElasticClientSpi] =
    ServiceLoader.load(classOf[ElasticClientSpi], classOf[ElasticClientSpi].getClassLoader)

  // The clients are cached by their effective `elastic.*` configuration (ClientKey), never by URL
  // alone: a URL key handed a second user the first user's client, credentials included.
  private[this] val cache = new ElasticClientCache((config: Config) => buildClient(config))

  // Shutdown hook to close all clients. Named so that a test which defines a fresh copy of this
  // object (ElasticClientFactoryIsolationSpec, #258) can remove the hook that copy registered.
  private[spi] val shutdownHook: ShutdownHookThread = sys.addShutdownHook {
    logger.info("JVM shutdown detected, closing all Elasticsearch clients")
    shutdown()
  }

  /** Builds a new client through the first provider. A `ServiceLoader` is not safe for concurrent
    * iteration, and clients for different configurations may now be built at the same time.
    *
    * @param config
    *   Typesafe configuration
    * @return
    *   a new base Elasticsearch client
    */
  private[this] def buildClient(config: Config): ElasticClientApi = factories.synchronized {
    factories
      .iterator()
      .asScala
      .map(_.client(config))
      .toSeq
      .headOption
      .getOrElse(
        // The leading substring is pinned by the jdbc/arrow isolation specs - append, never replace.
        throw new IllegalStateException(
          "No ElasticClientSpi implementation found through " +
          s"${classOf[ElasticClientSpi].getClassLoader}: the client jar must be on the same " +
          "classpath as softclient4es-core - providers are resolved against the classloader that " +
          "loaded softclient4es-core, never the thread context classloader"
        )
      )
  }

  //format:off
  /** Creates an Elasticsearch client with optional metrics and monitoring.
    *
    * The returned client type depends on configuration:
    *   - If metrics.enabled = false: Base client without metrics
    *   - If metrics.enabled = true and monitoring.enabled = false: Client with metrics
    *   - If metrics.enabled = true and monitoring.enabled = true: Client with metrics and
    *     monitoring
    *
    * Callers with an equal effective `elastic.*` configuration get the same cached instance; call
    * `close()` once when done with it.
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
  //format:on
  def create(config: Config = ConfigFactory.load()): ElasticClientApi =
    cache.create(config, ElasticConfig(config))

  //format:off
  /** Creates a client with explicitly enabled metrics, regardless of configuration.
    *
    * Uses caching: calls with an equal effective `elastic.*` configuration return the same
    * instance; call `close()` once per call.
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
  //format:on
  def createWithMetrics(config: Config = ConfigFactory.load()): MetricsElasticClient =
    cache.metrics(config, ElasticConfig(config))

  //format:off
  /** Creates a client with a custom metrics collector.
    *
    * Useful for sharing a collector between multiple clients or for testing. Always creates a new
    * client instance, over the cached base client of the configuration; its `close()` releases that
    * share.
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
  //format:on
  def createWithCustomMetrics(
    config: Config = ConfigFactory.load(),
    metricsCollector: MetricsCollector
  ): MetricsElasticClient =
    cache.customMetrics(config, ElasticConfig(config), metricsCollector)

  //format:off
  /** Creates a client with automatic monitoring and alerting.
    *
    * Monitoring generates periodic reports and triggers alerts when configured thresholds are
    * exceeded. Uses caching: calls with an equal effective `elastic.*` configuration return the
    * same instance; call `close()` once per call (`shutdown()` would stop the monitoring of every
    * caller sharing it).
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
    * // Done with it: releases this caller's share
    * client.close()
    * }}}
    */
  //format:on
  def createWithMonitoring(config: Config = ConfigFactory.load()): MonitoredElasticClient =
    cache.monitored(config, ElasticConfig(config))

  /** Creates a monitored client with a custom metrics collector.
    *
    * Always creates a new client instance, over the cached base client of the configuration; its
    * `close()` stops its monitoring and releases that share.
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
  ): MonitoredElasticClient =
    cache.customMonitored(config, ElasticConfig(config), metricsCollector)

  /** Shuts down all cached clients.
    *
    * This method should be called when the application terminates. It's automatically called via a
    * JVM shutdown hook.
    */
  def shutdown(): Unit = {
    logger.info("Shutting down all Elasticsearch clients")
    cache.shutdown()
    logger.info("All Elasticsearch clients shut down")
  }

  /** Clears all caches without shutting down clients.
    *
    * For tests. Clients handed out before stay usable but are no longer managed: their `close()` is
    * a no-op and `shutdown()` does not close them.
    */
  def clearCache(): Unit = {
    logger.warn("Clearing Elasticsearch client cache")
    cache.clear()
  }

  /** Gets statistics about cached clients.
    *
    * @return
    *   Map with cache statistics
    */
  def getCacheStats: Map[String, Int] = cache.stats
}
