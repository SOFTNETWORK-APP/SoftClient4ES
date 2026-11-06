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
import app.softnetwork.elastic.client.{ElasticClientApi, ElasticClientDelegator}
import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import app.softnetwork.elastic.client.monitoring.{MonitoredElasticClient, MonitoringConfig}

import java.util.concurrent.atomic.AtomicBoolean

/* The clients ElasticClientFactory hands out. A cached client is shared by every caller with the
 * same effective configuration, so its close() releases ONE share and never closes the real client
 * (or stops the shared monitor) under another caller: the cache closes it once nobody holds it and
 * it has stayed idle (ElasticClientCache). Caller contract: one close() per create(). */

/** The cached client of the metrics-off path. */
private[spi] final class SharedClient(val delegate: ElasticClientApi, release: () => Unit)
    extends ElasticClientDelegator {
  override def close(): Unit = release()
}

/** The cached client of the metrics path. */
private[spi] final class SharedMetricsClient(
  delegate: ElasticClientApi,
  metricsCollector: MetricsCollector,
  release: () => Unit
) extends MetricsElasticClient(delegate, metricsCollector) {
  override def close(): Unit = release()
}

/** The cached client of the monitored path (the default). `shutdown()` stays the real stop: the
  * cache calls it when it closes the entry.
  */
private[spi] final class SharedMonitoredClient(
  delegate: ElasticClientApi,
  metricsCollector: MetricsCollector,
  monitoringConfig: MonitoringConfig,
  release: () => Unit
)(implicit val actorSystem: ActorSystem)
    extends MonitoredElasticClient(delegate, metricsCollector, monitoringConfig)(actorSystem) {
  override def close(): Unit = release()
}

/** One caller's share of a cached base client, for the paths that build a new wrapper per call (the
  * custom-collector ones). Its `close()` releases the share once, however often it is called.
  */
private[spi] final class ClientLease(val delegate: ElasticClientApi, release: () => Unit)
    extends ElasticClientDelegator {
  private[this] val released = new AtomicBoolean(false)
  override def close(): Unit = if (released.compareAndSet(false, true)) release()
}

/** The monitored client built per call with a caller's own collector: closing it stops ITS monitor
  * and releases its lease on the shared base client.
  */
private[spi] final class LeasedMonitoredClient(
  lease: ClientLease,
  metricsCollector: MetricsCollector,
  monitoringConfig: MonitoringConfig
)(implicit val actorSystem: ActorSystem)
    extends MonitoredElasticClient(lease, metricsCollector, monitoringConfig)(actorSystem) {
  override def close(): Unit =
    try shutdown()
    finally lease.close()
}
