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
import app.softnetwork.elastic.client.{ElasticClientApi, ElasticConfig}
import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import app.softnetwork.elastic.client.monitoring.MonitoredElasticClient
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.function.BiFunction
import scala.concurrent.duration._
import scala.util.control.NonFatal

/** The client cache behind [[ElasticClientFactory]].
  *
  *   - Two callers share a client only when their effective `elastic.*` configurations are equal
  *     ([[ClientKey]]); an equal key gets the SAME instance from each cached path.
  *   - Every acquisition opens a share and every `close()` of a handed-out client releases one
  *     (never below zero): a close never closes a client, or stops a monitor, that another caller
  *     still holds.
  *   - A client with no open share stays cached, so a caller that comes back within `idleTtl`
  *     reuses it; one idle for longer is closed, exactly once, by the first acquisition sweep after
  *     that (at most one sweep per [[ElasticClientCache.SweepInterval]]), or by `shutdown()`.
  *
  * Lock discipline (`ConcurrentHashMap` forbids updating another mapping from a remapping function:
  * `Recursive update` in one bin, a deadlock across two threads). The sweep runs only at a public
  * entry, before its first map operation. The only nested map operation is a wrapper's build
  * acquiring its base share inside the wrapper's `compute` (wrapper -> base, never the other way),
  * and that inner acquisition never sweeps. Every release and every real close runs after the
  * remapping function has returned.
  *
  * @param build
  *   builds a new client for a configuration (the SPI provider)
  * @param nanoTime
  *   the clock of the idle policy
  * @param idleTtl
  *   how long a client with no open share stays cached
  */
private[spi] final class ElasticClientCache(
  build: Config => ElasticClientApi,
  nanoTime: () => Long = () => System.nanoTime(),
  idleTtl: FiniteDuration = ElasticClientCache.IdleTtl
) {

  private[this] val logger: Logger = LoggerFactory.getLogger(classOf[ElasticClientCache])

  /** A cached client and its open shares. `refs` and `lastRelease` change only inside a `compute` /
    * `computeIfPresent` of the entry's key.
    */
  private abstract class Entry(val key: ClientKey, val authKind: String) {
    @volatile var refs: Int = 1
    @volatile var lastRelease: Long = nanoTime()
    def url: String = key.url
  }

  private final class BaseEntry(key: ClientKey, authKind: String, val raw: ElasticClientApi)
      extends Entry(key, authKind) {
    lazy val shared: SharedClient = new SharedClient(raw, () => release(baseEntries, this))
  }

  /** A cached wrapper; it holds ONE share of its base client, released when it is closed. */
  private abstract class WrapperEntry(key: ClientKey, authKind: String, val base: BaseEntry)
      extends Entry(key, authKind)

  private final class MetricsEntry(key: ClientKey, authKind: String, base: BaseEntry)
      extends WrapperEntry(key, authKind, base) {
    @volatile var client: SharedMetricsClient = _
  }

  private final class MonitoredEntry(key: ClientKey, authKind: String, base: BaseEntry)
      extends WrapperEntry(key, authKind, base) {
    @volatile var client: SharedMonitoredClient = _
  }

  private[this] val baseEntries = new ConcurrentHashMap[ClientKey, BaseEntry]()
  private[this] val metricsEntries = new ConcurrentHashMap[ClientKey, MetricsEntry]()
  private[this] val monitoredEntries = new ConcurrentHashMap[ClientKey, MonitoredEntry]()

  private[this] val idleTtlNanos: Long = idleTtl.toNanos
  private[this] val sweepIntervalNanos: Long = ElasticClientCache.SweepInterval.toNanos
  private[this] val lastSweep = new AtomicLong(nanoTime())

  private[this] val systemLock = new Object
  @volatile private[this] var system: ActorSystem = _

  // ==================== the cached paths ====================

  /** `create` with the configuration's own choice of path. */
  def create(config: Config, ec: ElasticConfig): ElasticClientApi =
    if (ec.metrics.enabled) {
      if (ec.metrics.monitoring.enabled) monitored(config, ec) else metrics(config, ec)
    } else base(config, ec)

  /** The base client (metrics off). */
  def base(config: Config, ec: ElasticConfig): ElasticClientApi = {
    sweepIfDue()
    acquireBase(ClientKey.of(ec), config, authKind(ec)).shared
  }

  /** The client with metrics. */
  def metrics(config: Config, ec: ElasticConfig): MetricsElasticClient = {
    sweepIfDue()
    val key = ClientKey.of(ec)
    val kind = authKind(ec)
    acquireWrapper(metricsEntries, key) {
      logger.info(s"Creating new MetricsElasticClient for URL: ${key.url} (auth: $kind)")
      acquireBase(key, config, kind)
    } { base =>
      val entry = new MetricsEntry(key, kind, base)
      entry.client = new SharedMetricsClient(
        base.raw,
        new MetricsCollector(),
        () => release(metricsEntries, entry)
      )
      entry
    }.client
  }

  /** The client with metrics and monitoring. */
  def monitored(config: Config, ec: ElasticConfig): MonitoredElasticClient = {
    sweepIfDue()
    val key = ClientKey.of(ec)
    val kind = authKind(ec)
    acquireWrapper(monitoredEntries, key) {
      logger.info(s"Creating new MonitoredElasticClient for URL: ${key.url} (auth: $kind)")
      acquireBase(key, config, kind)
    } { base =>
      val entry = new MonitoredEntry(key, kind, base)
      // the monitoring system is created only now, once the base client exists (a failed build
      // creates none)
      entry.client = new SharedMonitoredClient(
        base.raw,
        new MetricsCollector(),
        ec.metrics.monitoring,
        () => release(monitoredEntries, entry)
      )(monitoringSystem())
      entry
    }.client
  }

  // ==================== the per-call paths ====================

  /** One caller's share of the cached base client. */
  def lease(config: Config, ec: ElasticConfig): ClientLease = {
    sweepIfDue()
    val base = acquireBase(ClientKey.of(ec), config, authKind(ec))
    new ClientLease(base.raw, () => release(baseEntries, base))
  }

  /** A new metrics client over a lease on the cached base client, with the caller's collector. */
  def customMetrics(
    config: Config,
    ec: ElasticConfig,
    metricsCollector: MetricsCollector
  ): MetricsElasticClient = {
    logger.info(
      s"Creating new MetricsElasticClient with custom collector for URL: " +
      s"${ec.credentials.url} (auth: ${authKind(ec)})"
    )
    val share = lease(config, ec)
    try new MetricsElasticClient(share, metricsCollector)
    catch {
      case t: Throwable =>
        share.close()
        throw t
    }
  }

  /** A new monitored client over a lease on the cached base client, with the caller's collector.
    */
  def customMonitored(
    config: Config,
    ec: ElasticConfig,
    metricsCollector: MetricsCollector
  ): MonitoredElasticClient = {
    logger.info(
      s"Creating new MonitoredElasticClient with custom collector for URL: " +
      s"${ec.credentials.url} (auth: ${authKind(ec)})"
    )
    val share = lease(config, ec)
    try new LeasedMonitoredClient(share, metricsCollector, ec.metrics.monitoring)(
      monitoringSystem()
    )
    catch {
      case t: Throwable =>
        share.close()
        throw t
    }
  }

  // ==================== lifecycle ====================

  /** The ONE monitoring system of every monitored client this cache builds, created lazily. */
  private[spi] def monitoringSystem(): ActorSystem = {
    val current = system
    if (current != null) current
    else
      systemLock.synchronized {
        if (system == null) system = ActorSystem("monitoring-system")
        system
      }
  }

  /** Closes every cached client exactly once, empties the cache and terminates the monitoring
    * system. A client handed out before stays closed; its `close()` is then a no-op.
    */
  def shutdown(): Unit = {
    // monitored clients first: they run a scheduler
    drain(monitoredEntries).foreach { entry =>
      try entry.client.shutdown()
      catch {
        case NonFatal(ex) =>
          logger.error(s"Error shutting down monitored client: ${ex.getMessage}", ex)
      }
    }
    // a metrics client holds nothing of its own: its base client is closed below
    drain(metricsEntries).foreach(_ => ())
    drain(baseEntries).foreach { entry =>
      try entry.raw.close()
      catch {
        case NonFatal(ex) =>
          logger.error(s"Error shutting down base client: ${ex.getMessage}", ex)
      }
    }
    terminateMonitoringSystem()
  }

  /** Empties the cache without closing anything. */
  def clear(): Unit = {
    baseEntries.clear()
    metricsEntries.clear()
    monitoredEntries.clear()
  }

  def stats: Map[String, Int] =
    Map(
      "baseClients"      -> baseEntries.size(),
      "metricsClients"   -> metricsEntries.size(),
      "monitoredClients" -> monitoredEntries.size()
    )

  // ==================== shares ====================

  private[this] def acquire[E <: Entry](
    map: ConcurrentHashMap[ClientKey, E],
    key: ClientKey
  )(newEntry: => E): E =
    map.compute(
      key,
      new BiFunction[ClientKey, E, E] {
        override def apply(k: ClientKey, existing: E): E =
          if (existing != null) {
            existing.refs += 1
            existing
          } else newEntry
      }
    )

  /** Never sweeps: it also runs nested, inside a wrapper's build. */
  private[this] def acquireBase(key: ClientKey, config: Config, kind: String): BaseEntry =
    acquire(baseEntries, key) {
      logger.info(s"Creating new Elasticsearch client for URL: ${key.url} (auth: $kind)")
      new BaseEntry(key, kind, build(config))
    }

  /** Acquires a wrapper; a new one takes its base share inside the wrapper's `compute`. A share
    * taken for a wrapper that then failed to build is released once `compute` has returned.
    */
  private[this] def acquireWrapper[E <: WrapperEntry](
    map: ConcurrentHashMap[ClientKey, E],
    key: ClientKey
  )(acquireBaseShare: => BaseEntry)(wrap: BaseEntry => E): E = {
    var orphan: BaseEntry = null
    try acquire(map, key) {
      val base = acquireBaseShare
      orphan = base
      val entry = wrap(base)
      orphan = null
      entry
    } catch {
      case t: Throwable =>
        if (orphan != null) release(baseEntries, orphan)
        throw t
    }
  }

  /** Releases one share of `entry`, never below zero. A no-op once the entry has left the cache
    * (closed by the sweep or `shutdown()`, or cleared), even if an equal key was cached again.
    */
  private[this] def release[E <: Entry](
    map: ConcurrentHashMap[ClientKey, E],
    entry: E,
    at: Long = nanoTime()
  ): Unit = {
    map.computeIfPresent(
      entry.key,
      new BiFunction[ClientKey, E, E] {
        override def apply(k: ClientKey, current: E): E = {
          if ((current eq entry) && current.refs > 0) {
            current.refs -= 1
            if (at - current.lastRelease > 0) current.lastRelease = at
          }
          current
        }
      }
    )
    ()
  }

  // ==================== the idle sweep ====================

  private[this] def sweepIfDue(): Unit = {
    val now = nanoTime()
    val last = lastSweep.get()
    if (now - last >= sweepIntervalNanos && lastSweep.compareAndSet(last, now)) sweep(now)
  }

  private[this] def sweep(now: Long): Unit = {
    // Wrappers first. An evicted wrapper releases its base share as of its own last release (when
    // its last caller left), so a base client whose only share was that wrapper's is closed by the
    // same sweep.
    evictIdle(monitoredEntries, now).foreach { entry =>
      logger.info(
        s"Closing idle MonitoredElasticClient for URL: ${entry.url} (auth: ${entry.authKind})"
      )
      try entry.client.shutdown()
      catch {
        case NonFatal(ex) =>
          logger.error(s"Error shutting down monitored client: ${ex.getMessage}", ex)
      } finally release(baseEntries, entry.base, entry.lastRelease)
    }
    evictIdle(metricsEntries, now).foreach { entry =>
      logger.info(
        s"Closing idle MetricsElasticClient for URL: ${entry.url} (auth: ${entry.authKind})"
      )
      release(baseEntries, entry.base, entry.lastRelease)
    }
    evictIdle(baseEntries, now).foreach { entry =>
      logger.info(
        s"Closing idle Elasticsearch client for URL: ${entry.url} (auth: ${entry.authKind})"
      )
      try entry.raw.close()
      catch {
        case NonFatal(ex) =>
          logger.error(s"Error closing idle base client: ${ex.getMessage}", ex)
      }
    }
  }

  private[this] def isIdle(entry: Entry, now: Long): Boolean =
    entry.refs == 0 && now - entry.lastRelease > idleTtlNanos

  /** Removes every idle entry (re-checked under the key's lock) and returns them, to be closed once
    * outside it.
    */
  private[this] def evictIdle[E <: Entry](
    map: ConcurrentHashMap[ClientKey, E],
    now: Long
  ): List[E] = {
    var evicted = List.empty[E]
    val candidates = map.values().iterator()
    while (candidates.hasNext) {
      val candidate = candidates.next()
      if (isIdle(candidate, now)) {
        map.computeIfPresent(
          candidate.key,
          new BiFunction[ClientKey, E, E] {
            override def apply(k: ClientKey, current: E): E =
              if (isIdle(current, now)) {
                evicted = current :: evicted
                null.asInstanceOf[E]
              } else current
          }
        )
      }
    }
    evicted
  }

  private[this] def drain[E <: Entry](map: ConcurrentHashMap[ClientKey, E]): List[E] = {
    var drained = List.empty[E]
    val keys = map.keySet().iterator()
    while (keys.hasNext) {
      val removed = map.remove(keys.next())
      if (removed != null) drained = removed :: drained
    }
    drained
  }

  private[this] def terminateMonitoringSystem(): Unit = {
    val current = systemLock.synchronized {
      val s = system
      system = null
      s
    }
    if (current != null) {
      try {
        current.terminate()
        ()
      } catch {
        case NonFatal(ex) =>
          logger.error(s"Error terminating the monitoring system: ${ex.getMessage}", ex)
      }
    }
  }

  /** The auth method KIND for the log lines: never a username, a secret or a fingerprint. */
  private[this] def authKind(ec: ElasticConfig): String =
    ec.credentials.authMethod.fold("none")(_.toString)
}

private[spi] object ElasticClientCache {

  /** How long a client with no open share stays cached. */
  val IdleTtl: FiniteDuration = 10.minutes

  /** At most one idle sweep per interval. */
  val SweepInterval: FiniteDuration = 1.minute
}
