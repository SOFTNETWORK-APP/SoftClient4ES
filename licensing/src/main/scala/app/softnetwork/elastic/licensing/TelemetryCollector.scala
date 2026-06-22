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

package app.softnetwork.elastic.licensing

import app.softnetwork.elastic.licensing.metrics.{AggregatedMetrics, MetricsApi}

import java.util.concurrent.atomic.AtomicLong

/** Snapshot of runtime telemetry for inclusion in refresh requests.
  *
  * `queriesTotal` (and the cluster gauges) are cumulative since server startup (stateless -- reset
  * on restart). Backend note: the backend receives cumulative totals per instance_id. To compute
  * per-interval deltas, the backend stores the previous snapshot and subtracts. When queries_total
  * drops below the previous value for the same instance_id, this indicates a process restart --
  * start a new session, don't compute negative delta.
  *
  * Story 15.3 -- `joinQueryCount` / `joinQueryByRow` are DIFFERENT: they are per-interval DELTAS
  * (reset on each `collectAndReset` / `collectAndResetJoinCounts`, flushed on clean shutdown), NOT
  * cumulative like `queriesTotal`. `joinQueryCount` is the total of executed classified cross-index
  * JOINs in the interval; `joinQueryByRow` disaggregates it into the three execution rows and
  * ALWAYS carries all three keys (`passthrough`, `cross_cluster`, `coordinator`) on every COLLECTED
  * snapshot (a zero is an explicit `0`, never an absent entry) so the wire shape stays stable for
  * the Story-15.2 daily ping. `joinQueryCount == passthrough + cross_cluster + coordinator`. The
  * `Map.empty` default below applies only to the bare/no-op construction, never to a `collect()` /
  * `collectAndReset()` result. Story 15.2 reads `joinQueryCount` into the `InstancePing` proto
  * `join_query_count` field.
  */
case class TelemetryData(
  queriesTotal: Long = 0,
  mvsActive: Int = 0,
  clustersConnected: Int = 0,
  clusterId: Option[String] = None,
  clusterName: Option[String] = None,
  clusterVersion: Option[String] = None,
  aggregatedMetrics: AggregatedMetrics = AggregatedMetrics.empty,
  joinQueryCount: Long = 0,
  joinQueryByRow: Map[String, Long] = Map.empty
)

/** Mutable telemetry collector with atomic counters.
  *
  * Accessible (read-only snapshot) via
  * `LicenseRefreshStrategy.telemetryCollector.collect(metrics)`. Extensions (e.g. CoreDqlExtension)
  * update counters via increment/set methods through
  * `LicenseRefreshStrategyFactory.create(config).telemetryCollector`.
  *
  * Thread-safe: `queriesTotal` uses `AtomicLong` for high-frequency increments. Cluster info fields
  * (`clusterId`, `clusterName`, `clusterVersion`) and gauges (`mvsActive`, `clustersConnected`) are
  * guarded by `clusterInfoLock` to ensure atomic reads and writes.
  */
class TelemetryCollector {

  private val _queriesTotal = new AtomicLong(0L)
  // Story 15.3 -- per-interval JOIN-query DELTA buckets, one per execution row. Lock-free
  // (AtomicLong); independent so the increment never contends with collect/collectAndReset.
  private val _joinPassthrough = new AtomicLong(0L)
  private val _joinCrossCluster = new AtomicLong(0L)
  private val _joinCoordinator = new AtomicLong(0L)
  private val clusterInfoLock = new AnyRef
  private var _mvsActive: Int = 0
  private var _clustersConnected: Int = 0
  private var _clusterId: Option[String] = None
  private var _clusterName: Option[String] = None
  private var _clusterVersion: Option[String] = None

  // --- Write methods (called by extensions) ---

  def incrementQueries(): Unit = { val _ = _queriesTotal.incrementAndGet() }

  /** Record one classified, executed cross-index JOIN, attributed to its execution row (Story
    * 15.3). Real-time, lock-free (AtomicLong); the three buckets are independent so the increment
    * never contends with collect/collectAndReset. Mirrors `incrementQueries`, but keyed by row and
    * resettable (per-interval delta, NOT cumulative).
    */
  def incrementJoin(row: TelemetryCollector.JoinRow): Unit = {
    val _ = (row match {
      case TelemetryCollector.JoinRow.Passthrough  => _joinPassthrough
      case TelemetryCollector.JoinRow.CrossCluster => _joinCrossCluster
      case TelemetryCollector.JoinRow.Coordinator  => _joinCoordinator
    }).incrementAndGet()
  }

  /** Snapshot+reset ONLY the JOIN delta (read+zero with `getAndSet`). Used by the scheduled tick
    * when `config.telemetryEnabled` is false -- so the buckets still reset (AC 5a) -- and by the
    * clean-shutdown flush (AC 5b), without going through the full metrics `collectAndReset` (which
    * the disabled branch deliberately skips). Returns (total, byRow-with-all-3-keys, even when 0).
    */
  def collectAndResetJoinCounts(): (Long, Map[String, Long]) = {
    val p = _joinPassthrough.getAndSet(0L)
    val x = _joinCrossCluster.getAndSet(0L)
    val c = _joinCoordinator.getAndSet(0L)
    (p + x + c, Map("passthrough" -> p, "cross_cluster" -> x, "coordinator" -> c))
  }

  def setMvsActive(count: Int): Unit = clusterInfoLock.synchronized { _mvsActive = count }

  def setClustersConnected(count: Int): Unit = clusterInfoLock.synchronized {
    _clustersConnected = count
  }

  def setClusterInfo(
    id: String,
    name: Option[String],
    version: Option[String]
  ): Unit = clusterInfoLock.synchronized {
    _clusterId = Some(id)
    _clusterName = name
    _clusterVersion = version
  }

  // --- Read methods (called by AutoRefreshStrategy.doScheduleRefresh) ---

  def collect(metrics: MetricsApi): TelemetryData = clusterInfoLock.synchronized {
    // JOIN buckets: read with .get() -- collect NEVER resets (interval read without flush).
    val p = _joinPassthrough.get()
    val x = _joinCrossCluster.get()
    val c = _joinCoordinator.get()
    TelemetryData(
      queriesTotal = _queriesTotal.get(),
      mvsActive = _mvsActive,
      clustersConnected = _clustersConnected,
      clusterId = _clusterId,
      clusterName = _clusterName,
      clusterVersion = _clusterVersion,
      aggregatedMetrics = metrics.getAggregatedMetrics,
      joinQueryCount = p + x + c,
      joinQueryByRow = Map("passthrough" -> p, "cross_cluster" -> x, "coordinator" -> c)
    )
  }

  /** Collect a snapshot and atomically reset the metrics, ensuring no operations recorded between
    * snapshot and reset are lost. The JOIN buckets are per-interval deltas -- they are read+zeroed
    * here (getAndSet) so the next interval starts from zero (Story 15.3 AC 5).
    */
  def collectAndReset(metrics: MetricsApi): TelemetryData = clusterInfoLock.synchronized {
    // JOIN buckets: read+zero with .getAndSet(0L) -- per-interval delta; this IS the flush.
    val p = _joinPassthrough.getAndSet(0L)
    val x = _joinCrossCluster.getAndSet(0L)
    val c = _joinCoordinator.getAndSet(0L)
    TelemetryData(
      queriesTotal = _queriesTotal.get(),
      mvsActive = _mvsActive,
      clustersConnected = _clustersConnected,
      clusterId = _clusterId,
      clusterName = _clusterName,
      clusterVersion = _clusterVersion,
      aggregatedMetrics = metrics.collectAndResetAggregatedMetrics,
      joinQueryCount = p + x + c,
      joinQueryByRow = Map("passthrough" -> p, "cross_cluster" -> x, "coordinator" -> c)
    )
  }
}

object TelemetryCollector {

  /** Execution row a classified cross-index JOIN is attributed to (Story 15.3). Each executed JOIN
    * increments exactly one row bucket (no double-counting across rows).
    */
  sealed trait JoinRow
  object JoinRow {

    /** Row 1 -- a single downstream owns the whole join (+ write); sidecar local JOIN; federation
      * `WriteWithJoinPassthroughResult`.
      */
    case object Passthrough extends JoinRow

    /** Row 2 -- federation cross-cluster conveyor (single source, different target;
      * `WriteWithJoinCrossClusterResult`).
      */
    case object CrossCluster extends JoinRow

    /** Row 3 -- federation multi-source coordinator (`WriteWithJoinCoordinatorResult`; also the
      * cross-cluster SELECT `FederatedResult` the coordinator runs).
      */
    case object Coordinator extends JoinRow
  }

  /** Default collector returning zero-valued data. Used when telemetry is disabled or no runtime
    * wires a real collector. Write methods are no-ops to prevent accidental mutation of the shared
    * singleton.
    */
  val Noop: TelemetryCollector = new TelemetryCollector {
    override def incrementQueries(): Unit = ()
    override def incrementJoin(row: JoinRow): Unit = ()
    override def collectAndResetJoinCounts(): (Long, Map[String, Long]) =
      (0L, Map("passthrough" -> 0L, "cross_cluster" -> 0L, "coordinator" -> 0L))
    override def setMvsActive(count: Int): Unit = ()
    override def setClustersConnected(count: Int): Unit = ()
    override def setClusterInfo(
      id: String,
      name: Option[String],
      version: Option[String]
    ): Unit = ()
  }
}
