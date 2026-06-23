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
  *
  * Story P0.6 -- `capHitsByKind` is the per-quota-type cap-hit DELTA, with the SAME per-interval
  * semantics as `joinQueryByRow` (NOT cumulative like `queriesTotal`): it counts how many times a
  * license quota REJECTED an operation in the interval, keyed by the four quota types
  * (`max_materialized_views`, `max_query_results`, `max_joins`, `max_clusters`). This is the launch
  * "is the meter biting?" leading indicator (PRD §15.1 cap-hits). Like the JOIN buckets it ALWAYS
  * carries all four keys on a COLLECTED snapshot (a zero is an explicit `0`, never an absent entry)
  * so the wire shape stays stable for the daily ping; the `Map.empty` default applies only to the
  * bare/no-op construction. Story P0.6 reads these into the `InstancePing` proto `capHitMax*`
  * fields (14-17).
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
  joinQueryByRow: Map[String, Long] = Map.empty,
  capHitsByKind: Map[String, Long] = Map.empty
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
  // Story P0.6 -- per-interval cap-hit DELTA buckets, one per quota type. Same lock-free pattern as
  // the JOIN buckets: independent AtomicLong so the increment never contends with collect/reset.
  private val _capMv = new AtomicLong(0L)
  private val _capResults = new AtomicLong(0L)
  private val _capJoins = new AtomicLong(0L)
  private val _capClusters = new AtomicLong(0L)
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

  /** Record one license cap-hit (a quota REJECTED an operation), attributed to its quota type
    * (Story P0.6). Real-time, lock-free (AtomicLong); the four buckets are independent so the
    * increment never contends with collect/collectAndReset. Called at each of the four reject sites
    * (CoreDqlExtension / MaterializedViewExtension / JoinPlanner / JoinLicenseGuard), regardless of
    * whether that reject surfaces as an HTTP 402, a `Left(String)`, or a startup exit -- this is a
    * SEMANTIC cap-hit, not an HTTP-status filter (PRD §15.1 / P0.6 OQ-1).
    */
  def incrementCapHit(kind: TelemetryCollector.CapHitKind): Unit = {
    val _ = (kind match {
      case TelemetryCollector.CapHitKind.MaterializedViews => _capMv
      case TelemetryCollector.CapHitKind.QueryResults      => _capResults
      case TelemetryCollector.CapHitKind.Joins             => _capJoins
      case TelemetryCollector.CapHitKind.Clusters          => _capClusters
    }).incrementAndGet()
  }

  /** Snapshot+reset ONLY the cap-hit delta (read+zero with `getAndSet`). Used by the scheduled tick
    * when `config.telemetryEnabled` is false (so the buckets still reset, mirroring AC 5a) and by
    * the clean-shutdown flush (mirroring AC 5b / P0.6 OQ-8 -- the `maxClusters` cap-hit happens on
    * a startup CrashLoop, so its delta MUST be flushed before `sys.exit`). ALWAYS returns all four
    * keys (an explicit `0`, never an absent entry) so the wire shape stays stable for the daily
    * ping, exactly like `collectAndResetJoinCounts`.
    */
  def collectAndResetCapHits(): Map[String, Long] = Map(
    "max_materialized_views" -> _capMv.getAndSet(0L),
    "max_query_results"      -> _capResults.getAndSet(0L),
    "max_joins"              -> _capJoins.getAndSet(0L),
    "max_clusters"           -> _capClusters.getAndSet(0L)
  )

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

  /** Read-only snapshot of the cap-hit buckets (all four keys, NO reset). Used by `collect`. */
  private def readCapHits(): Map[String, Long] = Map(
    "max_materialized_views" -> _capMv.get(),
    "max_query_results"      -> _capResults.get(),
    "max_joins"              -> _capJoins.get(),
    "max_clusters"           -> _capClusters.get()
  )

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
      joinQueryByRow = Map("passthrough" -> p, "cross_cluster" -> x, "coordinator" -> c),
      // Cap-hit buckets: read with .get() -- collect NEVER resets (interval read without flush).
      capHitsByKind = readCapHits()
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
      joinQueryByRow = Map("passthrough" -> p, "cross_cluster" -> x, "coordinator" -> c),
      // Cap-hit buckets: read+zero with .getAndSet(0L) -- per-interval delta; this IS the flush.
      capHitsByKind = collectAndResetCapHits()
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

  /** The license quota type a cap-hit is attributed to (Story P0.6). Each rejected operation
    * increments exactly one bucket. The four types map 1:1 to the four enforced quotas; the `key`
    * is the snake-case wire/column name carried to the license-server `instance_ping` cap-hit
    * columns.
    */
  sealed trait CapHitKind { def key: String }
  object CapHitKind {

    /** `maxMaterializedViews` -- `MaterializedViewExtension` CREATE over quota (the quota-exceeded
      * 402 branch ONLY; NOT the feature-absent path -- P0.6 OQ-7).
      */
    case object MaterializedViews extends CapHitKind { val key = "max_materialized_views" }

    /** `maxQueryResults` -- `CoreDqlExtension` single-index result boundary: BOTH the
      * explicit-LIMIT 402 reject AND the P0.5 no-LIMIT truncation/cappedScroll bite (P0.6 OQ-2).
      */
    case object QueryResults extends CapHitKind { val key = "max_query_results" }

    /** `maxJoins` -- `JoinPlanner.plan()` cross-index JOIN-count reject (`Left(String)`, NOT a
      * 402).
      */
    case object Joins extends CapHitKind { val key = "max_joins" }

    /** `maxClusters` -- `JoinLicenseGuard` federation cluster-count reject (startup `Left`/exit or
      * `checkClusterLimit` `Left(QuotaExceeded)`, NOT a 402).
      */
    case object Clusters extends CapHitKind { val key = "max_clusters" }

    val values: Seq[CapHitKind] = Seq(MaterializedViews, QueryResults, Joins, Clusters)
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
    override def incrementCapHit(kind: CapHitKind): Unit = ()
    override def collectAndResetCapHits(): Map[String, Long] =
      CapHitKind.values.map(_.key -> 0L).toMap
    override def setMvsActive(count: Int): Unit = ()
    override def setClustersConnected(count: Int): Unit = ()
    override def setClusterInfo(
      id: String,
      name: Option[String],
      version: Option[String]
    ): Unit = ()
  }
}
