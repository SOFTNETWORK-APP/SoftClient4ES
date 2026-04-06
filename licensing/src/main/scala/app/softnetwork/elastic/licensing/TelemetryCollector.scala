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

import java.util.concurrent.atomic.AtomicLong

/** Snapshot of runtime telemetry for inclusion in refresh requests. All counters are cumulative
  * since server startup (stateless -- reset on restart).
  *
  * Backend note: the backend receives cumulative totals per instance_id. To compute per-interval
  * deltas, the backend stores the previous snapshot and subtracts. When queries_total drops below
  * the previous value for the same instance_id, this indicates a process restart -- start a new
  * session, don't compute negative delta.
  */
case class TelemetryData(
  queriesTotal: Long = 0,
  joinsTotal: Long = 0,
  mvsActive: Int = 0,
  clustersConnected: Int = 0
)

/** Mutable telemetry collector with atomic counters.
  *
  * Accessible (read-only snapshot) via `LicenseRefreshStrategy.telemetryCollector.collect()`.
  * Extensions (e.g. CoreDqlExtension) update counters via increment/set methods through
  * `LicenseManagerFactory.currentStrategy.telemetryCollector`.
  *
  * Thread-safe: counters use `AtomicLong`, gauges use `@volatile`.
  */
class TelemetryCollector {

  private val _queriesTotal = new AtomicLong(0L)
  private val _joinsTotal = new AtomicLong(0L)
  @volatile private var _mvsActive: Int = 0
  @volatile private var _clustersConnected: Int = 0

  // --- Write methods (called by extensions) ---

  def incrementQueries(): Unit = { val _ = _queriesTotal.incrementAndGet() }

  def incrementJoins(): Unit = { val _ = _joinsTotal.incrementAndGet() }

  def setMvsActive(count: Int): Unit = { _mvsActive = count }

  def setClustersConnected(count: Int): Unit = { _clustersConnected = count }

  // --- Read method (called by AutoRefreshStrategy.doScheduleRefresh) ---

  def collect(): TelemetryData = TelemetryData(
    queriesTotal = _queriesTotal.get(),
    joinsTotal = _joinsTotal.get(),
    mvsActive = _mvsActive,
    clustersConnected = _clustersConnected
  )
}

object TelemetryCollector {

  /** Default collector returning zero-valued data. Used when telemetry is disabled or no runtime
    * wires a real collector. Write methods are no-ops to prevent accidental mutation of the shared
    * singleton.
    */
  val Noop: TelemetryCollector = new TelemetryCollector {
    override def incrementQueries(): Unit = ()
    override def incrementJoins(): Unit = ()
    override def setMvsActive(count: Int): Unit = ()
    override def setClustersConnected(count: Int): Unit = ()
  }
}
