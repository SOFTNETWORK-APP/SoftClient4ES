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

import app.softnetwork.elastic.licensing.metrics.MetricsApi

/** Strategy for license lifecycle management (initialization and refresh).
  *
  * Concrete implementations:
  *   - `OnDemandRefreshStrategy` (Driver mode): single fetch at startup, checkExpiry on access, no
  *     implicit network calls
  *   - `AutoRefreshStrategy` (LongRunning mode): periodic background refresh via scheduler (Story
  *     5.8)
  */
trait LicenseRefreshStrategy {

  /** Called once at startup to resolve the initial license. Always succeeds (worst case: Community
    * default).
    */
  def initialize(): LicenseKey

  /** Called by REFRESH LICENSE command to force an explicit re-fetch. */
  def refresh(): Either[LicenseError, LicenseKey]

  /** Access the current LicenseManager for SHOW LICENSE and feature/quota checks. */
  def licenseManager: LicenseManager

  /** Telemetry collector for this strategy. Extensions update counters via this reference. Default
    * returns `TelemetryCollector.Noop` (zero counters, never updated).
    */
  def telemetryCollector: TelemetryCollector = TelemetryCollector.Noop

  /** Metrics API for operation-level performance tracking. Default returns `MetricsApi.Noop`. */
  def metrics: MetricsApi = MetricsApi.Noop

  /** Shutdown background resources (scheduler, etc.). Default is no-op. */
  def shutdown(): Unit = ()

  /** Story 15.2 (A15) -- emit ONE daily product-instance telemetry ping for the given surface.
    *
    * Best-effort, fire-and-forget: never throws to the caller, gated on the NEW
    * `softclient4es.telemetry.enabled` opt-out ([[TelemetryConfig]]). This trait lives in the
    * Apache `licensing` module (visible to `core`/REPL), but the actual ELv2 ping transport
    * (`InstancePingClient`) lives downstream in the extensions module — so this is the seam the
    * REPL (and any surface holding only a `LicenseRefreshStrategy`) calls; the extensions
    * strategies override it to build the surface-specific report and POST it. The strategy itself
    * supplies the stable `instance_id`, `uptime_seconds`, `license_tier` and `join_query_count`
    * (from [[telemetryCollector]] / [[licenseManager]]); the caller supplies only the
    * surface-specific fields. The default implementation is a no-op (Community/Apache build with no
    * extension wired).
    *
    * @param product
    *   the surface discriminator (`ProductType.displayName`, e.g. `"repl"`)
    * @param version
    *   the surface's own build version
    * @param sessionDurationSeconds
    *   REPL session duration (None for non-REPL surfaces)
    * @param commandsExecuted
    *   REPL executed-statement count (None for non-REPL surfaces)
    * @param clusterName
    *   sidecar single-cluster name (None where N/A)
    * @param clusterCount
    *   federation connected-cluster count (None where N/A)
    */
  def emitInstancePing(
    product: String,
    version: String,
    sessionDurationSeconds: Option[Long] = None,
    commandsExecuted: Option[Int] = None,
    clusterName: Option[String] = None,
    clusterCount: Option[Int] = None
  ): Unit = ()
}
