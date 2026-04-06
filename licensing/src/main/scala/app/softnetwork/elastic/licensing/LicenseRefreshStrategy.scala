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

  /** Shutdown background resources (scheduler, etc.). Default is no-op. */
  def shutdown(): Unit = ()
}
