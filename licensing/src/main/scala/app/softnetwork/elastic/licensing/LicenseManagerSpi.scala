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
import com.typesafe.config.Config

/** Runtime context that determines licensing behavior.
  *
  * The LicenseManagerSpi uses this to wire the appropriate refresh strategy:
  *   - LongRunning: AutoRefreshStrategy (periodic background refresh)
  *   - Driver: OnDemandRefreshStrategy (single fetch, checkExpiry on access, no implicit calls)
  */
sealed trait LicenseMode

object LicenseMode {

  /** Long-running process: REPL, Arrow Flight SQL server, Federation server. License refreshes
    * automatically in the background.
    */
  case object LongRunning extends LicenseMode

  /** Short-lived driver session: JDBC, ADBC. License fetched once at connect time, never refreshed
    * implicitly. Expired JWTs silently fall back to Community tier via checkExpiry().
    */
  case object Driver extends LicenseMode
}

/** Service Provider Interface for pluggable license manager implementations.
  *
  * Discovered via `java.util.ServiceLoader`. The implementation with the lowest priority value
  * wins.
  *
  * Subclasses implement `buildStrategy()` (pure construction, no initialization). The default
  * `create()` and `createStrategy()` delegate to `buildStrategy()`. Strategy initialization and
  * caching are handled by `LicenseManagerFactory`, not by the SPI.
  *
  * Register implementations in:
  * `META-INF/services/app.softnetwork.elastic.licensing.LicenseManagerSpi`
  */
trait LicenseManagerSpi {

  /** Priority for SPI resolution (lower = higher priority). Default: 100.
    * CommunityLicenseManagerSpi uses Int.MaxValue (lowest priority).
    */
  def priority: Int = 100

  /** Build a LicenseRefreshStrategy for this SPI. Subclasses must implement.
    *
    * MUST NOT call `strategy.initialize()` — lifecycle is managed by `LicenseManagerFactory`. Each
    * call builds a fresh strategy (no caching).
    *
    * @param config
    *   The application configuration (typically from `ConfigFactory.load()`)
    * @param mode
    *   Runtime context hint. `None` = unknown context (safe default: Driver semantics).
    *   `Some(LongRunning)` = wire AutoRefreshStrategy. `Some(Driver)` = wire
    *   OnDemandRefreshStrategy with checkExpiry().
    * @return
    *   A freshly constructed (not initialized) LicenseRefreshStrategy
    */
  protected def buildStrategy(
    config: Config,
    mode: Option[LicenseMode],
    metrics: MetricsApi = MetricsApi.Noop
  ): LicenseRefreshStrategy

  /** Create a LicenseManager by building a strategy and returning its licenseManager.
    *
    * Note: the strategy is NOT initialized here — callers who need full lifecycle management should
    * use `LicenseManagerFactory.create()` instead.
    */
  def create(
    config: Config,
    mode: Option[LicenseMode] = None,
    metrics: MetricsApi = MetricsApi.Noop
  ): LicenseManager =
    buildStrategy(config, mode, metrics).licenseManager

  /** Create a LicenseRefreshStrategy directly (not initialized, not cached).
    *
    * Returns the full strategy object, enabling `LicenseRefreshStrategyFactory` to manage its
    * lifecycle (initialize, cache, replace).
    */
  def createStrategy(
    config: Config,
    mode: Option[LicenseMode] = None,
    metrics: MetricsApi = MetricsApi.Noop
  ): LicenseRefreshStrategy =
    buildStrategy(config, mode, metrics)
}
