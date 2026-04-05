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
  * Register implementations in:
  * `META-INF/services/app.softnetwork.elastic.licensing.LicenseManagerSpi`
  */
trait LicenseManagerSpi {

  /** Priority for SPI resolution (lower = higher priority). Default: 100.
    * CommunityLicenseManagerSpi uses Int.MaxValue (lowest priority).
    */
  def priority: Int = 100

  /** Create a LicenseManager from application configuration.
    *
    * @param config
    *   The application configuration (typically from `ConfigFactory.load()`)
    * @param mode
    *   Runtime context hint. `None` = unknown context (safe default: Driver semantics).
    *   `Some(LongRunning)` = wire AutoRefreshStrategy. `Some(Driver)` = wire
    *   OnDemandRefreshStrategy with checkExpiry().
    * @return
    *   A configured LicenseManager instance with appropriate refresh behavior
    */
  def create(config: Config, mode: Option[LicenseMode] = None): LicenseManager
}
