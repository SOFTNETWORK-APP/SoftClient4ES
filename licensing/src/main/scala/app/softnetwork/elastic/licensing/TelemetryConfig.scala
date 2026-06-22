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

import com.typesafe.config.{Config, ConfigFactory}

/** Story 15.2 (A8) -- reads the NEW top-level `softclient4es.telemetry.enabled` opt-out switch for
  * the daily product-instance telemetry ping (all five surfaces, all tiers including Community).
  *
  * This is DELIBERATELY DISTINCT from `softclient4es.license.telemetry.enabled`
  * ([[LicenseConfig.telemetryEnabled]]), which gates ONLY the detailed operation/index metrics
  * inside the license-REFRESH body (and is semantically meaningless for Community, which has no
  * refresh). The daily ping reads THIS key, never `LicenseConfig`. See the privacy doc (Story 15.6)
  * two-key table.
  *
  * Default is `true` (opt-out, not opt-in) for ALL tiers -- supplied by the `reference.conf`
  * `softclient4es.telemetry { enabled = true }` block.
  */
case class TelemetryConfig(enabled: Boolean)

object TelemetryConfig {

  /** The HOCON key the daily ping gates on. The endpoint is NOT a config key (A9 anti-tamper). */
  val EnabledKey: String = "softclient4es.telemetry.enabled"

  def load(): TelemetryConfig = load(ConfigFactory.load())

  def load(config: Config): TelemetryConfig =
    TelemetryConfig(enabled =
      if (config.hasPath(EnabledKey)) config.getBoolean(EnabledKey) else true
    )
}
