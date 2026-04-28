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

/** Fallback SPI that provides Community-tier licensing with no external dependencies. Priority
  * Int.MaxValue ensures any other SPI implementation takes precedence. Ignores licenseMode —
  * Community is always Community regardless of runtime context.
  */
class CommunityLicenseManagerSpi extends LicenseManagerSpi {
  override def priority: Int = Int.MaxValue
  override protected def buildStrategy(
    config: Config,
    mode: Option[LicenseMode],
    metrics: MetricsApi
  ): LicenseRefreshStrategy = new NopRefreshStrategy()
}
