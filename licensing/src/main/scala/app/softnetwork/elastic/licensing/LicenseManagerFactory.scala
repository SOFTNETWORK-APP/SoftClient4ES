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
import com.typesafe.scalalogging.LazyLogging

/** Factory for creating LicenseManager instances.
  *
  * Delegates to `LicenseRefreshStrategyFactory` for strategy lifecycle management and returns the
  * strategy's `licenseManager`.
  */
object LicenseManagerFactory extends LazyLogging {

  /** Create a LicenseManager from config. Derives LicenseMode from config. Resolves the best SPI,
    * builds the strategy, initializes it, caches it, and returns the strategy's licenseManager.
    */
  def create(config: Config, metrics: MetricsApi = MetricsApi.Noop): LicenseManager =
    LicenseRefreshStrategyFactory.create(config, metrics).licenseManager
}
