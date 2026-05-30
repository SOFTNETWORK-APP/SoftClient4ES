/*
 * Copyright 2026 SOFTNETWORK
 *
 * Licensed under the Elastic License 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.elastic.co/licensing/elastic-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.licensing

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Verifies the mode-resolution contract that the JDBC driver (Story 10.3) relies on for AC2:
  * overlay `softclient4es.license.refresh-enabled = false` onto the merged HOCON config and
  * [[LicenseRefreshStrategyFactory.resolveMode]] returns `Some(LicenseMode.Driver)`.
  *
  * The companion integration test in `JdbcIntegrationSpec` cannot verify this end-to-end —
  * the JDBC test classpath registers a priority-1 `TestLicenseManagerSpi` that ignores the
  * `mode` parameter, so a strategy-class assertion there is vacuous. `resolveMode` is pure
  * on the config (no SPI involvement), so a direct call here is the authoritative check.
  */
class LicenseRefreshStrategyFactoryDriverModeSpec extends AnyFlatSpec with Matchers {

  behavior of "LicenseRefreshStrategyFactory.resolveMode"

  it should "resolve LicenseMode.Driver when softclient4es.license.refresh.enabled = false" in {
    // Same HOCON merge that ElasticConnection.licenseStrategy / ConnectionUrl.toConfig
    // build: an inline overlay with refresh.enabled=false on top of ConfigFactory.load
    // (reference.conf defaults). The HOCON path is `softclient4es.license.refresh.enabled`
    // (with a dot between refresh and enabled) — that is what `LicenseConfig.load` reads
    // via `license.getBoolean("refresh.enabled")`.
    val cfg = ConfigFactory
      .parseString("softclient4es.license.refresh.enabled = false")
      .withFallback(ConfigFactory.load())
      .resolve()

    LicenseRefreshStrategyFactory.resolveMode(cfg) shouldBe Some(LicenseMode.Driver)
  }

  it should "resolve LicenseMode.LongRunning when softclient4es.license.refresh.enabled = true" in {
    // Symmetric verification: the same factory hook used by sidecar / federation startup
    // (which leaves refresh.enabled at its true default) must NOT be silently downgraded
    // to Driver mode if someone refactors resolveMode.
    val cfg = ConfigFactory
      .parseString("softclient4es.license.refresh.enabled = true")
      .withFallback(ConfigFactory.load())
      .resolve()

    LicenseRefreshStrategyFactory.resolveMode(cfg) shouldBe Some(LicenseMode.LongRunning)
  }
}
