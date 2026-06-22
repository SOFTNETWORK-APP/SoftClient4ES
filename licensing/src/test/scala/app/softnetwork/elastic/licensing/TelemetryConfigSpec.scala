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

import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 15.2 (A8) -- the NEW top-level `softclient4es.telemetry.enabled` daily-ping opt-out,
  * DISTINCT from `softclient4es.license.telemetry.enabled`.
  */
class TelemetryConfigSpec extends AnyFlatSpec with Matchers {

  private def configFrom(hocon: String): TelemetryConfig =
    TelemetryConfig.load(
      ConfigFactory
        .parseString(hocon)
        .withFallback(ConfigFactory.load())
    )

  "reference.conf default" should "enable the daily ping (true)" in {
    TelemetryConfig.load(ConfigFactory.load()).enabled shouldBe true
  }

  "an explicit softclient4es.telemetry.enabled = false" should "disable the daily ping" in {
    configFrom("softclient4es.telemetry.enabled = false").enabled shouldBe false
  }

  "an explicit softclient4es.telemetry.enabled = true" should "enable the daily ping" in {
    configFrom("softclient4es.telemetry.enabled = true").enabled shouldBe true
  }

  // The daily-ping key MUST NOT be conflated with the license-refresh metrics key.
  "softclient4es.telemetry.enabled" should "be INDEPENDENT of softclient4es.license.telemetry.enabled" in {
    // flip ONLY the license key -> the daily-ping key keeps its default (true)
    configFrom("softclient4es.license.telemetry.enabled = false").enabled shouldBe true
    // flip ONLY the daily-ping key -> the license key is unaffected
    val cfg = ConfigFactory
      .parseString("softclient4es.telemetry.enabled = false")
      .withFallback(ConfigFactory.load())
    TelemetryConfig.load(cfg).enabled shouldBe false
    LicenseConfig.load(cfg).telemetryEnabled shouldBe true
  }

  "TelemetryConfig.load with no telemetry block at all" should "default to true" in {
    // a bare config with no softclient4es.telemetry path -> default true (opt-out, not opt-in)
    TelemetryConfig.load(ConfigFactory.parseString("foo = bar")).enabled shouldBe true
  }
}
