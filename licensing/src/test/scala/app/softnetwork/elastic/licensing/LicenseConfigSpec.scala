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

import scala.concurrent.duration._

class LicenseConfigSpec extends AnyFlatSpec with Matchers {

  private def configFrom(hocon: String): LicenseConfig =
    LicenseConfig.load(
      ConfigFactory
        .parseString(hocon)
        .withFallback(ConfigFactory.load())
    )

  "reference.conf defaults" should "load without explicit HOCON override" in {
    val cfg = LicenseConfig.load(ConfigFactory.load())
    cfg.key shouldBe None
    cfg.apiKey shouldBe None
    cfg.refreshEnabled shouldBe true
    cfg.refreshInterval shouldBe 24.hours
    cfg.telemetryEnabled shouldBe true
    cfg.gracePeriod shouldBe 14.days
    cfg.cacheDir should include(".softclient4es")
  }

  "HOCON with key" should "extract static JWT" in {
    val cfg = configFrom("""softclient4es.license.key = "eyJhbGciOiJFZERTQSJ9.test" """)
    cfg.key shouldBe Some("eyJhbGciOiJFZERTQSJ9.test")
  }

  "HOCON with api-key" should "extract API key" in {
    val cfg = configFrom("""softclient4es.license.api-key = "sk-abc123" """)
    cfg.apiKey shouldBe Some("sk-abc123")
  }

  "HOCON with neither key nor api-key" should "return None for both" in {
    val cfg = configFrom("") // just reference.conf defaults
    cfg.key shouldBe None
    cfg.apiKey shouldBe None
  }

  "HOCON with empty string key" should "treat as absent (blank-as-absent)" in {
    val cfg = configFrom("""softclient4es.license.key = "" """)
    cfg.key shouldBe None
  }

  "HOCON with whitespace-only api-key" should "treat as absent (blank-as-absent)" in {
    val cfg = configFrom("""softclient4es.license.api-key = "  " """)
    cfg.apiKey shouldBe None
  }

  "custom refresh settings" should "be parsed correctly" in {
    val cfg = configFrom("""
      softclient4es.license.refresh {
        enabled = false
        interval = 12h
      }
    """)
    cfg.refreshEnabled shouldBe false
    cfg.refreshInterval shouldBe 12.hours
  }

  "custom telemetry setting" should "be parsed correctly" in {
    val cfg = configFrom("""softclient4es.license.telemetry.enabled = false""")
    cfg.telemetryEnabled shouldBe false
  }

  "custom grace-period" should "be parsed correctly" in {
    val cfg = configFrom("""softclient4es.license.grace-period = 7d""")
    cfg.gracePeriod shouldBe 7.days
  }

  "custom cache-dir" should "be parsed correctly" in {
    val cfg = configFrom("""softclient4es.license.cache-dir = "/tmp/test-cache" """)
    cfg.cacheDir shouldBe "/tmp/test-cache"
  }

  "all custom settings" should "be parsed together" in {
    val cfg = configFrom("""
      softclient4es.license {
        key = "eyJ.test.jwt"
        api-key = "sk-custom"
        refresh {
          enabled = false
          interval = 6h
        }
        telemetry.enabled = false
        grace-period = 30d
        cache-dir = "/opt/licenses"
      }
    """)
    cfg.key shouldBe Some("eyJ.test.jwt")
    cfg.apiKey shouldBe Some("sk-custom")
    cfg.refreshEnabled shouldBe false
    cfg.refreshInterval shouldBe 6.hours
    cfg.telemetryEnabled shouldBe false
    cfg.gracePeriod shouldBe 30.days
    cfg.cacheDir shouldBe "/opt/licenses"
  }
}
