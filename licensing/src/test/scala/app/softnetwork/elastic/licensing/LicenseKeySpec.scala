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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LicenseKeySpec extends AnyFlatSpec with Matchers {

  "LicenseKey" should "support FlightSql in features set" in {
    val key = LicenseKey(
      id = "test-key",
      licenseType = LicenseType.Pro,
      features = Set(Feature.FlightSql),
      expiresAt = None
    )
    key.features should contain(Feature.FlightSql)
  }

  it should "support Federation in features set" in {
    val key = LicenseKey(
      id = "test-key",
      licenseType = LicenseType.Enterprise,
      features = Set(Feature.Federation),
      expiresAt = None
    )
    key.features should contain(Feature.Federation)
  }

  it should "support all features combined" in {
    val key = LicenseKey(
      id = "test-key",
      licenseType = LicenseType.Enterprise,
      features = Feature.values.toSet,
      expiresAt = None
    )
    key.features should have size 7
    key.features should contain(Feature.MaterializedViews)
    key.features should contain(Feature.JdbcDriver)
    key.features should contain(Feature.OdbcDriver)
    key.features should contain(Feature.UnlimitedResults)
    key.features should contain(Feature.AdvancedAggregations)
    key.features should contain(Feature.FlightSql)
    key.features should contain(Feature.Federation)
  }

  it should "store JWT metadata claims" in {
    val key = LicenseKey(
      id = "jwt-key",
      licenseType = LicenseType.Pro,
      features = Set(Feature.FlightSql),
      expiresAt = None,
      metadata = Map(
        "org_name" -> "Acme Corp",
        "jti"      -> "abc-123",
        "trial"    -> "true"
      )
    )
    key.metadata("org_name") shouldBe "Acme Corp"
    key.metadata("jti") shouldBe "abc-123"
    key.metadata("trial") shouldBe "true"
  }

  it should "default to empty metadata" in {
    val key = LicenseKey(
      id = "test-key",
      licenseType = LicenseType.Community,
      features = Set.empty,
      expiresAt = None
    )
    key.metadata shouldBe empty
  }
}
