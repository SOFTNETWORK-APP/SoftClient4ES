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

import java.time.{Duration, Instant}

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
    key.features should have size 6
    key.features should contain(Feature.MaterializedViews)
    key.features should contain(Feature.JdbcDriver)
    key.features should contain(Feature.AdbcDriver)
    key.features should contain(Feature.FlightSql)
    key.features should contain(Feature.Federation)
    key.features should contain(Feature.Repl)
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

  "isTrial" should "return true when trial field is set" in {
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.now().plus(Duration.ofDays(30))),
      trial = true
    )
    key.isTrial shouldBe true
  }

  it should "return false for paid Pro" in {
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.now().plus(Duration.ofDays(365)))
    )
    key.isTrial shouldBe false
  }

  it should "return false when trial defaults to false" in {
    LicenseKey.Community.isTrial shouldBe false
  }

  "daysRemaining" should "compute days until expiry" in {
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.now().plus(Duration.ofDays(15))),
      metadata = Map.empty
    )
    key.daysRemaining.get should (be >= 14L and be <= 15L)
  }

  it should "return None for Community (no expiry)" in {
    LicenseKey.Community.daysRemaining shouldBe None
  }

  it should "return negative for expired keys" in {
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.now().minus(Duration.ofDays(5))),
      metadata = Map.empty
    )
    key.daysRemaining.get should be < 0L
  }

  "daysRemainingAt" should "be deterministic with a fixed instant" in {
    val now = Instant.parse("2026-01-15T00:00:00Z")
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.parse("2026-01-25T00:00:00Z")),
      metadata = Map.empty
    )
    key.daysRemainingAt(now) shouldBe Some(10L)
  }

  "daysSinceExpiryAt" should "return positive for expired keys" in {
    val now = Instant.parse("2026-01-20T00:00:00Z")
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.parse("2026-01-15T00:00:00Z")),
      metadata = Map.empty
    )
    key.daysSinceExpiryAt(now) shouldBe Some(5L)
  }

  it should "return negative for not-yet-expired keys" in {
    val now = Instant.parse("2026-01-10T00:00:00Z")
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.parse("2026-01-15T00:00:00Z")),
      metadata = Map.empty
    )
    key.daysSinceExpiryAt(now) shouldBe Some(-5L)
  }

  it should "be consistent with daysRemainingAt (sum to zero)" in {
    val now = Instant.parse("2026-01-15T00:00:00Z")
    val key = LicenseKey(
      id = "org-123",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews),
      expiresAt = Some(Instant.parse("2026-01-22T00:00:00Z")),
      metadata = Map.empty
    )
    val remaining = key.daysRemainingAt(now).get
    val expired = key.daysSinceExpiryAt(now).get
    (remaining + expired) shouldBe 0L
  }

  "LicenseKey.Community" should "carry Community quota with no usage or platform" in {
    LicenseKey.Community.quota shouldBe Some(Quota.Community)
    LicenseKey.Community.usage shouldBe None
    LicenseKey.Community.platform shouldBe None
  }

  "LicenseKey with all new fields" should "preserve quota, usage, and platform" in {
    val key = LicenseKey(
      id = "pro-key",
      licenseType = LicenseType.Pro,
      features = Set(Feature.MaterializedViews, Feature.Federation),
      expiresAt = Some(Instant.now().plus(Duration.ofDays(365))),
      metadata = Map("org_name" -> "Acme Corp"),
      quota = Some(Quota.Pro),
      usage = Some(LicenseUsage(totalMvsActive = 10, totalFederatedClusters = 2)),
      platform = Some(Platform.Production)
    )
    key.quota shouldBe Some(Quota.Pro)
    key.usage shouldBe Some(LicenseUsage(totalMvsActive = 10, totalFederatedClusters = 2))
    key.platform shouldBe Some(Platform.Production)
  }

  it should "support equality with identical field values" in {
    val now = Instant.now()
    val usage = LicenseUsage(totalMvsActive = 5, totalFederatedClusters = 1)
    val key1 = LicenseKey(
      "k",
      LicenseType.Pro,
      Set.empty,
      Some(now),
      quota = Some(Quota.Pro),
      usage = Some(usage),
      platform = Some(Platform.Staging)
    )
    val key2 = LicenseKey(
      "k",
      LicenseType.Pro,
      Set.empty,
      Some(now),
      quota = Some(Quota.Pro),
      usage = Some(usage),
      platform = Some(Platform.Staging)
    )
    key1 shouldBe key2
  }
}
