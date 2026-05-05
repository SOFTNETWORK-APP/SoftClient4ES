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

class LicenseUsageSpec extends AnyFlatSpec with Matchers {

  "LicenseUsage" should "default to zero counters" in {
    val usage = LicenseUsage()
    usage.totalMvsActive shouldBe 0
    usage.totalFederatedClusters shouldBe 0
  }

  it should "reject negative totalMvsActive" in {
    an[IllegalArgumentException] should be thrownBy LicenseUsage(totalMvsActive = -1)
  }

  it should "reject negative totalFederatedClusters" in {
    an[IllegalArgumentException] should be thrownBy LicenseUsage(totalFederatedClusters = -1)
  }

  it should "accept boundary values" in {
    noException should be thrownBy LicenseUsage(totalMvsActive = 0, totalFederatedClusters = 0)
    noException should be thrownBy LicenseUsage(
      totalMvsActive = Int.MaxValue,
      totalFederatedClusters = Int.MaxValue
    )
  }

  "LicenseUsage.Empty" should "equal default-constructed LicenseUsage" in {
    LicenseUsage.Empty shouldBe LicenseUsage()
  }

  "checkQuota for MaterializedViews" should "return None when within quota" in {
    val usage = LicenseUsage(totalMvsActive = 2)
    usage.checkQuota(Feature.MaterializedViews, Quota.Community) shouldBe None
  }

  it should "return None at exact quota boundary" in {
    val usage = LicenseUsage(totalMvsActive = 3)
    usage.checkQuota(Feature.MaterializedViews, Quota.Community) shouldBe None
  }

  it should "detect exceeded maxMaterializedViews" in {
    val usage = LicenseUsage(totalMvsActive = 4)
    usage.checkQuota(Feature.MaterializedViews, Quota.Community) shouldBe Some(
      QuotaExceeded("maxMaterializedViews", 4, 3)
    )
  }

  it should "not check maxClusters" in {
    val usage = LicenseUsage(totalMvsActive = 0, totalFederatedClusters = 100)
    usage.checkQuota(Feature.MaterializedViews, Quota.Community) shouldBe None
  }

  it should "return None for Enterprise (unlimited)" in {
    val usage = LicenseUsage(totalMvsActive = 1000)
    usage.checkQuota(Feature.MaterializedViews, Quota.Enterprise) shouldBe None
  }

  "checkQuota for Federation" should "return None when within quota" in {
    val usage = LicenseUsage(totalFederatedClusters = 4)
    usage.checkQuota(Feature.Federation, Quota.Pro) shouldBe None
  }

  it should "detect exceeded maxClusters" in {
    val usage = LicenseUsage(totalFederatedClusters = 6)
    usage.checkQuota(Feature.Federation, Quota.Pro) shouldBe Some(
      QuotaExceeded("maxClusters", 6, 5)
    )
  }

  it should "not check maxMaterializedViews" in {
    val usage = LicenseUsage(totalMvsActive = 100, totalFederatedClusters = 0)
    usage.checkQuota(Feature.Federation, Quota.Community) shouldBe None
  }

  it should "return None for Enterprise (unlimited)" in {
    val usage = LicenseUsage(totalFederatedClusters = 100)
    usage.checkQuota(Feature.Federation, Quota.Enterprise) shouldBe None
  }

  "checkQuota for other features" should "always return None" in {
    val usage = LicenseUsage(totalMvsActive = 100, totalFederatedClusters = 100)
    usage.checkQuota(Feature.JdbcDriver, Quota.Community) shouldBe None
    usage.checkQuota(Feature.FlightSql, Quota.Community) shouldBe None
    usage.checkQuota(Feature.UnlimitedResults, Quota.Community) shouldBe None
  }
}
