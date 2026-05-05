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

class LicenseManagerSpec extends AnyFlatSpec with Matchers {

  private val manager = new CommunityLicenseManager

  "CommunityLicenseManager" should "include MaterializedViews" in {
    manager.hasFeature(Feature.MaterializedViews) shouldBe true
  }

  it should "include JdbcDriver" in {
    manager.hasFeature(Feature.JdbcDriver) shouldBe true
  }

  it should "include FlightSql" in {
    manager.hasFeature(Feature.FlightSql) shouldBe true
  }

  it should "not include Federation" in {
    manager.hasFeature(Feature.Federation) shouldBe false
  }

  it should "not include OdbcDriver" in {
    manager.hasFeature(Feature.OdbcDriver) shouldBe false
  }

  it should "not include UnlimitedResults" in {
    manager.hasFeature(Feature.UnlimitedResults) shouldBe false
  }

  it should "not include AdvancedAggregations" in {
    manager.hasFeature(Feature.AdvancedAggregations) shouldBe false
  }

  it should "return Community quotas" in {
    manager.quotas shouldBe Quota.Community
  }

  it should "always be Community type" in {
    manager.licenseType shouldBe LicenseType.Community
  }

  it should "reject any key validation" in {
    manager.validate("PRO-test-key") shouldBe a[Left[_, _]]
    manager.validate("ENT-test-key") shouldBe a[Left[_, _]]
    manager.validate("anything") shouldBe a[Left[_, _]]
    // State remains Community after rejected validation
    manager.licenseType shouldBe LicenseType.Community
    manager.quotas shouldBe Quota.Community
  }

  it should "return Left(RefreshNotSupported) on refresh" in {
    manager.refresh() shouldBe Left(RefreshNotSupported)
  }

  it should "return LicenseKey.Community as currentLicenseKey" in {
    manager.currentLicenseKey shouldBe LicenseKey.Community
  }

  it should "not be a trial" in {
    manager.isTrial shouldBe false
  }

  "LicenseManager trait" should "be source-compatible" in {
    val m: LicenseManager = new CommunityLicenseManager
    m.licenseType shouldBe LicenseType.Community
    m.quotas shouldBe Quota.Community
  }

  it should "default refresh to Left(RefreshNotSupported)" in {
    val stub = new LicenseManager {
      def validate(key: String): Either[LicenseError, LicenseKey] = Left(InvalidLicense("stub"))
      def hasFeature(feature: Feature): Boolean = false
      def quotas: Quota = Quota.Community
      def licenseType: LicenseType = LicenseType.Community
    }
    stub.refresh() shouldBe Left(RefreshNotSupported)
  }

  it should "default currentLicenseKey to LicenseKey.Community" in {
    val stub = new LicenseManager {
      def validate(key: String): Either[LicenseError, LicenseKey] = Left(InvalidLicense("stub"))
      def hasFeature(feature: Feature): Boolean = false
      def quotas: Quota = Quota.Community
      def licenseType: LicenseType = LicenseType.Community
    }
    stub.currentLicenseKey shouldBe LicenseKey.Community
    stub.isTrial shouldBe false
  }

  "DefaultLicenseManager" should "be a deprecated alias for CommunityLicenseManager" in {
    val m: DefaultLicenseManager = new CommunityLicenseManager
    m shouldBe a[CommunityLicenseManager]
  }
}
