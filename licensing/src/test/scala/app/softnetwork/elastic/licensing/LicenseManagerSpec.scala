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

  "DefaultLicenseManager with Community license" should "include MaterializedViews" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.MaterializedViews) shouldBe true
  }

  it should "include JdbcDriver" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.JdbcDriver) shouldBe true
  }

  it should "not include FlightSql" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.FlightSql) shouldBe false
  }

  it should "not include Federation" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.Federation) shouldBe false
  }

  it should "not include OdbcDriver" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.OdbcDriver) shouldBe false
  }

  it should "not include UnlimitedResults" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.UnlimitedResults) shouldBe false
  }

  it should "not include AdvancedAggregations" in {
    val manager = new DefaultLicenseManager
    manager.hasFeature(Feature.AdvancedAggregations) shouldBe false
  }

  it should "return Community quotas" in {
    val manager = new DefaultLicenseManager
    manager.quotas shouldBe Quota.Community
  }

  "DefaultLicenseManager with Pro license" should "include FlightSql" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.hasFeature(Feature.FlightSql) shouldBe true
  }

  it should "include MaterializedViews" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.hasFeature(Feature.MaterializedViews) shouldBe true
  }

  it should "include JdbcDriver" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.hasFeature(Feature.JdbcDriver) shouldBe true
  }

  it should "include UnlimitedResults" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.hasFeature(Feature.UnlimitedResults) shouldBe true
  }

  it should "not include Federation" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.hasFeature(Feature.Federation) shouldBe false
  }

  it should "return Pro quotas" in {
    val manager = new DefaultLicenseManager
    manager.validate("PRO-test-key")
    manager.quotas shouldBe Quota.Pro
  }

  "DefaultLicenseManager with Enterprise license" should "include FlightSql" in {
    val manager = new DefaultLicenseManager
    manager.validate("ENT-test-key")
    manager.hasFeature(Feature.FlightSql) shouldBe true
  }

  it should "include Federation" in {
    val manager = new DefaultLicenseManager
    manager.validate("ENT-test-key")
    manager.hasFeature(Feature.Federation) shouldBe true
  }

  it should "include all features" in {
    val manager = new DefaultLicenseManager
    manager.validate("ENT-test-key")
    Feature.values.foreach { feature =>
      manager.hasFeature(feature) shouldBe true
    }
  }

  it should "return Enterprise quotas" in {
    val manager = new DefaultLicenseManager
    manager.validate("ENT-test-key")
    manager.quotas shouldBe Quota.Enterprise
  }

  "LicenseManager trait" should "be source-compatible" in {
    val manager: LicenseManager = new DefaultLicenseManager
    manager.licenseType shouldBe LicenseType.Community
    manager.quotas shouldBe Quota.Community
  }
}
