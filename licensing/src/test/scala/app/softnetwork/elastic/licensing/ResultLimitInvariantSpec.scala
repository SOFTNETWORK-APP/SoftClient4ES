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

/** ADR D3 invariant: "unlimited results" has ONE source of truth — `Quota.maxQueryResults` — not a
  * removed `UnlimitedResults` feature flag. Community = Some(10000), Pro = Some(1000000),
  * Enterprise = None (unlimited).
  */
class ResultLimitInvariantSpec extends AnyFlatSpec with Matchers {

  "Result-limit policy" should "be driven solely by Quota.maxQueryResults per tier" in {
    Quota.Community.maxQueryResults shouldBe Some(10000)
    Quota.Pro.maxQueryResults shouldBe Some(1000000)
    Quota.Enterprise.maxQueryResults shouldBe None // unlimited
  }

  it should "treat Enterprise (maxQueryResults = None) as the unlimited case" in {
    Quota.Enterprise.maxQueryResults.isEmpty shouldBe true
  }

  "The Feature enum" should "carry no result-limit flag" in {
    // The retired UnlimitedResults / AdvancedAggregations flags must not exist
    // under any (case-insensitive) spelling, and the catalog must stay at 6.
    Feature.values should have size 6
    Feature.values.map(Feature.toSnakeCase) should contain noneOf (
      "unlimited_results",
      "advanced_aggregations"
    )
    Feature.fromString("unlimited_results") shouldBe None
    Feature.fromString("advanced_aggregations") shouldBe None
  }

  "Community manager" should "express the result cap via quota, not a feature" in {
    val mgr = new CommunityLicenseManager
    mgr.quotas.maxQueryResults shouldBe Some(10000)
    // No flag encodes the limit:
    Feature.values.foreach(f =>
      mgr.hasFeature(f) shouldBe LicenseKey.Community.features.contains(f)
    )
  }
}
