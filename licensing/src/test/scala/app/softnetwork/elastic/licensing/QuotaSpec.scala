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

class QuotaSpec extends AnyFlatSpec with Matchers {

  "Quota.Community" should "have maxClusters = Some(1)" in {
    Quota.Community.maxClusters shouldBe Some(1)
  }

  it should "have maxJoins = Some(2)" in {
    Quota.Community.maxJoins shouldBe Some(2)
  }

  it should "have maxMaterializedViews = Some(1)" in {
    Quota.Community.maxMaterializedViews shouldBe Some(1)
  }

  it should "have maxQueryResults = Some(10000)" in {
    Quota.Community.maxQueryResults shouldBe Some(10000)
  }

  "Quota.Pro" should "have maxClusters = Some(5)" in {
    Quota.Pro.maxClusters shouldBe Some(5)
  }

  it should "have maxJoins = Some(5)" in {
    Quota.Pro.maxJoins shouldBe Some(5)
  }

  it should "have maxMaterializedViews = Some(50)" in {
    Quota.Pro.maxMaterializedViews shouldBe Some(50)
  }

  it should "have maxQueryResults = Some(1000000)" in {
    Quota.Pro.maxQueryResults shouldBe Some(1000000)
  }

  "Quota.Enterprise" should "have maxClusters = None (unlimited)" in {
    Quota.Enterprise.maxClusters shouldBe None
  }

  it should "have maxJoins = None (unlimited)" in {
    Quota.Enterprise.maxJoins shouldBe None
  }

  it should "have maxMaterializedViews = None (unlimited)" in {
    Quota.Enterprise.maxMaterializedViews shouldBe None
  }

  it should "have maxQueryResults = None (unlimited)" in {
    Quota.Enterprise.maxQueryResults shouldBe None
  }

  "Quota default constructor" should "use maxClusters = Some(0)" in {
    val quota = Quota(
      maxMaterializedViews = Some(10),
      maxQueryResults = Some(100)
    )
    quota.maxClusters shouldBe Some(0)
    quota.maxJoins shouldBe Some(0)
  }
}
