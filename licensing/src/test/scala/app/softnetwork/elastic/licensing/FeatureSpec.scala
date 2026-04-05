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

class FeatureSpec extends AnyFlatSpec with Matchers {

  "Feature.FlightSql" should "exist as a case object" in {
    Feature.FlightSql shouldBe a[Feature]
  }

  "Feature.Federation" should "exist as a case object" in {
    Feature.Federation shouldBe a[Feature]
  }

  "Feature.values" should "contain all 7 features" in {
    Feature.values should have size 7
  }

  it should "contain FlightSql and Federation" in {
    Feature.values should contain(Feature.FlightSql)
    Feature.values should contain(Feature.Federation)
  }

  it should "preserve insertion order" in {
    Feature.values shouldBe Seq(
      Feature.MaterializedViews,
      Feature.JdbcDriver,
      Feature.OdbcDriver,
      Feature.UnlimitedResults,
      Feature.AdvancedAggregations,
      Feature.FlightSql,
      Feature.Federation
    )
  }
}
