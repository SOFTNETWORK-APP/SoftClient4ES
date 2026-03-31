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

class FeatureFromStringSpec extends AnyFlatSpec with Matchers {

  "Feature.fromString" should "map materialized_views" in {
    Feature.fromString("materialized_views") shouldBe Some(Feature.MaterializedViews)
  }

  it should "map jdbc_driver" in {
    Feature.fromString("jdbc_driver") shouldBe Some(Feature.JdbcDriver)
  }

  it should "map odbc_driver" in {
    Feature.fromString("odbc_driver") shouldBe Some(Feature.OdbcDriver)
  }

  it should "map unlimited_results" in {
    Feature.fromString("unlimited_results") shouldBe Some(Feature.UnlimitedResults)
  }

  it should "map advanced_aggregations" in {
    Feature.fromString("advanced_aggregations") shouldBe Some(Feature.AdvancedAggregations)
  }

  it should "map flight_sql" in {
    Feature.fromString("flight_sql") shouldBe Some(Feature.FlightSql)
  }

  it should "map federation" in {
    Feature.fromString("federation") shouldBe Some(Feature.Federation)
  }

  it should "return None for unknown string" in {
    Feature.fromString("warp_drive") shouldBe None
  }

  it should "be case-insensitive" in {
    Feature.fromString("MATERIALIZED_VIEWS") shouldBe Some(Feature.MaterializedViews)
    Feature.fromString("Flight_Sql") shouldBe Some(Feature.FlightSql)
  }

  it should "handle whitespace" in {
    Feature.fromString("  federation  ") shouldBe Some(Feature.Federation)
  }

  "Feature.toSnakeCase round-trip" should "work for all features" in {
    Feature.values.foreach { f =>
      Feature.fromString(Feature.toSnakeCase(f)) shouldBe Some(f)
    }
  }
}
