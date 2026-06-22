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

/** Story 15.2 (A11/A13) -- `ProductType.values.map(_.displayName)` and
  * `LicenseType.values.map(_.displayName)` are the allowlist sources for the license-server
  * `InstancePingDecorator.validate()`. They MUST be the exact wire values the five surfaces emit.
  */
class ProductTypeSpec extends AnyFlatSpec with Matchers {

  "ProductType.values displayNames" should "be the five client surface snake feature names" in {
    ProductType.values.map(_.displayName) shouldBe Seq(
      "jdbc_driver",
      "flight_sql",
      "adbc_driver",
      "repl",
      "federation"
    )
  }

  "every ProductType" should "also be a Feature" in {
    ProductType.values.foreach { p =>
      p shouldBe a[Feature]
    }
  }

  "ProductType.displayName" should "equal Feature.toSnakeCase" in {
    ProductType.values.foreach { p =>
      p.displayName shouldBe Feature.toSnakeCase(p)
    }
  }

  "LicenseType.values displayNames" should "be the capitalised tier names" in {
    LicenseType.values.map(_.displayName) shouldBe Seq("Community", "Pro", "Enterprise")
  }
}
