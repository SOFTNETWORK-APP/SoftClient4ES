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

class LicenseTypeFromStringSpec extends AnyFlatSpec with Matchers {

  "LicenseType.fromString" should "map community" in {
    LicenseType.fromString("community") shouldBe LicenseType.Community
  }

  it should "map pro" in {
    LicenseType.fromString("pro") shouldBe LicenseType.Pro
  }

  it should "map enterprise" in {
    LicenseType.fromString("enterprise") shouldBe LicenseType.Enterprise
  }

  it should "default to Community for unknown string" in {
    LicenseType.fromString("platinum") shouldBe LicenseType.Community
  }

  it should "be case-insensitive" in {
    LicenseType.fromString("PRO") shouldBe LicenseType.Pro
    LicenseType.fromString("Enterprise") shouldBe LicenseType.Enterprise
  }

  it should "handle whitespace" in {
    LicenseType.fromString("  pro  ") shouldBe LicenseType.Pro
  }
}
