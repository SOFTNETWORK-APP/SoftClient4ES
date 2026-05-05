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

class PlatformSpec extends AnyFlatSpec with Matchers {

  "Platform.fromString" should "parse PRODUCTION" in {
    Platform.fromString("PRODUCTION") shouldBe Some(Platform.Production)
  }

  it should "parse case-insensitively" in {
    Platform.fromString("staging") shouldBe Some(Platform.Staging)
    Platform.fromString("Integration") shouldBe Some(Platform.Integration)
    Platform.fromString("development") shouldBe Some(Platform.Development)
  }

  it should "return None for unknown values" in {
    Platform.fromString("UNKNOWN") shouldBe None
    Platform.fromString("CUSTOM_ENV") shouldBe None
    Platform.fromString("") shouldBe None
  }

  it should "trim whitespace" in {
    Platform.fromString("  PRODUCTION  ") shouldBe Some(Platform.Production)
  }

  "Platform.toString" should "return uppercase name" in {
    Platform.Production.toString shouldBe "PRODUCTION"
    Platform.Staging.toString shouldBe "STAGING"
    Platform.Integration.toString shouldBe "INTEGRATION"
    Platform.Development.toString shouldBe "DEVELOPMENT"
  }

  it should "round-trip through fromString" in {
    Platform.values.foreach { p =>
      Platform.fromString(p.toString) shouldBe Some(p)
    }
  }

  "Platform.values" should "contain all four platforms" in {
    Platform.values should have size 4
  }
}
