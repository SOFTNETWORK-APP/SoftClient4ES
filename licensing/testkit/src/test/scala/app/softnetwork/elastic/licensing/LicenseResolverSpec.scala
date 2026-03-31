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

import java.util.Date
import scala.concurrent.duration._

class LicenseResolverSpec extends AnyFlatSpec with Matchers {

  private def manager = new JwtLicenseManager(
    publicKeyOverride = Some(JwtTestHelper.publicKey)
  )

  private def defaultConfig(
    licenseKey: Option[String] = None,
    apiKey: Option[String] = None
  ): LicenseConfig =
    LicenseConfig(
      key = licenseKey,
      apiKey = apiKey,
      refreshEnabled = true,
      refreshInterval = 24.hours,
      telemetryEnabled = true,
      gracePeriod = 14.days,
      cacheDir = "/tmp/test-cache"
    )

  private def validProJwt: String =
    JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())

  private def expiredJwt(daysAgo: Int): String = {
    val expDate = new Date(System.currentTimeMillis() - daysAgo.toLong * 24 * 3600000L)
    JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
  }

  // --- Step 1: Static JWT ---

  "LicenseResolver with valid static JWT" should "resolve to that JWT's tier" in {
    val m = manager
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(validProJwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver with valid static JWT + API key" should "use static JWT, not call API key" in {
    val m = manager
    var apiKeyCalled = false
    val fetcher: String => Either[LicenseError, String] = { _ =>
      apiKeyCalled = true
      Right(validProJwt)
    }
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(validProJwt), apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    apiKeyCalled shouldBe false
  }

  "LicenseResolver with expired static JWT within grace period" should "resolve with grace" in {
    val m = manager
    val jwt = expiredJwt(1) // expired 1 day ago, grace = 14 days
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(jwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  // --- Step 1 → Step 2 fallthrough ---

  "LicenseResolver with expired static JWT beyond grace + API key" should "call API key fetcher" in {
    val m = manager
    val jwt = expiredJwt(30) // expired 30 days ago, grace = 14 days
    val freshJwt = validProJwt
    val fetcher: String => Either[LicenseError, String] = { _ => Right(freshJwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(jwt), apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver with invalid static JWT (bad signature)" should "fall through to API key" in {
    val m = manager
    val jwt = validProJwt
    val tampered = jwt.split("\\.")(0) + "." + jwt.split("\\.")(1) + "." + jwt
      .split("\\.")(2)
      .reverse
    var apiKeyCalled = false
    val fetcher: String => Either[LicenseError, String] = { _ =>
      apiKeyCalled = true
      Right(validProJwt)
    }
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(tampered), apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    resolver.resolve()
    apiKeyCalled shouldBe true
  }

  // --- Step 2: API key ---

  "LicenseResolver with API key but no fetcher" should "skip step 2 and degrade to Community" in {
    val m = manager
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = None
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Community
  }

  "LicenseResolver with no static JWT + API key fetch succeeds" should "resolve to fetched JWT" in {
    val m = manager
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  // --- Step 2 → Step 3 fallthrough ---

  "LicenseResolver with API key fetch fails + cache hit" should "resolve to cached JWT" in {
    val m = manager
    val cachedJwt = validProJwt
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(cachedJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver with cached JWT expired within grace" should "resolve with grace" in {
    val m = manager
    val cachedJwt = expiredJwt(1) // 1 day ago, grace = 14 days
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(cachedJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver with cached JWT expired beyond grace" should "degrade to Community" in {
    val m = manager
    val cachedJwt = expiredJwt(30)
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(cachedJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Community
  }

  // --- Step 3 → Step 4 fallthrough ---

  "LicenseResolver with API key fetch fails + no cache" should "degrade to Community" in {
    val m = manager
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Community
  }

  // --- Step 4: Community default ---

  "LicenseResolver with no JWT and no API key" should "default to Community" in {
    val m = manager
    val resolver = new LicenseResolver(
      config = defaultConfig(),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key shouldBe LicenseKey.Community
  }

  // --- resetToCommunity on re-resolution ---

  "LicenseResolver degrading to Community" should "reset manager state from prior Pro" in {
    val m = manager
    // First, resolve with a valid Pro JWT
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey =Some(validProJwt)),
      jwtLicenseManager = m
    )
    resolver1.resolve()
    m.licenseType shouldBe LicenseType.Pro

    // Now resolve with nothing → should reset to Community
    val resolver2 = new LicenseResolver(
      config = defaultConfig(),
      jwtLicenseManager = m
    )
    resolver2.resolve()
    m.licenseType shouldBe LicenseType.Community
    m.quotas shouldBe Quota.Community
  }
}
