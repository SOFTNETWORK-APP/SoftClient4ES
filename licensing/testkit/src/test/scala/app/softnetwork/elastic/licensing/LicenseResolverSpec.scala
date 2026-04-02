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

import java.nio.file.Files
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
      apiUrl = "https://license.softclient4es.com",
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
      config = defaultConfig(licenseKey = Some(validProJwt)),
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
      config = defaultConfig(licenseKey = Some(validProJwt), apiKey = Some("sk-test")),
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
      config = defaultConfig(licenseKey = Some(jwt)),
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
      config = defaultConfig(licenseKey = Some(jwt), apiKey = Some("sk-test")),
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
      config = defaultConfig(licenseKey = Some(tampered), apiKey = Some("sk-test")),
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

  "LicenseResolver with no static JWT + API key fetch succeeds" should "resolve to fetched JWT and store in manager" in {
    val m = manager
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    // AC #8: JWT stored in memory for per-request enforcement
    m.licenseType shouldBe LicenseType.Pro
    m.quotas shouldBe Quota.Pro
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

  // --- Tier change logging (AC #7) ---

  "LicenseResolver cold start upgrade (Community -> Pro)" should "resolve to Pro with correct manager state" in {
    val m = manager
    // Fresh manager defaults to Community
    m.licenseType shouldBe LicenseType.Community
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.licenseType shouldBe LicenseType.Pro
    m.quotas shouldBe Quota.Pro
  }

  "LicenseResolver upgrade (Pro -> Enterprise)" should "resolve to Enterprise" in {
    val m = manager
    // First resolve with Pro via static key
    val proJwt = validProJwt
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(proJwt)),
      jwtLicenseManager = m
    )
    resolver1.resolve()
    m.licenseType shouldBe LicenseType.Pro

    // Second resolve with Enterprise via API key fetcher
    val entJwt = JwtTestHelper.signJwt(JwtTestHelper.enterpriseClaimsBuilder().build())
    val fetcher: String => Either[LicenseError, String] = { _ => Right(entJwt) }
    val resolver2 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver2.resolve()
    key.licenseType shouldBe LicenseType.Enterprise
    m.licenseType shouldBe LicenseType.Enterprise
  }

  "LicenseResolver downgrade (Pro -> Community)" should "resolve to Community" in {
    val m = manager
    // First resolve with Pro via static key
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(validProJwt)),
      jwtLicenseManager = m
    )
    resolver1.resolve()
    m.licenseType shouldBe LicenseType.Pro

    // Second resolve with Community via API key fetcher
    val communityJwt = JwtTestHelper.signJwt(JwtTestHelper.communityClaimsBuilder().build())
    val fetcher: String => Either[LicenseError, String] = { _ => Right(communityJwt) }
    val resolver2 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver2.resolve()
    key.licenseType shouldBe LicenseType.Community
    m.licenseType shouldBe LicenseType.Community
    m.quotas shouldBe Quota.Community
  }

  "LicenseResolver same tier (Pro -> Pro)" should "resolve to Pro without tier change" in {
    val m = manager
    // First resolve with Pro via static key
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(validProJwt)),
      jwtLicenseManager = m
    )
    resolver1.resolve()
    m.licenseType shouldBe LicenseType.Pro

    // Second resolve with different Pro JWT via API key fetcher
    val anotherProJwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val fetcher: String => Either[LicenseError, String] = { _ => Right(anotherProJwt) }
    val resolver2 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver2.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.licenseType shouldBe LicenseType.Pro
  }

  // --- resetToCommunity on re-resolution ---

  "LicenseResolver degrading to Community" should "reset manager state from prior Pro" in {
    val m = manager
    // First, resolve with a valid Pro JWT
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(validProJwt)),
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

  // --- Cache writer/invalidator (Story 5.5) ---

  "LicenseResolver cache writer on API key fetch success" should "write JWT to cache" in {
    val m = manager
    var cachedJwt: Option[String] = None
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val writer: String => Unit = { jwt => cachedJwt = Some(jwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheWriter = Some(writer)
    )
    resolver.resolve()
    cachedJwt shouldBe Some(validProJwt)
  }

  "LicenseResolver cache writer on static JWT success" should "not write to cache" in {
    val m = manager
    var cacheWriteCalled = false
    val writer: String => Unit = { _ => cacheWriteCalled = true }
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(validProJwt)),
      jwtLicenseManager = m,
      cacheWriter = Some(writer)
    )
    resolver.resolve()
    cacheWriteCalled shouldBe false
  }

  "LicenseResolver cache invalidator on stale cached JWT" should "delete cache file" in {
    val m = manager
    var invalidateCalled = false
    val invalidator: () => Unit = () => { invalidateCalled = true }
    val staleJwt = expiredJwt(30) // expired beyond 14-day grace
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(staleJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader),
      cacheInvalidator = Some(invalidator)
    )
    resolver.resolve()
    invalidateCalled shouldBe true
  }

  "LicenseResolver cache invalidator on successful resolution" should "not be called" in {
    val m = manager
    var invalidateCalled = false
    val invalidator: () => Unit = () => { invalidateCalled = true }
    val cachedJwt = validProJwt
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(cachedJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader),
      cacheInvalidator = Some(invalidator)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    invalidateCalled shouldBe false
  }

  "LicenseResolver cache round-trip" should "use cached JWT when API key fetch fails on second resolve" in {
    val m = manager
    var cachedJwt: Option[String] = None
    val proJwt = validProJwt

    // First resolve: API key succeeds, writes to cache
    var fetchSucceeds = true
    val fetcher: String => Either[LicenseError, String] = { _ =>
      if (fetchSucceeds) Right(proJwt)
      else Left(InvalidLicense("Network error"))
    }
    val writer: String => Unit = { jwt => cachedJwt = Some(jwt) }
    val reader: () => Option[String] = () => cachedJwt

    val resolver1 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader),
      cacheWriter = Some(writer)
    )
    resolver1.resolve().licenseType shouldBe LicenseType.Pro
    cachedJwt shouldBe Some(proJwt)

    // Second resolve: API key fails, falls back to cache
    fetchSucceeds = false
    val resolver2 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader),
      cacheWriter = Some(writer)
    )
    resolver2.resolve().licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver cache writer failure" should "still resolve successfully" in {
    val m = manager
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val writer: String => Unit = { _ => throw new RuntimeException("Disk full") }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheWriter = Some(writer)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }

  "LicenseResolver with real LicenseCache" should "write and delete cache file" in {
    val m = manager
    val tempDir = Files.createTempDirectory("license-resolver-cache-test")
    try {
      val cache = new LicenseCache(tempDir.toString)
      val proJwt = validProJwt

      // First resolve: API key succeeds -> writes to disk
      val fetcher: String => Either[LicenseError, String] = { _ => Right(proJwt) }
      val resolver1 = new LicenseResolver(
        config = defaultConfig(apiKey = Some("sk-test")),
        jwtLicenseManager = m,
        apiKeyFetcher = Some(fetcher),
        cacheReader = Some(() => cache.read()),
        cacheWriter = Some(jwt => cache.write(jwt)),
        cacheInvalidator = Some(() => cache.delete())
      )
      resolver1.resolve().licenseType shouldBe LicenseType.Pro
      val cacheFile = tempDir.resolve(LicenseCache.CacheFileName)
      Files.exists(cacheFile) shouldBe true
      cache.read() shouldBe Some(proJwt)

      // Second resolve: stale cache -> invalidated
      val staleJwt = expiredJwt(30)
      cache.write(staleJwt)
      val failingFetcher: String => Either[LicenseError, String] = { _ =>
        Left(InvalidLicense("Network error"))
      }
      val resolver2 = new LicenseResolver(
        config = defaultConfig(apiKey = Some("sk-test")),
        jwtLicenseManager = m,
        apiKeyFetcher = Some(failingFetcher),
        cacheReader = Some(() => cache.read()),
        cacheWriter = Some(jwt => cache.write(jwt)),
        cacheInvalidator = Some(() => cache.delete())
      )
      resolver2.resolve().licenseType shouldBe LicenseType.Community
      Files.exists(cacheFile) shouldBe false
    } finally {
      val stream = Files.list(tempDir)
      try { stream.forEach(f => Files.deleteIfExists(f)) }
      finally { stream.close() }
      Files.deleteIfExists(tempDir)
    }
  }

  // --- Grace status at startup (AC #1, #2, #3) ---

  "LicenseResolver with early grace static JWT" should "resolve with full access" in {
    val m = manager
    val jwt = expiredJwt(3) // 3 days ago, grace = 14d, earlyThreshold = 7d
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(jwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.graceStatus shouldBe a[GraceStatus.EarlyGrace]
  }

  "LicenseResolver with mid-grace static JWT" should "resolve with full access" in {
    val m = manager
    val jwt = expiredJwt(10) // 10 days ago, grace = 14d
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(jwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
  }

  "LicenseResolver with beyond-grace static JWT and no API key" should "degrade to Community" in {
    val m = manager
    val jwt = expiredJwt(30) // 30 days ago, grace = 14d
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(jwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Community
    m.wasDegraded shouldBe true
  }

  "LicenseResolver with mid-grace cached JWT" should "resolve with full access and correct grace status" in {
    val m = manager
    val jwt = expiredJwt(10)
    val fetcher: String => Either[LicenseError, String] = { _ =>
      Left(InvalidLicense("Network error"))
    }
    val reader: () => Option[String] = () => Some(jwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher),
      cacheReader = Some(reader)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
  }

  // --- Restoration detection (AC #4) ---

  "LicenseResolver restoration after degradation" should "log restored when renewed via API key" in {
    val m = manager
    // Simulate degradation: resolve with expired JWT, no API key
    val expJwt = expiredJwt(30)
    val resolver1 = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(expJwt)),
      jwtLicenseManager = m
    )
    resolver1.resolve().licenseType shouldBe LicenseType.Community
    m.wasDegraded shouldBe true

    // Restore via API key
    val freshJwt = validProJwt
    val fetcher: String => Either[LicenseError, String] = { _ => Right(freshJwt) }
    val resolver2 = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    val key = resolver2.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.wasDegraded shouldBe false
  }

  "LicenseResolver restoration after degradation" should "log restored when renewed via static JWT" in {
    val m = manager
    // Degrade
    m.resetToCommunity()
    m.wasDegraded shouldBe true

    // Restore via static JWT
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(validProJwt)),
      jwtLicenseManager = m
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
    m.wasDegraded shouldBe false
  }

  "LicenseResolver cold start" should "not report restoration" in {
    val m = manager
    m.wasDegraded shouldBe false
    val fetcher: String => Either[LicenseError, String] = { _ => Right(validProJwt) }
    val resolver = new LicenseResolver(
      config = defaultConfig(apiKey = Some("sk-test")),
      jwtLicenseManager = m,
      apiKeyFetcher = Some(fetcher)
    )
    resolver.resolve().licenseType shouldBe LicenseType.Pro
    m.wasDegraded shouldBe false
  }

  "LicenseResolver cache fallback without API key" should "use cached JWT when static JWT is invalid" in {
    val m = manager
    val cachedJwt = validProJwt
    val tampered = validProJwt.split("\\.")(0) + "." + validProJwt.split("\\.")(1) + "." +
      validProJwt.split("\\.")(2).reverse
    val reader: () => Option[String] = () => Some(cachedJwt)
    val resolver = new LicenseResolver(
      config = defaultConfig(licenseKey = Some(tampered)),
      jwtLicenseManager = m,
      cacheReader = Some(reader)
    )
    val key = resolver.resolve()
    key.licenseType shouldBe LicenseType.Pro
  }
}
