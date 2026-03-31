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

import com.nimbusds.jose.jwk.Curve
import com.nimbusds.jose.jwk.gen.OctetKeyPairGenerator
import com.nimbusds.jwt.JWTClaimsSet
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Duration
import java.util.Date

class JwtLicenseManagerSpec extends AnyFlatSpec with Matchers {

  private def manager = new JwtLicenseManager(
    publicKeyOverride = Some(JwtTestHelper.publicKey)
  )

  "JwtLicenseManager default state" should "be Community tier" in {
    val m = manager
    m.licenseType shouldBe LicenseType.Community
    m.quotas shouldBe Quota.Community
  }

  "JwtLicenseManager with valid Pro JWT" should "return Right(LicenseKey) with correct tier" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val result = m.validate(jwt)
    result shouldBe a[Right[_, _]]
    val key = result.toOption.get
    key.licenseType shouldBe LicenseType.Pro
    key.id shouldBe "org-acme-123"
  }

  it should "have correct features" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    m.validate(jwt)
    m.hasFeature(Feature.MaterializedViews) shouldBe true
    m.hasFeature(Feature.JdbcDriver) shouldBe true
    m.hasFeature(Feature.UnlimitedResults) shouldBe true
    m.hasFeature(Feature.FlightSql) shouldBe true
    m.hasFeature(Feature.Federation) shouldBe false
    m.hasFeature(Feature.OdbcDriver) shouldBe false
  }

  it should "have JWT-embedded quotas (not tier defaults)" in {
    val m = manager
    val claims = JwtTestHelper
      .proClaimsBuilder()
      .claim(
        "quotas", {
          val q = new java.util.LinkedHashMap[String, AnyRef]()
          q.put("max_result_rows", Integer.valueOf(500000))
          q.put("max_clusters", Integer.valueOf(3))
          q
        }
      )
      .build()
    val jwt = JwtTestHelper.signJwt(claims)
    m.validate(jwt)
    m.quotas.maxQueryResults shouldBe Some(500000)
    m.quotas.maxClusters shouldBe Some(3)
    m.quotas.maxMaterializedViews shouldBe None // not in JWT → None (unlimited)
    m.quotas.maxConcurrentQueries shouldBe None
  }

  it should "have correct expiresAt" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() + 7200000L)
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    val result = m.validate(jwt)
    val key = result.toOption.get
    key.expiresAt shouldBe defined
    // JWT exp is truncated to seconds
    key.expiresAt.get.getEpochSecond shouldBe (expDate.getTime / 1000)
  }

  it should "populate metadata" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val result = m.validate(jwt)
    val key = result.toOption.get
    key.metadata("org_name") shouldBe "Acme Corp"
    key.metadata("jti") shouldBe "lic-001"
    key.metadata("trial") shouldBe "false"
  }

  "JwtLicenseManager with valid Enterprise JWT" should "have all 7 features" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.enterpriseClaimsBuilder().build())
    m.validate(jwt)
    Feature.values.foreach { feature =>
      m.hasFeature(feature) shouldBe true
    }
  }

  it should "have unlimited quotas when JWT quotas object is empty" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.enterpriseClaimsBuilder().build())
    m.validate(jwt)
    m.quotas.maxMaterializedViews shouldBe None
    m.quotas.maxQueryResults shouldBe None
    m.quotas.maxConcurrentQueries shouldBe None
    m.quotas.maxClusters shouldBe None
  }

  "JwtLicenseManager with valid Community JWT" should "have only community features" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.communityClaimsBuilder().build())
    m.validate(jwt)
    m.hasFeature(Feature.MaterializedViews) shouldBe true
    m.hasFeature(Feature.JdbcDriver) shouldBe true
    m.hasFeature(Feature.FlightSql) shouldBe false
    m.hasFeature(Feature.Federation) shouldBe false
  }

  "JwtLicenseManager with tampered signature" should "return Left(InvalidLicense)" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    // Tamper by replacing a character in the signature part
    val parts = jwt.split("\\.")
    val tampered = parts(0) + "." + parts(1) + "." + parts(2).reverse
    val result = m.validate(tampered)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get shouldBe a[InvalidLicense]
    result.left.toOption.get.message should include("Invalid signature")
  }

  "JwtLicenseManager with expired JWT" should "return Left(ExpiredLicense)" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() - 3600000L) // 1 hour ago
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    val result = m.validate(jwt)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get shouldBe an[ExpiredLicense]
  }

  "JwtLicenseManager with unknown kid" should "return Left(InvalidLicense) when no override" in {
    val m = new JwtLicenseManager(publicKeyOverride = None)
    val jwt = JwtTestHelper.signJwt(
      JwtTestHelper.proClaimsBuilder().build(),
      kid = "unknown-key-2099"
    )
    val result = m.validate(jwt)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get.message should include("Unknown key ID")
  }

  "JwtLicenseManager with wrong issuer" should "return Left(InvalidLicense)" in {
    val m = manager
    val claims = new JWTClaimsSet.Builder(JwtTestHelper.proClaimsBuilder().build())
      .issuer("https://evil.example.com")
      .build()
    val jwt = JwtTestHelper.signJwt(claims)
    val result = m.validate(jwt)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get.message should include("Invalid issuer")
  }

  "JwtLicenseManager with malformed string" should "return Left(InvalidLicense)" in {
    val m = manager
    val result = m.validate("not-a-jwt-at-all")
    result shouldBe a[Left[_, _]]
    result.left.toOption.get.message should include("Malformed JWT")
  }

  "JwtLicenseManager with unknown tier" should "default to Community" in {
    val m = manager
    val claims = new JWTClaimsSet.Builder(JwtTestHelper.proClaimsBuilder().build())
      .claim("tier", "platinum")
      .build()
    val jwt = JwtTestHelper.signJwt(claims)
    val result = m.validate(jwt)
    result shouldBe a[Right[_, _]]
    result.toOption.get.licenseType shouldBe LicenseType.Community
  }

  "JwtLicenseManager with unknown feature strings" should "silently ignore them" in {
    val m = manager
    val claims = new JWTClaimsSet.Builder(JwtTestHelper.proClaimsBuilder().build())
      .claim("features", java.util.Arrays.asList("materialized_views", "time_travel", "warp_drive"))
      .build()
    val jwt = JwtTestHelper.signJwt(claims)
    val result = m.validate(jwt)
    result shouldBe a[Right[_, _]]
    val key = result.toOption.get
    key.features shouldBe Set(Feature.MaterializedViews)
  }

  "JwtLicenseManager re-validation" should "replace the previous license" in {
    val m = manager
    val proJwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    m.validate(proJwt)
    m.licenseType shouldBe LicenseType.Pro

    val entJwt = JwtTestHelper.signJwt(JwtTestHelper.enterpriseClaimsBuilder().build())
    m.validate(entJwt)
    m.licenseType shouldBe LicenseType.Enterprise
  }

  "JwtLicenseManager quota mapping" should "map null/missing to None (unlimited)" in {
    val m = manager
    val claims = new JWTClaimsSet.Builder(JwtTestHelper.proClaimsBuilder().build())
      .claim(
        "quotas", {
          val q = new java.util.LinkedHashMap[String, AnyRef]()
          q.put("max_result_rows", null)
          q.put("max_clusters", Integer.valueOf(10))
          q
        }
      )
      .build()
    val jwt = JwtTestHelper.signJwt(claims)
    m.validate(jwt)
    m.quotas.maxQueryResults shouldBe None
    m.quotas.maxClusters shouldBe Some(10)
    m.quotas.maxMaterializedViews shouldBe None
  }

  // --- validateWithGracePeriod tests ---

  "validateWithGracePeriod with non-expired JWT" should "return Right (same as validate)" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    val result = m.validateWithGracePeriod(jwt, Duration.ofDays(14))
    result shouldBe a[Right[_, _]]
    result.toOption.get.licenseType shouldBe LicenseType.Pro
  }

  "validateWithGracePeriod with expired JWT within grace" should "return Right (grace mode)" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() - 3600000L) // 1 hour ago
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    // Grace period of 14 days — 1 hour ago is well within
    val result = m.validateWithGracePeriod(jwt, Duration.ofDays(14))
    result shouldBe a[Right[_, _]]
    result.toOption.get.licenseType shouldBe LicenseType.Pro
  }

  "validateWithGracePeriod with expired JWT beyond grace" should "return Left(ExpiredLicense)" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() - 30L * 24 * 3600000L) // 30 days ago
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    val result = m.validateWithGracePeriod(jwt, Duration.ofDays(14))
    result shouldBe a[Left[_, _]]
    result.left.toOption.get shouldBe an[ExpiredLicense]
  }

  "validateWithGracePeriod with zero grace" should "reject all expired JWTs" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() - 1000L) // 1 second ago
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    val result = m.validateWithGracePeriod(jwt, Duration.ZERO)
    result shouldBe a[Left[_, _]]
    result.left.toOption.get shouldBe an[ExpiredLicense]
  }

  "validateWithGracePeriod with 365-day grace" should "accept recently expired JWTs" in {
    val m = manager
    val expDate = new Date(System.currentTimeMillis() - 60L * 24 * 3600000L) // 60 days ago
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
    val result = m.validateWithGracePeriod(jwt, Duration.ofDays(365))
    result shouldBe a[Right[_, _]]
    result.toOption.get.licenseType shouldBe LicenseType.Pro
  }

  // --- resetToCommunity tests ---

  "resetToCommunity after Pro validation" should "revert to Community tier and quotas" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    m.validate(jwt)
    m.licenseType shouldBe LicenseType.Pro

    m.resetToCommunity()
    m.licenseType shouldBe LicenseType.Community
    m.quotas shouldBe Quota.Community
  }

  "resetToCommunity then re-validate" should "update state to Pro again" in {
    val m = manager
    val jwt = JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())
    m.validate(jwt)
    m.resetToCommunity()
    m.licenseType shouldBe LicenseType.Community

    m.validate(jwt)
    m.licenseType shouldBe LicenseType.Pro
  }
}
