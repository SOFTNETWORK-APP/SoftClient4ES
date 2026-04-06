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

package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.{ElasticSuccess, QueryRows}
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.sql.query.{RefreshLicense, ShowLicense}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.Instant
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

class LicenseExecutorSpec extends AnyFlatSpec with Matchers {

  implicit val system: ActorSystem = ActorSystem("license-executor-test")

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private def mkStrategy(
    mgr: LicenseManager,
    refreshResult: Either[LicenseError, LicenseKey] = Left(RefreshNotSupported)
  ): LicenseRefreshStrategy = new LicenseRefreshStrategy {
    override def initialize(): LicenseKey = LicenseKey.Community
    override def refresh(): Either[LicenseError, LicenseKey] = refreshResult
    override def licenseManager: LicenseManager = mgr
  }

  private val nopStrategy = new NopRefreshStrategy()

  private def execute(executor: LicenseExecutor, stmt: ShowLicense.type): ListMap[String, Any] = {
    val result = Await.result(executor.execute(stmt), 5.seconds)
    result shouldBe a[ElasticSuccess[_]]
    val rows = result.asInstanceOf[ElasticSuccess[QueryRows]].value.rows
    rows should have size 1
    rows.head.asInstanceOf[ListMap[String, Any]]
  }

  private def executeRefresh(executor: LicenseExecutor): ListMap[String, Any] = {
    val result = Await.result(executor.execute(RefreshLicense), 5.seconds)
    result shouldBe a[ElasticSuccess[_]]
    val rows = result.asInstanceOf[ElasticSuccess[QueryRows]].value.rows
    rows should have size 1
    rows.head.asInstanceOf[ListMap[String, Any]]
  }

  // -------------------------------------------------------------------------
  // SHOW LICENSE
  // -------------------------------------------------------------------------

  behavior of "LicenseExecutor - SHOW LICENSE"

  it should "return Community defaults with NopRefreshStrategy" in {
    val executor = new LicenseExecutor(strategy = nopStrategy)
    val row = execute(executor, ShowLicense)

    row should contain key "license_type"
    row("license_type") shouldBe "Community"
    row should contain key "max_materialized_views"
    row("max_materialized_views") shouldBe "3"
    row should contain key "max_clusters"
    row("max_clusters") shouldBe "2"
    row should contain key "max_result_rows"
    row("max_result_rows") shouldBe "10000"
    row should contain key "max_concurrent_queries"
    row("max_concurrent_queries") shouldBe "5"
    row should contain key "expires_at"
    row("expires_at") shouldBe "never"
    row should contain key "status"
    row("status") shouldBe "Active"
  }

  it should "return strategy license info when strategy is configured" in {
    val proManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = Quota.Pro
      override def licenseType: LicenseType = LicenseType.Pro
      override def currentLicenseKey: LicenseKey = LicenseKey(
        id = "test-pro",
        licenseType = LicenseType.Pro,
        features = Feature.values.toSet,
        expiresAt = Some(Instant.parse("2026-12-31T23:59:59Z"))
      )
    }
    val executor = new LicenseExecutor(strategy = mkStrategy(proManager))
    val row = execute(executor, ShowLicense)

    row("license_type") shouldBe "Pro"
    row("max_materialized_views") shouldBe "50"
    row("max_clusters") shouldBe "5"
    row("max_result_rows") shouldBe "1000000"
    row("max_concurrent_queries") shouldBe "50"
    row("expires_at") shouldBe "2026-12-31T23:59:59Z"
    row("status") shouldBe "Active"
  }

  it should "show degraded suffix when license was degraded" in {
    val degradedManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean =
        LicenseKey.Community.features.contains(feature)
      override def quotas: Quota = Quota.Community
      override def licenseType: LicenseType = LicenseType.Community
      override def wasDegraded: Boolean = true
    }
    val executor = new LicenseExecutor(strategy = mkStrategy(degradedManager))
    val row = execute(executor, ShowLicense)

    row("license_type") shouldBe "Community (degraded)"
  }

  it should "show grace status when in early grace" in {
    val graceManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = Quota.Pro
      override def licenseType: LicenseType = LicenseType.Pro
      override def graceStatus: GraceStatus = GraceStatus.EarlyGrace(3)
    }
    val executor = new LicenseExecutor(strategy = mkStrategy(graceManager))
    val row = execute(executor, ShowLicense)

    row("status") shouldBe "Expired (3 days ago, in grace period)"
  }

  it should "show grace status when in mid grace" in {
    val graceManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = Quota.Pro
      override def licenseType: LicenseType = LicenseType.Pro
      override def graceStatus: GraceStatus = GraceStatus.MidGrace(10, 4)
    }
    val executor = new LicenseExecutor(strategy = mkStrategy(graceManager))
    val row = execute(executor, ShowLicense)

    row("status") shouldBe "Expired (10 days ago, 4 days until Community fallback)"
  }

  // -------------------------------------------------------------------------
  // REFRESH LICENSE
  // -------------------------------------------------------------------------

  behavior of "LicenseExecutor - REFRESH LICENSE"

  it should "return failure with NopRefreshStrategy" in {
    val executor = new LicenseExecutor(strategy = nopStrategy)
    val row = executeRefresh(executor)

    row should contain key "previous_tier"
    row("previous_tier") shouldBe "Community"
    row should contain key "new_tier"
    row("new_tier") shouldBe "Community"
    row should contain key "expires_at"
    row("expires_at") shouldBe "never"
    row should contain key "status"
    row("status") shouldBe "Failed"
    row should contain key "message"
    row("message") shouldBe "License refresh is not supported in Community mode"
  }

  it should "return success result on successful refresh" in {
    val proManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = Quota.Pro
      override def licenseType: LicenseType = LicenseType.Pro
    }
    val newKey = LicenseKey(
      id = "refreshed-pro",
      licenseType = LicenseType.Pro,
      features = Feature.values.toSet,
      expiresAt = Some(Instant.parse("2027-06-30T23:59:59Z"))
    )
    val executor = new LicenseExecutor(
      strategy = mkStrategy(proManager, refreshResult = Right(newKey))
    )
    val row = executeRefresh(executor)

    row("previous_tier") shouldBe "Pro"
    row("new_tier") shouldBe "Pro"
    row("expires_at") shouldBe "2027-06-30T23:59:59Z"
    row("status") shouldBe "Refreshed"
    row("message") shouldBe ""
  }

  it should "return failure result with descriptive message when no API key" in {
    val communityManager = new CommunityLicenseManager()
    val executor = new LicenseExecutor(
      strategy = mkStrategy(
        communityManager,
        refreshResult = Left(
          InvalidLicense(
            "No API key configured — license loaded from static JWT or Community default"
          )
        )
      )
    )
    val row = executeRefresh(executor)

    row("status") shouldBe "Failed"
    row("message").toString should include("No API key configured")
    row("previous_tier") shouldBe "Community"
    row("new_tier") shouldBe "Community"
  }

  it should "return failure result on refresh error" in {
    val proManager = new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = Quota.Pro
      override def licenseType: LicenseType = LicenseType.Pro
      override def currentLicenseKey: LicenseKey = LicenseKey(
        id = "test-pro",
        licenseType = LicenseType.Pro,
        features = Feature.values.toSet,
        expiresAt = Some(Instant.parse("2026-12-31T23:59:59Z"))
      )
    }
    val executor = new LicenseExecutor(
      strategy = mkStrategy(
        proManager,
        refreshResult = Left(InvalidLicense("Network error"))
      )
    )
    val row = executeRefresh(executor)

    row("status") shouldBe "Failed"
    row("message").toString should include("Network error")
    row("previous_tier") shouldBe "Pro"
    row("new_tier") shouldBe "Pro"
    row("expires_at") shouldBe "2026-12-31T23:59:59Z"
  }
}
