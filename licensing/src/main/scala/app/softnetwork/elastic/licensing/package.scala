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

package app.softnetwork.elastic

package object licensing {

  sealed trait LicenseType {
    def isPaid: Boolean = this != LicenseType.Community
    def isEnterprise: Boolean = this == LicenseType.Enterprise
    def isPro: Boolean = this == LicenseType.Pro
    def ordinal: Int = this match {
      case LicenseType.Community  => 0
      case LicenseType.Pro        => 1
      case LicenseType.Enterprise => 2
    }
    def displayName: String = this match {
      case LicenseType.Community  => "Community"
      case LicenseType.Pro        => "Pro"
      case LicenseType.Enterprise => "Enterprise"
    }
  }

  object LicenseType {
    case object Community extends LicenseType // Gratuit
    case object Pro extends LicenseType // Payant
    case object Enterprise extends LicenseType // Payant + support

    /** Story 15.2 (A11) -- allowlist source for the daily-ping `license_tier` validation
      * (`validTiers = LicenseType.values.map(_.displayName)`).
      */
    def values: Seq[LicenseType] = Seq(Community, Pro, Enterprise)

    def upgradeTo(licenseType: LicenseType): LicenseType = licenseType match {
      case Community  => Pro
      case Pro        => Enterprise
      case Enterprise => Enterprise
    }

    def fromString(s: String): LicenseType = s.trim.toLowerCase match {
      case "pro"        => Pro
      case "enterprise" => Enterprise
      case _            => Community
    }
  }

  sealed trait Feature

  /** Story 15.2 (A11) -- marker for product-surface features. The daily product-instance ping's
    * `product` discriminator is `ProductType.displayName` (the snake feature name), and the
    * license-server `InstancePingDecorator.validate()` allowlist is
    * `ProductType.values.map(_.displayName)`.
    */
  sealed trait ProductType extends Feature {
    def displayName: String = Feature.toSnakeCase(this)
  }

  object ProductType {
    def values: Seq[ProductType] =
      Seq(
        Feature.JdbcDriver,
        Feature.FlightSql,
        Feature.AdbcDriver,
        Feature.Repl,
        Feature.Federation
      )
  }

  object Feature {
    case object MaterializedViews extends Feature
    case object JdbcDriver extends ProductType
    case object AdbcDriver extends ProductType // Story 15.2 (A11) -- replaces OdbcDriver
    case object UnlimitedResults extends Feature
    case object AdvancedAggregations extends Feature
    case object FlightSql extends ProductType
    case object Federation extends ProductType
    case object Repl extends ProductType // Story 15.2 (A11) -- NEW
    def values: Seq[Feature] = Seq(
      MaterializedViews,
      JdbcDriver,
      AdbcDriver,
      UnlimitedResults,
      AdvancedAggregations,
      FlightSql,
      Federation,
      Repl
    )

    def fromString(s: String): Option[Feature] = s.trim.toLowerCase match {
      case "materialized_views"    => Some(MaterializedViews)
      case "jdbc_driver"           => Some(JdbcDriver)
      case "adbc_driver"           => Some(AdbcDriver)
      case "unlimited_results"     => Some(UnlimitedResults)
      case "advanced_aggregations" => Some(AdvancedAggregations)
      case "flight_sql"            => Some(FlightSql)
      case "federation"            => Some(Federation)
      case "repl"                  => Some(Repl)
      case _                       => None
    }

    def toSnakeCase(f: Feature): String = f match {
      case MaterializedViews    => "materialized_views"
      case JdbcDriver           => "jdbc_driver"
      case AdbcDriver           => "adbc_driver"
      case UnlimitedResults     => "unlimited_results"
      case AdvancedAggregations => "advanced_aggregations"
      case FlightSql            => "flight_sql"
      case Federation           => "federation"
      case Repl                 => "repl"
    }
  }

  /** Compute the number of days between now and a target instant. Positive = future, negative =
    * past. Single source of truth for expiry/grace computations.
    */
  def daysBetween(now: java.time.Instant, target: java.time.Instant): Long =
    java.time.Duration.between(now, target).toDays

  case class LicenseKey(
    id: String,
    licenseType: LicenseType,
    features: Set[Feature],
    expiresAt: Option[java.time.Instant],
    metadata: Map[String, String] = Map.empty,
    quota: Option[Quota] = None,
    usage: Option[LicenseUsage] = None,
    platform: Option[Platform] = None,
    trial: Boolean = false
  ) {

    /** Whether this is a trial license (Pro trial via API key). */
    def isTrial: Boolean = trial

    /** Days remaining until expiration, or None if no expiry. Positive = not yet expired. */
    def daysRemaining: Option[Long] = daysRemainingAt(java.time.Instant.now())

    /** Testable variant: days remaining relative to a given instant. */
    def daysRemainingAt(now: java.time.Instant): Option[Long] = expiresAt.map(daysBetween(now, _))

    /** Days since expiration, or None if no expiry. Positive = expired. */
    def daysSinceExpiry: Option[Long] = daysSinceExpiryAt(java.time.Instant.now())

    /** Testable variant: days since expiry relative to a given instant. */
    def daysSinceExpiryAt(now: java.time.Instant): Option[Long] =
      expiresAt.map(exp => -daysBetween(now, exp))
  }

  object LicenseKey {
    val Community: LicenseKey = LicenseKey(
      id = "community",
      licenseType = LicenseType.Community,
      features = Set(
        Feature.MaterializedViews,
        Feature.JdbcDriver,
        Feature.AdbcDriver,
        Feature.FlightSql,
        Feature.Federation,
        Feature.Repl
      ),
      expiresAt = None,
      quota = Some(Quota.Community)
    )
  }

  case class Quota(
    maxMaterializedViews: Option[Int], // None = unlimited
    maxQueryResults: Option[Int], // None = unlimited
    maxConcurrentQueries: Option[Int],
    maxClusters: Option[Int] = Some(0), // None = unlimited
    maxJoins: Option[Int] = Some(0) // None = unlimited
  )

  object Quota {
    val Community: Quota = Quota(
      maxMaterializedViews = Some(3),
      maxQueryResults = Some(10000),
      maxConcurrentQueries = Some(5),
      maxClusters = Some(1),
      maxJoins = Some(1)
    )

    val Pro: Quota = Quota(
      maxMaterializedViews = Some(50),
      maxQueryResults = Some(1000000),
      maxConcurrentQueries = Some(50),
      maxClusters = Some(5),
      maxJoins = Some(5)
    )

    val Enterprise: Quota = Quota(
      maxMaterializedViews = None, // Unlimited
      maxQueryResults = None,
      maxConcurrentQueries = None,
      maxClusters = None,
      maxJoins = None
    )
  }

  sealed trait Platform {
    def name: String
    override def toString: String = name
  }

  object Platform {
    case object Production extends Platform { val name = "PRODUCTION" }
    case object Staging extends Platform { val name = "STAGING" }
    case object Integration extends Platform { val name = "INTEGRATION" }
    case object Development extends Platform { val name = "DEVELOPMENT" }

    val values: Seq[Platform] = Seq(Production, Staging, Integration, Development)

    def fromString(s: String): Option[Platform] = s.trim.toUpperCase match {
      case "PRODUCTION"  => Some(Production)
      case "STAGING"     => Some(Staging)
      case "INTEGRATION" => Some(Integration)
      case "DEVELOPMENT" => Some(Development)
      case _             => None
    }
  }

  case class LicenseUsage(
    totalMvsActive: Int = 0,
    totalFederatedClusters: Int = 0
  ) {
    require(totalMvsActive >= 0, s"totalMvsActive must be non-negative, got $totalMvsActive")
    require(
      totalFederatedClusters >= 0,
      s"totalFederatedClusters must be non-negative, got $totalFederatedClusters"
    )

    /** Check usage for a specific feature against a quota. Returns the exceeded limit, if any. */
    def checkQuota(feature: Feature, quota: Quota): Option[QuotaExceeded] = feature match {
      case Feature.MaterializedViews =>
        quota.maxMaterializedViews.collect {
          case max if totalMvsActive > max =>
            QuotaExceeded("maxMaterializedViews", totalMvsActive, max)
        }
      case Feature.Federation =>
        quota.maxClusters.collect {
          case max if totalFederatedClusters > max =>
            QuotaExceeded("maxClusters", totalFederatedClusters, max)
        }
      case _ => None
    }
  }

  object LicenseUsage {
    val Empty: LicenseUsage = LicenseUsage()
  }

  sealed trait GraceStatus
  object GraceStatus {
    case object NotInGrace extends GraceStatus

    /** @param daysExpired must be computed via `LicenseKey.daysSinceExpiryAt(now)` */
    case class EarlyGrace(daysExpired: Long) extends GraceStatus

    /** @param daysExpired
      *   must be computed via `LicenseKey.daysSinceExpiryAt(now)`
      * @param daysRemaining
      *   grace period days remaining (gracePeriodDays - daysExpired)
      */
    case class MidGrace(daysExpired: Long, daysRemaining: Long) extends GraceStatus
  }

  trait LicenseManager {

    /** Validate license key */
    def validate(key: String): Either[LicenseError, LicenseKey]

    /** Check if feature is available */
    def hasFeature(feature: Feature): Boolean

    /** Get current quotas */
    def quotas: Quota

    /** Get license type */
    def licenseType: LicenseType

    /** Get current grace status */
    def graceStatus: GraceStatus = GraceStatus.NotInGrace

    /** Whether the license was degraded to Community due to expiry/failure */
    def wasDegraded: Boolean = false

    /** Log a warning if the license is in mid-grace period (per-request use) */
    def warnIfInGrace(): Unit = ()

    /** Refresh the license (re-resolve from backend/cache/config). Returns Right(LicenseKey) on
      * success, Left(LicenseError) on failure. Default: returns Right(LicenseKey.Community) (no-op
      * for stub implementations).
      */
    def refresh(): Either[LicenseError, LicenseKey] = Left(RefreshNotSupported)

    /** Get the current LicenseKey (needed by SHOW LICENSE for expiresAt). Default returns
      * Community.
      */
    def currentLicenseKey: LicenseKey = LicenseKey.Community

    /** Whether the current license is a trial. */
    def isTrial: Boolean = currentLicenseKey.isTrial
  }

  sealed trait LicenseError {
    def message: String
    def statusCode: Int = 402 // Payment Required
  }

  case class InvalidLicense(reason: String) extends LicenseError {
    def message: String = s"Invalid license: $reason"
  }

  case class ExpiredLicense(expiredAt: java.time.Instant) extends LicenseError {
    def message: String = s"License expired at $expiredAt"
  }

  case class FeatureNotAvailable(feature: Feature) extends LicenseError {
    def message: String = s"Feature $feature requires a paid license"
  }

  case class QuotaExceeded(quota: String, current: Int, max: Int) extends LicenseError {
    def message: String = s"Quota exceeded: $quota ($current/$max)"
  }

  case object RefreshNotSupported extends LicenseError {
    def message: String = "License refresh is not supported in Community mode"
    override def statusCode: Int = 501
  }

  @deprecated("Use CommunityLicenseManager", "0.20.0")
  type DefaultLicenseManager = CommunityLicenseManager

}
