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
  }

  object LicenseType {
    case object Community extends LicenseType // Gratuit
    case object Pro extends LicenseType // Payant
    case object Enterprise extends LicenseType // Payant + support
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

  object Feature {
    case object MaterializedViews extends Feature
    case object JdbcDriver extends Feature
    case object OdbcDriver extends Feature
    case object UnlimitedResults extends Feature
    case object AdvancedAggregations extends Feature
    case object FlightSql extends Feature
    case object Federation extends Feature
    def values: Seq[Feature] = Seq(
      MaterializedViews,
      JdbcDriver,
      OdbcDriver,
      UnlimitedResults,
      AdvancedAggregations,
      FlightSql,
      Federation
    )

    def fromString(s: String): Option[Feature] = s.trim.toLowerCase match {
      case "materialized_views"    => Some(MaterializedViews)
      case "jdbc_driver"           => Some(JdbcDriver)
      case "odbc_driver"           => Some(OdbcDriver)
      case "unlimited_results"     => Some(UnlimitedResults)
      case "advanced_aggregations" => Some(AdvancedAggregations)
      case "flight_sql"            => Some(FlightSql)
      case "federation"            => Some(Federation)
      case _                       => None
    }

    def toSnakeCase(f: Feature): String = f match {
      case MaterializedViews    => "materialized_views"
      case JdbcDriver           => "jdbc_driver"
      case OdbcDriver           => "odbc_driver"
      case UnlimitedResults     => "unlimited_results"
      case AdvancedAggregations => "advanced_aggregations"
      case FlightSql            => "flight_sql"
      case Federation           => "federation"
    }
  }

  case class LicenseKey(
    id: String,
    licenseType: LicenseType,
    features: Set[Feature],
    expiresAt: Option[java.time.Instant],
    metadata: Map[String, String] = Map.empty
  )

  case class Quota(
    maxMaterializedViews: Option[Int], // None = unlimited
    maxQueryResults: Option[Int], // None = unlimited
    maxConcurrentQueries: Option[Int],
    maxClusters: Option[Int] = Some(2) // None = unlimited
  )

  object Quota {
    val Community: Quota = Quota(
      maxMaterializedViews = Some(3),
      maxQueryResults = Some(10000),
      maxConcurrentQueries = Some(5),
      maxClusters = Some(2)
    )

    val Pro: Quota = Quota(
      maxMaterializedViews = Some(50),
      maxQueryResults = Some(1000000),
      maxConcurrentQueries = Some(50),
      maxClusters = Some(5)
    )

    val Enterprise: Quota = Quota(
      maxMaterializedViews = None, // Unlimited
      maxQueryResults = None,
      maxConcurrentQueries = None,
      maxClusters = None
    )
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

}
