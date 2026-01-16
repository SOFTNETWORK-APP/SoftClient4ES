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

  sealed trait LicenseType

  object LicenseType {
    case object Community extends LicenseType // Gratuit
    case object Pro extends LicenseType // Payant
    case object Enterprise extends LicenseType // Payant + support
  }

  sealed trait Feature

  object Feature {
    case object MaterializedViews extends Feature
    case object JdbcDriver extends Feature
    case object OdbcDriver extends Feature
    case object UnlimitedResults extends Feature
    case object AdvancedAggregations extends Feature
    def values: Seq[Feature] = Seq(
      MaterializedViews,
      JdbcDriver,
      OdbcDriver,
      UnlimitedResults,
      AdvancedAggregations
    )
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
    maxConcurrentQueries: Option[Int]
  )

  object Quota {
    val Community: Quota = Quota(
      maxMaterializedViews = Some(3),
      maxQueryResults = Some(10000),
      maxConcurrentQueries = Some(5)
    )

    val Pro: Quota = Quota(
      maxMaterializedViews = Some(50),
      maxQueryResults = Some(1000000),
      maxConcurrentQueries = Some(50)
    )

    val Enterprise: Quota = Quota(
      maxMaterializedViews = None, // Unlimited
      maxQueryResults = None,
      maxConcurrentQueries = None
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
