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

class DefaultLicenseManager extends LicenseManager {

  private var currentLicense: LicenseKey = LicenseKey(
    id = "community",
    licenseType = LicenseType.Community,
    features = Set(
      Feature.MaterializedViews,
      Feature.JdbcDriver
    ),
    expiresAt = None
  )

  override def validate(key: String): Either[LicenseError, LicenseKey] = {
    key match {
      case k if k.startsWith("PRO-") =>
        val license = LicenseKey(
          id = k,
          licenseType = LicenseType.Pro,
          features = Set(
            Feature.MaterializedViews,
            Feature.JdbcDriver,
            Feature.UnlimitedResults
          ),
          expiresAt = None
        )
        currentLicense = license
        Right(license)

      case k if k.startsWith("ENT-") =>
        val license = LicenseKey(
          id = k,
          licenseType = LicenseType.Enterprise,
          features = Set(
            Feature.MaterializedViews,
            Feature.JdbcDriver,
            Feature.OdbcDriver,
            Feature.UnlimitedResults
          ),
          expiresAt = None
        )
        currentLicense = license
        Right(license)

      case _ =>
        Left(InvalidLicense("Invalid license key format"))
    }
  }

  override def hasFeature(feature: Feature): Boolean = {
    currentLicense.features.contains(feature)
  }

  override def quotas: Quota = currentLicense.licenseType match {
    case LicenseType.Community  => Quota.Community
    case LicenseType.Pro        => Quota.Pro
    case LicenseType.Enterprise => Quota.Enterprise
  }

  override def licenseType: LicenseType = currentLicense.licenseType
}
