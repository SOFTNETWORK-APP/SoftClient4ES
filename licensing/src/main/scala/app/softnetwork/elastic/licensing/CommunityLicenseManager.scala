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

import com.typesafe.scalalogging.LazyLogging

/** Community-tier license manager. No crypto dependencies, no key validation. Always returns
  * Community features and quotas. Used as the fallback when no extensions JAR is on the classpath.
  */
class CommunityLicenseManager extends LicenseManager with LazyLogging {

  override def validate(key: String): Either[LicenseError, LicenseKey] =
    Left(InvalidLicense("Community mode — license key validation requires the extensions JAR"))

  override def hasFeature(feature: Feature): Boolean =
    LicenseKey.Community.features.contains(feature)

  override def quotas: Quota = Quota.Community

  override def licenseType: LicenseType = LicenseType.Community

  override def refresh(): Either[LicenseError, LicenseKey] = {
    logger.debug("Community mode — refresh not supported")
    Left(RefreshNotSupported)
  }
}
