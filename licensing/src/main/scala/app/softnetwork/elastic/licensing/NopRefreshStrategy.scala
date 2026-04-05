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

/** No-op refresh strategy for Community mode.
  *
  * Always returns Community defaults. Used as the fallback when no extensions JAR is on the
  * classpath or when no LicenseManagerSpi provides a real strategy.
  */
class NopRefreshStrategy extends LicenseRefreshStrategy {

  private val manager: LicenseManager = new CommunityLicenseManager()

  override def initialize(): LicenseKey = LicenseKey.Community

  override def refresh(): Either[LicenseError, LicenseKey] = Left(RefreshNotSupported)

  override def licenseManager: LicenseManager = manager
}
