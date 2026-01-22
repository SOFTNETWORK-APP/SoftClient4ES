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

import app.softnetwork.elastic.licensing.{DefaultLicenseManager, LicenseManager}

trait ExtensionApi { self: ElasticClientApi =>

  // ✅ Inject license manager (overridable)
  def licenseManager: LicenseManager = new DefaultLicenseManager()

  /** Extension registry (lazy loaded) */
  lazy val extensionRegistry: ExtensionRegistry = new ExtensionRegistry(config, licenseManager)
}
