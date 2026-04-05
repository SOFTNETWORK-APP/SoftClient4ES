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

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import app.softnetwork.elastic.licensing.{
  CommunityLicenseManager,
  LicenseManager,
  LicenseManagerSpi,
  LicenseMode
}

trait ExtensionApi { self: ElasticClientApi =>

  /** Runtime context for licensing behavior. Override in concrete implementations:
    *   - REPL/CLI: `Some(LicenseMode.LongRunning)` — auto-refresh
    *   - JDBC/ADBC: `Some(LicenseMode.Driver)` — on-demand, no implicit calls
    *   - Default: `None` — safe default (Driver semantics)
    */
  def licenseMode: Option[LicenseMode] = None

  /** License manager resolved via SPI (highest-priority LicenseManagerSpi wins). The licenseMode is
    * passed to the SPI to wire the appropriate refresh strategy. Falls back to
    * CommunityLicenseManager if no SPI implementation is found or if the winning SPI fails to
    * create a manager.
    *
    * CONSTRAINT: SPI create() must NOT reference this ElasticClientApi instance.
    */
  lazy val licenseManager: LicenseManager = {
    val loader = ServiceLoader.load(classOf[LicenseManagerSpi])
    val spis = loader.iterator().asScala.toSeq.sortBy(_.priority)
    spis.headOption
      .flatMap { spi =>
        try {
          Some(spi.create(config, licenseMode))
        } catch {
          case e: Exception =>
            logger.error(
              s"Failed to create LicenseManager from ${spi.getClass.getName}: ${e.getMessage}",
              e
            )
            None
        }
      }
      .getOrElse(new CommunityLicenseManager())
  }

  /** Extension registry (lazy loaded) */
  lazy val extensionRegistry: ExtensionRegistry =
    new ExtensionRegistry(config, licenseManager)
}
