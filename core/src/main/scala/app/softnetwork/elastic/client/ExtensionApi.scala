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

import app.softnetwork.elastic.client.result.ElasticSuccess
import app.softnetwork.elastic.licensing.{
  LicenseManager,
  LicenseRefreshStrategy,
  LicenseRefreshStrategyFactory
}

import scala.concurrent.{ExecutionContext, Future}

trait ExtensionApi { self: ElasticClientApi =>

  /** License refresh strategy resolved via LicenseRefreshStrategyFactory. The factory uses SPI
    * discovery and derives LicenseMode from config (refreshEnabled=true -> LongRunning, else ->
    * Driver). Cluster info is set once during initialization.
    */
  lazy val licenseRefreshStrategy: LicenseRefreshStrategy = {
    val ret = LicenseRefreshStrategyFactory.create(config, metrics)
    clusterUuid match {
      case ElasticSuccess(uuid) =>
        ret.telemetryCollector.setClusterInfo(
          id = uuid,
          name = clusterName match {
            case ElasticSuccess(n) => Some(n)
            case _                 => None
          },
          version = version match {
            case ElasticSuccess(v) => Some(v)
            case _                 => None
          }
        )
      case _ =>
    }
    ret
  }

  /** License manager resolved from the refresh strategy. */
  lazy val licenseManager: LicenseManager = licenseRefreshStrategy.licenseManager

  /** Extension registry (lazy loaded) */
  lazy val extensionRegistry: ExtensionRegistry =
    new ExtensionRegistry(config, licenseRefreshStrategy)

  /** #163 — eagerly force extension discovery + initialization on the given execution context,
    * instead of paying the ServiceLoader scan (~442ms measured over a 278-jar REPL classpath) on
    * the first query. Additive: the underlying lazy vals stay the single initialization path — a
    * concurrent first query simply blocks on the lazy-val monitor until the one scan finishes (and
    * a FAILED lazy init is not cached, so a later query retries).
    *
    * @return
    *   the number of successfully initialized extensions.
    */
  def initializeExtensions()(implicit ec: ExecutionContext): Future[Int] =
    Future(extensionRegistry.extensions.size)
}
