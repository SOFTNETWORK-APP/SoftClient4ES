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
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.licensing.LicenseManager
import app.softnetwork.elastic.sql.query.Statement
import com.typesafe.config.Config

import scala.concurrent.Future

/** Service Provider Interface for Elasticsearch extensions.
  *
  * Extensions can add new capabilities to the client (e.g., materialized views, advanced analytics,
  * custom drivers) while keeping the core API stable.
  */
trait ExtensionSpi {

  /** Unique identifier for this extension.
    */
  def extensionId: String

  /** Human-readable name.
    */
  def extensionName: String

  /** Version of the extension.
    */
  def version: String

  /** Priority for extension resolution (lower = higher priority).
    *
    * Default: 100 (core extensions) Premium extensions should use lower values (e.g., 50, 10)
    */
  def priority: Int = 100

  /** Initialize the extension with configuration and license manager.
    *
    * @param config
    *   Extension-specific configuration
    * @param licenseManager
    *   License manager for checking quotas
    * @return
    *   Success or error message
    */
  def initialize(
    config: Config,
    licenseManager: LicenseManager
  ): Either[String, Unit]

  /** Check if this extension can handle a given SQL statement.
    */
  def canHandle(statement: Statement): Boolean

  /** Execute a statement handled by this extension.
    *
    * @param statement
    *   SQL statement to execute
    * @param client
    *   Elasticsearch client API for low-level operations
    * @return
    *   Query result
    */
  def execute(
    statement: Statement,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]]

  /** List of SQL keywords/syntax added by this extension.
    *
    * Example: ["CREATE MATERIALIZED VIEW", "REFRESH MATERIALIZED VIEW"]
    */
  def supportedSyntax: Seq[String]

  /** Shutdown hook for cleanup.
    */
  def shutdown(): Unit = ()
}
