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

package app.softnetwork.elastic.client.extensions

import akka.actor.ActorSystem
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.sql.query._
import com.typesafe.config.Config

import scala.concurrent.{ExecutionContext, Future}

//format:off
/** Core extension for basic DDL operations.
  *
  * {{{
  * ✅ OSS (always loaded)
  * ✅ Handles simple DDL (CREATE TABLE, DROP TABLE, etc.)
  * ✅ Does NOT handle materialized views (premium extension)
  * }}}
  */
//format:on
class CoreDdlExtension extends ExtensionSpi {

  private var licenseManager: Option[LicenseManager] = None
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def extensionId: String = "core-ddl"
  override def extensionName: String = "Core DDL"
  override def version: String = "0.1.0"

  override def initialize(
    config: Config,
    manager: LicenseManager
  ): Either[String, Unit] = {
    logger.info("🔌 Initializing Core DDL extension")
    licenseManager = Some(manager)
    Right(())
  }

  override def priority: Int = 100

  override def canHandle(statement: Statement): Boolean = statement match {
    case ddl: DdlStatement =>
      // ✅ Handle basic DDL, but NOT materialized views
      ddl match {
        case _: MaterializedViewStatement => false
        case _                            => true
      }
    case _ => false
  }

  override def execute(
    statement: Statement,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    statement match {
      case ddl: DdlStatement =>
        // ✅ No quota checks for basic DDL
        client.ddlExecutor.execute(ddl)

      case _ =>
        Future.successful(
          ElasticFailure(
            ElasticError(
              message = "Statement not supported by this extension",
              statusCode = Some(400),
              operation = Some("extension")
            )
          )
        )
    }
  }

  override def supportedSyntax: Seq[String] = Seq(
    "CREATE TABLE",
    "DROP TABLE",
    "ALTER TABLE",
    "TRUNCATE TABLE",
    "SHOW CREATE TABLE",
    "SHOW TABLE",
    "DESCRIBE TABLE",
    "CREATE PIPELINE",
    "ALTER PIPELINE",
    "DROP PIPELINE",
    "SHOW PIPELINE",
    "DESCRIBE PIPELINE"
  )
}
