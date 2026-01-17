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
/** Core extension for DQL quota enforcement.
  *
  * {{{
  * ✅ OSS (always loaded)
  * ✅ Applies Community quotas by default
  * ✅ Can be upgraded with Pro license
  * }}}
  */
//format:on
class CoreDqlExtension extends ExtensionSpi {

  private var licenseManager: Option[LicenseManager] = None
  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  override def extensionId: String = "core-dql"
  override def extensionName: String = "Core DQL Quotas"
  override def version: String = "0.1.0"

  override def initialize(
    config: Config,
    manager: LicenseManager
  ): Either[String, Unit] = {
    logger.info("🔌 Initializing Core DQL extension")
    licenseManager = Some(manager)
    Right(())
  }

  // ✅ Priority: 100 (lower = higher priority)
  // Built-in extensions should have lower priority than premium ones
  override def priority: Int = 100

  override def canHandle(statement: Statement): Boolean = statement match {
    case _: DqlStatement => true
    case _               => false
  }

  override def execute(
    statement: Statement,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    statement match {
      case dql: DqlStatement =>
        licenseManager match {
          case Some(manager) =>
            checkQuotasAndExecute(dql, manager, client)
          case None =>
            // No license manager, execute without checks
            client.dqlExecutor.execute(dql)
        }

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
    "SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... LIMIT ..."
  )

  // ════════════════════════════════════════════════════════════════════
  // ✅ QUOTA CHECK LOGIC (in extension, not in core)
  // ════════════════════════════════════════════════════════════════════

  private def checkQuotasAndExecute(
    dql: DqlStatement,
    manager: LicenseManager,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val quota = manager.quotas

    dql match {
      case single: SingleSearch =>
        single.limit match {
          case Some(l) if quota.maxQueryResults.exists(_ < l.limit) =>
            logger.warn(
              s"⚠️ Query result limit (${l.limit}) exceeds license quota (${quota.maxQueryResults.get})"
            )
            Future.successful(
              ElasticFailure(
                ElasticError(
                  message =
                    s"Query result limit (${l.limit}) exceeds license quota (${quota.maxQueryResults.get}). Upgrade to Pro license.",
                  statusCode = Some(402),
                  operation = Some("license")
                )
              )
            )

          case _ =>
            // Quota OK, execute
            client.dqlExecutor.execute(single)
        }

      case _ =>
        // Other DQL types (no quota check needed)
        client.dqlExecutor.execute(dql)
    }
  }
}
