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

package app.softnetwork.elastic.client.licensing

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.sql.query._
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

/** License checker for DDL statements.
  */
class DdlLicenseChecker(
  licenseManager: LicenseManager,
  logger: Logger,
  mvCounter: () => Future[Int] // ✅ Dependency injection
)(implicit system: ActorSystem)
    extends LicenseChecker[DdlStatement, ElasticResult[QueryResult]] {

  implicit val ec: ExecutionContext = system.dispatcher

  override def check(
    request: DdlStatement,
    execute: DdlStatement => Future[ElasticResult[QueryResult]]
  ): Future[Either[LicenseError, ElasticResult[QueryResult]]] = {

    request match {
      case create: CreateMaterializedView =>
        // Check feature availability
        if (!licenseManager.hasFeature(Feature.MaterializedViews)) {
          val error = FeatureNotAvailable(Feature.MaterializedViews)
          logger.warn(s"⚠️ ${error.message}")
          return Future.successful(Left(error))
        }

        // Check quota
        licenseManager.quotas.maxMaterializedViews match {
          case Some(max) =>
            mvCounter().flatMap { count =>
              if (count >= max) {
                val error = QuotaExceeded("materialized_views", count, max)
                logger.warn(s"⚠️ ${error.message}")
                Future.successful(Left(error))
              } else {
                execute(create).map(Right(_))
              }
            }

          case None =>
            execute(create).map(Right(_))
        }

      case _ =>
        execute(request).map(Right(_))
    }
  }
}
