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

/** License checker for DQL statements.
  */
class DqlLicenseChecker(
  licenseManager: LicenseManager,
  logger: Logger
)(implicit system: ActorSystem)
    extends LicenseChecker[DqlStatement, ElasticResult[QueryResult]] {

  implicit val ec: ExecutionContext = system.dispatcher

  override def check(
    request: DqlStatement,
    execute: DqlStatement => Future[ElasticResult[QueryResult]]
  ): Future[Either[LicenseError, ElasticResult[QueryResult]]] = {

    val quota = licenseManager.quotas

    request match {
      case single: SingleSearch =>
        single.limit match {
          case Some(l) if quota.maxQueryResults.exists(_ < l.limit) =>
            val error = QuotaExceeded(
              "query_results",
              l.limit,
              quota.maxQueryResults.get
            )
            logger.warn(s"⚠️ ${error.message}")
            Future.successful(Left(error))

          case _ =>
            execute(single).map(Right(_))
        }

      case _ =>
        execute(request).map(Right(_))
    }
  }
}
