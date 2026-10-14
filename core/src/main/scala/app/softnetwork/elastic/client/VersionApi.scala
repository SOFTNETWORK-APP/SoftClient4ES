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

import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}

import java.util.concurrent.atomic.AtomicReference

trait VersionApi extends ElasticClientHelpers {

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  // Cache ES version (avoids calling it every time)
  private val cachedVersion = new AtomicReference[Option[String]](None)

  /** Get Elasticsearch version.
    * @return
    *   the Elasticsearch version
    */
  def version: ElasticResult[String] = {
    cachedVersion.get match {
      case Some(version) =>
        ElasticSuccess(version)
      case None =>
        executeVersion() match {
          case ElasticSuccess(version) =>
            logger.info(s"✅ Elasticsearch version: $version")
            cachedVersion.compareAndSet(None, Some(version))
            ElasticSuccess(cachedVersion.get.getOrElse(version))
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ Failed to get Elasticsearch version: ${error.message}")
            failure
        }
    }
  }

  /** The major of Elasticsearch the client module is built for (its build info's `elasticMajor`),
    * by which dates are read when the cluster's version cannot be ([[dateDialectVersion]]); `None`
    * in core, which builds for no major.
    */
  private[client] def buildElasticMajor: Option[Int] = None

  /** The Elasticsearch version a statement reads dates as (`SetOperationValues`): the cluster's
    * ([[version]]), else -- a user whose role is denied `GET /` -- the major the client module is
    * built for ([[buildElasticMajor]]), else `None`. Asked ONCE per client, and kept: a privilege
    * granted later is seen after the client restarts.
    */
  private[elastic] lazy val dateDialectVersion: Option[String] =
    version.toOption.orElse(buildElasticMajor.map(_.toString))

  // def checkVersion(check: Int => Boolean)

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeVersion(): ElasticResult[String]
}
