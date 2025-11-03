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

trait VersionApi extends ElasticClientHelpers { _: SerializationApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  // Cache ES version (avoids calling it every time)
  @volatile private var cachedVersion: Option[String] = None

  /** Get Elasticsearch version.
    * @return
    *   the Elasticsearch version
    */
  def version: ElasticResult[String] = {
    cachedVersion match {
      case Some(version) =>
        ElasticSuccess(version)
      case None =>
        executeVersion() match {
          case success @ ElasticSuccess(version) =>
            logger.info(s"✅ Elasticsearch version: $version")
            cachedVersion = Some(version)
            success
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ Failed to get Elasticsearch version: ${error.message}")
            failure
        }
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeVersion(): ElasticResult[String]
}
