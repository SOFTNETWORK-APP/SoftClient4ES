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

trait ClusterApi extends ElasticClientHelpers {

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  // Cache cluster name (avoids calling it every time)
  private val cachedClusterName = new AtomicReference[Option[String]](None)

  // Cache cluster UUID (avoids calling it every time)
  private val cachedClusterUuid = new AtomicReference[Option[String]](None)

  /** Get Elasticsearch cluster name.
    * @return
    *   the Elasticsearch cluster name
    */
  def clusterName: ElasticResult[String] = {
    cachedClusterName.get match {
      case Some(name) =>
        ElasticSuccess(name)
      case None =>
        executeGetClusterName() match {
          case ElasticSuccess(name) =>
            logger.info(s"✅ Elasticsearch cluster name: $name")
            cachedClusterName.compareAndSet(None, Some(name))
            ElasticSuccess(cachedClusterName.get.getOrElse(name))
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ Failed to get Elasticsearch cluster name: ${error.message}")
            failure
        }
    }
  }

  /** Get Elasticsearch cluster UUID. This is a stable, unique identifier for the cluster.
    * @return
    *   the Elasticsearch cluster UUID
    */
  def clusterUuid: ElasticResult[String] = {
    cachedClusterUuid.get match {
      case Some(uuid) =>
        ElasticSuccess(uuid)
      case None =>
        executeGetClusterUuid() match {
          case ElasticSuccess(uuid) =>
            logger.info(s"✅ Elasticsearch cluster uuid: $uuid")
            cachedClusterUuid.compareAndSet(None, Some(uuid))
            ElasticSuccess(cachedClusterUuid.get.getOrElse(uuid))
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ Failed to get Elasticsearch cluster UUID: ${error.message}")
            failure
        }
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeGetClusterName(): ElasticResult[String]

  private[client] def executeGetClusterUuid(): ElasticResult[String]
}
