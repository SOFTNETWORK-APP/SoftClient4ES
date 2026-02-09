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

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.sql.watcher.{Watcher, WatcherStatus}

trait WatcherApi extends ElasticClientHelpers { _: VersionApi =>

  /** Create a watcher
    * @param watcher
    *   the watcher to create
    * @param active
    *   whether the watcher should be active or not
    * @return
    *   true if the watcher was created, false otherwise
    */
  def createWatcher(watcher: Watcher, active: Boolean = true): ElasticResult[Boolean] = {
    logger.info(s"Creating Watcher with id: ${watcher.id}")
    executeCreateWatcher(watcher, active)
  }

  /** Delete a watcher by its id
    * @param id
    *   the id of the watcher to delete
    * @return
    *   true if the watcher was deleted, false otherwise
    */
  def deleteWatcher(id: String): ElasticResult[Boolean] = {
    logger.info(s"Deleting Watcher with id: $id")
    executeDeleteWatcher(id)
  }

  /** Get a watcher status by its id
    * @param id
    *   the id of the watcher
    * @return
    *   the watcher status if found, None otherwise
    */
  def getWatcherStatus(id: String): ElasticResult[Option[WatcherStatus]] = {
    logger.info(s"Getting Watcher status for id: $id")
    executeGetWatcherStatus(id)
  }

  /** List all watchers
    * @return
    *   a sequence of watchers
    */
  def listWatchers(): ElasticResult[Seq[WatcherStatus]] = {
    // Get Elasticsearch version
    val elasticVersion = {
      this.version match {
        case ElasticSuccess(v) => v
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
          return ElasticFailure(error)
      }
    }
    if (ElasticsearchVersion.supportsQueryWatchers(elasticVersion)) {
      logger.info(s"Listing all Watchers")
      executeListWatchers() match {
        case success @ ElasticSuccess(watchers) =>
          logger.info(s"✅ Successfully retrieved list of ${watchers.size} Watchers")
          success
        case failure @ ElasticFailure(error) =>
          logger.error(s"❌ Failed to list Watchers: ${error.message}")
          failure
      }
    } else {
      logger.error(
        s"❌ Elasticsearch version $elasticVersion does not support querying watchers directly."
      )
      ElasticFailure(
        ElasticError.notFound("_watcher", "_query", "listWatchers")
      )
    }
  }

  private[client] def sanitizeWatcherJson(json: String): String = {
    // Hide Authorization header content
    json.replaceAll(
      """"Authorization"\s*:\s*"[^"]+"""",
      """"Authorization":"***REDACTED***""""
    )
  }

  private[client] def executeCreateWatcher(
    watcher: Watcher,
    active: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeDeleteWatcher(id: String): ElasticResult[Boolean]

  private[client] def executeGetWatcherStatus(id: String): ElasticResult[Option[WatcherStatus]]

  private[client] def executeListWatchers(): ElasticResult[Seq[WatcherStatus]]
}
