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

/** Refresh management API for Elasticsearch clients.
  * @see
  *   [[RefreshApi]] for generic API documentation
  */
trait RefreshApi extends ElasticClientHelpers {

  /** Refresh the index to make sure all documents are indexed and searchable.
    * @param index
    *   - the name of the index to refresh
    * @return
    *   true if the index was refreshed successfully, false otherwise
    */
  def refresh(index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("refresh")
          )
        )
      case None => // continue
    }

    logger.debug(s"Refreshing index: $index")

    executeRefresh(index) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' refreshed successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not refreshed")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to refresh index '$index': ${error.message}")
        failure
    }
  }

  private[client] def executeRefresh(index: String): ElasticResult[Boolean]
}
