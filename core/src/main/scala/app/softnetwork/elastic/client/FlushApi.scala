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

trait FlushApi extends ElasticClientHelpers {

  /** Flush the index to make sure all operations are written to disk.
    * @param index
    *   - the name of the index to flush
    * @param force
    *   - true to force the flush, false otherwise
    * @param wait
    *   - true to wait for the flush to complete, false otherwise
    * @return
    *   true if the index was flushed successfully, false otherwise
    */
  def flush(index: String, force: Boolean = true, wait: Boolean = true): ElasticResult[Boolean] = {
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

    logger.debug(s"Flushing index: $index")

    executeFlush(index, force, wait) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' flushed successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not flushed")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to flush index '$index': ${error.message}")
        failure
    }

  }

  private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean]
}
