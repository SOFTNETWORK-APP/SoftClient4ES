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

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

/** Delete Management API
  */
trait DeleteApi extends ElasticClientHelpers { _: RefreshApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Delete an entity from the given index.
    * @param id
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  def delete(id: String, index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("delete")
          )
        )
      case None => // continue
    }

    logger.debug(s"Deleting document with id '$id' from index '$index'")

    executeDelete(index, id) match {
      case _ @ElasticSuccess(true) =>
        logger.info(s"✅ Successfully deleted document with id '$id' from index '$index'")
        this.refresh(index)
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Document with id '$id' not found in index '$index'")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to delete document with id '$id' from index '$index': ${error.message}"
        )
        failure
    }
  }

  /** Delete an entity from the given index asynchronously.
    * @param id
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = {
    validateIndexName(index) match {
      case Some(error) =>
        return Future.successful(
          ElasticResult.failure(
            error.copy(
              message = s"Invalid index: ${error.message}",
              statusCode = Some(400),
              index = Some(index),
              operation = Some("deleteAsync")
            )
          )
        )
      case None => // continue
    }

    logger.debug(s"Deleting asynchronously document with id '$id' from index '$index'")

    val promise: Promise[ElasticResult[Boolean]] = Promise()
    executeDeleteAsync(index, id) onComplete {
      case Success(s) =>
        s match {
          case _ @ElasticSuccess(true) =>
            logger.info(s"✅ Successfully deleted document with id '$id' from index '$index'")
            promise.success(this.refresh(index))
          case success @ ElasticSuccess(_) =>
            logger.warn(s"❌ Document with id '$id' in index '$index' not deleted")
            promise.success(success)
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ ${error.message}")
            promise.success(failure)
        }
      case Failure(exception) =>
        val error = ElasticError(
          message =
            s"Exception while deleting document with id '$id' from index '$index': ${exception.getMessage}",
          operation = Some("deleteAsync"),
          index = Some(index)
        )
        logger.error(s"❌ ${error.message}")
        promise.success(ElasticResult.failure(error))
    }
    promise.future
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeDelete(index: String, id: String): ElasticResult[Boolean]

  private[client] def executeDeleteAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]]
}
