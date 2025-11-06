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
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.reflect.ClassTag
import scala.util.{Failure, Success}

/** Update Management API
  */
trait UpdateApi extends ElasticClientHelpers { _: RefreshApi with SerializationApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Update an entity in the given index.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def update(index: String, id: String, source: String, upsert: Boolean): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("update")
          )
        )
      case None => // continue
    }

    validateJson("update", source) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid JSON source: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("update")
          )
        )
      case None => // continue
    }

    logger.debug(s"Updating document with id '$id' in index '$index'")

    executeUpdate(index, id, source, upsert) match {
      case ElasticSuccess(true) =>
        logger.info(s"✅ Successfully updated document with id '$id' in index '$index'")
        this.refresh(index)
      case ElasticSuccess(false) =>
        val error = s"Document with id '$id' in index '$index' not updated"
        logger.warn(s"❌ $error")
        ElasticResult.failure(
          ElasticError(
            message = error,
            operation = Some("update"),
            index = Some(index)
          )
        )
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to update document with id '$id' in index '$index': ${error.message}"
        )
        failure
    }
  }

  /** Update an entity in the given index.
    * @param entity
    *   - the entity to update
    * @param id
    *   - the id of the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def updateAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    val indexName = index.getOrElse(indexType)

    ElasticResult
      .attempt {
        serialization.write[U](entity)
      }
      .flatMap { source =>
        this.update(indexName, id, source, upsert)
      }
  }

  /** Update an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
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
              operation = Some("update")
            )
          )
        )
      case None => // continue
    }

    validateJson("update", source) match {
      case Some(error) =>
        return Future.successful(
          ElasticResult.failure(
            error.copy(
              message = s"Invalid JSON source: ${error.message}",
              statusCode = Some(400),
              index = Some(index),
              operation = Some("update")
            )
          )
        )
      case None => // continue
    }

    logger.debug(s"Updating document with id '$id' in index '$index' asynchronously")

    val promise: Promise[ElasticResult[Boolean]] = Promise()
    executeUpdateAsync(index, id, source, upsert) onComplete {
      case Success(s) =>
        s match {
          case _ @ElasticSuccess(true) =>
            logger.info(s"✅ Successfully updated document with id '$id' in index '$index'")
            promise.success(this.refresh(index))
          case success @ ElasticSuccess(_) =>
            logger.warn(s"❌ Document with id '$id' in index '$index' not updated")
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

  /** Update an entity in the given index asynchronously.
    * @param entity
    *   - the entity to update
    * @param id
    *   - the id of the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsyncAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit
    u: ClassTag[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Boolean]] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    val indexName = index.getOrElse(indexType)

    ElasticResult.attempt {
      serialization.write[U](entity)
    } match {
      case failure @ ElasticFailure(_) =>
        logger.error(s"❌ Failed to serialize entity for update in index '$indexName'")
        Future.successful(failure)
      case ElasticSuccess(source) => this.updateAsync(indexName, id, source, upsert)
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
}
