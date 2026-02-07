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

/** Index Management API
  */
trait IndexApi extends ElasticClientHelpers { _: SettingsApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Index an entity in the given index.
    * @param entity
    *   - the entity to index
    * @param id
    *   - the id of the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param wait
    *   - whether to wait for a refresh to happen after indexing (default is false)
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def indexAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    wait: Boolean = false
  )(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    val indexName = index.getOrElse(indexType)

    ElasticResult
      .attempt {
        serialization.write[U](entity)
      }
      .flatMap { source =>
        this.index(indexName, id, source, wait)
      }
  }

  /** Index an entity in the given index.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @param wait
    *   - whether to wait for a refresh to happen after indexing (default is false)
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def index(
    index: String,
    id: String,
    source: String,
    wait: Boolean = false
  ): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("index")
          )
        )
      case None => // continue
    }

    logger.debug(s"Indexing document with id '$id' in index '$index'")

    // if wait for next refresh is enabled, we should make sure that the refresh is enabled (different to -1)
    val waitEnabled = wait && isRefreshEnabled(index).getOrElse(false)
    executeIndex(index, id, source, waitEnabled) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Document with id '$id' indexed successfully in index '$index'")
        if (waitEnabled)
          logger.info(s"Waiting for a refresh of index $index to happen")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Document with id '$id' not indexed in index '$index'")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to index document with id '$id' in index '$index': ${error.message}"
        )
        failure
    }
  }

  /** Index an entity in the given index asynchronously.
    * @param entity
    *   - the entity to index
    * @param id
    *   - the id of the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param wait
    *   - whether to wait for a refresh to happen after indexing (default is false)
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsyncAs[U <: AnyRef](
    entity: U,
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    wait: Boolean = false
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
      case ElasticSuccess(source) => this.indexAsync(indexName, id, source, wait)
    }
  }

  /** Index an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @param wait
    *   - whether to wait for a refresh to happen after indexing (default is false)
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsync(index: String, id: String, source: String, wait: Boolean = false)(implicit
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
              operation = Some("indexAsync")
            )
          )
        )
      case None => // continue
    }

    logger.debug(s"Indexing asynchronously document with id '$id' in index '$index'")

    val promise: Promise[ElasticResult[Boolean]] = Promise()

    // if wait for next refresh is enabled, we should make sure that the refresh is enabled (different to -1)
    val waitEnabled = wait && isRefreshEnabled(index).getOrElse(false)
    executeIndexAsync(index, id, source, waitEnabled) onComplete {
      case scala.util.Success(result) =>
        result match {
          case success @ ElasticSuccess(true) =>
            logger.info(s"✅ Successfully indexed document with id '$id' in index '$index'")
            if (waitEnabled)
              logger.info(s"Waiting for a refresh of index $index to happen")
            promise.success(success)
          case success @ ElasticSuccess(_) =>
            logger.info(s"✅ Document with id '$id' not indexed in index '$index'")
            promise.success(success)
          case failure @ ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to index document with id '$id' in index '$index': ${error.message}"
            )
            promise.success(failure)
        }
      case scala.util.Failure(exception) =>
        val error = ElasticError(
          message =
            s"Failed to index document with id '$id' in index '$index': ${exception.getMessage}",
          operation = Some("indexAsync"),
          index = Some(index),
          cause = Some(exception)
        )
        logger.error(s"❌ ${error.message}")
        promise.success(ElasticResult.failure(error))
    }

    promise.future
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
}
