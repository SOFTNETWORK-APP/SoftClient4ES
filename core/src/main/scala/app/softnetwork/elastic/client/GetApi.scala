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

trait GetApi extends ElasticClientHelpers { _: SerializationApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Check if a document exists by its id in the given index.
    * @param id
    *   - the id of the document to check
    * @param index
    *   - the name of the index to check the document in
    * @return
    *   true if the document exists, false otherwise
    */
  def exists(id: String, index: String): ElasticResult[Boolean] = {
    get(id, index).map {
      case Some(_) => true
      case None    => false
    }
  }

  /** Get a document by its id from the given index.
    * @param id
    *   - the id of the document to get
    * @param index
    *   - the name of the index to get the document from
    * @return
    *   an Option containing the document as a JSON string if it was found, None otherwise
    */
  def get(id: String, index: String): ElasticResult[Option[String]] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("get")
          )
        )
      case None => // continue
    }

    logger.debug(s"Getting document with id '$id' from index '$index'")

    executeGet(index, id) match {
      case success @ ElasticSuccess(Some(_)) =>
        logger.info(s"✅ Successfully retrieved document with id '$id' from index '$index'")
        success
      case _ @ElasticSuccess(None) =>
        val error =
          ElasticError(
            message = s"Document with id '$id' not found in index '$index'",
            statusCode = Some(404),
            index = Some(index),
            operation = Some("get")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to retrieve document with id '$id' from index '$index': ${error.message}"
        )
        failure
    }
  }

  /** Get an entity by its id from the given index.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   an Option containing the entity if it was found, None otherwise
    */
  def getAs[U <: AnyRef](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Option[U]] = {
    val indexType = maybeType.getOrElse(m.runtimeClass.getSimpleName.toLowerCase)
    val indexName = index.getOrElse(indexType)
    get(id, indexName).flatMap {
      case Some(jsonString) =>
        ElasticResult.attempt {
          serialization.read[U](jsonString)(formats, m)
        } match {
          case ElasticSuccess(entity) =>
            logger.info(s"✅ Successfully retrieved document with id '$id' from index '$indexName'")
            ElasticSuccess(Some(entity))
          case failure @ ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to retrieve document with id '$id' from index '$indexName': ${error.message}"
            )
            failure
        }
      case None =>
        val error =
          ElasticError(
            message = s"Document with id '$id' not found in index '$indexName'",
            statusCode = Some(404),
            index = Some(indexName),
            operation = Some("get")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
    }
  }

  /** Get a document by its id from the given index asynchronously.
    * @param id
    *   - the id of the document to get
    * @param index
    *   - the name of the index to get the document from
    * @return
    *   a Future that completes with an Option containing the document as a JSON string if it was
    *   found, None otherwise
    */
  def getAsync(
    id: String,
    index: String
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] = {
    validateIndexName(index) match {
      case Some(error) =>
        return Future.successful(
          ElasticResult.failure(
            error.copy(
              message = s"Invalid index: ${error.message}",
              statusCode = Some(400),
              index = Some(index),
              operation = Some("getAsync")
            )
          )
        )
      case None => // continue
    }

    logger.debug(s"Getting document with id '$id' from index '$index' asynchronously")

    val promise: Promise[ElasticResult[Option[String]]] = Promise()
    executeGetAsync(index, id) onComplete {
      case scala.util.Success(result) =>
        result match {
          case success @ ElasticSuccess(Some(_)) =>
            logger.info(s"✅ Successfully retrieved document with id '$id' from index '$index'")
            promise.success(success)
          case _ @ElasticSuccess(None) =>
            val error =
              ElasticError(
                message = s"Document with id '$id' not found in index '$index'",
                statusCode = Some(404),
                index = Some(index),
                operation = Some("getAsync")
              )
            logger.error(s"❌ ${error.message}")
            promise.success(ElasticResult.failure(error))
          case failure @ ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to retrieve document with id '$id' from index '$index': ${error.message}"
            )
            promise.success(failure)
        }
      case scala.util.Failure(exception) =>
        val error =
          ElasticError(
            message =
              s"Exception occurred while retrieving document with id '$id' from index '$index': ${exception.getMessage}",
            statusCode = Some(500),
            index = Some(index),
            operation = Some("getAsync")
          )
        logger.error(s"❌ ${error.message}")
        promise.success(ElasticResult.failure(error))
    }

    promise.future

  }

  /** Get an entity by its id from the given index asynchronously.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with an Option containing the entity if it was found, None otherwise
    */
  def getAsyncAs[U <: AnyRef](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Option[U]]] = {
    val indexType = maybeType.getOrElse(m.runtimeClass.getSimpleName.toLowerCase)
    val indexName = index.getOrElse(indexType)
    getAsync(id, indexName).flatMap {
      case ElasticSuccess(Some(jsonString)) =>
        ElasticResult
          .attempt {
            serialization.read[U](jsonString)(formats, m)
          }
          .map { entity =>
            logger.info(s"✅ Successfully retrieved document with id '$id' from index '$indexName'")
            Some(entity)
          } match {
          case success @ ElasticSuccess(_) => Future.successful(success)
          case failure @ ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to retrieve document with id '$id' from index '$indexName': ${error.message}"
            )
            Future.successful(failure)
        }
      case ElasticSuccess(None) =>
        val error =
          ElasticError(
            message = s"Document with id '$id' not found in index '$indexName'",
            statusCode = Some(404),
            index = Some(indexName),
            operation = Some("getAsyncAs")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticResult.failure(error))
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to retrieve document with id '$id' from index '$indexName': ${error.message}"
        )
        Future.successful(failure)
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeGet(
    index: String,
    id: String
  ): ElasticResult[Option[String]]

  private[client] def executeGetAsync(
    index: String,
    id: String
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]]
}
