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

package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.client.ElasticClientHelpers
import app.softnetwork.elastic.client.result.{ElasticError, ElasticResult}

import scala.util.{Failure, Success, Try}

trait JavaClientHelpers extends ElasticClientHelpers with JavaClientConversion {
  _: JavaClientCompanion =>

  // ========================================================================
  // GENERIC METHODS FOR EXECUTING JAVA CLIENT ACTIONS
  // ========================================================================

  //format:off
  /** Execute a Java Client action with a generic transformation of the result.
    *
    * @tparam Resp
    *   type of the response
    * @tparam T
    *   type of the desired final result
    * @param operation
    *   name of the operation (for logging and error context)
    * @param index
    *   relevant index (optional, for logging)
    * @param retryable
    *   true if the operation can be retried in case of a transient error
    * @param action
    *   function executing the action and returning the response
    * @param transformer
    *   function transforming the response into T
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeJavaAction[CreateIndexResponse, Boolean](
    *   operation = "createIndex",
    *   index = Some("my-index"),
    *   retryable = false
    * )(
    *   action = apply().indices().create(builder => builder.index("my-index"))
    * )(
    *   transformer = resp => resp.acknowledged()
    * )
    * }}}
    */
  //format:on
  private[client] def executeJavaAction[Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    transformer: Resp => T
  ): ElasticResult[T] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing operation '$operation'$indexStr")

    // ✅ Execution with exception handling
    val tryResult: Try[Resp] = Try {
      action
    }

    // ✅ Conversion to ElasticResult[Resp]
    val elasticResult: ElasticResult[Resp] = tryResult match {
      case Success(result) =>
        ElasticResult.success(result)
      case Failure(ex: co.elastic.clients.elasticsearch._types.ElasticsearchException) =>
        // Extract error details from Elasticsearch exception
        val statusCode = Option(ex.status()).map(_.intValue())
        val errorType = Option(ex.error()).flatMap(e => Option(e.`type`()))
        val reason = Option(ex.error()).flatMap(e => Option(e.reason()))

        val message =
          s"Elasticsearch error during $operation: ${errorType.getOrElse("unknown")} - ${reason
            .getOrElse(ex.getMessage)}"
        logger.error(s"$message$indexStr", ex)

        ElasticResult.failure(
          ElasticError(
            message = message,
            cause = Some(ex),
            statusCode = statusCode,
            operation = Some(operation)
          )
        )
      case Failure(ex: java.io.IOException) =>
        logger.error(s"IO exception during operation '$operation'$indexStr: ${ex.getMessage}", ex)
        ElasticResult.failure(
          ElasticError(
            message = s"IO error during $operation: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = None,
            operation = Some(operation)
          )
        )
      case Failure(ex) =>
        logger.error(s"Exception during operation '$operation'$indexStr: ${ex.getMessage}", ex)
        ElasticResult.failure(
          ElasticError(
            message = s"Exception during $operation: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = None,
            operation = Some(operation)
          )
        )
    }

    // ✅ Apply transformation
    elasticResult.flatMap { result =>
      Try(transformer(result)) match {
        case Success(transformed) =>
          logger.debug(s"Operation '$operation'$indexStr succeeded")
          ElasticResult.success(transformed)
        case Failure(ex) =>
          logger.error(s"Transformation failed for operation '$operation'$indexStr", ex)
          ElasticResult.failure(
            ElasticError(
              message = s"Failed to transform result: ${ex.getMessage}",
              cause = Some(ex),
              statusCode = None,
              operation = Some(operation)
            )
          )
      }
    }
  }

  /** Simplified variant for operations returning Boolean values (acknowledged).
    *
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function executing the action
    * @param acknowledgedExtractor
    *   function to extract the acknowledged status
    * @return
    *   ElasticResult[Boolean]
    */
  private[client] def executeJavaBooleanAction[Resp](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    acknowledgedExtractor: Resp => Boolean
  ): ElasticResult[Boolean] = {
    executeJavaAction[Resp, Boolean](operation, index, retryable)(action)(acknowledgedExtractor)
  }

  //format:off
  /** Variant to execute an action and extract a specific field from the response.
    *
    * @tparam Resp
    *   type of the response
    * @tparam T
    *   type of the final result
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function executing the action
    * @param extractor
    *   function extracting T from the response
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeJavaWithExtractor[CountResponse, Long](
    *   operation = "countDocuments",
    *   index = Some("my-index")
    * )(
    *   action = apply().count(builder => builder.index("my-index"))
    * )(
    *   extractor = resp => resp.count()
    * )
    * }}}
    */
  //format:on
  private[client] def executeJavaWithExtractor[Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    extractor: Resp => T
  ): ElasticResult[T] = {
    executeJavaAction[Resp, T](operation, index, retryable)(action)(extractor)
  }

  //format:off
  /** Asynchronous variant to execute a Java Client action. Note: The Java Client doesn't have
    * native async support, so we wrap in Future.
    *
    * @tparam Resp
    *   type of the response
    * @tparam T
    *   type of the desired final result
    * @param operation
    *   name of the operation
    * @param index
    *   relevant index (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function executing the action
    * @param transformer
    *   function transforming the response into T
    * @return
    *   Future of [ElasticResult[T]
    *
    * @example
    * {{{
    * executeAsyncJavaAction[IndexResponse, String](
    *   operation = "indexDocument",
    *   index = Some("my-index")
    * )(
    *   action = apply().index(builder => builder.index("my-index").document(doc))
    * )(
    *   transformer = resp => resp.id()
    * )
    * }}}
    */
  //format:on
  private[client] def executeAsyncJavaAction[Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    transformer: Resp => T
  )(implicit ec: scala.concurrent.ExecutionContext): scala.concurrent.Future[ElasticResult[T]] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing operation '$operation'$indexStr asynchronously")

    scala.concurrent.Future {
      executeJavaAction[Resp, T](operation, index, retryable)(action)(transformer)
    }
  }

  /** Simplified asynchronous variant for operations returning Boolean values.
    *
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function executing the action
    * @param acknowledgedExtractor
    *   function to extract the acknowledged status
    * @return
    *   Future of ElasticResult[Boolean]
    */
  private[client] def executeAsyncJavaBooleanAction[Resp](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    acknowledgedExtractor: Resp => Boolean
  )(implicit
    ec: scala.concurrent.ExecutionContext
  ): scala.concurrent.Future[ElasticResult[Boolean]] = {
    executeAsyncJavaAction[Resp, Boolean](operation, index, retryable)(action)(
      acknowledgedExtractor
    )
  }

}
