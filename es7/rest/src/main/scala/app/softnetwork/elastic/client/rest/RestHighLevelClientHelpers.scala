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

package app.softnetwork.elastic.client.rest

import app.softnetwork.elastic.client.ElasticClientHelpers
import app.softnetwork.elastic.client.result.{ElasticError, ElasticResult}

import scala.concurrent.Promise
import scala.util.{Failure, Success, Try}

trait RestHighLevelClientHelpers extends ElasticClientHelpers { _: RestHighLevelClientCompanion =>

  // ========================================================================
  // GENERIC METHODS FOR EXECUTING REST HIGH LEVEL CLIENT ACTIONS
  // ========================================================================

  //format:off
  /** Execute a Rest High Level Client action with a generic transformation of the result.
    *
    * @tparam Req
    *   type of the request
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
    * @param request
    *   the request to be executed
    * @param executor
    *   function executing the request and returning the response
    * @param transformer
    *   function transforming the response into T
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeRestAction[CreateIndexRequest, CreateIndexResponse, Boolean](
    *   operation = "createIndex",
    *   index = Some("my-index"),
    *   retryable = false
    * )(
    *   request = new CreateIndexRequest("my-index")
    * )(
    *   executor = req => apply().indices().create(req, RequestOptions.DEFAULT)
    * )(
    *   transformer = resp => resp.isAcknowledged
    * )
    * }}}
    */
  //format:on
  private[client] def executeRestAction[Req, Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => Req
  )(
    executor: Req => Resp
  )(
    transformer: Resp => T
  ): ElasticResult[T] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing operation '$operation'$indexStr")

    // ✅ Execution with exception handling
    val tryResult: Try[Resp] = Try {
      executor(request)
    }

    // ✅ Conversion to ElasticResult[Resp]
    val elasticResult: ElasticResult[Resp] = tryResult match {
      case Success(result) =>
        ElasticResult.success(result)
      case Failure(ex: org.elasticsearch.ElasticsearchException) =>
        // Extract status code from Elasticsearch exception
        val statusCode = Option(ex.status()).map(_.getStatus)
        logger.error(
          s"Elasticsearch exception during operation '$operation'$indexStr: ${ex.getMessage}",
          ex
        )
        ElasticResult.failure(
          ElasticError(
            message = s"Elasticsearch error during $operation: ${ex.getDetailedMessage}",
            cause = Some(ex),
            statusCode = statusCode,
            operation = Some(operation)
          )
        )
      case Failure(ex: org.elasticsearch.client.ResponseException) =>
        val statusCode = Some(ex.getResponse.getStatusLine.getStatusCode)
        logger.error(
          s"Response exception during operation '$operation'$indexStr: ${ex.getMessage}",
          ex
        )
        ElasticResult.failure(
          ElasticError(
            message = s"HTTP error during $operation: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = statusCode,
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
    * @param request
    *   the request to be executed
    * @param executor
    *   function executing the request
    * @return
    *   ElasticResult[Boolean]
    */
  private[client] def executeRestBooleanAction[
    Req,
    Resp <: org.elasticsearch.action.support.master.AcknowledgedResponse
  ](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => Req
  )(
    executor: Req => Resp
  ): ElasticResult[Boolean] = {
    executeRestAction[Req, Resp, Boolean](operation, index, retryable)(request)(executor)(
      _.isAcknowledged
    )
  }

  //format:off
  /** Variant to execute an action using the low-level REST client. Useful for operations not
    * supported by the high-level client.
    *
    * @tparam T
    *   type of the final result
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param request
    *   the low-level Request
    * @param transformer
    *   function transforming the Response into T
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeRestLowLevelAction[String](
    *   operation = "customEndpoint",
    *   index = Some("my-index")
    * )(
    *   request = new Request("GET", "/my-index/_custom")
    * )(
    *   transformer = resp => EntityUtils.toString(resp.getEntity)
    * )
    * }}}
    */
  //format:on
  private[client] def executeRestLowLevelAction[T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => org.elasticsearch.client.Request
  )(
    transformer: org.elasticsearch.client.Response => T
  ): ElasticResult[T] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing low-level operation '$operation'$indexStr")

    // ✅ Execution with exception handling
    val tryResult: Try[org.elasticsearch.client.Response] = Try {
      apply().getLowLevelClient.performRequest(request)
    }

    // ✅ Conversion to ElasticResult[Response]
    val elasticResult: ElasticResult[org.elasticsearch.client.Response] = tryResult match {
      case Success(result) =>
        ElasticResult.success(result)
      case Failure(ex: org.elasticsearch.client.ResponseException) =>
        val statusCode = Some(ex.getResponse.getStatusLine.getStatusCode)
        logger.error(
          s"Response exception during operation '$operation'$indexStr: ${ex.getMessage}",
          ex
        )
        ElasticResult.failure(
          ElasticError(
            message = s"HTTP error during $operation: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = statusCode,
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

    // ✅ Check status and apply transformation
    elasticResult.flatMap { result =>
      val statusCode = result.getStatusLine.getStatusCode

      if (statusCode >= 200 && statusCode < 300) {
        // ✅ Success: applying the transformation
        Try(transformer(result)) match {
          case Success(transformed) =>
            logger.debug(s"Operation '$operation'$indexStr succeeded with status $statusCode")
            ElasticResult.success(transformed)
          case Failure(ex) =>
            logger.error(s"Transformation failed for operation '$operation'$indexStr", ex)
            ElasticResult.failure(
              ElasticError(
                message = s"Failed to transform result: ${ex.getMessage}",
                cause = Some(ex),
                statusCode = Some(statusCode),
                operation = Some(operation)
              )
            )
        }
      } else {
        // ✅ Failure: extract the error
        val errorMessage = Option(result.getStatusLine.getReasonPhrase)
          .filter(_.nonEmpty)
          .getOrElse("Unknown error")

        val error = ElasticError(
          message = errorMessage,
          cause = None,
          statusCode = Some(statusCode),
          operation = Some(operation)
        )

        logError(operation, indexStr, error)
        ElasticResult.failure(error)
      }
    }
  }

  //format:off
  /** Asynchronous variant to execute a Rest High Level Client action.
    *
    * @tparam Req
    *   type of the request
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
    * @param request
    *   the request to be executed
    * @param executor
    *   function executing the request asynchronously
    * @param transformer
    *   function transforming the response into T
    * @return
    *   Future[ElasticResult[T]]
    *
    * @example
    * {{{
    * executeAsyncRestAction[IndexRequest, IndexResponse, String](
    *   operation = "indexDocument",
    *   index = Some("my-index")
    * )(
    *   request = new IndexRequest("my-index").source(...)
    * )(
    *   executor = (req, listener) => apply().indexAsync(req, RequestOptions.DEFAULT, listener)
    * )(
    *   transformer = resp => resp.getId
    * )
    * }}}
    */
  //format:on
  private[client] def executeAsyncRestAction[Req, Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => Req
  )(
    executor: (Req, org.elasticsearch.action.ActionListener[Resp]) => Unit
  )(
    transformer: Resp => T
  )(implicit ec: scala.concurrent.ExecutionContext): scala.concurrent.Future[ElasticResult[T]] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing operation '$operation'$indexStr asynchronously")

    val promise: Promise[ElasticResult[T]] = Promise()

    try {
      val listener = new org.elasticsearch.action.ActionListener[Resp] {
        override def onResponse(response: Resp): Unit = {
          logger.debug(s"Operation '$operation'$indexStr succeeded asynchronously")

          // ✅ Success: applying the transformation
          Try(transformer(response)) match {
            case Success(transformed) =>
              promise.success(ElasticResult.success(transformed))
            case Failure(ex) =>
              logger.error(s"Transformation failed for operation '$operation'$indexStr", ex)
              promise.success(
                ElasticResult.failure(
                  ElasticError(
                    message = s"Failed to transform result: ${ex.getMessage}",
                    cause = Some(ex),
                    statusCode = None,
                    operation = Some(operation)
                  )
                )
              )
          }
        }

        override def onFailure(ex: Exception): Unit = {
          val (message, statusCode) = ex match {
            case esEx: org.elasticsearch.ElasticsearchException =>
              (
                s"Elasticsearch error during $operation: ${esEx.getDetailedMessage}",
                Option(esEx.status()).map(_.getStatus)
              )
            case _ =>
              (s"Exception during $operation: ${ex.getMessage}", None)
          }

          logger.error(s"Exception during operation '$operation'$indexStr: ${ex.getMessage}", ex)

          promise.success(
            ElasticResult.failure(
              ElasticError(
                message = message,
                cause = Some(ex),
                statusCode = statusCode,
                operation = Some(operation)
              )
            )
          )
        }
      }

      executor(request, listener)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to initiate async operation '$operation'$indexStr", ex)
        promise.success(
          ElasticResult.failure(
            ElasticError(
              message = s"Failed to initiate $operation: ${ex.getMessage}",
              cause = Some(ex),
              statusCode = None,
              operation = Some(operation)
            )
          )
        )
    }

    promise.future
  }

  /** Simplified asynchronous variant for operations returning Boolean values.
    *
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param request
    *   the request to be executed
    * @param executor
    *   function executing the request asynchronously
    * @return
    *   Future of ElasticResult[Boolean]
    */
  private[client] def executeAsyncRestBooleanAction[
    Req,
    Resp <: org.elasticsearch.action.support.master.AcknowledgedResponse
  ](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => Req
  )(
    executor: (Req, org.elasticsearch.action.ActionListener[Resp]) => Unit
  )(implicit
    ec: scala.concurrent.ExecutionContext
  ): scala.concurrent.Future[ElasticResult[Boolean]] = {
    executeAsyncRestAction[Req, Resp, Boolean](operation, index, retryable)(request)(executor)(
      _.isAcknowledged
    )
  }
}
