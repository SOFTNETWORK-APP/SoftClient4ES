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

import app.softnetwork.elastic.client.{ElasticClientHelpers, ShardFailures}
import app.softnetwork.elastic.client.result.{ElasticError, ElasticResult}
import app.softnetwork.elastic.sql.serialization.JacksonConfig

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
        logger.warn(
          s"Elasticsearch exception during operation '$operation'$indexStr: ${ex.getMessage}"
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
        logger.warn(s"Response exception during operation '$operation'$indexStr: ${ex.getMessage}")
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
              statusCode = Some(500),
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
        logger.warn(s"Response exception during operation '$operation'$indexStr: ${ex.getMessage}")
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
                statusCode = Some(500),
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

  /** Asynchronous variant of [[executeRestLowLevelAction]] (#228): same error mapping and status
    * handling, driven by the low-level client's [[org.elasticsearch.client.ResponseListener]].
    */
  private[client] def executeAsyncRestLowLevelAction[T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    request: => org.elasticsearch.client.Request
  )(
    transformer: org.elasticsearch.client.Response => T
  )(implicit ec: scala.concurrent.ExecutionContext): scala.concurrent.Future[ElasticResult[T]] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")
    logger.debug(s"Executing low-level operation '$operation'$indexStr asynchronously")

    val promise: Promise[ElasticResult[T]] = Promise()

    def transform(result: org.elasticsearch.client.Response): ElasticResult[T] = {
      val statusCode = result.getStatusLine.getStatusCode
      if (statusCode >= 200 && statusCode < 300) {
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
                statusCode = Some(500),
                operation = Some(operation)
              )
            )
        }
      } else {
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

    try {
      val listener = new org.elasticsearch.client.ResponseListener {
        override def onSuccess(response: org.elasticsearch.client.Response): Unit =
          promise.success(transform(response))

        override def onFailure(ex: Exception): Unit = {
          val (message, statusCode) = ex match {
            case respEx: org.elasticsearch.client.ResponseException =>
              (
                s"HTTP error during $operation: ${respEx.getMessage}",
                Try(respEx.getResponse.getStatusLine.getStatusCode).toOption
              )
            case _ =>
              (s"Exception during $operation: ${ex.getMessage}", None)
          }

          logger.warn(s"Exception during operation '$operation'$indexStr: ${ex.getMessage}")

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

      apply().getLowLevelClient.performRequestAsync(request, listener)
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
                    statusCode = Some(500),
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

          logger.warn(s"Exception during operation '$operation'$indexStr: ${ex.getMessage}")

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

  /** @see
    *   [[app.softnetwork.elastic.client.ElasticClientHelpers.statusOf]]
    *
    * `org.elasticsearch.ElasticsearchException.status()` returns a `RestStatus`
    * (`ElasticsearchStatusException`, raised for `index_not_found_exception`, is a subclass);
    * `org.elasticsearch.client.ResponseException` carries the raw HTTP status line. Both are
    * already mapped in the synchronous branches of this trait — this override makes the same
    * mapping reachable from `core`'s asynchronous flattening sites.
    *
    * `ResponseException` is read through `Try`: `getResponse` may be null and `getStatusLine` is
    * not null-checked either, and an extractor must never throw (see `statusOrServerError`).
    */
  override private[client] def statusOf(t: Throwable): Option[Int] =
    unwrapThrowable(t) match {
      case ex: org.elasticsearch.ElasticsearchException =>
        Option(ex.status()).map(_.getStatus)
      case ex: org.elasticsearch.client.ResponseException =>
        Try(ex.getResponse.getStatusLine.getStatusCode).toOption
      case ex: RestHighLevelClientHelpers.SearchPageFailure => ex.error.statusCode
      case other                                            => super.statusOf(other)
    }

  /** A search page Elasticsearch rejected for good: a 4xx except 408 / 429.
    *
    * A `ResponseException` is an `IOException`, which `retryWithBackoff` retries. A rejected page
    * fails fast instead, with [[pageRejection]]. A 408, a 429 and a 5xx keep the original
    * exception, so a transient cluster condition is still retried.
    */
  private[client] def isNonRetriable4xx(ex: org.elasticsearch.client.ResponseException): Boolean =
    statusOf(ex).exists(status => status >= 400 && status < 500 && status != 408 && status != 429)

  /** The failure of a search page Elasticsearch rejected ([[isNonRetriable4xx]]): Elasticsearch's
    * status and reason, for every paging door of this client.
    *
    *   - The message is read from the response body. It leads with the error's type and reason,
    *     `[<error.type>] <error.reason>`, as the ES 8 / ES 9 client reports them, then adds `:
    *     <error.root_cause[0].reason>` when the root cause says something else, then `: <why a
    *     shard failed>` ([[ShardFailures.shardReason]]), then ` [<error.index>]` when the error is
    *     about an index its message does not name: Elasticsearch 6 names it beside its reason only
    *     (`no such index`), later majors in the reason (`no such index [<index>]`). Without a
    *     top-level reason it is the root cause's reason; without a readable body, the HTTP status
    *     line.
    *   - The message names no class of the client and never the request: the request line names the
    *     host, the index and its parameters. The `ResponseException` is not kept as the cause for
    *     the same reason: its message carries that request line.
    *   - It is thrown as a [[RestHighLevelClientHelpers.SearchPageFailure]]: an `Exception`, as the
    *     page's failure always was, and not an `IOException`, so no retry takes it.
    */
  private[client] def pageRejection(
    ex: org.elasticsearch.client.ResponseException
  ): RestHighLevelClientHelpers.SearchPageFailure = {
    def text(node: com.fasterxml.jackson.databind.JsonNode): Option[String] =
      Option(node).filter(n => n.isTextual && n.textValue.trim.nonEmpty).map(_.textValue)
    val fromBody = Try {
      val error =
        JacksonConfig.objectMapper.readTree(ex.getResponse.getEntity.getContent).path("error")
      val reason = text(error.path("reason"))
      val rootCause = text(error.path("root_cause").path(0).path("reason"))
      val shardReason =
        ShardFailures.shardReason(error.path("failed_shards"), reason.toSeq ++ rootCause)
      val index = text(error.path("index")).filterNot(_ == "_na_")
      (reason.map(r => text(error.path("type")).fold(r)(t => s"[$t] $r")) match {
        case Some(head) =>
          Some(rootCause.filterNot(reason.contains).fold(head)(rc => s"$head: $rc"))
        case None => rootCause
      }).map(m => shardReason.fold(m)(r => s"$m: $r"))
        .map(m => index.filterNot(m.contains).fold(m)(i => s"$m [$i]"))
    }.toOption.flatten
    val message = fromBody
      .orElse(Try(ex.getResponse.getStatusLine.toString).toOption)
      .getOrElse("Elasticsearch rejected the search page")
    new RestHighLevelClientHelpers.SearchPageFailure(
      ElasticError(message = message, statusCode = statusOf(ex), operation = Some("search"))
    )
  }

  /** The failure of a search page Elasticsearch answered with HTTP 200 and failed shards (`None`
    * when no shard failed). Such a page is silent row loss on a paging path, so every paging door
    * fails, by the rule of every search door, core's [[ShardFailures]] (#292):
    *   - a 4xx (every failure is the caller's): a [[RestHighLevelClientHelpers.SearchPageFailure]],
    *     `<door>: n of m shards failed: ...`, with that status;
    *   - otherwise the door's own exception, `otherwise`, with its text as before.
    */
  private[client] def failedShardsPage(door: String, tree: com.fasterxml.jackson.databind.JsonNode)(
    otherwise: String => Exception
  ): Option[Exception] =
    ShardFailures.of(tree).map { failures =>
      val error = failures.toError
      if (error.statusCode.exists(status => status >= 400 && status < 500))
        new RestHighLevelClientHelpers.SearchPageFailure(
          error.copy(message = s"$door: ${error.message}", operation = Some("search"))
        )
      else otherwise(s"$door: ${failures.reasons}")
    }
}

object RestHighLevelClientHelpers {

  /** A search page that failed for good, with the `ElasticError` that describes it.
    *
    * It is an `Exception`, so every `Exception` arm that handled the page's failure before still
    * does (a stream's recovery, a Flight SQL producer, a Java caller); an `ElasticError` alone is a
    * `Throwable` only. It is not an `IOException`, so the retry policy does not take it. Its
    * message is the error's.
    */
  final class SearchPageFailure(val error: ElasticError)
      extends RuntimeException(error.message, error.cause.orNull)
}
