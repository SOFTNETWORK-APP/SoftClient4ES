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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.ElasticClientHelpers
import app.softnetwork.elastic.client.result.{ElasticError, ElasticResult}
import io.searchbox.action.{AbstractAction, Action}
import io.searchbox.client.JestResult

import scala.concurrent.Promise
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

trait JestClientHelpers extends ElasticClientHelpers { _: JestClientCompanion =>

  // ========================================================================
  // GENERIC METHODS FOR EXECUTIVE JEST ACTIONS
  // ========================================================================

  //format:off
  /** Execute a Jest action with a generic transformation of the result. This generic method allows
    * you to execute any Jest action and transform the result into a type T using a transformation
    * function.
    * @tparam R
    *   type of the Jest result (usually JestResult or a subclass)
    * @tparam T
    *   type of the desired final result
    * @param operation
    *   name of the operation (for logging and error context)
    * @param index
    *   relevant index (optional, for logging)
    * @param retryable
    *   true if the operation can be retried in case of a transient error
    * @param action
    *   function constructing the Jest action to be executed
    * @param transformer
    *   function transforming the JestResult into T
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeJestAction[JestResult, Boolean](
    *   operation = "createIndex",
    *   index = Some("my-index"),
    *   retryable = false
    * )(
    *   action = new CreateIndex.Builder("my-index").build()
    * )(
    *   transformer = result => result.isSucceeded
    * )
    * }}}
    */
  //format:on
  private[client] def executeJestAction[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => AbstractAction[R]
  )(
    transformer: R => T
  ): ElasticResult[T] = {

    val indexStr = index.map(i => s" on index '$i'").getOrElse("")

    logger.debug(s"Executing operation '$operation'$indexStr")

    // ✅ Execution with exception handling
    val tryResult: Try[R] = Try {
      apply().execute(action)
    }

    // ✅ Conversion to ElasticResult[R]
    val elasticResult: ElasticResult[R] = tryResult match {
      case Success(result) =>
        ElasticResult.success(result)

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

    // ✅ Jest success check and transformation
    elasticResult.flatMap { result =>
      if (result.isSucceeded) {
        // ✅ Success: applying the transformation
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
                statusCode = Some(result.getResponseCode),
                operation = Some(operation)
              )
            )
        }
      } else {
        // ✅ Failure: extract the error
        val errorMessage = Option(result.getErrorMessage)
          .filter(_.nonEmpty)
          .getOrElse("Unknown error")

        val statusCode = result.getResponseCode match {
          case 0    => None // No HTTP response
          case code => Some(code)
        }

        val error = ElasticError(
          message = errorMessage,
          cause = None,
          statusCode = statusCode,
          operation = Some(operation)
        )

        // ✅ Log according to severity
        logError(operation, indexStr, error)

        ElasticResult.failure(error)
      }
    }
  }

  /** Simplified variant for operations returning Boolean values.
    *
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function constructing the Jest action
    * @return
    *   ElasticResult[Boolean]
    */
  private[client] def executeJestBooleanAction[R <: JestResult: ClassTag](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => AbstractAction[R]
  ): ElasticResult[Boolean] = {
    executeJestAction[R, Boolean](operation, index, retryable)(action)(_.isSucceeded)
  }

  //format:off
  /** Variant to execute an action and extract a specific field from the JSON.
    * @tparam R
    *   type of the Jest result
    * @tparam T
    *   type of the final result
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function constructing the Jest action
    * @param extractor
    *   function extracting T from the JsonObject of the result
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * executeJestWithExtractor[JestResult, Int](
    *   operation = "getDocCount",
    *   index = Some("my-index")
    * )(
    *   action = new Count.Builder().addIndex("my-index").build()
    * )(
    *   extractor = json => json.get("count").getAsInt
    * )
    * }}}
    */
  //format:on
  private[client] def executeJestWithExtractor[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => AbstractAction[R]
  )(
    extractor: com.google.gson.JsonObject => T
  ): ElasticResult[T] = {
    executeJestAction[R, T](operation, index, retryable)(action) { result =>
      extractor(result.getJsonObject)
    }
  }

  //format:off
  /** Variant to execute an action and parse the complete JSON.
    * @tparam R
    *   type of the Jest result
    * @tparam T
    *   type of the final result (typically a case class)
    * @param operation
    *   name of the operation
    * @param index
    *   index concerned (optional)
    * @param retryable
    *   true if retryable
    * @param action
    *   function constructing the Jest action
    * @param parser
    *   function parsing the JSON into T (uses json4s or another)
    * @return
    *   ElasticResult[T]
    *
    * @example
    * {{{
    * case class IndexStats(docsCount: Long, storeSize: String)
    *
    * executeJestWithParser[JestResult, IndexStats](
    *   operation = "getIndexStats",
    *   index = Some("my-index")
    * )(
    *   action = new Stats.Builder().addIndex("my-index").build()
    * )(
    *   parser = json => {
    *     implicit val formats = DefaultFormats
    *     parse(json).extract[IndexStats]
    *   }
    * )
    * }}}
    */
  //format:on
  private[client] def executeJestWithParser[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => AbstractAction[R]
  )(
    parser: String => T
  ): ElasticResult[T] = {
    executeJestAction[R, T](operation, index, retryable)(action) { result =>
      parser(result.getJsonString)
    }
  }

  //format:off
  /** Asynchronous variant to execute a Jest action with a generic transformation of the result.
    * @tparam R
    *   type of the Jest result (usually JestResult or a subclass)
    * @tparam T
    *   type of the desired final result
    * @param operation
    *   name of the operation (for logging and error context)
    * @param index
    *   relevant index (optional, for logging)
    * @param retryable
    *   true if the operation can be retried in case of a transient error
    * @param action
    *   function constructing the Jest action to be executed
    * @param transformer
    *   function transforming the JestResult into T
    * @return
    *   Future ElasticResult[T]
    *
    * @example
    * {{{
    * executeAsyncJestAction[JestResult, Boolean](
    *   operation = "createIndex",
    *   index = Some("my-index"),
    *   retryable = false
    * )(
    *   action = new CreateIndex.Builder("my-index").build()
    * )(
    *   transformer = result => result.isSucceeded
    * )
    * }}}
    */
  //format:on
  private[client] def executeAsyncJestAction[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Action[R]
  )(
    transformer: R => T
  )(implicit ec: scala.concurrent.ExecutionContext): scala.concurrent.Future[ElasticResult[T]] = {
    val indexStr = index.map(i => s" on index '$i'").getOrElse("")

    logger.debug(s"Executing operation '$operation'$indexStr asynchronously")

    val promise: Promise[ElasticResult[T]] = Promise()
    import JestClientResultHandler._
    apply().executeAsyncPromise(action) onComplete {
      case Success(result) =>
        if (result.isSucceeded) {
          logger.debug(s"Operation '$operation'$indexStr succeeded asynchronously")
          // ✅ Success: applying the transformation
          Try(transformer(result)) match {
            case Success(transformed) =>
              promise.success(ElasticResult.success(transformed))
            case Failure(ex) =>
              logger.error(s"Transformation failed for operation '$operation'$indexStr", ex)
              promise.success(
                ElasticResult.failure(
                  ElasticError(
                    message = s"Failed to transform result: ${ex.getMessage}",
                    cause = Some(ex),
                    statusCode = Some(result.getResponseCode),
                    operation = Some(operation)
                  )
                )
              )
          }
        } else {
          // ✅ Failure: extract the error
          val errorMessage = Option(result.getErrorMessage)
            .filter(_.nonEmpty)
            .getOrElse("Unknown error")
          val statusCode = result.getResponseCode match {
            case 0    => None // No HTTP response
            case code => Some(code)
          }
          val error = ElasticError(
            message = errorMessage,
            cause = None,
            statusCode = statusCode,
            operation = Some(operation)
          )
          // ✅ Log according to severity
          logError(operation, indexStr, error)
          promise.success(ElasticResult.failure(error))
        }
      case Failure(ex) =>
        logger.error(s"Exception during operation '$operation'$indexStr: ${ex.getMessage}", ex)
        val error = ElasticError(
          message = s"Exception during $operation: ${ex.getMessage}",
          cause = Some(ex),
          statusCode = None,
          operation = Some(operation)
        )
        promise.success(ElasticResult.failure(error))
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
    * @param action
    *   function constructing the Jest action
    * @return
    *   Future ElasticResult[Boolean]
    */
  private[client] def executeAsyncJestBooleanAction[R <: JestResult: ClassTag](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Action[R]
  )(implicit
    ec: scala.concurrent.ExecutionContext
  ): scala.concurrent.Future[ElasticResult[Boolean]] = {
    executeAsyncJestAction[R, Boolean](operation, index, retryable)(action)(_.isSucceeded)
  }
}
