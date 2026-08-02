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

import app.softnetwork.elastic.client.result.ElasticError
import io.searchbox.action.Action
import io.searchbox.client.{JestClient, JestResult, JestResultHandler}
import io.searchbox.core.BulkResult

import scala.concurrent.{Future, Promise}

/** Created by smanciot on 28/04/17.
  */
private class JestClientResultHandler[T <: JestResult] extends JestResultHandler[T] {

  protected val promise: Promise[T] = Promise()

  override def completed(result: T): Unit =
    if (!result.isSucceeded)
      // SoftClient4ES#184 — this handler is the ONLY place a non-succeeded `JestResult` is turned
      // into a throwable, and a plain `Exception` here used to destroy the HTTP status the result
      // carried: `JestClientHelpers.executeAsyncJestAction`'s `Failure` branch could then only
      // report `statusCode = None`, so ES 6 Jest answered `None` (never `Some(404)`) for every
      // asynchronous call against a missing index. Raising an `ElasticError` instead keeps the
      // status readable through `ElasticClientHelpers.statusOf`, whose core default already
      // understands the framework's own status-bearing throwables. The message is deliberately
      // byte-identical to the previous `Exception`'s so downstream text does not move.
      //
      // ⚠️ `ElasticError` extends `Throwable`, NOT `Exception`. The single consumer of this
      // promise (`JestClientHelpers.executeAsyncJestAction`) matches `case Failure(ex)`, which
      // catches any throwable, so nothing changes today — but a future
      // `recover { case e: Exception => … }` on the ASYNC Jest path would silently stop catching
      // this. (`JestScrollApi`'s `case ex: Exception` recovers are on the synchronous
      // `apply().execute(...)` path and are unaffected.)
      promise.failure(
        ElasticError(
          message = s"${result.getErrorMessage} - ${result.getJsonString}",
          statusCode = result.getResponseCode match {
            case 0    => None // No HTTP response
            case code => Some(code)
          }
        )
      )
    else {
      result match {
        case r: BulkResult if !r.getFailedItems.isEmpty =>
          promise.failure(
            new Exception(s"We don't allow any failed item while indexing ${result.getJsonString}")
          )
        case _ => promise.success(result)

      }
    }

  override def failed(exception: Exception): Unit = promise.failure(exception)

  def future: Future[T] = promise.future

}

object JestClientResultHandler {

  implicit class PromiseJestClient(jestClient: JestClient) {
    def executeAsyncPromise[T <: JestResult](clientRequest: Action[T]): Future[T] = {
      val resultHandler = new JestClientResultHandler[T]()
      jestClient.executeAsync(clientRequest, resultHandler)
      resultHandler.future
    }
  }
}
