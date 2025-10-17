/*
 * Copyright 2015 SOFTNETWORK
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
      promise.failure(new Exception(s"${result.getErrorMessage} - ${result.getJsonString}"))
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
