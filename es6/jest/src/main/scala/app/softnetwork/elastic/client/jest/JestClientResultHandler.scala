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
