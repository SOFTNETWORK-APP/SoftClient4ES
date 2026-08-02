package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import org.mockito.MockitoSugar
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** SoftClient4ES#184 — the three sibling asynchronous flattening sites (`IndexApi.indexAsync`,
  * `UpdateApi.updateAsync`, `DeleteApi.deleteAsync`) must derive the HTTP status exactly like
  * `GetApi.getAsync`, and `updateAsync` must report its own identity (AC 6 — it used to be a
  * verbatim copy-paste of `DeleteApi`, telling operators an UPDATE failure came from
  * `deleteAsync`).
  *
  * Docker-free: the `executeXAsync` members are stubbed, so a failure here means the status
  * derivation or the operation identity is wrong, never that a cluster was unavailable.
  */
class AsyncFlatteningStatusSpec extends AnyWordSpec with Matchers with MockitoSugar {

  private val mockLogger: Logger = mock[Logger]

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val timeout = Duration(10, TimeUnit.SECONDS)

  private class TestApi(failure: Throwable) extends NopeClientApi {
    override protected def logger: Logger = mockLogger

    override private[client] def executeIndexAsync(
      index: String,
      id: String,
      source: String,
      wait: Boolean
    )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = Future.failed(failure)

    override private[client] def executeUpdateAsync(
      index: String,
      id: String,
      source: String,
      upsert: Boolean,
      wait: Boolean
    )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = Future.failed(failure)

    override private[client] def executeDeleteAsync(index: String, id: String, wait: Boolean)(
      implicit ec: ExecutionContext
    ): Future[ElasticResult[Boolean]] = Future.failed(failure)
  }

  private val notFound = ElasticError("no such index [absent]", statusCode = Some(404))
  private val refused = new java.net.ConnectException("Connection refused")

  "indexAsync" should {

    "preserve the HTTP status of a status-bearing exception" in {
      val error =
        Await.result(new TestApi(notFound).indexAsync("some_index", "1", "{}"), timeout).error.get

      error.statusCode shouldBe Some(404)
      error.operation shouldBe Some("indexAsync")
      error.index shouldBe Some("some_index")
      error.cause shouldBe Some(notFound)
    }

    "fall back to 500 for a transport failure" in {
      Await
        .result(new TestApi(refused).indexAsync("some_index", "1", "{}"), timeout)
        .error
        .get
        .statusCode shouldBe Some(500)
    }
  }

  "updateAsync" should {

    "preserve the HTTP status of a status-bearing exception" in {
      val error = Await
        .result(new TestApi(notFound).updateAsync("some_index", "1", "{}", upsert = false), timeout)
        .error
        .get

      error.statusCode shouldBe Some(404)
      error.cause shouldBe Some(notFound)
    }

    "report its own operation and an 'updating' message, not deleteAsync's (AC 6)" in {
      val error = Await
        .result(new TestApi(refused).updateAsync("some_index", "1", "{}", upsert = false), timeout)
        .error
        .get

      error.operation shouldBe Some("updateAsync")
      error.message should include("updating")
      error.message should not include "deleting"
      error.statusCode shouldBe Some(500)
    }
  }

  "deleteAsync" should {

    "preserve the HTTP status of a status-bearing exception" in {
      val error = Await
        .result(new TestApi(notFound).deleteAsync("1", "some_index", wait = false), timeout)
        .error
        .get

      error.statusCode shouldBe Some(404)
      error.operation shouldBe Some("deleteAsync")
      error.cause shouldBe Some(notFound)
    }

    "fall back to 500 for a transport failure" in {
      Await
        .result(new TestApi(refused).deleteAsync("1", "some_index", wait = false), timeout)
        .error
        .get
        .statusCode shouldBe Some(500)
    }
  }
}
