package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import org.mockito.MockitoSugar
import org.scalatest.BeforeAndAfterEach
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.Logger

import java.util.concurrent.{CompletionException, TimeUnit}
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}

/** SoftClient4ES#184 — the asynchronous flattening sites must map a thrown client exception back to
  * its HTTP status, falling back to 500 only when no status can be determined.
  *
  * Docker-free by construction: `executeGetAsync` is stubbed, so this spec fails for exactly one
  * reason — the status derivation — and never for cluster availability.
  */
class GetApiStatusSpec extends AnyWordSpec with Matchers with BeforeAndAfterEach with MockitoSugar {

  private val mockLogger: Logger = mock[Logger]

  implicit val ec: ExecutionContext = ExecutionContext.global

  class TestGetApi extends NopeClientApi {
    override protected def logger: Logger = mockLogger

    var asyncFailure: Option[Throwable] = None

    override private[client] def executeGetAsync(index: String, id: String)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[String]]] =
      asyncFailure match {
        case Some(t) => Future.failed(t)
        case None    => Future.successful(ElasticSuccess(Some("""{"id":"1"}""")))
      }
  }

  private var api: TestGetApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    api = new TestGetApi()
  }

  private def getAsyncResult(): ElasticResult[Option[String]] =
    Await.result(api.getAsync("1", "some_index"), Duration(10, TimeUnit.SECONDS))

  "statusOf" should {

    "read the status carried by an ElasticError" in {
      api.statusOf(ElasticError("boom", statusCode = Some(404))) shouldBe Some(404)
    }

    "read the status carried by an ElasticFailure" in {
      api.statusOf(
        ElasticFailure(ElasticError("boom", statusCode = Some(409)))
      ) shouldBe Some(409)
    }

    "unwrap a CompletionException before reading the status" in {
      val wrapped = new CompletionException(ElasticError("boom", statusCode = Some(404)))
      api.statusOf(wrapped) shouldBe Some(404)
    }

    "unwrap nested plumbing wrappers" in {
      val wrapped = new CompletionException(
        new java.util.concurrent.ExecutionException(ElasticError("boom", statusCode = Some(403)))
      )
      api.statusOf(wrapped) shouldBe Some(403)
    }

    "return None for a throwable that carries no status" in {
      api.statusOf(new java.net.ConnectException("Connection refused")) shouldBe None
    }

    "not loop on a self-referential cause" in {
      // `initCause` is NOT usable here: every CompletionException constructor sets the cause
      // (to null if none is given), and Throwable.initCause then throws IllegalStateException.
      // Overriding getCause is the only way to build a genuinely self-causing throwable.
      class SelfCause extends CompletionException("self", null) {
        override def getCause: Throwable = this
      }
      api.statusOf(new SelfCause) shouldBe None
    }

    "terminate on a two-object cause cycle instead of recursing" in {
      // The self-cause guard alone does NOT cover this shape: a.getCause == b, b.getCause == a.
      // Without the depth cap in `unwrapThrowable` this recurses to StackOverflowError, which is a
      // VirtualMachineError — neither NonFatal nor statusOrServerError catches it, so the caller's
      // Promise would never complete. Assert termination, not a particular value.
      class Cyclic(name: String) extends CompletionException(name, null) {
        var other: Throwable = _
        override def getCause: Throwable = other
      }
      val a = new Cyclic("a")
      val b = new Cyclic("b")
      a.other = b
      b.other = a

      api.statusOf(a) shouldBe None
      api.statusOrServerError(a) shouldBe Some(500)
    }
  }

  "statusOrServerError" should {

    "fall back to 500 when no status can be determined" in {
      api.statusOrServerError(new java.net.ConnectException("Connection refused")) shouldBe Some(
        500
      )
    }

    "never propagate an exception thrown by an extractor (AC 8)" in {
      // Models `TransportException.statusCode()` NPE-ing on a null `response`: a throw here would
      // escape the Future.onComplete callback and leave the Promise uncompleted forever.
      class Exploding extends RuntimeException("boom")
      val exploding = new TestGetApi {
        override private[client] def statusOf(t: Throwable): Option[Int] = t match {
          case _: Exploding => throw new NullPointerException("response is null")
          case other        => super.statusOf(other)
        }
      }
      exploding.statusOrServerError(new Exploding) shouldBe Some(500)
    }
  }

  "getAsync" should {

    "preserve the HTTP status of a status-bearing exception (SoftClient4ES#184)" in {
      api.asyncFailure = Some(
        ElasticError("no such index [absent]", statusCode = Some(404), index = Some("absent"))
      )

      val result = getAsyncResult()

      result.isFailure shouldBe true
      result.error.get.statusCode shouldBe Some(404)
      result.error.get.operation shouldBe Some("getAsync")
      result.error.get.index shouldBe Some("some_index")
    }

    "preserve the status through a CompletableFuture wrapper" in {
      api.asyncFailure = Some(
        new CompletionException(ElasticError("no such index [absent]", statusCode = Some(404)))
      )

      getAsyncResult().error.get.statusCode shouldBe Some(404)
    }

    "still report 500 for a genuine transport failure (AC 2)" in {
      api.asyncFailure = Some(new java.net.ConnectException("Connection refused"))

      val result = getAsyncResult()

      result.isFailure shouldBe true
      result.error.get.statusCode shouldBe Some(500)
    }

    "attach the exception as the error cause" in {
      val boom = new java.net.SocketTimeoutException("read timed out")
      api.asyncFailure = Some(boom)

      getAsyncResult().error.get.cause shouldBe Some(boom)
    }

    "leave the success path untouched" in {
      api.asyncFailure = None
      getAsyncResult() shouldBe ElasticSuccess(Some("""{"id":"1"}"""))
    }
  }
}
