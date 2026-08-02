package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.query.{FileFormat, Json}
import org.apache.hadoop.conf.Configuration
import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/** SoftClient4ES#184 / AC 5 — `copyInto` was the ONE public method among the six touched by this
  * story that `ElasticClientDelegator` did not forward. The REPL and the JDBC driver never hold a
  * raw client (`MonitoredElasticClient` -> `MetricsElasticClient` -> `ElasticClientDelegator`), so
  * without the forward the `recover` block inside `IndicesApi.copyInto` would resolve `statusOf`
  * against the wrapper's core default and silently report `Some(500)` for every failure.
  *
  * This spec locks the forward in place. If it ever goes red, the status derivation for `COPY INTO`
  * has become inert on every wrapped client.
  */
class ElasticClientDelegatorCopyIntoSpec extends AnyWordSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("elastic-client-delegator-copy-into-test")

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  /** Records whether the delegate — i.e. the real client — actually ran the call, and with which
    * arguments. ALL five parameters are captured on purpose: an override written
    * `delegate.copyInto(source, target, doUpdate)` would forward correctly enough to keep a
    * 3-argument assertion green while silently discarding the user's `FILE_FORMAT` clause and
    * Hadoop configuration — the same silent-drop class of bug this spec exists to prevent.
    */
  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass.getName)

    var copyIntoCalls: List[(String, String, Boolean, Option[FileFormat], Option[Configuration])] =
      Nil

    override def copyInto(
      source: String,
      target: String,
      doUpdate: Boolean,
      fileFormat: Option[FileFormat],
      hadoopConf: Option[Configuration]
    )(implicit system: ActorSystem): Future[ElasticResult[DmlResult]] = {
      copyIntoCalls = copyIntoCalls :+ ((source, target, doUpdate, fileFormat, hadoopConf))
      Future.successful(ElasticSuccess(DmlResult(inserted = 1L, rejected = 0L)))
    }
  }

  "ElasticClientDelegator" should {

    "forward copyInto to the delegate so the status derivation runs on the real client (AC 5)" in {
      val recording = new RecordingClient
      val delegator = new ElasticClientDelegator {
        override val delegate: ElasticClientApi = recording
      }
      val conf = new Configuration()

      val result = Await.result(
        delegator.copyInto(
          "/tmp/source.json",
          "target_index",
          doUpdate = true,
          fileFormat = Some(Json),
          hadoopConf = Some(conf)
        ),
        Duration(10, TimeUnit.SECONDS)
      )

      recording.copyIntoCalls shouldBe List(
        ("/tmp/source.json", "target_index", true, Some(Json), Some(conf))
      )
      result shouldBe ElasticSuccess(DmlResult(inserted = 1L, rejected = 0L))
    }

    "forward statusOf to the delegate so a wrapper can never disagree with its client" in {
      // Stands in for a real client family override (JavaClientHelpers / RestHighLevelClientHelpers)
      // that recognises a library-specific exception the core default knows nothing about.
      val recording = new RecordingClient {
        override private[client] def statusOf(t: Throwable): Option[Int] = Some(418)
      }
      val delegator = new ElasticClientDelegator {
        override val delegate: ElasticClientApi = recording
      }

      // Without the delegator's forward this is the core default's None, and every core body that
      // runs on the wrapper would report an undifferentiated 500.
      delegator.statusOf(new RuntimeException("boom")) shouldBe Some(418)
      delegator.statusOrServerError(new RuntimeException("boom")) shouldBe Some(418)
    }
  }
}
