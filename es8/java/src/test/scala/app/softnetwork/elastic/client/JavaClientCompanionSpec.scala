package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.java.JavaClientCompanion
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import com.typesafe.config.ConfigFactory
import configs.ConfigReader
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.util.Try

class JavaClientCompanionSpec
    extends AnyWordSpec
    with ElasticDockerTestKit
    with Matchers
    with ScalaFutures {

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  "RestHighLevelClientCompanion" should {

    "initialize client lazily" in {
      val companion = TestCompanion()
      companion.isInitialized shouldBe false

      val client = companion.apply()
      client should not be null
      companion.isInitialized shouldBe true
    }

    "return same instance on multiple calls" in {
      val companion = TestCompanion()
      val client1 = companion.apply()
      val client2 = companion.apply()

      client1 should be theSameInstanceAs client2
    }

    "be thread-safe during initialization" in {
      val companion = TestCompanion()
      val futures = (1 to 100).map { _ =>
        Future {
          companion.apply()
        }
      }

      val clients = Future.sequence(futures).futureValue

      // Tous les clients doivent être la même instance
      clients.distinct.size shouldBe 1
    }

    "close client properly" in {
      val companion = TestCompanion()
      companion.apply()
      companion.isInitialized shouldBe true

      companion.close()
      companion.isInitialized shouldBe false
    }

    "handle invalid URL gracefully" in {
      val companion = TestCompanion("invalid-url")

      Try(an[IllegalArgumentException] should be thrownBy {
        companion.apply()
      })
    }

    "test connection successfully" in {
      val companion = TestCompanion()
      companion.testConnection() shouldBe true
    }
  }

  case class TestCompanion(config: ElasticConfig) extends JavaClientCompanion {
    override def elasticConfig: ElasticConfig = config
  }

  object TestCompanion {
    def apply(): TestCompanion = TestCompanion(
      ConfigReader[ElasticConfig]
        .read(elasticConfig.withFallback(ConfigFactory.load("softnetwork-elastic.conf")), "elastic")
        .toEither match {
        case Left(configError) =>
          throw configError.configException
        case Right(r) => r
      }
    )

    def apply(url: String): TestCompanion = TestCompanion(
      ConfigReader[ElasticConfig]
        .read(
          ConfigFactory
            .parseString(elasticConfigAsString)
            .withFallback(ConfigFactory.load("softnetwork-elastic.conf")),
          "elastic"
        )
        .toEither match {
        case Left(configError) =>
          throw configError.configException
        case Right(r) => r.copy(credentials = ElasticCredentials(url))
      }
    )
  }
}
