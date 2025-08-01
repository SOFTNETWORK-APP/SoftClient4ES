package app.softnetwork.elastic.persistence.person

import akka.actor.typed.ActorSystem
import app.softnetwork.elastic.client.java.ElasticsearchClientApi
import app.softnetwork.elastic.persistence.query.{ElasticProvider, PersonToElasticProcessorStream}
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import app.softnetwork.persistence.person.query.PersonToExternalProcessorStream
import app.softnetwork.persistence.query.ExternalPersistenceProvider
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor

class ElasticsearchClientPersonHandlerSpec extends ElasticPersonTestKit {

  implicit val ec: ExecutionContextExecutor = typedSystem().executionContext

  override def externalPersistenceProvider: ExternalPersistenceProvider[Person] =
    new ElasticProvider[Person] with ElasticsearchClientApi with ManifestWrapper[Person] {
      override protected val manifestWrapper: ManifestW = ManifestW()
      override lazy val config: Config = ElasticsearchClientPersonHandlerSpec.this.elasticConfig
    }

  override def person2ExternalProcessorStream: ActorSystem[_] => PersonToExternalProcessorStream =
    sys =>
      new PersonToElasticProcessorStream with ElasticsearchClientApi {
        override val forTests: Boolean = true
        override protected val manifestWrapper: ManifestW = ManifestW()
        override implicit def system: ActorSystem[_] = sys
        override def log: Logger = LoggerFactory getLogger getClass.getName
        override lazy val config: Config = ElasticsearchClientPersonHandlerSpec.this.elasticConfig
      }

  override def log: Logger = LoggerFactory getLogger getClass.getName
}
