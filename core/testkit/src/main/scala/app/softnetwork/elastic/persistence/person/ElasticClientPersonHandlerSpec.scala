package app.softnetwork.elastic.persistence.person

import akka.actor.typed.ActorSystem
import app.softnetwork.elastic.persistence.query.{ElasticProvider, PersonToElasticProcessorStream}
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import app.softnetwork.persistence.person.query.PersonToExternalProcessorStream
import app.softnetwork.persistence.query.ExternalPersistenceProvider
import com.typesafe.config.Config
import org.slf4j.{Logger, LoggerFactory}

trait ElasticClientPersonHandlerSpec extends ElasticPersonTestKit {

  override def externalPersistenceProvider: ExternalPersistenceProvider[Person] =
    new ElasticProvider[Person] with ManifestWrapper[Person] {
      override protected val manifestWrapper: ManifestW = ManifestW()
      override lazy val config: Config = ElasticClientPersonHandlerSpec.this.elasticConfig
    }

  override def person2ExternalProcessorStream: ActorSystem[_] => PersonToExternalProcessorStream =
    sys =>
      new PersonToElasticProcessorStream {
        override val forTests: Boolean = true
        override protected val manifestWrapper: ManifestW = ManifestW()
        override implicit def system: ActorSystem[_] = sys
        override def log: Logger = LoggerFactory getLogger getClass.getName
        override lazy val config: Config = ElasticClientPersonHandlerSpec.this.elasticConfig
      }

  override def log: Logger = LoggerFactory getLogger getClass.getName
}
