package app.softnetwork.elastic.client

import app.softnetwork.elastic.model.{Binary, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import com.typesafe.config.Config

object ElasticProviders {

  class PersonProvider(conf: Config) extends ElasticProvider[Person] with ManifestWrapper[Person] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class SampleProvider(conf: Config) extends ElasticProvider[Sample] with ManifestWrapper[Sample] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class BinaryProvider(conf: Config) extends ElasticProvider[Binary] with ManifestWrapper[Binary] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class ParentProvider(conf: Config) extends ElasticProvider[Parent] with ManifestWrapper[Parent] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }
}
