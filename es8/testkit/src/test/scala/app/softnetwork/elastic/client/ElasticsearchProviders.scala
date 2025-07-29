package app.softnetwork.elastic.client

import app.softnetwork.elastic.model.{Binary, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticsearchClientProvider
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import co.elastic.clients.elasticsearch.ElasticsearchClient
import com.typesafe.config.Config

object ElasticsearchProviders {

  class PersonProvider(es: Config)
      extends ElasticsearchClientProvider[Person]
      with ManifestWrapper[Person] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val elasticsearchClient: ElasticsearchClient = apply()
  }

  class SampleProvider(es: Config)
      extends ElasticsearchClientProvider[Sample]
      with ManifestWrapper[Sample] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val elasticsearchClient: ElasticsearchClient = apply()
  }

  class BinaryProvider(es: Config)
      extends ElasticsearchClientProvider[Binary]
      with ManifestWrapper[Binary] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val elasticsearchClient: ElasticsearchClient = apply()
  }

  class ParentProvider(es: Config)
      extends ElasticsearchClientProvider[Parent]
      with ManifestWrapper[Parent] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val elasticsearchClient: ElasticsearchClient = apply()
  }
}
