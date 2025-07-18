package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.rest.RestHighLevelClientProvider
import app.softnetwork.elastic.model.{Binary, Sample}
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import com.typesafe.config.Config
import org.elasticsearch.client.RestHighLevelClient

object RestHighLevelProviders {

  class PersonProvider(es: Config)
      extends RestHighLevelClientProvider[Person]
      with ManifestWrapper[Person] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val restHighLevelClient: RestHighLevelClient = apply()
  }

  class SampleProvider(es: Config)
      extends RestHighLevelClientProvider[Sample]
      with ManifestWrapper[Sample] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val restHighLevelClient: RestHighLevelClient = apply()
  }

  class BinaryProvider(es: Config)
      extends RestHighLevelClientProvider[Binary]
      with ManifestWrapper[Binary] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = es

    implicit lazy val restHighLevelClient: RestHighLevelClient = apply()
  }
}
