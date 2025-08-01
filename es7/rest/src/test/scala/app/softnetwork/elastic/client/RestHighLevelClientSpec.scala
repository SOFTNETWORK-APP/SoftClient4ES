package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.RestHighLevelProviders.{
  BinaryProvider,
  ParentProvider,
  PersonProvider,
  SampleProvider
}
import app.softnetwork.elastic.model.{Binary, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.persistence.person.model.Person

class RestHighLevelClientSpec extends ElasticClientSpec {

  lazy val pClient: ElasticProvider[Person] with ElasticClientApi = new PersonProvider(
    elasticConfig
  )
  lazy val sClient: ElasticProvider[Sample] with ElasticClientApi = new SampleProvider(
    elasticConfig
  )
  lazy val bClient: ElasticProvider[Binary] with ElasticClientApi = new BinaryProvider(
    elasticConfig
  )

  override def parentClient: ElasticProvider[Parent] with ElasticClientApi = new ParentProvider(
    elasticConfig
  )
}
