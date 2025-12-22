package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class JestClientTemplateApiSpec extends TemplateApiSpec with EmbeddedElasticTestKit {
  override def client: TemplateApi with VersionApi = new JestClientSpi().client(elasticConfig)

  override def elasticVersion: String = "6.7.2"
}
