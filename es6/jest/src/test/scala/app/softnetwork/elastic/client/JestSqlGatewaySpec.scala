package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class JestSqlGatewaySpec extends SqlGatewayIntegrationSpec with EmbeddedElasticTestKit {
  override def client: SqlGateway = new JestClientSpi().client(elasticConfig)

  override def elasticVersion: String = "6.7.2"
}
