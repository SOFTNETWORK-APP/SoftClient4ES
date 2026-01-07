package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JestGatewayApiSpec extends GatewayApiIntegrationSpec with ElasticDockerTestKit {
  override def client: GatewayApi = new JestClientSpi().client(elasticConfig)

  override def elasticVersion: String = "6.7.2"
}
