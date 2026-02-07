package app.softnetwork.elastic.client.repl

import app.softnetwork.elastic.client.GatewayApi
import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JestReplGatewayIntegrationSpec extends ReplGatewayIntegrationSpec with ElasticDockerTestKit {

  override def gateway: GatewayApi = new JestClientSpi().client(elasticConfig)

  override def elasticVersion: String = "6.7.2"
}
