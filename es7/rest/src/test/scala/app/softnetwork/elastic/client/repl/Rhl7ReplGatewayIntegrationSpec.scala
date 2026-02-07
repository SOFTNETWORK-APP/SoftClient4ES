package app.softnetwork.elastic.client.repl

import app.softnetwork.elastic.client.GatewayApi
import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class Rhl7ReplGatewayIntegrationSpec extends ReplGatewayIntegrationSpec with ElasticDockerTestKit {
  override def gateway: GatewayApi = new RestHighLevelClientSpi().client(elasticConfig)
}
