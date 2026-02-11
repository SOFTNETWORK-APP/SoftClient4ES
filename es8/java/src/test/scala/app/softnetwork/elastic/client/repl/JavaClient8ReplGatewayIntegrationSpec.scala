package app.softnetwork.elastic.client.repl

import app.softnetwork.elastic.client.GatewayApi
import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClient8ReplGatewayIntegrationSpec
    extends ReplGatewayIntegrationSpec
    with ElasticDockerTestKit {
  override def gateway: GatewayApi = new JavaClientSpi().client(elasticConfig)
}
