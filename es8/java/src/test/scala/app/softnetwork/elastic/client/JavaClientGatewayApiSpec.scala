package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientGatewayApiSpec extends GatewayApiIntegrationSpec with ElasticDockerTestKit {
  override lazy val client: GatewayApi = new JavaClientSpi().client(elasticConfig)
}
