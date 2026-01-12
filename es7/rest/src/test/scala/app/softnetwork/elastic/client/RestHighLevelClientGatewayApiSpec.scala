package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClientGatewayApiSpec
    extends GatewayApiIntegrationSpec
    with ElasticDockerTestKit {
  override lazy val client: GatewayApi = new RestHighLevelClientSpi().client(elasticConfig)
}
