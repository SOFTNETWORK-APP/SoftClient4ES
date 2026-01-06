package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClientSqlGatewaySpec
    extends SqlGatewayIntegrationSpec
    with ElasticDockerTestKit {
  override lazy val client: SqlGateway = new RestHighLevelClientSpi().client(elasticConfig)
}
