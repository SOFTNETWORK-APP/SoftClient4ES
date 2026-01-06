package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class RestHighLevelClientSqlGatewaySpec
    extends SqlGatewayIntegrationSpec
    with EmbeddedElasticTestKit {
  override lazy val client: SqlGateway = new RestHighLevelClientSpi().client(elasticConfig)
}
