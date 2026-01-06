package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientSqlGatewaySpec extends SqlGatewayIntegrationSpec with ElasticDockerTestKit {
  override lazy val client: SqlGateway = new JavaClientSpi().client(elasticConfig)
}
