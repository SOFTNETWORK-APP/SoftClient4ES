package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientInsertByQuerySpec extends InsertByQuerySpec with ElasticDockerTestKit {
  override lazy val client: ElasticClientApi = new JavaClientSpi().client(elasticConfig)
}
