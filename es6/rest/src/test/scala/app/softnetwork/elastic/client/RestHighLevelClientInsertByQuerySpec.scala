package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClientInsertByQuerySpec extends InsertByQuerySpec with ElasticDockerTestKit {
  override lazy val client: ElasticClientApi = new RestHighLevelClientSpi().client(elasticConfig)
}
