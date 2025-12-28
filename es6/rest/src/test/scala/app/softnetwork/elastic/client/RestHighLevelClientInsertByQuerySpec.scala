package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class RestHighLevelClientInsertByQuerySpec extends InsertByQuerySpec with EmbeddedElasticTestKit {
  override lazy val client: ElasticClientApi = new RestHighLevelClientSpi().client(elasticConfig)
}
