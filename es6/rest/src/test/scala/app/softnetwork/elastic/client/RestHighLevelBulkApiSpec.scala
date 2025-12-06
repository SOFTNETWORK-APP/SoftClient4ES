package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class RestHighLevelBulkApiSpec extends BulkApiSpec with EmbeddedElasticTestKit {
  override lazy val client: BulkApi = new RestHighLevelClientSpi().client(elasticConfig)
}
