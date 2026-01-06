package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelBulkApiSpec extends BulkApiSpec with ElasticDockerTestKit {
  override lazy val client: BulkApi = new RestHighLevelClientSpi().client(elasticConfig)
}
