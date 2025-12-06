package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class JestBulkApiSpec extends BulkApiSpec with EmbeddedElasticTestKit {
  override lazy val client: BulkApi = new JestClientSpi().client(elasticConfig)
}
