package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaBulkApiSpec extends BulkApiSpec with ElasticDockerTestKit {
  override lazy val client: BulkApi = new JavaClientSpi().client(elasticConfig)
}
