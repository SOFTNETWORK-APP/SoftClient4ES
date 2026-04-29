package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClusterApiSpec extends ClusterApiSpec with ElasticDockerTestKit {
  override lazy val client: ClusterApi = new RestHighLevelClientSpi().client(elasticConfig)
}
