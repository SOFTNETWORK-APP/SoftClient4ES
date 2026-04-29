package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JestClusterApiSpec extends ClusterApiSpec with ElasticDockerTestKit {
  override lazy val client: ClusterApi = new JestClientSpi().client(elasticConfig)
}
