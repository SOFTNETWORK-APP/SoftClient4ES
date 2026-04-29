package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientClusterApiSpec extends ClusterApiSpec with ElasticDockerTestKit {
  override lazy val client: ClusterApi = new JavaClientSpi().client(elasticConfig)
}
