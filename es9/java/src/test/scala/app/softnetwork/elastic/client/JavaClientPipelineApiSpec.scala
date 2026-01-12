package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientPipelineApiSpec extends PipelineApiSpec with ElasticDockerTestKit {
  override lazy val client: PipelineApi = new JavaClientSpi().client(elasticConfig)
}
