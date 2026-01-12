package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClientPipelineApiSpec extends PipelineApiSpec with ElasticDockerTestKit {
  override lazy val client: PipelineApi = new RestHighLevelClientSpi().client(elasticConfig)
}
