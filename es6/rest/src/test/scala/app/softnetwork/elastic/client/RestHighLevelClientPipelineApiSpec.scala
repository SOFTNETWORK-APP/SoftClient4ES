package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class RestHighLevelClientPipelineApiSpec extends PipelineApiSpec with EmbeddedElasticTestKit {
  override lazy val client: PipelineApi = new RestHighLevelClientSpi().client(elasticConfig)
}
