package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.scalatest.EmbeddedElasticTestKit

class JestClientPipelineApiSpec extends PipelineApiSpec with EmbeddedElasticTestKit {
  override def client: PipelineApi = new JestClientSpi().client(elasticConfig)
}
