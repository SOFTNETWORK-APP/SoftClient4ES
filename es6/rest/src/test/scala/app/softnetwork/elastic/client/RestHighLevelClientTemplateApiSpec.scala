package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.RestHighLevelClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class RestHighLevelClientTemplateApiSpec extends TemplateApiSpec with ElasticDockerTestKit {
  override lazy val client: TemplateApi with VersionApi =
    new RestHighLevelClientSpi().client(elasticConfig)
}
