package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit

class JavaClientTemplateApiSpec extends TemplateApiSpec with ElasticDockerTestKit {
  override lazy val client: TemplateApi with VersionApi = new JavaClientSpi().client(elasticConfig)
}
