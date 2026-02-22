package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.spi.JavaClientSpi
import app.softnetwork.elastic.scalatest.{ElasticDockerTestKit, MinioTestKit}

/** Integration test for `COPY INTO ... FROM 's3a://...'` using the ES8 Java client.
  *
  * Requires Docker to run two containers:
  *   - Elasticsearch (managed by [[ElasticDockerTestKit]])
  *   - MinIO S3-compatible store (managed by [[MinioTestKit]])
  *
  * The `hadoop-aws` JAR must be on the test classpath (declared as `% Test` in
  * `es8/java/build.sbt`) for the S3A filesystem to work at runtime.
  */
class JavaClientCopyIntoS3Spec
    extends CopyIntoS3IntegrationSpec
    with ElasticDockerTestKit
    with MinioTestKit {

  override lazy val client: GatewayApi = new JavaClientSpi().client(elasticConfig)
}
