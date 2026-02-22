/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.scalatest

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Suite
import org.testcontainers.containers.MinIOContainer

/** A test kit trait that starts a MinIO container and provides helper methods for uploading files
  * to MinIO via the Hadoop S3A filesystem.
  *
  * Uses the official Testcontainers MinIO module (`testcontainers-minio`). Mix this trait into a
  * concrete test class together with [[ElasticDockerTestKit]].
  *
  * The concrete module must declare `hadoop-aws` as a `% Test` dependency so that the
  * `S3AFileSystem` implementation is available on the test classpath at runtime.
  *
  * @example
  * {{{
  *   class MySpec
  *     extends CopyIntoS3IntegrationSpec
  *     with ElasticDockerTestKit
  *     with MinioTestKit {
  *     override lazy val client: GatewayApi = new JavaClientSpi().client(elasticConfig)
  *   }
  * }}}
  */
trait MinioTestKit extends ElasticTestKit { _: Suite =>

  lazy val minioBucket: String = "copy-into-test"

  lazy val minioContainer: MinIOContainer =
    new MinIOContainer("minio/minio:latest")

  def minioEndpoint: String = minioContainer.getS3URL
  def minioAccessKey: String = minioContainer.getUserName
  def minioSecretKey: String = minioContainer.getPassword

  /** Returns a Hadoop [[Configuration]] pointing at the in-process MinIO container.
    *
    * The S3AFileSystem implementation (`hadoop-aws`) must be on the classpath at runtime.
    */
  def minioHadoopConf(): Configuration = {
    val conf = new Configuration()
    conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    conf.set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")
    conf.set("fs.s3a.access.key", minioAccessKey)
    conf.set("fs.s3a.secret.key", minioSecretKey)
    conf.set("fs.s3a.endpoint", minioEndpoint)
    conf.setBoolean("fs.s3a.path.style.access", true)
    conf.set("fs.s3a.connection.ssl.enabled", "false")
    conf.set("fs.s3a.attempts.maximum", "3")
    conf.set(
      "fs.s3a.aws.credentials.provider",
      "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider"
    )
    conf
  }

  /** Uploads UTF-8 `content` as object `objectKey` inside [[minioBucket]]. */
  def uploadToMinio(content: String, objectKey: String): Unit = {
    val conf = minioHadoopConf()
    val objectPath = new Path(s"s3a://$minioBucket/$objectKey")
    val fs = FileSystem.get(objectPath.toUri, conf)
    val out = fs.create(objectPath, /* overwrite = */ true)
    try out.write(content.getBytes("UTF-8"))
    finally {
      out.close()
      fs.close()
    }
  }

  /** Starts the MinIO container, creates the test bucket, and then starts Elasticsearch. */
  abstract override def start(): Unit = {
    minioContainer.start()

    // Expose MinIO credentials as JVM system properties so that
    // HadoopConfigurationFactory.s3aConf() picks them up via envOrProp().
    System.setProperty("AWS_ACCESS_KEY_ID", minioAccessKey)
    System.setProperty("AWS_SECRET_ACCESS_KEY", minioSecretKey)
    System.setProperty("AWS_ENDPOINT_URL", minioEndpoint)

    // Create the test bucket using the mc CLI bundled in the minio/minio image.
    // mc connects to localhost:9000 (the internal port) from within the container.
    minioContainer.execInContainer(
      "mc",
      "alias",
      "set",
      "myminio",
      "http://localhost:9000",
      minioContainer.getUserName,
      minioContainer.getPassword
    )
    minioContainer.execInContainer("mc", "mb", "--ignore-existing", s"myminio/$minioBucket")

    super.start()
  }

  /** Stops Elasticsearch first, then tears down the MinIO container and clears system properties.
    */
  abstract override def stop(): Unit = {
    super.stop()
    System.clearProperty("AWS_ACCESS_KEY_ID")
    System.clearProperty("AWS_SECRET_ACCESS_KEY")
    System.clearProperty("AWS_ENDPOINT_URL")
    if (minioContainer.isRunning) minioContainer.stop()
  }
}
