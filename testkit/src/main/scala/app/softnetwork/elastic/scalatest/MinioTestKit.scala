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

import app.softnetwork.elastic.client.file.HadoopConfigurationFactory
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.Suite
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName

import java.time.Duration
import scala.util.control.NonFatal

object MinioTestKit {

  /** The S3-compatible server the COPY INTO tests run against.
    *
    * It is NOT MinIO any more: `minio/minio` was REMOVED from Docker Hub, so every
    * `*CopyIntoS3Spec` aborted at container start with `ContainerFetchException ... pull access
    * denied for minio/minio`. `adobe/s3mock` is the replacement; the trait keeps its historical
    * name so the four concrete specs and `CopyIntoS3IntegrationSpec` are untouched (renaming it to
    * `S3TestKit` is a follow-up).
    */
  val S3MockImage: String = "adobe/s3mock:5.2.2"

  /** Port s3mock serves HTTP on.
    *
    * The image declares NO `EXPOSE` (it is a buildpacks image: `Config.ExposedPorts` is null), so
    * `withExposedPorts` is REQUIRED — Testcontainers has nothing to infer a mapping from.
    */
  val S3MockPort: Int = 9090

  /** Env var s3mock reads the comma-separated list of buckets to pre-create from.
    *
    * Adobe RENAMED this property between the 4.x and 5.x lines. Passing the OLD name is SILENT: the
    * container starts, answers `200` on `/`, and `ListBuckets` returns an empty list — every upload
    * then fails deep inside S3A with a confusing 404. That is why `start()` asserts the bucket
    * really exists and names this constant when it does not.
    */
  val InitialBucketsEnv: String = "COM_ADOBE_TESTING_S3MOCK_STORE_INITIAL_BUCKETS"

  /** s3mock does not verify credentials at all — any non-empty key/secret pair is accepted. These
    * exist only because S3A (and `HadoopConfigurationFactory.s3aConf()`) require a non-empty pair
    * to select the static-credentials provider instead of the default AWS chain.
    */
  val AccessKey: String = "s3mock-access-key"
  val SecretKey: String = "s3mock-secret-key"

  /** A `GenericContainer` subclass rather than a raw `new GenericContainer(...)`:
    * `GenericContainer` is F-bounded (`SELF extends GenericContainer<SELF>`), which Scala cannot
    * express inline.
    *
    * The `s3mock-testcontainers` MODULE is deliberately NOT used: 5.2.2 is Java-17 bytecode (class
    * file major 61) built against Testcontainers 1.21.3, while this build's default JDK is 11 and
    * it pins Testcontainers 2.x.
    */
  final class S3MockContainer(image: DockerImageName)
      extends GenericContainer[S3MockContainer](image)
}

/** A test kit trait that starts an S3-compatible container (`adobe/s3mock`) and provides helper
  * methods for uploading files to it via the Hadoop S3A filesystem.
  *
  * Mix this trait into a concrete test class together with [[ElasticDockerTestKit]].
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

  import MinioTestKit._

  lazy val minioBucket: String = "copy-into-test"

  lazy val minioContainer: S3MockContainer =
    new S3MockContainer(DockerImageName.parse(S3MockImage))
      .withExposedPorts(Integer.valueOf(S3MockPort))
      .withEnv(InitialBucketsEnv, minioBucket)
      .waitingFor(Wait.forHttp("/").forStatusCode(200))
      .withStartupTimeout(Duration.ofMinutes(2))

  def minioEndpoint: String =
    s"http://${minioContainer.getHost}:${minioContainer.getMappedPort(S3MockPort)}"

  def minioAccessKey: String = AccessKey
  def minioSecretKey: String = SecretKey

  /** Returns a Hadoop [[Configuration]] pointing at the in-process S3 container.
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

  /** Fails loudly if [[minioBucket]] was not pre-created by the container.
    *
    * Bucket seeding is SILENTLY fallible (see [[MinioTestKit.InitialBucketsEnv]]): a wrong env-var
    * name leaves a perfectly healthy container serving an empty bucket list, and the first symptom
    * would be a failure deep inside an upload, with a message naming neither the bucket nor the
    * container.
    *
    * It deliberately builds its [[Configuration]] from the PRODUCTION
    * `HadoopConfigurationFactory.forPath`, not from [[minioHadoopConf]]: that is the only thing in
    * the suite that exercises the AWS_* system properties `start()` has just set. With
    * `minioHadoopConf` (explicit credentials) a typo in one of those three property names would go
    * unnoticed - s3mock accepts any credentials, so no COPY INTO assertion can see it either. What
    * the properties must then produce is asserted as a unit test in core's `LocalPathSpec`.
    */
  private def assertBucketSeeded(): Unit = {
    val conf = HadoopConfigurationFactory.forPath(s"s3a://$minioBucket/")
    // `fs.s3a.bucket.probe = 2` makes initialize() HEAD the bucket and fail with
    // UnknownStoreException when it is absent. It is NOT redundant with the listStatus below, and
    // neither is redundant with the obvious `fs.exists(s3a://<bucket>/)` - that one is VACUOUS:
    // MEASURED against this very container with a wrong seeding env var, `exists` on the ROOT path
    // returned true (S3A answers it from a synthetic directory status without contacting the
    // store) and the run failed much later, inside an upload, with an unrelated-looking message.
    conf.setInt("fs.s3a.bucket.probe", 2)
    val bucketPath = new Path(s"s3a://$minioBucket/")

    var fs: FileSystem = null
    try {
      fs = FileSystem.get(bucketPath.toUri, conf)
      fs.listStatus(bucketPath)
      ()
    } catch {
      case NonFatal(e) =>
        // Only an absent STORE points at the seeding env var. A missing `hadoop-aws` on the test
        // classpath, or Docker networking, must not be reported as "check that env var name".
        val storeAbsent =
          e.getClass.getSimpleName == "UnknownStoreException" ||
          e.isInstanceOf[java.io.FileNotFoundException]
        val diagnosis =
          if (storeAbsent)
            s"S3 bucket '$minioBucket' does not exist on $S3MockImage at $minioEndpoint. " +
            s"The container seeds buckets from the env var '$InitialBucketsEnv' - check that name: " +
            "an unknown one is ignored silently and leaves the container healthy but empty."
          else
            s"Could not reach the S3 bucket '$minioBucket' on $S3MockImage at $minioEndpoint. " +
            "Check that this module declares hadoop-aws as a Test dependency (the S3AFileSystem " +
            "class is loaded by name) and that Docker networking to the mapped port works."
        throw new IllegalStateException(diagnosis, e)
    } finally {
      // Outside the diagnosed region on purpose: a throw from close() here would REPLACE the real
      // "bucket absent" exception with a teardown failure.
      if (fs != null) fs.close()
    }
  }

  /** Starts the S3 container, verifies the test bucket exists, and then starts Elasticsearch. */
  abstract override def start(): Unit = {
    minioContainer.start()

    // Expose the S3 credentials as JVM system properties so that
    // HadoopConfigurationFactory.s3aConf() picks them up via envOrProp().
    // (core's LocalPathSpec, under the subject "HadoopConfigurationFactory.s3aConf", is what
    // guards that contract as a unit test — s3mock itself accepts ANY credentials, so a COPY INTO
    // test passing here proves nothing about it.)
    System.setProperty("AWS_ACCESS_KEY_ID", minioAccessKey)
    System.setProperty("AWS_SECRET_ACCESS_KEY", minioSecretKey)
    System.setProperty("AWS_ENDPOINT_URL", minioEndpoint)

    // Defensive, and the reason is NOT "afterAll does not run": ScalaTest 3.2.19 DOES run it after
    // a failed beforeAll. It is that `ElasticTestKit.afterAll` closes `restClient` BEFORE calling
    // `stop()`, and with Elasticsearch never started that throws first and ScalaTest swallows it -
    // so `stop()` is never reached and Ryuk would reap this container minutes later.
    try assertBucketSeeded()
    catch {
      case NonFatal(e) =>
        clearS3SystemProperties()
        minioContainer.stop()
        throw e
    }

    super.start()
  }

  /** Stops Elasticsearch first, then tears down the S3 container and clears system properties.
    */
  abstract override def stop(): Unit = {
    super.stop()
    clearS3SystemProperties()
    if (minioContainer.isRunning) minioContainer.stop()
  }

  private def clearS3SystemProperties(): Unit = {
    System.clearProperty("AWS_ACCESS_KEY_ID")
    System.clearProperty("AWS_SECRET_ACCESS_KEY")
    System.clearProperty("AWS_ENDPOINT_URL")
  }
}
