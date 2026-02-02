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

import app.softnetwork.elastic.client.ElasticsearchVersion
import org.scalatest.Suite
import org.testcontainers.containers.BindMode

import java.nio.charset.StandardCharsets
import java.nio.file.StandardOpenOption
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

import java.nio.file.Files
import java.time.Duration

/** Created by smanciot on 28/06/2018.
  */
trait ElasticDockerTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://${elasticContainer.getHttpHostAddress}"

  lazy val localExecution: Boolean = sys.props.get("LOCAL_EXECUTION") match {
    case Some("true") => true
    case _            => false
  }

  lazy val xpackWatcherEnabled: Boolean = true

  lazy val xpackSecurityEnabled: Boolean = false

  lazy val xpackMLEnabled: Boolean = false

  lazy val xpackGraphEnabled: Boolean = false

  lazy val elasticContainer: ElasticsearchContainer = {
    val tmpDir =
      if (localExecution) {
        val tmp = Files.createTempDirectory("es-tmp")
        tmp.toFile.setWritable(true, false)
        tmp.toAbsolutePath.toString
      } else {
        "/tmp"
      }

    Console.println(s"📁 Temp directory: $tmpDir")

    val configFile = createElasticsearchYml()

    Console.println(s"⚙️ Config file: $configFile")

    val container = new ElasticsearchContainer(
      DockerImageName
        .parse("docker.elastic.co/elasticsearch/elasticsearch")
        .withTag(elasticVersion)
    )

    container
      .withEnv("ES_TMPDIR", "/usr/share/elasticsearch/tmp")
      //.withEnv("ES_JAVA_OPTS", "-Xms1024m -Xmx1024m")
      .withFileSystemBind(tmpDir, "/usr/share/elasticsearch/tmp", BindMode.READ_WRITE)
      .withFileSystemBind(
        configFile,
        "/usr/share/elasticsearch/config/elasticsearch.yml",
        BindMode.READ_ONLY
      )
      // .withCommand("bin/elasticsearch-syskeygen --silent", "bin/elasticsearch")
      .withStartupTimeout(Duration.ofMinutes(2))
  }

  private def createElasticsearchYml(): String = {
    val enrollmentLine =
      if (ElasticsearchVersion.isEs8OrHigher(elasticVersion)) {
        s"xpack.security.enrollment.enabled: $xpackSecurityEnabled\n"
      } else {
        "" // not compatible with ES versions < 8.x
      }
    val config =
      s"""# Cluster settings
        |cluster.name: docker-cluster
        |node.name: test-node
        |
        |# Network
        |network.host: 0.0.0.0
        |http.port: 9200
        |transport.port: 9300
        |
        |# Discovery
        |discovery.type: single-node
        |
        |# X-Pack License (force Trial license)
        |xpack.license.self_generated.type: trial
        |
        |# X-Pack Security (disabled for tests)
        |xpack.security.enabled: $xpackSecurityEnabled
        |${enrollmentLine}xpack.security.http.ssl.enabled: false
        |xpack.security.transport.ssl.enabled: false
        |
        |# X-Pack Features
        |xpack.ml.enabled: $xpackMLEnabled
        |xpack.graph.enabled: $xpackGraphEnabled
        |
        |# X-Pack Watcher
        |xpack.watcher.enabled: $xpackWatcherEnabled
        |xpack.watcher.encrypt_sensitive_data: false # for tests
        |# xpack.watcher.encryption_key: "your-32-char-encryption-key" # set a fixed key for tests if needed
        |
        |# Set default throttle period to 5s to allow frequent executions during tests
        |xpack.watcher.execution.default_throttle_period: 5s
        |
        |# Performance
        |bootstrap.memory_lock: false
        |""".stripMargin

    val configFile = Files.createTempFile("elasticsearch-", ".yml")
    Files.write(
      configFile,
      config.getBytes(StandardCharsets.UTF_8),
      StandardOpenOption.WRITE
    )

    Console.println(s"✅ Elasticsearch config created:\n$config")

    configFile.toAbsolutePath.toString
  }

  override def start(): Unit = elasticContainer.start()

  override def stop(): Unit = elasticContainer.stop()

}
