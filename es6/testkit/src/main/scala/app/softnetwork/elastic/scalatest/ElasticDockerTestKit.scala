package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import org.testcontainers.containers.BindMode
//import org.testcontainers.containers.wait.strategy.Wait
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

  lazy val elasticContainer: ElasticsearchContainer = {
    val tmpDir =
      if (localExecution) {
        val tmp = Files.createTempDirectory("es-tmp")
        tmp.toFile.setWritable(true, false)
        tmp.toAbsolutePath.toString
      } else {
        "/tmp"
      }
    Console.println(s"Using temporary directory for Elasticsearch: $tmpDir")
    val container = new ElasticsearchContainer(
      DockerImageName
        .parse("docker.elastic.co/elasticsearch/elasticsearch")
        .withTag(elasticVersion)
    )
    container.addEnv("ES_TMPDIR", "/usr/share/elasticsearch/tmp")
    container.addEnv("discovery.type", "single-node")
    container.addEnv("xpack.security.enabled", "false")
    container.addEnv("xpack.ml.enabled", "false")
    container.addEnv("xpack.watcher.enabled", "false")
    container.addEnv("xpack.graph.enabled", "false")
    container.addFileSystemBind(
      tmpDir,
      "/usr/share/elasticsearch/tmp",
      BindMode.READ_WRITE
    )
    // container.addEnv("ES_JAVA_OPTS", "-Xms1024m -Xmx1024m")
    // container.setWaitStrategy(Wait.forHttp("/").forStatusCode(200))
    container.withStartupTimeout(Duration.ofMinutes(2))
  }

  override def start(): Unit = elasticContainer.start()

  override def stop(): Unit = elasticContainer.stop()

}
