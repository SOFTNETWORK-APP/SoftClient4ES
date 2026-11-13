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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}
import org.testcontainers.elasticsearch.ElasticsearchContainer
import org.testcontainers.utility.DockerImageName

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.charset.StandardCharsets

/** `ElasticDockerTestKit`'s lifecycle without Docker: an overridden `elasticContainer` (the
  * downstream basic-licence pattern) keeps today's lifecycle whatever the shared-container
  * property, the default container is unshared without the property, the opt-out survives
  * linearization in both mixin orders, and the rendered `elasticsearch.yml` is byte-identical to
  * the one main wrote for the default knobs.
  *
  * The suites under test are anonymous (never discovered). No test reads a default
  * `elasticContainer` with the property on: that would boot a container.
  */
class ElasticDockerTestKitLifecycleSpec extends AnyFlatSpec with Matchers {

  /** A container whose `start` and `stop` only count (no Docker; `getContainerId` stays null). */
  private final class StubContainer
      extends ElasticsearchContainer(
        DockerImageName.parse("docker.elastic.co/elasticsearch/elasticsearch").withTag("8.18.3")
      ) {
    @volatile var starts = 0
    @volatile var stops = 0
    override def start(): Unit = starts += 1
    override def stop(): Unit = stops += 1
  }

  /** Runs `body` with the property set to `value` (absent for `None`), then restores it.
    *
    * The lock is the property name itself: a string literal is ONE JVM-wide instance, so the copies
    * of this spec that `copyTestkit` puts in every `softclient4es{N}-core-testkit` (run in the same
    * sbt JVM, possibly concurrently) never see each other's value.
    */
  private def withProperty[T](value: Option[String])(body: => T): T =
    SharedElasticContainers.Property.synchronized {
      val saved = sys.props.get(SharedElasticContainers.Property)
      value match {
        case Some(v) => System.setProperty(SharedElasticContainers.Property, v)
        case None    => System.clearProperty(SharedElasticContainers.Property)
      }
      try body
      finally saved match {
        case Some(v) => System.setProperty(SharedElasticContainers.Property, v)
        case None    => System.clearProperty(SharedElasticContainers.Property)
      }
    }

  /** `body`'s result and what it printed (kept off the build log: the CI boot counters must count
    * real containers only).
    */
  private def captured[T](body: => T): (T, String) = {
    val buffer = new ByteArrayOutputStream()
    val out = new PrintStream(buffer, true, StandardCharsets.UTF_8.name())
    val result = Console.withOut(out)(body)
    out.flush()
    (result, buffer.toString(StandardCharsets.UTF_8.name()))
  }

  private val probeLog: Logger = LoggerFactory.getLogger(classOf[ElasticDockerTestKitLifecycleSpec])

  private val UnsharedLine =
    """\[es-container\] boot=\d+\.\d\ds licence=- key=8\.18\.3/[0-9a-f]{8} shared=false module=.+ suite=LifecycleProbe id=- live=\d+"""

  Seq(None, Some("true")).foreach { property =>
    "An overridden elasticContainer" should s"be started and stopped once each, never registered (property ${property
      .getOrElse("absent")})" in {
      withProperty(property) {
        val stub = new StubContainer
        val suite = new AnyFlatSpec with ElasticDockerTestKit {
          lazy val log: Logger = probeLog
          override def suiteName: String = "LifecycleProbe"
          override def elasticVersion: String = "8.18.3" // the key in the line, whatever the copy
          override lazy val elasticContainer: ElasticsearchContainer = stub
        }
        SharedElasticContainers.enabled shouldBe property.isDefined
        val (_, started) = captured(suite.start())
        (stub.starts, stub.stops) shouldBe ((1, 0))
        SharedElasticContainers.isShared(stub) shouldBe false
        val (_, stopped) = captured(suite.stop())
        (stub.starts, stub.stops) shouldBe ((1, 1))
        SharedElasticContainers.isShared(stub) shouldBe false
        // one line, unshared, no licence wait, no reset, no leak report
        started.linesIterator.toList match {
          case List(line) => line should fullyMatch regex UnsharedLine
          case lines      => fail(s"expected one [es-container] line, got: $lines")
        }
        stopped shouldBe empty
      }
    }
  }

  "The default elasticContainer" should "be a container of the suite's own without the property" in {
    withProperty(None) {
      val suite = new AnyFlatSpec with ElasticDockerTestKit {
        lazy val log: Logger = probeLog
        override def elasticVersion: String = "8.18.3"
      }
      val (container, printed) = captured(suite.elasticContainer)
      SharedElasticContainers.isShared(container) shouldBe false
      container.getContainerId shouldBe null // never started
      printed should include("📁 Temp directory:")
      printed should include(LifecycleYml.es8)
    }
  }

  "The opt-out" should "win in both mixin orders" in {
    trait OptOut extends ElasticContainerSharing { self: ElasticTestKit =>
      override def sharedContainer: Boolean = false
    }
    val specFirst = new AnyFlatSpec with OptOut with ElasticDockerTestKit {
      lazy val log: Logger = probeLog
    }
    val kitFirst = new AnyFlatSpec with ElasticDockerTestKit with OptOut {
      lazy val log: Logger = probeLog
    }
    val default = new AnyFlatSpec with ElasticDockerTestKit {
      lazy val log: Logger = probeLog
    }
    (specFirst.sharedContainer, kitFirst.sharedContainer, default.sharedContainer) shouldBe
    ((false, false, true))
  }

  "The rendered elasticsearch.yml" should "be byte-identical to main's for the default knobs" in {
    def rendered(version: String): String = {
      val suite = new AnyFlatSpec with ElasticDockerTestKit {
        lazy val log: Logger = probeLog
        override def elasticVersion: String = version
      }
      suite.renderElasticsearchYml()
    }
    rendered("8.18.3") shouldBe LifecycleYml.es8
    rendered("6.8.23") shouldBe LifecycleYml.es6
    // the key is the rendered text: a knob that changes the yml changes the key
    val secure = new AnyFlatSpec with ElasticDockerTestKit {
      lazy val log: Logger = probeLog
      override def elasticVersion: String = "8.18.3"
      override lazy val xpackSecurityEnabled: Boolean = true
    }
    val open = new AnyFlatSpec with ElasticDockerTestKit {
      lazy val log: Logger = probeLog
      override def elasticVersion: String = "8.18.3"
    }
    open.containerKey shouldBe ContainerKey("8.18.3", LifecycleYml.es8)
    secure.containerKey should not be open.containerKey
    secure.resetTarget shouldBe ResetTarget("8.18.3", security = true, watcher = true)
  }
}

/** The `elasticsearch.yml` main (ec9b065a) wrote for the default knobs, as its suites printed it.
  */
private object LifecycleYml {

  private def lines(ls: String*): String = ls.mkString("", "\n", "\n")

  private def body(es8: Boolean): String = lines(
    Seq(
      "# Cluster settings",
      "cluster.name: docker-cluster",
      "node.name: test-node",
      "",
      "# Network",
      "network.host: 0.0.0.0",
      "http.port: 9200",
      "transport.port: 9300",
      "",
      "# Discovery",
      "discovery.type: single-node",
      ""
    ) ++
    (if (es8)
       Seq(
         "# Deprecation indexing (the suite must not create the index it asserts is absent)",
         "cluster.deprecation_indexing.enabled: false",
         ""
       )
     else Nil) ++
    Seq(
      "# X-Pack License (forced; see xpackLicenseType)",
      "xpack.license.self_generated.type: trial",
      "",
      "# X-Pack Security (disabled for tests)",
      "xpack.security.enabled: false"
    ) ++
    (if (es8) Seq("xpack.security.enrollment.enabled: false") else Nil) ++
    Seq(
      "xpack.security.http.ssl.enabled: false",
      "xpack.security.transport.ssl.enabled: false",
      "",
      "# X-Pack Features",
      "xpack.ml.enabled: false",
      "xpack.graph.enabled: false",
      "",
      "# X-Pack Watcher",
      "xpack.watcher.enabled: true",
      "xpack.watcher.encrypt_sensitive_data: false # for tests",
      "# xpack.watcher.encryption_key: \"your-32-char-encryption-key\" # set a fixed key for tests if needed",
      "",
      "# Set default throttle period to 5s to allow frequent executions during tests",
      "xpack.watcher.execution.default_throttle_period: 5s",
      "",
      "# Script compilation rate limit (default 75/5m can be exhausted during tests)",
      "script.max_compilations_rate: 10000/5m",
      "",
      "# Performance",
      "bootstrap.memory_lock: false"
    ): _*
  )

  val es8: String = body(es8 = true)

  val es6: String = body(es8 = false)
}
