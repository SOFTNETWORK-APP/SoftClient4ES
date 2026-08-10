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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.jest.JestClientHelpers
import app.softnetwork.elastic.client.result.ElasticSuccess
import app.softnetwork.elastic.client.spi.JestClientSpi
import app.softnetwork.elastic.sql.watcher.Watcher
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}

import java.net.{InetSocketAddress, ServerSocket}
import scala.util.Failure

/** SoftClient4ES#204 — `JestLicenseApi` called `apply().execute(...)` directly instead of routing
  * through the module's Try-wrapping `executeJestAction`, so a network fault during `GET _license`
  * **threw** (`IOException` / `CouldNotConnectException`) rather than returning an
  * `ElasticFailure`. `LicenseApi` documents no exception contract and consumers — including core's
  * own `enableBasicLicense`, which chains `licenseInfo` first — treat `ElasticResult` as total.
  *
  * No Docker: the point is precisely that nothing is listening. The port is obtained by opening a
  * `ServerSocket` on an ephemeral port and closing it, so the number is real and free rather than a
  * guess that some other process might occupy.
  */
class JestLicenseApiFaultSpec extends AnyFlatSpec with Matchers {

  private def closedPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private def clientOnDeadEndpoint: ElasticClientApi = {
    val conf: Config = ConfigFactory.parseString(
      s"""elastic {
         |  credentials { scheme = "http", host = "127.0.0.1", port = $closedPort }
         |  connection-timeout = 250 ms
         |  socket-timeout = 250 ms
         |}""".stripMargin
    )
    new JestClientSpi().client(conf)
  }

  "licenseInfo against an unreachable cluster" should "return a failure, not throw" in {
    val result = clientOnDeadEndpoint.licenseInfo
    result.isFailure shouldBe true
    result.error.map(_.operation) shouldBe Some(Some("licenseInfo"))
  }

  "enableBasicLicense against an unreachable cluster" should "return a failure, not throw" in {
    val result = clientOnDeadEndpoint.enableBasicLicense()
    result.isFailure shouldBe true
  }

  "enableTrialLicense against an unreachable cluster" should "return a failure, not throw" in {
    val result = clientOnDeadEndpoint.enableTrialLicense()
    result.isFailure shouldBe true
  }

  // ── SoftClient4ES#215 — the same defect in the Pipeline, Template and Watcher APIs ──
  //
  // #204 called `executeLicenseInfo` "the odd man out" and said every other jest API already
  // routed through the wrapper. It did not: twelve more sites called `apply().execute(...)`
  // directly. `JestWatcherApi` is the one that stings — extensions calls it on the
  // materialized-view deployment path, which is the scenario #204 was found under.

  "createPipeline against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.createPipeline("p", "{}").isFailure shouldBe true
  }

  "deletePipeline against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.deletePipeline("p", ifExists = false).isFailure shouldBe true
  }

  "getPipeline against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.getPipeline("p").isFailure shouldBe true
  }

  "pipelines against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.pipelines().isFailure shouldBe true
  }

  "createTemplate against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.createTemplate("t", "{}").isFailure shouldBe true
  }

  "getTemplate against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.getTemplate("t").isFailure shouldBe true
  }

  "listTemplates against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.listTemplates().isFailure shouldBe true
  }

  /** `templateExists` is the one method deliberately left off `executeJestAction`: it must keep
    * answering "no" with a *success* carrying `false` when Elasticsearch 404s, or `DROP … IF
    * EXISTS` would fail on the case it exists to tolerate. A transport fault is still a failure —
    * that distinction is the whole point.
    */
  "templateExists against an unreachable cluster" should "return a failure, not throw" in {
    // The public entry point probes the cluster version first to choose composable vs legacy, so
    // the failure legitimately surfaces from there; `executeLegacyTemplateExists` below pins the
    // converted method itself.
    clientOnDeadEndpoint.templateExists("t").isFailure shouldBe true
  }

  "executeLegacyTemplateExists against an unreachable cluster" should "return a failure, not throw" in {
    val result = clientOnDeadEndpoint.executeLegacyTemplateExists("t")
    result.isFailure shouldBe true
    result.error.map(_.operation) shouldBe Some(Some("legacyTemplateExists"))
  }

  "deleteTemplate with ifExists against an unreachable cluster" should "return a failure, not throw" in {
    // Goes through templateExists first — a transport fault there must surface, not be read as
    // "the template is absent, nothing to do".
    clientOnDeadEndpoint.deleteTemplate("t", ifExists = true).isFailure shouldBe true
  }

  "deleteWatcher against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.deleteWatcher("w").isFailure shouldBe true
  }

  "getWatcherStatus against an unreachable cluster" should "return a failure, not throw" in {
    clientOnDeadEndpoint.getWatcherStatus("w").isFailure shouldBe true
  }

  "createWatcher against an unreachable cluster" should "return a failure, not throw" in {
    val result = clientOnDeadEndpoint.createWatcher(Watcher(id = "w"))
    result.isFailure shouldBe true
    result.error.map(_.operation) shouldBe Some(Some("createWatcher"))
  }

  /** `templateExists` answers a question, so it must distinguish "no" from "I could not ask". A 404
    * is an answer and stays `ElasticSuccess(false)` — `DROP … IF EXISTS` propagates any failure, so
    * making the 404 a failure would break the case that clause exists to tolerate. Any other status
    * is *not* an answer: reporting a 503 as "absent" made `DROP … IF EXISTS` claim success while
    * the template was still there.
    *
    * Served by an in-JVM `HttpServer` rather than Docker — the point is the status code, not
    * Elasticsearch.
    */
  private def withStubCluster[T](status: Int)(f: ElasticClientApi => T): T = {
    val server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0)
    server.createContext(
      "/",
      new HttpHandler {
        override def handle(exchange: HttpExchange): Unit = {
          exchange.sendResponseHeaders(status, -1L)
          exchange.close()
        }
      }
    )
    server.start()
    try {
      val conf = ConfigFactory.parseString(
        s"""elastic {
           |  credentials { scheme = "http", host = "127.0.0.1", port = ${server.getAddress.getPort} }
           |  connection-timeout = 2 s
           |  socket-timeout = 2 s
           |}""".stripMargin
      )
      f(new JestClientSpi().client(conf))
    } finally server.stop(0)
  }

  "executeLegacyTemplateExists" should "report a 404 as an absent template, not a failure" in {
    withStubCluster(404) { client =>
      client.executeLegacyTemplateExists("t") shouldBe ElasticSuccess(false)
    }
  }

  it should "report a 200 as a present template" in {
    withStubCluster(200) { client =>
      client.executeLegacyTemplateExists("t") shouldBe ElasticSuccess(true)
    }
  }

  it should "report a 503 as a failure rather than as an absent template" in {
    withStubCluster(503) { client =>
      val result = client.executeLegacyTemplateExists("t")
      result.isFailure shouldBe true
      result.error.flatMap(_.statusCode) shouldBe Some(503)
    }
  }

  /** The corollary: `DROP … IF EXISTS` must not swallow an outage as "nothing to drop". */
  "executeDeleteLegacyTemplate with ifExists" should "fail on a 503 rather than report success" in {
    withStubCluster(503) { client =>
      client.executeDeleteLegacyTemplate("t", ifExists = true).isFailure shouldBe true
    }
  }

  it should "report no-op on a 404" in {
    withStubCluster(404) { client =>
      client.executeDeleteLegacyTemplate("t", ifExists = true) shouldBe ElasticSuccess(false)
    }
  }

  /** The absent-resource contract the wrapper conversion had to preserve: these actions mark 404 as
    * *succeeded* with no body, so a missing resource is `None` / empty, never a failure.
    */
  "getPipeline on a 404" should "yield None rather than a failure" in {
    withStubCluster(404) { client =>
      client.executeGetPipeline("p") shouldBe ElasticSuccess(None)
    }
  }

  "getWatcherStatus on a 404" should "yield None rather than a failure" in {
    withStubCluster(404) { client =>
      client.executeGetWatcherStatus("w") shouldBe ElasticSuccess(None)
    }
  }

  /** The connection-refused cases above are `NonFatal`, so they would pass with a plain `Try` and
    * prove nothing about the half of the fix the issue actually spelled out: `NonFatal` does NOT
    * cover `LinkageError`, yet `NoClassDefFoundError` is an observed failure mode in the es6
    * closure (SoftClient4ES#168). This exercises `tryAction` directly.
    */
  "tryAction" should "convert a LinkageError into a Failure rather than let it escape" in {
    val helper = new JestClientSpi().client(ConfigFactory.empty()).asInstanceOf[JestClientHelpers]
    val error = new NoClassDefFoundError("org/apache/logging/log4j/LogManager")
    helper.tryAction[Int](throw error) shouldBe Failure(error)
  }

  it should "still let a VirtualMachineError through — the JVM is already lost" in {
    val helper = new JestClientSpi().client(ConfigFactory.empty()).asInstanceOf[JestClientHelpers]
    an[OutOfMemoryError] should be thrownBy helper.tryAction[Int](
      throw new OutOfMemoryError("boom")
    )
  }
}
