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
import app.softnetwork.elastic.client.spi.JestClientSpi
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.ServerSocket
import scala.util.Failure

/** SoftClient4ES#204 — `JestLicenseApi` called `apply().execute(...)` directly instead of routing
  * through the module's Try-wrapping `executeJestAction`, so a network fault during `GET _license`
  * **threw** (`IOException` / `CouldNotConnectException`) rather than returning an
  * `ElasticFailure`. `LicenseApi` documents no exception contract and consumers — including core's
  * own `enableBasicLicense`, which chains `licenseInfo` first — treat `ElasticResult` as total.
  *
  * No Docker: the point is precisely that nothing is listening. The port is obtained by opening a
  * `ServerSocket` on an ephemeral port and closing it, so the number is real and free rather than
  * a guess that some other process might occupy.
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
    an[OutOfMemoryError] should be thrownBy helper.tryAction[Int](throw new OutOfMemoryError("boom"))
  }
}
