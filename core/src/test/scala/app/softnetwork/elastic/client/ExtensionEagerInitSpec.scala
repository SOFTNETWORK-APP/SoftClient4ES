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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

/** #163 — eager extension init. Two guarantees:
  *   1. `initializeExtensions()` forces the ServiceLoader scan and reports the count (core's own
  *      META-INF/services registers CoreDdlExtension + CoreDqlExtension → always >= 2 here).
  *   1. THE TRAP — a delegator forwards to its delegate — the wrapper's own (inherited, unused)
  *      registry must NOT be the one warmed, because run() delegates to delegate.run().
  */
class ExtensionEagerInitSpec extends AnyFlatSpec with Matchers {

  implicit val ec: ExecutionContext = ExecutionContext.global

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  // `protected def logger` is the ONLY abstract member NopeClientApi leaves open; `config` and
  // `metrics` have concrete defaults, so licenseRefreshStrategy resolves from ConfigFactory.load()
  // → Community fallback, and clusterUuid/clusterName/version no-op failures are absorbed by
  // ExtensionApi's `case _ =>`. Same recipe as CoreDqlExtensionSpec's RecordingClient.
  private def newNopeClient(): ElasticClientApi = new NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  "initializeExtensions" should "force the registry and report the loaded-extension count" in {
    val client = newNopeClient()
    val n = Await.result(client.initializeExtensions(), 30.seconds)
    n should be >= 2 // CoreDdlExtension + CoreDqlExtension from core's META-INF/services
  }

  it should "be forwarded to the delegate by ElasticClientDelegator" in {
    @volatile var forwarded = false
    val probe: ElasticClientApi = new NopeClientApi {
      override protected def logger: Logger = testLogger
      override def initializeExtensions()(implicit ec: ExecutionContext): Future[Int] = {
        forwarded = true
        Future.successful(42)
      }
    }
    val wrapper = new ElasticClientDelegator {
      override val delegate: ElasticClientApi = probe
    }
    Await.result(wrapper.initializeExtensions(), 5.seconds) shouldBe 42
    forwarded shouldBe true
  }
}
