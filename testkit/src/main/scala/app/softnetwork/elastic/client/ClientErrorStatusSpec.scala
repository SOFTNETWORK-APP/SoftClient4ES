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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.ElasticFailure
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

/** A minimal document shape for the `getAsyncAs` probe — mirrors the call
  * `MaterializedViewExtension.loadMetadata` makes (extensions#40 / SoftClient4ES#184).
  */
case class StatusProbeDocument(id: String)

/** SoftClient4ES#184 — `ElasticError.statusCode` must reflect what Elasticsearch actually returned,
  * so that `IF EXISTS` and other 404-keyed branches work.
  *
  * Asserted on all four ES lines. Before the fix this trait is RED on es8/es9 (their
  * `executeGetAsync` returns a failed `Future`, which `GetApi.getAsync` flattened to a hardcoded
  * 500) and GREEN on es6/es7 (their `executeGetAsync` always returns a successful `Future` holding
  * an `ElasticFailure` that already carries the status). Both cases are wanted: the fix for one,
  * regression protection for the other.
  */
trait ClientErrorStatusSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // `implicit def formats: Formats` comes from ElasticRestClientTestKit — do NOT redeclare it
  // (it is a concrete member; redeclaring fails with "override modifier required").

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  /** Never created by this or any other spec — the whole point is that the index is absent. */
  private val absentIndex = "r1fix3_absent_index"

  private val timeout = Duration(30, TimeUnit.SECONDS)

  "get on a non-existent index" should "report HTTP 404, not 500" in {
    val result = client.get("any-id", absentIndex)

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "getAsync on a non-existent index" should "report HTTP 404, not 500" in {
    val result = Await.result(client.getAsync("any-id", absentIndex), timeout)

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "getAsyncAs on a non-existent index" should "report HTTP 404, not 500" in {
    // The exact call shape used by MaterializedViewExtension.loadMetadata (extensions#40).
    val result = Await.result(
      client.getAsyncAs[StatusProbeDocument](id = "any-id", index = Some(absentIndex)),
      timeout
    )

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "a failure on a non-existent index" should "be recognised by ElasticFailure.isNotFound" in {
    client.get("any-id", absentIndex) match {
      case f: ElasticFailure => f.isNotFound shouldBe true
      case other             => fail(s"expected an ElasticFailure, got $other")
    }
  }
}
