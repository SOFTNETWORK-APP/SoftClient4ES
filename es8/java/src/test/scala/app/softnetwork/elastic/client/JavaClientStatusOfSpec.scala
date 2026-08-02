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

import app.softnetwork.elastic.client.java.{JavaClientCompanion, JavaClientHelpers}
import co.elastic.clients.elasticsearch._types.{ElasticsearchException, ErrorCause, ErrorResponse}
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// `java` is shadowed by the sibling `app.softnetwork.elastic.client.java` sub-package — the
// `_root_.` prefix is mandatory here (SoftClient4ES#184, Task 10.4).
import _root_.java.util.concurrent.CompletionException

/** SoftClient4ES#184 — proves the ES 8/9 `statusOf` extractor against hand-built real exception
  * instances, without Docker: `ElasticClientCompanion` creates the underlying client lazily, and
  * `apply()` is never called here.
  */
class JavaClientStatusOfSpec extends AnyWordSpec with Matchers {

  private case class Probe(config: ElasticConfig)
      extends JavaClientCompanion
      with JavaClientHelpers {
    override def elasticConfig: ElasticConfig = config
  }

  private val probe = Probe(ElasticConfig(ConfigFactory.load()))

  private def esException(status: Int, errType: String): ElasticsearchException =
    new ElasticsearchException(
      "es/get",
      ErrorResponse.of((r: ErrorResponse.Builder) =>
        r.status(status)
          .error((e: ErrorCause.Builder) => e.`type`(errType).reason("no such index [absent]"))
      )
    )

  "statusOf" should {

    "map an ElasticsearchException to its status" in {
      probe.statusOf(esException(404, "index_not_found_exception")) shouldBe Some(404)
    }

    "unwrap a CompletionException first" in {
      probe.statusOf(
        new CompletionException(esException(409, "version_conflict_engine_exception"))
      ) shouldBe Some(409)
    }

    "return None for a plain transport failure" in {
      probe.statusOf(new _root_.java.net.ConnectException("Connection refused")) shouldBe None
    }

    "treat the primitive 0 sentinel as 'unknown', never as an HTTP status" in {
      // `ElasticsearchException.status()` returns a primitive int, so an absent status is 0.
      // Some(0) would defeat isNotFound and every IF EXISTS check (SoftClient4ES#184).
      probe.statusOf(esException(0, "unknown")) shouldBe None
      probe.statusOrServerError(esException(0, "unknown")) shouldBe Some(500)
    }

    "fall through to the core default for a framework throwable" in {
      probe.statusOf(
        app.softnetwork.elastic.client.result.ElasticError("boom", statusCode = Some(403))
      ) shouldBe Some(403)
    }
  }
}
