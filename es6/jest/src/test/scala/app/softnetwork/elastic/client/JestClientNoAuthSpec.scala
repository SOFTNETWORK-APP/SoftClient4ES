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

import app.softnetwork.elastic.client.jest.JestClientCompanion
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Pure-JVM regression for the #162 follow-up: the builtin softnetwork-elastic.conf no longer
  * defaults `method = "noauth"`, so a configuration with no credentials and no explicit method
  * resolves `authMethod` to `None`. `JestClientCompanion.buildHttpConfig` used to throw
  * `IllegalStateException("Invalid authentication configuration: None")` at client CREATION time
  * (172 CI failures) — `None` must be treated as no-auth, exactly like `Some(NoAuth)`.
  *
  * Client construction never dials the cluster, so this spec needs no embedded/Docker Elasticsearch
  * — it runs everywhere (the embedded-ES suite cannot start on Apple Silicon: ES 6.8's jvm.options
  * passes the x86-only `-XX:UseAVX=2`).
  */
class JestClientNoAuthSpec extends AnyWordSpec with Matchers {

  "JestClientCompanion" should {

    "create a client when no credentials and no method are configured (authMethod None)" in {
      val config = ElasticConfig(ConfigFactory.load("softnetwork-elastic.conf"))
        // hermetic: immune to ELASTIC_* env vars resolved by the builtin conf
        .copy(credentials = ElasticCredentials())
      config.credentials.authMethod shouldBe None

      val companion = new JestClientCompanion {
        override def elasticConfig: ElasticConfig = config
      }
      try {
        val client = companion.apply()
        client should not be null
      } finally {
        companion.close()
      }
    }

    "still reject an explicitly selected method with missing credentials" in {
      val config = ElasticConfig(ConfigFactory.load("softnetwork-elastic.conf"))
        .copy(credentials = ElasticCredentials(method = Some("basic")))
      config.credentials.authMethod shouldBe Some(BasicAuth)

      val companion = new JestClientCompanion {
        override def elasticConfig: ElasticConfig = config
      }
      an[IllegalStateException] should be thrownBy companion.apply()
    }
  }
}
