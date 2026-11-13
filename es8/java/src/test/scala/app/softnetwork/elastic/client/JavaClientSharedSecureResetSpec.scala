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

import app.softnetwork.elastic.scalatest.{ElasticDockerTestKit, ResetStep, SharedElasticContainers}
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.{Header, HttpHost}
import org.elasticsearch.client.{Request, ResponseException, RestClient}
import org.json4s.jackson.JsonMethods.parse
import org.json4s.{JArray, JBool, JString}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.nio.charset.StandardCharsets
import _root_.java.util.Base64

/** The control of the shared-container reset on the SECURE es8 key (the one
  * `JavaClientReadOnlyUserSpec` and `JavaClientFactoryIdentitySpec` share): a user, a role and an
  * API key created after the snapshot are deleted or invalidated by `reset`, while the reserved
  * `elastic` user and the built-in `superuser` role remain.
  *
  * It FAILS (never cancels) when its container is not shared: run without the
  * `softclient4es.testkit.shared-container` property, it is filtered out (`*ResetSpec`).
  */
class JavaClientSharedSecureResetSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  override lazy val xpackSecurityEnabled: Boolean = true

  // the password Testcontainers sets for ES >= 8 (ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD)
  private def basic(user: String, password: String): String =
    "Basic " + Base64.getEncoder.encodeToString(s"$user:$password".getBytes(StandardCharsets.UTF_8))

  override lazy val restClient: RestClient =
    RestClient
      .builder(HttpHost.create(elasticURL))
      .setDefaultHeaders(
        Array[Header](new BasicHeader("Authorization", basic("elastic", "changeme")))
      )
      .build()

  private def http(method: String, path: String, body: Option[String] = None): (Int, String) = {
    val request = new Request(method, path)
    body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
    try {
      val response = restClient.performRequest(request)
      (
        response.getStatusLine.getStatusCode,
        Option(response.getEntity).map(EntityUtils.toString).getOrElse("")
      )
    } catch {
      case e: ResponseException =>
        (
          e.getResponse.getStatusLine.getStatusCode,
          Option(e.getResponse.getEntity).map(EntityUtils.toString).getOrElse("")
        )
    }
  }

  private def ok(call: (Int, String)): Unit =
    withClue(s"set-up call answered ${call._1}: ${call._2.take(300)}: ")(call._1 should be < 300)

  private val Role = "ci1_reset_role"
  private val User = "ci1_reset_user"

  "The secure shared-container reset" should "run on a shared container" in {
    SharedElasticContainers.isShared(elasticContainer) shouldBe true
  }

  it should "delete the users and roles, and invalidate the API keys, created after the snapshot" in {
    val t = SharedElasticContainers
      .targetOf(elasticContainer)
      .getOrElse(fail("this suite's container is not shared"))
    t.security shouldBe true
    ok(
      http(
        "PUT",
        s"/_security/role/$Role",
        Some("""{"indices":[{"names":["ci1_*"],"privileges":["read"]}]}""")
      )
    )
    ok(
      http(
        "PUT",
        s"/_security/user/$User",
        Some(s"""{"password":"ci1-reset-pass","roles":["$Role"]}""")
      )
    )
    val (status, created) = http("POST", "/_security/api_key", Some("""{"name":"ci1-reset-key"}"""))
    ok((status, created))
    val keyId = parse(created) \ "id" match {
      case JString(id) => id
      case other       => fail(s"no API key id in $other")
    }

    def listed(step: ResetStep): Set[String] =
      step.listed(SharedElasticContainers.list(restClient, step, t), t)

    // the steps SEE every planted object (a parse that missed one would pass vacuously below)
    listed(ResetStep.Users) should contain(User)
    listed(ResetStep.Roles) should contain(Role)
    listed(ResetStep.ApiKeys) should contain(keyId)

    try {
      SharedElasticContainers.reset(elasticContainer, suiteName, resetSteps)

      http("GET", s"/_security/user/$User")._1 shouldBe 404
      http("GET", s"/_security/role/$Role")._1 shouldBe 404
      val (keyStatus, key) = http("GET", s"/_security/api_key?id=$keyId")
      keyStatus shouldBe 200
      parse(key) \ "api_keys" match {
        case JArray(List(k)) => (k \ "invalidated") shouldBe JBool(true)
        case other           => fail(s"API key $keyId not listed once: $other")
      }
      listed(ResetStep.ApiKeys) should not contain keyId

      // reserved objects are never touched
      http("GET", "/_security/user/elastic")._1 shouldBe 200
      http("GET", "/_security/role/superuser")._1 shouldBe 200
    } finally {
      // an in-test reset deletes `all_templates`, which `ElasticTestKit.beforeAll` created
      ok(
        http(
          "PUT",
          "/_template/all_templates",
          Some(
            """{"index_patterns":["*"],"settings":{"number_of_shards":1,"number_of_replicas":0}}"""
          )
        )
      )
    }
  }
}
