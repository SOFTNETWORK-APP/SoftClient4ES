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

import app.softnetwork.elastic.client.rest.{
  RestHighLevelClientApi,
  RestHighLevelClientSearchBodySerializer
}
import app.softnetwork.elastic.client.result.ElasticError
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** Issue #222 (story BIDC-3) -- the ES 7 REST-client half: `STDDEV` / `VARIANCE` (the whole
  * `extended_stats` family) over a TRANSFORMED expression is REFUSED before any JSON exists, with a
  * named `ElasticError` (status 400), for BOTH binds and on BOTH serialisation doors -- never
  * executed against the raw field, which is what elastic4s 7.17.x's script-dropping builder used to
  * do silently. A raw-field extended_stats is untouched.
  *
  * No Docker: `ElasticClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network.
  */
class RestHighLevelClientExtendedStatsRejectionSpec extends AnyWordSpec with Matchers {

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.load()
  }

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  private val expectedMajor = 7

  private def single(sql: String): SingleSearch =
    SelectStatement(sql).statement match {
      case Some(s: SingleSearch) => s
      case other                 => fail(s"Not a single search: $other")
    }

  private val family = Seq("STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP")

  private def plain(fn: String, operand: String) =
    s"SELECT id, $fn($operand) AS s FROM t GROUP BY id"

  private def windowed(fn: String, operand: String) =
    s"SELECT id, name, $fn($operand) OVER (PARTITION BY id) AS s FROM t"

  private def assertRefused(refusal: ElasticError): Unit = {
    refusal.message shouldBe RestHighLevelClientSearchBodySerializer.TransformExtendedStatsUnsupported
    refusal.message should include(s"Elasticsearch $expectedMajor")
    refusal.message should include("elastic4s#4100")
    refusal.statusCode shouldBe Some(400)
    refusal.operation shouldBe Some("search")
  }

  s"the ES $expectedMajor client" should {

    "refuse a transform-bearing extended_stats before emission, for every family member and both binds" in {
      family.foreach { fn =>
        Seq(
          plain(fn, "YEAR(createdAt)"),
          plain(fn, "ABS(salary)"),
          windowed(fn, "YEAR(createdAt)"),
          windowed(fn, "ABS(salary)")
        ).foreach { sql =>
          withClue(s"[$sql] ") {
            assertRefused(intercept[ElasticError](client.singleSearchToJsonQuery(single(sql))))
          }
        }
      }
    }

    "refuse it on the sqlQueryToAggregations door too" in {
      implicit val serializer: SearchBodySerializer = client.searchBodySerializer
      val refusal = intercept[ElasticError] {
        val aggs: Seq[ElasticAggregation] = SelectStatement(plain("STDDEV", "YEAR(createdAt)"))
        aggs
      }
      assertRefused(refusal)
    }

    "leave a raw-field extended_stats exactly as the default serializer emits it" in {
      Seq(plain("STDDEV", "salary"), windowed("VAR_POP", "salary")).foreach { sql =>
        withClue(s"[$sql] ") {
          val request: ElasticSearchRequest = requestToElasticSearchRequest(single(sql))
          request.hasTransformExtendedStats shouldBe false
          client.singleSearchToJsonQuery(single(sql)) shouldBe
          SearchBodySerializer.Default.serialize(request.search).replace("\"version\":true,", "")
          client.singleSearchToJsonQuery(single(sql)) should include(
            """"extended_stats":{"field":"salary"}"""
          )
        }
      }
    }

    "leave every OTHER scripted metric untouched (MAX(YEAR(x)) keeps its script)" in {
      client.singleSearchToJsonQuery(
        single("SELECT id, MAX(YEAR(createdAt)) AS m FROM t GROUP BY id")
      ) should include(""""max":{"field":"createdAt","script":{"lang":"painless"""")
    }

    "inject the refusing serializer" in {
      client.searchBodySerializer shouldBe RestHighLevelClientSearchBodySerializer
      RestHighLevelClientSearchBodySerializer.ElasticsearchMajor shouldBe expectedMajor
    }
  }
}
