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

/** The ES 6 REST-client half: a GROUP BY key filter pairing a pattern with a list of several values
  * is REFUSED before any JSON exists, with a named `ElasticError` (status 400), because
  * Elasticsearch 6.8 rejects that pair outright. Every other pair is written exactly as the default
  * serializer writes it. No Docker: the underlying client is built lazily and never used.
  *
  * Unlike the extended_stats rejection spec, there is no `sqlQueryToAggregations` row: that
  * deprecated door never builds a `terms` bucket (`ElasticAggregation.buildBuckets`, the only place
  * a key filter is written, is reached from `requestToRootAggregations` /
  * `requestToScopedAggregations` only), so it cannot carry a key filter to refuse.
  */
class RestHighLevelClientTermsFilterRejectionSpec extends AnyWordSpec with Matchers {

  private val client: RestHighLevelClientApi = new RestHighLevelClientApi {
    override def config: Config = ConfigFactory.load()
  }

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  private def single(sql: String): SingleSearch =
    SelectStatement(sql).statement match {
      case Some(s: SingleSearch) => s
      case other                 => fail(s"Not a single search: $other")
    }

  private val group = "SELECT sval, COUNT(*) AS c FROM t GROUP BY sval HAVING "

  "the ES 6 REST client" should {

    "refuse a pattern beside a list of several values on one GROUP BY key, before any JSON exists" in {
      Seq(
        "sval LIKE 'a%' AND sval NOT IN ('a.b','axb')",
        "sval IN ('a.b','axb') AND sval NOT LIKE 'x%'",
        "sval RLIKE 'a.*' AND sval NOT IN ('b','c')",
        "MATCH (sval) AGAINST ('a') AND sval NOT IN ('b','c')"
      ).foreach { predicate =>
        withClue(s"[$predicate] ") {
          val refusal =
            intercept[ElasticError](client.singleSearchToJsonQuery(single(group + predicate)))
          refusal.message shouldBe
          RestHighLevelClientSearchBodySerializer.unmixableTermsFilterRefusal("sval")
          refusal.message should include("Elasticsearch 6")
          refusal.message should include("7.11")
          refusal.statusCode shouldBe Some(400)
          refusal.operation shouldBe Some("search")
        }
      }
    }

    "write every other pair exactly as the default serializer does" in {
      Seq(
        "sval = 'a.b'",
        "sval = 'a.b' AND sval NOT LIKE 'x%'",
        "sval LIKE 'a%' AND sval <> 'a.b'",
        "sval IN ('a','b') AND sval <> 'c'",
        "sval LIKE 'a%' AND sval NOT LIKE 'b%'"
      ).foreach { predicate =>
        withClue(s"[$predicate] ") {
          val request: ElasticSearchRequest =
            requestToElasticSearchRequest(single(group + predicate))
          client.singleSearchToJsonQuery(single(group + predicate)) shouldBe
          SearchBodySerializer.Default.serialize(request.search).replace("\"version\":true,", "")
        }
      }
    }

    "inject the refusing serializer" in {
      client.searchBodySerializer shouldBe RestHighLevelClientSearchBodySerializer
    }
  }
}
