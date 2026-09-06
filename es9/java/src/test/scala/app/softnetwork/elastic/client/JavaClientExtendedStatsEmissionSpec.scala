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

import app.softnetwork.elastic.client.java.{JavaClientApi, JavaClientSearchBodySerializer}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import com.fasterxml.jackson.databind.ObjectMapper
import com.sksamuel.elastic4s.requests.searches.aggs.{AbstractAggregation, Aggregation}
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.jdk.CollectionConverters._

/** Issue #222 (story BIDC-3) -- the ES 8 / ES 9 Java-client half: `STDDEV` / `VARIANCE` (the whole
  * `extended_stats` family) over a TRANSFORMED expression carries its script in the emitted JSON,
  * on BOTH serialisation doors and for BOTH binds (plain, and windowed under a partition bucket).
  *
  * Before the fix the same statements emitted `"extended_stats":{"field":"createdAt"}` -- the
  * statistic of the raw timestamps -- or `"extended_stats":{}` when the transform had no raw field
  * behind it (T2 capture on the base commit, both bridge copies). An execution-success assertion
  * cannot see either; these assert the JSON.
  *
  * No Docker: `ElasticClientCompanion` builds the underlying client lazily and `apply()` is never
  * called, so nothing here touches the network. Kept byte-identical between the ES 8 and ES 9
  * modules (their elastic4s builders are identical).
  */
class JavaClientExtendedStatsEmissionSpec extends AnyWordSpec with Matchers {

  private val client: JavaClientApi = new JavaClientApi {
    override def config: Config = ConfigFactory.load()
  }

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  private val mapper = new ObjectMapper()

  private def single(sql: String): SingleSearch =
    SelectStatement(sql).statement match {
      case Some(s: SingleSearch) => s
      case other                 => fail(s"Not a single search: $other")
    }

  /** Door 1 -- what the client sends: `singleSearchToJsonQuery`. */
  private def emitted(sql: String): String = client.singleSearchToJsonQuery(single(sql))

  private val family = Seq("STDDEV", "STDDEV_SAMP", "STDDEV_POP", "VARIANCE", "VAR_SAMP", "VAR_POP")

  private def plain(fn: String, operand: String) =
    s"SELECT id, $fn($operand) AS s FROM t GROUP BY id"

  private def windowed(fn: String, operand: String) =
    s"SELECT id, name, $fn($operand) OVER (PARTITION BY id) AS s FROM t"

  private val yearScript =
    """"script":{"lang":"painless","source":"def param1 = (doc['createdAt'].size() == 0 ? null : """ +
    """doc['createdAt'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"}"""

  private val absScript =
    """"script":{"lang":"painless","source":"def param1 = (doc['salary'].size() == 0 ? null : """ +
    """doc['salary'].value); (param1 == null) ? null : Double.valueOf(Math.abs(param1))"}"""

  private val partition = """"terms":{"field":"id","size":65536,"min_doc_count":1}"""

  /** The script of every ScriptedExtendedStatsAggregation marker in the aggregation tree. */
  private def markerScriptsOf(aggs: Iterable[AbstractAggregation]): Seq[String] =
    aggs.toSeq.flatMap {
      case m: ScriptedExtendedStatsAggregation => m.inner.script.map(_.script).toSeq
      case a: Aggregation                      => markerScriptsOf(a.subaggs)
      case _                                   => Seq.empty
    }

  "the ES 8/9 client" should {

    "emit the script of STDDEV over a field-derived transform (plain bind) -- issue #222" in {
      emitted(plain("STDDEV", "YEAR(createdAt)")) shouldBe
      s"""{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{$partition,""" +
      s""""aggs":{"s":{"extended_stats":{"field":"createdAt",$yearScript}}}}}}"""
    }

    "emit a pure script when the transform has no raw field behind it (ABS(salary))" in {
      // Before: `"extended_stats":{}` -- neither field nor script, rejected by Elasticsearch.
      emitted(plain("VARIANCE", "ABS(salary)")) shouldBe
      s"""{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{$partition,""" +
      s""""aggs":{"s":{"extended_stats":{$absScript}}}}}}"""
    }

    "emit the script on the WINDOWED bind (under the partition bucket)" in {
      emitted(windowed("STDDEV", "YEAR(createdAt)")) shouldBe
      s"""{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{$partition,""" +
      s""""aggs":{"s":{"extended_stats":{"field":"createdAt",$yearScript}}}}}}"""
    }

    "emit the script for EVERY family member, plain and windowed" in {
      family.foreach { fn =>
        Seq(plain(fn, "YEAR(createdAt)"), windowed(fn, "YEAR(createdAt)")).foreach { sql =>
          withClue(s"[$sql] ") {
            val json = emitted(sql)
            json should include(""""extended_stats":{"field":"createdAt","script":{""")
            json should include("ChronoField.YEAR")
            json should not include """"extended_stats":{"field":"createdAt"}"""
          }
        }
        Seq(plain(fn, "ABS(salary)"), windowed(fn, "ABS(salary)")).foreach { sql =>
          withClue(s"[$sql] ") {
            val json = emitted(sql)
            json should include(""""extended_stats":{"script":{""")
            json should not include """"extended_stats":{}"""
          }
        }
      }
    }

    "emit the script on the sqlQueryToAggregations door too" in {
      implicit val serializer: SearchBodySerializer = client.searchBodySerializer
      val aggs: Seq[ElasticAggregation] = SelectStatement(plain("STDDEV_POP", "YEAR(createdAt)"))
      aggs should have size 1
      aggs.head.hasTransformExtendedStats shouldBe true
      aggs.head.query shouldBe Some(
        s"""{"query":{"match_all":{}},"size":0,"aggs":{"s":{"extended_stats":{"field":"createdAt",$yearScript}}}}"""
      )
    }

    "render the marker's script VERBATIM -- the script the bridge's null-safety guard vets" in {
      // The null-safety rule (lead directive 2026-09-06) has ONE owner: AggregationNamingSpec's
      // marker-tree guard in the bridge template (and its es6 twin). What this module adds is the
      // rendering, so what it must prove is that the rendered `script.source` is byte-for-byte the
      // script carried by the ScriptedExtendedStatsAggregation marker that guard walks.
      family.foreach { fn =>
        Seq(plain(fn, "YEAR(createdAt)"), windowed(fn, "ABS(salary)")).foreach { sql =>
          withClue(s"[$sql] ") {
            val request: ElasticSearchRequest = requestToElasticSearchRequest(single(sql))
            val vetted = markerScriptsOf(request.search.aggs)
            vetted should have size 1
            val rendered = mapper.readTree(emitted(sql)).findValues("script").asScala.flatMap { s =>
              Option(s.get("source")).map(_.asText())
            }
            rendered shouldBe vetted
          }
        }
      }
    }
  }

  "JavaClientSearchBodySerializer" should {

    "be byte-identical to the default serializer for any request without a transform-bearing extended_stats" in {
      Seq(
        plain("STDDEV", "salary"),
        windowed("VAR_POP", "salary"),
        "SELECT id, MAX(YEAR(createdAt)) AS m FROM t GROUP BY id",
        "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(YEAR(createdAt)) > 2020",
        "SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id",
        """SELECT c.id, COUNT(DISTINCT e.address) AS nb FROM customers c JOIN UNNEST(c.emails) AS e
          |GROUP BY c.id HAVING COUNT(DISTINCT e.address) > 1""".stripMargin,
        "SELECT name, salary FROM t WHERE salary > 10 ORDER BY salary DESC LIMIT 5"
      ).foreach { sql =>
        withClue(s"[$sql] ") {
          val request: ElasticSearchRequest = requestToElasticSearchRequest(single(sql))
          request.hasTransformExtendedStats shouldBe false
          JavaClientSearchBodySerializer.serialize(request.search) shouldBe
          SearchBodySerializer.Default.serialize(request.search)
        }
      }
    }

    "be the serializer the client injects" in {
      client.searchBodySerializer shouldBe JavaClientSearchBodySerializer
    }

    "keep elastic4s's own diagnostic for an aggregation NEITHER it nor we can build (R3-1)" in {
      // The custom handler REPLACES `defaultCustomAggregationHandler`, whose only job is to fail an
      // unknown aggregation with a NotImplementedError naming the class. A one-case handler would
      // raise a bare MatchError instead -- strictly worse for anyone debugging a missing builder.
      val unknown = new Aggregation {
        type T = Aggregation
        override def name: String = "unknown"
        override def metadata: Map[String, AnyRef] = Map.empty
        override def subaggs: Seq[AbstractAggregation] = Seq.empty
        override def subAggregations(aggs: Iterable[AbstractAggregation]): T = this
        override def metadata(map: Map[String, AnyRef]): T = this
      }
      val request = com.sksamuel.elastic4s.ElasticApi.search("t").aggregations(unknown)
      val thrown = intercept[NotImplementedError](JavaClientSearchBodySerializer.serialize(request))
      thrown.getMessage should include(unknown.getClass.getName)
    }
  }
}
