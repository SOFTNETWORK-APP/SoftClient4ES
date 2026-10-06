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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** A per-group calculation that reads the request clock (`CURRENT_DATE - MAX(d)`, `NOW() -
  * MAX(ts)`) is sent WITH the clock: its `bucket_script` carries the script object -- `source`,
  * `lang`, `params.__now__` -- where the elastic4s builder writes the bare source string and drops
  * the params (`BucketScriptParams`). Every other `bucket_script` is sent as it was.
  *
  * The clock is a PARAM, not text spliced into the source: the source is the same from one request
  * to the next, so Elasticsearch compiles it once.
  */
class BucketScriptParamsSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  private val now: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  implicit def timestamp: Long = now

  implicit def sqlQueryToRequest(sqlQuery: SelectStatement): ElasticSearchRequest =
    sqlQuery.statement match {
      case Some(value: SingleSearch) => value.copy(score = sqlQuery.score)
      case other => throw new IllegalArgumentException(s"Not a single search: $other")
    }

  private val mapper = new ObjectMapper()

  private def bucketScripts(sql: String): Seq[JsonNode] = {
    val request: ElasticSearchRequest = SelectStatement(sql)
    def all(node: JsonNode): Seq[JsonNode] =
      Option(node.get("bucket_script")).toSeq ++ node.elements().asScala.toSeq.flatMap(all)
    all(mapper.readTree(request.query))
  }

  "a per-group calculation reading the clock" should "send the clock as a script param" in {
    Seq(
      "SELECT g, CURRENT_DATE - MAX(d) AS x FROM t GROUP BY g",
      "SELECT g, NOW() - MAX(ts) AS x FROM t GROUP BY g",
      "SELECT g, DATEDIFF(CURRENT_DATE, MAX(d)) AS x FROM t GROUP BY g"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val scripts = bucketScripts(sql)
        scripts should have size 1
        val script = scripts.head.get("script")
        script.isObject shouldBe true
        script.get("source").asText() should include("params.__now__")
        script.get("lang").asText() shouldBe "painless"
        script.get("params").get("__now__").asLong() shouldBe now
      }
    }
  }

  "a per-group calculation without the clock" should "be sent as it always was" in {
    val scripts = bucketScripts("SELECT g, MAX(d) - MIN(d) AS x FROM t GROUP BY g")
    scripts should have size 1
    scripts.head.get("script").isTextual shouldBe true
  }
}
