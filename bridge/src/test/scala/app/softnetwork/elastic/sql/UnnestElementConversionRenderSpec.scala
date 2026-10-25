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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** A conversion of a bare UNNEST element column (`CAST(i.qty AS VARCHAR)`) is answered by the
  * READER: the request carries NO script for it -- Elasticsearch would evaluate it once per parent,
  * where the element is not visible -- and fetches the element's column in the UNNEST's inner hits,
  * which the row explosion places under the column's name. Same cases as the es6 bridge's copy.
  */
class UnnestElementConversionRenderSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private val u = "FROM t AS t JOIN UNNEST(t.items) AS i"

  private def rendered(sql: String): JsonNode = {
    val ss = Parser(sql) match {
      case Right(ss: SingleSearch) => ss
      case other                   => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = ss
    mapper.readTree(request.query)
  }

  /** Every text the `inner_hits` blocks named `name` hold (their `_source` filtering), wherever
    * they are rendered.
    */
  private def innerHitsSources(node: JsonNode, name: String): Seq[String] = {
    def texts(n: JsonNode): Seq[String] =
      if (n.isTextual) Seq(n.asText()) else n.elements().asScala.toSeq.flatMap(texts)
    val blocks = node.findValues("inner_hits").asScala.toSeq
    blocks.filter(b => Option(b.get("name")).exists(_.asText() == name)).flatMap(b => texts(b))
  }

  "a conversion of a bare UNNEST element column" should "send no script and fetch the element's column" in {
    Seq(
      s"SELECT t.id, CAST(i.qty AS VARCHAR) AS v $u"                -> "qty",
      s"SELECT t.id, TRY_CAST(i.qty AS SMALLINT) AS v $u LIMIT 100" -> "qty",
      s"SELECT t.id, i.qty::DOUBLE AS v $u"                         -> "qty",
      s"SELECT t.id, CONVERT(VARCHAR, i.at) AS v $u"                -> "at",
      s"SELECT t.id, DATE_TRUNC(HOUR, i.at) AS v $u"                -> "at"
    ).foreach { case (sql, column) =>
      withClue(s"[$sql] ") {
        val json = rendered(sql)
        json.has("script_fields") shouldBe false
        withClue(s"$json ") {
          innerHitsSources(json, "i").exists(t =>
            t == column || t.endsWith(s".$column")
          ) shouldBe true
        }
      }
    }
  }

  it should "leave a conversion of a PARENT column beside it its script" in {
    val json = rendered(s"SELECT t.id, i.sku AS s, CAST(t.m AS VARCHAR) AS c $u")
    json.path("script_fields").has("c") shouldBe true
    withClue(s"$json ") {
      innerHitsSources(json, "i").exists(t => t == "sku" || t.endsWith(".sku")) shouldBe true
    }
  }
}
