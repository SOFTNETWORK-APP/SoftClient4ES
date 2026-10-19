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

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{mergedSchema, Schema}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.collection.JavaConverters._

/** A field sort over a column the statement's schema resolves carries that column's
  * `unmapped_type`, and a `geo_distance` over a column resolved as a GEO_POINT carries
  * `ignore_unmapped`.
  *
  * Without them, an index of a pattern or list that does not map the column fails each of its
  * shards (`No mapping found for [c] in order to sort on`, `failed to find geo_point field`), so
  * that index's documents were missing from the answer or, once core reads `_shards`, the whole
  * statement answers an error. With them, those documents sort as having no value (and a
  * `geo_distance` matches none of them).
  *
  * The schema is loaded from the document `GET <index>` answers, as core attaches it
  * (`SearchApi.resolveWithSchema`). Without a schema every request keeps its bytes. Same cases as
  * the hand-maintained es6 bridge's copy.
  */
class SortUnmappedTypeSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private def schemaOf(index: String, properties: String): Schema =
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": {"properties": $properties}, """ +
      s""""settings": {"index": {"number_of_shards": "3"}}}}"""
    ).schema

  private val properties: String =
    """{"id": {"type": "keyword"}, "name": {"type": "keyword"}, "txt": {"type": "text"},
      | "num": {"type": "long"}, "amt": {"type": "double"}, "ts": {"type": "date"},
      | "flag": {"type": "boolean"}, "loc": {"type": "geo_point"},
      | "items": {"type": "nested", "properties": {"k": {"type": "keyword"}}}}""".stripMargin

  private val tSchema: Schema = schemaOf("t", properties)

  private def body(sql: String, schema: Option[Schema]): JsonNode = {
    val parsed: SingleSearch = Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = schema.fold(parsed)(s => parsed.update(Some(s)))
    mapper.readTree(request.query)
  }

  /** Every field sort of the body -- the row sort and every `top_hits` sort -- as field name -> its
    * `unmapped_type`. A script sort is listed under `_script`.
    */
  private def sortsOf(node: JsonNode): Seq[(String, Option[String])] =
    node.findValues("sort").asScala.toSeq.flatMap { sorts =>
      sorts.elements().asScala.toSeq.flatMap { sort =>
        sort.fields().asScala.toSeq.map { entry =>
          entry.getKey -> Option(entry.getValue.get("unmapped_type")).map(_.asText())
        }
      }
    }

  private def sorts(sql: String, schema: Option[Schema] = Some(tSchema)) =
    sortsOf(body(sql, schema))

  private def geoDistances(sql: String, schema: Option[Schema] = Some(tSchema)): Seq[JsonNode] =
    body(sql, schema).findValues("geo_distance").asScala.toSeq

  "a row sort over a column the schema resolves" should "carry the column's unmapped_type" in {
    sorts("SELECT id, name FROM t ORDER BY name LIMIT 100") shouldBe Seq("name" -> Some("keyword"))
    sorts("SELECT id FROM t ORDER BY num LIMIT 100") shouldBe Seq("num" -> Some("long"))
    sorts("SELECT id FROM t ORDER BY ts DESC LIMIT 100") shouldBe Seq("ts" -> Some("date"))
    sorts("SELECT id FROM t ORDER BY amt LIMIT 100") shouldBe Seq("amt" -> Some("double"))
    sorts("SELECT id FROM t ORDER BY flag LIMIT 100") shouldBe Seq("flag" -> Some("boolean"))
  }

  it should "carry it without a LIMIT, and next to the null ordering" in {
    sorts("SELECT id, name FROM t ORDER BY name") shouldBe Seq("name" -> Some("keyword"))
    val node = body("SELECT id, name FROM t ORDER BY name DESC NULLS FIRST", Some(tSchema))
    sortsOf(node) shouldBe Seq("name" -> Some("keyword"))
    node.get("sort").get(0).get("name").get("missing").asText() shouldBe "_first"
    node.get("sort").get(0).get("name").get("order").asText() shouldBe "desc"
  }

  it should "carry one per sort key" in {
    sorts("SELECT id FROM t ORDER BY name ASC, num DESC LIMIT 10") shouldBe Seq(
      "name" -> Some("keyword"),
      "num"  -> Some("long")
    )
  }

  "a window's ORDER BY" should "carry the unmapped_type on its top_hits sort" in {
    val windowSorts = sorts(
      "SELECT id, FIRST_VALUE(num) OVER (PARTITION BY id ORDER BY name) AS fv FROM t"
    )
    windowSorts should not be empty
    windowSorts shouldBe Seq("name" -> Some("keyword"))
    sorts(
      "SELECT id, LAST_VALUE(num) OVER (PARTITION BY id ORDER BY ts DESC) AS lv FROM t"
    ) shouldBe Seq("ts" -> Some("date"))
  }

  "a sort the schema cannot type" should "keep its bytes" in {
    // A character column: `text` has no doc values to sort on.
    sorts("SELECT id FROM t ORDER BY txt LIMIT 100") shouldBe Seq("txt" -> None)
    // A column the schema does not know: a typo must keep failing loudly.
    sorts("SELECT id FROM t ORDER BY nme LIMIT 100") shouldBe Seq("nme" -> None)
    // A function of a column: a script sort.
    sorts("SELECT id FROM t ORDER BY UPPER(name) LIMIT 100").map(_._2) shouldBe Seq(None)
    // A column named through its SELECT alias.
    sorts("SELECT name AS n FROM t ORDER BY name LIMIT 100").map(_._2) shouldBe Seq(None)
    // A nested column.
    val nestedSorts =
      sorts("SELECT t.id, i.k FROM t JOIN UNNEST(t.items) AS i ORDER BY i.k LIMIT 100")
    nestedSorts should not be empty
    nestedSorts.flatMap(_._2) shouldBe empty
  }

  it should "keep its bytes when the indices of a pattern disagree on the column's type" in {
    val (disagreeing, conflicts) = mergedSchema(
      "p_*",
      Seq(
        "p_a" -> schemaOf("p_a", """{"name": {"type": "keyword"}}"""),
        "p_b" -> schemaOf("p_b", """{"name": {"type": "text"}}""")
      )
    )
    conflicts should not be empty
    sorts("SELECT name FROM p_* ORDER BY name LIMIT 100", Some(disagreeing)) shouldBe Seq(
      "name" -> None
    )
  }

  "every sort without a schema" should "render as before" in {
    Seq(
      "SELECT id, name FROM t ORDER BY name LIMIT 100",
      "SELECT id FROM t ORDER BY num DESC",
      "SELECT id, FIRST_VALUE(num) OVER (PARTITION BY id ORDER BY name) AS fv FROM t"
    ).foreach { sql =>
      withClue(sql) {
        val found = sorts(sql, schema = None)
        found should not be empty
        found.flatMap(_._2) shouldBe empty
      }
    }
  }

  "a geo_distance over a column resolved as a GEO_POINT" should "carry ignore_unmapped" in {
    val sql = "SELECT id FROM t WHERE ST_DISTANCE(loc, POINT(48.85, 2.35)) <= 10 km"
    val withSchema = geoDistances(sql)
    withSchema should have size 1
    withSchema.head.get("ignore_unmapped").asBoolean() shouldBe true

    val withoutSchema = geoDistances(sql, schema = None)
    withoutSchema should have size 1
    withoutSchema.head.has("ignore_unmapped") shouldBe false
  }

  it should "not carry it over a column the schema does not type as a GEO_POINT" in {
    val other = schemaOf("t", """{"id": {"type": "keyword"}, "loc": {"type": "keyword"}}""")
    val found =
      geoDistances(
        "SELECT id FROM t WHERE ST_DISTANCE(loc, POINT(48.85, 2.35)) <= 10 km",
        Some(other)
      )
    found should have size 1
    found.head.has("ignore_unmapped") shouldBe false
  }
}
