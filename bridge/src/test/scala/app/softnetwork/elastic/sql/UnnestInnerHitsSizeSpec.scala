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
import app.softnetwork.elastic.sql.schema.Schema
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** How many elements per parent an UNNEST asks Elasticsearch for: its `inner_hits` size.
  *
  * Without a `LIMIT`, an UNNEST projection asks for `NestedElements.innerHitsSize`: 100
  * (Elasticsearch's default `index.max_inner_result_window`), or the index's own setting when it is
  * lower, read from the schema core attaches before rendering. Left unset, `inner_hits` returns 3,
  * and a five-element parent answered three rows with HTTP 200. With a `LIMIT` the size is the
  * LIMIT, as before; a nested FILTER or AGGREGATE, whose elements are not rows, keeps the default.
  *
  * The emission is pinned here; that Elasticsearch returns the elements is asserted at RUN level
  * (`UnnestInnerHitsSizeExecutionSpec`). Same cases as the hand-maintained es6 bridge's copy.
  */
class UnnestInnerHitsSizeSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  /** The schema of `index` as core attaches it (`SearchApi.resolveWithSchema`, #306): loaded from
    * the document `GET <index>` answers, so the setting travels the path it travels in production.
    */
  private def schemaOf(index: String, maxInnerResultWindow: Option[Int]): Schema = {
    val setting =
      maxInnerResultWindow.map(w => s""", "max_inner_result_window": "$w"""").getOrElse("")
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"}}}, """ +
      s""""settings": {"index": {"number_of_shards": "1"$setting}}}}"""
    ).schema
  }

  /** inner hits name -> its `size`, for every `inner_hits` block of the rendered search. */
  private def innerHitsSizes(
    sql: String,
    schema: Option[Schema] = None
  ): Map[String, Option[Int]] = {
    val parsed: SingleSearch = Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] did not parse as a SingleSearch: $other")
    }
    val request: ElasticSearchRequest = schema.fold(parsed)(s => parsed.update(Some(s)))
    val blocks = mapper.readTree(request.query).findValues("inner_hits")
    (0 until blocks.size()).map { i =>
      val block = blocks.get(i)
      block.get("name").asText() -> Option(block.get("size")).map(_.asInt())
    }.toMap
  }

  private val from = "FROM t JOIN UNNEST(t.items) AS item"

  private val nestedOfNested =
    "FROM blogs AS blogs JOIN UNNEST(blogs.comments) AS c JOIN UNNEST(c.replies) AS r"

  "An UNNEST projection without LIMIT" should "ask for 100 elements per parent" in {
    innerHitsSizes(s"SELECT t.id, item.product $from") shouldBe Map("item" -> Some(100))
  }

  it should "ask for 100 through the nested filter that carries its inner hits" in {
    innerHitsSizes(s"SELECT t.id, item.product $from WHERE item.price > 2") shouldBe
    Map("item" -> Some(100))
  }

  it should "ask for 100 at every level of a nested UNNEST, the parent level included" in {
    innerHitsSizes(s"SELECT c.author, r.reply_author $nestedOfNested") shouldBe
    Map("c" -> Some(100), "r" -> Some(100))
    // only the inner level is projected: the outer level's inner hits carry it, so it is sized too
    innerHitsSizes(s"SELECT blogs.id, r.reply_author $nestedOfNested") shouldBe
    Map("c" -> Some(100), "r" -> Some(100))
  }

  it should "ask for the index's max_inner_result_window when it is lower" in {
    val low = Some(schemaOf("t", Some(4)))
    innerHitsSizes(s"SELECT t.id, item.product $from", low) shouldBe Map("item" -> Some(4))
    innerHitsSizes(s"SELECT t.id, item.product $from WHERE item.price > 2", low) shouldBe
    Map("item" -> Some(4))
    innerHitsSizes(
      s"SELECT c.author, r.reply_author $nestedOfNested",
      Some(schemaOf("blogs", Some(4)))
    ) shouldBe
    Map("c" -> Some(4), "r" -> Some(4))
  }

  it should "ask for 100 when the index's setting is higher or absent" in {
    innerHitsSizes(s"SELECT t.id, item.product $from", Some(schemaOf("t", Some(250)))) shouldBe
    Map("item" -> Some(100))
    innerHitsSizes(s"SELECT t.id, item.product $from", Some(schemaOf("t", None))) shouldBe
    Map("item" -> Some(100))
  }

  "An UNNEST projection with a LIMIT" should "keep asking for LIMIT elements" in {
    innerHitsSizes(s"SELECT t.id, item.product $from LIMIT 10") shouldBe Map("item" -> Some(10))
    innerHitsSizes(s"SELECT t.id, item.product $from WHERE item.price > 2 LIMIT 5") shouldBe
    Map("item" -> Some(5))
    innerHitsSizes(s"SELECT c.author, r.reply_author $nestedOfNested LIMIT 7") shouldBe
    Map("c" -> Some(7), "r" -> Some(7))
  }

  it should "keep asking for LIMIT elements whatever the index's setting" in {
    innerHitsSizes(
      s"SELECT t.id, item.product $from LIMIT 10",
      Some(schemaOf("t", Some(4)))
    ) shouldBe
    Map("item" -> Some(10))
  }

  "A nested filter or aggregate whose elements are not rows" should "keep the default size" in {
    innerHitsSizes(s"SELECT t.id $from WHERE item.price > 2") shouldBe Map("item" -> None)
    innerHitsSizes(s"SELECT COUNT(item.id) AS n $from WHERE item.price > 2") shouldBe
    Map("item" -> None)
    innerHitsSizes(s"SELECT t.id $from WHERE item.price > 2", Some(schemaOf("t", Some(4)))) shouldBe
    Map("item" -> None)
  }
}
