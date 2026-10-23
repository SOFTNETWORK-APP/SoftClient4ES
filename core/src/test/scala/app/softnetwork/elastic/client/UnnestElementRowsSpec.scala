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

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SingleSearch, UnnestScope}
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `… JOIN UNNEST(t.items) AS i WHERE f(i.x)` answers one row per matching ELEMENT, as DuckDB does:
  * the inner hits the WHERE's nested query returns are exploded into rows even when no column of
  * `i` is projected. The same statements are executed on every major by the shared testkit trait
  * `GroupedAggregateSemanticsSpec`.
  */
class UnnestElementRowsSpec extends AnyFlatSpec with Matchers {

  implicit private val context: ConversionContext = NativeContext

  private val schema: Schema =
    Index(
      "t",
      """{"t": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"},
        | "items": {"type": "nested", "properties": {"sku": {"type": "keyword"},
        | "at": {"type": "date"}}}}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private val filtered =
    "SELECT t.id FROM t AS t JOIN UNNEST(t.items) AS i WHERE YEAR(i.at) = 2024"

  "the row explosion of a WHERE over an UNNEST element" should
  "explode the elements its nested query matched, placing no column" in {
    UnnestScope.rowExplosion(statement(filtered)) shouldBe Map("i" -> Seq.empty)
    UnnestScope.rowExplosion(
      statement("SELECT t.id FROM t AS t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'")
    ) shouldBe Map("i" -> Seq.empty)
    // the controls: a projected element column keeps its mapping, an aggregation is left alone
    val projected =
      statement("SELECT t.id, i.sku FROM t AS t JOIN UNNEST(t.items) AS i WHERE YEAR(i.at) = 2024")
    UnnestScope.rowExplosion(projected) shouldBe projected.nestedHitsMappings
    UnnestScope.rowExplosion(
      statement("SELECT COUNT(*) AS c FROM t AS t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'")
    ) shouldBe Map.empty
  }

  it should "answer one row per matching element" in {
    val single = statement(filtered)
    val response =
      """{"hits": {"total": {"value": 2, "relation": "eq"}, "hits": [
        | {"_id": "a2", "_source": {"id": "a2"}, "inner_hits": {"i": {"hits": {"hits": [
        |   {"_source": {"sku": "s1", "at": "2024-02-10"}},
        |   {"_source": {"sku": "s1", "at": "2024-03-01"}}]}}}},
        | {"_id": "a3", "_source": {"id": "a3"}, "inner_hits": {"i": {"hits": {"hits": [
        |   {"_source": {"sku": "s3", "at": "2024-06-01"}}]}}}}
        |]}}""".stripMargin
    val rows = ElasticConversion
      .parseResponse(
        response,
        single.fieldAliases,
        collection.immutable.ListMap.empty,
        Seq("id"),
        UnnestScope.rowExplosion(single)
      )
      .get
    rows.map(_("id")) shouldBe Seq("a2", "a2", "a3")
    // and the inner hits are not an extra column
    rows.foreach(_.keySet shouldBe Set("id"))
  }
}
