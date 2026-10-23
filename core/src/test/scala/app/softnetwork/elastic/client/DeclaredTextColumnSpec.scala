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
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.LocalTime
import scala.collection.immutable.ListMap

/** A column a statement declares VARCHAR is read as its text, never re-read as a temporal
  * ([[TextColumns]]). The same statement is executed on every major by the shared testkit trait
  * `GroupedAggregateSemanticsSpec`.
  */
class DeclaredTextColumnSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema =
    Index(
      "t",
      """{"t": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"},
        | "ts": {"type": "date"}}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The value of `v` the hit `fields` entry `text` answers for `sql`. */
  private def valueOf(sql: String, text: String): Any = {
    val single = statement(sql)
    implicit val context: ConversionContext = TextColumns.of(single, NativeContext)
    val response =
      s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [
         | {"_id": "a3", "_source": {"id": "a3"}, "fields": {"v": ["$text"]}}]}}""".stripMargin
    ElasticConversion
      .parseResponse(response, single.fieldAliases, ListMap.empty, Seq("id", "v"))
      .get
      .head("v") match {
      case Seq(one) => one
      case other    => other
    }
  }

  "a script column declared VARCHAR" should "answer its text" in {
    valueOf(
      "SELECT id, DATETIME_FORMAT(ts, '%H:%i:%s') AS v FROM t",
      "00:00:00"
    ) shouldBe "00:00:00"
    valueOf("SELECT id, DATETIME_FORMAT(ts, '%Y-%m-%d') AS v FROM t", "2024-07-04") shouldBe
    "2024-07-04"
  }

  it should "leave a column of another type as it is read (the control)" in {
    valueOf("SELECT id, CAST(ts AS TIME) AS v FROM t", "00:00:00") shouldBe LocalTime.of(0, 0)
  }
}
