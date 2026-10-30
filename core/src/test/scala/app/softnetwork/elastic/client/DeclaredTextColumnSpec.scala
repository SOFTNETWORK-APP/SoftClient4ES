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

import java.time.{LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** A column a statement declares VARCHAR is read as its text, never re-read as a temporal
  * ([[SchemaColumns]]). The same statement is executed on every major by the shared testkit trait
  * `GroupedAggregateSemanticsSpec`.
  */
class DeclaredTextColumnSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema =
    Index(
      "t",
      """{"t": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"},
        | "ts": {"type": "date"}, "d": {"type": "date"}, "kd": {"type": "keyword"}}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The value of `v` the hit `fields` entry `text` answers for `sql`. */
  private def valueOf(sql: String, text: String): Any = {
    val single = statement(sql)
    implicit val context: ConversionContext =
      SchemaColumns.of(single, NativeContext, Some("8.18.3"))
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

  /** The row `sql` answers for one hit whose `_source` is `source` and whose `fields` is `fields`.
    */
  private def rowOf(sql: String, source: String, fields: String = "{}"): ListMap[String, Any] = {
    val single = statement(sql)
    implicit val context: ConversionContext =
      SchemaColumns.of(single, NativeContext, Some("8.18.3"))
    val response =
      s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [
         | {"_id": "a1", "_source": $source, "fields": $fields}]}}""".stripMargin
    val names =
      if (single.select.fields.exists(_.identifier.identifierName == "*")) Seq.empty
      else single.select.fieldsWithComputedAliases.map(_.outputName)
    ElasticConversion
      .parseResponse(response, single.fieldAliases, ListMap.empty, names)
      .get
      .head
      .map {
        case (k, Seq(one)) => k -> one
        case other         => other
      }
  }

  "a computed VARCHAR column" should "leave the column it reads in that column's own type" in {
    // DuckDB: ts TIMESTAMPTZ, y VARCHAR '2025'
    val formatted = rowOf(
      "SELECT id, ts, DATE_FORMAT(ts, '%Y') AS y FROM t",
      """{"id": "a1", "ts": "2025-03-31T10:00:00Z"}""",
      """{"y": ["2025"]}"""
    )
    formatted("ts") shouldBe a[ZonedDateTime]
    formatted("y") shouldBe "2025"
    // DuckDB: d DATE, s VARCHAR
    val cast = rowOf(
      "SELECT id, d, CAST(d AS VARCHAR) AS s FROM t",
      """{"id": "a1", "d": "2024-01-03"}""",
      """{"s": ["2024-01-03"]}"""
    )
    // SCHEMA-2 (D2): `d`, a `date` field declared by no `_meta`, is a TIMESTAMP -- read at UTC
    cast("d") shouldBe ZonedDateTime.of(2024, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC)
    cast("s") shouldBe a[String]
  }

  "a bare VARCHAR column" should "answer its text, selected by name or by *" in {
    val source = """{"id": "a1", "d": "2024-01-03", "kd": "2025-03-31T10:00:00"}"""
    rowOf("SELECT id, kd FROM t", source)("kd") shouldBe "2025-03-31T10:00:00"
    rowOf("SELECT id, kd AS k2 FROM t", source)("k2") shouldBe "2025-03-31T10:00:00"
    val star = rowOf("SELECT * FROM t", source)
    star("kd") shouldBe "2025-03-31T10:00:00"
    // the control: a date column of the same row keeps its type (SCHEMA-2, D2: a TIMESTAMP)
    star("d") shouldBe ZonedDateTime.of(2024, 1, 3, 0, 0, 0, 0, ZoneOffset.UTC)
    // and without a schema no schema is declared (SCHEMA-2, D10: rows as read)
    val context: ConversionContext = Parser("SELECT * FROM t") match {
      case Right(s: SingleSearch) => SchemaColumns.of(s, NativeContext)
      case other                  => fail(s"$other")
    }
    SchemaColumns.schemaOf(context) shouldBe None
  }
}
