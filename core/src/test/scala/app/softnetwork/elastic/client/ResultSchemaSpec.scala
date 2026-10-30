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

import app.softnetwork.elastic.client.SetOperationValues.DateReading
import app.softnetwork.elastic.client.result.{
  CsvFormatter,
  JsonFormatter,
  QueryRows,
  QueryStructured,
  ResultRenderer
}
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes, ValueCoercion}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{FromlessSelect, MultiSearch, SingleSearch}
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration

/** SCHEMA-2 (D1, D12, A-13, A-14, A-16) -- a statement's result schema, declared before any row
  * from the types its items have as set-operation positions, and the presenters that read it.
  */
class ResultSchemaSpec extends AnyFlatSpec with Matchers {

  private val mapping: String =
    """{"s2_t": {"aliases": {}, "mappings": {
      | "_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"},
      |  "ts": {"type": "date"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "c_c1": {"type": "date", "format": "yyyy/MM/dd"},
      |  "tsz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"},
      |  "n": {"type": "integer"}, "x": {"type": "double"}, "r": {"type": "float"},
      |  "b": {"type": "boolean"}, "g": {"type": "geo_point"}, "bin": {"type": "binary"},
      |  "nul": {"type": "keyword"}, "txt": {"type": "text"},
      |  "obj": {"properties": {"z": {"type": "keyword"}, "a": {"type": "integer"}}},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}}
      | }}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin

  private val schema: Schema = Index("s2_t", mapping).schema

  private val es8 = DateReading(Some("8.18.3"))

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def schemaOf(sql: String): ResultSchema =
    ResultSchemas.of(statement(sql), es8).getOrElse(fail(s"[$sql] has no schema"))

  private def types(sql: String): Seq[(String, SQLType)] =
    schemaOf(sql).columns.map(c => c.name -> c.sqlType)

  "A statement's result schema" should "type each column as its set-operation position (P1)" in {
    types("SELECT id, ts, c_des, c_c1, n, x, r, b, bin, nul, txt FROM s2_t") shouldBe Seq(
      "id"    -> SQLTypes.Varchar,
      "ts"    -> SQLTypes.Timestamp,
      "c_des" -> SQLTypes.Date,
      "c_c1"  -> SQLTypes.Timestamp,
      "n"     -> SQLTypes.Int,
      "x"     -> SQLTypes.Double,
      "r"     -> SQLTypes.Real,
      "b"     -> SQLTypes.Boolean,
      "bin"   -> SQLTypes.VarBinary,
      "nul"   -> SQLTypes.Varchar,
      "txt"   -> SQLTypes.Varchar
    )
    // an alias names the column, the type stays the source's
    types("SELECT n AS k, CAST(n AS VARCHAR) AS s FROM s2_t") shouldBe Seq(
      "k" -> SQLTypes.Int,
      "s" -> SQLTypes.Varchar
    )
    // nullable is always true (A-16)
    schemaOf("SELECT id, n FROM s2_t").columns.map(_.nullable).distinct shouldBe Seq(true)
  }

  it should "answer a date its format cannot read for sure VARCHAR (AsStored)" in {
    types("SELECT tsz FROM s2_t") shouldBe Seq("tsz" -> SQLTypes.Varchar)
    // ...and every date VARCHAR when the Elasticsearch version is not known
    ResultSchemas
      .of(statement("SELECT ts FROM s2_t"), DateReading(None))
      .get
      .columns
      .map(
        _.sqlType
      ) shouldBe Seq(SQLTypes.Varchar)
  }

  it should "give a STRUCT its fields by name, a point {lat, lon}, a list its element (A-13)" in {
    val obj = schemaOf("SELECT obj, g, items FROM s2_t").columns
    obj.head.sqlType shouldBe SQLTypes.Struct
    obj.head.fields.map(f => f.name -> f.sqlType) shouldBe Seq(
      "a" -> SQLTypes.Int,
      "z" -> SQLTypes.Varchar
    )
    obj(1).sqlType shouldBe SQLTypes.GeoPoint
    obj(1).fields.map(f => f.name -> f.sqlType) shouldBe Seq(
      "lat" -> SQLTypes.Double,
      "lon" -> SQLTypes.Double
    )
    obj(2).sqlType shouldBe SQLTypes.Array(SQLTypes.Struct)
    obj(2).element.map(_.fields.map(_.name)) shouldBe Some(Seq("p"))
  }

  it should "expand * to the mapping's columns in their table order (A-14)" in {
    val star = schemaOf("SELECT * FROM s2_t")
    star.names shouldBe schema.columns.map(_.name)
  }

  it should "be None without a mapping, Some for a FROM-less statement" in {
    val bare = Parser("SELECT id FROM s2_t") match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"$other")
    }
    ResultSchemas.of(bare, es8) shouldBe None
    val fromless = Parser("SELECT 1 AS one, 'a' AS s") match {
      case Right(f: FromlessSelect) => f
      case other                    => fail(s"$other")
    }
    ResultSchemas.of(fromless).columns.map(c => c.name -> c.sqlType) shouldBe Seq(
      "one" -> SQLTypes.Int,
      "s"   -> SQLTypes.Varchar
    )
  }

  it should "type a computed column by its declared type (T0-5)" in {
    // recorded, not asserted beyond being declared: NUMERIC / TEMPORAL cells are value-typed
    val computed = types("SELECT n + 1 AS v, DATE_FORMAT(ts, '%Y') AS y FROM s2_t")
    computed.map(_._1) shouldBe Seq("v", "y")
    computed(1)._2 shouldBe SQLTypes.Varchar
    computed.foreach(c => info(s"${c._1}: ${c._2.typeId}"))
  }

  private def union(sql: String): MultiSearch = Parser(sql) match {
    case Right(m: MultiSearch) => m.copy(requests = m.requests.map(_.update(Some(schema))))
    case other                 => fail(s"[$sql] expected a MultiSearch, got $other")
  }

  "A UNION ALL's schema" should "be typed as its converters type it, named by its first branch (D12)" in {
    ResultSchemas
      .of(union("SELECT n AS k FROM s2_t UNION ALL SELECT x AS k FROM s2_t"), es8)
      .map(
        _.columns.map(c => c.name -> c.sqlType)
      ) shouldBe Some(Seq("k" -> SQLTypes.Double))
    // a date one branch reads as stored: the column is VARCHAR, in the schema and the converters
    val stored = union("SELECT tsz AS k FROM s2_t UNION ALL SELECT ts AS k FROM s2_t")
    ResultSchemas.of(stored, es8).map(_.columns.map(_.sqlType)) shouldBe Some(Seq(SQLTypes.Varchar))
    val converters = SetOperationValues.converters(stored, Some("8.18.3"))
    converters.forall(_.isDefined) shouldBe true
    converters(1).get.apply(0)(java.time.ZonedDateTime.parse("2024-01-31T10:30:00Z")) shouldBe
    "2024-01-31 10:30:00"
    converters(0).get.apply(0)("2024-01-31 10:30 CET") shouldBe "2024-01-31 10:30 CET"
    // an opaque first branch declares nothing
    ResultSchemas.of(union("SELECT * FROM s2_t UNION ALL SELECT id FROM s2_t"), es8) shouldBe None
  }

  "The presenters" should "take the header from the schema, the rows keeping their key order (A-14)" in {
    val s = ResultSchema(Seq(ResultColumn("a", SQLTypes.Int), ResultColumn("b", SQLTypes.Varchar)))
    val rows = Seq(ListMap[String, Any]("b" -> "x", "a" -> 1))
    CsvFormatter.format(QueryRows(rows, schema = Some(s))).split("\n").head shouldBe "a,b"
    val table = ResultRenderer.renderAscii(QueryRows(rows, schema = Some(s)), Duration.Zero)
    val header = table.split("\n")(1)
    header.indexOf("a") should be < header.indexOf("b")
    // no schema: the first row's keys, as before
    CsvFormatter.format(QueryRows(rows)).split("\n").head shouldBe "b,a"
    // a QueryStructured carries its response's schema (A-18)
    QueryStructured(
      ElasticResponse(None, "{}", rows, ListMap.empty, ListMap.empty, Some(s))
    ).schema shouldBe
    Some(s)
  }

  it should "print a VARBINARY as its base64 and a REAL as a number, as 0.24.0 did (D2)" in {
    val bytes = "hello".getBytes("UTF-8")
    ValueCoercion.coerceToString(bytes) shouldBe "aGVsbG8="
    CsvFormatter.format(QueryRows(Seq(ListMap[String, Any]("bin" -> bytes)))) should include(
      "aGVsbG8="
    )
    ResultRenderer.renderAscii(
      QueryRows(Seq(ListMap[String, Any]("bin" -> bytes))),
      Duration.Zero
    ) should include("aGVsbG8=")
    val json = JsonFormatter
      .format(
        QueryRows(Seq(ListMap[String, Any]("r" -> java.lang.Float.valueOf(1.5f), "bin" -> bytes))),
        Duration.Zero
      )
      .replaceAll("\\s", "")
    json should include("\"r\":1.5")
    json should include("\"bin\":\"aGVsbG8=\"")
  }
}
