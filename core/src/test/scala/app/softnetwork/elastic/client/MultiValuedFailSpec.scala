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
import app.softnetwork.elastic.sql.query.{
  AlterColumnField,
  AlterColumnType,
  AlterTable,
  SingleSearch
}
import app.softnetwork.elastic.sql.schema.Schema
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** SCHEMA-6 (D6, A-26, lead Q1) -- several values in a column NOT declared `ARRAY<X>` fail the
  * statement, by default, naming the column, the document, the index it came from and the `ALTER`
  * that declares it a list -- a statement that parses and runs (meta-only,
  * `ArrayDeclarationDiffSpec`). After that `ALTER`, the column reads its values as a list.
  *
  * Reached at both places a scalar reader meets several values: a hit's `_source`
  * (`parseHitsByPlan`) and a top hit of FIRST_VALUE / LAST_VALUE / ARRAY_AGG (`readElement`), where
  * the bare column name escaped before. A list of objects in an `object` column keeps answering its
  * list of struct reads (no in-place remedy exists for it, A-26); a scalar path under it holds the
  * values its elements hold -- Elasticsearch's `obj.a` -- and fails or reads them as any column
  * does. An entity read (a WHERE subquery's inner statement) never goes through the plan (A-24).
  */
class MultiValuedFailSpec extends AnyFlatSpec with Matchers {

  private val version = "8.18.3"

  /** `zs_ext`, created by a raw mapping: no declaration but the mapping's. */
  private val raw: Schema = Index(
    "zs_ext",
    """{"zs_ext": {"aliases": {}, "mappings": {"properties": {
      |  "id": {"type": "keyword"}, "tags": {"type": "keyword"}, "n": {"type": "integer"},
      |  "geo": {"type": "geo_point"}, "ts": {"type": "date"},
      |  "obj": {"properties": {"a": {"type": "integer"}, "b": {"type": "keyword"}}}
      |}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
  ).schema

  private def statement(sql: String, schema: Schema): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def outputs(single: SingleSearch): Seq[String] = {
    val fields = single.select.fieldsWithComputedAliases
    if (fields.size == 1 && fields.head.identifier.identifierName == "*") Seq.empty
    else fields.map(_.outputName)
  }

  private def rows(
    sql: String,
    hits: String,
    schema: Schema = raw,
    context: SingleSearch => ConversionContext = s =>
      SchemaColumns.of(s, NativeContext, Some(version))
  ): Seq[ListMap[String, Any]] = {
    val single = statement(sql, schema)
    val aggregations = ListMap(single.sqlAggregations.toSeq.map { case (k, v) =>
      k -> sqlAggregationToClientAggregation(v)
    }: _*)
    ElasticConversion
      .parseResponse(
        s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [$hits]}}""",
        single.fieldAliases,
        aggregations,
        outputs(single)
      )(context(single))
      .get
  }

  private def failure(body: => Any): String = {
    val e = intercept[Exception](body)
    Option(e.getCause).fold(e.getMessage)(_.getMessage)
  }

  /** The ALTER TABLE statement a failure names, re-parsed. */
  private def remedy(message: String): AlterTable = {
    val at = message.indexOf("ALTER TABLE")
    at should be >= 0
    Parser(message.substring(at)) match {
      case Right(alter: AlterTable) => alter
      case other                    => fail(s"the remedy of [$message] does not parse: $other")
    }
  }

  private def altered(schema: Schema, alter: AlterTable): Schema = schema.merge(alter.statements)

  private val doc = """"_index": "zs_ext", "_id": "d1""""

  behavior of "several values in a scalar column (D6)"

  it should "fail by default, naming the column, the document, its index and a parseable ALTER" in {
    HitReading.Default shouldBe MultiValued.Fail
    val message = failure(
      rows("SELECT id, tags FROM zs_ext", s"""{$doc, "_source": {"id": "1", "tags": ["a", "b"]}}""")
    )
    message should include("Column tags")
    message should include("document d1")
    message should include("index zs_ext")
    val alter = remedy(message)
    alter shouldBe AlterTable(
      "zs_ext",
      ifExists = false,
      List(AlterColumnType("tags", SQLTypes.Array(SQLTypes.Keyword)))
    )
    alter.sql should include("SET DATA TYPE ARRAY<KEYWORD>")
  }

  it should "read the list once the ALTER has declared it, a scalar as a list of one, NULL as NULL" in {
    val alter = remedy(
      failure(rows("SELECT id, tags FROM zs_ext", s"""{$doc, "_source": {"tags": ["a", "b"]}}"""))
    )
    val declared = altered(raw, alter)
    declared.find("tags").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Keyword))
    val read = rows(
      "SELECT id, tags FROM zs_ext ORDER BY id",
      """{"_id": "1", "_source": {"id": "1", "tags": ["a", "b"]}},
        |{"_id": "2", "_source": {"id": "2", "tags": "c"}},
        |{"_id": "3", "_source": {"id": "3"}}""".stripMargin,
      declared
    )
    read.map(_("tags")) shouldBe Seq(List("a", "b"), List("c"), null)
  }

  it should "name the index the hit came from, not the statement's pattern" in {
    val message = failure(
      rows(
        "SELECT id, n FROM zs_*",
        """{"_index": "zs_ext_2024", "_id": "d9", "_source": {"n": [1, 2]}}"""
      )
    )
    message should include("index zs_ext_2024")
    remedy(message).table shouldBe "zs_ext_2024"
    remedy(message).statements shouldBe List(AlterColumnType("n", SQLTypes.Array(SQLTypes.Int)))
  }

  it should "name the declared TIMESTAMP of an undeclared date and the point of a geo column" in {
    remedy(
      failure(
        rows(
          "SELECT ts FROM zs_ext",
          s"""{$doc, "_source": {"ts": ["2024-01-01", "2024-01-02"]}}"""
        )
      )
    ).statements shouldBe List(AlterColumnType("ts", SQLTypes.Array(SQLTypes.Timestamp)))
    val geo = remedy(
      failure(
        rows("SELECT geo FROM zs_ext", s"""{$doc, "_source": {"geo": [[1.0, 2.0], [3.0, 4.0]]}}""")
      )
    )
    geo.statements shouldBe List(AlterColumnType("geo", SQLTypes.Array(SQLTypes.GeoPoint)))
    // a [lon, lat] pair is ONE point, before and after the ALTER
    rows("SELECT geo FROM zs_ext", s"""{$doc, "_source": {"geo": [2.0, 1.0]}}""")
      .head("geo") shouldBe ListMap("lat" -> 1.0, "lon" -> 2.0)
    val points = rows(
      "SELECT geo FROM zs_ext",
      s"""{$doc, "_source": {"geo": [2.0, 1.0]}}, {"_id": "d2", "_source": {"geo": [[2.0, 1.0], [4.0, 3.0]]}}""",
      altered(raw, geo)
    )
    points.map(_("geo")) shouldBe Seq(
      List(ListMap("lat" -> 1.0, "lon" -> 2.0)),
      List(ListMap("lat" -> 1.0, "lon" -> 2.0), ListMap("lat" -> 3.0, "lon" -> 4.0))
    )
  }

  it should "fail a several-valued top hit of FIRST_VALUE / ARRAY_AGG with the same message" in {
    def global(sql: String, aggregations: String): Seq[ListMap[String, Any]] = {
      val single = statement(sql, raw)
      val clients = ListMap(single.sqlAggregations.toSeq.map { case (k, v) =>
        k -> sqlAggregationToClientAggregation(v)
      }: _*)
      ElasticConversion
        .parseResponse(
          s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": []},
             | "aggregations": {$aggregations}}""".stripMargin,
          single.fieldAliases,
          clients,
          outputs(single)
        )(SchemaColumns.of(single, NativeContext, Some(version)))
        .get
    }
    val first = failure(
      global(
        "SELECT FIRST(tags) AS f FROM zs_ext",
        """"f": {"hits": {"hits": [{"_index": "zs_ext", "_id": "t1", "_source": {"tags": ["a", "b"]}}]}}"""
      )
    )
    first should include("Column tags")
    first should include("document t1")
    first should include("index zs_ext")
    remedy(first).statements shouldBe List(
      AlterColumnType("tags", SQLTypes.Array(SQLTypes.Keyword))
    )
    val agg = failure(
      global(
        "SELECT ARRAY_AGG(tags) AS a FROM zs_ext",
        """"a": {"hits": {"hits": [{"_index": "zs_ext", "_id": "t2", "_source": {"tags": "x"}},
          |  {"_index": "zs_ext", "_id": "t3", "_source": {"tags": ["a", "b"]}}]}}""".stripMargin
      )
    )
    agg should include("document t3")
    remedy(agg).statements shouldBe List(AlterColumnType("tags", SQLTypes.Array(SQLTypes.Keyword)))
  }

  it should "keep an entity read (a WHERE subquery's inner statement) as read (A-24)" in {
    rows(
      "SELECT tags FROM zs_ext",
      s"""{$doc, "_source": {"tags": ["a", "b"]}}""",
      context = _ => EntityContext
    ).head("tags") shouldBe List("a", "b")
  }

  behavior of "a list of objects in an object column (A-26)"

  private val objects =
    s"""{$doc, "_source": {"id": "1", "obj": [{"a": 5, "b": "x"}, {"a": 50}]}}"""

  it should "answer the STRUCT column's list of struct reads, as main did" in {
    rows("SELECT id, obj FROM zs_ext", objects).head("obj") shouldBe List(
      ListMap("a" -> 5, "b" -> "x"),
      ListMap("a" -> 50)
    )
  }

  it should "fail a scalar path under it, naming SET FIELD, then read the elements' values" in {
    val message = failure(rows("SELECT id, obj.a FROM zs_ext", objects))
    message should include("Column obj.a")
    val alter = remedy(message)
    alter.statements match {
      case List(AlterColumnField("obj", field, false)) =>
        field.name shouldBe "a"
        field.dataType shouldBe SQLTypes.Array(SQLTypes.Int)
      case other => fail(s"remedy $other")
    }
    val declared = altered(raw, alter)
    rows("SELECT id, obj.a FROM zs_ext", objects, declared).head("obj.a") shouldBe List(5, 50)
    // beside the parent read whole, in each spelling
    val both = rows("SELECT id, obj, obj.a FROM zs_ext", objects, declared).head
    both("obj.a") shouldBe List(5, 50)
    // the parent's elements read `a` by its declared type too: a list of one
    both("obj") shouldBe List(ListMap("a" -> List(5), "b" -> "x"), ListMap("a" -> List(50)))
    rows("SELECT *, obj.a FROM zs_ext", objects, declared).head("obj.a") shouldBe List(5, 50)
    failure(rows("SELECT id, obj, obj.a FROM zs_ext", objects)) should include("Column obj.a")
  }

  it should "read one value under a list of objects as that value, none as NULL, never NULL by the list" in {
    rows(
      "SELECT id, obj.a FROM zs_ext",
      s"""{$doc, "_source": {"obj": [{"a": 5}, {"b": "x"}]}}"""
    ).head("obj.a") shouldBe 5
    rows(
      "SELECT id, obj.a FROM zs_ext",
      s"""{$doc, "_source": {"obj": [{"b": "x"}]}}"""
    ).head("obj.a") shouldBe (null: Any)
    // an element's own list is flattened, as Elasticsearch indexes `obj.a`
    val declared = altered(
      raw,
      remedy(failure(rows("SELECT id, obj.a FROM zs_ext", objects)))
    )
    rows(
      "SELECT id, obj.a FROM zs_ext",
      s"""{$doc, "_source": {"obj": [{"a": [1, 2]}, {"a": 3}]}}""",
      declared
    ).head("obj.a") shouldBe List(1, 2, 3)
  }
}
