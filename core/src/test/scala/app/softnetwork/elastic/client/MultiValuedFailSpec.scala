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

import app.softnetwork.elastic.schema.{FieldCaps, Index}
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

  it should "fail the STRUCT column's cell of several objects, naming the in-place ALTER, then read its list (SCHEMA-17 A-168; A-26 read it as read)" in {
    val message = failure(rows("SELECT id, obj FROM zs_ext", objects))
    message should include("Column obj")
    message should include("SET DATA TYPE ARRAY<STRUCT>")
    val declared = altered(raw, remedy(message))
    // declared in place: the field stays an object (A-166)
    declared.find("obj").map(_.mappingType) shouldBe Some("object")
    rows("SELECT id, obj FROM zs_ext", objects, declared).head("obj") shouldBe List(
      ListMap("a" -> 5, "b" -> "x"),
      ListMap("a" -> 50)
    )
  }

  it should "fail a scalar path under it naming the list's in-place ALTER, then read the objects' values as a list (SCHEMA-17 A-181)" in {
    // several objects hold the path: the list is the cause, not the path's own field
    val message = failure(rows("SELECT id, obj.a FROM zs_ext", objects))
    message should include("Column obj holds several values")
    message should not include "SET FIELD"
    val alter = remedy(message)
    alter.statements shouldBe List(AlterColumnType("obj", SQLTypes.Array(SQLTypes.Struct)))
    val declared = altered(raw, alter)
    declared.find("obj").map(_.mappingType) shouldBe Some("object")
    // a path under the list of objects is a list: its objects' values, one object a list of one
    rows("SELECT id, obj.a FROM zs_ext", objects, declared).head("obj.a") shouldBe List(5, 50)
    val one = s"""{$doc, "_source": {"id": "1", "obj": {"a": 5, "b": "x"}}}"""
    rows("SELECT id, obj.a FROM zs_ext", one, declared).head("obj.a") shouldBe List(5)
    // beside the parent read whole, in each spelling; the objects keep `a` one value each
    val both = rows("SELECT id, obj, obj.a FROM zs_ext", objects, declared).head
    both("obj.a") shouldBe List(5, 50)
    both("obj") shouldBe List(ListMap("a" -> 5, "b" -> "x"), ListMap("a" -> 50))
    rows("SELECT *, obj.a FROM zs_ext", objects, declared).head("obj.a") shouldBe List(5, 50)
    rows("SELECT id, obj, obj.a FROM zs_ext", one, declared).head("obj.a") shouldBe List(5)
    rows("SELECT *, obj.a FROM zs_ext", one, declared).head("obj.a") shouldBe List(5)
    // undeclared: the parent or its path names itself first
    failure(rows("SELECT id, obj, obj.a FROM zs_ext", objects)) should include("Column obj")
  }

  it should "name the path's own SET FIELD where one object's field holds several values (SCHEMA-17 A-181)" in {
    Seq(
      s"""{$doc, "_source": {"obj": {"a": [1, 2]}}}""",
      s"""{$doc, "_source": {"obj": [{"a": [1, 2]}]}}""",
      s"""{$doc, "_source": {"obj": [{"a": [1, 2]}, {"b": "x"}]}}"""
    ).foreach { hit =>
      withClue(s"$hit: ") {
        val message = failure(rows("SELECT id, obj.a FROM zs_ext", hit))
        message should include("Column obj.a holds several values")
        remedy(message).statements match {
          case List(AlterColumnField("obj", field, false)) =>
            field.name shouldBe "a"
            field.dataType shouldBe SQLTypes.Array(SQLTypes.Int)
          case other => fail(s"remedy $other")
        }
      }
    }
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

  behavior of "a path under a nested field, or deep under lists of objects (SCHEMA-17 A-181)"

  /** `items` a nested field, `o` an object holding the object `p`. */
  private val deep: Schema = Index(
    "zs_deep",
    """{"zs_deep": {"aliases": {}, "mappings": {"properties": {
      |  "id": {"type": "keyword"},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}},
      |  "o": {"properties": {"p": {"properties": {"a": {"type": "integer"}}}}}
      |}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
  ).schema

  it should "read a path under a nested field as its objects' values, a list" in {
    def p(source: String): Any =
      rows("SELECT id, items.p FROM zs_deep", s"""{$doc, "_source": $source}""", deep).head
        .getOrElse("items.p", null)
    p("""{"items": [{"p": 1}]}""") shouldBe List(1)
    p("""{"items": [{"p": 2}, {"p": 3}]}""") shouldBe List(2, 3)
    p("""{"items": []}""") shouldBe (null: Any)
    p("""{"id": "x"}""") shouldBe (null: Any)
  }

  it should "name the outermost list of objects whose objects hold a deep path's values" in {
    // `o` a list: each of its objects holds one `o.p.a`
    val outer = failure(
      rows(
        "SELECT id, o.p.a FROM zs_deep",
        s"""{$doc, "_source": {"o": [{"p": {"a": 1}}, {"p": {"a": 2}}]}}""",
        deep
      )
    )
    outer should include("Column o holds several values")
    remedy(outer).statements shouldBe List(AlterColumnType("o", SQLTypes.Array(SQLTypes.Struct)))
    // `o` one object, its `p` a list: `p` declared a list of objects in place, as a sub-field
    val inner = failure(
      rows(
        "SELECT id, o.p.a FROM zs_deep",
        s"""{$doc, "_source": {"o": {"p": [{"a": 1}, {"a": 2}]}}}""",
        deep
      )
    )
    inner should include("Column o.p holds several values")
    remedy(inner).statements match {
      case List(AlterColumnField("o", field, false)) =>
        field.name shouldBe "p"
        field.dataType shouldBe SQLTypes.Array(SQLTypes.Struct)
        field.mappingType shouldBe "object"
      case other => fail(s"remedy $other")
    }
  }

  behavior of "a point, and a path beside its own selected prefix, under a list of objects (SCHEMA-17 A-181)"

  /** `obj` an object and `items` a nested field, each holding a point and an object. */
  private val geo: Schema = Index(
    "zs_geo",
    """{"zs_geo": {"aliases": {}, "mappings": {"properties": {
      |  "id": {"type": "keyword"},
      |  "obj": {"properties": {"loc": {"type": "geo_point"},
      |    "in": {"properties": {"q": {"type": "integer"}}}}},
      |  "items": {"type": "nested", "properties": {"loc": {"type": "geo_point"},
      |    "o": {"properties": {"x": {"type": "integer"}}}}}
      |}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
  ).schema

  /** `obj` declared a list of objects in place (A-166). */
  private lazy val geoListed: Schema =
    Parser("ALTER TABLE zs_geo ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>") match {
      case Right(alter: AlterTable) => altered(geo, alter)
      case other                    => fail(s"the ALTER does not parse: $other")
    }

  private def geoRow(sql: String, source: String, schema: Schema): ListMap[String, Any] =
    rows(sql, s"""{"_index": "zs_geo", "_id": "g1", "_source": $source}""", schema).head

  private def point(lat: Double, lon: Double): ListMap[String, Any] =
    ListMap("lat" -> lat, "lon" -> lon)

  it should "read a point stored as [lon, lat] as ONE point, never its numbers, in every spelling" in {
    val p12 = point(2.0, 1.0)
    val p34 = point(4.0, 3.0)
    val p56 = point(6.0, 5.0)
    val several = "several points in one element"
    // (label, the parent's value, the points its path holds)
    val cases = Seq(
      ("two points", """[{"loc": [1.0, 2.0]}, {"loc": [3.0, 4.0]}]""", List(p12, p34)),
      ("one point", """[{"loc": [1.0, 2.0]}]""", List(p12)),
      ("one object", """{"loc": [1.0, 2.0]}""", List(p12)),
      (several, """[{"loc": [[1.0, 2.0], [3.0, 4.0]]}]""", List(p12, p34)),
      (
        "each spelling",
        """[{"loc": "2.0,1.0"}, {"loc": {"lat": 4.0, "lon": 3.0}}, {"loc": [5.0, 6.0]}]""",
        List(p12, p34, p56)
      )
    )
    for {
      parent                   <- Seq("items", "obj")
      (label, value, expected) <- cases
      // the parent read whole reads one element's own field one value: several there fail it
      sql <- s"SELECT id, $parent.loc FROM zs_geo" +: (
        if (label == several) Nil
        else
          Seq(s"SELECT id, $parent, $parent.loc FROM zs_geo", s"SELECT *, $parent.loc FROM zs_geo")
      )
    } withClue(s"[$sql] $label: ") {
      geoRow(sql, s"""{"id": "g", "$parent": $value}""", geoListed)(
        s"$parent.loc"
      ) shouldBe expected
    }
  }

  it should "read a point field declared a list under an undeclared list of objects point by point" in {
    // one object's field holding two points names its own SET FIELD (SCHEMA-6)
    val message = failure(
      geoRow(
        "SELECT id, obj.loc FROM zs_geo",
        """{"obj": {"loc": [[1.0, 2.0], [3.0, 4.0]]}}""",
        geo
      )
    )
    message should include("Column obj.loc holds several values")
    val alter = remedy(message)
    alter.statements match {
      case List(AlterColumnField("obj", field, false)) =>
        field.name shouldBe "loc"
        field.dataType shouldBe SQLTypes.Array(SQLTypes.GeoPoint)
      case other => fail(s"remedy $other")
    }
    val declared = altered(geo, alter)
    def loc(value: String): Any =
      geoRow("SELECT id, obj.loc FROM zs_geo", s"""{"obj": $value}""", declared)("obj.loc")
    loc("""[{"loc": [1.0, 2.0]}, {"loc": [3.0, 4.0]}]""") shouldBe List(
      point(2.0, 1.0),
      point(4.0, 3.0)
    )
    loc("""[{"loc": [1.0, 2.0]}]""") shouldBe List(point(2.0, 1.0))
    loc("""{"loc": [[1.0, 2.0], [3.0, 4.0]]}""") shouldBe List(point(2.0, 1.0), point(4.0, 3.0))
  }

  it should "read a path beside its own selected prefix as its objects' values, the prefix as its objects" in {
    // (label, the parent's value, the prefix's objects, the path's values)
    def cases(child: String, leaf: String): Seq[(String, String, Any, Any)] = Seq(
      ("one object", s"""{"$child": {"$leaf": 1}}""", List(ListMap(leaf -> 1)), List(1)),
      ("a list of one", s"""[{"$child": {"$leaf": 1}}]""", List(ListMap(leaf -> 1)), List(1)),
      (
        "two objects",
        s"""[{"$child": {"$leaf": 2}}, {"$child": {"$leaf": 3}}]""",
        List(ListMap(leaf -> 2), ListMap(leaf -> 3)),
        List(2, 3)
      ),
      ("none", """[{"loc": [1.0, 2.0]}]""", null, null)
    )
    for {
      (parent, child, leaf)           <- Seq(("obj", "in", "q"), ("items", "o", "x"))
      (label, value, objects, values) <- cases(child, leaf)
      prefix = s"$parent.$child"
      path = s"$prefix.$leaf"
      sql <- Seq(
        s"SELECT id, $prefix, $path FROM zs_geo",
        s"SELECT id, $parent, $prefix, $path FROM zs_geo",
        s"SELECT id, $path FROM zs_geo"
      )
    } withClue(s"[$sql] $label: ") {
      val row = geoRow(sql, s"""{"id": "g", "$parent": $value}""", geoListed)
      row.getOrElse(path, null) shouldBe values
      if (sql.contains(s"$prefix,")) row.getOrElse(prefix, null) shouldBe objects
    }
    // aliased: the same values under the aliases
    val aliased = geoRow(
      "SELECT id, obj.in AS c1, obj.in.q AS c2 FROM zs_geo",
      """{"obj": {"in": {"q": 1}}}""",
      geoListed
    )
    aliased.getOrElse("c1", null) shouldBe List(ListMap("q" -> 1))
    aliased.getOrElse("c2", null) shouldBe List(1)
    // not under a list of objects: one object is its struct, its path one value, as before
    val one =
      geoRow("SELECT id, obj.in, obj.in.q FROM zs_geo", """{"obj": {"in": {"q": 1}}}""", geo)
    one("obj.in") shouldBe ListMap("q" -> 1)
    one("obj.in.q") shouldBe 1
  }

  behavior of "a read-only user's leaf no index declares (SCHEMA-17 fix R-150, A-183, A-184)"

  private val mapper = new com.fasterxml.jackson.databind.ObjectMapper()

  /** A `_field_caps` entry of one Elasticsearch type, with core's `sql_type` when declared. */
  private def caps(tpe: String, declared: Option[String] = None): String = {
    val meta = declared.fold("")(d => ", \"meta\": {\"sql_type\": [\"" + d + "\"]}")
    s"""{"$tpe": {"type": "$tpe", "metadata_field": false$meta}}"""
  }

  /** The table a read-only user is typed by, built as the client builds it
    * (`IndicesApi.fetchFieldCapsSchema`): the answer read, its members merged, `FieldCaps.table`.
    */
  private def readOnly(source: String, fieldCaps: String): Schema = {
    val answer = FieldCaps.read(source, mapper.readTree(fieldCaps))
    FieldCaps.table(
      answer,
      app.softnetwork.elastic.sql.schema
        .mergedSchema(
          source,
          answer.members.map { case (name, document) => name -> Index(name, document).schema }
        )
        ._1
    )
  }

  /** `zs_ro`, created outside core: no field carries core's meta. */
  private lazy val undeclared: Schema = readOnly(
    "zs_ro",
    s"""{"indices": ["zs_ro"], "fields": {
       | "id": ${caps("keyword")}, "n": ${caps("integer")}, "g": ${caps("geo_point")},
       | "ts": ${caps("date")}, "obj": ${caps("object")}, "obj.a": ${caps("integer")},
       | "items": ${caps("nested")}, "items.p": ${caps("integer")}
       |}}""".stripMargin
  )

  /** `zs_ro` created by core (each leaf's meta), `m` and `obj.b` added later outside it. */
  private lazy val declared: Schema = readOnly(
    "zs_ro",
    s"""{"indices": ["zs_ro"], "fields": {
       | "id": ${caps("keyword", Some("KEYWORD"))}, "n": ${caps("integer", Some("INT"))},
       | "m": ${caps("integer")}, "g": ${caps("geo_point", Some("GEO_POINT"))},
       | "obj": ${caps("object")}, "obj.a": ${caps("integer", Some("INT"))},
       | "obj.b": ${caps("integer")},
       | "items": ${caps("nested")}, "items.p": ${caps("integer", Some("INT"))}
       |}}""".stripMargin
  )

  /** Two indices: `n` declared a scalar (by both, or one -- Elasticsearch's merged `meta` cannot
    * tell), `l` declared a list by one, `u` by none.
    */
  private lazy val several: Schema = readOnly(
    "zs_ra,zs_rb",
    s"""{"indices": ["zs_ra", "zs_rb"], "fields": {
       | "id": ${caps("keyword")}, "n": ${caps("integer", Some("INT"))},
       | "l": ${caps("integer", Some("ARRAY<INT>"))}, "u": ${caps("integer")}
       |}}""".stripMargin
  )

  private def roRow(
    sql: String,
    source: String,
    schema: Schema,
    on: String = version
  ): ListMap[String, Any] =
    rows(
      sql,
      s"""{"_index": "zs_ro", "_id": "r1", "_source": $source}""",
      schema,
      s => SchemaColumns.of(s, NativeContext, Some(on))
    ).head

  private def classes(value: Any): Seq[Class[_]] = value match {
    case list: Seq[_] => list.map(_.getClass)
    case other        => fail(s"not a list: $other")
  }

  it should "mark every leaf no index declares, never an object, and nothing on another table" in {
    FieldCaps.undeclared(undeclared) shouldBe Set("id", "n", "g", "ts", "obj.a", "items.p")
    FieldCaps.undeclared(declared) shouldBe Set("m", "obj.b")
    // over several indices only a list declaration declares (A-179)
    FieldCaps.undeclared(several) shouldBe Set("id", "n", "u")
    several.find("l").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Int))
    several.find("n").map(_.dataType) shouldBe Some(SQLTypes.Int)
    // a mapping reader's table, and a table marked `field_caps` another way, carry none
    FieldCaps.undeclared(raw) shouldBe empty
    FieldCaps.undeclared(
      raw.copy(tableType = app.softnetwork.elastic.sql.schema.TableType.FieldCapabilities)
    ) shouldBe empty
  }

  it should "answer a leaf's own several values as stored, each by the leaf's type (AC1)" in {
    val n = roRow("SELECT id, n FROM zs_ro", """{"id": "r1", "n": [1, 2]}""", undeclared)("n")
    n shouldBe List(1, 2)
    classes(n) shouldBe Seq(classOf[java.lang.Integer], classOf[java.lang.Integer])
    roRow("SELECT id, n FROM zs_ro", """{"id": "r1", "n": 3}""", undeclared)("n") shouldBe 3
    roRow("SELECT id, g FROM zs_ro", """{"g": [[1.0, 2.0], [3.0, 4.0]]}""", undeclared)(
      "g"
    ) shouldBe List(point(2.0, 1.0), point(4.0, 3.0))
    roRow(
      "SELECT id, ts FROM zs_ro",
      """{"ts": ["2024-01-31T10:30:00Z", "2024-02-01T00:00:00Z"]}""",
      undeclared
    )("ts") shouldBe List(
      java.time.ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 0, java.time.ZoneOffset.UTC),
      java.time.ZonedDateTime.of(2024, 2, 1, 0, 0, 0, 0, java.time.ZoneOffset.UTC)
    )
    // a leaf in one object, or in a list holding one object: its own values
    Seq(
      """{"obj": {"a": [1, 2]}}""",
      """{"obj": [{"a": [1, 2]}]}"""
    ).foreach { source =>
      withClue(source) {
        roRow("SELECT id, obj.a FROM zs_ro", source, undeclared)("obj.a") shouldBe List(1, 2)
        // beside its parent read whole, in each spelling
        val both = roRow("SELECT id, obj, obj.a FROM zs_ro", source, undeclared)
        both("obj.a") shouldBe List(1, 2)
        roRow("SELECT *, obj.a FROM zs_ro", source, undeclared)("obj.a") shouldBe List(1, 2)
      }
    }
    // ONE object of a list of several holds them: the path is that object's list; the parent read
    // whole is a STRUCT of several objects, which keeps the statement's rule (A-168, A-183)
    val oneHolder = """{"obj": [{"a": [1, 2]}, {"b": "x"}]}"""
    roRow("SELECT id, obj.a FROM zs_ro", oneHolder, undeclared)("obj.a") shouldBe List(1, 2)
    Seq("SELECT id, obj, obj.a FROM zs_ro", "SELECT *, obj.a FROM zs_ro").foreach { sql =>
      withClue(sql) {
        val struct = failure(roRow(sql, oneHolder, undeclared))
        struct should include("Column obj holds several values")
        struct should include("SET DATA TYPE ARRAY<STRUCT>")
      }
    }
    roRow("SELECT id, obj FROM zs_ro", """{"obj": [{"a": [1, 2]}]}""", undeclared)(
      "obj"
    ) shouldBe ListMap("a" -> List(1, 2))
    // an element's own leaf of a nested field
    roRow("SELECT id, items FROM zs_ro", """{"items": [{"p": [1, 2]}, {"p": 3}]}""", undeclared)(
      "items"
    ) shouldBe List(ListMap("p" -> List(1, 2)), ListMap("p" -> 3))
    // `SELECT *`
    val star = roRow(
      "SELECT * FROM zs_ro",
      """{"id": "r1", "n": [1, 2], "obj": {"a": [3, 4]}, "items": [{"p": [5, 6]}]}""",
      undeclared
    )
    star("n") shouldBe List(1, 2)
    star("obj") shouldBe ListMap("a" -> List(3, 4))
    star("items") shouldBe List(ListMap("p" -> List(5, 6)))
  }

  it should "keep the statement's rule for objects: a STRUCT of several objects, a path several objects hold (A-183)" in {
    val objects = """{"obj": [{"a": 1}, {"a": 2}]}"""
    val struct = failure(roRow("SELECT id, obj FROM zs_ro", objects, undeclared))
    struct should include("Column obj holds several values")
    struct should include("SET DATA TYPE ARRAY<STRUCT>")
    // the list is the cause: never read with the leaf's own rule, which would answer [1, 2]
    val path = failure(roRow("SELECT id, obj.a FROM zs_ro", objects, undeclared))
    path should include("Column obj holds several values")
    path should not include "SET FIELD"
    remedy(path).statements shouldBe List(AlterColumnType("obj", SQLTypes.Array(SQLTypes.Struct)))
    failure(roRow("SELECT id, obj, obj.a FROM zs_ro", objects, undeclared)) should include(
      "Column obj holds several values"
    )
  }

  it should "fail a leaf core declared as a mapping reader's does, each field by its own rule (AC2, AC3)" in {
    val n = failure(roRow("SELECT id, n FROM zs_ro", """{"n": [1, 2]}""", declared))
    n should include("Column n holds several values")
    remedy(n).statements shouldBe List(AlterColumnType("n", SQLTypes.Array(SQLTypes.Int)))
    // a field added beside them by a plain mapping update: no core meta, its list
    roRow("SELECT id, m FROM zs_ro", """{"m": [1, 2]}""", declared)("m") shouldBe List(1, 2)
    // in one object: the declared leaf names its SET FIELD, the undeclared one is its list
    val a = failure(roRow("SELECT id, obj.a FROM zs_ro", """{"obj": {"a": [1, 2]}}""", declared))
    a should include("Column obj.a holds several values")
    a should include("SET FIELD")
    Seq("""{"obj": {"b": [1, 2]}}""", """{"obj": [{"b": [1, 2]}]}""").foreach { source =>
      withClue(source) {
        roRow("SELECT id, obj.b FROM zs_ro", source, declared)("obj.b") shouldBe List(1, 2)
      }
    }
    failure(
      roRow("SELECT id, items FROM zs_ro", """{"items": [{"p": [1, 2]}]}""", declared)
    ) should include("Column items.p holds several values")
    failure(
      roRow("SELECT id, g FROM zs_ro", """{"g": [[1.0, 2.0], [3.0, 4.0]]}""", declared)
    ) should include("Column g holds several values")
  }

  it should "read a scalar over several indices as stored, whatever their meta, a list declared by one as a list (AC4, A-184)" in {
    def two(sql: String, source: String): ListMap[String, Any] =
      rows(sql, s"""{"_index": "zs_ra", "_id": "r1", "_source": $source}""", several).head
    two("SELECT id, n FROM zs_ra,zs_rb", """{"n": [1, 2]}""")("n") shouldBe List(1, 2)
    two("SELECT id, u FROM zs_ra,zs_rb", """{"u": [1, 2]}""")("u") shouldBe List(1, 2)
    two("SELECT id, l FROM zs_ra,zs_rb", """{"l": [1, 2]}""")("l") shouldBe List(1, 2)
    two("SELECT id, l FROM zs_ra,zs_rb", """{"l": 3}""")("l") shouldBe List(3)
  }

  it should "leave Elasticsearch 6.8's read-only reading as read, objects included (LEAD-Q2)" in {
    val objects = """{"obj": [{"a": 1}, {"a": 2}]}"""
    roRow("SELECT id, n FROM zs_ro", """{"n": [1, 2]}""", undeclared, "6.8.23")("n") shouldBe
    List(1, 2)
    roRow("SELECT id, obj.a FROM zs_ro", objects, undeclared, "6.8.23")("obj.a") shouldBe
    List(1, 2)
    roRow("SELECT id, obj FROM zs_ro", objects, undeclared, "6.8.23")("obj") shouldBe
    List(ListMap("a" -> 1), ListMap("a" -> 2))
  }

  it should "read a top hit's and an UNNEST element's undeclared leaf as stored, a declared one failing" in {
    def first(schema: Schema): Seq[ListMap[String, Any]] = {
      val single = statement("SELECT FIRST(n) AS f FROM zs_ro", schema)
      val clients = ListMap(single.sqlAggregations.toSeq.map { case (k, v) =>
        k -> sqlAggregationToClientAggregation(v)
      }: _*)
      ElasticConversion
        .parseResponse(
          s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": []},
             | "aggregations": {"f": {"hits": {"hits": [
             |   {"_index": "zs_ro", "_id": "t1", "_source": {"n": [1, 2]}}]}}}}""".stripMargin,
          single.fieldAliases,
          clients,
          outputs(single)
        )(SchemaColumns.of(single, NativeContext, Some(version)))
        .get
    }
    // FIRST answers its top hit's source (beside the response's `bucket_root`): its `n` read
    first(undeclared).head("f") match {
      case top: scala.collection.Map[_, _] =>
        top.asInstanceOf[scala.collection.Map[String, Any]].get("n") shouldBe Some(List(1, 2))
      case other => fail(s"not a top hit: $other")
    }
    failure(first(declared)) should include("Column n holds several values")
    // an UNNEST element's field: the leaf of the unnested path (`items.p`)
    def element(schema: Schema): com.fasterxml.jackson.databind.JsonNode => Any =
      SchemaColumns.of(
        statement("SELECT t.id, i.p AS ip FROM zs_ro AS t JOIN UNNEST(t.items) AS i", schema),
        NativeContext,
        Some(version)
      ) match {
        case s: SchemaColumns =>
          s.reading.inner
            .get("i")
            .flatMap(byField => Option(byField.get("p")))
            .getOrElse(
              fail(s"no element reader in ${s.reading.inner}")
            )
        case other => fail(s"not read by a plan: $other")
      }
    element(undeclared)(mapper.readTree("[1, 2]")) shouldBe List(1, 2)
    intercept[HitReading.MultiValuedCell](element(declared)(mapper.readTree("[1, 2]")))
  }
}
