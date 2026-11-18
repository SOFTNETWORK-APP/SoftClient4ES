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

package app.softnetwork.elastic.schema

import app.softnetwork.elastic.sql.{ObjectValue, StringValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{CreateTable, TemporalLiterals}
import app.softnetwork.elastic.sql.schema.Table
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** SCHEMA-17 (A-169) -- core's declaration of each LEAF field, copied into the field's mapping
  * `meta` (`sql_type`, `sql_format`, `sql_list_depths`), the one place `_field_caps` returns to a
  * user who may not read the mapping: written by the ONE mapping writer, within Elasticsearch's
  * limits (5 entries, 20-char keys, 50-char values; never on `object` / `nested`), all or none per
  * field beside the user's own keys, and invisible on the read side (`SHOW CREATE TABLE`, a diff,
  * the column's options).
  */
class FieldMetaSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private def created(ddl: String): Table = Parser(ddl) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"[$ddl] $other")
  }

  private def read(properties: String, meta: String = "{}"): Table =
    Index(
      "t",
      s"""{"t": {"aliases": {}, "mappings": {"_meta": {"columns": $meta},
         | "properties": $properties}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  /** The field at `path` (through `properties`) of the mapping `table` writes. */
  private def field(table: Table, path: String*): JsonNode =
    path.foldLeft(table.indexMappings.get("properties"): JsonNode) { (node, name) =>
      Option(node.get("properties")).getOrElse(node).get(name)
    }

  private def metaOf(table: Table, path: String*): Map[String, String] =
    Option(field(table, path: _*).get("meta"))
      .map(_.properties().asScala.map(e => e.getKey -> e.getValue.asText()).toMap)
      .getOrElse(Map.empty)

  private def sqlType(table: Table, path: String*): Option[String] =
    metaOf(table, path: _*).get("sql_type").map(_.toLowerCase)

  /** 70 chars: over Elasticsearch's 50-char `meta` value limit. */
  private val longFormat = "yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss||yyyy/MM/dd||epoch_millis"

  "The mapping core writes" should "carry core's keys in each LEAF field's meta" in {
    val t = created(
      "CREATE TABLE t (id KEYWORD, d DATE OPTIONS (format = 'dd/MM/yyyy'), ts TIMESTAMP, tm TIME, " +
      "tags ARRAY<KEYWORD>, n INT, obj STRUCT FIELDS (a INT, s KEYWORD), " +
      "lst ARRAY<STRUCT> FIELDS (p INT, inner ARRAY<STRUCT> FIELDS (q INT) OPTIONS (type = 'object')) " +
      "OPTIONS (type = 'object'), nst ARRAY<STRUCT> FIELDS (r INT))"
    )
    sqlType(t, "id") shouldBe Some("keyword")
    sqlType(t, "d") shouldBe Some("date")
    metaOf(t, "d").get("sql_format") shouldBe Some("dd/MM/yyyy")
    // every date carries its format, the default included
    metaOf(t, "ts").contains("sql_format") shouldBe true
    sqlType(t, "tm") shouldBe Some("time")
    sqlType(t, "tags") shouldBe Some("array<keyword>")
    sqlType(t, "n") shouldBe Some("int")
    sqlType(t, "obj", "a") shouldBe Some("int")
    // never on an object or a nested field: Elasticsearch refuses it there
    Seq("obj", "lst", "nst").foreach(c => withClue(c)(field(t, c).has("meta") shouldBe false))
    // a leaf under declared ARRAY<STRUCT> ancestors over object: how many levels up each one is
    metaOf(t, "lst", "p").get("sql_list_depths") shouldBe Some("1")
    metaOf(t, "lst", "inner", "q").get("sql_list_depths") shouldBe Some("1,2")
    metaOf(t, "obj", "a").contains("sql_list_depths") shouldBe false
    metaOf(t, "nst", "r").contains("sql_list_depths") shouldBe false
  }

  it should "stay within Elasticsearch's limits: all or none per field, a long format dropping only sql_format" in {
    // four keys of the user's own: core's would make six entries -- none of core's, the user's kept
    val crowded = read(
      """{"d": {"type": "date", "format": "dd/MM/yyyy",
        | "meta": {"a": "1", "b": "2", "c": "3", "e": "4"}}}""".stripMargin,
      """{"d": {"data_type": "DATE"}}"""
    )
    metaOf(crowded, "d") shouldBe Map("a" -> "1", "b" -> "2", "c" -> "3", "e" -> "4")
    // a format over 50 chars: no sql_format, sql_type kept
    val long = created(s"CREATE TABLE t (dl TIMESTAMP OPTIONS (format = '$longFormat'))")
    sqlType(long, "dl") shouldBe Some("timestamp")
    metaOf(long, "dl").contains("sql_format") shouldBe false
    // a user's own key beside core's: both sent
    val mixed = read(
      """{"d": {"type": "date", "format": "dd/MM/yyyy", "meta": {"unit": "day"}}}""",
      """{"d": {"data_type": "DATE"}}"""
    )
    metaOf(mixed, "d").get("unit") shouldBe Some("day")
    sqlType(mixed, "d") shouldBe Some("date")
  }

  "A field's meta read back" should "hide core's keys from the column, and keep the user's" in {
    val t = read(
      """{"d": {"type": "date", "format": "dd/MM/yyyy",
        | "meta": {"sql_type": "date", "sql_format": "dd/MM/yyyy", "unit": "day"}},
        | "k": {"type": "keyword", "meta": {"sql_type": "keyword"}}}""".stripMargin,
      """{"d": {"data_type": "DATE"}}"""
    )
    t.find("d").flatMap(_.options.get("meta")) match {
      case Some(m: ObjectValue) => m.value.keys.toSeq shouldBe Seq("unit")
      case other                => fail(s"d's meta: $other")
    }
    // a meta holding core's keys only is no option at all
    t.find("k").flatMap(_.options.get("meta")) shouldBe None
    t.sql should not include "sql_type"
    t.sql should include("unit")
  }

  "A table read back from the mapping core wrote" should "show no diff against its DDL" in {
    val written = created(
      "CREATE TABLE t (id KEYWORD, d DATE OPTIONS (format = 'dd/MM/yyyy'), tags ARRAY<KEYWORD>, " +
      "lst ARRAY<STRUCT> FIELDS (p INT) OPTIONS (type = 'object'))"
    )
    val back = Index(
      "t",
      s"""{"t": {"aliases": {}, "mappings": ${mapper.writeValueAsString(written.indexMappings)},
         | "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema
    val diff = back.diff(written)
    diff.columns shouldBe empty
    diff.mappings.map(_.toString).filter(_.contains("sql_")) shouldBe empty
    back.sql should not include "sql_"
  }

  "FieldMeta" should "derive core's keys per leaf, the default date format included (A-169)" in {
    val t = created(
      "CREATE TABLE t (d DATE OPTIONS (format = 'dd/MM/yyyy'), ts TIMESTAMP, n INT, " +
      "obj STRUCT FIELDS (a INT))"
    )
    def column(name: String) = t.find(name).getOrElse(fail(s"no $name"))
    FieldMeta.of(column("d"), Nil) shouldBe
    ListMap(FieldMeta.TypeKey -> "DATE", FieldMeta.FormatKey -> "dd/MM/yyyy")
    FieldMeta.of(column("ts"), Nil).get(FieldMeta.FormatKey) shouldBe
    Some(TemporalLiterals.DefaultDateFormat)
    FieldMeta.of(column("n"), Seq(1, 3)) shouldBe
    ListMap(FieldMeta.TypeKey -> "INT", FieldMeta.ListsKey -> "1,3")
    // never on an object, a nested or an alias field
    FieldMeta.of(column("obj"), Nil) shouldBe empty
    Seq("object", "nested", "alias").foreach(tpe => FieldMeta.takesMeta(tpe) shouldBe false)
    FieldMeta.takesMeta("keyword") shouldBe true
  }

  it should "write no meta on a passthrough field, an object-family mapper, and keep its leaves' own" in {
    // Elasticsearch 8.18 refuses `meta` on a passthrough field: every whole-mapping write over the
    // index would fail where main wrote it
    FieldMeta.takesMeta("passthrough") shouldBe false
    val t = read(
      """{"attrs": {"type": "passthrough", "priority": 10},
        | "res": {"type": "passthrough", "priority": 20,
        |  "properties": {"host": {"type": "keyword"}}}}""".stripMargin
    )
    t.find("attrs").map(_.mappingType) shouldBe Some("passthrough")
    Seq("attrs", "res").foreach(c => withClue(c)(field(t, c).has("meta") shouldBe false))
    t.find("res.host").map(FieldMeta.of(_, Nil).get(FieldMeta.TypeKey)) shouldBe
    Some(Some("KEYWORD"))
  }

  it should "merge the user's keys beside core's within the limits, else none of core's" in {
    val core = ListMap(FieldMeta.TypeKey -> "INT")
    val unit: ListMap[String, Value[_]] = ListMap("unit" -> StringValue("m"))
    FieldMeta.merged(unit, core).map(_.keys.toSeq) shouldBe Some(Seq("unit", FieldMeta.TypeKey))
    val five: ListMap[String, Value[_]] =
      ListMap(Seq("a", "b", "c", "e", "f").map(k => k -> (StringValue(k): Value[_])): _*)
    FieldMeta.merged(five, core) shouldBe None
    FieldMeta.merged(five.take(4), core).map(_.size) shouldBe Some(5)
    // a value over 50 characters: none of core's
    FieldMeta.merged(unit, ListMap(FieldMeta.TypeKey -> ("x" * 51))) shouldBe None
    // a user key spelled like one of core's is core's
    FieldMeta.merged(ListMap(FieldMeta.TypeKey -> StringValue("BOOLEAN")), core) shouldBe
    Some(ListMap(FieldMeta.TypeKey -> StringValue("INT")))
  }

  it should "keep the user's keys only on the read side, and read core's keys of a _field_caps meta" in {
    FieldMeta.userOnly(
      ObjectValue(ListMap(FieldMeta.TypeKey -> StringValue("INT"), "unit" -> StringValue("m")))
    ) shouldBe Some(ObjectValue(ListMap("unit" -> StringValue("m"))))
    FieldMeta.userOnly(ObjectValue(ListMap(FieldMeta.FormatKey -> StringValue("x")))) shouldBe None
    FieldMeta.declarations(
      mapper.readTree(
        """{"sql_type": ["array<int>"], "sql_list_depths": ["1", "1,2"], "unit": ["m"]}"""
      )
    ) shouldBe ListMap(
      FieldMeta.TypeKey  -> Seq("array<int>"),
      FieldMeta.ListsKey -> Seq("1", "1,2")
    )
    FieldMeta.declarations(mapper.readTree("""{"sql_type": "date"}""")) shouldBe
    ListMap(FieldMeta.TypeKey -> Seq("date"))
    FieldMeta.declarations(null) shouldBe empty
  }
}
