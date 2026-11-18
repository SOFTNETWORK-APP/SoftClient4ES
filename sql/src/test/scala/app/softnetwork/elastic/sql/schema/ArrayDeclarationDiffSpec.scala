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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** SCHEMA-6 (D4, D5, D6) -- declaring an existing field a list is a META-ONLY change.
  *
  * The remedy a several-valued column fails with (lead Q1) is `ALTER TABLE t ALTER COLUMN c SET
  * DATA TYPE ARRAY<X>` -- and for a sub-field `ALTER COLUMN p SET FIELD <f as declared, ARRAY<X>>`.
  * Its only diff is the declared type in the table's `_meta` (`Column.diff` compares Elasticsearch
  * types, and `ARRAY<X>` maps to X's), and that key is now `Safe` (it was UNSAFE by default, so the
  * ALTER reindexed: MEASURED on main, ES 8, a `_meta`-only column diff logs `requires REINDEX`). A
  * change of X itself on a list column stays `Impossible`, as on 0.24.0 where every list read ANY.
  */
class ArrayDeclarationDiffSpec extends AnyFlatSpec with Matchers {

  /** `table`'s mapping `properties` without core's field `meta` keys (SCHEMA-17 A-169, `sql_*`):
    * the Elasticsearch-side bytes a list declaration leaves unchanged -- the declaration itself is
    * copied into each leaf's `meta`, which an in-place mapping update writes.
    */
  private def esSide(table: Table): String = {
    val copy = table.indexMappings.path("properties").deepCopy[JsonNode]()
    def strip(fields: JsonNode): Unit =
      if (fields != null && fields.isObject)
        fields.elements().forEachRemaining { field =>
          field match {
            case node: ObjectNode =>
              node.get("meta") match {
                case meta: ObjectNode =>
                  Seq("sql_type", "sql_format", "sql_list_depths").foreach(k => meta.remove(k))
                  if (meta.size() == 0) node.remove("meta")
                case _ =>
              }
              strip(node.get("properties"))
              strip(node.get("fields"))
            case _ =>
          }
        }
    strip(copy)
    copy.toString
  }

  private val mapper = new ObjectMapper()

  /** The table core reads for an index created by a raw mapping (no `_meta`): `zs_ext`. */
  private val rawMapping: Table =
    Index(
      name = "zs_ext",
      mappings = mapper.readTree(
        """{"mappings": {"properties": {
          |  "id": {"type": "keyword"},
          |  "tags": {"type": "keyword"},
          |  "n": {"type": "integer"},
          |  "s": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
          |  "ts": {"type": "date"},
          |  "geo": {"type": "geo_point"},
          |  "obj": {"properties": {
          |    "a": {"type": "integer"},
          |    "f": {"type": "keyword", "ignore_above": 256, "fields": {"raw": {"type": "keyword"}}},
          |    "x": {"properties": {"y": {"type": "integer"}}}
          |  }}
          |}}}""".stripMargin
      ),
      settings = mapper.readTree("""{"settings": {}}""")
    ).schema

  private def alter(table: Table, sql: String): Table =
    Parser(sql) match {
      case Right(at: AlterTable) => table.merge(at.statements)
      case other                 => fail(s"[$sql] is not an ALTER TABLE: $other")
    }

  /** The remedy core names for the column at `path` (HitReading's rendering): a column's `SET DATA
    * TYPE ARRAY<X>`, a sub-field's `SET FIELD` with the sub-field as declared, typed `ARRAY<X>`.
    */
  private def remedy(table: Table, path: String): String = {
    val c = table.find(path).getOrElse(fail(s"no column $path"))
    val list = SQLTypes.Array(c.dataType)
    val dot = path.lastIndexOf('.')
    if (dot < 0) s"ALTER TABLE ${table.name} ALTER COLUMN $path SET DATA TYPE ${list.typeId}"
    else {
      val field = c.copy(dataType = list, struct = None).update()
      s"ALTER TABLE ${table.name} ALTER COLUMN ${path.substring(0, dot)} SET FIELD " +
      field.sql.replaceAll("\\n\\t*", " ")
    }
  }

  behavior of "declaring an existing field a list (D4)"

  it should "diff KEYWORD -> ARRAY<KEYWORD> as ONE Safe _meta data_type key" in {
    val desired = alter(rawMapping, remedy(rawMapping, "tags"))
    desired.find("tags").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Keyword))
    val diff = rawMapping.diff(desired)
    diff.columns shouldBe empty
    diff.mappings.collect { case MappingSet(key, value) => key -> value.value } shouldBe
    List("_meta.columns.tags.data_type" -> "ARRAY<KEYWORD>")
    diff.safety shouldBe Safe
  }

  it should "be meta-only for every scalar zs_ext shape: a number, a dynamic string, a date, a point" in {
    for (c <- Seq("n", "s", "ts", "geo")) {
      val desired = alter(rawMapping, remedy(rawMapping, c))
      val diff = rawMapping.diff(desired)
      withClue(s"$c: columns=${diff.columns} mappings=${diff.mappings}: ") {
        diff.columns shouldBe empty
        diff.mappings.collect { case MappingSet(key, _) => key } shouldBe
        List(s"_meta.columns.$c.data_type")
        diff.safety shouldBe Safe
      }
    }
    // the dynamic string keeps its `keyword` multi-field and its `ignore_above`
    val s = alter(rawMapping, remedy(rawMapping, "s")).find("s").get
    s.dataType shouldBe SQLTypes.Array(SQLTypes.Text)
    s.multiFields.map(_.name) shouldBe List("keyword")
  }

  it should "be meta-only for a sub-field, its options and multi-fields kept by the rendered SET FIELD" in {
    for (path <- Seq("obj.a", "obj.f", "obj.x.y")) {
      val statement = remedy(rawMapping, path)
      val desired = alter(rawMapping, statement)
      val diff = rawMapping.diff(desired)
      val segments = path.split("\\.")
      val key =
        s"_meta.columns.${segments.head}" + segments.tail.map(s => s".multi_fields.$s").mkString +
        ".data_type"
      withClue(s"$statement: columns=${diff.columns} mappings=${diff.mappings}: ") {
        desired.find(path).map(_.dataType.typeId).getOrElse("") should startWith("ARRAY<")
        // the Elasticsearch mapping is the same bytes: only `_meta` changes (a sub-field whose
        // options re-parse as another number class reports a no-op `FieldAltered`, Safe)
        esSide(desired) shouldBe esSide(rawMapping)
        diff.columns.filterNot(_.isInstanceOf[FieldAltered]) shouldBe empty
        diff.mappings.collect { case MappingSet(k, _) => k } shouldBe List(key)
        diff.safety shouldBe Safe
      }
    }
    val f = alter(rawMapping, remedy(rawMapping, "obj.f")).find("obj.f").get
    f.options.keySet should contain("ignore_above")
    f.multiFields.map(_.name) shouldBe List("raw")
  }

  it should "keep a sub-field's DEFAULT, COMMENT and NOT NULL (an SQL-created table)" in {
    val declared = Parser(
      "CREATE TABLE t (id INT, obj STRUCT FIELDS (" +
      "f KEYWORD FIELDS (raw KEYWORD) DEFAULT 'none' NOT NULL COMMENT 'a note' " +
      "OPTIONS (ignore_above = 256), a INT))"
    ) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }
    val statement = remedy(declared, "obj.f")
    val desired = alter(declared, statement)
    val diff = declared.diff(desired)
    withClue(s"$statement: columns=${diff.columns} mappings=${diff.mappings}: ") {
      esSide(desired) shouldBe esSide(declared)
      diff.columns.filterNot(_.isInstanceOf[FieldAltered]) shouldBe empty
      diff.mappings.collect { case MappingSet(k, _) => k } shouldBe
      List("_meta.columns.obj.multi_fields.f.data_type")
      diff.safety shouldBe Safe
      // the sub-field keeps its position (D9)
      desired.find("obj").get.multiFields.map(_.name) shouldBe List("f", "a")
    }
  }

  it should "keep today's safety for every other _meta key (a COMMENT reindexes, as measured)" in {
    val declared = Parser("CREATE TABLE t (id INT, c KEYWORD)") match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }
    val diff = declared.diff(alter(declared, "ALTER TABLE t ALTER COLUMN c SET COMMENT 'x'"))
    diff.mappings.collect { case MappingSet(k, _) => k } should contain(
      "_meta.columns.c.comment"
    )
    diff.safety shouldBe UnsafeReindex
    MappingsRules.isSafe("_meta.columns.c.comment") shouldBe false
    MappingsRules.isSafe("_meta.columns.c.data_type") shouldBe true
    MappingsRules.isSafe("_meta.columns.c.multi_fields.f.data_type") shouldBe true
    MappingsRules.isSafe("_meta.columns.c.field_order") shouldBe true
    MappingsRules.isSafe("_meta.columns.c.multi_fields.f.field_order") shouldBe true
    MappingsRules.isSafe("_meta.primary_key") shouldBe false
    MappingsRules.isSafe("_meta.columns.c.script") shouldBe false
  }

  behavior of "changing X itself on a list column (D5)"

  it should "stay Impossible, as on 0.24.0" in {
    val declared = Parser("CREATE TABLE t (id INT, arr ARRAY<INT>, k KEYWORD)") match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }
    Seq(
      "ALTER TABLE t ALTER COLUMN arr SET DATA TYPE ARRAY<BIGINT>",
      "ALTER TABLE t ALTER COLUMN arr SET DATA TYPE ARRAY<KEYWORD>",
      "ALTER TABLE t ALTER COLUMN k SET DATA TYPE ARRAY<INT>"
    ).foreach { sql =>
      val diff = declared.diff(alter(declared, sql))
      withClue(s"$sql: ${diff.columns}: ") {
        diff.columns.collect { case c: ColumnTypeChanged => c } should not be empty
        diff.safety shouldBe Impossible
      }
    }
    ColumnTypeChanged("c", SQLTypes.Int, SQLTypes.BigInt)().safety shouldBe UnsafeReindex
  }

  behavior of "declaring an object column a list of objects in place (SCHEMA-17 A-166)"

  it should "keep the object mapping: ONE Safe _meta data_type key, the field still object" in {
    val desired =
      alter(rawMapping, "ALTER TABLE zs_ext ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>")
    val obj = desired.find("obj").getOrElse(fail("no obj"))
    obj.dataType shouldBe SQLTypes.Array(SQLTypes.Struct)
    obj.mappingType shouldBe "object"
    obj.node.get("type").asText() shouldBe "object"
    obj.multiFields.map(_.name) shouldBe List("a", "f", "x")
    val diff = rawMapping.diff(desired)
    diff.columns shouldBe empty
    diff.mappings.collect { case MappingSet(key, value) => key -> value.value } shouldBe
    List("_meta.columns.obj.data_type" -> "ARRAY<STRUCT>")
    diff.safety shouldBe Safe
  }

  it should "render ARRAY<STRUCT> OPTIONS (type = 'object') in SHOW CREATE TABLE, which re-parses to the same table" in {
    val desired =
      alter(rawMapping, "ALTER TABLE zs_ext ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>")
    val text = desired.sql
    // the renderer quotes an option's text with double quotes, as every other option it renders
    text should include("""ARRAY<STRUCT> FIELDS (""")
    text should include(""") OPTIONS (type = "object")""")
    val recreated = Parser(text) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"[$text] does not re-parse: $other")
    }
    val obj = recreated.find("obj").getOrElse(fail("no obj"))
    obj.dataType shouldBe SQLTypes.Array(SQLTypes.Struct)
    obj.mappingType shouldBe "object"
    recreated.indexMappings.get("properties").get("obj").get("type").asText() shouldBe "object"
  }

  it should "keep CREATE TABLE ... ARRAY<STRUCT> meaning nested, and OPTIONS (type = 'object') the list over object" in {
    val created = Parser(
      "CREATE TABLE t (id KEYWORD, nst ARRAY<STRUCT> FIELDS (p INT), " +
      "lst ARRAY<STRUCT> FIELDS (q INT) OPTIONS (type = 'object'))"
    ) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }
    created.find("nst").map(_.mappingType) shouldBe Some("nested")
    created.find("lst").map(_.mappingType) shouldBe Some("object")
    created.find("lst").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Struct))
  }

  /** The target `CREATE TABLE … AS SELECT` builds from its source. */
  private def copyOf(source: Table, sql: String): Table =
    Parser(sql) match {
      case Right(s: SingleSearch) => source.mergeWithSearch(s)
      case other                  => fail(s"[$sql] is not a single search: $other")
    }

  it should "write a copy (CREATE TABLE ... AS SELECT) of a list over object as nested, at any depth (A-167)" in {
    // declared in place: the source stays `object`, the copy is `nested`, its sub-fields kept
    val declared =
      alter(rawMapping, "ALTER TABLE zs_ext ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>")
    val copied = copyOf(declared, "SELECT id, obj FROM zs_ext")
    val obj = copied.find("obj").getOrElse(fail("no obj in the copy"))
    obj.dataType shouldBe SQLTypes.Array(SQLTypes.Struct)
    obj.mappingType shouldBe "nested"
    obj.options.contains("type") shouldBe false
    obj.multiFields.map(_.name) shouldBe List("a", "f", "x")
    copied.find("obj.f").map(_.options.contains("ignore_above")) shouldBe Some(true)
    copied.indexMappings.get("properties").get("obj").get("type").asText() shouldBe "nested"
    declared.find("obj").map(_.mappingType) shouldBe Some("object")
    // created with the option, under a struct too; a `nested` list and every other kept `type`
    // (SCHEMA-7: `ip`, a CREATE's `keyword` over VARCHAR) are copied as they are
    val created = Parser(
      "CREATE TABLE t (id KEYWORD, c VARCHAR OPTIONS (type = 'keyword'), " +
      "i KEYWORD OPTIONS (type = 'ip'), nst ARRAY<STRUCT> FIELDS (p INT), " +
      "lst ARRAY<STRUCT> FIELDS (q INT) OPTIONS (type = 'object'), " +
      "st STRUCT FIELDS (sub ARRAY<STRUCT> FIELDS (r INT) OPTIONS (type = 'object')))"
    ) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }
    created.find("st.sub").map(_.mappingType) shouldBe Some("object")
    val copy = copyOf(created, "SELECT id, c, i, nst, lst, st FROM t")
    Seq(
      "c"      -> "keyword",
      "i"      -> "ip",
      "nst"    -> "nested",
      "lst"    -> "nested",
      "st"     -> "object",
      "st.sub" -> "nested"
    ).foreach { case (path, esType) =>
      withClue(s"$path: ")(copy.find(path).map(_.mappingType) shouldBe Some(esType))
    }
    val properties = copy.indexMappings.get("properties")
    properties.get("lst").get("type").asText() shouldBe "nested"
    properties.get("st").get("properties").get("sub").get("type").asText() shouldBe "nested"
    copy.find("st.sub.r").flatMap(_.struct).map(_.path) shouldBe Some("st.sub")
  }
}
