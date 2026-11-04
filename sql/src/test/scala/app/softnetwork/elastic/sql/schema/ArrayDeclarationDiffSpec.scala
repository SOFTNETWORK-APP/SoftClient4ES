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
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.ObjectMapper
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
        desired.indexMappings.path("properties").toString shouldBe
        rawMapping.indexMappings.path("properties").toString
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
      desired.indexMappings.path("properties").toString shouldBe
      declared.indexMappings.path("properties").toString
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
    ColumnTypeChanged("c", SQLTypes.Int, SQLTypes.BigInt).safety shouldBe UnsafeReindex
  }
}
