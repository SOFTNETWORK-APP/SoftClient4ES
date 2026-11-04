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
import app.softnetwork.elastic.sql.{ObjectValue, StringValue, StringValues}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, MultiSearch}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** SCHEMA-6 (D9, AC5) -- object sub-fields keep their DECLARED order.
  *
  * MEASURED (SCHEMA-2 T0-4, ES 8): Elasticsearch returns a mapping's `properties` SORTED by name
  * and a `_meta` object in no reliable order, so `CREATE TABLE so_t (obj STRUCT FIELDS (y INT, b
  * KEYWORD))` read back `b, y`. Only a LIST survives: `_meta.columns.<c>.field_order`. A table that
  * has none (a raw mapping, or one created by 0.24.0) keeps its mapping's order, and an ALTER on it
  * is `Safe`.
  */
class StructFieldOrderSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private val ddl =
    "CREATE TABLE so_t (id INT, obj STRUCT FIELDS (y INT, b KEYWORD), " +
    "items ARRAY<STRUCT> FIELDS (q INT, p KEYWORD))"

  private def declared(sql: String = ddl): Table =
    Parser(sql) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"$other")
    }

  /** `node` as Elasticsearch returns a mapping: every `properties` object sorted by name. */
  private def sortedProperties(node: JsonNode): JsonNode =
    node match {
      case obj: ObjectNode =>
        val copy = mapper.createObjectNode()
        obj.properties().asScala.foreach { e =>
          val value = e.getValue match {
            case props: ObjectNode if e.getKey == "properties" =>
              val sorted = mapper.createObjectNode()
              props.properties().asScala.toSeq.sortBy(_.getKey).foreach { p =>
                sorted.set[JsonNode](p.getKey, sortedProperties(p.getValue))
              }
              sorted
            case other => sortedProperties(other)
          }
          copy.set[JsonNode](e.getKey, value)
        }
        copy
      case other => other
    }

  /** The table core reads back for `table`'s index: mapping `properties` sorted, as Elasticsearch
    * returns them; `withMeta` false drops the `_meta` (a raw mapping).
    */
  private def readBack(table: Table, dropFieldOrder: Boolean = false): Table = {
    val mapping = sortedProperties(table.indexMappings).asInstanceOf[ObjectNode]
    if (dropFieldOrder) {
      val columns = mapping.path("_meta").path("columns")
      columns.properties().asScala.foreach { e =>
        e.getValue match {
          case c: ObjectNode => c.remove("field_order")
          case _             =>
        }
      }
    }
    val mappings = mapper.createObjectNode()
    mappings.set[JsonNode]("mappings", mapping)
    val settings = mapper.createObjectNode()
    settings.set[JsonNode]("settings", table.indexSettings)
    Index(name = table.name, mappings = mappings, settings = settings).schema
  }

  private def names(table: Table, column: String): List[String] =
    table.find(column).map(_.multiFields.map(_.name)).getOrElse(fail(s"no column $column"))

  behavior of "a struct's declared field order"

  it should "be written to _meta.field_order, a list" in {
    val t = declared()
    t.find("obj").get._meta.get("field_order") shouldBe Some(
      StringValues(Seq(StringValue("y"), StringValue("b")))
    )
    t.find("items").get._meta.get("field_order") shouldBe Some(
      StringValues(Seq(StringValue("q"), StringValue("p")))
    )
    t.find("id").get._meta.get("field_order") shouldBe None
  }

  it should "be read back in the declared order, although the mapping is sorted" in {
    val read = readBack(declared())
    names(read, "obj") shouldBe List("y", "b")
    names(read, "items") shouldBe List("q", "p")
    // DESCRIBE TABLE rows, SHOW CREATE TABLE
    // (the top-level columns of a table read back are in name order, as they always were)
    read.describe
      .map(_("Field").toString)
      .filter(f => f.startsWith("obj") || f.startsWith("items")) shouldBe
    Seq("items", "items.q", "items.p", "obj", "obj.y", "obj.b")
    val rendered = read.sql
    rendered.indexOf("y INT") should be < rendered.indexOf("b KEYWORD")
    rendered.indexOf("q INT") should be < rendered.indexOf("p KEYWORD")
    read.diff(declared()).columns shouldBe empty
    read.diff(declared()).mappings shouldBe empty
  }

  it should "type a STRUCT position's fields in that order (ResultSchema, columnTypes)" in {
    val read = readBack(declared())
    MultiSearch.columnType(read.find("obj").get).fields.map(_.name) shouldBe Seq("y", "b")
    MultiSearch.columnType(read.find("items").get).element.get.fields.map(_.name) shouldBe
    Seq("q", "p")
  }

  it should "keep the mapping's order without field_order (raw mapping, 0.24.0 table)" in {
    val legacy = readBack(declared(), dropFieldOrder = true)
    names(legacy, "obj") shouldBe List("b", "y")
    names(legacy, "items") shouldBe List("p", "q")
    MultiSearch.columnType(legacy.find("obj").get).fields.map(_.name) shouldBe Seq("b", "y")
    // and an ALTER on it is Safe: its field_order is written as its order stands, no diff
    legacy.diff(legacy.merge(Nil)).mappings shouldBe empty
    val altered =
      Parser(
        "ALTER TABLE so_t ALTER COLUMN obj SET FIELD b KEYWORD OPTIONS (ignore_above = 10)"
      ) match {
        case Right(at: AlterTable) => legacy.merge(at.statements)
        case other                 => fail(s"$other")
      }
    val diff = legacy.diff(altered)
    withClue(s"columns=${diff.columns} mappings=${diff.mappings}: ")(diff.safety shouldBe Safe)
    names(altered, "obj") shouldBe List("b", "y")
  }

  it should "append a mapped field field_order does not list, and ignore a listed absent one" in {
    val t = declared()
    val mapping = sortedProperties(t.indexMappings).asInstanceOf[ObjectNode]
    mapping
      .path("properties")
      .path("obj")
      .path("properties")
      .asInstanceOf[ObjectNode]
      .set[JsonNode]("a", mapper.readTree("""{"type": "integer"}"""))
    mapping
      .path("_meta")
      .path("columns")
      .path("obj")
      .asInstanceOf[ObjectNode]
      .set[JsonNode]("field_order", mapper.readTree("""["y", "gone", "b"]"""))
    val mappings = mapper.createObjectNode()
    mappings.set[JsonNode]("mappings", mapping)
    val read = Index(
      name = "so_t",
      mappings = mappings,
      settings = mapper.readTree("""{"settings": {}}""")
    ).schema
    names(read, "obj") shouldBe List("y", "b", "a")
  }

  it should "keep a rewritten sub-field in its position (SET FIELD), and append a new one" in {
    val t = declared()
    val rewritten = Parser("ALTER TABLE so_t ALTER COLUMN obj SET FIELD y BIGINT") match {
      case Right(at: AlterTable) => t.merge(at.statements)
      case other                 => fail(s"$other")
    }
    names(rewritten, "obj") shouldBe List("y", "b")
    val added = Parser("ALTER TABLE so_t ALTER COLUMN obj ADD FIELD c KEYWORD") match {
      case Right(at: AlterTable) => t.merge(at.statements)
      case other                 => fail(s"$other")
    }
    names(added, "obj") shouldBe List("y", "b", "c")
    // the new order is a Safe key of its own (the new field's `_meta` keeps today's safety)
    val diff = t.diff(added)
    diff.mappings.collect { case m: MappingSet => m.key } should contain(
      "_meta.columns.obj.field_order"
    )
    diff.mappings.collect {
      case m: MappingSet if m.key.endsWith("field_order") => m.safety
    } shouldBe
    List(Safe)
    ObjectValue(added.find("obj").get._meta).value("field_order") shouldBe StringValues(
      Seq(StringValue("y"), StringValue("b"), StringValue("c"))
    )
  }
}
