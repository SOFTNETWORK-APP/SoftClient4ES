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

package app.softnetwork.elastic.sql.`type`

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.schema.{nestedArrayRefusal, Column, Table}
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** SCHEMA-6 (D1, D2, D3, AC0) -- a column declared `ARRAY<X>` reads back as `ARRAY<X>`, for every X
  * the DDL accepts: ONE read function (`SQLTypes.apply(String)`), ONE write function
  * (`SQLTypeUtils.elasticType`), and the DDL grammar accepting `ARRAY<X>` of every column type.
  *
  * 0.24.0 read every `array<…>` name but `array<struct>` as ANY (`case _ => Any`): `DESCRIBE`
  * answered ANY, the type checks saw ANY, and the rows were as stored. A list of lists is refused
  * by every DDL statement that declares a type (Elasticsearch flattens nested arrays).
  */
class ArrayTypeMappingSpec extends AnyFlatSpec with Matchers {

  /** P1: every X the DDL accepts, with the Elasticsearch type its list maps to. */
  private val p1: Seq[(SQLType, String)] = Seq(
    SQLTypes.TinyInt   -> "byte",
    SQLTypes.SmallInt  -> "short",
    SQLTypes.Int       -> "integer",
    SQLTypes.BigInt    -> "long",
    SQLTypes.Real      -> "float",
    SQLTypes.Double    -> "double",
    SQLTypes.Boolean   -> "boolean",
    SQLTypes.Keyword   -> "keyword",
    SQLTypes.Text      -> "text",
    SQLTypes.Varchar   -> "text",
    SQLTypes.Char      -> "text",
    SQLTypes.Date      -> "date",
    SQLTypes.Time      -> "date",
    SQLTypes.DateTime  -> "date",
    SQLTypes.Timestamp -> "date",
    SQLTypes.VarBinary -> "binary",
    SQLTypes.GeoPoint  -> "geo_point",
    SQLTypes.Struct    -> "nested"
  )

  behavior of "SQLTypes.apply / SQLTypeUtils.elasticType over ARRAY<X> (D1, D2)"

  it should "read every ARRAY<X> type name back as ARRAY<X>, whatever its case" in {
    p1.size shouldBe 18
    for ((x, _) <- p1) {
      val declared = SQLTypes.Array(x)
      withClue(s"${declared.typeId}: ") {
        SQLTypes(declared.typeId) shouldBe declared
        SQLTypes(declared.typeId.toLowerCase) shouldBe declared
      }
    }
  }

  it should "map ARRAY<X> to the Elasticsearch type of X (VARBINARY binary, STRUCT nested)" in {
    for ((x, es) <- p1) {
      withClue(s"ARRAY<${x.typeId}>: ") {
        SQLTypeUtils.elasticType(SQLTypes.Array(x)) shouldBe es
        if (x != SQLTypes.Struct) SQLTypeUtils.elasticType(x) shouldBe es
      }
    }
  }

  it should "read CHAR back as CHAR (it read ANY: no arm)" in {
    SQLTypes("CHAR") shouldBe SQLTypes.Char
    SQLTypes("char") shouldBe SQLTypes.Char
  }

  it should "read an unknown element, or a list of lists, as ANY (R-7)" in {
    // a mapping type core does not read (SCHEMA-7 lists it among those that stay ANY)
    SQLTypes("ARRAY<WILDCARD>") shouldBe SQLTypes.Any
    // SCHEMA-7 (D1): `ip` reads KEYWORD, so a list of them is a list of KEYWORD -- the element's
    // reading, as for every other X
    SQLTypes("ARRAY<IP>") shouldBe SQLTypes.Array(SQLTypes.Keyword)
    SQLTypes("ARRAY<ANY>") shouldBe SQLTypes.Any
    // a 0.24.0 `_meta` could hold one: Elasticsearch has no list of lists
    SQLTypes("ARRAY<ARRAY<INT>>") shouldBe SQLTypes.Any
    SQLTypes("nested") shouldBe SQLTypes.Array(SQLTypes.Struct)
    SQLTypes("array<struct>") shouldBe SQLTypes.Array(SQLTypes.Struct)
  }

  it should "read a declared ARRAY<X> back through the mapping and its _meta" in {
    val mapper = new ObjectMapper()
    for ((x, es) <- p1.filterNot(_._1 == SQLTypes.Struct)) {
      val declared = Parser(s"CREATE TABLE t (id INT, c ARRAY<${x.typeId}>)") match {
        case Right(ct: CreateTable) => ct.schema
        case other                  => fail(s"ARRAY<${x.typeId}>: $other")
      }
      val mappings = mapper.createObjectNode()
      mappings.set("mappings", declared.indexMappings)
      val settings = mapper.createObjectNode()
      settings.set("settings", declared.indexSettings)
      val read = Index(name = "t", mappings = mappings, settings = settings).schema
      withClue(s"ARRAY<${x.typeId}>: ") {
        declared.indexMappings.path("properties").path("c").path("type").asText() shouldBe es
        read.find("c").map(_.dataType) shouldBe Some(SQLTypes.Array(x))
        read.diff(declared).columns shouldBe empty
        read.diff(declared).mappings shouldBe empty
      }
    }
  }

  behavior of "the DDL grammar over ARRAY<X> (D3)"

  it should "parse and re-render CREATE TABLE with ARRAY<X> of every X, ARRAY<GEO_POINT> included" in {
    for ((x, _) <- p1) {
      val fields = if (x == SQLTypes.Struct) " FIELDS (q INT, p KEYWORD)" else ""
      val sql = s"CREATE TABLE t (c ARRAY<${x.typeId}>$fields)"
      Parser(sql) match {
        case Right(ct: CreateTable) =>
          withClue(s"$sql: ") {
            ct.schema.find("c").map(_.dataType) shouldBe Some(SQLTypes.Array(x))
            Parser(ct.sql) shouldBe Right(ct)
            // SHOW CREATE TABLE renders the table's own DDL, which re-parses to the same table
            Parser(ct.schema.sql) match {
              case Right(again: CreateTable) =>
                again.schema.find("c").map(_.dataType) shouldBe Some(SQLTypes.Array(x))
              case other => fail(s"SHOW CREATE TABLE of $sql: $other")
            }
          }
        case other => fail(s"$sql: $other")
      }
    }
  }

  it should "parse SET DATA TYPE ARRAY<GEO_POINT> (the remedy of a several-point geo field)" in {
    val sql = "ALTER TABLE t ALTER COLUMN geo SET DATA TYPE ARRAY<GEO_POINT>"
    Parser(sql) match {
      case Right(at: AlterTable) =>
        Parser(at.sql) shouldBe Right(at)
        at.sql should include("ARRAY<GEO_POINT>")
      case other => fail(s"$sql: $other")
    }
  }

  it should "keep a CAST to ARRAY<X> as it was (sql_type's array_type is untouched)" in {
    Parser("SELECT CAST(n AS ARRAY<INT>) AS a FROM t").isRight shouldBe
    Parser("SELECT CAST(n AS INT) AS a FROM t").isRight
  }

  behavior of "a list of lists (D2)"

  private val refusal =
    "ARRAY<ARRAY<INT>> is not supported: Elasticsearch flattens nested arrays; declare ARRAY<X>"

  it should "be refused at VALIDATE by CREATE TABLE, naming the column" in {
    Parser("CREATE TABLE t (id INT, c ARRAY<ARRAY<INT>>)") match {
      case Left(error) => error.msg should include(s"c: $refusal")
      case other       => fail(s"accepted: $other")
    }
    Parser(
      "CREATE TABLE t (id INT, obj STRUCT FIELDS (y INT, z ARRAY<ARRAY<INT>>))"
    ) match {
      case Left(error) => error.msg should include(s"obj.z: $refusal")
      case other       => fail(s"accepted: $other")
    }
  }

  it should "be refused at VALIDATE by ADD COLUMN, SET DATA TYPE and SET FIELD, naming the column" in {
    Seq(
      "ALTER TABLE t ADD COLUMN c ARRAY<ARRAY<INT>>"                    -> s"c: $refusal",
      "ALTER TABLE t ALTER COLUMN c SET DATA TYPE ARRAY<ARRAY<INT>>"    -> s"c: $refusal",
      "ALTER TABLE t ALTER COLUMN obj SET FIELD z ARRAY<ARRAY<INT>>"    -> s"obj.z: $refusal",
      "ALTER TABLE t ALTER COLUMN obj SET FIELDS (z ARRAY<ARRAY<INT>>)" -> s"obj.z: $refusal"
    ).foreach { case (sql, expected) =>
      Parser(sql) match {
        case Left(error) => withClue(s"$sql: ")(error.msg should include(expected))
        case other       => fail(s"$sql accepted: $other")
      }
    }
  }

  it should "be refused for a CTAS column (ARRAY_AGG of a list column), by the same check" in {
    val source = Table(
      "src",
      List(Column("g", SQLTypes.Keyword), Column("arr", SQLTypes.Array(SQLTypes.Int)))
    )
    val search = Parser("SELECT g, ARRAY_AGG(arr) AS a FROM src GROUP BY g") match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"$other")
    }
    val merged = source.mergeWithSearch(search)
    merged.columns.flatMap(nestedArrayRefusal) shouldBe Seq(s"a: $refusal")
    // ...and a list of a scalar is accepted
    val scalar = Parser("SELECT g, ARRAY_AGG(g) AS a FROM src GROUP BY g") match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"$other")
    }
    source.mergeWithSearch(scalar).columns.flatMap(nestedArrayRefusal) shouldBe empty
  }
}
