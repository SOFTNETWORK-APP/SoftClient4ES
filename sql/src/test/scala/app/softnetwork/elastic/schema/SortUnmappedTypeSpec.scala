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

import app.softnetwork.elastic.sql.SQLKeywords
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `SQLTypeUtils.sortUnmappedType`: the `unmapped_type` a field sort over a column carries.
  *
  * The key must name the type the column is MAPPED as, else an index that does not map the column
  * sorts its documents under a different type than the indices that do, and Elasticsearch refuses
  * the search. So the oracle is a round trip: a field whose Elasticsearch mapping type is `m`, read
  * as the engine reads an undeclared field (`IndexField.undeclaredType`, then `SQLTypes`), gets
  * `Some(m)` exactly when `m` is a type that sorts without a parameter of its own, and `None` for
  * every other type.
  */
class SortUnmappedTypeSpec extends AnyFlatSpec with Matchers {

  /** The field types sortable without a mapping parameter of their own. */
  private val sortable: Set[String] =
    Set("keyword", "boolean", "byte", "short", "integer", "long", "float", "double", "date")

  /** Every Elasticsearch field (mapping) type, from the 8.x reference, plus `string` (ES 6's
    * pre-5.0 name, still read in old mappings).
    */
  private val esMappingTypes: Seq[String] = Seq(
    "aggregate_metric_double",
    "alias",
    "annotated_text",
    "binary",
    "boolean",
    "byte",
    "completion",
    "constant_keyword",
    "date",
    "date_nanos",
    "date_range",
    "dense_vector",
    "double",
    "double_range",
    "flattened",
    "float",
    "float_range",
    "geo_point",
    "geo_shape",
    "half_float",
    "histogram",
    "integer",
    "integer_range",
    "ip",
    "ip_range",
    "join",
    "keyword",
    "long",
    "long_range",
    "match_only_text",
    "murmur3",
    "nested",
    "object",
    "percolator",
    "point",
    "rank_feature",
    "rank_features",
    "scaled_float",
    "search_as_you_type",
    "semantic_text",
    "shape",
    "short",
    "sparse_vector",
    "string",
    "text",
    "token_count",
    "unsigned_long",
    "version",
    "wildcard"
  )

  "a field read off its Elasticsearch mapping type" should "sort under that same type, or carry no key" in {
    esMappingTypes.foreach { m =>
      withClue(s"mapping type '$m': ") {
        val key = SQLTypeUtils.sortUnmappedType(SQLTypes(IndexField.undeclaredType(m)))
        if (sortable.contains(m)) key shouldBe Some(m)
        else key shouldBe None
      }
    }
  }

  it should "carry no key for each type a parameter, a text analysis or a structure rules out" in {
    Seq(
      "text",
      "half_float",
      "scaled_float",
      "unsigned_long",
      "date_nanos",
      "ip",
      "wildcard",
      "constant_keyword",
      "geo_point",
      "object",
      "nested",
      "binary"
    ).foreach { m =>
      withClue(m) {
        SQLTypeUtils.sortUnmappedType(SQLTypes(IndexField.undeclaredType(m))) shouldBe None
      }
    }
  }

  /** What `CREATE TABLE` maps each DDL type as, for the types whose mapping sorts without a
    * parameter: the key must be exactly that mapping. Every other DDL type gets no key.
    */
  private val ddlKeys: Map[String, Option[String]] = Map(
    "BIGINT"    -> Some("long"),
    "BINARY"    -> None,
    "BOOLEAN"   -> Some("boolean"),
    "BYTE"      -> Some("byte"),
    "CHAR"      -> None,
    "DATE"      -> Some("date"),
    "DATETIME"  -> Some("date"),
    "DEC"       -> Some("double"),
    "DECIMAL"   -> Some("double"),
    "DOUBLE"    -> Some("double"),
    "FLOAT"     -> Some("float"),
    "GEOPOINT"  -> None,
    "GEO_POINT" -> None,
    "INT"       -> Some("integer"),
    "INTEGER"   -> Some("integer"),
    "KEYWORD"   -> Some("keyword"),
    "LONG"      -> Some("long"),
    "NUMERIC"   -> Some("double"),
    "REAL"      -> Some("float"),
    "SHORT"     -> Some("short"),
    "SIGNED"    -> Some("long"),
    "SMALLINT"  -> Some("short"),
    "STRING"    -> None,
    "STRUCT"    -> None,
    "TEXT"      -> None,
    "TIME"      -> Some("date"),
    "TIMESTAMP" -> Some("date"),
    "TINYINT"   -> Some("byte"),
    "UNSIGNED"  -> Some("long"),
    "VARBINARY" -> None,
    "VARCHAR"   -> None
  )

  "a column declared by CREATE TABLE" should "sort under the type CREATE TABLE maps it as" in {
    val declared = SQLKeywords.typeWords.toSeq.sorted.flatMap { word =>
      Parser(s"CREATE TABLE ddl_probe (c $word)") match {
        case Right(ct: CreateTable) =>
          ct.ddl match {
            case Right(columns) => columns.headOption.map(c => word -> c.dataType)
            case Left(_)        => None
          }
        case _ => None
      }
    }
    // Every type name the parser accepts on its own is in the oracle, and the oracle names no
    // other: a new DDL type must be given its key here.
    declared.map(_._1).toSet shouldBe ddlKeys.keySet
    declared.foreach { case (word, t) =>
      withClue(s"DDL type $word ($t): ") {
        SQLTypeUtils.sortUnmappedType(t) shouldBe ddlKeys(word)
        ddlKeys(word).foreach(_ shouldBe SQLTypeUtils.elasticType(t))
      }
    }
  }

  "an array, a struct, an untyped or a null column" should "carry no key" in {
    Seq(
      SQLTypes.Array(SQLTypes.Keyword),
      SQLTypes.Array(SQLTypes.BigInt),
      SQLTypes.Array(SQLTypes.Struct),
      SQLTypes.Struct,
      SQLTypes.Any,
      SQLTypes.Null,
      SQLTypes.Numeric,
      SQLTypes.GeoPoint,
      SQLTypes.VarBinary
    ).foreach { t =>
      withClue(t.toString) { SQLTypeUtils.sortUnmappedType(t) shouldBe None }
    }
  }
}
