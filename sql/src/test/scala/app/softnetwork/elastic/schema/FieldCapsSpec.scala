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

import app.softnetwork.elastic.sql.StringValue
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SingleSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.query.TemporalLiterals.FieldFormat
import app.softnetwork.elastic.sql.schema.{mergedSchema, Table, TableType}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** SCHEMA-7 (D7, D8, A-40, A-65, A-67) -- a `_field_caps` answer read as the `GET <index>`
  * documents it describes (`FieldCaps.members`), then typed by the same seam as a mapping
  * (`Index.schema`, `mergedSchema`): the schema of a user who may search an index but not read its
  * mapping. The bodies are the ones Elasticsearch 8.18.3 (a `read`-only user) and 6.8.23 answered
  * for the story's fixtures.
  */
class FieldCapsSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  /** ES 8.18.3, `read`-only user, `GET s7_t/_field_caps?fields=*` (`rt` is a runtime field). */
  private val es8Table: String =
    """{"indices":["s7_t"],"fields":{"rt":{"keyword":{"type":"keyword","metadata_field":false,"searchable":true,"aggregatable":true}},"_ignored_source":{"_ignored_source":{"type":"_ignored_source","metadata_field":true,"searchable":false,"aggregatable":false}},"_index":{"_index":{"type":"_index","metadata_field":true,"searchable":true,"aggregatable":true}},"_feature":{"_feature":{"type":"_feature","metadata_field":true,"searchable":false,"aggregatable":false}},"fl":{"flattened":{"type":"flattened","metadata_field":false,"searchable":true,"aggregatable":true}},"dn":{"date_nanos":{"type":"date_nanos","metadata_field":false,"searchable":true,"aggregatable":true}},"_ignored":{"_ignored":{"type":"_ignored","metadata_field":true,"searchable":true,"aggregatable":true}},"sf":{"scaled_float":{"type":"scaled_float","metadata_field":false,"searchable":true,"aggregatable":true}},"_data_stream_timestamp":{"_data_stream_timestamp":{"type":"_data_stream_timestamp","metadata_field":true,"searchable":false,"aggregatable":false}},"id":{"keyword":{"type":"keyword","metadata_field":false,"searchable":true,"aggregatable":true}},"_version":{"_version":{"type":"_version","metadata_field":true,"searchable":false,"aggregatable":true}},"_routing":{"_routing":{"type":"_routing","metadata_field":true,"searchable":true,"aggregatable":false}},"_inference_fields":{"_inference_fields":{"type":"_inference_fields","metadata_field":true,"searchable":false,"aggregatable":false}},"_doc_count":{"integer":{"type":"integer","metadata_field":true,"searchable":false,"aggregatable":false}},"d":{"date":{"type":"date","metadata_field":false,"searchable":true,"aggregatable":true}},"ip":{"ip":{"type":"ip","metadata_field":false,"searchable":true,"aggregatable":true}},"ck":{"keyword":{"type":"keyword","metadata_field":false,"searchable":true,"aggregatable":true}},"k":{"keyword":{"type":"keyword","metadata_field":false,"searchable":true,"aggregatable":true}},"_index_mode":{"_index_mode":{"type":"_index_mode","metadata_field":true,"searchable":true,"aggregatable":true}},"n":{"long":{"type":"long","metadata_field":false,"searchable":true,"aggregatable":true}},"_tier":{"keyword":{"type":"keyword","metadata_field":true,"searchable":true,"aggregatable":true}},"_seq_no":{"_seq_no":{"type":"_seq_no","metadata_field":true,"searchable":true,"aggregatable":true}},"r":{"float":{"type":"float","metadata_field":false,"searchable":true,"aggregatable":true}},"_nested_path":{"_nested_path":{"type":"_nested_path","metadata_field":true,"searchable":true,"aggregatable":false}},"u":{"unsigned_long":{"type":"unsigned_long","metadata_field":false,"searchable":true,"aggregatable":true}},"_field_names":{"_field_names":{"type":"_field_names","metadata_field":true,"searchable":true,"aggregatable":false}},"x":{"double":{"type":"double","metadata_field":false,"searchable":true,"aggregatable":true}},"_source":{"_source":{"type":"_source","metadata_field":true,"searchable":false,"aggregatable":false}},"_id":{"_id":{"type":"_id","metadata_field":true,"searchable":true,"aggregatable":false}},"hf":{"half_float":{"type":"half_float","metadata_field":false,"searchable":true,"aggregatable":true}}}}"""

  /** ES 8.18.3, `GET s7_p*` with `_field_caps`: `k` a keyword in `s7_p1`, a long in `s7_p2`. */
  private val es8Pattern: String =
    """{"indices":["s7_p1","s7_p2"],"fields":{"_routing":{"_routing":{"type":"_routing","metadata_field":true,"searchable":true,"aggregatable":false}},"_doc_count":{"integer":{"type":"integer","metadata_field":true,"searchable":false,"aggregatable":false}},"_index":{"_index":{"type":"_index","metadata_field":true,"searchable":true,"aggregatable":true}},"k":{"keyword":{"type":"keyword","metadata_field":false,"searchable":true,"aggregatable":true,"indices":["s7_p1"]},"long":{"type":"long","metadata_field":false,"searchable":true,"aggregatable":true,"indices":["s7_p2"]}},"_tier":{"keyword":{"type":"keyword","metadata_field":true,"searchable":true,"aggregatable":true}},"_source":{"_source":{"type":"_source","metadata_field":true,"searchable":false,"aggregatable":false}},"_id":{"_id":{"type":"_id","metadata_field":true,"searchable":true,"aggregatable":false}},"_version":{"_version":{"type":"_version","metadata_field":true,"searchable":false,"aggregatable":true}}}}"""

  /** ES 6.8.23, `GET s7_t/_field_caps?fields=*`: no `indices`, metadata typed by their own name. */
  private val es6Table: String =
    """{"fields":{"_routing":{"_routing":{"type":"_routing","searchable":true,"aggregatable":false}},"_index":{"_index":{"type":"_index","searchable":true,"aggregatable":true}},"d":{"date":{"type":"date","searchable":true,"aggregatable":true}},"ip":{"ip":{"type":"ip","searchable":true,"aggregatable":true}},"_type":{"_type":{"type":"_type","searchable":true,"aggregatable":true}},"k":{"keyword":{"type":"keyword","searchable":true,"aggregatable":true}},"n":{"long":{"type":"long","searchable":true,"aggregatable":true}},"_all":{"_all":{"type":"_all","searchable":true,"aggregatable":false}},"_ignored":{"_ignored":{"type":"_ignored","searchable":true,"aggregatable":false}},"_seq_no":{"_seq_no":{"type":"_seq_no","searchable":true,"aggregatable":true}},"r":{"float":{"type":"float","searchable":true,"aggregatable":true}},"_parent":{"_parent":{"type":"_parent","searchable":false,"aggregatable":true}},"sf":{"scaled_float":{"type":"scaled_float","searchable":true,"aggregatable":true}},"_field_names":{"_field_names":{"type":"_field_names","searchable":true,"aggregatable":false}},"x":{"double":{"type":"double","searchable":true,"aggregatable":true}},"_source":{"_source":{"type":"_source","searchable":false,"aggregatable":false}},"_id":{"_id":{"type":"_id","searchable":true,"aggregatable":true}},"id":{"keyword":{"type":"keyword","searchable":true,"aggregatable":true}},"_version":{"_version":{"type":"_version","searchable":false,"aggregatable":false}},"_uid":{"_uid":{"type":"_uid","searchable":false,"aggregatable":true}},"hf":{"half_float":{"type":"half_float","searchable":true,"aggregatable":true}}}}"""

  /** ES 6.8.23, `GET s7_p*` with `_field_caps`: the conflict names the indices. */
  private val es6Pattern: String =
    """{"fields":{"_routing":{"_routing":{"type":"_routing","searchable":true,"aggregatable":false}},"_index":{"_index":{"type":"_index","searchable":true,"aggregatable":true}},"_type":{"_type":{"type":"_type","searchable":true,"aggregatable":true}},"k":{"keyword":{"type":"keyword","searchable":true,"aggregatable":true,"indices":["s7_p1"]},"long":{"type":"long","searchable":true,"aggregatable":true,"indices":["s7_p2"]}},"_all":{"_all":{"type":"_all","searchable":true,"aggregatable":false}},"_source":{"_source":{"type":"_source","searchable":false,"aggregatable":false}},"_id":{"_id":{"type":"_id","searchable":true,"aggregatable":true}},"_uid":{"_uid":{"type":"_uid","searchable":false,"aggregatable":true}}}}"""

  /** An object, its properties, a multi-field under a keyword and a nested field (ES 8 shape). */
  private val structured: String =
    """{"indices":["t"],"fields":{
      |"k":{"keyword":{"type":"keyword","metadata_field":false}},
      |"k.raw":{"keyword":{"type":"keyword","metadata_field":false}},
      |"obj":{"object":{"type":"object","metadata_field":false}},
      |"obj.a":{"long":{"type":"long","metadata_field":false}},
      |"obj.in":{"object":{"type":"object","metadata_field":false}},
      |"obj.in.b":{"keyword":{"type":"keyword","metadata_field":false}},
      |"nst":{"nested":{"type":"nested","metadata_field":false}},
      |"nst.c":{"keyword":{"type":"keyword","metadata_field":false}},
      |"_id":{"_id":{"type":"_id","metadata_field":true}}
      |}}""".stripMargin

  private def members(source: String, body: String): Seq[(String, Table)] =
    FieldCaps.members(source, mapper.readTree(body)).map { case (name, document) =>
      name -> Index(name, document).schema
    }

  /** What the client's `_field_caps` fallback answers (`IndicesApi.fetchFieldCapsSchema`). */
  private def caps(source: String, body: String): Table =
    mergedSchema(source, members(source, body))._1.copy(tableType = TableType.FieldCapabilities)

  private def types(table: Table): Map[String, (String, String)] =
    table.columns.map(c => c.name -> (c.dataType.typeId, c.mappingType)).toMap

  "FieldCaps.members" should "describe each index of an ES 8 answer, its metadata fields dropped" in {
    members("s7_t", es8Table).map(_._1) shouldBe Seq("s7_t")
    types(caps("s7_t", es8Table)) shouldBe Map(
      "id" -> ("KEYWORD", "keyword"),
      "k"  -> ("KEYWORD", "keyword"),
      // A-67: `constant_keyword` is reported as `keyword`, a runtime field as a mapped keyword
      "ck" -> ("KEYWORD", "keyword"),
      "rt" -> ("KEYWORD", "keyword"),
      "ip" -> ("KEYWORD", "ip"),
      "sf" -> ("DOUBLE", "scaled_float"),
      "hf" -> ("REAL", "half_float"),
      "u"  -> ("BIGINT", "unsigned_long"),
      "dn" -> ("TIMESTAMP", "date_nanos"),
      "d"  -> ("TIMESTAMP", "date"),
      "n"  -> ("BIGINT", "long"),
      "r"  -> ("REAL", "float"),
      "x"  -> ("DOUBLE", "double"),
      "fl" -> ("ANY", "flattened")
    )
  }

  it should "describe an ES 6.8 answer, which lists no index, as the source itself" in {
    members("s7_t", es6Table).map(_._1) shouldBe Seq("s7_t")
    types(caps("s7_t", es6Table)) shouldBe Map(
      "id" -> ("KEYWORD", "keyword"),
      "k"  -> ("KEYWORD", "keyword"),
      "ip" -> ("KEYWORD", "ip"),
      "sf" -> ("DOUBLE", "scaled_float"),
      "hf" -> ("REAL", "half_float"),
      "d"  -> ("TIMESTAMP", "date"),
      "n"  -> ("BIGINT", "long"),
      "r"  -> ("REAL", "float"),
      "x"  -> ("DOUBLE", "double")
    )
  }

  it should "give each index of a pattern the type its conflict names, on both majors" in {
    Seq(es8Pattern, es6Pattern).foreach { body =>
      val described = members("s7_p*", body)
      described.map(_._1) shouldBe Seq("s7_p1", "s7_p2")
      described.map { case (name, t) => name -> t.find("k").map(_.dataType) } shouldBe Seq(
        "s7_p1" -> Some(SQLTypes.Keyword),
        "s7_p2" -> Some(SQLTypes.BigInt)
      )
      // AC1 / D6: merged on the lattice, as the privileged client merges the two mappings
      val (merged, disagreements) = mergedSchema("s7_p*", described)
      disagreements shouldBe empty
      merged.find("k").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    }
  }

  it should "describe no index for a pattern that matches none" in {
    FieldCaps.members("s7_none*", mapper.readTree("""{"indices":[],"fields":{}}""")) shouldBe empty
    FieldCaps.members("s7_none*", mapper.readTree("""{"fields":{}}""")) shouldBe empty
  }

  it should "place a path under an object or a nested parent as a property, else as a multi-field" in {
    val t = caps("t", structured)
    t.find("k").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    t.find("k").get.multiFields.map(c => c.name -> c.dataType) shouldBe List(
      "raw" -> SQLTypes.Keyword
    )
    t.find("obj").map(_.dataType) shouldBe Some(SQLTypes.Struct)
    t.find("obj.a").map(_.dataType) shouldBe Some(SQLTypes.BigInt)
    t.find("obj.in.b").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    t.find("nst").map(_.dataType) shouldBe Some(SQLTypes.Array(SQLTypes.Struct))
    t.find("nst.c").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    t.find("_id") shouldBe None
  }

  it should "read a date by the ISO format alone: _field_caps knows no format (A-40)" in {
    val t = caps("s7_t", es8Table)
    t.find("d").flatMap(_.options.get("format")) shouldBe Some(
      StringValue(TemporalLiterals.FieldCapsDateFormat)
    )
    FieldFormat.of(t.find("d").get).spec shouldBe TemporalLiterals.FieldCapsDateFormat
    FieldFormat.of(t.find("dn").get).spec shouldBe TemporalLiterals.FieldCapsDateNanosFormat
    // two date formats are one ISO format to this user: the fields agree
    val (merged, disagreements) = mergedSchema(
      "p*",
      members(
        "p*",
        """{"indices":["p1","p2"],"fields":{"d":{"date":{"type":"date","metadata_field":false}}}}"""
      )
    )
    disagreements shouldBe empty
    merged.find("d").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
  }

  private def statement(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss
      case other                   => fail(s"[$sql] $other")
    }

  "TemporalLiterals" should "forward a literal verbatim over a _field_caps schema (D8)" in {
    val fieldCaps = caps("s7_t", es8Table)
    // the same columns as a mapping would have them: the counterfactual
    val mapping = fieldCaps.copy(tableType = TableType.Regular)
    def literal(value: String) = statement(s"SELECT id FROM s7_t WHERE d > '$value'")
    // a text the field's format does not read, the SQL spelling and a custom spelling: each one
    // forwarded as written, as main forwarded it for this user
    Seq("abc", "2024-01-31 10:30:00", "2024/01/31").foreach { value =>
      withClue(value) {
        val search = literal(value).update(Some(fieldCaps))
        TemporalLiterals(search, fieldCaps) shouldBe Right(search)
      }
    }
    // ...where the format, were it the field's own, would refuse the first and rewrite the second
    // (T0-7: a custom spelling is forwarded under both)
    TemporalLiterals(literal("abc").update(Some(mapping)), mapping).isLeft shouldBe true
    val spelled = literal("2024-01-31 10:30:00").update(Some(mapping))
    TemporalLiterals(spelled, mapping) should not be Right(spelled)
    TemporalLiterals(spelled, mapping).isRight shouldBe true
  }

  /** A-65 (R-83): arithmetic over `ip` and `constant_keyword` is refused by type, as over any
    * KEYWORD -- on the mapping origin (`ck` keeps `constant_keyword`) and the `_field_caps` one
    * (`ck` reads `keyword`) alike.
    */
  "arithmetic over ip and constant_keyword" should "be refused by type, on both origins (A-65)" in {
    val mapping = Index(
      "s7_t",
      """{"mappings":{"properties":{"id":{"type":"keyword"},"ip":{"type":"ip"},""" +
      """"ck":{"type":"constant_keyword","value":"C"},"k":{"type":"keyword"}}},"settings":{}}"""
    ).schema
    val origins = Seq("mapping" -> mapping, "field caps" -> caps("s7_t", es8Table))
    def refusal(sql: String, table: Table): Either[String, Unit] =
      statement(sql).update(Some(table)).validateResolved()
    for {
      (origin, table) <- origins
      c               <- Seq("ip", "ck")
      op              <- Seq("+", "-", "*", "/", "%")
      shape <- Seq(
        "SELECT id, %s AS v FROM s7_t",
        "SELECT id FROM s7_t WHERE %s > 0",
        "SELECT %s AS v, COUNT(*) AS n FROM s7_t GROUP BY v",
        "SELECT id FROM s7_t ORDER BY %s"
      )
    } {
      val sql = shape.replace("%s", s"$c $op 1")
      withClue(s"[$origin] $sql: ") {
        // refused, in the words a plain keyword column `k` takes at the same place: the
        // validator's single-index KEYWORD rule, no message of its own
        val control = refusal(shape.replace("%s", s"k $op 1"), table)
        control.isLeft shouldBe true
        refusal(sql, table) shouldBe control.left.map(_.replace(s"k $op 1", s"$c $op 1"))
      }
    }
    origins.foreach { case (origin, table) =>
      withClue(origin) {
        statement("SELECT id, ip + 1 AS v FROM s7_t")
          .update(Some(table))
          .validateResolved() shouldBe
        Left("Type mismatch: output 'KEYWORD' is not compatible with input 'BIGINT'")
        // listed as unchanged: a cast, and a SUM Elasticsearch answers for
        statement("SELECT id, CAST(ip AS VARCHAR) AS v FROM s7_t")
          .update(Some(table))
          .validateResolved() shouldBe Right(())
      }
    }
  }

  /** A-69 (F1): the ONE comparison rule (lead, 2026-10-05) and the date-arithmetic rule (10-04)
    * read a D1 column by its declared type, as a native column of that type: a comparison with
    * another type family and a TIMESTAMP multiplied or divided are refused, with the native
    * column's message, on the mapping origin and the `_field_caps` one; a cast is the remedy.
    */
  "a D1 column beside another type family" should "be refused as a native column of its type (A-69)" in {
    val mapping = Index(
      "s7_t",
      """{"mappings":{"properties":{"id":{"type":"keyword"},"k":{"type":"keyword"},""" +
      """"n":{"type":"long"},"x":{"type":"double"},"r":{"type":"float"},"d":{"type":"date"},""" +
      """"sf":{"type":"scaled_float","scaling_factor":100},"hf":{"type":"half_float"},""" +
      """"u":{"type":"unsigned_long"},"dn":{"type":"date_nanos"},"ip":{"type":"ip"},""" +
      """"ck":{"type":"constant_keyword","value":"C"}}},"settings":{}}"""
    ).schema
    val origins = Seq("mapping" -> mapping, "field caps" -> caps("s7_t", es8Table))
    def refusal(sql: String, table: Table): Either[String, Unit] =
      statement(sql).update(Some(table)).validateResolved()
    // (the D1 column, the native column of its SQL type, the expression over `%c`)
    val comparisons: Seq[(String, String, String)] =
      Seq("sf" -> "x", "hf" -> "r", "u" -> "n", "dn" -> "d").map { case (c, n) =>
        (c, n, "%c = k")
      } ++
      Seq("ip" -> "k", "ck" -> "k", "dn" -> "d").map { case (c, n) => (c, n, "%c > n") } ++
      Seq("sf" -> "x", "hf" -> "r", "u" -> "n", "ip" -> "k", "ck" -> "k").map { case (c, n) =>
        (c, n, "%c > d")
      } ++ Seq(("dn", "d", "%c * 2 > 1"), ("dn", "d", "%c / 2 > 1"))
    val places = Seq(
      "SELECT id FROM s7_t WHERE %s",
      "SELECT id, CASE WHEN %s THEN 1 ELSE 0 END AS v FROM s7_t"
    )
    for {
      (origin, table)   <- origins
      (c, native, expr) <- comparisons
      place             <- places
    } {
      val sql = place.replace("%s", expr.replace("%c", c))
      withClue(s"[$origin] $sql: ") {
        val control = refusal(place.replace("%s", expr.replace("%c", native)), table)
        control.isLeft shouldBe true
        refusal(sql, table) shouldBe control.left.map(_.replaceAll(s"\\b$native\\b", c))
      }
    }
    // the messages, as measured over the mapping (the review's census of the branch)
    refusal("SELECT id FROM s7_t WHERE sf = k", mapping) shouldBe
    Left("Type mismatch: 'DOUBLE' is not compatible with 'KEYWORD' in expression: sf = k")
    refusal("SELECT id FROM s7_t WHERE dn > n", mapping) shouldBe
    Left("Type mismatch: 'TIMESTAMP' is not compatible with 'BIGINT' in expression: dn > n")
    // a projection of `dn * 2` / `dn / 2` takes the date-arithmetic refusal too
    Seq("*", "/").foreach { op =>
      refusal(s"SELECT id, dn $op 2 AS v FROM s7_t", mapping).left.getOrElse("") should startWith(
        s"Type mismatch: operator $op cannot be applied to TIMESTAMP and BIGINT in expression: dn $op 2"
      )
    }
    // the remedy: a cast to text compares as text
    origins.foreach { case (origin, table) =>
      withClue(origin) {
        Seq(
          "SELECT id FROM s7_t WHERE CAST(u AS VARCHAR) = k",
          "SELECT id, CASE WHEN CAST(u AS VARCHAR) = k THEN 1 ELSE 0 END AS v FROM s7_t"
        ).foreach(sql => withClue(sql)(refusal(sql, table) shouldBe Right(())))
      }
    }
  }
}
