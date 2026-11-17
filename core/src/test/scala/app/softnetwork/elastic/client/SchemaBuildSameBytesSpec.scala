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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.schema.{FieldCaps, Index}
import app.softnetwork.elastic.sql.{ObjectValue, StringValue, Value}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.security.MessageDigest
import java.util.Locale
import scala.collection.immutable.{ListMap, TreeMap}

/** SCHEMA-17 fix R-155 (A-186) -- a schema is built without linear `ListMap` work per field, and
  * answers the SAME BYTES as before: the `_field_caps` path (`FieldCaps.read` -> each member's
  * document -> `Index(..).schema` -> `mergedSchema` -> `FieldCaps.table`), the mapping reader
  * (`Index(name, document).schema`: `IndexMappings.apply`, `IndexField.apply`, the `Value` map arm
  * that builds `_meta.columns`), and `Table.update()` (`cols`, `Table._meta`, `Column._meta`).
  *
  * Each cell renders what a consumer reads -- `Table.sql`, the columns (path, SQL type, mapping
  * type, order), the mapping core sends (`_meta` and `field_caps_undeclared` included) and, for a
  * `_field_caps` answer, each member's document and the undeclared leaves -- and is pinned by its
  * SHA-256, recorded on the tree before the change: a moved digest is a finding, never a re-pin.
  * Sizes 4 / 5 cross the boundary where an immutable `Map` turns from insertion order to hash
  * order; 2,000 is the size the cost was measured at.
  */
class SchemaBuildSameBytesSpec extends AnyFlatSpec with Matchers {

  private val mapper = JacksonConfig.objectMapper

  /** Cell -> SHA-256 of its bytes, recorded on the tree before the change (b8480d85). */
  private val pins: Map[String, String] = Map(
    "A/m1/flat"               -> "5adb595478355d695d3808e57e985da6b5a4c51f293862f0a2900a7f56ba0712",
    "A/m1/object"             -> "c0f4513fe6577c021a653808264d5559ca517374fd3e10295da2c9a0b356cbbe",
    "A/m1/multifield"         -> "b84c7b84aa6f00cba1443c14a2093b50a5852a88f05170ad235ba658b845afb2",
    "A/m1/lists"              -> "33c25507e319fd42dc090f2901be4955d7098e4aabec1f4c5c62bdd8910f563b",
    "A/m1/dates"              -> "8f4bfe00fbdc3b8b92e648c9e54c89890157f36d149a7eda5b10e0635c123a97",
    "A/m1/arrays"             -> "bcf9b9deadaca6163aacfd40760b7380b252974d59767e3f46ab3f0c35997be1",
    "A/m1/undeclared"         -> "49721306c81764cbca323a74aabf7601509acf91a69ff4757e07ab887060db92",
    "A/m1/sharedAncestor"     -> "9e00b12dff1004dd717a945736a4de0e530b07b2a61c21117c588161355444fd",
    "A/m1/mixed2002"          -> "e95fadad38aa7adb0ce957e9d6135fd13bf7aea59fe6d2b06c38767411bac8de",
    "A/m1/c5shape"            -> "b3be2779d1fc5b552e757d14f1981d25341e208497f8d282773356427c58e683",
    "A/m2/flat"               -> "334af73f493b0a6aa075c090c044211c18cbae8ce57164761b47f193bcfb2d67",
    "A/m2/object"             -> "c28afe3e33521fecf572dbe0e3d0b63e11b20ab23349bd6fe7c8c0d04cc52a4e",
    "A/m2/multifield"         -> "e72a2a1f47c47577ffe4768ac31e0dd621d48e5ef9732910077744e6877d038a",
    "A/m2/lists"              -> "b0eb40559e67e43ac06d2f82db17f99ef4c35a559f401b58c6f4586605347c7f",
    "A/m2/dates"              -> "11c9cb192777b90a2d463a7480805da58a068e71f855f42a82cbab0ef8150bb4",
    "A/m2/arrays"             -> "9f0403b7012bef600af428ef6c6332b2d38578955b7324342d988a3ea0f7a9b9",
    "A/m2/undeclared"         -> "78e936d315d10b4d8eb2e17278ea3ee10fe115cb1cf2d877ad052e78b51e18d4",
    "A/m2/sharedAncestor"     -> "2e1bd6e71b52907efe24f769db973b1ed0c260c68f3a1c4192a53107243dafaa",
    "A/m2/mixed2002"          -> "fb8219cd2cf80a197fab615c7eb4f8e66c58d3b6c661ba70ca3cf2900eda06e1",
    "A/m2/c5shape"            -> "ce03dad71d2bb55b86059e41e6f96808fce18b71ed0d7d5676e253df0c14d0a5",
    "B/absent/4/typeless"     -> "16d40acdc77814a67e3dd07301ff07caa8dc7bd73c12380166cf2b864e4e4696",
    "B/absent/4/_doc"         -> "16d40acdc77814a67e3dd07301ff07caa8dc7bd73c12380166cf2b864e4e4696",
    "B/absent/4/doc"          -> "16d40acdc77814a67e3dd07301ff07caa8dc7bd73c12380166cf2b864e4e4696",
    "B/absent/5/typeless"     -> "0166bad24c275eafef348f665e9681e0907a768d4ffe9f44dde9313359cf55ee",
    "B/absent/5/_doc"         -> "0166bad24c275eafef348f665e9681e0907a768d4ffe9f44dde9313359cf55ee",
    "B/absent/5/doc"          -> "0166bad24c275eafef348f665e9681e0907a768d4ffe9f44dde9313359cf55ee",
    "B/absent/2000/typeless"  -> "1ffb49325bf01ff8c814e7d9353876ea507c9691d84ebc42ea8de9d955daf82c",
    "B/absent/2000/_doc"      -> "1ffb49325bf01ff8c814e7d9353876ea507c9691d84ebc42ea8de9d955daf82c",
    "B/absent/2000/doc"       -> "1ffb49325bf01ff8c814e7d9353876ea507c9691d84ebc42ea8de9d955daf82c",
    "B/flat/4/typeless"       -> "6ac069bf1363651aec5f0b4fe41669597f44777fa4bea1d791fb11fa982e734c",
    "B/flat/4/_doc"           -> "6ac069bf1363651aec5f0b4fe41669597f44777fa4bea1d791fb11fa982e734c",
    "B/flat/4/doc"            -> "6ac069bf1363651aec5f0b4fe41669597f44777fa4bea1d791fb11fa982e734c",
    "B/flat/5/typeless"       -> "c2bcb5c6709765e41afd17371a7c29b15ab7c844573c400f83e1fb11f20852fe",
    "B/flat/5/_doc"           -> "c2bcb5c6709765e41afd17371a7c29b15ab7c844573c400f83e1fb11f20852fe",
    "B/flat/5/doc"            -> "c2bcb5c6709765e41afd17371a7c29b15ab7c844573c400f83e1fb11f20852fe",
    "B/flat/2000/typeless"    -> "6c745f3ff5ae3774116823ac4694fc7f1fe6c56a4bb55e3d28b7773b44c34163",
    "B/flat/2000/_doc"        -> "6c745f3ff5ae3774116823ac4694fc7f1fe6c56a4bb55e3d28b7773b44c34163",
    "B/flat/2000/doc"         -> "6c745f3ff5ae3774116823ac4694fc7f1fe6c56a4bb55e3d28b7773b44c34163",
    "B/nested/4/typeless"     -> "5f37c1af2bc21f7f28622d2d5c94852752dae9809a432aa874cf79ca9b8403a5",
    "B/nested/4/_doc"         -> "5f37c1af2bc21f7f28622d2d5c94852752dae9809a432aa874cf79ca9b8403a5",
    "B/nested/4/doc"          -> "5f37c1af2bc21f7f28622d2d5c94852752dae9809a432aa874cf79ca9b8403a5",
    "B/nested/5/typeless"     -> "89cb1c1c0cd30c9ee6af278137778167cb32ba1d403fa9fcd06ac71edd4fc65a",
    "B/nested/5/_doc"         -> "89cb1c1c0cd30c9ee6af278137778167cb32ba1d403fa9fcd06ac71edd4fc65a",
    "B/nested/5/doc"          -> "89cb1c1c0cd30c9ee6af278137778167cb32ba1d403fa9fcd06ac71edd4fc65a",
    "B/nested/2000/typeless"  -> "cf1a324b346ecf32cd59ae1fceb4a97aec23a50659c71019f90b0fbcaff807c8",
    "B/nested/2000/_doc"      -> "cf1a324b346ecf32cd59ae1fceb4a97aec23a50659c71019f90b0fbcaff807c8",
    "B/nested/2000/doc"       -> "cf1a324b346ecf32cd59ae1fceb4a97aec23a50659c71019f90b0fbcaff807c8",
    "B/fieldOrder/4/typeless" -> "9ef402d7a6e7e598f54173a0e346ebf86c82cc15fb19ecfe91914ff88f007d9f",
    "B/fieldOrder/4/_doc"     -> "9ef402d7a6e7e598f54173a0e346ebf86c82cc15fb19ecfe91914ff88f007d9f",
    "B/fieldOrder/4/doc"      -> "9ef402d7a6e7e598f54173a0e346ebf86c82cc15fb19ecfe91914ff88f007d9f",
    "B/fieldOrder/5/typeless" -> "32ec1e6a833918c83312cd5d10bb14117d3edc724e248fc8f69c3418f7efcb08",
    "B/fieldOrder/5/_doc"     -> "32ec1e6a833918c83312cd5d10bb14117d3edc724e248fc8f69c3418f7efcb08",
    "B/fieldOrder/5/doc"      -> "32ec1e6a833918c83312cd5d10bb14117d3edc724e248fc8f69c3418f7efcb08",
    "B/fieldOrder/2000/typeless" -> "93e5030a4897ae541d59a36a98059da5513249bd3b0703b9d2222869595033ef",
    "B/fieldOrder/2000/_doc" -> "93e5030a4897ae541d59a36a98059da5513249bd3b0703b9d2222869595033ef",
    "B/fieldOrder/2000/doc"  -> "93e5030a4897ae541d59a36a98059da5513249bd3b0703b9d2222869595033ef",
    "D/listMap6"             -> "902db63bbaaaeb01103ee8fd50ee64e4ddf0fbbc07e20e842c94a58e26c25254",
    "D/treeMap6"             -> "21c70662f268f99d01f4f1c53294ddf3bce1ce1432535b1b6ea4751b00544060",
    "D/map4"                 -> "84fbf16253c6961f238457709bfcd0c2996f4ddd9f8bd49fe4e2fbeb65dcc06e",
    "D/map5"                 -> "bbff7b3c0ea1678755331b22014ff416ef4b10b84d654d95856e19ab06f1830e",
    "D/seqOfMaps"            -> "a8f8f86a48cb2a5c9ee49c424c21d2380d3df460569ff258479ef4e34dac3d30",
    "D/nested"               -> "72b61fee176dd85440470aa8790c718735737f699da7218a2856b164c3e9b2b9",
    "D/json2000"             -> "c6f72428d90c3f3331acc0501a0ee3bbfcd4f96dd8bfaef57972b92bdaa61b45",
    "C/plain"                -> "940fb3c5a5a0772d10df74e3673c262a893b615641aff51d639fa1ec5efe073a",
    "C/pk"                   -> "2583ced3fae960e2a8357e035f8c99997e4837f9617bed60b58b4e03e54cd59c",
    "C/partition"            -> "1eb85f1ab1d7a1efd6c5dd9b11d1742cf54e960ed86ba9eb60037ed8d830f79c",
    "C/mvs"                  -> "20164cfcbe9b23877962b5951f4aa8137d06812569797011cc3c8f904434c7ea",
    "C/type"                 -> "9e4148d3fca348ed2b030582a98fafccf0569b3ba6842777fcba3ef0a314858c",
    "C/userMeta"             -> "82da1dec21da96931e9e99ce8bbc22add450d8679476af3e764680dfd910d185",
    "C/all"                  -> "af677b49810924381b1c7745fcc52fd8ff650c52bb93f846928f17846c7ff2a9",
    "C/lineage"              -> "d629728a1f69e6a044153b8839505b35fe6477e624298a4b167298dbb1ee6874",
    "C/repeated"             -> "de12be0e8a94cddbf30abc3a28e917cb6b660bb5e4d456e714fbeae8fb6db9cf",
    "C/wide2000"             -> "8de57470d8f3b5a18cb24abe8c0082176dbb058ff14bb7e0c54ebd93ce7677e3",
    "C/wide2000Meta"         -> "0647ad2c79eddc66d2872a2439c2594940924abb30e14657ab32169804b5d630"
  )

  // ---------------------------------------------------------------------------------------------
  // Rendering and pinning
  // ---------------------------------------------------------------------------------------------

  private def sha256(text: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(text.getBytes(StandardCharsets.UTF_8))
      .map(b => "%02x".formatLocal(Locale.ROOT, b))
      .mkString

  private def columnLines(columns: Seq[Column]): Seq[String] =
    columns.flatMap { c =>
      s"${c.path} ${c.dataType.typeId} ${c.mappingType}" +: columnLines(c.multiFields)
    }

  private def render(table: Table): String =
    Seq(
      "SQL",
      table.sql,
      "COLUMNS",
      columnLines(table.columns).mkString("\n"),
      "MAPPING",
      mapper.writeValueAsString(table.indexMappings)
    ).mkString("\n")

  /** Every cell of `population` is pinned and answers its pinned digest; a moved cell prints its
    * new digest.
    */
  private def check(population: String, cells: Seq[(String, String)]): Unit = {
    val digests = cells.map { case (name, bytes) => s"$population/$name" -> sha256(bytes) }
    val names = digests.map(_._1)
    names.distinct.size shouldBe names.size
    pins.keySet.filter(_.startsWith(s"$population/")) shouldBe names.toSet
    val moved = digests.filter { case (cell, digest) => !pins.get(cell).contains(digest) }
    moved.foreach { case (cell, digest) => println(s"R155-DIGEST $cell $digest") }
    moved.map(_._1) shouldBe empty
  }

  // ---------------------------------------------------------------------------------------------
  // Population A -- a read-only user's schema, typed from a `_field_caps` answer
  // ---------------------------------------------------------------------------------------------

  private val Type = "sql_type"
  private val Format = "sql_format"
  private val Lists = "sql_list_depths"

  /** One field of a `_field_caps` answer under one Elasticsearch type: core's `meta` keys, and the
    * indices mapping it so when the indices disagree.
    */
  private final case class Leaf(
    path: String,
    tpe: String,
    meta: Seq[(String, String)] = Nil,
    in: Seq[String] = Nil
  )

  private def quoted(s: String): String = "\"" + s + "\""

  /** The ES 8 shape: each field's `meta` merged per key (an array of values), a metadata field. */
  private def caps(members: Seq[String], leaves: Seq[Leaf]): String = {
    val byPath = scala.collection.mutable.LinkedHashMap.empty[String, Vector[Leaf]]
    leaves.foreach(l => byPath.update(l.path, byPath.getOrElse(l.path, Vector.empty) :+ l))
    val fields = byPath.toSeq.map { case (path, types) =>
      val entries = types.map { l =>
        val indices =
          if (l.in.isEmpty) "" else s""","indices":[${l.in.map(quoted).mkString(",")}]"""
        val meta =
          if (l.meta.isEmpty) ""
          else
            l.meta
              .map { case (k, v) => s"${quoted(k)}:[${quoted(v)}]" }
              .mkString(""","meta":{""", ",", "}")
        s"""${quoted(l.tpe)}:{"type":${quoted(l.tpe)},"metadata_field":false$indices$meta}"""
      }
      s"""${quoted(path)}:{${entries.mkString(",")}}"""
    }
    val metadata = """"_id":{"_id":{"type":"_id","metadata_field":true}}"""
    s"""{"indices":[${members.map(quoted).mkString(",")}],"fields":{${(metadata +: fields)
      .mkString(",")}}}"""
  }

  private def kinds(members: Seq[String]): Seq[(String, Seq[Leaf])] = Seq(
    "flat" -> Seq(
      Leaf("f1", "keyword", Seq(Type -> "keyword")),
      Leaf("f2", "long", Seq(Type -> "int"))
    ),
    "object" -> Seq(
      Leaf("o", "object"),
      Leaf("o.a", "integer", Seq(Type -> "int")),
      Leaf("o.b", "keyword", Seq(Type -> "varchar"))
    ),
    "multifield" -> Seq(
      Leaf("k", "text", Seq(Type -> "varchar")),
      Leaf("k.raw", "keyword", Seq(Type -> "keyword"))
    ),
    "lists" -> Seq(
      Leaf("l", "object"),
      Leaf("l.p", "integer", Seq(Type -> "int", Lists -> "1")),
      Leaf("l.q", "object"),
      Leaf("l.q.r", "keyword", Seq(Type -> "keyword", Lists -> "1,2"))
    ),
    "dates" -> Seq(
      Leaf("d", "date", Seq(Type -> "date", Format -> "dd/MM/yyyy")),
      Leaf(
        "dn",
        "date_nanos",
        Seq(Type -> "timestamp", Format -> "strict_date_optional_time_nanos")
      ),
      Leaf("d2", "date", Seq(Type -> "timestamp"))
    ),
    "arrays" -> (Seq(
      Leaf("tags", "keyword", Seq(Type -> "array<keyword>")),
      Leaf("nums", "long", Seq(Type -> "array<bigint>"))
    ) ++ (if (members.size > 1)
            Seq(
              Leaf("cf", "keyword", Seq(Type -> "keyword"), Seq(members.head)),
              Leaf("cf", "long", Seq(Type -> "bigint"), members.tail)
            )
          else Nil)),
    "undeclared" -> Seq(
      Leaf("u", "long"),
      Leaf("ip", "ip"),
      Leaf("o2", "object"),
      Leaf("o2.z", "keyword")
    ),
    "sharedAncestor" -> Seq(
      Leaf("l2", "object"),
      Leaf("l2.a", "integer", Seq(Type -> "int", Lists -> "1")),
      Leaf("l2.b", "keyword", Seq(Type -> "keyword", Lists -> "1"))
    )
  )

  /** Every kind above, 182 groups of 11 leaves (2,002), in no sorted order: a group's leaves come
    * before their parent, the groups in reverse.
    */
  private def mixed: Seq[Leaf] =
    (181 to 0 by -1).flatMap { g =>
      val p = "g%03d".formatLocal(Locale.ROOT, g)
      Seq(
        Leaf(s"${p}_f", "keyword", Seq(Type -> "keyword")),
        Leaf(s"${p}_o.a", "integer", Seq(Type -> "int")),
        Leaf(s"${p}_o", "object"),
        Leaf(s"${p}_k", "text", Seq(Type -> "varchar")),
        Leaf(s"${p}_k.raw", "keyword", Seq(Type -> "keyword")),
        Leaf(s"${p}_l.p", "integer", Seq(Type -> "int", Lists -> "1")),
        Leaf(s"${p}_l.q", "keyword", Seq(Type -> "keyword", Lists -> "1")),
        Leaf(s"${p}_l", "object"),
        Leaf(s"${p}_d", "date", Seq(Type -> "date", Format -> "dd/MM/yyyy")),
        Leaf(
          s"${p}_n",
          "date_nanos",
          Seq(Type -> "timestamp", Format -> "strict_date_optional_time_nanos")
        ),
        Leaf(s"${p}_t", "keyword", Seq(Type -> "array<keyword>")),
        Leaf(s"${p}_u", "long")
      )
    }

  /** The answer the cost was measured on: 1,500 top-level leaves and 50 objects of 10. */
  private def c5Shape: Seq[Leaf] = {
    val types = Seq(
      "keyword" -> "KEYWORD",
      "long"    -> "BIGINT",
      "integer" -> "INT",
      "double"  -> "DOUBLE",
      "date"    -> "TIMESTAMP",
      "boolean" -> "BOOLEAN",
      "text"    -> "TEXT",
      "float"   -> "REAL",
      "short"   -> "SMALLINT"
    )
    def leaf(path: String, i: Int): Leaf = {
      val (es, sql) = types(i % types.size)
      Leaf(
        path,
        es,
        if (es == "date") Seq(Type -> sql, Format -> "strict_date_optional_time")
        else Seq(Type              -> sql)
      )
    }
    (0 until 1500).map(i => leaf("f%04d".formatLocal(Locale.ROOT, i), i)) ++
    (0 until 50).flatMap { o =>
      val parent = "o%02d".formatLocal(Locale.ROOT, o)
      Leaf(parent, "object") +: (0 until 10).map(j => leaf(s"$parent.g$j", o * 3 + j))
    }
  }

  /** A user holding only the index `read` privilege: `GET` answers 403, `_field_caps` `answer`. */
  private final class ReadOnlyClient(answer: String) extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
      ElasticFailure(
        ElasticError(
          message = s"action [indices:admin/get] is unauthorized for user [ro] on indices [$index]",
          statusCode = Some(403),
          index = Some(index)
        )
      )

    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] =
      ElasticResult.success(Some(answer))
  }

  private def answered(json: String): String = {
    val answer = FieldCaps.read("src", mapper.readTree(json))
    val documents = answer.members.map { case (index, document) =>
      s"$index ${mapper.writeValueAsString(document)}"
    }
    val table = new ReadOnlyClient(json).loadQuerySchema("src") match {
      case ElasticSuccess(schema) => schema
      case ElasticFailure(error)  => fail(s"refused: ${error.message}")
    }
    Seq(
      "DOCUMENTS",
      documents.mkString("\n"),
      "UNDECLARED",
      answer.undeclared.toSeq.sorted.mkString(","),
      render(table),
      "TABLE-UNDECLARED",
      FieldCaps.undeclared(table).toSeq.sorted.mkString(",")
    ).mkString("\n")
  }

  "A read-only user's schema typed from _field_caps" should "answer the same bytes (population A)" in {
    val cells = Seq("m1" -> Seq("t"), "m2" -> Seq("m1", "m2")).flatMap { case (set, members) =>
      (kinds(members) ++ Seq("mixed2002" -> mixed, "c5shape" -> c5Shape)).map {
        case (kind, leaves) => s"$set/$kind" -> answered(caps(members, leaves))
      }
    }
    check("A", cells)
  }

  // ---------------------------------------------------------------------------------------------
  // Population B -- the mapping reader: `Index(name, document).schema`, and the `Value` map arm
  // ---------------------------------------------------------------------------------------------

  private def name(i: Int): String = "c%04d".formatLocal(Locale.ROOT, i)

  /** One column: its mapping property and its `_meta.columns` entry. */
  private def flatColumn(i: Int): (String, String) = i % 5 match {
    case 0 => """{"type":"keyword"}""" -> """{"data_type":"KEYWORD"}"""
    case 1 => """{"type":"integer"}""" -> """{"data_type":"INT","not_null":"true"}"""
    case 2 =>
      """{"type":"date","format":"yyyy-MM-dd"}""" -> """{"data_type":"DATE","comment":"day"}"""
    case 3 =>
      """{"type":"text","fields":{"raw":{"type":"keyword"}}}""" -> """{"data_type":"VARCHAR"}"""
    case _ => """{"type":"long","null_value":0}""" -> """{"data_type":"BIGINT","default_value":0}"""
  }

  /** A struct (5 sub-fields every 4th column, else 2) or a multi-field, declared with
    * `multi_fields` and, `ordered`, a `field_order` naming the sub-fields out of mapping order.
    */
  private def nestedColumn(i: Int, ordered: Boolean): (String, String) =
    if (i % 2 == 0) {
      val subs = if (i % 4 == 0) Seq("a", "b", "c", "d", "e") else Seq("a", "b")
      def mapped(j: Int): String = if (j % 2 == 0) "keyword" else "integer"
      def sqlType(j: Int): String = if (j % 2 == 0) "KEYWORD" else "INT"
      def comment(j: Int): String = if (j == 0) ",\"comment\":\"sub\"" else ""
      val properties = subs.zipWithIndex
        .map { case (s, j) => s"""${quoted(s)}:{"type":"${mapped(j)}"}""" }
        .mkString(",")
      val declared = subs.zipWithIndex.reverse
        .map { case (s, j) => s"""${quoted(s)}:{"data_type":"${sqlType(j)}"${comment(j)}}""" }
        .mkString(",")
      val order =
        if (ordered) s""","field_order":[${subs.reverse.map(quoted).mkString(",")}]""" else ""
      s"""{"type":"object","properties":{$properties}}""" ->
      s"""{"data_type":"STRUCT","multi_fields":{$declared}$order}"""
    } else {
      val order = if (ordered) ""","field_order":["raw","en"]""" else ""
      """{"type":"text","fields":{"en":{"type":"text"},"raw":{"type":"keyword"}}}""" ->
      s"""{"data_type":"VARCHAR","multi_fields":{"raw":{"data_type":"KEYWORD"},"en":{"data_type":"TEXT"}}$order}"""
    }

  private def mappingDocument(columns: String, size: Int, wrapper: String): String = {
    val entries = (0 until size).map { i =>
      columns match {
        case "nested"     => nestedColumn(i, ordered = false)
        case "fieldOrder" => nestedColumn(i, ordered = true)
        case _            => flatColumn(i)
      }
    }
    val properties =
      entries.zipWithIndex.map { case ((p, _), i) => s"${quoted(name(i))}:$p" }.mkString(",")
    val declared =
      if (columns == "absent") None
      else
        Some(entries.zipWithIndex.reverse.map { case ((_, m), i) => s"${quoted(name(i))}:$m" })
    // 3 top-level `_meta` keys at size 4 (insertion order), 5 beyond (hash order)
    val userKeys =
      if (size <= 4) Seq("created_by" -> "\"test\"", "purpose" -> "\"r155\"")
      else
        Seq(
          "created_by"  -> "\"test\"",
          "purpose"     -> "\"r155\"",
          "owner"       -> "\"ops\"",
          "primary_key" -> "[\"c0000\"]"
        )
    val meta = (userKeys ++ declared.map(d => "columns" -> d.mkString("{", ",", "}")))
      .map { case (k, v) => s"${quoted(k)}:$v" }
      .mkString("{", ",", "}")
    val body = s""""_meta":$meta,"properties":{$properties}"""
    val mappings = wrapper match {
      case "typeless" => s"{$body}"
      case "_doc"     => s"""{"_doc":{$body}}"""
      case _          => s"""{"doc":{$body}}"""
    }
    s"""{"b_idx":{"aliases":{},"settings":{"index":{"number_of_shards":"1"}},"mappings":$mappings}}"""
  }

  "A schema read from a mapping" should "answer the same bytes (population B)" in {
    val cells = for {
      columns <- Seq("absent", "flat", "nested", "fieldOrder")
      size    <- Seq(4, 5, 2000)
      wrapper <- Seq("typeless", "_doc", "doc")
    } yield s"$columns/$size/$wrapper" ->
    render(Index("b_idx", mappingDocument(columns, size, wrapper)).schema)
    check("B", cells)
  }

  "A Value read from a map" should "keep its keys, their order and its values (population B, Value cells)" in {
    val six: Seq[(String, Any)] = Seq(
      "zeta"  -> 1,
      "alpha" -> "a",
      "mu"    -> 2.5d,
      "beta"  -> true,
      "omega" -> Seq("x", "y"),
      "kappa" -> ListMap("in" -> 1L)
    )
    val json2000 = mapper.readTree(
      (0 until 2000)
        .map(i => s"""${quoted("k%04d".formatLocal(Locale.ROOT, (i * 7919) % 2000))}:$i""")
        .mkString("{", ",", "}")
    )
    val inputs: Seq[(String, Any)] = Seq(
      "listMap6"  -> ListMap(six: _*),
      "treeMap6"  -> TreeMap(six: _*),
      "map4"      -> Map(six.take(4): _*),
      "map5"      -> Map(six.take(5): _*),
      "seqOfMaps" -> Seq(ListMap(six: _*), Map(six.take(3): _*)),
      "nested"   -> Map("outer" -> TreeMap(six: _*), "x" -> 1, "inner" -> ListMap(six.reverse: _*)),
      "json2000" -> Value(json2000).getOrElse(fail("json2000 read nothing"))
    )
    // `ddl` renders an object's keys in their order: the digest pins the order too
    val cells = inputs.map { case (cell, input) =>
      val value = Value(input)
      cell -> s"${value.getClass.getSimpleName}\n${value.ddl}"
    }
    check("D", cells)
  }

  // ---------------------------------------------------------------------------------------------
  // Population C -- `Table.update()`: `cols`, `Table._meta`, `Column._meta`
  // ---------------------------------------------------------------------------------------------

  private def createTable(
    table: String,
    pk: Boolean,
    partition: Boolean,
    options: String
  ): Table = {
    val sql =
      s"""CREATE TABLE $table (
         |  id INT NOT NULL COMMENT 'user identifier',
         |  name VARCHAR FIELDS(raw KEYWORD COMMENT 'sortable') DEFAULT 'anonymous' OPTIONS (analyzer = 'french', search_analyzer = 'french'),
         |  birthdate DATE,
         |  age INT SCRIPT AS (YEAR(birthdate)),
         |  age_stored INT SCRIPT AS (YEAR(birthdate)) STORED,
         |  ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
         |  profile STRUCT FIELDS(
         |    bio VARCHAR,
         |    followers INT NOT NULL,
         |    join_date DATE COMMENT 'joined',
         |    seniority INT SCRIPT AS (DATEDIFF(CURRENT_DATE, profile.join_date, DAY))
         |  ) COMMENT 'user profile'${if (pk) ",\n  PRIMARY KEY (id)" else ""}
         |)${if (partition) " PARTITION BY birthdate (MONTH)" else ""}$options""".stripMargin
    Parser(sql) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"[$sql] $other")
    }
  }

  private val userMeta =
    """_meta = (created_by = "test", primary_key = ["zz"], partition_by = (column = "zz"), """ +
    """columns = (stale = (data_type = "INT")), materialized_views = ["old"], type = "changelog", """ +
    """purpose = "r155")"""

  private def wide(table: String, mappings: ListMap[String, Value[_]]): Table = {
    val types: Seq[SQLType] = Seq(SQLTypes.Keyword, SQLTypes.Int, SQLTypes.Date, SQLTypes.Varchar)
    Table(
      name = table,
      columns = (0 until 2000).map { i =>
        if (i % 10 == 0)
          Column(
            s"s$i",
            SQLTypes.Struct,
            multiFields = List(
              Column("b", SQLTypes.Int),
              Column("a", SQLTypes.Keyword, comment = Some("sub"))
            )
          )
        else
          Column(
            name(i),
            types(i % types.size),
            notNull = i % 3 == 0,
            comment = if (i % 7 == 0) Some(s"c$i") else None
          )
      }.toList,
      primaryKey = List(name(1)),
      mappings = mappings
    ).update()
  }

  "A table built by Table.update()" should "answer the same bytes (population C)" in {
    val withUserMeta: ListMap[String, Value[_]] = ListMap(
      "dynamic" -> StringValue("false"),
      "_meta" -> ObjectValue(
        ListMap(
          "created_by" -> StringValue("test"),
          "columns"    -> ObjectValue(ListMap("stale" -> ObjectValue(ListMap()))),
          "type"       -> StringValue("view"),
          "purpose"    -> StringValue("r155")
        )
      )
    )
    val lineage = Table(
      name = "c_lineage",
      columns = List(
        Column("id", SQLTypes.Keyword),
        Column(
          "total",
          SQLTypes.Double,
          lineage = ListMap(
            "p1" -> Seq(("orders", "amount"), ("orders_mv", "total")),
            "p2" -> Seq(("refunds", "amount"))
          )
        ),
        Column(
          "s",
          SQLTypes.Struct,
          multiFields = List(Column("a", SQLTypes.Int, lineage = ListMap("p1" -> Seq(("t", "a")))))
        )
      ),
      primaryKey = List("id")
    ).update()
    val repeated = Table(
      name = "c_repeated",
      columns = List(
        Column("x", SQLTypes.Keyword),
        Column(
          "s",
          SQLTypes.Struct,
          multiFields = List(
            Column("a", SQLTypes.Int),
            Column("b", SQLTypes.Keyword),
            Column("a", SQLTypes.BigInt, comment = Some("second a"))
          )
        ),
        Column("y", SQLTypes.Int),
        Column("x", SQLTypes.BigInt, comment = Some("second x"))
      )
    ).update()
    val cells = Seq(
      "plain"     -> createTable("c_plain", pk = false, partition = false, ""),
      "pk"        -> createTable("c_pk", pk = true, partition = false, ""),
      "partition" -> createTable("c_part", pk = true, partition = true, ""),
      "mvs" -> createTable(
        "c_mvs",
        pk = true,
        partition = false,
        """ OPTIONS (materialized_views = ["mv_a", "mv_b"])"""
      ),
      "type" -> createTable(
        "c_type",
        pk = true,
        partition = false,
        """ OPTIONS (mappings = (_meta = (type = "enrichment")))"""
      ),
      "userMeta" -> createTable(
        "c_user",
        pk = true,
        partition = true,
        s""", OPTIONS (mappings = (dynamic = false, $userMeta), materialized_views = ["mv_a"])"""
      ),
      "all" -> createTable(
        "c_all",
        pk = true,
        partition = true,
        s""", OPTIONS (mappings = ($userMeta), settings = (number_of_shards = "1"), """ +
        """materialized_views = ["mv_a", "mv_b", "mv_c"])"""
      ),
      "lineage"      -> lineage,
      "repeated"     -> repeated,
      "wide2000"     -> wide("c_wide", ListMap.empty),
      "wide2000Meta" -> wide("c_wide_meta", withUserMeta)
    ).map { case (cell, table) => cell -> render(table) }
    check("C", cells)
  }
}
