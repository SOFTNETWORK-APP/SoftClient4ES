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
import app.softnetwork.elastic.sql.{SQLKeywords, StringValue}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.query.TemporalLiterals
import app.softnetwork.elastic.sql.query.TemporalLiterals.FieldFormat
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** SCHEMA-7 (D1, D2, A-60..A-62, A-73, A-75) -- the Elasticsearch mapping types core reads but
  * never writes, read and written back, and a `type` a DDL statement writes.
  *
  *   - READ (D1): `SQLTypes.apply` gives each its SQL type -- `scaled_float` DOUBLE, `half_float`
  *     REAL, `unsigned_long` BIGINT, `date_nanos` TIMESTAMP, `ip` and `constant_keyword` KEYWORD;
  *     `flattened` stays ANY.
  *   - WRITE (D2): `IndexField.apply` keeps the mapping's `type` as an option exactly when it is
  *     not the one elasticsql writes for that SQL type -- never for a field elasticsql created --
  *     and `Column.mappingType` is THE Elasticsearch type of the column for every consumer that
  *     turns it into an Elasticsearch-side decision: the mapping an ALTER re-sends (`node`), a
  *     sort's `unmapped_type`, a proven GROUP BY key kind, the date format, the doc-value reading,
  *     a type change of `diff` (A-61). A SET DATA TYPE to another type drops it; `ARRAY<X>` of its
  *     own type keeps it (A-60); a `constant_keyword` column keeps its type (A-62).
  */
class MappingTypeRoundTripSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  /** The fixture of `MappingTypeReadingSpec` (`s7_t`), as `GET s7_t` maps it: no `_meta`. */
  private val s7Properties: String =
    """"id":{"type":"keyword"},
      |"k":{"type":"keyword"},
      |"sf":{"type":"scaled_float","scaling_factor":100},
      |"hf":{"type":"half_float"},
      |"u":{"type":"unsigned_long"},
      |"dn":{"type":"date_nanos"},
      |"ip":{"type":"ip","fields":{"kw":{"type":"keyword"}}},
      |"ck":{"type":"constant_keyword","value":"C"}""".stripMargin

  private def raw(name: String, properties: String): Table =
    Index(name, s"""{"mappings":{"properties":{$properties}},"settings":{}}""").schema

  /** The six typed fields, plus `id` and `k`: every column re-parses from `SHOW CREATE`. */
  private lazy val typed: Table = raw("s7_t", s7Properties)

  /** With the two types that stay `ANY`-typed (a `flattened` and a `geo_shape` field). */
  private lazy val s7: Table =
    raw(
      "s7_t",
      s7Properties + ""","fl":{"type":"flattened"},"gs":{"type":"geo_shape"}"""
    )

  private def column(table: Table, name: String): Column =
    table.find(name).getOrElse(fail(s"no column $name"))

  private def alter(table: Table, sql: String): Table =
    Parser(sql) match {
      case Right(at: AlterTable) => table.merge(at.statements)
      case other                 => fail(s"[$sql] is not an ALTER TABLE: $other")
    }

  /** The table core reads back after `table` was written: its mapping and `_meta`, as `GET` answers
    * them.
    */
  private def reRead(table: Table): Table = {
    val mappings = mapper.createObjectNode()
    mappings.set("mappings", table.indexMappings)
    val settings = mapper.createObjectNode()
    settings.set("settings", table.indexSettings)
    Index(name = table.name, mappings = mappings, settings = settings).schema
  }

  private def resolved(table: Table, sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(table))
      case other                   => fail(s"[$sql] $other")
    }

  // ------------------------------------------------------------------------------------------
  // D1 -- read
  // ------------------------------------------------------------------------------------------

  "SQLTypes.apply" should "give each mapping type core reads but never writes its SQL type (D1)" in {
    SQLTypes("scaled_float") shouldBe SQLTypes.Double
    SQLTypes("half_float") shouldBe SQLTypes.Real
    SQLTypes("unsigned_long") shouldBe SQLTypes.BigInt
    SQLTypes("date_nanos") shouldBe SQLTypes.Timestamp
    SQLTypes("ip") shouldBe SQLTypes.Keyword
    SQLTypes("constant_keyword") shouldBe SQLTypes.Keyword
    // a STRUCT per value, of keys no mapping declares: value-typed (Q3 / R-7)
    SQLTypes("flattened") shouldBe SQLTypes.Any
    // and so does a mapping read through the declaration seam
    Seq(
      "sf" -> SQLTypes.Double,
      "hf" -> SQLTypes.Real,
      "u"  -> SQLTypes.BigInt,
      "dn" -> SQLTypes.Timestamp,
      "ip" -> SQLTypes.Keyword,
      "ck" -> SQLTypes.Keyword,
      "fl" -> SQLTypes.Any,
      "gs" -> SQLTypes.Any
    ).foreach { case (c, t) => withClue(c)(column(s7, c).dataType shouldBe t) }
  }

  // ------------------------------------------------------------------------------------------
  // D2 -- the kept `type`, `mappingType`
  // ------------------------------------------------------------------------------------------

  "IndexField.apply" should "keep the mapping's type for the seven types and geo_shape, and only them" in {
    Seq(
      "sf" -> "scaled_float",
      "hf" -> "half_float",
      "u"  -> "unsigned_long",
      "dn" -> "date_nanos",
      "ip" -> "ip",
      "ck" -> "constant_keyword",
      "fl" -> "flattened",
      "gs" -> "geo_shape"
    ).foreach { case (c, t) =>
      withClue(c) {
        column(s7, c).options.get("type") shouldBe Some(StringValue(t))
        column(s7, c).mappingType shouldBe t
        column(s7, c).foreignMapping shouldBe true
      }
    }
    Seq("id", "k", "ip.kw").foreach { c =>
      withClue(c) {
        column(s7, c).options.get("type") shouldBe None
        column(s7, c).mappingType shouldBe "keyword"
        column(s7, c).foreignMapping shouldBe false
      }
    }
  }

  it should "keep no type for any field elasticsql created, lists and CHAR included" in {
    val scalars = SQLKeywords.typeWords.toSeq.sorted.flatMap { word =>
      Parser(s"CREATE TABLE ddl_probe (c $word)") match {
        case Right(ct: CreateTable) => Some(word -> ct)
        case _                      => None
      }
    }
    scalars.map(_._1) should contain allOf ("CHAR", "KEYWORD", "TIMESTAMP", "DOUBLE", "REAL")
    val lists = scalars.flatMap { case (word, _) =>
      Parser(s"CREATE TABLE ddl_probe (c ARRAY<$word>)") match {
        case Right(ct: CreateTable) => Some(s"ARRAY<$word>" -> ct)
        case _                      => None
      }
    }
    lists.map(_._1) should contain allOf ("ARRAY<KEYWORD>", "ARRAY<INT>", "ARRAY<CHAR>")
    (scalars ++ lists).foreach { case (word, ct) =>
      withClue(s"[$word] ") {
        val created = ct.schema
        val read = reRead(created)
        val c = column(read, "c")
        c.options.get("type") shouldBe None
        c.foreignMapping shouldBe false
        c.mappingType shouldBe SQLTypeUtils.elasticType(column(created, "c").dataType)
        // DESCRIBE and SHOW CREATE show no change, nor does a diff against the created table
        read.diff(created).columns shouldBe empty
      }
    }
  }

  "Column.mappingType" should "write the field as it is mapped, its parameters and multi-fields kept" in {
    val sf = column(s7, "sf").node
    sf.get("type").asText() shouldBe "scaled_float"
    sf.get("scaling_factor").asInt() shouldBe 100
    column(s7, "ck").node.get("type").asText() shouldBe "constant_keyword"
    column(s7, "ck").node.get("value").asText() shouldBe "C"
    column(s7, "fl").node.get("type").asText() shouldBe "flattened"
    column(s7, "gs").node.get("type").asText() shouldBe "geo_shape"
    val ip = column(s7, "ip").node
    ip.get("type").asText() shouldBe "ip"
    ip.has("properties") shouldBe false
    ip.get("fields").get("kw").get("type").asText() shouldBe "keyword"
    // the kept `type` is written once, by `mappingType`
    Seq("sf", "hf", "u", "dn", "ip", "ck", "fl", "gs").foreach { c =>
      withClue(c)(column(s7, c).node.get("type").asText() shouldBe column(s7, c).mappingType)
    }
  }

  it should "decide a sort's unmapped_type: none for a foreign type, as before it was typed" in {
    def key(c: String): Option[String] =
      resolved(s7, s"SELECT id FROM s7_t ORDER BY $c").orderBy
        .flatMap(_.sorts.headOption)
        .flatMap(_.unmappedType)
    Seq("sf", "hf", "u", "dn", "ip", "ck").foreach(c => withClue(c)(key(c) shouldBe None))
    key("k") shouldBe Some("keyword")
  }

  it should "never make a foreign type a proven GROUP BY key kind" in {
    // `'10.0.0.1'` and `'::ffff:10.0.0.1'` name one `ip` key, two in a keyword's lists: the
    // combination is refused on `ip` as before it was typed, decided on the native keyword
    def having(c: String, a: String, b: String): Either[String, Unit] =
      resolved(
        s7,
        s"SELECT $c AS g, COUNT(*) AS n FROM s7_t GROUP BY $c " +
        s"HAVING $c = '$a' AND ($c IS NOT NULL OR NOT $c = '$b')"
      ).validateResolved()
    having("ip", "10.0.0.1", "::ffff:10.0.0.1").isLeft shouldBe true
    having("k", "a", "b") shouldBe Right(())
  }

  it should "give a date_nanos field its own default format (D3)" in {
    FieldFormat.of(column(s7, "dn")).spec shouldBe TemporalLiterals.DefaultDateNanosFormat
    FieldFormat.of(column(raw("t", """"d":{"type":"date"}"""), "d")).spec shouldBe
    TemporalLiterals.DefaultDateFormat
  }

  // ------------------------------------------------------------------------------------------
  // ALTER -- AC5, A-60, A-61, A-62
  // ------------------------------------------------------------------------------------------

  "ALTER TABLE ADD COLUMN" should "keep every foreign field's mapping type and parameters" in {
    val desired = alter(s7, "ALTER TABLE s7_t ADD COLUMN z INT")
    val diff = s7.diff(desired)
    diff.columns.map(_.getClass.getSimpleName) shouldBe List("ColumnAdded")
    Seq("sf", "hf", "u", "dn", "ip", "ck", "fl", "gs").foreach { c =>
      withClue(c)(column(desired, c).node shouldBe column(s7, c).node)
    }
    // and the field read back after the `_meta` it now carries is still the field it was
    val written = reRead(desired)
    Seq("sf", "hf", "u", "dn", "ip", "ck").foreach { c =>
      withClue(c) {
        column(written, c).mappingType shouldBe column(s7, c).mappingType
        column(written, c).dataType shouldBe column(s7, c).dataType
      }
    }
  }

  "ALTER COLUMN SET DATA TYPE" should "drop the kept type for another type, and reindex" in {
    val desired = alter(s7, "ALTER TABLE s7_t ALTER COLUMN ip SET DATA TYPE VARCHAR")
    val ip = column(desired, "ip")
    ip.options.get("type") shouldBe None
    ip.mappingType shouldBe "text"
    ip.node.get("type").asText() shouldBe "text"
    val diff = s7.diff(desired)
    diff.columns.collect { case c: ColumnTypeChanged => (c.name, c.from, c.to) } shouldBe
    List(("ip", SQLTypes.Keyword, SQLTypes.Varchar))
    diff.safety shouldBe UnsafeReindex
  }

  it should "keep the mapping type for ARRAY<X> of its own type: a _meta-only Safe change (A-60)" in {
    Seq(
      "sf" -> "ARRAY<DOUBLE>",
      "hf" -> "ARRAY<REAL>",
      "u"  -> "ARRAY<BIGINT>",
      "dn" -> "ARRAY<TIMESTAMP>",
      "ip" -> "ARRAY<KEYWORD>",
      "ck" -> "ARRAY<KEYWORD>"
    ).foreach { case (c, list) =>
      withClue(s"$c $list: ") {
        val desired = alter(s7, s"ALTER TABLE s7_t ALTER COLUMN $c SET DATA TYPE $list")
        column(desired, c).mappingType shouldBe column(s7, c).mappingType
        column(desired, c).node.get("type").asText() shouldBe column(s7, c).mappingType
        val diff = s7.diff(desired)
        diff.columns shouldBe empty
        diff.mappings.collect { case MappingSet(key, _) => key } shouldBe
        List(s"_meta.columns.$c.data_type")
        diff.safety shouldBe Safe
      }
    }
  }

  it should "compare Elasticsearch types: date_nanos to DATE reindexes into a date (A-61)" in {
    val desired = alter(s7, "ALTER TABLE s7_t ALTER COLUMN dn SET DATA TYPE DATE")
    column(desired, "dn").mappingType shouldBe "date"
    column(desired, "dn").options.get("type") shouldBe None
    val diff = s7.diff(desired)
    diff.columns.collect { case c: ColumnTypeChanged => (c.name, c.from, c.to) } shouldBe
    List(("dn", SQLTypes.Timestamp, SQLTypes.Date))
    diff.safety shouldBe UnsafeReindex
  }

  it should "refuse to move a constant_keyword column off its type, saying why (A-62)" in {
    val desired = alter(s7, "ALTER TABLE s7_t ALTER COLUMN ck SET DATA TYPE VARCHAR")
    val diff = s7.diff(desired)
    val changes = diff.columns.collect { case c: ColumnTypeChanged => c }
    changes.map(_.name) shouldBe List("ck")
    changes.head.safety shouldBe Impossible
    changes.head.reason.getOrElse("") should include("constant_keyword")
    diff.safety shouldBe Impossible
    // a change between two other types is decided by the two SQL types alone, as before
    ColumnTypeChanged("c", SQLTypes.Keyword, SQLTypes.Varchar)().reason shouldBe None
    ColumnTypeChanged("c", SQLTypes.Keyword, SQLTypes.Varchar)().safety shouldBe UnsafeReindex
  }

  // ------------------------------------------------------------------------------------------
  // A-73 -- a `type` written in the DDL
  // ------------------------------------------------------------------------------------------

  private def created(sql: String): CreateTable =
    Parser(sql) match {
      case Right(ct: CreateTable) => ct
      case other                  => fail(s"[$sql] is not a CREATE TABLE: $other")
    }

  /** A `type` option written in a `CREATE TABLE` maps the field with it, as before the types were
    * read (main's mapping of each, measured), whatever the SQL type reads as.
    */
  "A type written in the DDL" should "map the field with it, as it did before (A-73)" in {
    Seq(
      "c VARCHAR OPTIONS (type = 'keyword')"      -> "keyword",
      "c KEYWORD OPTIONS (type = 'wildcard')"     -> "wildcard",
      "c KEYWORD OPTIONS (type = 'ip')"           -> "ip",
      "c TEXT OPTIONS (type = 'match_only_text')" -> "match_only_text"
    ).foreach { case (definition, written) =>
      withClue(s"[$definition] ") {
        val ct = created(s"CREATE TABLE t (id KEYWORD, $definition)")
        ct.columns.find(_.name == "c").map(_.node.toString) shouldBe Some(
          s"""{"type":"$written"}"""
        )
        ct.schema.indexMappings.get("properties").get("c").get("type").asText() shouldBe written
        column(ct.schema, "c").mappingType shouldBe written
      }
    }
  }

  it should "be dropped by a retype to another type, and kept by ARRAY of its own (A-73, A-60)" in {
    val table = created("CREATE TABLE t (id KEYWORD, c VARCHAR OPTIONS (type = 'keyword'))").schema
    val text = column(alter(table, "ALTER TABLE t ALTER COLUMN c SET DATA TYPE TEXT"), "c")
    text.options.get("type") shouldBe None
    text.mappingType shouldBe "text"
    val list =
      column(alter(table, "ALTER TABLE t ALTER COLUMN c SET DATA TYPE ARRAY<VARCHAR>"), "c")
    list.options.get("type") shouldBe Some(StringValue("keyword"))
    list.mappingType shouldBe "keyword"
    // the type the option reads as keeps it too: a KEYWORD mapped `keyword` is no foreign field
    val keyword = column(alter(table, "ALTER TABLE t ALTER COLUMN c SET DATA TYPE KEYWORD"), "c")
    keyword.mappingType shouldBe "keyword"
    keyword.foreignMapping shouldBe false
    // and a retype to itself changes nothing
    table.diff(alter(table, "ALTER TABLE t ALTER COLUMN c SET DATA TYPE VARCHAR")).columns shouldBe
    empty
  }

  it should "be read back from the mapping, re-sent by ALTER, and re-created by SHOW CREATE (A-73)" in {
    val table = created("CREATE TABLE t (id KEYWORD, c VARCHAR OPTIONS (type = 'keyword'))").schema
    // `_meta` declares VARCHAR, the mapping says `keyword`: the column reports the field's type
    val read = Index("t", s"""{"mappings":${table.indexMappings},"settings":{}}""").schema
    val c = column(read, "c")
    c.dataType shouldBe SQLTypes.Varchar
    c.mappingType shouldBe "keyword"
    c.node.get("type").asText() shouldBe "keyword"
    read.diff(table).columns shouldBe empty
    // an ALTER re-sends the field as it is mapped, never the `text` of its SQL type
    val added = alter(read, "ALTER TABLE t ADD COLUMN z INT")
    read.diff(added).columns.map(_.getClass.getSimpleName) shouldBe List("ColumnAdded")
    column(added, "c").node.get("type").asText() shouldBe "keyword"
    // SHOW CREATE TABLE prints it, and its text re-creates it
    val stmt = created(read.sql)
    Parser(stmt.sql) shouldBe Right(stmt)
    column(stmt.schema, "c").mappingType shouldBe "keyword"
    stmt.schema.indexMappings.get("properties").get("c").get("type").asText() shouldBe "keyword"
  }

  /** A-75: a sort over a declared type a DDL statement wrote another type for, which that type does
    * not read as (`KEYWORD` mapped `wildcard`, `INT` mapped `long`), carries the `unmapped_type` of
    * the declared type, byte for byte as before the types were read (main's bytes, measured). A
    * written type that reads as the declared one (`DOUBLE` mapped `scaled_float`, `TIMESTAMP`
    * mapped `date_nanos`, `KEYWORD` mapped `ip`) sorts by its own type, as a field read from its
    * mapping: none (D2; R-93).
    */
  it should "keep the unmapped_type of a declared type it does not read as (A-75)" in {
    val written = reRead(
      created(
        "CREATE TABLE d1 (id KEYWORD, w KEYWORD OPTIONS (type = 'wildcard'), " +
        "i INT OPTIONS (type = 'long'), c VARCHAR OPTIONS (type = 'keyword'), " +
        "sf DOUBLE OPTIONS (type = 'scaled_float', scaling_factor = 100), " +
        "dz TIMESTAMP OPTIONS (type = 'date_nanos'), ip KEYWORD OPTIONS (type = 'ip'), kk KEYWORD)"
      ).schema
    )
    // a list whose other index maps none of them
    val (list, _) =
      mergedSchema("d*", Seq("d1" -> written, "d2" -> raw("d2", """"id":{"type":"keyword"}""")))
    def key(table: Table, from: String, c: String): Option[String] =
      resolved(table, s"SELECT id, $c FROM $from ORDER BY $c LIMIT 10").orderBy
        .flatMap(_.sorts.headOption)
        .flatMap(_.unmappedType)
    Seq(written -> "d1", list -> "d*").foreach { case (table, from) =>
      withClue(s"FROM $from: ") {
        key(table, from, "w") shouldBe Some("keyword")
        key(table, from, "i") shouldBe Some("integer")
        // a VARCHAR's `text` sorts with no `unmapped_type`, as before
        key(table, from, "c") shouldBe None
        key(table, from, "kk") shouldBe Some("keyword")
        Seq("sf", "dz", "ip").foreach(c => withClue(s"$c: ")(key(table, from, c) shouldBe None))
      }
    }
  }

  // ------------------------------------------------------------------------------------------
  // SHOW CREATE -- AC5
  // ------------------------------------------------------------------------------------------

  "SHOW CREATE TABLE of the six typed fields" should "re-parse, and re-create the same mapping types" in {
    val text = typed.sql
    val stmt = Parser(text) match {
      case Right(ct: CreateTable) => ct
      case other                  => fail(s"[$text] does not re-parse: $other")
    }
    Parser(stmt.sql) shouldBe Right(stmt)
    val recreated = stmt.schema
    Seq("sf", "hf", "u", "dn", "ip", "ck").foreach { c =>
      withClue(c) {
        column(recreated, c).mappingType shouldBe column(typed, c).mappingType
        column(recreated, c).dataType shouldBe column(typed, c).dataType
      }
    }
    val properties = recreated.indexMappings.get("properties")
    properties.get("sf").get("scaling_factor").asInt() shouldBe 100
    properties.get("ck").get("value").asText() shouldBe "C"
    properties.get("ip").get("fields").get("kw").get("type").asText() shouldBe "keyword"
  }
}
