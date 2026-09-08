package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.5 — the DDL round trip, and why `Column.dataType` must stay the DECLARED type.
  *
  * `_meta.columns.<c>.data_type` records what the user wrote, and `IndexField.apply` prefers it
  * over Elasticsearch's own mapping type, so a table declared `birthdate DATE` reads back as DATE
  * and the round trip is exact.
  *
  * 🔴 This spec exists because that was briefly broken. An earlier revision mapped an ES `date` to
  * `Timestamp` inside `SQLTypes.apply(IndexField)` — the right intent in the wrong layer, since it
  * conflated the DECLARED type with the type Painless sees at RUNTIME. The declared column then
  * re-serialised its `_meta` as TIMESTAMP, and the mappings diff reported
  * `MappingSet(_meta.columns.birthdate.data_type, 'DATE')` on a table nobody had altered — a
  * spurious change on every existing index with a date column, on every `CREATE TABLE IF NOT
  * EXISTS`, table diff and ALTER planning pass. The workaround (excluding `_meta.columns` from the
  * mappings diff) then emptied that diff, which rerouted ALTER into a pipeline render that does not
  * parse on ES 6.8, and slowed materialized-view creation past its timeout.
  *
  * All of that is gone: the runtime type is derived where it is needed, at
  * `GenericIdentifier.baseType` via `SQLTypeUtils.runtimeType`, and `Column.dataType` is left
  * alone. So the mappings diff compares `_meta.columns` in full again, and is clean because the
  * data genuinely round-trips.
  */
class ColumnMetaDiffSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  private def schemaOf(sql: String): Table =
    Parser(sql).toOption.get match {
      case ct: CreateTable => ct.schema
      case other           => fail(s"expected a CreateTable, got $other")
    }

  /** Serialise a declared table to index mappings/settings and read it back, exactly as a client
    * does against a live index.
    */
  private def roundTrip(declared: Table): Table = {
    val mappings = mapper.createObjectNode()
    mappings.set("mappings", declared.indexMappings)
    val settings = mapper.createObjectNode()
    settings.set("settings", declared.indexSettings)
    Index(name = declared.name, mappings = mappings, settings = settings).schema
  }

  "a DATE column" should "survive the round trip with its declared spelling and NO diff" in {
    val declared = schemaOf("CREATE TABLE users (id INT NOT NULL, birthdate DATE)")
    val actual = roundTrip(declared)

    // 🔴 The declared spelling SURVIVES the round trip. If this ever reads TIMESTAMP again, the
    // runtime type has leaked back into `Column.dataType` and the spurious-diff cascade is back.
    declared.find("birthdate").map(_.dataType.typeId) shouldBe Some("DATE")
    actual.find("birthdate").map(_.dataType.typeId) shouldBe Some("DATE")

    // ...and nothing is reported as changed, with `_meta.columns` fully compared
    val diff = actual.diff(declared)
    withClue(s"columns=${diff.columns} mappings=${diff.mappings}: ") {
      diff.columns shouldBe empty
      diff.mappings shouldBe empty
    }
  }

  it should "still report every column property a _meta entry carries" in {
    // Falsification of the exclusion: if `Column.diff` did NOT cover one of these, dropping
    // `_meta.columns` would have silently stopped detecting it. Each pair differs in exactly one
    // property of a `_meta.columns` entry, and each must still be reported.
    val cases: Seq[(String, String, String)] = Seq(
      (
        "data_type",
        "CREATE TABLE t (c INT)",
        "CREATE TABLE t (c VARCHAR)"
      ),
      (
        "not_null",
        "CREATE TABLE t (c INT)",
        "CREATE TABLE t (c INT NOT NULL)"
      ),
      (
        "comment",
        "CREATE TABLE t (c INT)",
        "CREATE TABLE t (c INT COMMENT 'hello')"
      ),
      (
        "default_value",
        "CREATE TABLE t (c INT)",
        "CREATE TABLE t (c INT DEFAULT 1)"
      ),
      (
        "multi_fields",
        "CREATE TABLE t (c VARCHAR)",
        "CREATE TABLE t (c VARCHAR FIELDS(raw KEYWORD))"
      )
    )
    cases.foreach { case (property, aSql, bSql) =>
      withClue(s"[$property] ") {
        val a = schemaOf(aSql)
        val b = schemaOf(bSql)
        a.diff(b).columns should not be empty
      }
    }
  }
}
