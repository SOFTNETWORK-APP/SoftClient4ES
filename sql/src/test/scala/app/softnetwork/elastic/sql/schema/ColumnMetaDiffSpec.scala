package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.5 / issue #306 — the DDL round trip, and the `_meta.columns` exclusion it forced.
  *
  * Mapping an Elasticsearch `date` field to `Timestamp` made a table declared `birthdate DATE` read
  * back as TIMESTAMP. `Column.diff` was unmoved (it compares `SQLTypeUtils.elasticType`, and every
  * temporal type is the ES type `date`), but the raw JSON comparison of `_meta.columns` — a SHADOW
  * of those same columns, keyed on the SQL SPELLING — reported a change. One fact, two derivations,
  * disagreeing: a spurious mapping diff on every existing index with a date column.
  *
  * `_meta.columns` is therefore excluded from the mappings diff. This spec pins BOTH halves: that
  * the round trip is clean, and that excluding it costs nothing — every field a `_meta.columns`
  * entry carries is still caught, by the step that owns it.
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

  "a DATE column" should "survive the round trip with NO diff, though its spelling changes" in {
    val declared = schemaOf("CREATE TABLE users (id INT NOT NULL, birthdate DATE)")
    val actual = roundTrip(declared)

    // the spelling DOES move -- that is the #306 fix, and it is why the diff had to be checked
    declared.find("birthdate").map(_.dataType.typeId) shouldBe Some("DATE")
    actual.find("birthdate").map(_.dataType.typeId) shouldBe Some("TIMESTAMP")

    // ...and NOTHING is reported as changed, because Elasticsearch stores one `date` either way
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
