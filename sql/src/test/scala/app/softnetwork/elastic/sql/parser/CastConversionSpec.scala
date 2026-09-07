package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.5 part C1 — a `CAST` over a text/keyword COLUMN emitted NO conversion at all.
  *
  * 🔴 This defect is INVISIBLE to every other spec in this module, and that is exactly how it
  * survived: a schema-less parse leaves an identifier's `baseType` at `SQLTypes.Any`, so
  * `SQLTypeUtils.coerce` falls through for EVERY column and "an identifier operand's Painless is
  * just the raw doc value" looks like the whole story. It is not. The fixture below attaches a real
  * schema (`ss.update(Some(schema))`), which is the only way `baseType` becomes the mapped column's
  * type and the arms are reached at all.
  *
  * Measured broken before this story: the `VARCHAR -> NUMERIC` and `VARCHAR -> TEMPORAL` arms
  * matched the `SQLTypes.Varchar` case object. No Elasticsearch mapping ever reports `VARCHAR` —
  * every string field is `text` or `keyword` — so the one arm that worked was unreachable from a
  * real index, and `CAST(name AS BIGINT)` emitted the raw string. Its twin, C2 (numeric narrowing),
  * lives in `NarrowingCastSpec` — separate arms, separate revert boundary.
  *
  * Never pin the BEFORE strings — they are the silent wrong answers this story fixes.
  */
class CastConversionSpec extends AnyFlatSpec with Matchers {

  /** The mapping a real index produces: string fields are `text`/`keyword`, NEVER `varchar`.
    * `legacy` is the one type that already converted, kept so the fix can be shown to be a
    * generalisation rather than a replacement.
    */
  private val schema: Table = Table(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("descr", SQLTypes.Text),
      Column("legacy", SQLTypes.Varchar),
      Column("amount", SQLTypes.Double),
      Column("ratio", SQLTypes.Real),
      Column("big", SQLTypes.BigInt),
      Column("n", SQLTypes.Int),
      Column("small", SQLTypes.SmallInt)
    )
  )

  /** Parse, then re-run `update` WITH the schema, then extract the emitted Painless of the first
    * select field. `GenericIdentifier.baseType` is `col.map(_.dataType)` and `col` is populated
    * from `request.schema` inside `update` — that chain is what the schema-less probe cannot see.
    */
  private def painlessOf(sql: String, s: Table = schema): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(s)).select.fields.head.painless(None)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def columnType(name: String): SQLType =
    schema.columns.find(_.name == name).map(_.dataType).getOrElse(fail(s"no column $name"))

  // -- C1 - string SOURCES ----------------------------------------------------
  "a cast over a text or keyword column" should "emit the real numeric conversion" in {
    // The headline shape of #275: over a real index this used to return the string unconverted.
    painlessOf("SELECT CAST(name AS BIGINT) FROM t") should include("Long.parseLong")
    painlessOf("SELECT CAST(descr AS BIGINT) FROM t") should include("Long.parseLong")
    painlessOf("SELECT CAST(name AS INT) FROM t") should include("Integer.parseInt")
    painlessOf("SELECT CAST(name AS DOUBLE) FROM t") should include("Double.parseDouble")
    painlessOf("SELECT CAST(name AS REAL) FROM t") should include("Float.parseFloat")
    painlessOf("SELECT CAST(name AS SMALLINT) FROM t") should include("Short.parseShort")
    painlessOf("SELECT CAST(name AS TINYINT) FROM t") should include("Byte.parseByte")
    // ... and via #275's own new spelling, which is what made the gap visible.
    painlessOf("SELECT CAST(name AS SIGNED) FROM t") should include("Long.parseLong")
    painlessOf("SELECT CAST(name AS DECIMAL(10,2)) FROM t") should include("Double.parseDouble")
  }

  it should "emit the real temporal conversion" in {
    painlessOf("SELECT CAST(name AS DATE) FROM t") should include("LocalDate.parse")
    painlessOf("SELECT CAST(descr AS DATE) FROM t") should include("LocalDate.parse")
    painlessOf("SELECT CAST(name AS TIME) FROM t") should include("LocalTime.parse")
    painlessOf("SELECT CAST(name AS DATETIME) FROM t") should include("LocalDateTime.parse")
    painlessOf("SELECT CAST(name AS TIMESTAMP) FROM t") should include("ZonedDateTime.parse")
  }

  it should "still convert a VARCHAR column exactly as before (a generalisation, not a swap)" in {
    painlessOf("SELECT CAST(legacy AS BIGINT) FROM t") should include("Long.parseLong")
    painlessOf("SELECT CAST(legacy AS DATE) FROM t") should include("LocalDate.parse")
  }

  it should "keep a string-to-string cast an identity or a String.valueOf, never a parse" in {
    // The identity arm precedes the widened target arm, so `keyword -> keyword` stays untouched.
    // Asserted EXACTLY, not as `should not include "parse"`: an absence matcher is satisfied by any
    // wrong emission at all, so it cannot tell "the identity arm fired" from "something else broke".
    painlessOf("SELECT CAST(name AS KEYWORD) FROM t") shouldBe
    "(doc['name'].size() == 0 ? null : doc['name'].value)"
    painlessOf("SELECT CAST(name AS VARCHAR) FROM t") should include("String.valueOf")
  }

  it should "give a mixed TEXT/KEYWORD set a string least-common-super-type (L4)" in {
    // `leastCommonSuperType` asked `distinct.contains(SQLTypes.Varchar)` — the same
    // case-object-equality hole as the `coerce` arms above, 100 lines up the same file, and NEWLY
    // REACHABLE because this story made `CAST(x AS TEXT)` / `AS KEYWORD` legal targets. Without the
    // widening a mixed set fell through every branch to `SQLTypes.Any`.
    SQLTypeUtils.leastCommonSuperType(
      List(SQLTypes.Text, SQLTypes.Keyword)
    ) shouldBe SQLTypes.Varchar
    SQLTypeUtils.leastCommonSuperType(
      List(SQLTypes.Keyword, SQLTypes.Int)
    ) shouldBe SQLTypes.Varchar
    SQLTypeUtils.leastCommonSuperType(
      List(SQLTypes.Varchar, SQLTypes.Text)
    ) shouldBe SQLTypes.Varchar
    // A single-element set still short-circuits to itself, and a non-string set is untouched.
    SQLTypeUtils.leastCommonSuperType(List(SQLTypes.Keyword)) shouldBe SQLTypes.Keyword
    SQLTypeUtils.leastCommonSuperType(List(SQLTypes.Int, SQLTypes.Double)) shouldBe SQLTypes.Double
  }

  it should "leave a NON-string column's numeric cast alone" in {
    // The widening is `_: SQLVarchar`, so it cannot capture a numeric source. `SQLChar` is
    // deliberately excluded too — no Elasticsearch mapping produces it.
    columnType("amount") shouldBe SQLTypes.Double
    painlessOf("SELECT CAST(amount AS DOUBLE) FROM t") shouldBe
    "(doc['amount'].size() == 0 ? null : doc['amount'].value)"
  }
}
