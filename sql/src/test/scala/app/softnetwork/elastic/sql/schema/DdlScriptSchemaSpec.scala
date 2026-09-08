package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.8 part G — a DDL `SCRIPT AS (…)` built its ingest processor with NO schema attached.
  *
  * #306 (story 21.5) made the schema reach the QUERY execution path. It never reached the DDL one:
  * the processor is derived at PARSE time, when the column list does not exist yet, so every
  * operand's `baseType` was `Any`, no `SQLTypeUtils.coerce` arm fired, and the ingest script stored
  * the UNCONVERTED operand.
  *
  * MEASURED before the fix:
  * {{{
  * CREATE TABLE t (zip_code KEYWORD, zip_n BIGINT SCRIPT AS (CAST(zip_code AS BIGINT)))
  *   -> ctx.zip_n = param1            // the keyword STRING, into a `long`-mapped field
  * }}}
  * `zip_code KEYWORD` is declared in the same statement and was not consulted. Nothing failed:
  * Elasticsearch never rewrites `_source` and `coerce` defaults true, so the mapping stayed correct
  * while the document was wrong — the #205/#253 silent-wrong-value family, invisible to every
  * mapping-level check.
  *
  * 🔴 The fix is ONE seam, `Table.update()`, and that is what covers both DDL sites.
  * `CreateTable.schema` builds its table from the parsed columns and ends there; `Table.merge`
  * applies `ALTER … SET SCRIPT AS` to the LIVE table and ends there too. The spec expected `ALTER`
  * to need a client-side schema load because the STATEMENT carries no column list — but the point
  * where it is APPLIED already holds the whole table, so neither site needs I/O and neither adds
  * anything to the parse path.
  */
class DdlScriptSchemaSpec extends AnyFlatSpec with Matchers {

  private def createdColumns(sql: String): List[Column] =
    Parser(sql) match {
      case Right(ct: CreateTable) => ct.schema.columns
      case other                  => fail(s"[$sql] expected a CreateTable, got $other")
    }

  private def sourceOf(cols: List[Column], column: String): String =
    cols
      .find(_.name == column)
      .flatMap(_.script)
      .map(_.source)
      .getOrElse(fail(s"no script processor for column [$column] in ${cols.map(_.name)}"))

  private def createSource(sql: String, column: String): String =
    sourceOf(createdColumns(sql), column)

  // -- AC-G1: CREATE TABLE ----------------------------------------------------------------------

  "a computed column over a sibling KEYWORD" should "convert on ingest" in {
    // The headline shape. Verified end to end on a real Elasticsearch 8.18.3: this exact `source`
    // run as an ingest pipeline over {"zip_code":"75001"} stores `"zip_n": 75001` — the NUMBER.
    // 🔴 Asserted on the stored `_source` there, never on the mapping and never on a read path: a
    // correct mapping and a wrong `_source` coexist silently, and a read path cannot tell a stored
    // string from a read-time conversion.
    val source = createSource(
      "CREATE TABLE t (zip_code KEYWORD, zip_n BIGINT SCRIPT AS (CAST(zip_code AS BIGINT)))",
      "zip_n"
    )
    source should include("Long.parseLong")
    source should include("ctx.zip_n =")
  }

  "a computed column that NARROWS a sibling numeric" should "convert on ingest" in {
    // The C2 (narrowing) arms, equally unreachable from DDL before this story. Verified on ES
    // 8.18.3: {"amount":12.9} stores `"cents": 12`.
    createSource(
      "CREATE TABLE t (amount DOUBLE, cents BIGINT SCRIPT AS (CAST(amount AS BIGINT)))",
      "cents"
    ) should include("((long) param1)")
  }

  // -- AC-G3: the ingest-context guard ----------------------------------------------------------

  "a TEMPORAL-source arm" should "NOT fire in ingest context, byte for byte" in {
    // 🔴 In an ingest script the operand is `ctx.<field>` — the RAW JSON scalar — not the temporal
    // object a query's `doc['f'].value` yields, so an arm keyed on a temporal SOURCE must decline.
    // Attaching the schema is what makes those arms reachable for the first time, which turns that
    // guard from theoretical into load-bearing. The expected bytes are the PRE-fix emission,
    // captured on the baseline: this column's script must not have moved at all.
    createSource(
      "CREATE TABLE t (created DATE, y INTEGER SCRIPT AS (YEAR(created)))",
      "y"
    ) shouldBe "def param1 = ctx.created.get(ChronoField.YEAR); ctx.y = param1"
  }

  "a string function over a sibling KEYWORD" should "be unchanged" in {
    // A keyword is a `String` in BOTH contexts, so this arm was always correct and must stay so.
    createSource(
      "CREATE TABLE t (a KEYWORD, b KEYWORD SCRIPT AS (UPPER(a)))",
      "b"
    ) shouldBe "def param1 = ctx.a; ctx.b = (param1 == null) ? null : param1.toUpperCase()"
  }

  // -- OQ-4: an operand the schema cannot resolve -----------------------------------------------

  "an operand that names no declared column" should "stay silent and emit the identity" in {
    // The lead's OQ-4 ruling. A `CREATE TABLE` that works today keeps working: the conversion
    // simply does not fire, exactly as before, rather than the statement being rejected.
    createSource(
      "CREATE TABLE t (zip_n BIGINT SCRIPT AS (CAST(absent_column AS BIGINT)))",
      "zip_n"
    ) shouldBe "def param1 = ctx.absent_column; ctx.zip_n = param1"
  }

  // -- the DDL text is not disturbed ------------------------------------------------------------

  "the rendered DDL" should "round-trip unchanged" in {
    // `Column.sql` renders ` SCRIPT AS (<sql>)` from the processor's `script` text, and the
    // re-derivation must not perturb it: materialized-view deployment renders a stage's schema to
    // DDL and runs the text, so a render that stopped re-parsing would break deployment, not a test.
    val sql = "CREATE TABLE t (zip_code KEYWORD, zip_n BIGINT SCRIPT AS (CAST(zip_code AS BIGINT)))"
    val rendered = createdColumns(sql).map(_.sql).mkString(", ")
    rendered should include("zip_n BIGINT SCRIPT AS (CAST(zip_code AS BIGINT))")
    Parser(s"CREATE TABLE t2 ($rendered)") match {
      case Right(_: CreateTable) => succeed
      case other                 => fail(s"generated DDL did not re-parse: $other")
    }
  }

  // -- AC-G2: the ALTER site --------------------------------------------------------------------

  "ALTER TABLE ... SET SCRIPT AS" should "convert against the LIVE table" in {
    // The site the spec expected to need a client-side schema load. `Table.merge` applies the
    // statement to the table that was loaded from Elasticsearch and then calls `update()`, so the
    // whole column list is already in hand.
    val live = Table(
      "t",
      columns = List(Column("zip_code", SQLTypes.Keyword), Column("zip_n", SQLTypes.BigInt))
    ).update()
    val statements =
      Parser("ALTER TABLE t ALTER COLUMN zip_n SET SCRIPT AS (CAST(zip_code AS BIGINT))") match {
        case Right(at: AlterTable) => at.statements
        case other                 => fail(s"expected an AlterTable, got $other")
      }
    sourceOf(live.merge(statements).columns, "zip_n") should include("Long.parseLong")
  }

  it should "stay silent when the live table does not know the operand" in {
    val live = Table("t", columns = List(Column("zip_n", SQLTypes.BigInt))).update()
    val statements =
      Parser("ALTER TABLE t ALTER COLUMN zip_n SET SCRIPT AS (CAST(absent AS BIGINT))") match {
        case Right(at: AlterTable) => at.statements
        case other                 => fail(s"expected an AlterTable, got $other")
      }
    sourceOf(live.merge(statements).columns, "zip_n") shouldBe
    "def param1 = ctx.absent; ctx.zip_n = param1"
  }

  // -- the load path keeps what it read ---------------------------------------------------------

  "a processor read back from Elasticsearch" should "keep its stored source untouched" in {
    // It has JSON and no AST, so `expr` is `None` and `resolvedAgainst` returns it unchanged. That
    // is what keeps this change from rewriting pipelines it merely looked at.
    val stored = ScriptProcessor(
      script = "CAST(zip_code AS BIGINT)",
      column = "zip_n",
      dataType = SQLTypes.BigInt,
      source = "def param1 = ctx.zip_code; ctx.zip_n = param1"
    )
    stored.expr shouldBe None
    val table = Table(
      "t",
      columns = List(
        Column("zip_code", SQLTypes.Keyword),
        Column("zip_n", SQLTypes.BigInt, script = Some(stored))
      )
    ).update()
    sourceOf(table.columns, "zip_n") shouldBe "def param1 = ctx.zip_code; ctx.zip_n = param1"
  }
}
