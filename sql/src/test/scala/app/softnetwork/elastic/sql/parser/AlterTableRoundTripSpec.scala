package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Every statement the engine can RENDER must parse back — `MaterializedViewExtension` executes
  * `client.run(alter.sql)` and writes the same string into a user-runnable `.sql` artifact, so a
  * rendering the grammar rejects is a runtime failure, not a cosmetic one.
  *
  * The family this pins down was found one member at a time: `AlterColumnField` rendered `SET
  * FIELD` while only `ADD FIELD` parsed, `AlterColumnType` rendered `SET TYPE` against the
  * grammar's `SET DATA TYPE`, `AlterTable`/`AlterPipeline` put `IF EXISTS` after the name, and `SET
  * DEFAULT _ingest.timestamp` — the shape the MV machinery emits for `_last_updated` — could be
  * rendered but not parsed. Enumerating the whole surface is the only way this stays closed.
  */
class AlterTableRoundTripSpec extends AnyFlatSpec with Matchers {

  private val statements = Seq(
    "ALTER TABLE users ADD COLUMN last_login TIMESTAMP",
    "ALTER TABLE users ADD COLUMN IF NOT EXISTS last_login TIMESTAMP",
    "ALTER TABLE users DROP COLUMN old_field",
    "ALTER TABLE users DROP COLUMN IF EXISTS old_field",
    "ALTER TABLE users RENAME COLUMN old_name TO new_name",
    "ALTER TABLE users ALTER COLUMN name SET DATA TYPE KEYWORD",
    "ALTER TABLE users ALTER COLUMN age SET SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))",
    "ALTER TABLE users ALTER COLUMN age DROP SCRIPT",
    "ALTER TABLE users ALTER COLUMN name SET DEFAULT 'unknown'",
    "ALTER TABLE users ALTER COLUMN name DROP DEFAULT",
    "ALTER TABLE users ALTER COLUMN name SET NOT NULL",
    "ALTER TABLE users ALTER COLUMN name DROP NOT NULL",
    "ALTER TABLE users ALTER COLUMN name SET COMMENT 'Full name'",
    "ALTER TABLE users ALTER COLUMN name DROP COMMENT",
    "ALTER TABLE users ALTER COLUMN name SET OPTION (fielddata = true)",
    "ALTER TABLE users ALTER COLUMN name ADD OPTION (fielddata = true)",
    "ALTER TABLE users ALTER COLUMN name DROP OPTION fielddata",
    "ALTER TABLE users ALTER COLUMN name SET FIELDS (raw KEYWORD)",
    "ALTER TABLE users ALTER COLUMN name SET FIELD raw KEYWORD",
    "ALTER TABLE users ALTER COLUMN name ADD FIELD raw KEYWORD",
    "ALTER TABLE users ALTER COLUMN name DROP FIELD raw",
    "ALTER TABLE users SET MAPPING _meta.owner = 'analytics'",
    "ALTER TABLE users ADD MAPPING _meta.owner = 'analytics'",
    "ALTER TABLE users DROP MAPPING _meta.owner",
    "ALTER TABLE users SET SETTING index.refresh_interval = '1s'",
    "ALTER TABLE users DROP SETTING index.refresh_interval",
    "ALTER TABLE users SET ALIAS recent = (routing = 'user1')",
    "ALTER TABLE users DROP ALIAS recent",
    // the shapes TableDiff renders for a materialized view
    "ALTER TABLE users ALTER COLUMN _last_updated SET DEFAULT _ingest.timestamp",
    "ALTER TABLE users SET MAPPING _meta.columns._last_updated.default_value = _ingest.timestamp",
    // IF EXISTS on the table itself, and the multi-statement (parenthesised) form
    "ALTER TABLE IF EXISTS users DROP COLUMN old_field",
    """ALTER TABLE users (
      |	ADD COLUMN a INT,
      |	DROP COLUMN b
      |)""".stripMargin,
    """ALTER TABLE IF EXISTS users (
      |	ADD COLUMN a INT,
      |	DROP COLUMN b
      |)""".stripMargin
  )

  "every ALTER TABLE statement" should "re-parse the SQL it renders" in {
    statements.foreach { sql =>
      val parsed = Parser(sql)
      withClue(s"did not parse: $sql -> ") { parsed.isRight shouldBe true }
      val stmt = parsed.toOption.get
      withClue(s"rendering of [$sql] was [${stmt.sql}] which ") {
        Parser(stmt.sql) shouldBe Right(stmt)
      }
    }
  }

  "an ALTER PIPELINE statement" should "re-parse the SQL it renders" in {
    Seq(
      """ALTER PIPELINE user_pipeline ADD PROCESSOR SET (field = "status", value = "active")""",
      """ALTER PIPELINE IF EXISTS user_pipeline ADD PROCESSOR SET (field = "status", value = "active")"""
    ).foreach { sql =>
      val parsed = Parser(sql)
      withClue(s"did not parse: $sql -> ") { parsed.isRight shouldBe true }
      val rendered = parsed.toOption.get.sql
      // Rendering fixed point rather than AST equality: two `AddPipelineProcessor`s that render
      // identically still compare unequal (a processor field that never reaches the SQL), which
      // is pre-existing and out of this fix's scope. What must hold is that the emitted SQL
      // parses and survives a second round.
      withClue(s"rendering of [$sql] was [$rendered] which ") {
        Parser(rendered).map(_.sql) shouldBe Right(rendered)
      }
    }
  }

  "a string carrying quotes or backslashes" should "survive rendering" in {
    // `SET COMMENT 'it's here'` used to be emitted verbatim and could not be parsed back.
    Seq(
      "ALTER TABLE users ALTER COLUMN name SET COMMENT 'it\\'s here'",
      "ALTER TABLE users ALTER COLUMN name SET DEFAULT 'C:\\\\data'",
      "ALTER TABLE users SET MAPPING _meta.note = 'don\\'t drop'"
    ).foreach { sql =>
      val parsed = Parser(sql)
      withClue(s"did not parse: $sql -> ") { parsed.isRight shouldBe true }
      val stmt = parsed.toOption.get
      withClue(s"rendering of [$sql] was [${stmt.sql}] which ") {
        Parser(stmt.sql) shouldBe Right(stmt)
      }
    }
  }

  "a parser guard" should "return a Left rather than throw" in {
    // These run inside a combinator action; they used to `throw` straight out of `Parser.apply`,
    // whose signature promises Either[ParserError, Statement].
    Seq(
      "ALTER TABLE users (ADD COLUMN a INT",
      "ALTER TABLE users ADD COLUMN a INT)",
      "ALTER TABLE users ADD COLUMN a INT, ADD COLUMN b INT",
      "ALTER PIPELINE p (ADD PROCESSOR SET (field = \"a\", value = \"b\")"
    ).foreach { sql =>
      withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
      withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    }
  }
}
