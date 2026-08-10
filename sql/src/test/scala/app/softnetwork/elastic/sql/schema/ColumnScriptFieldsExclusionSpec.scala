package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, AlterTableStatement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A column is either multi-field or script-defined, never both — the grammar offers one or the
  * other (`ident ~ extension_type ~ (script | optionalMultiFields)`). `Table.merge` did not honour
  * that: `ALTER COLUMN … SET SCRIPT AS` kept the existing sub-fields, and the resulting `Table.sql`
  * — what SHOW CREATE TABLE and the diff statements emit — rendered `name TEXT FIELDS (…) SCRIPT
  * AS (…)`, which cannot be parsed back.
  */
class ColumnScriptFieldsExclusionSpec extends AnyFlatSpec with Matchers {

  private def statementsOf(sql: String): Seq[AlterTableStatement] =
    Parser(sql) match {
      case Right(AlterTable(_, _, statements)) => statements
      case other                               => fail(s"Expected an AlterTable, got $other")
    }

  private val withFields: Table =
    Parser("CREATE TABLE t (id INT, name TEXT FIELDS (raw KEYWORD), PRIMARY KEY (id))") match {
      case Right(ct: app.softnetwork.elastic.sql.query.CreateTable) =>
        Table(
          name = ct.table,
          columns = ct.ddl.getOrElse(fail("expected columns")),
          primaryKey = ct.primaryKey
        ).update()
      case other => fail(s"Expected a CreateTable, got $other")
    }

  private def columnOf(table: Table, name: String): Column =
    table.columns.find(_.name == name).getOrElse(fail(s"no column $name"))

  "setting a script" should "drop the sub-fields the column had" in {
    val merged =
      withFields.merge(statementsOf("ALTER TABLE t ALTER COLUMN name SET SCRIPT AS (UPPER(name))"))
    val c = columnOf(merged, "name")
    c.script shouldBe defined
    c.multiFields shouldBe empty
  }

  "declaring sub-fields" should "drop a script the column had" in {
    val scripted =
      withFields.merge(statementsOf("ALTER TABLE t ALTER COLUMN name SET SCRIPT AS (UPPER(name))"))
    val refielded =
      scripted.merge(statementsOf("ALTER TABLE t ALTER COLUMN name SET FIELDS (raw KEYWORD)"))
    val c = columnOf(refielded, "name")
    c.multiFields.map(_.name) should contain("raw")
    c.script shouldBe empty
  }

  it should "drop a script when a single field is added" in {
    val scripted =
      withFields.merge(statementsOf("ALTER TABLE t ALTER COLUMN name SET SCRIPT AS (UPPER(name))"))
    val refielded =
      scripted.merge(statementsOf("ALTER TABLE t ALTER COLUMN name ADD FIELD raw KEYWORD"))
    columnOf(refielded, "name").script shouldBe empty
  }

  "the merged table" should "still render DDL that parses" in {
    val merged =
      withFields.merge(statementsOf("ALTER TABLE t ALTER COLUMN name SET SCRIPT AS (UPPER(name))"))
    withClue(s"rendered [${merged.sql}] ") { Parser(merged.sql).isRight shouldBe true }
  }

  "a table holding an impossible column" should "fail validation rather than render invalid DDL" in {
    // Reachable outside SQL — `IndexField.ddlColumn` fills `script` and `multiFields` from a live
    // mapping independently.
    val impossible = withFields.copy(columns =
      withFields.columns.map {
        case c if c.name == "name" =>
          c.copy(script = columnOf(
            withFields.merge(
              statementsOf("ALTER TABLE t ALTER COLUMN name SET SCRIPT AS (UPPER(name))")
            ),
            "name"
          ).script)
        case c => c
      }
    )
    impossible.columns.find(_.name == "name").exists(_.multiFields.nonEmpty) shouldBe true
    impossible.validate().isLeft shouldBe true
    impossible.validate().swap.toOption.get should include("cannot declare both FIELDS and SCRIPT AS")
  }

  "a plain column" should "still validate" in {
    withFields.validate() shouldBe Right(())
    Table(name = "u", columns = List(Column("id", SQLTypes.Int))).update().validate() shouldBe Right(
      ()
    )
  }
}
