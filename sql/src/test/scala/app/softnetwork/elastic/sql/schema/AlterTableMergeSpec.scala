package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, AlterTableStatement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `Table.merge` is what turns an ALTER TABLE into the schema the gateway diffs against
  * Elasticsearch (`GatewayApi.run`: merge -> diff -> push). The single-field branches used to
  * compute their `copy` and discard it, returning the table unchanged: `SET|ADD FIELD` and `DROP
  * FIELD` produced an EMPTY diff, so nothing reached the cluster and the DDL still reported
  * success. Parser-level tests cannot see that — only asserting the merged schema can.
  */
class AlterTableMergeSpec extends AnyFlatSpec with Matchers {

  private def statementsOf(sql: String): Seq[AlterTableStatement] =
    Parser(sql) match {
      case Right(AlterTable(_, _, statements)) => statements
      case other                               => fail(s"Expected an AlterTable, got $other")
    }

  private val users: Table =
    Table(
      name = "users",
      columns = List(
        Column("id", SQLTypes.Int),
        Column("name", SQLTypes.Varchar)
      ),
      primaryKey = List("id")
    ).update()

  private def multiFieldsOf(table: Table, column: String): List[String] =
    table.columns.find(_.name == column).map(_.multiFields.map(_.name)).getOrElse(Nil)

  "merge" should "apply ALTER COLUMN ... SET FIELD" in {
    val merged =
      users.merge(statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELD raw KEYWORD"))
    multiFieldsOf(merged, "name") should contain("raw")
  }

  it should "apply ALTER COLUMN ... ADD FIELD" in {
    val merged =
      users.merge(statementsOf("ALTER TABLE users ALTER COLUMN name ADD FIELD raw KEYWORD"))
    multiFieldsOf(merged, "name") should contain("raw")
  }

  it should "produce a non-empty diff for SET FIELD" in {
    // The gateway pushes `schema.diff(merged)`; an unchanged merge means an empty diff, i.e. a
    // DDL that silently does nothing to the index.
    val merged =
      users.merge(statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELD raw KEYWORD"))
    users.diff(merged).columns should not be empty
  }

  it should "replace a sub-field of the same name rather than duplicate it" in {
    val withRaw =
      users.merge(statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELD raw KEYWORD"))
    val reAdded =
      withRaw.merge(statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELD raw VARCHAR"))
    multiFieldsOf(reAdded, "name").count(_ == "raw") shouldBe 1
    reAdded.columns
      .find(_.name == "name")
      .flatMap(_.multiFields.find(_.name == "raw"))
      .map(_.dataType.typeId) shouldBe Some("VARCHAR")
  }

  it should "apply ALTER COLUMN ... DROP FIELD" in {
    val withRaw =
      users.merge(statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELD raw KEYWORD"))
    multiFieldsOf(withRaw, "name") should contain("raw")

    val dropped = withRaw.merge(statementsOf("ALTER TABLE users ALTER COLUMN name DROP FIELD raw"))
    multiFieldsOf(dropped, "name") should not contain "raw"
    withRaw.diff(dropped).columns should not be empty
  }

  it should "still apply ALTER COLUMN ... SET FIELDS (...)" in {
    // Control for the plural branch this fix was modelled on.
    val merged = users.merge(
      statementsOf("ALTER TABLE users ALTER COLUMN name SET FIELDS (raw KEYWORD, en VARCHAR)")
    )
    multiFieldsOf(merged, "name") should contain allOf ("raw", "en")
  }
}
