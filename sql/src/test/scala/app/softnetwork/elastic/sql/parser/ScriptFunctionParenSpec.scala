package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `identifierWithFunction` closed with `rep1(end)` — one or more parentheses — so it consumed a
  * `)` belonging to whatever production enclosed it. Inside `SCRIPT AS ( … )` that is the wrapper's
  * own closing paren, which made every name-only extractor function unusable in a generated column:
  * `age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate))` — the published example — could not
  * be parsed, while `SELECT YEAR(x)` always worked because nothing enclosed it.
  */
class ScriptFunctionParenSpec extends AnyFlatSpec with Matchers {

  private def scriptOf(sql: String): String =
    Parser(sql) match {
      case Right(CreateTable(_, Right(columns), _, _, _, _, _)) =>
        columns
          .find(_.script.isDefined)
          .flatMap(_.script.map(_.sql))
          .getOrElse(fail(s"no scripted column in [$sql]"))
      case other => fail(s"Expected a CreateTable, got $other")
    }

  "the documented CREATE TABLE example" should "parse" in {
    val sql =
      """CREATE TABLE users (
        |  id INT,
        |  birthdate DATE,
        |  age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate)),
        |  PRIMARY KEY (id)
        |)""".stripMargin
    Parser(sql).isRight shouldBe true
    scriptOf(sql) should include("YEAR")
  }

  "every date-part extractor" should "be usable inside SCRIPT AS" in {
    // Name-only extractors — the family that reaches the generic function path.
    Seq(
      "YEAR",
      "MONTH",
      "DAY",
      "WEEK",
      "QUARTER",
      "EPOCHDAY",
      "YEARDAY",
      "HOUR",
      "MINUTE",
      "SECOND"
    )
      .foreach { fn =>
        val sql = s"ALTER TABLE users ALTER COLUMN part SET SCRIPT AS ($fn(birthdate))"
        withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
      }
  }

  it should "still work for the functions that were never affected" in {
    Seq(
      "ALTER TABLE users ALTER COLUMN d SET SCRIPT AS (WEEKDAY(birthdate))",
      "ALTER TABLE users ALTER COLUMN a SET SCRIPT AS (ABS(salary))",
      "ALTER TABLE users ALTER COLUMN n SET SCRIPT AS (UPPER(name))",
      "ALTER TABLE users ALTER COLUMN g SET SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))",
      "ALTER TABLE users ALTER COLUMN p SET SCRIPT AS (birthdate + 1)"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  "a nested function call" should "still consume its own parentheses" in {
    // The balance has to hold at every depth, in the context where nothing encloses it …
    // `SELECT ABS(YEAR(x))` is deliberately absent: wrapping an extractor in a math function does
    // not parse on main either — a separate, pre-existing limitation, not this boundary.
    Seq(
      "SELECT YEAR(createdAt) FROM t",
      "SELECT YEAR(DATE_TRUNC(createdAt, MONTH)) FROM t",
      "SELECT MAX(YEAR(DATE_TRUNC(createdAt, MINUTE))) AS m FROM t GROUP BY id",
      "SELECT YEAR(CURRENT_DATE) - YEAR(birthdate) FROM t"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
    // … and where one does.
    Seq(
      "ALTER TABLE t ALTER COLUMN c SET SCRIPT AS (YEAR(DATE_TRUNC(createdAt, MONTH)))",
      "ALTER TABLE t ALTER COLUMN c SET SCRIPT AS (DATE_TRUNC(createdAt, MONTH))"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
  }

  "a malformed script body" should "say so, instead of blaming a parenthesis elsewhere" in {
    // The scanner's own diagnostics only surface as `Error`: a `Failure` is discarded by both call
    // sites — `column`'s `script | optionalMultiFields` keeps the alternative's Success, and
    // `alterTable`'s `repsep` accepts zero statements — which reported `')' expected but 'S'
    // found`, the exact symptom this production exists to eliminate.
    val unbalanced = Parser("CREATE TABLE t (a INT SCRIPT AS (b + 1")
    unbalanced.isLeft shouldBe true
    unbalanced.swap.toOption.get.msg should include("unbalanced parentheses in SCRIPT AS")

    val noParen = Parser("CREATE TABLE t (a INT SCRIPT AS b)")
    noParen.isLeft shouldBe true
    noParen.swap.toOption.get.msg should include("'(' expected after SCRIPT AS")

    val badBody = Parser("ALTER TABLE t ALTER COLUMN a SET SCRIPT AS (@@@)")
    badBody.isLeft shouldBe true
    badBody.swap.toOption.get.msg should include("Invalid SCRIPT AS expression")
  }

  "a scripted column" should "re-parse the SQL it renders" in {
    val sql = "ALTER TABLE users ALTER COLUMN age SET SCRIPT AS (YEAR(birthdate))"
    val stmt = Parser(sql).toOption.getOrElse(fail(s"did not parse: $sql"))
    stmt shouldBe a[AlterTable]
    Parser(stmt.sql) shouldBe Right(stmt)
  }
}
