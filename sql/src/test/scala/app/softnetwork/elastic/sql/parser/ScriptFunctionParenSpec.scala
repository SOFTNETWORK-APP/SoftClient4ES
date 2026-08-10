package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `identifierWithFunction` closed with `rep1(end)` — one or more parentheses — so it consumed a
  * `)` belonging to whatever production enclosed it. Inside `SCRIPT AS ( … )` that is the wrapper's
  * own closing paren, which made every name-only extractor function unusable in a generated column:
  * `age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate))` — the published example — could not
  * be parsed, while `SELECT YEAR(x)` always worked because nothing enclosed it.
  */
class ScriptFunctionParenSpec extends AnyFlatSpec with Matchers {

  private def identifierOf(sql: String): Identifier =
    Parser(sql) match {
      case Right(s: SingleSearch) => s.select.fields.head.identifier
      case other                  => fail(s"Expected a SingleSearch, got $other")
    }

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

  "a scalar function wrapping a date-part extractor" should "parse (issue #220)" in {
    // Assert the emitted script, never just `isRight`. The round trip is blind to this whole
    // family: every rebalancing attempt that dropped `YEAR` from the function chain still rendered
    // `.sql` with `YEAR(…)` in it, and the only visible symptom was the painless script losing
    // `.get(ChronoField.YEAR)` — so a parse-succeeds assertion would have passed all three times.
    Seq(
      "ABS"   -> "Math.abs",
      "CEIL"  -> "Math.ceil",
      "FLOOR" -> "Math.floor",
      "SQRT"  -> "Math.sqrt",
      "ROUND" -> "Math.round",
      "SIGN"  -> "arg0 > 0 ? 1",
      "UPPER" -> "toUpperCase",
      "LOWER" -> "toLowerCase"
    ).foreach { case (fn, marker) =>
      val sql = s"SELECT $fn(YEAR(createdAt)) FROM t"
      withClue(s"[$sql] ") {
        identifierOf(sql).painless(None) should (include("ChronoField.YEAR") and include(marker))
      }
    }
    Seq(
      "YEAR"     -> "ChronoField.YEAR",
      "MONTH"    -> "ChronoField.MONTH_OF_YEAR",
      "DAY"      -> "ChronoField.DAY_OF_MONTH",
      "WEEK"     -> "IsoFields.WEEK_OF_WEEK_BASED_YEAR",
      "QUARTER"  -> "IsoFields.QUARTER_OF_YEAR",
      "EPOCHDAY" -> "ChronoField.EPOCH_DAY",
      "YEARDAY"  -> "ChronoField.DAY_OF_YEAR",
      "HOUR"     -> "ChronoField.HOUR_OF_DAY",
      "MINUTE"   -> "ChronoField.MINUTE_OF_HOUR",
      "SECOND"   -> "ChronoField.SECOND_OF_MINUTE"
    ).foreach { case (fn, marker) =>
      val sql = s"SELECT ABS($fn(createdAt)) FROM t"
      withClue(s"[$sql] ") {
        identifierOf(sql).painless(None) should (include(marker) and include("Math.abs"))
      }
    }
    identifierOf("SELECT ABS(YEAR(DATE_TRUNC(createdAt, MONTH))) FROM t")
      .painless(None) should (include("ChronoField.YEAR") and include("truncatedTo") and include(
      "Math.abs"
    ))
    identifierOf("SELECT ROUND(ABS(YEAR(createdAt))) FROM t")
      .painless(None) should (include("ChronoField.YEAR") and include("Math.abs") and include(
      "Math.round"
    ))
    Parser("SELECT CAST(YEAR(createdAt) AS STRING) FROM t").isRight shouldBe true
  }

  it should "leave a self-closing inner function alone" in {
    // `opened == 0` — a single function that consumes its own parentheses is the whole match — is
    // the one shape the new count hard-fails where `rep1(end)` used to succeed by stealing the
    // enclosing `)`. Recovery depends on a sibling alternative existing, so pin it: these parsed
    // before the fix and must still parse, with the inner transform intact.
    identifierOf("SELECT ABS(DATE_TRUNC(createdAt, MONTH)) FROM t")
      .painless(None) should (include("truncatedTo") and include("Math.abs"))
    identifierOf("SELECT ABS(DATE_DIFF(a, b, DAY)) FROM t")
      .painless(None) should (include("ChronoUnit.DAYS.between") and include("Math.abs"))
    identifierOf("SELECT UPPER(DATE_TRUNC(createdAt, MONTH)) FROM t")
      .painless(None) should (include("truncatedTo") and include("toUpperCase"))
  }

  it should "leave a scalar wrapping an aggregate alone" in {
    // `MAX` is a bare-name alternative too, so this shape sits next to the one being fixed. It
    // never reaches the window branch of `Field.update` — the aggregate stays nested inside the
    // math function rather than flattening into the chain — and it parsed identically before.
    val id = identifierOf("SELECT ABS(MAX(salary)) AS m FROM t")
    id.functions.map(_.getClass.getSimpleName) shouldBe List("MathematicalFunctionWithOp")
  }

  "an aggregate wrapping an extractor" should "keep every transform in its function chain" in {
    // Now that the parentheses balance, `MAX(YEAR(x))` reaches the same window-aggregate production
    // as `MAX(x)` instead of the generic chain. `Field.update` then dropped the head of the WINDOW's
    // identifier — a leftover from when the trimmed list was the field's own, whose head IS the
    // window function — so `YEAR` vanished and the aggregation was scripted without it.
    val id = identifierOf("SELECT MAX(YEAR(DATE_TRUNC(createdAt, MINUTE))) AS m FROM t GROUP BY id")
    id.functions.map(_.getClass.getSimpleName) should contain allOf ("Year", "DateTrunc")
    id.painless(None) should (include("ChronoField.YEAR") and include("truncatedTo"))
    // And it is now the same aggregate the plain form produces — the two used to differ only
    // because one argument happened to be parenthesis-balanced and the other was not.
    id.functions.head.getClass shouldBe
    identifierOf("SELECT MAX(salary) AS m FROM t GROUP BY id").functions.head.getClass
  }

  "a postfix cast over an aggregate" should "stay a loud error, never a dropped cast" in {
    // `MAX(YEAR(x))::STRING` was rejected before this change — the chain is
    // `CastOperator :: MaxAgg :: Year` and the engine requires the aggregate to come first. Routing
    // it through the window production must not turn that error into a silently un-cast column, so
    // `Field.update` keeps whatever wraps the window instead of rebuilding from the window alone.
    // The same error now covers the shapes that always took this path and silently swallowed the
    // cast — `MAX(x)::T` returned a number where the user asked for a string.
    Seq(
      "SELECT MAX(YEAR(createdAt))::STRING AS m FROM t GROUP BY id",
      "SELECT MAX(DATE_TRUNC(createdAt, MINUTE))::STRING AS m FROM t GROUP BY id",
      "SELECT MAX(salary)::STRING AS m FROM t GROUP BY id",
      "SELECT COUNT(id)::STRING AS c FROM t GROUP BY dept"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        Parser(sql).swap.toOption.map(_.msg) shouldBe Some(
          "Aggregation function must be the first function in the chain"
        )
      }
    }
    // The cast is still honoured wherever the aggregate is not in the way.
    identifierOf("SELECT YEAR(createdAt)::STRING AS y FROM t").functions
      .map(_.getClass.getSimpleName) shouldBe List("CastOperator", "Year")
  }

  "a bare function name" should "not parse as a call with no arguments" in {
    // Closing exactly what was opened makes zero parentheses an arithmetically valid count, so the
    // production has to reject it explicitly — otherwise a naked keyword becomes a function applied
    // to nothing, where the previous `rep1(end)` required a `)`.
    Parser("SELECT MAX FROM t").isLeft shouldBe true
    // `YEAR` on its own stays what it has always been: an ordinary column of that name.
    identifierOf("SELECT YEAR FROM t").functions shouldBe empty
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
