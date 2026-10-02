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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.function.aggregate.{MaxAgg, MinAgg}
import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The three spellings of an aggregate in a `HAVING` -- bare `MAX(x)`, qualified `MAX(t.x)` and the
  * SELECT alias `mx` -- are ONE shape: one verdict, one message, one filter.
  *
  * `Having.resolveAggregateAliases` replaces a SELECT alias by its aggregate at EVERY depth of a
  * value expression -- a function argument, an arithmetic operand, a `CASE` condition, the value
  * side of a comparison -- through the one walk that reaches all of them, `update`. And `ISNULL` /
  * `ISNOTNULL` parse their operand like any value expression, so an aggregate there is the
  * aggregate every other position builds.
  *
  * The whole function family is measured over the help-corpus population outside this suite; these
  * rows pin the mechanism on every operand position the walk has to reach.
  */
class HavingAliasResolutionSpec extends AnyFlatSpec with Matchers {

  private def outcome(sql: String): Either[String, Option[String]] = Parser(sql) match {
    case Right(s: SingleSearch) =>
      Right(s.having.flatMap(_.criteria).flatMap(MetricSelectorScript.nullAwareSelectorScript))
    case Right(other) => fail(s"[$sql] expected a SingleSearch, got $other")
    case Left(e)      => Left(e.msg)
  }

  private def bare(having: String): String =
    "SELECT g, MAX(x) AS mx, MIN(y) AS my FROM t GROUP BY g HAVING " +
    having.replace("{A}", "MAX(x)").replace("{B}", "MIN(y)")

  private def qualified(having: String): String =
    "SELECT r.g, MAX(r.x) AS mx, MIN(r.y) AS my FROM t AS r GROUP BY r.g HAVING " +
    having.replace("{A}", "MAX(r.x)").replace("{B}", "MIN(r.y)")

  private def aliased(having: String): String =
    "SELECT g, MAX(x) AS mx, MIN(y) AS my FROM t GROUP BY g HAVING " +
    having.replace("{A}", "mx").replace("{B}", "my")

  /** Every operand position the substitution has to reach, accepted and refused shapes alike. */
  private val shapes: Seq[String] = Seq(
    "{A} > 1",
    "{A} > {B}",
    "{A} BETWEEN 1 AND 5",
    "{A} IN (1, 2)",
    "NOT {A} = 1",
    "COALESCE({A}, {B}) > 1",
    "GREATEST({A}, {B}, 3) > 1",
    "LEAST({A}, 1) > 0",
    "SIGN({A}) > 0",
    "{A} > COALESCE({B}, 1)",
    "{A} > 1 AND COALESCE({B}, 0) > 1",
    "COALESCE(COALESCE({A}, {B}), 5) > 1",
    "NULLIF({A}, 0) > 1",
    "ABS({A}) > 1",
    "ROUND({A}, 2) > 1",
    "CAST({A} AS DOUBLE) > 1",
    "{A} - {B} > 1",
    "{A} + 1 > 2",
    "CASE WHEN {A} > 1 THEN 1 ELSE 0 END = 1",
    "ISNULL({A})",
    "ISNOTNULL({A})"
  )

  "the bare and the alias spellings" should "get the same verdict, message and filter" in {
    var accepted, refused = 0
    shapes.foreach { shape =>
      withClue(s"[$shape] ") {
        val b = outcome(bare(shape))
        outcome(aliased(shape)) shouldBe b
        if (b.isRight) accepted += 1 else refused += 1
      }
    }
    // both outcomes occur, or the equality proved nothing
    accepted should be > 5
    refused should be > 5
  }

  "the qualified spelling" should "get the same verdict and filter, and the message in its own spelling" in {
    shapes.foreach { shape =>
      withClue(s"[$shape] ") {
        outcome(qualified(shape)).left.map(_.replace("r.", "")) shouldBe outcome(bare(shape))
      }
    }
  }

  "ISNULL and ISNOTNULL" should "parse their operand as the aggregate every other position builds" in {
    def nullTestOperand(sql: String) = Parser(sql) match {
      case Right(s: SingleSearch) =>
        s.having.flatMap(_.criteria) match {
          case Some(c: IsNullCriteria)    => c.identifier.aggregateFunction
          case Some(c: IsNotNullCriteria) => c.identifier.aggregateFunction
          case other                      => fail(s"[$sql] expected a null test, got $other")
        }
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }
    nullTestOperand(bare("ISNULL({B})")).get shouldBe a[MinAgg]
    nullTestOperand(bare("ISNOTNULL({A})")).get shouldBe a[MaxAgg]
    nullTestOperand(qualified("ISNULL({B})")).get shouldBe a[MinAgg]
  }

  it should "no longer mistake the qualified spelling for a different aggregate" in {
    // The bare `MIN` token rendered `MIN(y)` where the SELECT's `MinAgg` renders `MIN(r.y)`, so the
    // alias `my` looked like it named two aggregates and the statement was refused.
    outcome(qualified("ISNULL({B})")) shouldBe outcome(aliased("ISNULL({B})"))
    outcome(qualified("ISNOTNULL({B})")) shouldBe outcome(aliased("ISNOTNULL({B})"))
  }

  "DATEDIFF, DATE_DIFF and TIMESTAMPDIFF" should "parse an aggregate operand as the aggregate every other position builds" in {
    // The remedy for inline arithmetic -- alias it in SELECT -- was refused on the qualified
    // spelling: the bare `MAX` token rendered `MAX(d)` where the SELECT's `MaxAgg` renders
    // `MAX(r.d)`, so the alias `max_d` looked like it named two aggregates.
    Seq(
      "DATEDIFF(MAX(r.d), MIN(r.d))",
      "DATEDIFF(MAX(r.d), '2024-01-01')",
      "DATE_DIFF(MAX(r.d), MIN(r.d), DAY)",
      "DATE_DIFF(DAY, MAX(r.d), '2024-01-01')",
      "TIMESTAMPDIFF(DAY, MAX(r.d), MIN(r.d))"
    ).foreach { expr =>
      val sql = "SELECT r.g, MAX(r.d) AS max_d, MIN(r.d) AS min_d, " +
        s"$expr AS x FROM t AS r GROUP BY r.g HAVING x > 1"
      withClue(s"[$sql] ") {
        Parser(sql) match {
          case Right(s: SingleSearch) =>
            val item = s.select.fields.find(_.fieldAlias.exists(_.alias == "x")).get
            val operands = item.identifier.referencedAggregates.flatMap(_.aggregateFunction)
            operands should not be empty
            operands.foreach { af =>
              withClue(s"[$af] ")(
                (af.isInstanceOf[MaxAgg] || af.isInstanceOf[MinAgg]) shouldBe true
              )
            }
          case other => fail(s"expected a SingleSearch, got $other")
        }
      }
    }
  }

  "a cast or an interval after an aggregate operand" should "keep the verdict and the message it had" in {
    // The aggregate reading stops at the call's `)`: there the alternatives these five functions
    // read before read the operand, suffix included, exactly as they did -- not `end of input
    // expected`.
    val chain = Left("Aggregation function must be the first function in the chain")
    def inline(expr: String) = Left(
      s"HAVING cannot combine aggregates arithmetically inline ($expr); alias the expression in " +
      "SELECT and reference the alias"
    )
    val having = "SELECT g, MAX(a) AS ma, MAX(d) AS md FROM t GROUP BY g HAVING "
    Seq(
      having + "ISNULL(MAX(a)::DOUBLE)"             -> chain,
      having + "ISNOTNULL(MAX(a)::BIGINT)"          -> chain,
      having + "ISNULL(MAX(d) - INTERVAL 1 DAY)"    -> chain,
      having + "ISNOTNULL(MAX(d) + INTERVAL 1 DAY)" -> chain,
      having + "ISNULL(COUNT(*)::DOUBLE)"           -> chain,
      having + "DATEDIFF(MAX(d)::DATE, '2024-01-01') > 1" -> inline(
        "DATEDIFF(MAX(d)::DATE, '2024-01-01')"
      ),
      having + "DATEDIFF(MAX(d) - INTERVAL 1 DAY, '2024-01-01') > 1" -> inline(
        "DATEDIFF(MAX(d) - INTERVAL 1 DAY, '2024-01-01')"
      ),
      having + "DATE_DIFF(DAY, MAX(d) - INTERVAL 1 DAY, '2024-01-01') > 1" -> inline(
        "DATE_DIFF(DAY, MAX(d) - INTERVAL 1 DAY, '2024-01-01')"
      ),
      having + "TIMESTAMPDIFF(DAY, MAX(d)::DATE, '2024-01-01') > 1" -> inline(
        "DATE_DIFF(DAY, MAX(d)::DATE, '2024-01-01')"
      ),
      "SELECT g, ISNULL(MAX(a)::DOUBLE) AS x FROM t GROUP BY g"               -> chain,
      "SELECT g, ISNULL(MAX(d) - INTERVAL 1 DAY) AS x FROM t GROUP BY g"      -> chain,
      "SELECT g, DATEDIFF(MAX(d)::DATE, '2024-01-01') AS x FROM t GROUP BY g" -> Right(()),
      "SELECT g, DATEDIFF(MAX(d) - INTERVAL 1 DAY, CURRENT_DATE) AS x FROM t GROUP BY g" -> Right(
        ()
      )
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ")(Parser(sql).map(_ => ()).left.map(_.msg) shouldBe expected)
    }
  }

  "a window function operand of DATEDIFF, DATE_DIFF or TIMESTAMPDIFF" should
  "be refused by name with no GROUP BY, and its remedy accepted" in {
    // (the statement, the call as written, the window function as written, as rendered)
    Seq(
      (
        "SELECT g, DATEDIFF(MAX(d) OVER (PARTITION BY g), '2024-01-01') AS x FROM t",
        "DATEDIFF(MAX(d) OVER (PARTITION BY g), '2024-01-01')",
        "MAX(d) OVER (PARTITION BY g)",
        "MAX(d) OVER (PARTITION BY g)"
      ),
      (
        "SELECT g, DATE_DIFF(DAY, '2024-01-01', MIN(ts) OVER (PARTITION BY g)) AS x FROM t",
        "DATE_DIFF(DAY, '2024-01-01', MIN(ts) OVER (PARTITION BY g))",
        "MIN(ts) OVER (PARTITION BY g)",
        "MIN(ts) OVER (PARTITION BY g)"
      ),
      (
        "SELECT id, g, d, DATEDIFF(d, MIN(d) OVER (PARTITION BY g)) AS x FROM t",
        "DATEDIFF(d, MIN(d) OVER (PARTITION BY g))",
        "MIN(d) OVER (PARTITION BY g)",
        "MIN(d) OVER (PARTITION BY g)"
      ),
      (
        "SELECT id, TIMESTAMPDIFF(DAY, FIRST_VALUE(d) OVER (PARTITION BY g ORDER BY d), d) AS x FROM t",
        "TIMESTAMPDIFF(DAY, FIRST_VALUE(d) OVER (PARTITION BY g ORDER BY d), d)",
        "FIRST_VALUE(d) OVER (PARTITION BY g ORDER BY d)",
        "FIRST_VALUE(d) OVER (PARTITION BY g ORDER BY d ASC)"
      )
    ).foreach { case (sql, call, window, rendered) =>
      withClue(s"[$sql] ") {
        val message = Parser(sql).left.map(_.msg).left.toOption.getOrElse(fail("not refused"))
        message should startWith(
          s"DATEDIFF, DATE_DIFF and TIMESTAMPDIFF over a window function are not supported yet ($rendered in "
        )
        message should endWith("); select the window function on its own")
        Parser(sql.replace(call, window)).isRight shouldBe true
      }
    }
    // With a GROUP BY it is computed per group, as it was.
    Seq(
      "SELECT g, DATEDIFF(MAX(d) OVER (), '2024-01-01') AS x FROM t GROUP BY g",
      "SELECT g, DATEDIFF(MAX(d) OVER (PARTITION BY g), '2024-01-01') AS x FROM t GROUP BY g",
      "SELECT g, DATEDIFF('2024-01-01', MAX(d) OVER (), DAY) AS x FROM t GROUP BY g",
      "SELECT g, MAX(d) AS m, DATEDIFF(MAX(d) OVER (), '2024-01-01') AS x FROM t GROUP BY g"
    ).foreach(sql => withClue(s"[$sql] ")(Parser(sql).isRight shouldBe true))
  }

  private def overBucketScript(having: String): String =
    "SELECT g, MAX(x) AS mx, MIN(y) AS my, MAX(x) - MIN(y) AS d FROM t GROUP BY g HAVING " + having

  "a SELECT bucket_script alias" should "be read under a function that is NULL whenever it is" in {
    Seq("SIGN(d) > 0", "mx > SIGN(d)", "SIGN(d) BETWEEN -1 AND 0").foreach { having =>
      withClue(s"[$having] ")(outcome(overBucketScript(having)).isRight shouldBe true)
    }
  }

  it should "stay refused under COALESCE, GREATEST, LEAST and a conversion, as before" in {
    // Inside a NULL-deciding function the item is read through its OPERANDS, and an operand's NULL
    // throws there instead of making the item NULL; under a conversion of the alias the comparison
    // reads `params.d` and drops the conversion, and every other position reads undeclared
    // operands. So the alias is left in place and keeps the refusal it always had.
    Seq(
      "COALESCE(d, 0) > 1"          -> "(COALESCE(d, 0))",
      "GREATEST(d, 1) > 1"          -> "(GREATEST(d, 1))",
      "LEAST(d, my) < 3"            -> "(LEAST(d, my))",
      "SIGN(COALESCE(d, 0)) > 0"    -> "(SIGN(COALESCE(d, 0)))",
      "COALESCE(SIGN(d), 0) = 0"    -> "(COALESCE(SIGN(d), 0))",
      "CAST(d AS DOUBLE) > 1"       -> "(CAST(d AS DOUBLE))",
      "CAST(d AS BIGINT) > 1"       -> "(CAST(d AS BIGINT))",
      "d::DOUBLE > 1"               -> "(d::DOUBLE)",
      "TRY_CAST(d AS DOUBLE) > 1"   -> "(TRY_CAST(d AS DOUBLE))",
      "ISNULL(CAST(d AS DOUBLE))"   -> "(CAST(d AS DOUBLE))",
      "SIGN(CAST(d AS DOUBLE)) > 0" -> "(SIGN(CAST(d AS DOUBLE)))"
    ).foreach { case (having, operand) =>
      withClue(s"[$having] ") {
        outcome(overBucketScript(having)).left.toOption.get shouldBe
        s"HAVING cannot apply a function to the aggregate alias 'd' $operand; compare the aggregate itself"
      }
    }
    // beside an aggregate, the refusals they had
    outcome(overBucketScript("mx > COALESCE(d, 1)")).left.toOption.get should include(
      "HAVING cannot be applied to MAX(x) > COALESCE(d, 1): its rendering reads 'arg0'"
    )
    outcome(overBucketScript("COUNT(*) > CAST(d AS DOUBLE)")).left.toOption.get should include(
      "HAVING cannot be applied to COUNT(*) > CAST(d AS DOUBLE): its rendering can evaluate to NULL"
    )
  }

  "an alias named after its aggregate's own column" should "not be substituted into its operand" in {
    // `MAX(x) AS x`: the `x` inside `MAX(x)` is the column, never the alias.
    val sql = "SELECT g, MAX(x) AS x FROM t GROUP BY g HAVING x > 1 AND ABS(x) > 1"
    outcome(sql).left.toOption.get should include("ABS(MAX(x)) > 1")
    outcome("SELECT g, MAX(x) AS x FROM t GROUP BY g HAVING x > 1") shouldBe
    outcome("SELECT g, MAX(x) AS x FROM t GROUP BY g HAVING MAX(x) > 1")
  }

  "an alias the substitution leaves in place" should "still be refused by name inside a function" in {
    // Relation predicates are not substituted; a function of an alias there stays a bare column,
    // and the #389 rule names it rather than letting it render an unbound parameter.
    outcome(aliased("NESTED(ABS({A}) > 1)")).left.toOption.get should include(
      "HAVING cannot apply a function to the aggregate alias 'mx'"
    )
  }

  "a MATCH over an alias" should "keep its own message" in {
    outcome(aliased("MATCH ({A}) AGAINST ('z')")).left.toOption.get should include(
      "the aggregate mx (MAX(x))"
    )
  }
}
