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

import app.softnetwork.elastic.sql.{GenericIdentifier, Identifier}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** Story IDENT-4 -- a QUALIFIED GROUP BY key resolves to its bucket in HAVING.
  *
  * "Which bucket does this leaf name" keyed on a DISPLAY name collapses: after ONE resolution a
  * self-join's two levels share a name, and a lookup by name answers the LAST of them for either
  * leaf. So every assertion here is made at BOTH resolutions, on a DERIVED population, and the
  * self-join is asserted in BOTH directions (asserting only the last level is exactly what a
  * collapsed lookup gets right).
  */
class QualifiedGroupKeySpec extends AnyFlatSpec with Matchers {

  import QualifiedKeyPopulation._

  private val none = BucketIncludesExcludes()
  private val nothing = (none, none)
  private def keeps(values: String*) = (BucketIncludesExcludes(values = values.toSet), none)

  private def verdict(sql: String): Either[String, Unit] =
    Parser(sql) match {
      case Right(_)  => Right(())
      case Left(err) => Left(err.msg)
    }

  /** The HAVING rule a refusal comes from, read off its message (reporting only). */
  private def refusalFamily(reason: String): String =
    if (reason.contains("can only filter on a GROUP BY key or on an aggregate")) "(a)"
    else if (reason.contains("which can only express a direct comparison of the key")) "(b)"
    else if (reason.contains("the grouping key is a nested object")) "(b-nested)"
    else if (reason.contains("HAVING cannot combine the conditions")) "(b2)"
    else if (reason.contains("DIFFERENT GROUP BY keys")) "(c)-keys"
    else if (reason.contains("different mechanisms")) "(c)-mechanisms"
    else if (reason.contains("HAVING cannot apply MATCH")) "match-refusal"
    else if (reason.contains("MATERIALIZED VIEW cannot")) "view"
    else if (reason.contains("HAVING cannot filter a DATE grouping level")) "date-level"
    else if (reason.contains("HAVING cannot compare the integer GROUP BY key")) "fractional-key"
    else s"other(${reason.take(80)})"

  private def verdictFamily(sql: String): String =
    verdict(sql).fold(refusalFamily, _ => "accepted")

  private def channels(ss: SingleSearch): Seq[(BucketIncludesExcludes, BucketIncludesExcludes)] =
    channelsOf(ss).map { case (_, kept, removed) => (kept, removed) }

  /** One observation, COLLECTED: `None` when it holds, the observed value otherwise -- a test that
    * read as a per-cell map must never stop at its first red (`foreach { … shouldBe … }` does).
    */
  private def check(label: String)(actual: => Any, expected: Any): Option[String] =
    Try(actual).toEither match {
      case Right(a) if a == expected => None
      case Right(a)                  => Some(s"$label: $a, expected $expected")
      case Left(t)                   => Some(s"$label threw $t")
    }

  private val dated: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("s", SQLTypes.Keyword),
      Column("id", SQLTypes.Keyword),
      Column("amount", SQLTypes.Int)
    )
  )

  private val selfJoin =
    "SELECT COUNT(*) AS c FROM orders o JOIN orders p ON o.did = p.id GROUP BY o.category, p.category "

  private val selfJoinSchemas = Map("orders" -> ordersSchema)

  // 1 ------------------------------------------------------------------------------------------
  "IDENT-4: the qualified-key population" should
  "put each predicate on exactly the level it names, at BOTH resolutions, or refuse it" in {
    cells should have size ExpectedCellCount.toLong
    val outcomes = cells.map { cell =>
      val atR1 = Try(r1(cell.sql))
      val atR2 = atR1.flatMap(s => Try(r2(cell.shape, s)))
      val problems: Seq[String] = (atR1.map(channels).toOption, atR2.map(channels).toOption) match {
        case (Some(c1), Some(c2)) =>
          Seq(
            if (c1 != cell.expectedChannels) Some(s"R1 channels $c1") else None,
            if (c2 != cell.expectedChannels) Some(s"R2 channels $c2") else None
          ).flatten ++ (
            (verdict(cell.sql), cell.addressed) match {
              case (Right(()), Some(_)) => Nil
              // a non-addressed cell is refused by its operator's own family: rule (a) for the seven
              // comparison operators, the MATCH rule (a MATCH over a column no level names) for MATCH
              case (Left(r), None)
                  if refusalFamily(r) == (if (cell.op.label == "MATCH") "match-refusal"
                                          else "(a)") =>
                Nil
              case (other, expected) =>
                Seq(s"verdict $other, expected ${if (expected.isDefined) "accepted"
                else "refused (neither)"}")
            }
          )
        case _ => Seq(s"threw: R1 $atR1, R2 $atR2")
      }
      cell -> problems
    }
    val wrong = outcomes.filter(_._2.nonEmpty)
    // The ORACLE's split, printed for the reader -- a prediction about the alphabet, never asserted.
    info(
      s"${cells.count(_.addressed.isDefined)} accepted · ${cells.count(_.addressed.isEmpty)} refused · " +
      s"${wrong.size} wrong"
    )
    // The MEASURED verdicts of the tree under test, by operator kind and refusal family -- printed,
    // never asserted.
    cells
      .groupBy(c => if (c.op.label == "MATCH") "MATCH" else "non-MATCH")
      .toSeq
      .sortBy(_._1)
      .foreach { case (kind, group) =>
        val split =
          group.groupBy(c => verdictFamily(c.sql)).map { case (f, cs) => s"$f=${cs.size}" }
        info(s"verdicts, $kind (${group.size} cells): ${split.toSeq.sorted.mkString(" · ")}")
      }
    withClue(
      s"${wrong.size} of ${cells.size} cells wrong:\n" +
      wrong
        .map { case (c, p) => s"[${c.label}]\n    ${c.sql}\n    ${p.mkString("; ")}" }
        .mkString("\n") +
      "\n"
    ) { wrong shouldBe empty }
  }

  // 2 ------------------------------------------------------------------------------------------
  "a self-join's key filter on one level" should
  "sit on EXACTLY the level it names -- in BOTH directions" in {
    // Two failures to exclude: the include on EVERY level, and a lookup that answers the LAST
    // level -- which gets `p` right and `o` wrong. Hence both directions.
    channels(r1(selfJoin + "HAVING p.category = 'x'")) shouldBe Seq(nothing, keeps("x"))
    channels(r1(selfJoin + "HAVING o.category = 'x'")) shouldBe Seq(keeps("x"), nothing)
    verdict(selfJoin + "HAVING p.category = 'x'") shouldBe Right(())
    verdict(selfJoin + "HAVING o.category = 'x'") shouldBe Right(())
  }

  // 3 ------------------------------------------------------------------------------------------
  "a self-join's key filter after re-resolution" should
  "give IDENTICAL channels after a second and a third resolution (updateAll)" in {
    Seq("o", "p").foreach { alias =>
      val once = r1(selfJoin + s"HAVING $alias.category = 'x'")
      val twice = once.updateAll(selfJoinSchemas)
      withClue(s"[$alias, second] ") { channels(twice) shouldBe channels(once) }
      // `Table.mergeWithSearch` re-updates a statement that was already resolved
      withClue(s"[$alias, third] ") {
        channels(twice.updateAll(selfJoinSchemas)) shouldBe channels(once)
      }
    }
  }

  // 4 ------------------------------------------------------------------------------------------
  "a GROUP BY <select alias> over a qualified column" should
  "stay ACCEPTED, filtering its one level" in {
    Seq(
      "SELECT o.category AS cat, COUNT(*) AS c FROM orders o GROUP BY cat HAVING cat = 'x'",
      "SELECT o.category AS cat, COUNT(*) AS c FROM orders o JOIN orders p ON o.did = p.id " +
      "GROUP BY cat HAVING cat = 'x'"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql) shouldBe Right(())
        val once = r1(sql)
        channels(once) shouldBe Seq(keeps("x"))
        channels(once.update(Some(ordersSchema))) shouldBe Seq(keeps("x"))
        channels(once.updateAll(selfJoinSchemas)) shouldBe Seq(keeps("x"))
      }
    }
  }

  // 5 ------------------------------------------------------------------------------------------
  "two FUNCTIONS of one column as two keys" should
  "each keep their own filter -- a predicate on MONTH must not land on YEAR" in {
    // Unprojected on purpose: a projected function key gets a display alias (`__cN` or its own),
    // which separated the levels by accident. The include used to land on BOTH levels, and
    // Elasticsearch answered NO group (no year is `1`). A date part is a scripted key, so the
    // number is sent in every text a key of its value prints (`Bucket.keyFilterTexts`): the key
    // prints `1`. DuckDB 1.5.5 answers `GROUP BY YEAR(d), MONTH(d) HAVING MONTH(d) = 1` over the
    // HAVING key fixture of `GroupByCompletenessSpec` with (2024, 1, 2), the January group alone.
    val expected = Seq(nothing, keeps("1", "1.0"))
    val wrong = Seq(
      "SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d), MONTH(d) HAVING MONTH(d) = 1",
      "SELECT COUNT(*) AS c FROM t q GROUP BY YEAR(q.d), MONTH(q.d) HAVING MONTH(q.d) = 1",
      "SELECT COUNT(*) AS c FROM t q GROUP BY YEAR(q.d), MONTH(q.d) HAVING MONTH(d) = 1"
    ).flatMap { sql =>
      Seq(
        check(s"[$sql] R1")(channels(r1(sql)), expected),
        check(s"[$sql] R2")(channels(r1(sql).update(Some(dated))), expected),
        check(s"[$sql] verdict")(verdict(sql), Right(()))
      ).flatten
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  // 5b -----------------------------------------------------------------------------------------
  it should "answer the AND of the two, each half on its own level" in {
    // Refused until now: both leaves' copies were named `d`, so the YEAR level received BOTH
    // includes under an AND and the channel rule refused the statement -- a loud refusal of a
    // correct query, in the bare spelling as much as the qualified one. Each number in every text
    // its key prints; DuckDB 1.5.5 answers (2024, 1, 2) over the HAVING key fixture of
    // `GroupByCompletenessSpec`.
    val expected = Seq(keeps("2024", "2024.0"), keeps("1", "1.0"))
    val wrong = Seq(
      "SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 AND MONTH(d) = 1",
      "SELECT COUNT(*) AS c FROM t q GROUP BY YEAR(q.d), MONTH(q.d) " +
      "HAVING YEAR(q.d) = 2024 AND MONTH(q.d) = 1"
    ).flatMap { sql =>
      Seq(
        check(s"[$sql] verdict")(verdict(sql), Right(())),
        check(s"[$sql] R1")(channels(r1(sql)), expected),
        check(s"[$sql] R2")(channels(r1(sql).update(Some(dated))), expected)
      ).flatten
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  // 6 ------------------------------------------------------------------------------------------
  "an OR across two GROUP BY levels" should
  "be refused in a statement Elasticsearch groups, two functions of one column included" in {
    // The self-join row of the full spec moved to the JOIN family below: in a statement the
    // relational engine groups, rule (c) compares mechanisms only.
    val statements = Seq(
      "SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 OR MONTH(d) = 1",
      "SELECT COUNT(*) AS c FROM t q GROUP BY YEAR(q.d), MONTH(q.d) " +
      "HAVING YEAR(q.d) = 2024 OR MONTH(q.d) = 1"
    )
    val wrong = statements.flatMap { sql =>
      verdict(sql) match {
        case Left(reason) if reason.contains("DIFFERENT GROUP BY keys") => None
        case other =>
          Some(s"[$sql] $other -- Elasticsearch NESTS the two levels: the OR would run as an AND")
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "still accept the AND, putting each half on its own level" in {
    val sql = selfJoin + "HAVING o.category = 'x' AND p.category = 'y'"
    verdict(sql) shouldBe Right(())
    channels(r1(sql)) shouldBe Seq(keeps("x"), keeps("y"))
    channels(r1(sql).updateAll(selfJoinSchemas)) shouldBe Seq(keeps("x"), keeps("y"))
  }

  // 7 ------------------------------------------------------------------------------------------
  "a FUNCTION-of-column key (the registry, via IdentityPopulation)" should
  "attach its own spelling as the bare spelling does, and never a plain-column predicate" in {
    val schema = IdentityPopulation.schema
    val group = (key: String, predicate: String) =>
      s"SELECT COUNT(*) AS c FROM t GROUP BY $key HAVING $predicate"
    type Seen = (
      Seq[(BucketIncludesExcludes, BucketIncludesExcludes)],
      Seq[(BucketIncludesExcludes, BucketIncludesExcludes)]
    )
    def observe(sql: String): Option[Seen] =
      Try(r1(sql)).toOption.flatMap(s =>
        Try((channels(s), channels(s.update(Some(schema))))).toOption
      )
    val keys: Seq[(String, String, String)] = functionKeys
    val results = keys.map { case (spelling, qualified, bare) =>
      (observe(group(bare, s"$bare = 'v'")), observe(group(qualified, s"$qualified = 'v'"))) match {
        case (Some(b), Some(q)) =>
          val qualifiedSql = group(qualified, s"$qualified = 'v'")
          val named = Try(r1(qualifiedSql).buckets.head.identifier.name.nonEmpty).getOrElse(false)
          // A key whose function renders its OWN argument (`CAST(t.d AS VARCHAR)`, `ISNULL(t.d)`, an
          // aggregate) keeps the qualifier inside its grouped expression, so its identity is its
          // spelling, as a nameless key's is. Its mixed spellings are refused today (recorded);
          // never answered with a channel other than the bare spelling's.
          val spellingOnly =
            named && Try(r1(qualifiedSql).buckets.head.identifier.identifierName.contains("t.d"))
              .getOrElse(false)
          val stale = Try(staleArgument(r1(qualifiedSql))).getOrElse(false)
          val mixedSql = Seq(group(qualified, s"$bare = 'v'"), group(bare, s"$qualified = 'v'"))
          val mixed = mixedSql.map(observe)
          val plain = observe(group(qualified, "t.d = '2024-01-01'"))
          val noChannel = b._1.map(_ => nothing)
          val problems = Seq(
            if (q != b) Some(s"qualified $q, bare $b") else None,
            // a NAMELESS key matches by spelling only
            if (named && !spellingOnly && mixed.exists(_ != Some(b)))
              Some(s"a mixed spelling differs: $mixed")
            else if (
              spellingOnly && mixedSql.zip(mixed).exists { case (sql, seen) =>
                verdict(sql).isRight && seen != Some(b)
              }
            )
              Some(
                s"a mixed spelling of a spelling-only key is answered unlike its bare spelling: " +
                s"${mixedSql.map(verdict)} $mixed"
              )
            else None,
            if (plain.exists(p => p._1 != noChannel || p._2 != noChannel))
              Some(s"the plain column t.d attached: $plain")
            else None,
            if (stale)
              verdict(qualifiedSql) match {
                case Left(r) if r.contains("can only filter on a GROUP BY key") => None
                case other => Some(s"stale argument: verdict $other, expected rule (a)'s refusal")
              }
            else if (verdict(qualifiedSql).isRight != verdict(group(bare, s"$bare = 'v'")).isRight)
              Some("the two spellings' verdicts differ")
            else None,
            // once the chain resolves with its column, no function keeps its argument stale,
            // so a stale argument is a FINDING, never an excused class
            if (stale)
              Some("a stale argument survived the chain of a qualified column resolving with it")
            else None
          ).flatten
          val mixedRefused =
            if (spellingOnly) mixedSql.count(sql => verdict(sql).isLeft) else 0
          (
            true,
            named,
            stale,
            problems.map(p => s"[$spelling] $qualified: $p"),
            spellingOnly,
            mixedRefused
          )
        case _ =>
          (
            false,
            false,
            false,
            Nil,
            false,
            0
          ) // not a GROUP BY key in one of the spellings -- counted below
      }
    }
    val exercised = results.count(_._1)
    val wrong = results.flatMap(_._4)
    info(
      s"${keys.size} function keys · $exercised exercised in both spellings · " +
      s"${results.count(r => r._1 && !r._2)} nameless (spelling only) · " +
      s"${results.count(_._3)} fully-qualified spelling refused (stale argument) · " +
      s"${results.count(_._5)} named keys matched by spelling only · " +
      s"${results.map(_._6).sum} of their mixed spellings refused (recorded) · " +
      s"${keys.size - exercised} not exercised"
    )
    // the DERIVED spelling-only class, printed (never a hand list)
    info(
      s"spelling-only named keys: ${results.zip(keys).collect { case (r, k) if r._5 => k._1 }.mkString(" ")}"
    )
    exercised should be > 0
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  // 8 ------------------------------------------------------------------------------------------
  "a NESTED key (UNNEST)" should "keep today's route: no identity, the spelling lookup, at both resolutions" in {
    // These rows pin that the route is today's, and that a direct key predicate on a nested key
    // keeps its channel.
    Seq(
      "SELECT COUNT(*) AS c FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name HAVING e.name = 'x'",
      "SELECT COUNT(*) AS c FROM t JOIN UNNEST(t.emails) AS emails GROUP BY emails.name " +
      "HAVING emails.name = 'x'"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        Seq(r1(sql), r1(sql).update(Some(dated))).foreach { ss =>
          // `update()` wraps a nested leaf in `ElasticNested` and `havingLeaves` answers Nil for a
          // relation, so the leaf is reached through the relation (`leavesUnder`).
          val leaf: Identifier =
            ss.having
              .flatMap(_.criteria)
              .toSeq
              .flatMap(ss.leavesUnder)
              .headOption
              .map(_.identifier)
              .getOrElse(fail(s"no nested key leaf in ${ss.sql}"))
          ss.keyIdentityOf(leaf) shouldBe None
          ss.bucketFor(leaf) shouldBe ss.bucketNames.get(leaf.identifierName)
        }
        channels(r1(sql)).exists(_ != nothing) shouldBe true // the nested key filter is applied
      }
    }
  }

  // 9 ------------------------------------------------------------------------------------------
  "tableQualifierOf" should "agree with GenericIdentifier.update on every name a FROM can qualify" in {
    val statements = Seq(
      "SELECT COUNT(*) AS c FROM orders a GROUP BY id",
      "SELECT COUNT(*) AS c FROM orders a JOIN depts b ON a.did = b.id GROUP BY id",
      "SELECT COUNT(*) AS c FROM orders a JOIN orders b ON a.did = b.id GROUP BY id",
      "SELECT COUNT(*) AS c FROM orders GROUP BY id",
      "SELECT COUNT(*) AS c FROM orders o JOIN UNNEST(o.items) AS i GROUP BY id",
      "SELECT COUNT(*) AS c FROM orders o JOIN UNNEST(o.items) AS items GROUP BY id",
      "SELECT COUNT(*) AS c FROM (SELECT id, category FROM orders) AS d GROUP BY id",
      "WITH q AS (SELECT id, category FROM orders) SELECT COUNT(*) AS c FROM q GROUP BY id"
    )
    // DERIVED from each statement's OWN correlation names -- never a hand list of qualifiers -- plus
    // an undeclared one. Stated boundary: a name ending in `.` after a part (`a.b.`) is left out on
    // purpose; `String.split` drops the trailing empty part and the mirror does not, and the
    // grammar cannot produce such a name.
    val wrong = for {
      sql <- statements
      ss = r1(sql)
      correlations = (ss.aliasesToTable.keys ++ ss.unnestAliases.keys ++ ss.from.tables.map(
        _.name
      )).toSeq.distinct :+ "zz"
      name <- correlations
        .flatMap(q => Seq(s"$q.category", s"$q.x.y")) ++ Seq("category", "a.", ".a")
      updated = GenericIdentifier(name).update(ss)
      theirs = updated.tableAlias
        .filter(_ => !updated.nested)
        .map(alias => (alias, updated.name, updated.table.getOrElse("")))
      mine = ss.tableQualifierOf(name)
      if mine != theirs
    } yield s"[$sql] $name: tableQualifierOf $mine, update $theirs"
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // 10 -----------------------------------------------------------------------------------------
  "a predicate on a column that names no grouping key" should "be refused as neither key nor aggregate" in {
    Seq(
      // another correlation's column
      "SELECT COUNT(*) AS c FROM orders a JOIN depts b ON a.did = b.id GROUP BY a.category " +
      "HAVING b.category = 'x'",
      "SELECT COUNT(*) AS c FROM orders o JOIN orders p ON o.did = p.id GROUP BY o.category " +
      "HAVING p.category = 'x'",
      // one column through two correlations in ONE condition: two leaves, and `p.category` is no key
      "SELECT COUNT(*) AS c FROM orders o JOIN orders p ON o.did = p.id GROUP BY o.category " +
      "HAVING o.category = p.category",
      // a table's own name, once the table is aliased, is not a qualifier: an object path
      "SELECT COUNT(*) AS c FROM orders g GROUP BY g.category HAVING orders.category = 'x'",
      // an undeclared qualifier
      "SELECT COUNT(*) AS c FROM orders g GROUP BY g.category HAVING zz.category = 'x'"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql) match {
          case Left(reason) =>
            reason should include("can only filter on a GROUP BY key or on an aggregate")
          case Right(()) => fail("accepted")
        }
        channels(r1(sql)).foreach(_ shouldBe nothing)
      }
    }
  }

  // 11 -----------------------------------------------------------------------------------------
  "the venues that wrap a statement" should "treat the qualified key exactly as the bare one" in {
    val qualified =
      "SELECT g.category AS k, COUNT(*) AS c FROM orders g GROUP BY g.category HAVING g.category = 'x'"
    val bare =
      "SELECT category AS k, COUNT(*) AS c FROM orders GROUP BY category HAVING category = 'x'"
    // (venue, qualified statement, bare twin)
    val wrapped: Seq[(String, String, String)] = Seq(
      ("derived table", s"SELECT k, c FROM ($qualified) AS d", s"SELECT k, c FROM ($bare) AS d"),
      (
        "CTE",
        s"WITH q AS ($qualified) SELECT k, c FROM q",
        s"WITH q AS ($bare) SELECT k, c FROM q"
      ),
      (
        "WHERE subquery",
        "SELECT id FROM orders WHERE category IN (SELECT g.category FROM orders g GROUP BY g.category " +
        "HAVING g.category = 'x')",
        "SELECT id FROM orders WHERE category IN (SELECT category FROM orders GROUP BY category " +
        "HAVING category = 'x')"
      )
    )
    wrapped.foreach { case (venue, q, b) =>
      withClue(s"[$venue] ") {
        verdict(b) shouldBe Right(())
        verdict(q) shouldBe Right(())
        // the render is a fixed point (what a materialized view persists is a render)
        Parser(q).map(_.sql).flatMap(Parser(_)).map(_.sql) shouldBe Parser(q).map(_.sql)
      }
    }
    // a MATERIALIZED VIEW refuses every grouping-key HAVING -- now by its own rule, as the bare twin
    val view = (body: String) => s"CREATE MATERIALIZED VIEW mv_probe AS $body"
    Seq(qualified, bare).foreach { body =>
      withClue(s"[view over $body] ") {
        verdict(view(body)) match {
          case Left(reason) =>
            reason should include("MATERIALIZED VIEW cannot filter on a grouping key in HAVING")
          case Right(()) => fail("accepted")
        }
      }
    }
  }

  // 12 -----------------------------------------------------------------------------------------
  // The date-histogram key-filter refusal (required: this story would otherwise accept the qualified spelling of a dropped filter).
  "a key predicate on a DATE grouping level" should "be refused by name -- a date histogram has no key filter" in {
    Seq(
      "SELECT COUNT(*) AS c FROM t GROUP BY DATE_TRUNC(d, MONTH) HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'",
      "SELECT COUNT(*) AS c FROM t q GROUP BY DATE_TRUNC(q.d, MONTH) " +
      "HAVING DATE_TRUNC(q.d, MONTH) = '2024-01-01'"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql) match {
          case Left(reason) =>
            reason should startWith("HAVING cannot filter a DATE grouping level")
            reason should include("WHERE d >= '2024-01-01' AND d < '2024-02-01'")
          case Right(()) => fail("accepted -- the date_histogram would drop the condition")
        }
      }
    }
  }

  it should "leave a date level with no key predicate, and a non-date function of a date, alone" in {
    verdict(
      "SELECT COUNT(*) AS c FROM t GROUP BY DATE_TRUNC(d, MONTH) HAVING COUNT(*) > 1"
    ) shouldBe Right(())
    verdict("SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d) HAVING YEAR(d) = 2024") shouldBe Right(())
    r1("SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d) HAVING YEAR(d) = 2024")
      .update(Some(dated))
      .validateResolved() shouldBe Right(())
  }

  it should "leave a statement the RELATIONAL engine groups alone -- no date histogram exists there" in {
    // Answered after the join by the relational engine; rule (b), the only reader of the
    // date-histogram arm, does not run in such a statement. A derived-table FROM is as relational
    // as a JOIN.
    Seq(
      "SELECT COUNT(*) AS c FROM (SELECT d FROM t) AS x GROUP BY DATE_TRUNC(d, MONTH) " +
      "HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'",
      "SELECT COUNT(*) AS c FROM orders o JOIN depts p ON o.did = p.id GROUP BY DATE_TRUNC(d, MONTH) " +
      "HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'"
    ).foreach { sql =>
      withClue(s"[$sql] ") { verdict(sql) shouldBe Right(()) }
    }
  }

  // 13 -----------------------------------------------------------------------------------------
  // The HAVING rules seeing a MATCH (required: this story makes the qualified MATCH channel live at the index pattern).
  "a MATCH on the GROUP BY key" should "be seen by every HAVING rule" in {
    val group = "SELECT COUNT(*) AS c FROM t GROUP BY s HAVING "
    Seq(
      group + "MATCH (s) AGAINST ('x') OR COUNT(*) > 1" -> Seq("different mechanisms"),
      // since #408 a MATCH over a column that is neither a key nor an aggregate is refused by the
      // MATCH rule itself -- not a witness of the HAVING rules seeing a MATCH
      "SELECT COUNT(*) AS c FROM t GROUP BY s HAVING MATCH (amount) AGAINST ('x')" ->
      Seq("HAVING cannot apply MATCH"),
      // the message quotes the MATCH as written, never its desugaring into an OR
      "SELECT COUNT(*) AS c FROM t GROUP BY s, id HAVING MATCH (s, id) AGAINST ('x')" ->
      Seq("DIFFERENT GROUP BY keys", "MATCH (s,id) AGAINST")
    ).foreach { case (sql, reasons) =>
      withClue(s"[$sql] ") {
        verdict(sql) match {
          case Left(r)   => reasons.foreach(reason => r should include(reason))
          case Right(()) => fail("accepted -- the rules did not see the MATCH")
        }
      }
    }
    verdict(group + "MATCH (s) AGAINST ('x')") shouldBe Right(())
    verdict(group + "MATCH (s) AGAINST ('x') AND COUNT(*) > 1") shouldBe Right(())
  }

  it should "be seen by the materialized view's key rule and by the date-histogram rule" in {
    // A view's transform has no key filter, and a date histogram has none either: a MATCH on such a
    // key was carried by no channel, and every group came back.
    Seq(
      "CREATE MATERIALIZED VIEW mv_probe AS SELECT k, COUNT(*) AS c FROM kt GROUP BY k " +
      "HAVING MATCH (k) AGAINST ('x')" -> "MATERIALIZED VIEW cannot filter on a grouping key in HAVING",
      "SELECT DATE_TRUNC(d, MONTH) AS m, COUNT(*) AS c FROM t GROUP BY m HAVING MATCH (m) AGAINST ('2024')" ->
      "HAVING cannot filter a DATE grouping level"
    ).foreach { case (sql, reason) =>
      withClue(s"[$sql] ") {
        verdict(sql) match {
          case Left(r)   => r should include(reason)
          case Right(()) => fail("accepted -- no channel carries the MATCH")
        }
      }
    }
  }

  // 15 -----------------------------------------------------------------------------------------
  "a window PARTITION BY key" should "keep today's attachment (R-c) and never take the GROUP BY route (R-b)" in {
    // R-c: a qualified SELECT item naming a window partition key keeps the window bucket -- today's
    // answer.
    val windowed =
      r1(
        "SELECT a.category, ROW_NUMBER() OVER (PARTITION BY a.category ORDER BY a.id) AS rn FROM orders a"
      )
    val item = windowed.select.fields.head.identifier
    windowed.bucketFor(item).isDefined shouldBe true
    windowed.bucketFor(item).map(_.name) shouldBe windowed.bucketNames
      .get(item.identifierName)
      .map(_.name)
    // R-b is GROUP BY keys only: the window root `category` sorts BEFORE the GROUP BY root `id`
    // (`BucketTree.fromBuckets` sorts roots by path), so a route that admitted it would attach the
    // key predicate to the window copy -- which is NOT qualifier-resolved, so `sameKeyAs` then
    // misses the GROUP BY level.
    val mixed =
      "SELECT COUNT(*) AS c, MAX(o.amount) OVER (PARTITION BY o.category) AS m FROM orders o " +
      "GROUP BY o.id, o.category HAVING o.category = 'x'"
    verdict(mixed) shouldBe Right(())
    channelsOf(r1(mixed)).collectFirst { case ("id>category", kept, removed) =>
      (kept, removed)
    } shouldBe
    Some(keeps("x"))
  }

  // =============================================================================================
  // Further families, and the in-process verdict rows of the JOIN classes. Each row is COLLECTED.
  // =============================================================================================

  private type Channels = Seq[(BucketIncludesExcludes, BucketIncludesExcludes)]

  private val keyedTable: SchemaTable =
    SchemaTable("kt", columns = List(Column("k", SQLTypes.Keyword), Column("amount", SQLTypes.Int)))

  private val empDept: Map[String, SchemaTable] = Map(
    "emp" -> SchemaTable(
      "emp",
      columns = List(
        Column("dept_id", SQLTypes.Keyword),
        Column("name", SQLTypes.Keyword),
        Column("salary", SQLTypes.Int)
      )
    ),
    "dept" -> SchemaTable(
      "dept",
      columns = List(Column("dept_id", SQLTypes.Keyword), Column("dept_name", SQLTypes.Keyword))
    )
  )

  private val joinSchemas: Map[String, SchemaTable] =
    Map("orders" -> ordersSchema, "depts" -> deptsSchema) ++ empDept

  /** (verdict family, R1 channels, R2 channels): what the tree under test answers. */
  private def observation(
    sql: String,
    second: SingleSearch => SingleSearch
  ): (String, Try[Channels], Try[Channels]) = {
    val once = Try(r1(sql))
    (verdictFamily(sql), once.map(channels), once.flatMap(s => Try(channels(second(s)))))
  }

  // A one-key AND/OR mix ----------------------------------------------------------------------
  "a qualified twin of a one-key AND/OR mix (ANDOR)" should
  "answer exactly as its bare spelling -- verdict and channels at both resolutions" in {
    val conditions = Seq(
      "k = 'v1' OR k = 'v2' AND NOT k = 'v3'",
      "(k = 'v1' OR k = 'v2' AND NOT k = 'v3')",
      "NOT k = 'v1' AND NOT k = 'v2' AND k = 'v3' OR k = 'v4'",
      "k = 'v1' OR k = 'v2' AND NOT k = 'v3' AND NOT k = 'v4'",
      "k = 'a' OR k = 'b' AND k <> 'c'",
      // refused by (b2) in the bare spelling: its lists would drop a value SQL keeps
      "k = 'v3' OR k = 'v2' AND NOT k = 'v3'"
    )
    val second = (s: SingleSearch) => s.update(Some(keyedTable))
    val wrong = for {
      condition <- conditions
      bareSql = s"SELECT COUNT(*) AS c FROM kt GROUP BY k HAVING $condition"
      bare = observation(bareSql, second)
      (groupBy, having) <- Seq("q.k" -> "q.k", "k" -> "q.k", "q.k" -> "k", "k" -> "k")
      sql = s"SELECT COUNT(*) AS c FROM kt q GROUP BY $groupBy HAVING " +
        condition.replaceAll("\\bk\\b", having)
      seen = observation(sql, second)
      problem <- Seq(
        if (seen._1 != bare._1) Some(s"verdict ${seen._1}, bare ${bare._1}") else None,
        if (seen._2.toOption != bare._2.toOption) Some(s"R1 ${seen._2}, bare ${bare._2}") else None,
        if (seen._3.toOption != bare._3.toOption) Some(s"R2 ${seen._3}, bare ${bare._3}") else None
      ).flatten.map(p => s"[$sql] $p")
    } yield problem
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  // A nameless key over one correlation's column ---------------------------------------------
  "a NAMELESS GROUP BY key over one correlation's column" should
  "be accepted beside another correlation's same-named key and without it, at both resolutions" in {
    // At R1 and R2 (`updateAll`): accepted beside a grouping of `a.category` (the argument matches
    // that key by name), and without it (the argument addresses its nameless key's own level).
    // Without the companion it used to be refused by rule (a), which read only the argument's
    // name: the relational engine evaluates such a condition after the join.
    val cells = for {
      shape <- Seq(TwoIndices, SelfJoin)
      key   <- Seq("UPPER(b.category)", "CONCAT(a.category, b.category)")
      withX <- Seq(true, false)
      groupBy = if (withX) s"a.category, $key" else key
    } yield (
      s"SELECT COUNT(*) AS c ${shape.from} GROUP BY $groupBy HAVING $key = 'X'",
      "accepted"
    )
    val second = (s: SingleSearch) => s.updateAll(joinSchemas)
    val wrong = cells.flatMap { case (sql, expected) =>
      val (family, once, twice) = observation(sql, second)
      Seq(
        if (family == expected) None else Some(s"[$sql] $family, expected $expected"),
        // the two resolutions agree on the channels
        if (once.toOption == twice.toOption) None else Some(s"[$sql] R1 $once, R2 $twice")
      ).flatten
    }
    // the sibling with no same-named key, likewise
    val sibling =
      "SELECT COUNT(*) AS c FROM orders a JOIN depts p ON a.did = p.id GROUP BY UPPER(p.category) " +
      "HAVING UPPER(p.category) = 'X'"
    verdictFamily(sibling) shouldBe "accepted"
    withClue(s"${wrong.size} of ${cells.size} wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  // The population with its key aliased ------------------------------------------------------
  "the qualified-key population with its key ALIASED in the SELECT" should
  "put each predicate on exactly the level it names, at both resolutions, as unaliased" in {
    val aliased = cells.collect { case cell @ Cell(_, OneKey(q), _, _) =>
      cell -> cell.sql.replace(
        "SELECT COUNT(*) AS c,",
        s"SELECT ${spell(q, "category")} AS k, COUNT(*) AS c,"
      )
    }
    val outcomes = aliased.map { case (cell, sql) =>
      val once = Try(r1(sql))
      val twice = once.flatMap(s => Try(r2(cell.shape, s)))
      val family = verdictFamily(sql)
      val problems = Seq(
        if (once.map(channels).toOption != Some(cell.expectedChannels))
          Some(s"R1 ${once.map(channels)}")
        else None,
        if (twice.map(channels).toOption != Some(cell.expectedChannels))
          Some(s"R2 ${twice.map(channels)}")
        else None,
        (family, cell.addressed) match {
          case ("accepted", Some(_))                                                        => None
          case (f, None) if f == (if (cell.op.label == "MATCH") "match-refusal" else "(a)") => None
          case (f, expected) =>
            Some(s"verdict $f, expected ${if (expected.isDefined) "accepted" else "refused"}")
        }
      ).flatten
      (cell, sql, family, problems)
    }
    outcomes
      .groupBy(o => if (o._1.op.label == "MATCH") "MATCH" else "non-MATCH")
      .toSeq
      .sortBy(_._1)
      .foreach { case (kind, group) =>
        val split = group.groupBy(_._3).map { case (f, os) => s"$f=${os.size}" }
        info(
          s"verdicts, aliased, $kind (${group.size} cells): ${split.toSeq.sorted.mkString(" · ")}"
        )
      }
    val wrong = outcomes.filter(_._4.nonEmpty)
    withClue(
      s"${wrong.size} of ${outcomes.size} aliased cells wrong:\n" +
      wrong.map(o => s"[${o._1.label}] ${o._2}\n    ${o._4.mkString("; ")}").mkString("\n") + "\n"
    ) { wrong shouldBe empty }
  }

  // A JOIN: an OR across levels, and #389's key rows ------------------------------------------
  private val joinedAgg =
    "SELECT d.dept_name, COUNT(*) AS headcount FROM emp e JOIN dept d ON e.dept_id = d.dept_id " +
    "GROUP BY d.dept_name HAVING "

  "a HAVING the relational engine evaluates (a JOIN)" should
  "accept an OR across GROUP BY levels and #389's key rows, which DuckDB answers" in {
    val accepted = Seq(
      // the bare OR across two functions of one column, and its qualified spelling
      "SELECT COUNT(*) AS c FROM orders o JOIN depts p ON o.did = p.id GROUP BY YEAR(d), MONTH(d) " +
      "HAVING YEAR(d) = 2024 OR MONTH(d) = 1",
      "SELECT COUNT(*) AS c FROM orders o JOIN depts p ON o.did = p.id GROUP BY YEAR(o.d), MONTH(o.d) " +
      "HAVING YEAR(o.d) = 2024 OR MONTH(o.d) = 1",
      // the OR across same-named keys of two correlations, unaliased and SELECT-aliased
      selfJoin + "HAVING o.category = 'x' OR p.category = 'y'",
      "SELECT COUNT(*) AS c FROM orders o JOIN depts p ON o.did = p.id GROUP BY o.category, p.category " +
      "HAVING o.category = 'x' OR p.category = 'y'",
      "SELECT o.category AS c1, p.category AS c2, COUNT(*) AS c FROM orders o JOIN orders p " +
      "ON o.did = p.id GROUP BY c1, c2 HAVING c1 = 'x' OR c2 = 'y'",
      // #389's JOIN rows: the key itself (a bare key filter), a function of it, a range, an AND on it (a conjunction on the key)
      joinedAgg + "d.dept_name = 'eng'",
      joinedAgg + "UPPER(d.dept_name) = 'ENG'",
      joinedAgg + "d.dept_name > 'm'",
      joinedAgg + "d.dept_name = 'a' AND d.dept_name = 'b'",
      "SELECT dept_name, COUNT(*) AS headcount FROM emp e JOIN dept d ON e.dept_id = d.dept_id " +
      "GROUP BY dept_name HAVING UPPER(dept_name) = 'ENG'"
    )
    val second = (s: SingleSearch) => s.updateAll(joinSchemas)
    val wrong = accepted.flatMap { sql =>
      val (family, _, _) = observation(sql, second)
      if (family == "accepted") None else Some(s"[$sql] $family")
    }
    withClue(s"${wrong.size} of ${accepted.size} wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  it should "keep refusing the same rows wherever Elasticsearch groups them (the controls)" in {
    val levelOr =
      "SELECT COUNT(*) AS c FROM t GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 OR MONTH(d) = 1"
    val controls = Seq(
      levelOr                                 -> "(c)-keys",
      s"SELECT c FROM ($levelOr) AS x"        -> "(c)-keys",
      s"WITH x AS ($levelOr) SELECT c FROM x" -> "(c)-keys",
      "SELECT d.dept_name, COUNT(*) AS headcount FROM dept d GROUP BY d.dept_name " +
      "HAVING UPPER(d.dept_name) = 'ENG'" -> "(b)",
      "SELECT dept_name, COUNT(*) AS headcount FROM dept GROUP BY dept_name " +
      "HAVING UPPER(dept_name) = 'ENG'" -> "(b)",
      "SELECT dept_name, COUNT(*) AS headcount FROM dept GROUP BY dept_name HAVING dept_name > 'm'" -> "(b)",
      "SELECT dept_name, COUNT(*) AS headcount FROM dept GROUP BY dept_name " +
      "HAVING dept_name = 'a' AND dept_name = 'b'" -> "(b2)",
      "CREATE MATERIALIZED VIEW mv_probe AS SELECT o.category, COUNT(*) AS c FROM orders o " +
      "JOIN depts p ON o.did = p.id GROUP BY o.category HAVING o.category = 'x' OR o.category = 'y'" -> "view"
    )
    val wrong = controls.flatMap { case (sql, expected) =>
      val family = verdictFamily(sql)
      if (family == expected) None else Some(s"[$sql] $family, expected $expected")
    }
    withClue(s"${wrong.size} of ${controls.size} wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }
}
