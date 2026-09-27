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

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.Try

/** A function over an UNNEST column that Elasticsearch evaluates against a document that does not
  * hold the column is REFUSED by name, in SELECT and in WHERE -- and nothing else is.
  *
  * The ORACLE never asks the rule: it asks the engine what it would EVALUATE (the rendered Painless
  * of each SELECT script field and WHERE leaf, and the `doc['<path>']` reads in it) and WHERE (the
  * parent document, or the element of the `nested` query the leaf is rendered in). A read of a
  * column that document does not hold is what answered RAW / NULL / zero rows at RUN level; the
  * verdict must be a refusal exactly then. A walk that misses an argument kind (the `DISTANCE`
  * operands, a `CASE`'s `WHEN`) reddens the population row, because the rendered script still reads
  * it.
  */
class UnnestFunctionRefusalSpec extends AnyFlatSpec with Matchers {

  import UnnestPopulation._

  private val Phrase = UnnestScope.StablePhrase

  /** `Some(message)` when THIS rule refused, `None` when every rule accepted, `Left` otherwise. */
  private def verdict(sql: String): Either[String, Option[String]] =
    Parser(sql) match {
      case Right(_)                            => Right(None)
      case Left(e) if e.msg.startsWith(Phrase) => Right(Some(e.msg))
      case Left(e)                             => Left(e.msg)
    }

  private def unvalidated(sql: String): SingleSearch =
    Parser.parseUnvalidated(sql) match {
      case Right(ss: SingleSearch) => ss
      case other                   => fail(s"did not parse as a SingleSearch: $other\n$sql")
    }

  private val DocRead = "doc\\['([^']+)'\\]".r

  /** The UNNEST alias a rendered doc path belongs to -- by the nested PATH, or by the alias as
    * written (a copy `update` never resolved renders `doc['<alias>.<col>']`); `None` = the parent.
    */
  private def scopeOfPath(ss: SingleSearch, path: String): Option[String] = {
    val byPath = ss.unnestAliases.collectFirst {
      case (alias, (name, _)) if path.startsWith(name + ".") => alias
    }
    byPath.orElse(Some(path.takeWhile(_ != '.')).filter(ss.unnestAliases.contains))
  }

  private def reads(script: String): Seq[String] =
    DocRead.findAllMatchIn(script).map(_.group(1)).toSeq

  /** The script exactly as the bridge sends it: the context's parameter declarations (where every
    * `doc['…']` read is) followed by the body -- `s"$context$script"`, both bridges.
    */
  private def rendered(painless: Option[PainlessContext] => String): Option[String] =
    Try {
      val context = PainlessContext(PainlessContextType.Query)
      val body = painless(Some(context))
      s"$context$body"
    }.toOption

  /** The ORACLE for a SELECT script field: does its rendered script read an UNNEST column? */
  private def selectEscapes(ss: SingleSearch): Option[Boolean] =
    ss.scriptFields.headOption.flatMap { f =>
      rendered(f.painless).map(script => reads(script).exists(scopeOfPath(ss, _).isDefined))
    }

  /** The ORACLE for the one WHERE leaf: rendered under a `nested` query or at the top level, does
    * its script read a column that document does not hold?
    */
  private def whereEscapes(ss: SingleSearch): Option[Boolean] = {
    def leaf(c: Criteria, underNested: Boolean): Option[(Expression, Boolean)] = c match {
      case Predicate(l, _, r, _, _) => leaf(l, underNested).orElse(leaf(r, underNested))
      case n: ElasticNested         => leaf(n.criteria, underNested = true)
      case e: Expression            => Some(e -> underNested)
      case _                        => None
    }
    ss.where.flatMap(_.criteria).flatMap(leaf(_, underNested = false)).flatMap { case (e, nested) =>
      val evaluatedIn =
        if (nested) e.identifier.nestedElement.map(_.innerHitsName) else None
      rendered(e.painless).map(script => reads(script).exists(scopeOfPath(ss, _) != evaluatedIn))
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The DERIVED population
  // ---------------------------------------------------------------------------------------------

  "the UNNEST population" should "exercise every function family of the registry" in {
    val byFamily = exercised.groupBy(_.family).map { case (f, cs) => f -> cs.size }
    info(s"exercised ${exercised.size} of ${candidates.size} candidates: $byFamily")
    info(s"not exercised: ${notExercised.mkString(", ")}")
    // Floors = the counts MEASURED at BASE 9bf185f1 (story IDENT-5): they guard the population
    // against shrinking silently (a template or registry change), not the verdict.
    exercised.size should be >= 2427
    Seq("temporal", "string", "math", "conversion", "conditional", "arithmetic", "geo")
      .foreach(f => byFamily.getOrElse(f, 0) should be > 0)
    // The three ranking windows take no column: they are the only spellings no shape reaches.
    notExercised.map(_._1).toSet shouldBe Set("DENSE_RANK", "RANK", "ROW_NUMBER")
  }

  "a function projected over an UNNEST column" should "be refused by name, under both aliases" in {
    var refused, accepted = 0
    for {
      c     <- exercised.filterNot(_.family == "aggregate")
      alias <- aliases
    } {
      val sql = selectSql(c, alias)
      val ss = unvalidated(sql)
      if (ss.scriptFields.nonEmpty) { // not an aggregate / window: a script field
        withClue(s"[$sql] ") {
          selectEscapes(ss) shouldBe Some(true) // the oracle: the script reads the UNNEST column
          verdict(sql) match {
            case Right(Some(msg)) =>
              refused += 1
              msg should startWith(s"${Phrase}SELECT: ")
              msg should endWith("Select its columns and compute it from the returned rows.")
            case other => fail(s"not refused by name: $other")
          }
        }
      } else accepted += 1
    }
    info(s"SELECT refused $refused, not a script field $accepted")
    refused should be >= 4500
  }

  it should "not be refused when the same function reads the PARENT's column" in {
    var checked = 0
    for {
      c     <- exercised.filterNot(_.family == "aggregate")
      alias <- aliases
      sql   <- parentSelectSql(c, alias)
      if Parser.parseUnvalidated(sql).isRight
    } {
      withClue(s"[$sql] ") {
        verdict(sql) match {
          case Right(Some(msg)) => fail(s"refused a parent-column function: $msg")
          case _                => checked += 1
        }
      }
    }
    info(s"parent-column twins accepted $checked")
    checked should be >= 1500
  }

  "a function projected beside a window function" should "be refused like the plain projection -- the row query that runs carries it" in {
    // A window-enriched ROW query runs its rows through `withoutWindows` (core's `createBaseQuery`):
    // THAT statement's script fields are evaluated per parent. The statement as parsed has none
    // (a window counts as an aggregate), which is how this venue escaped the rule once.
    var refused, twins = 0
    for {
      c     <- exercised.filterNot(_.family == "aggregate")
      alias <- aliases
    } {
      val sql = selectBesideWindowSql(c, alias)
      val ss = unvalidated(sql)
      withClue(s"[$sql] ") {
        ss.windowRowQuery shouldBe true
        selectEscapes(ss.withoutWindows) shouldBe Some(true) // the oracle, on the query that runs
        verdict(sql) match {
          case Right(Some(msg)) =>
            refused += 1
            msg should startWith(s"${Phrase}SELECT: ")
          case other => fail(s"not refused by name: $other")
        }
      }
      // the same expression over the PARENT's column, beside the same window: untouched
      parentSelectSql(c, alias, Some(window)).filter(Parser.parseUnvalidated(_).isRight).foreach {
        twin =>
          withClue(s"[$twin] ") {
            verdict(twin) match {
              case Right(Some(msg)) => fail(s"refused a parent-column function: $msg")
              case _                => twins += 1
            }
          }
      }
    }
    info(s"beside a window: refused $refused, parent twins accepted $twins")
    refused should be >= 4500
    twins should be >= 1500
  }

  "a WHERE condition over an UNNEST column" should "be refused exactly when it is evaluated against a document that does not hold a column it reads" in {
    var refused, accepted, notExercisedCells = 0
    for {
      c     <- exercised.filterNot(_.family == "aggregate")
      alias <- aliases
    } {
      // the first literal the statement's OTHER rules accept (a type-mismatched comparison is not
      // this rule's to judge)
      val attempt = whereLiterals.iterator
        .map(l => whereSql(c, alias, l))
        .map(sql => sql -> verdict(sql))
        .find(_._2.isRight)
      attempt match {
        case None => notExercisedCells += 1
        case Some((sql, v)) =>
          whereEscapes(unvalidated(sql)) match {
            case None => notExercisedCells += 1 // the leaf does not render: not judged
            case Some(escapes) =>
              withClue(s"[$sql] ") {
                v.toOption.flatten.isDefined shouldBe escapes
                v.toOption.flatten.foreach { msg =>
                  msg should startWith(s"${Phrase}WHERE: ")
                  msg should endWith("Select its columns and filter the returned rows.")
                }
              }
              if (escapes) refused += 1 else accepted += 1
          }
      }
    }
    info(s"WHERE refused $refused, accepted $accepted, not exercised $notExercisedCells")
    // BOTH sides must be populated, or the row proves nothing about the one it lacks.
    refused should be >= 1000
    accepted should be >= 300
  }

  // ---------------------------------------------------------------------------------------------
  // The rows that decide what the rule must NOT touch
  // ---------------------------------------------------------------------------------------------

  private val u = "FROM t JOIN UNNEST(t.items) AS i"

  "a WHERE condition whose UNNEST column carries the function" should "keep working (evaluated per element)" in {
    Seq(
      // each MEASURED correct at RUN level (ES 8.18.3 and 6.8.23) against a flat twin index
      s"SELECT id, i.k AS k $u WHERE YEAR(i.d) = 2025",
      s"SELECT id, i.k AS k $u WHERE QUARTER(i.d) = 1",
      s"SELECT id, i.k AS k $u WHERE EXTRACT(YEAR FROM i.d) = 2025",
      s"SELECT id, i.k AS k $u WHERE CAST(i.x AS BIGINT) = 2",
      s"SELECT id, i.k AS k $u WHERE CAST(i.s AS VARCHAR) = 'abc'",
      s"SELECT id, i.k AS k $u WHERE ISNULL(i.d) = FALSE",
      s"SELECT id, i.k AS k $u WHERE ISNOTNULL(i.s) = TRUE",
      s"SELECT id, i.k AS k $u WHERE YEAR(i.d) BETWEEN 2024 AND 2025",
      // a function of NO column is a nameless carrier: it names no column, so it is no read of the
      // parent -- whether it is the compared value or the only function of the condition
      s"SELECT id, i.k AS k $u WHERE YEAR(i.d) = YEAR(CURRENT_DATE)",
      s"SELECT id, i.k AS k $u WHERE i.d > CURRENT_DATE - INTERVAL 1 YEAR",
      // IDENT-1 -- `DayOfWeek`'s receiver is never resolved at BASE (it fails loudly there, a
      // script error); IDENT-1 makes it correct. The rule must accept it either way.
      s"SELECT id, i.k AS k $u WHERE WEEKDAY(i.d) = 0",
      s"SELECT id, i.k AS k $u WHERE DAYOFWEEK(i.d) = 0",
      "SELECT id, items.k AS k FROM t JOIN UNNEST(t.items) AS items WHERE WEEKDAY(items.d) = 0",
      // one level down the receiver copy stays stale even after IDENT-1 (IDENT-4 AD-9): the
      // qualifier AS WRITTEN is what places it in the element
      s"SELECT id, i.k AS k $u WHERE WEEKDAY(LAST_DAY(i.d)) = 0"
    ).foreach(sql => withClue(s"[$sql] ")(verdict(sql) shouldBe Right(None)))
  }

  "WEEKDAY / DAYOFWEEK projected over an UNNEST column" should "be refused under either alias spelling" in {
    Seq(
      s"SELECT id, WEEKDAY(i.d) AS c $u",
      s"SELECT id, DAYOFWEEK(i.d) AS c $u",
      "SELECT id, WEEKDAY(items.d) AS c FROM t JOIN UNNEST(t.items) AS items",
      "SELECT id, DAYOFWEEK(items.d) AS c FROM t JOIN UNNEST(t.items) AS items"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val msg = verdict(sql).toOption.flatten.getOrElse(fail("not refused by name"))
        // the COLUMN as written, never the carrier's chain (`WEEKDAY(i.d)` would say nothing)
        msg should include regex "where (i|items)\\.d is not visible"
      }
    }
  }

  "the rule" should "leave every other UNNEST shape to the mechanism that owns it" in {
    Seq(
      // a plain UNNEST projection is fetched from the inner hits: correct
      s"SELECT id, i.s AS c $u",
      // a function over a PARENT column beside a nested column: correct
      s"SELECT id, i.k AS k, UPPER(t.name) AS c $u",
      // a dotted PARENT object column is no UNNEST column: only the statement's own aliases decide
      s"SELECT id, i.k AS k, UPPER(address.city) AS c $u",
      s"SELECT id, i.k AS k, UPPER(t.address.city) AS c $u WHERE UPPER(address.city) = 'X'",
      // query-DSL conditions the nested query places itself
      s"SELECT id, i.k AS k $u WHERE i.n > 0",
      s"SELECT id, i.k AS k $u WHERE MATCH (i.s) AGAINST ('abc')",
      s"SELECT id, i.k AS k $u WHERE i.s IN (SELECT s FROM w)",
      // an aggregate is not a script field (the GROUP BY family, recorded); a window over
      // arithmetic of UNNEST columns is evaluated in its nested aggregation
      s"SELECT id, MAX(i.n) AS c $u GROUP BY id",
      s"SELECT id, SUM(ABS(i.n)) AS c $u GROUP BY id",
      s"SELECT id, i.k AS k, SUM(i.n * i.x) OVER (PARTITION BY id) AS s $u",
      // other clauses are other mechanisms (recorded, not this rule)
      s"SELECT id, i.k AS k $u ORDER BY UPPER(i.s)",
      s"SELECT i.s AS g, COUNT(*) AS c $u GROUP BY i.s",
      // a column compared with a column of another document carries no function (recorded)
      s"SELECT id, i.k AS k $u WHERE i.n = t.m",
      // a statement the relational engine plans: arrow evaluates the SELECT list itself
      s"SELECT o.id, UPPER(i.s) AS c FROM orders o JOIN UNNEST(o.items) AS i JOIN customers c ON o.cid = c.id",
      s"SELECT o.id, i.k FROM orders o JOIN UNNEST(o.items) AS i JOIN customers c ON o.cid = c.id WHERE UPPER(i.s) = 'A'",
      s"SELECT UPPER(x.s) AS c FROM (SELECT id, i.s AS s $u) x",
      // no UNNEST at all
      "SELECT id, UPPER(s) AS c FROM t WHERE UPPER(s) = 'A'"
    ).foreach(sql => withClue(s"[$sql] ")(verdict(sql) shouldBe Right(None)))
  }

  it should "judge a projection beside a window on the row query that runs, and a window's argument where its aggregation evaluates it" in {
    Seq(
      s"SELECT id, i.k AS k, UPPER(i.s) AS c, SUM(i.n) OVER (PARTITION BY id) AS tot $u",
      s"SELECT id, i.k AS k, UPPER(i.s) AS c, SUM(i.n) OVER (PARTITION BY id) AS tot $u LIMIT 100",
      s"SELECT id, YEAR(i.d) AS y, ROW_NUMBER() OVER (PARTITION BY id ORDER BY i.n) AS rn $u",
      s"SELECT UPPER(i.s) AS c, SUM(i.n) OVER (PARTITION BY id) AS tot $u",
      // a window argument with no nested carrier: its aggregation reads the parent (0.0 at RUN)
      s"SELECT id, i.k AS k, SUM(ABS(i.n)) OVER (PARTITION BY id) AS tot $u"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql).toOption.flatten.getOrElse(fail("not refused by name")) should startWith(
          s"${Phrase}SELECT: "
        )
      }
    }
    Seq(
      // the window's argument is evaluated per element of its nested aggregation; its PARTITION BY
      // is not read
      s"SELECT id, i.k AS k, SUM(i.n) OVER (PARTITION BY id) AS tot $u",
      s"SELECT id, i.k AS k, SUM(i.n * 1) OVER (PARTITION BY t.id) AS tot $u",
      // a PARENT function beside a window: the row query reads the parent, where it is visible
      s"SELECT id, i.k AS k, UPPER(t.name) AS c, SUM(i.n) OVER (PARTITION BY id) AS tot $u",
      // beside a whole-table aggregate the statement is aggregation-shaped: the function is not
      // evaluated at all, on a flat index alike (recorded, not this rule's)
      s"SELECT UPPER(i.s) AS c, COUNT(*) AS n $u",
      s"SELECT id, UPPER(i.s) AS c, SUM(i.n) OVER () AS tot $u"
    ).foreach(sql => withClue(s"[$sql] ")(verdict(sql) shouldBe Right(None)))
  }

  it should "refuse a WHERE condition evaluated per element that reads a PARENT column" in {
    val sql = s"SELECT id, i.k AS k $u WHERE YEAR(i.d) = t.m"
    val msg = verdict(sql).toOption.flatten.getOrElse(fail(s"not refused: $sql"))
    msg should include("is evaluated per element of UNNEST alias 'i', where t.m is not visible")
  }

  it should "refuse a function over an UNNEST column whatever the WHERE operator" in {
    Seq(
      s"SELECT id, i.k AS k $u WHERE UPPER(i.s) LIKE 'A%'",
      s"SELECT id, i.k AS k $u WHERE UPPER(i.s) IN ('A', 'B')",
      s"SELECT id, i.k AS k $u WHERE ABS(i.n) BETWEEN 1 AND 10",
      s"SELECT id, i.k AS k $u WHERE t.m BETWEEN ABS(i.n) AND 10",
      s"SELECT id, i.k AS k $u WHERE t.m = YEAR(i.d)",
      s"SELECT id, i.k AS k $u WHERE UPPER(i.s) IN (SELECT s FROM w)",
      s"SELECT id, i.k AS k $u WHERE DISTANCE(i.loc, POINT(48.85, 2.35)) <= 100 km",
      // one leaf of several: every leaf is judged
      s"SELECT id, i.k AS k $u WHERE i.n > 0 AND UPPER(i.s) = 'A'",
      // a negated leaf is the same leaf
      s"SELECT id, i.k AS k $u WHERE NOT UPPER(i.s) = 'A'",
      // an UNNEST under an UNNEST, and two sibling UNNESTs: an element sees neither its parent
      // element's columns nor a sibling's
      "SELECT id, j.v AS v FROM t JOIN UNNEST(t.items) AS i JOIN UNNEST(i.subs) AS j WHERE YEAR(j.d) = i.n",
      "SELECT id, x.s AS s FROM t JOIN UNNEST(t.a) AS x JOIN UNNEST(t.b) AS y WHERE YEAR(x.d) = y.n",
      // an explicit NESTED(...) around a function with no nested carrier: its predicate was
      // rendered as `match_all` (no nested element to scope it) -- dropped
      s"SELECT id, i.k AS k $u WHERE NESTED(UPPER(i.s) = 'A')"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql).toOption.flatten.getOrElse(fail("not refused by name")) should startWith(
          s"${Phrase}WHERE: "
        )
      }
    }
    // a CASE WHEN condition is read whatever criterion it is: a DISTANCE, an OR, a MATCH
    Seq(
      s"SELECT id, DISTANCE(i.loc, POINT(48.85, 2.35)) AS c $u",
      s"SELECT id, CASE WHEN i.s = 'a' OR t.m > 1 THEN 1 ELSE 0 END AS c $u",
      s"SELECT id, CASE WHEN MATCH (i.s) AGAINST ('x') THEN 1 ELSE 0 END AS c $u"
    ).foreach { select =>
      withClue(s"[$select] ") {
        verdict(select).toOption.flatten.getOrElse(fail("not refused by name")) should startWith(
          s"${Phrase}SELECT: "
        )
      }
    }
  }

  it should "leave a comparison of two COLUMNS of different documents to its own story (no function)" in {
    // RECORDED, not this rule's (lead ruling: functions only) -- a cross-document column comparison
    // carries no function; a literal bound is no function either
    Seq(
      s"SELECT id, i.k AS k $u WHERE i.n = t.m",
      s"SELECT id, i.k AS k $u WHERE t.m = i.n",
      s"SELECT id, i.k AS k $u WHERE t.m BETWEEN i.n AND 10",
      s"SELECT id, i.k AS k $u WHERE i.n BETWEEN t.m AND 10",
      // a parent-child join relation reads OTHER documents, not an UNNEST element
      s"SELECT id, i.k AS k $u WHERE CHILD(UPPER(i.s) = 'A')"
    ).foreach(sql => withClue(s"[$sql] ")(verdict(sql) shouldBe Right(None)))
  }

  it should "refuse the ES-native BODY at every venue that executes one" in {
    Seq(
      s"WITH x AS (SELECT id, UPPER(i.s) AS c $u) SELECT c FROM x",
      s"SELECT c FROM (SELECT id, UPPER(i.s) AS c $u) x",
      s"SELECT a FROM v WHERE a IN (SELECT UPPER(i.s) $u)",
      s"SELECT id, UPPER(i.s) AS c $u UNION ALL SELECT id, s AS c FROM w",
      s"INSERT INTO z (id, c) SELECT id, UPPER(i.s) AS c $u",
      s"CREATE TABLE z AS SELECT id, UPPER(i.s) AS c $u",
      s"CREATE MATERIALIZED VIEW mv AS SELECT id, UPPER(i.s) AS c $u",
      "SELECT id, UPPER(i.s) AS c FROM t* JOIN UNNEST(t.items) AS i"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        Parser(sql) match {
          case Left(e)  => e.msg should startWith(s"${Phrase}SELECT: ")
          case Right(_) => fail("accepted")
        }
      }
    }
  }

  it should "be reported before the HAVING rules, which must not send the analyst back to it" in {
    // WHERE first: its remedy (filter the returned rows) never points at HAVING.
    val both =
      s"SELECT i.s AS g, COUNT(*) AS c $u WHERE UPPER(i.s) = 'A' GROUP BY i.s HAVING UPPER(i.s) = 'B'"
    verdict(both).toOption.flatten.getOrElse(fail("not refused by name")) should startWith(
      s"${Phrase}WHERE: "
    )
    // HAVING rule (a) advises "Move it to WHERE"; there, THIS rule answers with a remedy that ends
    // the chain -- one refusal each, never a loop.
    val havingOnly = s"SELECT id, COUNT(*) AS c $u GROUP BY id HAVING UPPER(i.s) = 'A'"
    Parser(havingOnly).left.map(_.msg).left.getOrElse("") should include("Move it to WHERE")
  }

  // ---------------------------------------------------------------------------------------------
  // The message
  // ---------------------------------------------------------------------------------------------

  "the refusal" should "fit the gateway's excerpt for a typical statement and name the column as written" in {
    val msg = verdict(s"SELECT id, UPPER(i.s) AS c $u").toOption.flatten.get
    msg shouldBe (
      "A function over an UNNEST column is not supported in SELECT: UPPER(i.s) is evaluated per " +
      "parent document, where i.s is not visible. Select its columns and compute it from the " +
      "returned rows."
    )
    // GatewayApi relays a reason of at most 200 characters whole (MaxExcerpt); beyond that it keeps
    // the first 120 and the last 77, which is why the stable phrase leads and the remedy closes.
    msg.length should be <= 200
    msg.takeRight(77) should endWith("Select its columns and compute it from the returned rows.")
    // No internal work-item identifier leaks into a customer-visible message.
    msg should not include "IDENT"
    "(?<![A-Za-z0-9_])#\\d+".r.findFirstIn(msg) shouldBe None
  }
}
