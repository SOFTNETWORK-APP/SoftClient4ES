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

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A HAVING that repeats a NAMELESS GROUP BY key -- a string, math, arithmetic, `COALESCE` or
  * `CASE` call over a column, `GROUP BY UPPER(s) HAVING UPPER(s) = 'A'` -- addresses that key's own
  * level.
  *
  * The scope rule read only the ARGUMENT's name, and no level is named like it, so the statement
  * was refused as reading "neither a GROUP BY key nor an aggregate", where 0.23.0 filtered the
  * level. The route is gated at the final seams: the filter compares the key's script value as
  * TEXT, so it is open only where that text is known to be the condition's spelling.
  */
class NamelessKeyFilterSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("s", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  private val GateLead = "HAVING cannot filter the GROUP BY key "

  private def parsed(sql: String): Either[String, SingleSearch] =
    Parser(sql) match {
      case Right(ss: SingleSearch) => Right(ss)
      case Right(other)            => fail(s"[$sql] expected a SingleSearch, got $other")
      case Left(err)               => Left(err.msg)
    }

  private def single(sql: String): SingleSearch =
    parsed(sql).fold(err => fail(s"[$sql] refused at parse: $err"), identity)

  private def resolved(sql: String): Either[String, Unit] =
    single(sql).update(Some(schema)).validateResolved()

  private def unresolved(sql: String): Either[String, Unit] = single(sql).validateUnresolved()

  /** The named column leaves of the first HAVING leaf, with the level and route each addresses. */
  private def routes(ss: SingleSearch): Seq[(String, Option[(String, ss.KeyRoute)])] =
    ss.havingLeaves.headOption.toSeq.flatMap { e =>
      ss.namedLeavesOf(e)
        .map((l: Identifier) =>
          l.name -> ss.keyLevelOf(l).map { case (b, r) =>
            (b.identifier.toString, r)
          }
        )
    }

  "an argument of a nameless key" should "address that key's level when no level is named like it" in {
    val ss =
      single("SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) = 'A'")
    routes(ss) shouldBe Seq("s" -> Some(("UPPER(s)", ss.KeyRoute.NamelessParent)))
    ss.havingScopeOf(ss.havingLeaves.head) shouldBe ss.HavingScope.GroupKey
  }

  it should "keep addressing the level named like it, when there is one" in {
    val ss = single(
      "SELECT s, UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY s, UPPER(s) HAVING UPPER(s) = 'A'"
    )
    routes(ss) shouldBe Seq("s" -> Some(("s", ss.KeyRoute.Direct)))
  }

  "the rows the scope rule must keep refusing" should "address no level, and be refused as before" in {
    Seq(
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING LOWER(s) = 'a'",
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING s = 'a'",
      "SELECT UPPER(a.s) AS k, COUNT(*) AS c FROM t a GROUP BY UPPER(a.s) HAVING UPPER(s) = 'A'"
    ).foreach { sql =>
      withClue(sql) {
        parsed(sql).left.getOrElse("") should include(
          "HAVING can only filter on a GROUP BY key or on an aggregate"
        )
      }
    }
    parsed("SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING s = 'a'").left
      .getOrElse("") shouldBe
    "HAVING can only filter on a GROUP BY key or on an aggregate; s = 'a' is neither. Move it to " +
    "WHERE, or add its column to the GROUP BY."
    // a function of a bare key, a range, an OR with an aggregate: refused by their own rules
    Seq(
      "SELECT s, COUNT(*) AS c FROM t GROUP BY s HAVING UPPER(s) = 'A'",
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) > 'A'",
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) = 'A' OR COUNT(*) > 1"
    ).foreach { sql => withClue(sql) { parsed(sql).isLeft shouldBe true } }
  }

  "the nameless-key route" should "be open with the mapping for a string key and a whole-number key" in {
    Seq(
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) = 'A'",
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) IN ('A', 'BB')",
      "SELECT LOWER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s), LOWER(s) HAVING LOWER(s) = 'a'",
      "SELECT LENGTH(s) AS k, COUNT(*) AS c FROM t GROUP BY LENGTH(s) HAVING LENGTH(s) = 1",
      "SELECT n + 1 AS k, COUNT(*) AS c FROM t GROUP BY n + 1 HAVING n + 1 = 2"
    ).foreach { sql => withClue(sql) { resolved(sql) shouldBe Right(()) } }
  }

  it should "be closed without the mapping, naming the column whose type is not known" in {
    Seq(
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) = 'A'",
      "SELECT LENGTH(s) AS k, COUNT(*) AS c FROM t GROUP BY LENGTH(s) HAVING LENGTH(s) = 1"
    ).foreach { sql =>
      withClue(sql) {
        val reason = unresolved(sql).left.getOrElse("")
        reason should startWith(GateLead)
        reason should include("the type of its column s is not known")
        reason should not include "#"
      }
    }
  }

  it should "be closed with the mapping for a key over a date, a decimal key or a whole number written 1.0" in {
    Seq(
      "CONCAT(d, '')"             -> "'2024-01-03'" -> "it reads the date column d",
      "COALESCE(d, '2024-01-01')" -> "'2024-01-03'" -> "it reads the date column d",
      "ABS(n)"                    -> "1.0"          -> "as 1 or as 1.0",
      "LENGTH(s)"                 -> "1.0"          -> "Compare it with 1."
    ).foreach { case ((key, value), cause) =>
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $key = $value"
      withClue(sql) {
        val reason = resolved(sql).left.getOrElse("")
        reason should startWith(GateLead + key)
        reason should include(cause)
      }
    }
  }

  it should "not gate a companion level's route, with or without the mapping" in {
    val sql =
      "SELECT s, UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY s, UPPER(s) HAVING UPPER(s) = 'A'"
    unresolved(sql) shouldBe Right(())
    resolved(sql) shouldBe Right(())
  }

  "a nameless key returning a date" should
  "be accepted at parse, and refused on the resolved statement and without the mapping" in {
    Seq("COALESCE(d, d)", "CASE WHEN n > 0 THEN d ELSE d END").foreach { key =>
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $key = '2024-01-03'"
      withClue(sql) {
        parsed(sql).isRight shouldBe true
        resolved(sql).left.getOrElse("") should startWith(
          "HAVING cannot filter a DATE grouping level"
        )
        unresolved(sql).left.getOrElse("") should startWith(GateLead + key)
      }
    }
  }
}
