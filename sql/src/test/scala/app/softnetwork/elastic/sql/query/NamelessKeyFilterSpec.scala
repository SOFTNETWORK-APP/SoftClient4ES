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
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("r", SQLTypes.Real)
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

  it should "be closed with the mapping for a key over a date" in {
    Seq(
      "CONCAT(d, '')"             -> "'2024-01-03'" -> "it reads the date column d",
      "COALESCE(d, '2024-01-01')" -> "'2024-01-03'" -> "it reads the date column d"
    ).foreach { case ((key, value), cause) =>
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $key = $value"
      withClue(sql) {
        val reason = resolved(sql).left.getOrElse("")
        reason should startWith(GateLead + key)
        reason should include(cause)
      }
    }
  }

  it should "be open with the mapping for a decimal key and for a whole number written 1.0" in {
    // These were refused ("writes such a number as 1 or as 1.0", "a whole number is written 1
    // there, never 1.0"): the filter sent the literal's own spelling, which the key's script value
    // may not print. It now sends every text a number of that value prints (`keyFilterTexts`),
    // so the key matches however it prints. DuckDB 1.5.5: `ABS(n) = 1.0` keeps the groups whose
    // |n| is 1 and `LENGTH(s) = 1.0` the groups whose s is one character long (`1 = 1.0` is TRUE).
    // Over the HAVING key fixture of `GroupByCompletenessSpec` it answers (1, 1) for ABS(n) = 1.0,
    // (2.0, 1) for SQRT(n) = 2 (n >= 0) and (1, 3) for LENGTH(s) = 1.0.
    Seq(
      "ABS(n)"    -> "1.0" -> Set("1", "1.0"),
      "SQRT(n)"   -> "2"   -> Set("2", "2.0"),
      "LENGTH(s)" -> "1.0" -> Set("1", "1.0")
    ).foreach { case ((key, value), sent) =>
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $key = $value"
      withClue(sql) {
        resolved(sql) shouldBe Right(())
        val ss = single(sql).update(Some(schema))
        ss.having
          .flatMap(_.criteria)
          .map(
            _.includes(ss.buckets.head, not = false, BucketIncludesExcludes()).values
          ) shouldBe Some(sent)
      }
    }
  }

  it should "stay closed, as before, for a key of a kind that is not proven" in {
    // A key over a REAL column is not of a proven kind: Painless computes it in double, where
    // DuckDB 1.5.5 computes it in REAL (it keeps (0.1, n) for ABS(r) = 0.1 over r = 0.1, and
    // (1.0, n) for r * 10 = 1), and so is a REAL produced by a cast. Such a key keeps the literal's
    // own text and the gate judges it as before, with its reason.
    val asBefore = "which writes such a number as 1 or as 1.0 depending on the column's type."
    Seq(
      "ABS(r)"               -> "0.1"       -> asBefore,
      "ABS(r)"               -> "2.5"       -> asBefore,
      "ABS(r)"               -> "2"         -> asBefore,
      "r + 1"                -> "1.1"       -> asBefore,
      "r * 10"               -> "1"         -> asBefore,
      "r * 10"               -> "IN (1, 2)" -> asBefore,
      "ABS(r)"               -> "'0.5'"     -> asBefore,
      "ABS(CAST(x AS REAL))" -> "0.1"       -> asBefore
    ).foreach { case ((key, value), cause) =>
      val condition = if (value.startsWith("IN")) s"$key $value" else s"$key = $value"
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM t GROUP BY $key HAVING $condition"
      withClue(sql) {
        val reason = resolved(sql).left.getOrElse("")
        reason should startWith(GateLead)
        reason should include(cause)
      }
    }
    // ... and open where it is: ROUND, to decimal places or not, over an integer or a DOUBLE (keys
    // of a proven kind: a ROUND to decimal places is the DOUBLE DuckDB computes -- DuckDB keeps
    // (2.5, n) for ROUND(x, 1) = 2.5 over x = 2.46), and, as before, a whole-number key over a REAL
    // column compared with an integer: it keeps the literal's own text, the integer the key prints
    // (DuckDB: FLOOR(r) = 1 keeps the groups whose r is in [1, 2))
    Seq(
      "SELECT ROUND(x, 1) AS k, COUNT(*) AS c FROM t GROUP BY ROUND(x, 1) HAVING ROUND(x, 1) = 2.5",
      "SELECT ROUND(x, 1) + 0 AS k, COUNT(*) AS c FROM t GROUP BY ROUND(x, 1) + 0 " +
      "HAVING ROUND(x, 1) + 0 = 5",
      "SELECT ROUND(n, 1) AS k, COUNT(*) AS c FROM t GROUP BY ROUND(n, 1) HAVING ROUND(n, 1) = 5",
      "SELECT ROUND(x) AS k, COUNT(*) AS c FROM t GROUP BY ROUND(x) HAVING ROUND(x) = 3",
      "SELECT FLOOR(r) AS k, COUNT(*) AS c FROM t GROUP BY FLOOR(r) HAVING FLOOR(r) = 1"
    ).foreach { sql => withClue(sql) { resolved(sql) shouldBe Right(()) } }
    val floor = single(
      "SELECT FLOOR(r) AS k, COUNT(*) AS c FROM t GROUP BY FLOOR(r) HAVING FLOOR(r) = 1"
    ).update(Some(schema))
    floor.having
      .flatMap(_.criteria)
      .map(_.includes(floor.buckets.head, not = false, BucketIncludesExcludes()).values) shouldBe
    Some(Set("1"))
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
