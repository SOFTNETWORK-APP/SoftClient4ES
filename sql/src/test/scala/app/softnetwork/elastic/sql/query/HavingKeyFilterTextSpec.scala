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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The texts a HAVING key filter sends for a literal (`Bucket.keyFilterTexts`).
  *
  * A scripted `terms` level compares each include / exclude value with the TEXT its script value
  * prints, and that text is the Java class's: a Long prints `5`, a Double `5.0`, an int date part
  * `1`. A number on such a level is sent in every text a Java number of that value prints, so the
  * filter matches the key whatever its runtime class. A field level, and a key that prints a
  * string, keep the literal's own text.
  */
class HavingKeyFilterTextSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("s", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("f", SQLTypes.Real)
    )
  )

  private def resolved(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema))
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The (kept, removed) values the HAVING sends on the statement's first level. */
  private def lists(sql: String): (Set[String], Set[String]) = listsOf(resolved(sql), sql)

  /** The same, on the statement as parsed, with no mapping attached. */
  private def unresolvedLists(sql: String): (Set[String], Set[String]) =
    Parser(sql) match {
      case Right(ss: SingleSearch) => listsOf(ss, sql)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def listsOf(ss: SingleSearch, sql: String): (Set[String], Set[String]) = {
    val level = ss.buckets.headOption.getOrElse(fail(s"no level in [$sql]"))
    val criteria = ss.having.flatMap(_.criteria).getOrElse(fail(s"no HAVING in [$sql]"))
    val empty = BucketIncludesExcludes()
    (
      criteria.includes(level, not = false, empty).values,
      criteria.excludes(level, not = false, empty).values
    )
  }

  "a number on a scripted numeric key" should "be sent in its integer and its Double texts" in {
    // `(double)` key over an integer column: the script prints 5.0
    lists(
      "SELECT CAST(n AS DOUBLE) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS DOUBLE) " +
      "HAVING CAST(n AS DOUBLE) IN (5, 1)"
    ) shouldBe ((Set("5", "5.0", "1", "1.0"), Set.empty[String]))
    // ABS over an integer column, in its integer form: the script prints 3
    lists(
      "SELECT ABS(n) AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = 3.0 OR k = 1.0"
    ) shouldBe ((Set("3", "3.0", "1", "1.0"), Set.empty[String]))
    // a date part prints an int
    lists(
      "SELECT MONTH(d) AS k, COUNT(*) AS c FROM t GROUP BY MONTH(d) HAVING MONTH(d) NOT IN (1.0, 3.0)"
    ) shouldBe ((Set.empty[String], Set("1", "1.0", "3", "3.0")))
    // a decimal key with no column of its own
    lists(
      "SELECT SQRT(n) AS k, COUNT(*) AS c FROM t GROUP BY SQRT(n) HAVING SQRT(n) = 2"
    ) shouldBe ((Set("2", "2.0"), Set.empty[String]))
    // a double field read through a function
    lists(
      "SELECT CAST(x AS DOUBLE) AS k, COUNT(*) AS c FROM t GROUP BY CAST(x AS DOUBLE) " +
      "HAVING CAST(x AS DOUBLE) <> 5"
    ) shouldBe ((Set.empty[String], Set("5", "5.0")))
  }

  "a number on a scripted key of a kind that is not proven" should "keep its own text, as before" in {
    // a REAL computed in double, where DuckDB computes in REAL: no spelling is known to match it
    lists(
      "SELECT CAST(f AS REAL) AS k, COUNT(*) AS c FROM t GROUP BY CAST(f AS REAL) " +
      "HAVING CAST(f AS REAL) = 0.3"
    )._1 shouldBe Set("0.3")
    lists(
      "SELECT ABS(f) AS k, COUNT(*) AS c FROM t GROUP BY ABS(f) HAVING ABS(f) IN (1, 2)"
    )._1 shouldBe Set("1", "2")
    lists(
      "SELECT CAST(n AS REAL) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS REAL) " +
      "HAVING CAST(n AS REAL) = 16777217"
    )._1 shouldBe Set("16777217")
    // a ROUND to decimal places over a non-integer value, whose script rounds to a whole number
    lists(
      "SELECT ROUND(x, 1) AS k, COUNT(*) AS c FROM t GROUP BY ROUND(x, 1) HAVING ROUND(x, 1) = 2"
    )._1 shouldBe Set("2")
    // a quoted number too
    lists(
      "SELECT ABS(f) AS k, COUNT(*) AS c FROM t GROUP BY ABS(f) HAVING ABS(f) = '5'"
    )._1 shouldBe Set("5")
  }

  "a number on a scripted key whose columns no mapping typed" should
  "be sent in every text, the Float text too on a key that renders REAL" in {
    // the printed class is unknown there; DuckDB 1.5.5: SELECT CAST(16777217 AS REAL) = 16777216
    // -> true (the literal is cast to REAL)
    unresolvedLists(
      "SELECT CAST(n AS REAL) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS REAL) " +
      "HAVING CAST(n AS REAL) = 16777217"
    )._1 shouldBe Set("16777217", "1.6777217E7", "1.6777216E7")
    unresolvedLists(
      "SELECT CAST(n AS DOUBLE) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS DOUBLE) " +
      "HAVING CAST(n AS DOUBLE) IN (5, 1)"
    )._1 shouldBe Set("5", "5.0", "1", "1.0")
  }

  "a QUOTED number on a scripted numeric key" should "be sent as the number it is" in {
    // DuckDB 1.5.5 casts '5' to the key's type: GROUP BY ABS(x) HAVING ABS(x) = '5' keeps (5.0, n)
    lists(
      "SELECT ABS(x) AS k, COUNT(*) AS c FROM t GROUP BY ABS(x) HAVING ABS(x) = '5'"
    ) shouldBe ((Set("5", "5.0"), Set.empty[String]))
    lists(
      "SELECT ABS(x) AS k, COUNT(*) AS c FROM t GROUP BY ABS(x) HAVING ABS(x) IN ('2.5', '1')"
    ) shouldBe ((Set("2.5", "1", "1.0"), Set.empty[String]))
    // not a number: its own text
    lists(
      "SELECT ABS(x) AS k, COUNT(*) AS c FROM t GROUP BY ABS(x) HAVING ABS(x) <> 'x5'"
    ) shouldBe ((Set.empty[String], Set("x5")))
  }

  "a number on a FIELD level" should "keep its own text, which Elasticsearch parses numerically" in {
    lists("SELECT n, COUNT(*) AS c FROM t GROUP BY n HAVING n = 5") shouldBe
    ((Set("5"), Set.empty[String]))
    lists("SELECT x, COUNT(*) AS c FROM t GROUP BY x HAVING x IN (5, 1)") shouldBe
    ((Set("5", "1"), Set.empty[String]))
  }

  "a literal on a key that prints a string" should "keep its own text" in {
    lists(
      "SELECT CAST(n AS VARCHAR) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS VARCHAR) " +
      "HAVING CAST(n AS VARCHAR) = '5'"
    ) shouldBe ((Set("5"), Set.empty[String]))
    val ss = resolved(
      "SELECT CAST(n AS VARCHAR) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS VARCHAR)"
    )
    // even a NUMBER: the key prints the text a string comparison reads
    ss.buckets.head.keyFilterTexts(java.lang.Long.valueOf(5)) shouldBe Set("5")
    lists(
      "SELECT UPPER(s) AS k, COUNT(*) AS c FROM t GROUP BY UPPER(s) HAVING UPPER(s) = 'A'"
    ) shouldBe ((Set("A"), Set.empty[String]))
  }

  "every text of a number" should "be each text a Java number of that value prints, and no other" in {
    Bucket.numberTexts(java.lang.Long.valueOf(5), real = false) shouldBe Set("5", "5.0")
    Bucket.numberTexts(java.lang.Double.valueOf(5.0), real = false) shouldBe Set("5", "5.0")
    Bucket.numberTexts(java.lang.Long.valueOf(-3), real = false) shouldBe Set("-3", "-3.0")
    Bucket.numberTexts(java.lang.Long.valueOf(10000000), real = false) shouldBe
    Set("10000000", "1.0E7")
    // zero, either sign: a Double key prints 0.0 or -0.0
    Bucket.numberTexts(java.lang.Long.valueOf(0), real = false) shouldBe Set("0", "0.0", "-0.0")
    Bucket.numberTexts(java.lang.Double.valueOf(-0.0), real = false) shouldBe
    Set("0", "0.0", "-0.0")
    // a fraction: its Double text, which no integral key prints
    Bucket.numberTexts(java.lang.Double.valueOf(1.5), real = false) shouldBe Set("1.5")
    Bucket.numberTexts(BigDecimal("2.50"), real = false) shouldBe Set("2.5")
    Bucket.numberTexts(java.lang.Double.valueOf(Double.NaN), real = false) shouldBe Set("NaN")
    Bucket.numberTexts(java.lang.Double.valueOf(Double.PositiveInfinity), real = false) shouldBe
    Set("Infinity")
    // a REAL key also prints the Float text
    Bucket.numberTexts(java.lang.Double.valueOf(0.1), real = true) shouldBe Set("0.1")
    Bucket.numberTexts(java.lang.Double.valueOf(1.1), real = true) shouldBe Set("1.1")
    Bucket.numberTexts(java.lang.Double.valueOf(0.3), real = true) shouldBe
    Set("0.3", java.lang.Float.toString(0.3f))
  }
}
