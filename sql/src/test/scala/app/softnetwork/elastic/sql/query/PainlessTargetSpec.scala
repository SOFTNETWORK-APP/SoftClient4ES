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
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, PainlessTarget}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A comparison between a `date` doc value and another date renders for the Elasticsearch major the
  * client module gives the bridge (`PainlessTarget`).
  *
  * Elasticsearch 6.8 hands a query script a `date` doc value as a `JodaCompatibleZonedDateTime`,
  * whose `isBefore` / `isAfter` / `isEqual` against a genuine `ZonedDateTime` fail with
  * `Class.cast`; so on 6.8 each doc-reading operand of such a comparison is normalised to UTC
  * first. From 7 on the doc value IS a `ZonedDateTime`: the scripts of those majors -- and of a
  * rendering no client module gave a major to -- stay byte-for-byte what they were, which the byte
  * pins of `ParameterIdentitySpec`, `PredicateTransformSurvivalSpec` and `PainlessResidualsSpec`
  * hold for the default target.
  */
class PainlessTargetSpec extends AnyFlatSpec with Matchers {

  private val schema = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int)
    )
  )

  private val normalisation = ".withZoneSameInstant(ZoneId.of('Z'))"

  private def occurrences(s: String): Int =
    s.sliding(normalisation.length).count(_ == normalisation)

  private def predicateOf(
    sql: String,
    target: PainlessTarget,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(contextType, target)
      val body = ss
        .update(Some(schema))
        .where
        .flatMap(_.criteria)
        .map(_.painless(Some(ctx)))
        .getOrElse(fail(s"[$sql] no WHERE criteria"))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** Item 1's comparison shapes: a doc value against a function, the clock, a literal or date
    * arithmetic, in both directions -- each with how many of its operands read a document.
    */
  private val shapes: Seq[(String, Int)] = Seq(
    "SELECT n FROM t WHERE CAST(d AS TIMESTAMP) >= ts"                   -> 2,
    "SELECT n FROM t WHERE LAST_DAY(d) = ts"                             -> 2,
    "SELECT n FROM t WHERE CAST(ts AS DATE) = d"                         -> 2,
    "SELECT n FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) > ts"             -> 2,
    "SELECT n FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) = d2"             -> 2,
    "SELECT n FROM t WHERE CURRENT_DATE > d"                             -> 1,
    "SELECT n FROM t WHERE COALESCE(d, d2) > CAST('2024-01-01' AS DATE)" -> 1,
    "SELECT n FROM t WHERE d2 = d + 1"                                   -> 2,
    "SELECT n FROM t WHERE d > d2 - 1"                                   -> 2,
    "SELECT n FROM t WHERE d >= CAST(ts AS DATE) - 1"                    -> 2
  )

  "a comparison of a date doc value" should
  "render as it always did for Elasticsearch 7, 8 and 9 and for no major at all" in {
    shapes.foreach { case (sql, _) =>
      withClue(s"[$sql] ") {
        val default = predicateOf(sql, PainlessTarget.Default)
        Seq(7, 8, 9).foreach(major => predicateOf(sql, PainlessTarget(major)) shouldBe default)
      }
    }
  }

  it should "normalise every operand that reads a document to UTC on Elasticsearch 6.8, and nothing else" in {
    shapes.foreach { case (sql, documentOperands) =>
      withClue(s"[$sql] ") {
        val es6 = predicateOf(sql, PainlessTarget(6))
        val default = predicateOf(sql, PainlessTarget.Default)
        // one normalisation per operand that reads a document (date arithmetic, a cast, may
        // already carry the same UTC conversion for reasons of their own: the DIFFERENCE is what
        // the target adds) -- and nothing else differs from what the other majors render
        occurrences(es6) - occurrences(default) shouldBe documentOperands
        es6.replace(normalisation, "") shouldBe default.replace(normalisation, "")
      }
    }
  }

  it should "not normalise outside a query: an ingest processor parses its own operands" in {
    shapes.foreach { case (sql, _) =>
      withClue(s"[$sql] ") {
        predicateOf(sql, PainlessTarget(6), PainlessContextType.Processor) shouldBe
        predicateOf(sql, PainlessTarget.Default, PainlessContextType.Processor)
      }
    }
  }

  "PainlessTarget" should "tell the major that hands Joda-compatible dates" in {
    PainlessTarget(6).jodaCompatibleDates shouldBe true
    Seq(7, 8, 9).foreach(major => PainlessTarget(major).jodaCompatibleDates shouldBe false)
    PainlessTarget.Default.jodaCompatibleDates shouldBe false
    PainlessContext().target shouldBe PainlessTarget.Default
  }
}
