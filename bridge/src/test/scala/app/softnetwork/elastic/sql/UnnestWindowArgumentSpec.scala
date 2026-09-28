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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** A window function's VALUE argument over an UNNEST column is refused exactly when the window's
  * aggregation is rendered WITHOUT a `nested` wrapper -- evaluated against the parent document,
  * where the element's columns are not visible.
  *
  * The ORACLE never asks the rule: it renders the window aggregation request exactly as core builds
  * it (`SearchApi.buildWindowAggregationRequest`) through this bridge and reads whether the metric
  * `w` sits under a `nested` aggregation. MEASURED on Elasticsearch 8.18.3 before the rule: `SUM(
  * ABS(i.n)) OVER (PARTITION BY id)` -- no wrapper -- answered 0.0 on every row, `SUM(i.n * 1) OVER
  * (..)` -- wrapped -- the right sums.
  */
class UnnestWindowArgumentSpec extends AnyFlatSpec with Matchers {

  import UnnestPopulation._

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private val Phrase = UnnestScope.StablePhrase

  private val u = from("i")

  private def windowSql(expr: String, alias: String): String =
    s"SELECT id, $alias.k AS k, SUM($expr) OVER (PARTITION BY id) AS w ${from(alias)}"

  /** `Some(message)` when THIS rule refused, `None` when every rule accepted, `Left` otherwise. */
  private def verdict(sql: String): Either[String, Option[String]] =
    Parser(sql) match {
      case Right(_)                            => Right(None)
      case Left(e) if e.msg.startsWith(Phrase) => Right(Some(e.msg))
      case Left(e)                             => Left(e.msg)
    }

  /** The window aggregation request, as core builds it before handing it to the bridge. */
  private def windowAggregation(ss: SingleSearch): SingleSearch =
    ss.copy(
      select = ss.select.copy(fields = ss.windowFields.map(_.update(ss))),
      groupBy = None,
      orderBy = None,
      limit = None
    ).update()

  /** The ORACLE: is the rendered metric `w` under a `nested` aggregation? `None`: no `w` rendered.
    */
  private def wrapped(sql: String): Option[Boolean] = {
    val ss = Parser.parseUnvalidated(sql) match {
      case Right(ss: SingleSearch) => ss
      case other                   => fail(s"did not parse as a SingleSearch: $other\n$sql")
    }
    val request: ElasticSearchRequest = windowAggregation(ss)
    def find(node: JsonNode, nested: Boolean): Option[Boolean] =
      Option(node.get("aggs"))
        .orElse(Option(node.get("aggregations")))
        .toSeq
        .flatMap(_.fields().asScala.toSeq)
        .iterator
        .map { entry =>
          val here = nested || entry.getValue.has("nested")
          if (entry.getKey == "w") Some(here) else find(entry.getValue, here)
        }
        .collectFirst { case Some(found) => found }
    find(mapper.readTree(request.query), nested = false)
  }

  /** Every single-column cell of the derived population -- string, math, conditional (`COALESCE`,
    * `CASE`), conversion, temporal and geo functions, the arithmetic operators -- and the bare
    * column. A mixed UNNEST / parent pair reads two documents: not this oracle's question.
    */
  private lazy val cells: Seq[Cell] =
    nestedCols.map(col => Cell("bare", col, s"bare:$col", (o, _) => o(col))) ++
    exercised.filterNot(c =>
      c.family == "aggregate" || c.shape.startsWith("pair:") || c.shape.startsWith("op-pair:")
    )

  "a window function over an UNNEST column" should "be refused exactly when its aggregation is rendered without a nested wrapper" in {
    var refused, accepted, notExercised = 0
    val acceptedByFamily = scala.collection.mutable.Map.empty[String, (Int, Int)]
    for {
      c     <- cells
      alias <- aliases
    } {
      val sql = windowSql(c.nested(alias), alias)
      verdict(sql) match {
        case Left(_) => notExercised += 1 // another rule refused it: not this rule's to judge
        case Right(v) =>
          val isWrapped = wrapped(sql).getOrElse(fail(s"no window aggregation rendered: $sql"))
          withClue(s"[$sql] wrapped=$isWrapped ") {
            v.isDefined shouldBe !isWrapped
            v.foreach { msg =>
              msg should startWith(s"${Phrase}SELECT: ")
              msg should include(" OVER (PARTITION BY id) is evaluated per parent document, where ")
            }
          }
          if (isWrapped) accepted += 1 else refused += 1
          val (a, n) = acceptedByFamily.getOrElse(c.family, (0, 0))
          acceptedByFamily(c.family) = (a + (if (isWrapped) 1 else 0), n + 1)
      }
    }
    info(
      s"windows over UNNEST cells: refused $refused, accepted $accepted, other rule $notExercised"
    )
    info(s"accepted / judged by family: ${acceptedByFamily.toSeq.sortBy(_._1).mkString(", ")}")
    // Floors = the counts MEASURED at RENDER on this tree: they guard the population against
    // shrinking silently, and BOTH sides must be populated or the row proves nothing.
    refused should be >= 2920
    accepted should be >= 1160
    // the bare column and every arithmetic operator are wrapped: all accepted
    Seq("bare", "arithmetic").foreach { family =>
      val (a, n) = acceptedByFamily.getOrElse(family, (0, 0))
      withClue(s"[$family] ")(a shouldBe n)
      n should be > 0
    }
  }

  it should "keep accepting a bare column, a CAST, a date part and arithmetic -- all wrapped" in {
    Seq(
      s"SELECT id, i.k AS k, SUM(i.n) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(CAST(i.n AS DOUBLE)) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(YEAR(i.d)) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(i.n * 1) OVER (PARTITION BY t.id) AS w $u",
      s"SELECT id, i.k AS k, SUM(i.n * i.x) OVER (PARTITION BY id) AS w $u"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql) shouldBe Right(None)
        wrapped(sql) shouldBe Some(true)
      }
    }
  }

  it should "refuse a string, math, COALESCE or CASE function -- none wrapped" in {
    Seq(
      s"SELECT id, i.k AS k, SUM(LENGTH(i.s)) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(ABS(i.n)) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(COALESCE(i.n, 0)) OVER (PARTITION BY id) AS w $u",
      s"SELECT id, i.k AS k, SUM(CASE WHEN i.n > 0 THEN i.n ELSE 0 END) OVER (PARTITION BY id) AS w $u"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        verdict(sql).toOption.flatten.getOrElse(fail("not refused by name")) should startWith(
          s"${Phrase}SELECT: "
        )
        wrapped(sql) shouldBe Some(false)
      }
    }
  }
}
