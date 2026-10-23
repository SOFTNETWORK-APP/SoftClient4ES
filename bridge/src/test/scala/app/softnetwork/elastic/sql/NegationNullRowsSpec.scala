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
import app.softnetwork.elastic.sql.query.SingleSearch
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** A negated comparison on a column keeps only the documents where the column has a value, as SQL
  * reads `NOT` over a NULL: `n <> 0` is NULL, not TRUE, where `n` is NULL, and DuckDB 1.5.5 leaves
  * such a row out. The `must_not` the comparison renders stands beside the column's `exists`.
  *
  * Before, a `must_not` alone matched every document the comparison did not -- the ones missing the
  * column included -- on every major and every field type.
  */
class NegationNullRowsSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private val mapper = new ObjectMapper()

  private def queryOf(sql: String): JsonNode = Parser(sql) match {
    case Right(s: SingleSearch) =>
      val request: ElasticSearchRequest = s
      mapper.readTree(request.query).get("query")
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** Every `bool` holding a `must_not`, with the fields its `filter` tests for existence. */
  private def negations(q: JsonNode): Seq[(JsonNode, Set[String])] = {
    def walk(n: JsonNode): Seq[(JsonNode, Set[String])] =
      if (n.isObject) {
        val here = Option(n.get("bool")).filter(_.has("must_not")).toSeq.map { b =>
          val exists = Option(b.get("filter")).toSeq
            .flatMap(_.elements().asScala)
            .flatMap(f => Option(f.get("exists")).map(_.get("field").asText))
            .toSet
          b.get("must_not") -> exists
        }
        here ++ n.elements().asScala.toSeq.flatMap(walk)
      } else if (n.isArray) n.elements().asScala.toSeq.flatMap(walk)
      else Nil
    walk(q)
  }

  "a negated comparison on a column" should "stand beside the column's exists, for every form" in {
    Seq(
      "n <> 0"                   -> "n",
      "n != 0"                   -> "n",
      "NOT n = 0"                -> "n",
      "k <> 'a'"                 -> "k",
      "x <> 2.5"                 -> "x",
      "b <> true"                -> "b",
      "d <> '2024-01-05'"        -> "d",
      "n NOT IN (0, 3)"          -> "n",
      "k NOT IN ('a', 'b')"      -> "k",
      "n NOT BETWEEN 1 AND 5"    -> "n",
      "k NOT LIKE 'a%'"          -> "k",
      "k NOT RLIKE 'a.*'"        -> "k",
      "g1 = 'b' AND NOT k = 'b'" -> "k",
      "d <> NOW()"               -> "d"
    ).foreach { case (condition, column) =>
      val q = queryOf(s"SELECT id FROM t WHERE $condition")
      withClue(s"[$condition] $q ") {
        negations(q).map(_._2) shouldBe Seq(Set(column))
      }
    }
  }

  it should "do so inside a nested query, on the element's column" in {
    val q = queryOf("SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.sku <> 's1'")
    withClue(q)(negations(q).map(_._2) shouldBe Seq(Set("items.sku")))
  }

  "a negated MATCH" should "stand beside the exists of every column it reads" in {
    val one = queryOf("SELECT id FROM t WHERE n = 50 AND NOT MATCH(k) AGAINST ('a')")
    withClue(one)(negations(one).map(_._2) shouldBe Seq(Set("k")))
    val two = queryOf("SELECT id FROM t WHERE n = 50 AND NOT MATCH(k, g1) AGAINST ('a')")
    withClue(two)(negations(two).map(_._2) shouldBe Seq(Set("k", "g1")))
  }

  "an IS NULL" should "stay the absence of the column" in {
    val q = queryOf("SELECT id FROM t WHERE n IS NULL")
    withClue(q)(negations(q).map(_._2) shouldBe Seq(Set.empty[String]))
  }

  "a negated range" should "stay a range, which a missing column never matches" in {
    val q = queryOf("SELECT id FROM t WHERE n > 1 AND NOT n >= 5")
    withClue(q)(negations(q) shouldBe empty)
  }
}
