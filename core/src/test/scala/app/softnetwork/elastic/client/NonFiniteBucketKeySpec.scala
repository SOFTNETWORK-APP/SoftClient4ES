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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** A non-finite key of a level sent as a numeric `terms` of doubles is the Double it is. JSON has
  * no number for it, so a response writes it as the text Java prints: MEASURED on Elasticsearch
  * 6.8.23, `GROUP BY EXP(n)` answered the String `"Infinity"` beside Doubles, where DuckDB 1.5.5
  * answers the DOUBLE `inf`. The same statement is executed on every major by the shared testkit
  * trait `GroupedAggregateSemanticsSpec`.
  */
class NonFiniteBucketKeySpec extends AnyFlatSpec with Matchers {

  private val schema: Schema =
    Index(
      "t",
      """{"t": {"aliases": {}, "mappings": {"properties": {"n": {"type": "integer"},
        | "k": {"type": "keyword"}}}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The keys of the level `kk` the response with `keys` answers for `sql`, read in `context`. */
  private def keysOf(sql: String, keys: Seq[String])(
    context: SingleSearch => ConversionContext
  ): Seq[Any] = {
    val single = statement(sql)
    implicit val conversion: ConversionContext = context(single)
    val buckets = keys.map(k => s"""{"key": $k, "doc_count": 1}""").mkString(", ")
    val response =
      s"""{"hits": {"total": 6, "hits": []}, "aggregations": {"kk": {"buckets": [$buckets]}}}"""
    ElasticConversion
      .parseResponse(response, single.fieldAliases, ListMap.empty, Seq("kk"))
      .get
      .map(_("kk"))
  }

  private val ofStatement: SingleSearch => ConversionContext = SchemaColumns.of(_, NativeContext)

  "a double key level" should "answer its non-finite keys as Doubles" in {
    statement("SELECT EXP(n) AS kk FROM t GROUP BY EXP(n)").doubleKeyLevels shouldBe Set("kk")
    val keys = keysOf(
      "SELECT EXP(n) AS kk FROM t GROUP BY EXP(n)",
      Seq("1.0", "\"Infinity\"", "\"-Infinity\"", "\"NaN\"")
    )(ofStatement)
    keys.foreach(k => withClue(s"[$k] ")(k shouldBe a[java.lang.Double]))
    keys.take(3) shouldBe Seq(1.0, Double.PositiveInfinity, Double.NegativeInfinity)
    keys(3).asInstanceOf[java.lang.Double].isNaN shouldBe true
  }

  it should "keep any other text as it is" in {
    keysOf("SELECT EXP(n) AS kk FROM t GROUP BY EXP(n)", Seq("\"inf\""))(ofStatement) shouldBe
    Seq("inf")
  }

  "a key level of another type" should "answer the text Infinity as a String (the control)" in {
    statement("SELECT k AS kk FROM t GROUP BY k").doubleKeyLevels shouldBe empty
    keysOf("SELECT k AS kk FROM t GROUP BY k", Seq("\"Infinity\"", "\"a\""))(ofStatement) shouldBe
    Seq("Infinity", "a")
    // a whole-number level: its keys are numbers
    statement("SELECT n + 0 AS kk FROM t GROUP BY n + 0").doubleKeyLevels shouldBe empty
    // and the double level read without the statement's context keeps the text
    keysOf("SELECT EXP(n) AS kk FROM t GROUP BY EXP(n)", Seq("\"Infinity\""))(_ =>
      NativeContext
    ) shouldBe Seq("Infinity")
  }
}
