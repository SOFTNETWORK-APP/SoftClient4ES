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

package app.softnetwork.elastic.sql.transform

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.mapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** `COUNT(*)` in a materialized view's transform must deploy on Elasticsearch 8+.
  *
  * It was a `value_count` on `_id`, and Elasticsearch 8 refuses field data on `_id` at deploy
  * ("Fielddata access on the _id field is disallowed") -- so every view with a `COUNT(*)` failed on
  * 8.x and 9.x. The count is now a `value_count` on `_index` (measured exact on 7.17.29 and 8.18.3;
  * see `CountAllDocumentsField`). The JSON asserted here is what `JavaClientApi` PUTs
  * (`TransformConfig.node`); the ES 7 client builds the same aggregation from the same
  * `CountTransformAggregation` field. Neither bridge emits a transform: this module does.
  */
class CountAllTransformSpec extends AnyFlatSpec with Matchers {

  private def search(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"expected a SingleSearch, got $other: [$sql]")
  }

  /** The aggregations a view's pivot computes, mapped the way the view generator maps them
    * (`Stage.buildAggregations`: `toTransformAggregation` per aggregate, keyed by its output name).
    */
  private def aggregations(sql: String): ListMap[String, TransformAggregation] =
    ListMap(
      search(sql).select.fields.flatMap { f =>
        f.aggregateFunction.flatMap(_.toTransformAggregation).map(f.outputName -> _)
      }: _*
    )

  /** The transform JSON as `JavaClientApi.executeCreateTransform` sends it. */
  private def transformJson(aggs: Map[String, TransformAggregation]): String = {
    val config = TransformConfig(
      id = "orders_by_city_mv_transform",
      viewName = "orders_by_city_mv",
      source = TransformSource(index = Seq("orders"), query = None),
      dest = TransformDest(index = "orders_by_city_mv"),
      pivot = Some(
        TransformPivot(
          groupBy = ListMap("city" -> TermsGroupBy("city"), "country" -> TermsGroupBy("country")),
          aggregations = aggs
        )
      ),
      delay = Delay.Default,
      frequency = Frequency.Default
    )
    mapper.writeValueAsString(config.node(_ => fail("this transform has no WHERE")))
  }

  private def aggregationJson(agg: TransformAggregation): String =
    mapper.writeValueAsString(agg.node)

  private val CountAll = """{"value_count":{"field":"_index"}}"""

  "COUNT(*) in a view's pivot" should "be a value_count on _index, never on _id" in {
    val aggs = aggregations(
      "SELECT city, country, COUNT(*) AS order_count FROM orders GROUP BY city, country"
    )
    aggs.keys.toSeq shouldBe Seq("order_count")
    val json = transformJson(aggs)
    json should include(
      """"pivot":{"group_by":{"city":{"terms":{"field":"city"}},"country":{"terms":{"field":"country"}}},""" +
      s""""aggregations":{"order_count":$CountAll}}"""
    )
    // The ES 8 refusal needs `_id` to be read; nothing in the transform may name it.
    json should not include "_id"
  }

  it should "render as COUNT(*) in the view's transform description" in {
    val agg = aggregations("SELECT city, COUNT(*) AS c FROM orders GROUP BY city")("c")
    agg shouldBe CountTransformAggregation(CountAllDocumentsField)
    agg.sql shouldBe "COUNT(*)"
  }

  "every COUNT spelling" should "map to the aggregation that counts what it names" in {
    val aggs = aggregations(
      """SELECT city,
        |  COUNT(*) AS star,
        |  COUNT(1) AS one,
        |  COUNT(DISTINCT *) AS distinct_star,
        |  COUNT(amount) AS amount_count,
        |  COUNT(DISTINCT amount) AS distinct_amount
        |FROM orders GROUP BY city""".stripMargin
    )
    aggs.map { case (name, agg) => name -> aggregationJson(agg) } shouldBe ListMap(
      "star" -> CountAll,
      // `COUNT(1)` IS `COUNT(*)` (the parser rewrites the operand to `*`).
      "one" -> CountAll,
      // The number of distinct documents IS the number of documents: the old `cardinality` of
      // `_id` estimated it (and needs field data on `_id`); the `cardinality` of `_index` would
      // count INDICES -- measured 1 per bucket.
      "distinct_star"   -> CountAll,
      "amount_count"    -> """{"value_count":{"field":"amount"}}""",
      "distinct_amount" -> """{"cardinality":{"field":"amount"}}"""
    )
  }
}
