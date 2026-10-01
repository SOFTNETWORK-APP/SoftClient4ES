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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MetricSelectorScript, SingleSearch}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** F1 -- the null-aware `HAVING` selector and the rule the response parser answers NULL with.
  *
  * `ClientAggregation.nullOverEmptyInput` decides which aggregates SELECT returns as NULL over a
  * group with no value; `MetricSelectorScript.nullAwareSelectorScript` decides which ones the
  * `HAVING` selector reads as NULL when Elasticsearch hands it `NaN`. They are two answers to one
  * question and live in two modules, so this spec -- the one place that sees both -- asserts they
  * agree over EVERY aggregation type.
  */
class HavingNullAwareReadSpec extends AnyFlatSpec with Matchers {

  private def parsed(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** How a SELECT publishes an aggregate of each type over `v` as `a`, and how a HAVING names it.
    * `None`: a multi-valued type, which no comparison reads.
    */
  private def spelling(t: AggregationType.AggregationType): Option[(String, String)] = {
    def plain(fn: String) = Some(s"$fn(v) AS a" -> s"$fn(v)")
    t match {
      case AggregationType.Count      => plain("COUNT")
      case AggregationType.Min        => plain("MIN")
      case AggregationType.Max        => plain("MAX")
      case AggregationType.Avg        => plain("AVG")
      case AggregationType.Sum        => plain("SUM")
      case AggregationType.Stddev     => plain("STDDEV")
      case AggregationType.StddevSamp => plain("STDDEV_SAMP")
      case AggregationType.StddevPop  => plain("STDDEV_POP")
      case AggregationType.Variance   => plain("VARIANCE")
      case AggregationType.VarSamp    => plain("VAR_SAMP")
      case AggregationType.VarPop     => plain("VAR_POP")
      case AggregationType.PercentileCont =>
        val p = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v)"
        Some(s"$p AS a" -> p)
      case AggregationType.PercentileDisc =>
        val p = "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v)"
        Some(s"$p AS a" -> p)
      case AggregationType.FirstValue =>
        val f = "FIRST_VALUE(v) OVER (PARTITION BY g ORDER BY v)"
        Some(s"$f AS a" -> f)
      case AggregationType.LastValue =>
        val f = "LAST_VALUE(v) OVER (PARTITION BY g ORDER BY v)"
        Some(s"$f AS a" -> f)
      case AggregationType.BucketScript => Some("MAX(v) - MIN(v) AS a" -> "a")
      case AggregationType.ArrayAgg | AggregationType.RowNumber | AggregationType.Rank |
          AggregationType.DenseRank =>
        None
      case other => fail(s"$other has no spelling here: classify it")
    }
  }

  private def nullOverEmptyInput(t: AggregationType.AggregationType): Boolean =
    ClientAggregation(
      aggName = "a",
      aggType = t,
      distinct = false,
      sourceField = "v",
      windowing = false,
      bucketPath = "",
      bucketRoot = ""
    ).nullOverEmptyInput

  "the null-aware HAVING selector" should
  "read as NULL exactly the aggregates SELECT answers NULL over no value" in {
    val checked = AggregationType.values.toSeq.flatMap(t => spelling(t).map(t -> _))
    checked.map(_._1).filter(nullOverEmptyInput) should not be empty
    checked.map(_._1).filterNot(nullOverEmptyInput) should not be empty
    checked.foreach { case (t, (selectItem, operand)) =>
      val sql = s"SELECT g, $selectItem FROM t GROUP BY g HAVING $operand > 1"
      withClue(s"[$t] [$sql] ") {
        val search = parsed(sql)
        // the spelling IS the type: the published column converts to it
        implicitly[ClientAggregation](search.sqlAggregations("a")).aggType shouldBe t
        val criteria = search.having.flatMap(_.criteria).get
        val plain = MetricSelectorScript.selectorScript(criteria)
        val nullAware = MetricSelectorScript.nullAwareSelectorScript(criteria)
        plain.map(_.contains("params.a")) shouldBe Some(true)
        val readAsNull = nullAware != plain
        readAsNull shouldBe nullOverEmptyInput(t)
        if (readAsNull) nullAware.get should include("Double.isNaN(params.a)")
      }
    }
  }
}
