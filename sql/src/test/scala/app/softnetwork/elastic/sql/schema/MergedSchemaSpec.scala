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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.StringValue
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** The schema an index PATTERN or a comma list answers with (`mergedSchema`): the union of the
  * matched indices' columns, merged FIELD BY FIELD -- a field the indices map alike keeps its type,
  * a field they map differently is left untyped (`Any`), so nothing is refused on it and
  * Elasticsearch answers for it, while every other field is typed as over a single index.
  */
class MergedSchemaSpec extends AnyFlatSpec with Matchers {

  private def index(name: String, properties: String): (String, Table) =
    name -> Index(name, s"""{"mappings":{"properties":{$properties}},"settings":{}}""").schema

  "indices that agree" should "answer the union of their columns" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"g":{"type":"keyword"}"""),
        index("logs-2", """"ts":{"type":"date"},"n":{"type":"long"}""")
      )
    )
    disagreements shouldBe empty
    merged.name shouldBe "logs-*"
    merged.columns.map(c => c.name -> c.dataType) should contain theSameElementsAs Seq(
      "g" -> SQLTypes.Keyword,
      "n" -> SQLTypes.BigInt,
      // a `date` nobody declared is a TIMESTAMP at the declaration seam, and stays one merged
      "ts" -> SQLTypes.Timestamp
    )
  }

  it should "agree on a TIMESTAMP one index declares and another holds undeclared" in {
    val declared = Index(
      "logs-1",
      """{"mappings":{"_meta":{"columns":{"ts":{"data_type":"TIMESTAMP","not_null":"false"}}},""" +
      """"properties":{"ts":{"type":"date"}}},"settings":{}}"""
    ).schema
    val (merged, disagreements) =
      mergedSchema("logs-*", Seq("logs-1" -> declared, index("logs-2", """"ts":{"type":"date"}""")))
    disagreements shouldBe empty
    merged.find("ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
  }

  "indices that disagree on a field" should "leave that field alone untyped, and type every other" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"c":{"type":"date"},"g":{"type":"keyword"}"""),
        index("logs-2", """"ts":{"type":"date"},"c":{"type":"keyword"},"n":{"type":"long"}""")
      )
    )
    disagreements shouldBe Seq("'c' (KEYWORD, TIMESTAMP)")
    merged.columns.map(c => c.name -> c.dataType) should contain theSameElementsAs Seq(
      "c"  -> SQLTypes.Any,
      "g"  -> SQLTypes.Keyword,
      "n"  -> SQLTypes.BigInt,
      "ts" -> SQLTypes.Timestamp
    )
  }

  it should "count a date's format as part of its mapping: a temporal literal is read against it" in {
    val withFormat = Table(
      "logs-2",
      columns = List(
        Column(
          "ts",
          SQLTypes.Timestamp,
          options = ListMap("format" -> StringValue("epoch_millis"))
        ),
        Column("g", SQLTypes.Keyword)
      )
    )
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index("logs-1", """"ts":{"type":"date"},"g":{"type":"keyword"}"""),
        "logs-2" -> withFormat
      )
    )
    disagreements shouldBe Seq("'ts' (TIMESTAMP)")
    merged.find("ts").map(_.dataType) shouldBe Some(SQLTypes.Any)
    merged.find("ts").flatMap(_.options.get("format")) shouldBe None
    merged.find("g").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  it should "merge sub-fields by the same rule, under their parent" in {
    val (merged, disagreements) = mergedSchema(
      "logs-*",
      Seq(
        index(
          "logs-1",
          """"s":{"type":"text","fields":{"raw":{"type":"keyword"},"n":{"type":"long"}}}"""
        ),
        index(
          "logs-2",
          """"s":{"type":"text","fields":{"raw":{"type":"keyword"},"n":{"type":"keyword"},"w":{"type":"keyword"}}}"""
        )
      )
    )
    disagreements shouldBe Seq("'s.n' (BIGINT, KEYWORD)")
    merged.find("s").map(_.dataType) shouldBe Some(SQLTypes.Text)
    merged.find("s.raw").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    merged.find("s.w").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
    merged.find("s.n").map(_.dataType) shouldBe Some(SQLTypes.Any)
  }

  /** The pattern of `DateArithmeticSpec`: two indices that agree on every field but `c`. */
  private lazy val pattern: Schema =
    mergedSchema(
      "ev_*",
      Seq(
        index(
          "ev_a",
          """"g":{"type":"keyword"},"d":{"type":"date"},"d2":{"type":"date"},"ts":{"type":"date"},"n":{"type":"integer"},"c":{"type":"date"}"""
        ),
        index("ev_b", """"g":{"type":"keyword"},"d":{"type":"date"},"c":{"type":"keyword"}""")
      )
    )._1

  private def resolved(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(pattern))
      case other                   => fail(s"[$sql] $other")
    }

  "a statement over a pattern with one conflicting field" should
  "type every field the indices agree on, and refuse nothing on the conflicting one" in {
    // the fields they agree on: the type rules apply as over a single index (`d` is a `date`
    // elasticsql never declared -- a TIMESTAMP -- so a difference of two is fractional days)
    resolved("SELECT g, MAX(d) - MIN(d) AS x FROM ev_* GROUP BY g").select.fields
      .find(_.fieldAlias.exists(_.alias == "x"))
      .map(_.identifier.reportedType) shouldBe Some(SQLTypes.Double)
    resolved("SELECT d * 2 AS x FROM ev_*").validateResolved().swap.getOrElse("") should include(
      "operator * cannot be applied to TIMESTAMP and BIGINT"
    )
    resolved("SELECT n + g AS x FROM ev_*").validateResolved().isLeft shouldBe true
    // the conflicting field: no type, so no type rule refuses it -- Elasticsearch answers
    Seq(
      "SELECT c * 2 AS x FROM ev_*",
      "SELECT c + d AS x FROM ev_*",
      "SELECT n FROM ev_* WHERE c = 1",
      "SELECT n FROM ev_* WHERE c + 1 > CURRENT_DATE",
      "SELECT g, MAX(c) - MIN(c) AS x FROM ev_* GROUP BY g"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        resolved(sql).validateResolved() shouldBe Right(())
      }
    }
    resolved(
      "SELECT c + 1 AS x FROM ev_*"
    ).select.fields.head.identifier.reportedType.isUnknown shouldBe
    true
  }
}
