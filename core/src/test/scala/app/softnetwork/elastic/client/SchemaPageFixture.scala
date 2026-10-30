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

import app.softnetwork.elastic.client.SetOperationValues.DateReading
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** SCHEMA-2 (AC2, `HitReadingSpec` "pages") -- one canned page of hits, read in its statement's
  * schema context: what every client's page extractor (scroll, `search_after`) answers, value AND
  * class, exactly as core's one-shot parse does. Network-free; shared by the client modules' page
  * specs (their tests depend on core's).
  */
private[client] object SchemaPageFixture {

  private val mapping: String =
    """{"s2_p": {"aliases": {}, "mappings": {
      | "_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"},
      |  "ts": {"type": "date"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "dm": {"type": "date", "ignore_malformed": true},
      |  "n": {"type": "integer"}, "l": {"type": "long"}, "b": {"type": "boolean"},
      |  "k": {"type": "keyword"}
      | }}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin

  val sql: String = "SELECT id, ts, c_des, dm, n, l, b, k FROM s2_p"

  val statement: SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(Index("s2_p", mapping).schema))
    case other                  => throw new IllegalStateException(s"[$sql] parsed as $other")
  }

  def fieldAliases: ListMap[String, String] = statement.fieldAliases

  /** The statement's schema context, as `SearchApi` builds it for `version`. */
  def context(version: String): ConversionContext = {
    val elements = ElementConversions.of(statement, Some(version))
    SchemaColumns(
      HitReading.of(statement, DateReading(Some(version)), HitReading.Default, elements.keySet),
      statement.doubleKeyLevels
    )(elements)
  }

  /** A scroll page in Elasticsearch's own shape: `_ignored` on the first hit, every value in a
    * spelling a typed reader converts (text numbers, a one-element array, a keyword number, an INT
    * outside its range kept as stored).
    */
  val page: String =
    """{"_scroll_id": "c1", "took": 1, "timed_out": false,
      |"_shards": {"total": 1, "successful": 1, "skipped": 0, "failed": 0},
      |"hits": {"total": {"value": 2, "relation": "eq"}, "max_score": 1.0, "hits": [
      | {"_index": "s2_p", "_id": "1", "_score": 1.0, "_ignored": ["dm"],
      |  "_source": {"id": "1", "ts": "2024-01-31T10:30:00", "c_des": 1706697000, "dm": "bad",
      |   "n": "5", "l": 7, "b": "true", "k": 10}},
      | {"_index": "s2_p", "_id": "2", "_score": 1.0,
      |  "_source": {"id": "2", "ts": "2024-02-01", "c_des": [1706745600], "dm": "2024-02-01",
      |   "n": 3000000000, "l": "8", "b": false, "k": "x"}}
      |]}}""".stripMargin

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  /** The rows the page answers, in D2's classes. */
  val expected: Seq[ListMap[String, Any]] = Seq(
    ListMap[String, Any](
      "id"    -> "1",
      "ts"    -> utc(2024, 1, 31, 10, 30),
      "c_des" -> LocalDate.of(2024, 1, 31),
      "dm"    -> null,
      "n"     -> java.lang.Integer.valueOf(5),
      "l"     -> java.lang.Long.valueOf(7L),
      "b"     -> java.lang.Boolean.TRUE,
      "k"     -> "10"
    ),
    ListMap[String, Any](
      "id"    -> "2",
      "ts"    -> utc(2024, 2, 1),
      "c_des" -> LocalDate.of(2024, 2, 1),
      "dm"    -> utc(2024, 2, 1),
      "n"     -> java.lang.Long.valueOf(3000000000L),
      "l"     -> java.lang.Long.valueOf(8L),
      "b"     -> java.lang.Boolean.FALSE,
      "k"     -> "x"
    )
  )

  /** Each row's column -> class name: value equality alone lets an `Integer` pass for a `Long`. */
  def classes(rows: Seq[ListMap[String, Any]]): Seq[Map[String, String]] =
    rows.map(_.map { case (k, v) => k -> (if (v == null) "null" else v.getClass.getName) })
}
