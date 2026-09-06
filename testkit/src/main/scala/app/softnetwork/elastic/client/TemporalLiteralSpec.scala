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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

/** Regression test for issue #276: a WHERE comparison against a `date`-mapped field must match the
  * same rows whichever standard literal spelling the BI tool emits.
  *
  * Before the fix the SQL-standard space-separated spelling (`'2026-06-04 00:00:00.000000'`, what
  * Superset / SQLAlchemy render) was forwarded verbatim and rejected by Elasticsearch's default
  * `strict_date_optional_time||epoch_millis` format, while the `T`-separated form matched. Eight
  * rows, ids asserted (not only counts). A second index carries a custom `format` that parses the
  * space form TODAY: it must keep working (the fix must not invert the defect). A `keyword` column
  * holding the same date-shaped string is the negative control: it must be compared verbatim.
  *
  * Both venues are exercised: the client API (`search`) and the gateway (`run`, the path the JDBC
  * driver, the REPL and the Flight SQL sidecar take).
  */
trait TemporalLiteralSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val defaultIndex = "temporal_literal_default"

  private val customIndex = "temporal_literal_custom"

  /** Eight timestamps (UTC), ids `e1`..`e8`; `>= 2026-06-04T00:00:00` selects `e4`..`e8`. */
  private val timestamps: Seq[String] = Seq(
    "2026-06-01T00:00:00",
    "2026-06-02T12:00:00",
    "2026-06-03T23:59:59",
    "2026-06-04T00:00:00",
    "2026-06-04T00:00:01",
    "2026-06-05T10:00:00",
    "2026-06-10T00:00:00",
    "2026-07-01T00:00:00"
  )

  private val fromJune4: Set[String] = Set("e4", "e5", "e6", "e7", "e8")

  override def beforeAll(): Unit = {
    super.beforeAll()

    val settings = """{"number_of_shards": 1, "number_of_replicas": 0}"""

    val defaultMapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "event_ts": { "type": "date" },
        |    "label":    { "type": "keyword" },
        |    "amount":   { "type": "integer" }
        |  }
        |}""".stripMargin

    val customMapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "event_ts": { "type": "date", "format": "yyyy-MM-dd HH:mm:ss" },
        |    "amount":   { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(defaultIndex, settings = settings).get shouldBe true
    client.setMapping(defaultIndex, defaultMapping).get shouldBe true
    client.createIndex(customIndex, settings = settings).get shouldBe true
    client.setMapping(customIndex, customMapping).get shouldBe true

    val defaultDocs = timestamps.zipWithIndex.map { case (ts, i) =>
      // `label` holds the SQL spelling of the same instant: the keyword negative control
      s"""{"id":"e${i + 1}","event_ts":"$ts","label":"${ts.replace('T', ' ')}","amount":${i + 1}}"""
    }.toList

    val customDocs = timestamps.zipWithIndex.map { case (ts, i) =>
      s"""{"id":"e${i + 1}","event_ts":"${ts.replace('T', ' ')}","amount":${i + 1}}"""
    }.toList

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    def load(index: String, docs: List[String]): Unit = {
      implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 1000)
      client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
        case ElasticSuccess(_) => // ok
        case ElasticFailure(error) =>
          error.cause.foreach(_.printStackTrace())
          fail(s"Bulk indexing into $index failed: ${error.message}")
      }
      client.refresh(index)
    }

    load(defaultIndex, defaultDocs)
    load(customIndex, customDocs)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(defaultIndex)
    client.deleteIndex(customIndex)
    super.afterAll()
  }

  private def ids(rows: Seq[ListMap[String, Any]]): Set[String] =
    rows.flatMap(_.get("id").map(_.toString)).toSet

  /** Client venue: `SearchApi.search`. */
  private def searchIds(sql: String): Set[String] =
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => ids(response.results)
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}\n$sql")
    }

  /** Gateway venue: `GatewayApi.run` -- what the JDBC driver, the REPL and the sidecar call. */
  private def gatewayIds(sql: String): Set[String] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => ids(rows)
      case ElasticSuccess(QueryStructured(response, _)) => ids(response.results)
      case ElasticSuccess(QueryStream(stream, _)) =>
        ids(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
      case ElasticSuccess(other) => fail(s"Unexpected result: $other")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}\n$sql")
    }

  // ---- AC 1: the three spellings select the same rows -------------------------------------------

  "a range comparison against a default-format date field" should "match the same rows for every standard spelling" in {
    Seq("2026-06-04 00:00:00.000000", "2026-06-04 00:00:00", "2026-06-04T00:00:00").foreach {
      literal =>
        withClue(literal) {
          searchIds(s"SELECT id FROM $defaultIndex WHERE event_ts >= '$literal'") shouldBe fromJune4
        }
    }
    log.info(s"OK: the three spellings each select ${fromJune4.size} rows on $defaultIndex")
  }

  it should "match the same rows through the gateway (JDBC / REPL / sidecar venue)" in {
    Seq("2026-06-04 00:00:00.000000", "2026-06-04T00:00:00").foreach { literal =>
      withClue(literal) {
        gatewayIds(s"SELECT id FROM $defaultIndex WHERE event_ts >= '$literal'") shouldBe fromJune4
      }
    }
  }

  "equality, BETWEEN and IN against a default-format date field" should "accept the space form" in {
    searchIds(s"SELECT id FROM $defaultIndex WHERE event_ts = '2026-06-04 00:00:00'") shouldBe
    Set("e4")
    searchIds(
      s"SELECT id FROM $defaultIndex WHERE event_ts BETWEEN '2026-06-02 00:00:00' AND '2026-06-04 00:00:00'"
    ) shouldBe Set("e2", "e3", "e4")
    searchIds(
      s"SELECT id FROM $defaultIndex WHERE event_ts IN ('2026-06-04 00:00:00', '2026-07-01 00:00:00')"
    ) shouldBe Set("e4", "e8")
    searchIds(s"SELECT id FROM $defaultIndex WHERE event_ts < '2026-06-04 00:00:00'") shouldBe
    Set("e1", "e2", "e3")
  }

  // ---- AC 4: epoch millis untouched ----------------------------------------------------------------

  "an epoch-millis literal against a default-format date field" should "still be accepted" in {
    // 1780531200000 == 2026-06-04T00:00:00Z
    searchIds(s"SELECT id FROM $defaultIndex WHERE event_ts >= '1780531200000'") shouldBe fromJune4
  }

  // ---- AC 3: keyword negative control ------------------------------------------------------------

  "a keyword column holding a date-shaped string" should "be compared verbatim" in {
    // `label` stores the space spelling; a rewrite to the T form would match nothing.
    searchIds(s"SELECT id FROM $defaultIndex WHERE label = '2026-06-04 00:00:00'") shouldBe
    Set("e4")
    searchIds(s"SELECT id FROM $defaultIndex WHERE label = '2026-06-04T00:00:00'") shouldBe
    Set.empty[String]
  }

  // ---- AC 2: custom-format field keeps working ----------------------------------------------------

  "a custom-format date field that accepts the space form" should "keep matching the same rows" in {
    searchIds(s"SELECT id FROM $customIndex WHERE event_ts >= '2026-06-04 00:00:00'") shouldBe
    fromJune4
    searchIds(
      s"SELECT id FROM $customIndex WHERE event_ts BETWEEN '2026-06-02 00:00:00' AND '2026-06-04 00:00:00'"
    ) shouldBe Set("e2", "e3", "e4")
    gatewayIds(s"SELECT id FROM $customIndex WHERE event_ts >= '2026-06-04 00:00:00'") shouldBe
    fromJune4
  }

  // ---- AC 6: unparseable literal is a named 400, not a raw shard failure -------------------------

  "an unparseable literal against a default-format date field" should "fail naming the literal and the field" in {
    client.search(
      SelectStatement(s"SELECT id FROM $defaultIndex WHERE event_ts >= 'not-a-date'")
    ) match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.message should include("'not-a-date'")
        error.message should include("'event_ts'")
        error.message should not include "search_phase_execution_exception"
      case ElasticSuccess(response) =>
        fail(s"expected a rejection, got ${response.results.size} rows")
    }
  }
}
