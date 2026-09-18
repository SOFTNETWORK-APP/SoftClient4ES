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

/** Issue #368 -- `LAST_DAY` and `EPOCHDAY` over a `date` column EXECUTE and return the right value.
  *
  * Both emitted Painless that called a method the receiver does not have -- `lengthOfMonth()` on a
  * `ZonedDateTime`, and `.get` for a field whose range needs `.getLong` -- so each failed the whole
  * query, and neither had ever worked on any supported Elasticsearch despite being documented in
  * five places.
  *
  * 🔴 This spec EXECUTES. That is the point of it, and the reason the defect survived at least four
  * releases: every `LAST_DAY` test in the estate asserted the SCRIPT TEXT or the PARSE
  * (`StandaloneCallAssemblySpec`, `ParserSpec`, a `DialectCensus` row, two `SQLQuerySpec` bridge
  * fixtures) and a byte pin proves the bytes did not move, not that they RUN. The emission itself
  * is pinned in `sql`'s `DateDocValueEmissionSpec`; what cannot be asserted there is that
  * Elasticsearch accepts it.
  *
  * 🔴 Three dates, not one, and one of them is a LEAP February: a `LAST_DAY` that returned a
  * constant, or truncated instead of adjusting, passes a single-date fixture. 28 / 29 / 31 is the
  * assertion that it really computes the length of the month it was given.
  *
  * Both venues are exercised: the client API (`search`) and the gateway (`run`, the path the JDBC
  * driver, the REPL and the Flight SQL sidecar take).
  */
trait DateFunctionExecutionSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "date_function_execution"

  /** id -> (date, last day of its month, its epoch day, its year). February 2024 is a LEAP February
    * and February 2025 is not, so the expected last days differ by one for the same month.
    */
  private val fixture: Seq[(String, String, String, Long, Int)] = Seq(
    ("d1", "2025-02-10T00:00:00Z", "2025-02-28", 20129L, 2025),
    ("d2", "2024-02-10T00:00:00Z", "2024-02-29", 19763L, 2024),
    ("d3", "2025-12-31T00:00:00Z", "2025-12-31", 20453L, 2025)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "id": { "type": "keyword" },
        |    "d":  { "type": "date"    }
        |  }
        |}""".stripMargin

    client
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = fixture.map { case (id, d, _, _, _) => s"""{"id":"$id","d":"$d"}""" }.toList

    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 10)

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing into $index failed: ${error.message}")
    }

    client.refresh(index)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    super.afterAll()
  }

  /** Elasticsearch wraps every `script_fields` value in a per-field ARRAY, on every client and
    * every path, so a raw row value is a single-element list. Flattened here to the scalar the
    * assertions talk about; `toString` keeps the comparison free of per-major numeric boxing.
    */
  private def scalar(v: Any): String = v match {
    case s: Seq[_]                  => s.headOption.map(scalar).getOrElse("null")
    case a: java.util.Collection[_] => scalar(a.toArray.toSeq)
    case null                       => "null"
    case other                      => other.toString
  }

  private def rowsOf(sql: String): Seq[ListMap[String, Any]] =
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}\n$sql")
    }

  /** id -> the single projected column, through the client venue. */
  private def byId(sql: String, column: String): Map[String, String] =
    rowsOf(sql).map { r =>
      r.getOrElse("id", fail(s"no id column in $r")).toString -> scalar(
        r.getOrElse(column, fail(s"no $column column in $r"))
      )
    }.toMap

  /** id -> the single projected column, through `GatewayApi.run` (JDBC / REPL / sidecar venue). */
  private def gatewayById(sql: String, column: String): Map[String, String] = {
    val rows = Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _)) =>
        Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case ElasticSuccess(other) => fail(s"Unexpected result: $other\n$sql")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}\n$sql")
    }
    rows.map { r =>
      r.getOrElse("id", fail(s"no id column in $r")).toString -> scalar(
        r.getOrElse(column, fail(s"no $column column in $r"))
      )
    }.toMap
  }

  private val expectedLastDay: Map[String, String] =
    fixture.map { case (id, _, last, _, _) => id -> last }.toMap

  private val expectedDayOfLastDay: Map[String, String] =
    fixture.map { case (id, _, last, _, _) => id -> last.substring(8).toInt.toString }.toMap

  private val expectedEpochDay: Map[String, String] =
    fixture.map { case (id, _, _, epoch, _) => id -> epoch.toString }.toMap

  private val expectedYear: Map[String, String] =
    fixture.map { case (id, _, _, _, year) => id -> year.toString }.toMap

  "LAST_DAY over a date column" should "execute and return the last day of that month" in {
    val got = byId(s"SELECT id, LAST_DAY(d) AS ld FROM $index", "ld")
    got.keySet shouldBe expectedLastDay.keySet
    expectedLastDay.foreach { case (id, last) =>
      withClue(s"$id: ") { got(id) should startWith(last) }
    }
    log.info(s"LAST_DAY: $got")
  }

  it should "compute the length of the month rather than a constant (leap February included)" in {
    // 28 / 29 / 31. A `DAY(...)` wrapper also re-reads the adjusted value, so a `LAST_DAY` that
    // returned something Elasticsearch could render but not read back would redden here.
    byId(s"SELECT id, DAY(LAST_DAY(d)) AS dom FROM $index", "dom") shouldBe expectedDayOfLastDay
  }

  it should "execute through the gateway too" in {
    val got = gatewayById(s"SELECT id, DAY(LAST_DAY(d)) AS dom FROM $index", "dom")
    got shouldBe expectedDayOfLastDay
  }

  "EPOCHDAY over a date column" should "execute and return the day count since the epoch" in {
    byId(s"SELECT id, EPOCHDAY(d) AS ed FROM $index", "ed") shouldBe expectedEpochDay
  }

  it should "execute through the gateway too" in {
    gatewayById(s"SELECT id, EPOCHDAY(d) AS ed FROM $index", "ed") shouldBe expectedEpochDay
  }

  "YEAR over the same column" should "stay unchanged (the control)" in {
    // The control the issue names: `.get(ChronoField.YEAR)` was always fine, and the accessor
    // change must not have moved it.
    byId(s"SELECT id, YEAR(d) AS y FROM $index", "y") shouldBe expectedYear
  }
}
