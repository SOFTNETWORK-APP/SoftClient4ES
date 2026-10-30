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
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** A table created through the gateway is typed by the NEXT statement, even when a statement before
  * it found the table missing.
  *
  * A schema lookup that answers 404 is remembered for up to five minutes, and the lookup is skipped
  * while the miss stands. Before the fix nothing but its expiry cleared it, so "SELECT, CREATE
  * TABLE, SELECT" ran the second SELECT with NO schema: its space-separated timestamp literal was
  * forwarded verbatim, and Elasticsearch's default date format refused it. The observable is that
  * statement's ANSWER: the rows from June 4th with the schema, a refusal without it. Both a
  * concrete name and a pattern the new table matches are covered, through the client the factory
  * builds (the metrics / monitoring wrappers over the real client).
  */
trait SchemaMissAfterCreateTableSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "schema_miss_table"
  private val patternMember = "schema_miss_pattern_1"
  private val pattern = "schema_miss_pattern_*"
  private val partitioned = "schema_miss_partitioned"

  /** Eight timestamps (UTC), ids `e1`..`e8`; `>= 2026-06-04 00:00:00` selects `e4`..`e8`. */
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

  private def statement(from: String): String =
    s"SELECT id FROM $from WHERE event_ts >= '2026-06-04 00:00:00'"

  override def afterAll(): Unit = {
    Seq(table, patternMember, partitioned).foreach(t =>
      Try(Await.result(client.run(s"DROP TABLE IF EXISTS $t"), 60.seconds))
    )
    super.afterAll()
  }

  private def run(sql: String): Unit =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    }

  /** The rows of a statement through `GatewayApi.run` (JDBC / REPL / Flight SQL), or its refusal.
    */
  private def answer(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Try {
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(QueryRows(rs, _, _))          => Right(rs)
        case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
        case ElasticSuccess(QueryStream(stream, _, _)) =>
          Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
        case ElasticSuccess(other) => Left(s"unexpected result $other")
        case ElasticFailure(error) => Left(error.message)
      }
    }.toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}").joinRight

  private def ids(sql: String): Either[String, Set[String]] =
    answer(sql).map(_.flatMap(_.get("id").map(String.valueOf)).toSet)

  /** The misses the client remembers -- held by the client that resolves the statements, i.e. the
    * delegate of a wrapper. Read only as a precondition: the row's verdict is the statement's
    * answer.
    */
  private def missesHeld: Int = client match {
    case wrapper: ElasticClientDelegator => wrapper.delegate.schemaMissCount
    case direct                          => direct.schemaMissCount
  }

  private def load(index: String): Unit = {
    val docs = timestamps.zipWithIndex.map { case (ts, i) =>
      s"""{"id":"e${i + 1}","event_ts":"$ts"}"""
    }.toList
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_)     => client.refresh(index)
      case ElasticFailure(error) => fail(s"Bulk indexing into $index failed: ${error.message}")
    }
  }

  "a table created through the gateway after a statement found it missing" should
  "be typed by the very next statement" in {
    val before = missesHeld
    ids(statement(table)) shouldBe a[Left[_, _]] // index_not_found
    missesHeld shouldBe before + 1 // the precondition: the 404 is remembered

    run(s"CREATE TABLE $table (id KEYWORD, event_ts TIMESTAMP, PRIMARY KEY (id))")
    load(table)

    ids(statement(table)) shouldBe Right(fromJune4)
  }

  "a table created through the gateway after a statement over a pattern found no index" should
  "type the pattern's very next statement" in {
    val before = missesHeld
    ids(statement(pattern)) // no index matches: no rows, whatever the venue answers
    missesHeld shouldBe before + 1

    run(s"CREATE TABLE $patternMember (id KEYWORD, event_ts TIMESTAMP, PRIMARY KEY (id))")
    load(patternMember)

    ids(statement(pattern)) shouldBe Right(fromJune4)
  }

  /** A partitioned table is a TEMPLATE until its first row: its name answers the schema lookup
    * through the template, while a search on the name itself still finds no index. So the
    * observable is the resolution: typed, a literal that cannot be a date is refused BEFORE it
    * reaches Elasticsearch, naming the literal and the column; untyped, it is forwarded and
    * Elasticsearch answers that there is no such index.
    */
  "a partitioned table created through the gateway after a statement found it missing" should
  "type the very next statement through its template" in {
    val notADate = s"SELECT id FROM $partitioned WHERE event_ts >= 'not-a-date'"
    val before = missesHeld
    ids(notADate) shouldBe a[Left[_, _]] // no index, no template
    missesHeld shouldBe before + 1

    run(
      s"CREATE TABLE $partitioned (id KEYWORD, event_ts TIMESTAMP, PRIMARY KEY (id)) " +
      "PARTITION BY event_ts (MONTH)"
    )

    ids(notADate) match {
      case Left(message) =>
        message should include("'not-a-date'")
        message should include("'event_ts'")
      case Right(rows) => fail(s"expected the typed refusal, got $rows")
    }
  }
}
