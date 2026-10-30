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

package app.softnetwork.elastic.arrow

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.{ElasticClientApi, ResultSchema, SetOperationValues}
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.scroll.ScrollMetrics
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.`type`.ValueCoercion
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-2 (D15, AC7) -- the downstream consumers' forms, run on core's canonical rows, in a
  * DOWNSTREAM package on purpose (SCHEMA-12 D11's precedent): arrow's `extractRows` (positional, 3
  * binders) and its leg readers (`DeclaredLegSchema.readers` = core's `LegSlot.read`, a no-op on a
  * canonical row of every date format), the JDBC driver's `rows` match and `getString`, the
  * extensions' `count(*)` fold, and the shape immune to the next arity change (`case q:
  * QueryRows`).
  */
class JavaClientSchemaConsumerSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "s2c_t"

  private val mapping: String =
    """{"_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "ts": {"type": "date"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "c_c1": {"type": "date", "format": "yyyy/MM/dd"},
      |  "n": {"type": "integer"}, "bin": {"type": "binary"}, "k": {"type": "keyword"}
      |}}""".stripMargin

  private val docs: Seq[(String, String)] = Seq(
    "c1" -> """{"id":"c1","ts":"2024-01-31T10:30:00","c_des":1706697000,"c_c1":"2024/01/31","n":1,"bin":"aGVsbG8=","k":"a"}""",
    "c2" -> """{"id":"c2","ts":"2024-02-01T00:00:00Z","c_des":1706745600,"c_c1":"2024/02/01","n":2,"k":"b"}"""
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(client.deleteIndex(table))
    client
      .createIndex(table, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(table, mapping).get shouldBe true
    docs.foreach { case (id, source) => client.index(table, id, source) }
    client.refresh(table)
    ()
  }

  override def afterAll(): Unit = {
    Try(client.deleteIndex(table))
    super.afterAll()
  }

  private def run(sql: String): QueryResult =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error)  => fail(s"[$sql] ${error.message}")
    }

  /** Arrow's `ElasticSqlExecutor.extractRows`, positional patterns with three binders (D15). */
  private def extractRows(result: QueryResult): Seq[ListMap[String, Any]] = result match {
    case QueryRows(rows, _, _)        => rows
    case QueryStructured(response, _) => response.results
    case QueryStream(stream, _, _) =>
      Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
    case _ => Seq.empty
  }

  /** The consumer shape immune to the next arity change, with the schema. */
  private def rowsAndSchema(
    result: QueryResult
  ): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    result match {
      case q: QueryRows       => (q.rows, q.schema)
      case q: QueryStructured => (q.response.results, q.schema)
      case q: QueryStream =>
        (Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds), q.schema)
      case other => fail(s"$other")
    }

  "Arrow" should "read core's rows through both match shapes, by the schema's names" in {
    Seq("", " LIMIT 10").foreach { limit =>
      val sql = s"SELECT id, ts, c_des, n FROM $table$limit"
      val result = run(sql)
      val (rows, schema) = rowsAndSchema(result)
      extractRows(result).map(_.toMap) shouldBe rows.map(_.toMap)
      val names = schema.getOrElse(fail("no schema")).names
      names shouldBe Seq("id", "ts", "c_des", "n")
      rows.foreach(row => names.foreach(n => row.contains(n) shouldBe true))
    }
  }

  it should "read a canonical leg row through its leg readers unchanged, every date format (D12)" in {
    val sqls = Seq(
      s"SELECT ts AS d, n FROM $table UNION SELECT ts AS d, n FROM $table",
      s"SELECT c_des AS d FROM $table UNION SELECT c_des AS d FROM $table",
      s"SELECT c_c1 AS d FROM $table UNION SELECT c_c1 AS d FROM $table",
      s"SELECT c_des AS d FROM $table UNION SELECT ts AS d FROM $table",
      s"SELECT bin AS b, k FROM $table UNION SELECT bin AS b, k FROM $table"
    )
    sqls.foreach { sql =>
      withClue(s"[$sql] ") {
        val multiple = Parser(sql) match {
          case Right(m: MultiSearch) =>
            m.copy(requests = m.requests.map(r => r.update(Some(client.loadSchema(table).get))))
          case other => fail(s"$other")
        }
        val legs = SetOperationValues
          .legSchemas(multiple.requests, multiple.resolvedOperators, Some("8.18.3"))
          .fold(refusal => fail(refusal), identity)
        multiple.requests.zip(legs).foreach { case (leg, schema) =>
          // each leg as core reads it on its own: its canonical rows
          val rows = extractRows(run(leg.sql))
          val names = leg.select.fieldsWithComputedAliases.map(_.outputName)
          schema.foreach { s =>
            rows.foreach { row =>
              names.zip(s.slots).foreach {
                case (name, Some(slot)) =>
                  val cell = row.getOrElse(name, null)
                  val read = slot.read(cell)
                  (read, cell) match {
                    case (a: Array[Byte], b: Array[Byte]) => a.toSeq shouldBe b.toSeq
                    case _                                => read shouldBe cell
                  }
                case _ =>
              }
            }
          }
        }
      }
    }
  }

  "The JDBC driver" should "read the rows by its match and spell a binary as its base64" in {
    Seq("", " LIMIT 10").foreach { limit =>
      val rows = run(s"SELECT id, bin FROM $table WHERE id = 'c1'$limit") match {
        case QueryRows(rows, _, _)        => rows
        case QueryStructured(response, _) => response.results
        case QueryStream(stream, _, _) =>
          Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
        case other => fail(s"$other")
      }
      rows.size shouldBe 1
      ValueCoercion.coerceToString(rows.head("bin")) shouldBe "aGVsbG8="
    }
  }

  "The extensions" should "fold a count(*) as they do" in {
    val count = run(s"SELECT count(*) as nb FROM $table") match {
      case QueryRows(rows, _, _) => rows.headOption.map(_("nb"))
      case QueryStream(stream, _, _) =>
        Await.result(
          stream.runWith(
            Sink.fold[Option[Any], (ListMap[String, Any], ScrollMetrics)](None) {
              case (_, (row, _)) => row.get("nb")
            }
          ),
          60.seconds
        )
      case QueryStructured(response, _) => response.results.headOption.map(_("nb"))
      case other                        => fail(s"$other")
    }
    count.map(_.asInstanceOf[Number].longValue()) shouldBe Some(docs.size.toLong)
  }
}
