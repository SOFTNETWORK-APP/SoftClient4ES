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
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** The types a statement reports, the rows its predicates keep and the values its functions answer,
  * each asserted against DuckDB 1.5.5's answer over the same seven documents.
  *
  * Every expected value here was computed by DuckDB over a table holding these documents (a
  * `date_nanos` column as `TIMESTAMP_NS`, `items` as a list of structs, a missing field as NULL).
  * Each row EXECUTES on Elasticsearch: a rendered query that parses is not a query that answers.
  *
  * Both venues a statement reaches are used where they differ: the client API (`search`) and the
  * gateway (`run`, the path the JDBC driver, the REPL and the Flight SQL sidecar take).
  */
trait TypesPredicatesFunctionsSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "types_predicates_functions"

  private lazy val esMajor: Int = elasticVersion.takeWhile(_ != '.').toInt

  /** `date_nanos` where the major has one (7 and up); 6.8 maps a plain `date`. */
  private lazy val nanos: String = if (esMajor >= 7) "date_nanos" else "date"

  private val docs: List[String] = List(
    """{"id":"p1","g1":"a","n":0,"k":"a","x":2.46,"r":0.1,"b":true,"d":"2024-01-05","ts":"2024-01-05T10:20:30Z","dn":"2024-01-15T10:00:00Z","df":"05/01/2024","items":[{"sku":"s1","qty":1},{"sku":"s2","qty":2}]}""",
    """{"id":"p2","g1":"a","n":3,"k":"b","x":-2.46,"r":0.5,"b":false,"d":"2024-02-10","ts":"2024-02-10T23:30:00Z","dn":"2024-03-01T23:30:00Z","df":"10/02/2024","items":[{"sku":"s2","qty":3}]}""",
    """{"id":"p3","g1":"b","n":10,"x":2.44,"r":0.1,"b":true,"d":"2024-03-31","ts":"2024-03-31T00:00:00Z","df":"31/03/2024","items":[{"sku":"s1","qty":5}]}""",
    """{"id":"p4","g1":"b","k":"a","dn":"2024-05-05T05:05:05Z","items":[]}""",
    """{"id":"p5","g1":"c","n":7,"k":"c","x":1.25,"r":0.5,"b":false,"d":"2023-12-31","ts":"2023-12-31T08:00:00Z","dn":"2023-12-31T08:00:00Z","df":"31/12/2023"}""",
    """{"id":"p6","g1":"d","x":-1.25,"d":"2024-01-05"}""",
    """{"id":"p7","g1":"e","n":1,"k":"ab","x":2.55,"r":0.25,"b":true,"d":"2024-06-15","ts":"2024-06-15T12:00:00Z","dn":"2024-06-15T12:00:00Z","df":"15/06/2024","items":[{"sku":"s3","qty":1}]}"""
  )

  private val allIds: Set[String] = (1 to 7).map(i => s"p$i").toSet

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mapping =
      s"""{
        |  "properties": {
        |    "id":    { "type": "keyword" },
        |    "g1":    { "type": "keyword" },
        |    "n":     { "type": "integer" },
        |    "k":     { "type": "keyword" },
        |    "x":     { "type": "double" },
        |    "r":     { "type": "float" },
        |    "b":     { "type": "boolean" },
        |    "d":     { "type": "date" },
        |    "ts":    { "type": "date" },
        |    "dn":    { "type": "$nanos" },
        |    "df":    { "type": "date", "format": "dd/MM/yyyy" },
        |    "items": { "type": "nested", "properties": {
        |      "sku": { "type": "keyword" },
        |      "qty": { "type": "integer" }
        |    } }
        |  }
        |}""".stripMargin

    client
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

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
    system.terminate()
    super.afterAll()
  }

  // -- helpers -------------------------------------------------------------------------------------

  /** A `script_fields` value is a one-value list; flattened to the scalar it holds. */
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

  /** The rows of `sql` through `GatewayApi.run`. */
  private def gatewayRowsOf(sql: String): Seq[ListMap[String, Any]] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _)) =>
        Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case ElasticSuccess(other) => fail(s"Unexpected result: $other\n$sql")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}\n$sql")
    }

  /** The values of `column` over the rows of `sql`, through both venues (which must agree). */
  private def columnOf(sql: String, column: String): Seq[String] = {
    val viaSearch = rowsOf(sql).map(r => scalar(r.getOrElse(column, null)))
    val viaGateway = gatewayRowsOf(sql).map(r => scalar(r.getOrElse(column, null)))
    withClue(s"[$sql] search $viaSearch, gateway $viaGateway ") {
      viaGateway.sorted shouldBe viaSearch.sorted
    }
    info(s"[$sql] $viaSearch")
    viaSearch
  }

  /** The set of ids `sql` selects. */
  private def idsOf(sql: String): Set[String] = columnOf(sql, "id").toSet

  /** id -> the `v` column. */
  private def valuesById(sql: String): Map[String, String] = {
    val rows =
      rowsOf(sql).map(r => scalar(r.getOrElse("id", null)) -> scalar(r.getOrElse("v", null)))
    info(s"[$sql] $rows")
    rows.toMap
  }

  /** The message the statement fails with, or the rows it answered. */
  private def failureOf(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => Right(response.results)
      case ElasticFailure(error)    => Left(error.message)
    }

  /** The type core REPORTS for each SELECT item once the mapping is attached: what DESCRIBE, a
    * `CREATE TABLE … AS SELECT`, JDBC and arrow read.
    */
  private def reportedTypes(sql: String): Map[String, SQLType] =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        client.resolveWithSchema(single) match {
          case ElasticSuccess(resolved) =>
            resolved.select.fields.map { f =>
              f.fieldAlias.map(_.alias).getOrElse(f.sourceField) -> f.identifier.reportedType
            }.toMap
          case ElasticFailure(error) => fail(s"[$sql] resolution failed: ${error.message}")
        }
      case other => fail(s"[$sql] expected a single search, got $other")
    }

  private def instantOf(text: String): Option[Instant] =
    Try(java.time.OffsetDateTime.parse(text).toInstant)
      .orElse(Try(java.time.ZonedDateTime.parse(text).toInstant))
      .orElse(Try(Instant.parse(text)))
      .toOption

  // -- types: a conditional beside an arm of a still-unknown type -----------------------------------

  /** `dn` is a `date_nanos` column, typed `Any`: beside a DATE arm the node holds its timestamps,
    * and DuckDB types `COALESCE(<TIMESTAMP_NS>, <DATE>)` a `TIMESTAMP_NS`. Core reported DATE, so
    * arrow and JDBC, which read core's type, dropped the time of day. On 6.8 `dn` is a plain `date`
    * (a TIMESTAMP), which reports TIMESTAMP already. The rows keep the doc value's instant.
    */
  "a CASE or COALESCE with a date_nanos arm beside a DATE arm" should "report a TIMESTAMP and answer its instants" in {
    Seq(
      s"SELECT id, COALESCE(dn, CAST(d AS DATE)) AS v FROM $index",
      s"SELECT id, COALESCE(CAST(d AS DATE), dn) AS v FROM $index",
      s"SELECT id, CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE dn END AS v FROM $index",
      s"SELECT id, CASE WHEN n > 0 THEN dn ELSE DATE_ADD(CAST(d AS DATE), INTERVAL 1 DAY) END AS v FROM $index"
    ).foreach { sql =>
      withClue(sql)(reportedTypes(sql).get("v") shouldBe Some(SQLTypes.Timestamp))
    }
    // control: a NULL arm is no type, and a DATE beside it stays a DATE
    reportedTypes(s"SELECT id, COALESCE(NULL, CAST(d AS DATE)) AS v FROM $index").get(
      "v"
    ) shouldBe Some(SQLTypes.Date)
    // the documents with a `dn` answer its instant, its time of day included (DuckDB's values)
    val got = valuesById(s"SELECT id, COALESCE(dn, CAST(d AS DATE)) AS v FROM $index")
    Map(
      "p1" -> "2024-01-15T10:00:00Z",
      "p2" -> "2024-03-01T23:30:00Z",
      "p4" -> "2024-05-05T05:05:05Z",
      "p5" -> "2023-12-31T08:00:00Z",
      "p7" -> "2024-06-15T12:00:00Z"
    ).foreach { case (id, iso) =>
      withClue(s"$id: ${got.get(id)} ")(
        got.get(id).flatMap(instantOf) shouldBe Some(Instant.parse(iso))
      )
    }
  }
}
