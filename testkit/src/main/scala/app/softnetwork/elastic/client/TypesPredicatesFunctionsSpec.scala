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

  /** A second index: `e1` holds five elements `s1` (and one `s2`), `e1` and `e2` a half (`h`),
    * `e3` neither.
    */
  private val elementsIndex = "types_predicates_functions_elements"

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

    // a parent holding more matching elements than an `inner_hits` returns by default (3)
    client
      .createIndex(elementsIndex, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client
      .setMapping(
        elementsIndex,
        """{"properties": {"id": {"type": "keyword"}, "h": {"type": "double"}, "items": {"type":
          | "nested", "properties": {"sku": {"type": "keyword"}, "qty": {"type": "integer"}}}}}""".stripMargin
      )
      .get shouldBe true
    locally {
      // the same name, so it shadows the statement's options rather than competing with them
      implicit val bulkOptions: BulkOptions =
        BulkOptions(defaultIndex = elementsIndex, logEvery = 10)
      client.bulk[String](
        List(
          """{"id":"e1","h":2.5,"items":[{"sku":"s1","qty":1},{"sku":"s1","qty":2},{"sku":"s1","qty":3},{"sku":"s1","qty":4},{"sku":"s1","qty":5},{"sku":"s2","qty":6}]}""",
          """{"id":"e2","h":-2.5,"items":[{"sku":"s1","qty":7},{"sku":"s3","qty":8}]}""",
          """{"id":"e3"}"""
        ),
        identity,
        idKey = Some(Set("id"))
      ) match {
        case ElasticSuccess(_) => // ok
        case ElasticFailure(error) =>
          fail(s"Bulk indexing into $elementsIndex failed: ${error.message}")
      }
    }
    client.refresh(elementsIndex)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(elementsIndex)
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

  // -- functions: CONCAT skips a NULL argument --------------------------------------------------

  /** DuckDB's `concat` skips a NULL argument: `concat(NULL, 'x')` is `'x'` and so is `concat(k,
    * 'x')` over a NULL `k`. Core printed the NULL literal (and a `COALESCE` / `GREATEST` answering
    * NULL) as the text `null`, and made the call NULL over a NULL column.
    */
  "CONCAT" should "skip a NULL argument, as DuckDB does" in {
    valuesById(s"SELECT id, CONCAT(NULL, 'x') AS v FROM $index") shouldBe allIds
      .map(_ -> "x")
      .toMap
    valuesById(s"SELECT id, CONCAT(GREATEST(NULL, NULL), 'x') AS v FROM $index") shouldBe allIds
      .map(_ -> "x")
      .toMap
    valuesById(s"SELECT id, CONCAT(k, 'x') AS v FROM $index") shouldBe Map(
      "p1" -> "ax",
      "p2" -> "bx",
      "p3" -> "x",
      "p4" -> "ax",
      "p5" -> "cx",
      "p6" -> "x",
      "p7" -> "abx"
    )
    valuesById(s"SELECT id, CONCAT(COALESCE(NULL, k), '-', g1) AS v FROM $index") shouldBe Map(
      "p1" -> "a-a",
      "p2" -> "b-a",
      "p3" -> "-b",
      "p4" -> "a-b",
      "p5" -> "c-c",
      "p6" -> "-d",
      "p7" -> "ab-e"
    )
    // control: no NULL argument
    valuesById(s"SELECT id, CONCAT(k, '-', g1) AS v FROM $index WHERE k IS NOT NULL") shouldBe Map(
      "p1" -> "a-a",
      "p2" -> "b-a",
      "p4" -> "a-b",
      "p5" -> "c-c",
      "p7" -> "ab-e"
    )
  }

  // -- functions: a TEMPORAL expression cast to TIME --------------------------------------------

  /** `DATE_TRUNC` over a CASE whose arms are a DATE and a date-time renders either class, row by
    * row: the cast to TIME is decided from the runtime class, and answers the UTC time of day of
    * the truncated value (DuckDB: `00:00:00`). It answered the timestamp itself under TIME.
    */
  "CAST of a TEMPORAL expression AS TIME" should "answer its time of day, as DuckDB does" in {
    def times(sql: String): Map[String, Option[LocalTime]] =
      valuesById(sql).map { case (id, v) =>
        id -> (if (v == "null") None else Some(Try(LocalTime.parse(v)).getOrElse(fail(s"$id: $v"))))
      }
    val midnight = Some(LocalTime.MIDNIGHT)
    val truncated = Map(
      "p1" -> midnight,
      "p2" -> midnight,
      "p3" -> midnight,
      "p4" -> None,
      "p5" -> midnight,
      "p6" -> midnight,
      "p7" -> midnight
    )
    times(
      s"SELECT id, CAST(DATE_TRUNC(CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE CAST(d AS DATETIME) END, MONTH) AS TIME) AS v FROM $index"
    ) shouldBe truncated
    times(
      s"SELECT id, CAST(DATE_TRUNC(CASE WHEN n > 1 THEN ts ELSE CAST(d AS DATETIME) END, MONTH) AS TIME) AS v FROM $index"
    ) shouldBe truncated
    // control: a TIMESTAMP column cast to TIME, its time of day
    times(s"SELECT id, CAST(ts AS TIME) AS v FROM $index") shouldBe Map(
      "p1" -> Some(LocalTime.parse("10:20:30")),
      "p2" -> Some(LocalTime.parse("23:30:00")),
      "p3" -> midnight,
      "p4" -> None,
      "p5" -> Some(LocalTime.parse("08:00:00")),
      "p6" -> None,
      "p7" -> Some(LocalTime.parse("12:00:00"))
    )
  }

  // -- functions: ROUND to decimal places ---------------------------------------------------------

  /** A number read back as the decimal it denotes, `None` for NULL. */
  private def decimal(v: String): Option[BigDecimal] =
    if (v == "null") None else Some(Try(BigDecimal(v)).getOrElse(fail(s"not a number: $v")))

  /** `ROUND(x, 1)` is DuckDB's `std::round(x * 10) / 10`, a half away from zero: `2.46` is `2.5`,
    * `-1.25` is `-1.3`. The script was `Math.round(x)`: every scale answered a whole number, in the
    * SELECT list, a WHERE, a GROUP BY key and the HAVING filter on that key.
    */
  "ROUND to decimal places" should "answer DuckDB's value in every clause" in {
    valuesById(s"SELECT id, ROUND(x, 1) AS v FROM $index").map { case (id, v) =>
      id -> decimal(v)
    } shouldBe Map(
      "p1" -> Some(BigDecimal("2.5")),
      "p2" -> Some(BigDecimal("-2.5")),
      "p3" -> Some(BigDecimal("2.4")),
      "p4" -> None,
      "p5" -> Some(BigDecimal("1.3")),
      "p6" -> Some(BigDecimal("-1.3")),
      "p7" -> Some(BigDecimal("2.6"))
    )
    idsOf(s"SELECT id FROM $index WHERE ROUND(x, 1) = 2.5") shouldBe Set("p1")
    // the GROUP BY key; DuckDB has a NULL group too, which a terms level does not return
    columnOf(
      s"SELECT ROUND(x, 1) AS rx, COUNT(*) AS c FROM $index GROUP BY ROUND(x, 1)",
      "rx"
    ).flatMap(decimal).toSet shouldBe Set("-2.5", "-1.3", "1.3", "2.4", "2.5", "2.6")
      .map(BigDecimal(_))
    // the HAVING key filter on that key
    columnOf(
      s"SELECT ROUND(x, 1) AS rx, COUNT(*) AS c FROM $index GROUP BY ROUND(x, 1) HAVING ROUND(x, 1) = 2.5",
      "rx"
    ).flatMap(decimal) shouldBe Seq(BigDecimal("2.5"))
    columnOf(
      s"SELECT ROUND(x, 1) AS rx, COUNT(*) AS c FROM $index GROUP BY ROUND(x, 1) HAVING ROUND(x, 1) IN (2.5, -2.5)",
      "rx"
    ).flatMap(decimal).toSet shouldBe Set(BigDecimal("2.5"), BigDecimal("-2.5"))
    // control: to no decimal place, a whole number, as before
    valuesById(s"SELECT id, ROUND(x) AS v FROM $index").map { case (id, v) =>
      id -> decimal(v)
    } shouldBe Map(
      "p1" -> Some(BigDecimal(2)),
      "p2" -> Some(BigDecimal(-2)),
      "p3" -> Some(BigDecimal(2)),
      "p4" -> None,
      "p5" -> Some(BigDecimal(1)),
      "p6" -> Some(BigDecimal(-1)),
      "p7" -> Some(BigDecimal(3))
    )
  }

  // -- predicates: a fractional number against an INTEGER key in HAVING --------------------------

  /** The groups of `n` a HAVING keeps, as the decimals their keys denote. */
  private def nGroups(having: String): Set[BigDecimal] =
    columnOf(s"SELECT n, COUNT(*) AS c FROM $index GROUP BY n HAVING $having", "n")
      .flatMap(v => if (v == "null") None else Some(BigDecimal(v)))
      .toSet

  /** No integer is equal to 1.5: `n = 1.5` keeps no group and `n <> 1.5` every one (DuckDB 1.5.5,
    * v0.23.0). It was refused ("no integer is equal to it"); sent as it is, Elasticsearch compares
    * the key with the floor, 1, which the fixture holds.
    */
  "HAVING on an integer key compared with a fractional number" should "answer DuckDB's groups" in {
    val every = Set(0, 1, 3, 7, 10).map(BigDecimal(_))
    nGroups("n = 1.5") shouldBe Set.empty
    nGroups("n <> 1.5") shouldBe every
    nGroups("n IN (1.5, 2.5)") shouldBe Set.empty
    nGroups("n NOT IN (1.5)") shouldBe every
    nGroups("n = 1.5 OR n = 3") shouldBe Set(BigDecimal(3))
    nGroups("n = 1.5 AND COUNT(*) > 0") shouldBe Set.empty
    // control: a whole number
    nGroups("n = 1") shouldBe Set(BigDecimal(1))
  }

  // -- predicates: a float key in HAVING -----------------------------------------------------------

  /** Elasticsearch prints a `float` key as the float widened to a double, so `HAVING r = 0.1` kept
    * no group with HTTP 200, where DuckDB keeps the group 0.1: refused by name now. A value a float
    * holds exactly is answered, as DuckDB answers it.
    */
  "HAVING on a float key" should "refuse a value a float does not hold, and answer one it holds" in {
    Seq(
      s"SELECT r, COUNT(*) AS c FROM $index GROUP BY r HAVING r = 0.1",
      s"SELECT r, COUNT(*) AS c FROM $index GROUP BY r HAVING r <> 0.1"
    ).foreach { sql =>
      withClue(sql) {
        failureOf(sql).left.toOption.getOrElse("") should include(
          "HAVING cannot compare the float GROUP BY key r with 0.1"
        )
      }
    }
    def keys(having: String): Set[BigDecimal] =
      columnOf(s"SELECT r, COUNT(*) AS c FROM $index GROUP BY r HAVING $having", "r")
        .map(BigDecimal(_))
        .toSet
    keys("r = 0.5") shouldBe Set(BigDecimal("0.5"))
    keys("r IN (0.25, 0.5)") shouldBe Set(BigDecimal("0.25"), BigDecimal("0.5"))
  }

  // -- predicates: a negation leaves the NULL rows out ---------------------------------------------

  /** SQL's `n <> 0` is NULL, not TRUE, where `n` is NULL: DuckDB leaves such a row out. A
    * `must_not` alone kept every document missing the column, on every field type; it now stands
    * beside the column's `exists`. One row per field type, and per negated form.
    */
  "A negated comparison" should "leave out the rows whose column is NULL, as DuckDB does" in {
    Seq(
      "n <> 0"                                          -> Set("p2", "p3", "p5", "p7"),
      "NOT n = 0"                                       -> Set("p2", "p3", "p5", "p7"),
      "k <> 'a'"                                        -> Set("p2", "p5", "p7"),
      "x <> 2.46"                                       -> Set("p2", "p3", "p5", "p6", "p7"),
      "r <> 0.5"                                        -> Set("p1", "p3", "p7"),
      "b <> true"                                       -> Set("p2", "p5"),
      "d <> '2024-01-05'"                               -> Set("p2", "p3", "p5", "p7"),
      "df <> '05/01/2024'"                              -> Set("p2", "p3", "p5", "p7"),
      "n NOT IN (0, 3)"                                 -> Set("p3", "p5", "p7"),
      "k NOT IN ('a', 'b')"                             -> Set("p5", "p7"),
      "n NOT BETWEEN 1 AND 5"                           -> Set("p1", "p3", "p5"),
      "k NOT LIKE 'a%'"                                 -> Set("p2", "p5"),
      "g1 = 'b' AND NOT k = 'b'"                        -> Set("p4"),
      "g1 IN ('a', 'b') AND NOT MATCH(k) AGAINST ('a')" -> Set("p2")
    ).foreach { case (condition, expected) =>
      withClue(condition)(idsOf(s"SELECT id FROM $index WHERE $condition") shouldBe expected)
    }
    // control: the rows the negations leave out
    idsOf(s"SELECT id FROM $index WHERE n IS NULL") shouldBe Set("p4", "p6")
  }

  // -- predicates: NESTED with no JOIN UNNEST ------------------------------------------------------

  /** `WHERE NESTED(items.sku = 's1')` keeps the documents one of whose elements satisfies the
    * condition (DuckDB: `EXISTS (SELECT 1 FROM unnest(items) AS e WHERE e.sku = 's1')`). It
    * rendered `match_all` and returned every document.
    */
  "WHERE NESTED with no JOIN UNNEST" should "filter by its condition, as DuckDB does" in {
    idsOf(s"SELECT id FROM $index WHERE NESTED(items.sku = 's1')") shouldBe Set("p1", "p3")
    idsOf(
      s"SELECT id FROM $index WHERE NESTED(items.sku = 's2' AND items.qty > 2)"
    ) shouldBe Set("p2")
    idsOf(s"SELECT id FROM $index WHERE NESTED(items.sku = 's1') AND g1 = 'a'") shouldBe Set("p1")
    // control: the same condition through a JOIN UNNEST, one row per element
    rowsOf(s"SELECT t.id FROM $index AS t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'")
      .map(r => scalar(r.getOrElse("id", null)))
      .toSet shouldBe Set("p1", "p3")
  }

  // -- UNNEST: a WHERE over an element, without LIMIT ----------------------------------------------

  /** `… JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'`, no column of `i` projected, answers one row
    * per matching element (DuckDB: six rows, five of `e1` and one of `e2`). The element's
    * `inner_hits` asked for Elasticsearch's default size of 3, so `e1` answered three rows.
    */
  "A WHERE over an UNNEST element without LIMIT" should "answer every matching element" in {
    def ids(where: String): Seq[String] =
      columnOf(
        s"SELECT t.id FROM $elementsIndex AS t JOIN UNNEST(t.items) AS i WHERE $where",
        "id"
      ).sorted
    ids("i.sku = 's1'") shouldBe Seq.fill(5)("e1") :+ "e2"
    ids("i.qty > 1") shouldBe Seq.fill(5)("e1") ++ Seq.fill(2)("e2")
    // control: an element column projected, the same rows
    columnOf(
      s"SELECT t.id, i.qty AS q FROM $elementsIndex AS t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'",
      "q"
    ).sorted shouldBe Seq("1", "2", "3", "4", "5", "7")
  }

  // -- functions: CONCAT in a computed column (an ingest processor) ---------------------------------

  /** A statement through the gateway that must succeed, whatever it answers. */
  private def execute(sql: String): Unit =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(_)     => // ok
      case ElasticFailure(error) => fail(s"Statement failed: ${error.message}\n$sql")
    }

  /** A computed column is the same CONCAT an ingest processor runs at index time: it skips a NULL
    * argument, as the CONCAT of a query does and as DuckDB's `concat` does. It made the whole value
    * NULL (#382 Ruling B, overturned), printed the NULL literal as the text `null`, and applied a
    * function over it to its last argument only (`UPPER(CONCAT(k, 'x'))` read `aX`).
    */
  "CONCAT in a computed column" should "skip a NULL argument, as the CONCAT of a query does" in {
    val table = "types_predicates_functions_concat"
    execute(
      s"""CREATE TABLE IF NOT EXISTS $table (
         |  id KEYWORD,
         |  k KEYWORD,
         |  g1 KEYWORD,
         |  c1 KEYWORD SCRIPT AS (CONCAT(k, 'x')),
         |  c2 KEYWORD SCRIPT AS (CONCAT(k, '-', g1)),
         |  c3 KEYWORD SCRIPT AS (UPPER(CONCAT(k, 'x'))),
         |  c4 KEYWORD SCRIPT AS (CONCAT(NULL, g1))
         |)""".stripMargin
    )
    try {
      execute(s"INSERT INTO $table (id, k, g1) VALUES ('q1', 'a', 'b')")
      execute(s"INSERT INTO $table (id, g1) VALUES ('q2', 'c')")
      client.refresh(table)
      // id -> `column`, through both venues (which must agree)
      def byId(column: String): Map[String, String] = {
        val sql = s"SELECT id, $column FROM $table"
        def of(rows: Seq[ListMap[String, Any]]): Map[String, String] =
          rows.map(r => scalar(r.getOrElse("id", null)) -> scalar(r.getOrElse(column, null))).toMap
        val viaSearch = of(rowsOf(sql))
        withClue(s"[$sql] ")(of(gatewayRowsOf(sql)) shouldBe viaSearch)
        viaSearch
      }
      byId("c1") shouldBe Map("q1" -> "ax", "q2" -> "x")
      byId("c2") shouldBe Map("q1" -> "a-b", "q2" -> "-c")
      byId("c3") shouldBe Map("q1" -> "AX", "q2" -> "X")
      byId("c4") shouldBe Map("q1" -> "b", "q2" -> "c")
      // control: the same CONCAT in a query over the stored columns answers the same values
      valuesById(s"SELECT id, CONCAT(k, '-', g1) AS v FROM $table") shouldBe
      Map("q1" -> "a-b", "q2" -> "-c")
    } finally execute(s"DROP TABLE IF EXISTS $table")
  }

  // -- functions: ROUND to no decimal place -----------------------------------------------------

  /** `ROUND(x)` and `ROUND(x, 0)` round a half AWAY from zero, as DuckDB 1.5.5 does: `ROUND(2.5)` is
    * `3` and `ROUND(-2.5)` is `-3`. The script rounded a half up (`Math.round`), so `-2.5` answered
    * `-2`. A NULL operand that is no name (`ROUND(ABS(x))` over a document missing `x`) answers
    * NULL: it was multiplied, and the shard failed.
    */
  "ROUND to no decimal place" should "round a half away from zero and answer NULL for NULL" in {
    def numbers(sql: String): Map[String, Option[BigDecimal]] =
      valuesById(sql).map { case (id, v) => id -> decimal(v) }
    val halves = Map(
      "e1" -> Some(BigDecimal(3)),
      "e2" -> Some(BigDecimal(-3)),
      "e3" -> None
    )
    numbers(s"SELECT id, ROUND(h) AS v FROM $elementsIndex") shouldBe halves
    numbers(s"SELECT id, ROUND(h, 0) AS v FROM $elementsIndex") shouldBe halves
    numbers(s"SELECT id, ROUND(ABS(h)) AS v FROM $elementsIndex") shouldBe Map(
      "e1" -> Some(BigDecimal(3)),
      "e2" -> Some(BigDecimal(3)),
      "e3" -> None
    )
    idsOf(s"SELECT id FROM $elementsIndex WHERE ROUND(h) = -3") shouldBe Set("e2")
    // ROUND over a NULL function value, on every document of the main index (DuckDB's values)
    numbers(s"SELECT id, ROUND(ABS(x)) AS v FROM $index") shouldBe Map(
      "p1" -> Some(BigDecimal(2)),
      "p2" -> Some(BigDecimal(2)),
      "p3" -> Some(BigDecimal(2)),
      "p4" -> None,
      "p5" -> Some(BigDecimal(1)),
      "p6" -> Some(BigDecimal(1)),
      "p7" -> Some(BigDecimal(3))
    )
    numbers(s"SELECT id, ROUND(ABS(x), 1) AS v FROM $index").get("p4") shouldBe Some(None)
    // control: a value that is no half
    numbers(s"SELECT id, ROUND(x, 0) AS v FROM $index").get("p2") shouldBe
    Some(Some(BigDecimal(-2)))
  }

  // -- types: ABS of a whole number ----------------------------------------------------------------

  /** `ABS` answers its argument's type (DuckDB: `abs(INTEGER)` is an INTEGER, `abs(DOUBLE)` a
    * DOUBLE). It rendered NUMERIC whatever the argument, so a GROUP BY `ABS(n)` level was a `double`
    * terms level and its keys came back as `3.0`. DuckDB's groups also hold a NULL one, which a
    * terms level does not return.
    */
  "ABS of an INTEGER column" should "report and answer an INTEGER, a GROUP BY key included" in {
    reportedTypes(s"SELECT ABS(n) AS v, ABS(x) AS w FROM $index") shouldBe Map(
      "v" -> SQLTypes.Int,
      "w" -> SQLTypes.Double
    )
    columnOf(
      s"SELECT ABS(n) AS v, COUNT(*) AS c FROM $index GROUP BY ABS(n)",
      "v"
    ).toSet shouldBe Set("0", "1", "3", "7", "10")
    valuesById(s"SELECT id, ABS(n) AS v FROM $index") shouldBe Map(
      "p1" -> "0",
      "p2" -> "3",
      "p3" -> "10",
      "p4" -> "null",
      "p5" -> "7",
      "p6" -> "null",
      "p7" -> "1"
    )
    // control: over a DOUBLE, a double
    valuesById(s"SELECT id, ABS(x) AS v FROM $index").get("p2") shouldBe Some("2.46")
  }

  // -- functions: DATETIME_FORMAT / DATE_FORMAT of a raw date column ---------------------------------

  /** DuckDB's `strftime` of each document's date. Elasticsearch 6.8 hands a query script a `date`
    * doc value as a `JodaCompatibleZonedDateTime`, which the formatter cannot read: the statement
    * answered HTTP 500 there. It is formatted as the UTC instant it holds, on every major.
    */
  "DATETIME_FORMAT of a raw date column" should "answer the formatted dates on every major" in {
    valuesById(
      s"SELECT id, DATETIME_FORMAT(ts, '%Y-%m-%d %H:%i:%s') AS v FROM $index"
    ) shouldBe Map(
      "p1" -> "2024-01-05 10:20:30",
      "p2" -> "2024-02-10 23:30:00",
      "p3" -> "2024-03-31 00:00:00",
      "p4" -> "null",
      "p5" -> "2023-12-31 08:00:00",
      "p6" -> "null",
      "p7" -> "2024-06-15 12:00:00"
    )
    idsOf(s"SELECT id FROM $index WHERE DATETIME_FORMAT(ts, '%Y') = '2024'") shouldBe
    Set("p1", "p2", "p3", "p7")
    valuesById(s"SELECT id, DATE_FORMAT(d, '%Y-%m-%d') AS v FROM $index") shouldBe Map(
      "p1" -> "2024-01-05",
      "p2" -> "2024-02-10",
      "p3" -> "2024-03-31",
      "p4" -> "null",
      "p5" -> "2023-12-31",
      "p6" -> "2024-01-05",
      "p7" -> "2024-06-15"
    )
    // control: a truncated column, a genuine ZonedDateTime on every major
    valuesById(
      s"SELECT id, DATETIME_FORMAT(DATE_TRUNC(ts, MONTH), '%Y-%m') AS v FROM $index"
    ).get("p2") shouldBe Some("2024-02")
  }

  // -- functions: a TEMPORAL expression cast to DATE or BIGINT ---------------------------------------

  /** `DATE_TRUNC` over a CASE of a DATE and a date-time arm renders either class, row by row: its
    * cast to DATE is the date part, and to BIGINT the epoch milliseconds (DuckDB's values, a DATE's
    * UTC midnight). Both answered the truncated timestamp itself under their label.
    */
  "CAST of a TEMPORAL expression AS DATE or BIGINT" should "convert the value, as DuckDB does" in {
    val months = Map(
      "p1" -> "2024-01-01",
      "p2" -> "2024-02-01",
      "p3" -> "2024-03-01",
      "p5" -> "2023-12-01",
      "p6" -> "2024-01-01",
      "p7" -> "2024-06-01"
    )
    Seq(
      "CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE CAST(d AS DATETIME) END",
      "CASE WHEN n > 1 THEN ts ELSE CAST(d AS DATETIME) END"
    ).foreach { operand =>
      withClue(operand) {
        val dates = valuesById(s"SELECT id, CAST(DATE_TRUNC($operand, MONTH) AS DATE) AS v FROM $index")
        dates.get("p4") shouldBe Some("null")
        // the date itself, never a timestamp under the DATE label (`2024-01-01T00:00Z`)
        months.foreach { case (id, day) =>
          withClue(s"$id: ${dates.get(id)} ")(dates.get(id) shouldBe Some(day))
        }
        reportedTypes(
          s"SELECT id, CAST(DATE_TRUNC($operand, MONTH) AS DATE) AS v FROM $index"
        ).get("v") shouldBe Some(SQLTypes.Date)
        val millis =
          valuesById(s"SELECT id, CAST(DATE_TRUNC($operand, MONTH) AS BIGINT) AS v FROM $index")
        millis.get("p4") shouldBe Some("null")
        months.foreach { case (id, day) =>
          withClue(s"$id: ${millis.get(id)} ")(
            millis.get(id).map(BigDecimal(_)) shouldBe Some(
              BigDecimal(java.time.LocalDate.parse(day).atStartOfDay(java.time.ZoneOffset.UTC).toInstant.toEpochMilli)
            )
          )
        }
      }
    }
    // control: a TIMESTAMP column cast to BIGINT, its epoch milliseconds
    valuesById(s"SELECT id, CAST(ts AS BIGINT) AS v FROM $index").get("p1").map(BigDecimal(_)) shouldBe
    Some(BigDecimal(Instant.parse("2024-01-05T10:20:30Z").toEpochMilli))
  }

  // -- predicates: NOT BETWEEN with a date-math and a script bound ---------------------------------

  /** `NOT BETWEEN` whose bounds render one as date math and the other as a script (`LAST_DAY`, a
    * `COALESCE`) is `NOT (a AND b)` (DuckDB's rows). It rendered `NOT a AND NOT b`: no row.
    */
  "NOT BETWEEN with a date-math and a script bound" should "answer the rows outside the range" in {
    val bounds = "CAST('2024-02-01' AS DATE) AND LAST_DAY(CAST('2024-03-01' AS DATE))"
    idsOf(s"SELECT id FROM $index WHERE d NOT BETWEEN $bounds") shouldBe
    Set("p1", "p5", "p6", "p7")
    idsOf(
      s"SELECT id FROM $index WHERE d NOT BETWEEN LAST_DAY(CAST('2024-01-01' AS DATE)) AND CAST('2024-03-31' AS DATE)"
    ) shouldBe Set("p1", "p5", "p6", "p7")
    idsOf(
      s"SELECT id FROM $index WHERE ts NOT BETWEEN CAST('2024-02-01' AS TIMESTAMP) AND COALESCE(CAST('2024-04-01' AS TIMESTAMP), NOW())"
    ) shouldBe Set("p1", "p5", "p7")
    // control: BETWEEN, both halves
    idsOf(s"SELECT id FROM $index WHERE d BETWEEN $bounds") shouldBe Set("p2", "p3")
  }

  // -- predicates: a negated MATCH under OR -----------------------------------------------------------

  /** `x OR NOT MATCH(k) AGAINST ('a')` keeps the documents satisfying either (DuckDB's rows for
    * `x OR NOT k = 'a'`; a NULL `k` makes the negation NULL). The negated MATCH stood in the bool's
    * own `must_not`, beside the `should`, so the OR answered an AND.
    */
  "A negated MATCH under OR" should "answer the OR, as DuckDB does" in {
    idsOf(s"SELECT id FROM $index WHERE n = 10 OR NOT MATCH(k) AGAINST ('a')") shouldBe
    Set("p2", "p3", "p5", "p7")
    idsOf(s"SELECT id FROM $index WHERE n = 50 OR NOT MATCH(k) AGAINST ('a')") shouldBe
    Set("p2", "p5", "p7")
    idsOf(
      s"SELECT id FROM $index WHERE (n = 10 OR NOT MATCH(k) AGAINST ('a')) AND g1 = 'b'"
    ) shouldBe Set("p3")
    // control: under AND, both
    idsOf(s"SELECT id FROM $index WHERE n = 10 AND NOT MATCH(k) AGAINST ('a')") shouldBe Set.empty
    idsOf(s"SELECT id FROM $index WHERE n = 3 AND NOT MATCH(k) AGAINST ('a')") shouldBe Set("p2")
  }

  // -- predicates: a condition over a nested column, with no NESTED and no UNNEST ---------------------

  /** `WHERE items.sku = 's1'` keeps the documents one of whose elements satisfies it (DuckDB:
    * `EXISTS (SELECT 1 FROM unnest(items) AS e WHERE e.sku = 's1')`); conditions over the same path
    * test the same element. It queried the root document, which holds no field of a nested path:
    * no row, HTTP 200. A condition no nested query can test is refused by name.
    */
  "A condition over a nested column without NESTED or UNNEST" should "keep the documents with such an element" in {
    Seq(
      "items.sku = 's1'"                  -> Set("p1", "p3"),
      "items.sku = 's2' AND items.qty > 2" -> Set("p2"),
      "items.sku <> 's1'"                 -> Set("p1", "p2", "p7"),
      "items.sku IN ('s1', 's3')"         -> Set("p1", "p3", "p7"),
      "items.qty BETWEEN 2 AND 4"         -> Set("p1", "p2"),
      "items.sku = 's1' OR g1 = 'e'"      -> Set("p1", "p3", "p7")
    ).foreach { case (condition, expected) =>
      withClue(condition)(idsOf(s"SELECT id FROM $index WHERE $condition") shouldBe expected)
    }
    failureOf(s"SELECT id FROM $index WHERE items.qty > n").left.toOption.getOrElse("") should
    include("a column of the nested path items and a column of the document")
    // control: the same condition written with NESTED
    idsOf(s"SELECT id FROM $index WHERE NESTED(items.sku = 's1')") shouldBe Set("p1", "p3")
  }
}
