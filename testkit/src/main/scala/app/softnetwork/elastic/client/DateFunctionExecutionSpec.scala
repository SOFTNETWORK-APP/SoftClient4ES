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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalDate, ZoneOffset}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

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
    if (diffIndexCreated) client.deleteIndex(diffIndex)
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

  // -----------------------------------------------------------------------------------------------
  // The DATEDIFF family against its documented semantics, computed HERE:
  //
  //   - HOUR, MINUTE and SECOND count the ELAPSED whole units between two instants (UTC), truncated
  //     toward zero; a DATE operand is the start of its day;
  //   - DAY and every larger unit compare two CALENDAR dates (UTC): days, weeks (days / 7), months
  //     (complete once the day of month is reached), quarters (months / 3), years (months / 12),
  //     truncated toward zero;
  //   - the answer is `second - first`, except MySQL's two-argument DATEDIFF: `first - second`;
  //   - a string literal is the temporal it spells, a DATE alone or a TIMESTAMP (UTC unless it names
  //     a zone); NULL when an operand has no value.
  //
  // The population is every spelling (DATE_DIFF / DATEDIFF with the unit last or first,
  // TIMESTAMPDIFF, DATE_DIFF without a unit, MySQL's DATEDIFF), every unit, every operand kind (a
  // date column, a timestamp column, a DATE literal, two TIMESTAMP literals, a date and a timestamp
  // aggregate) and every venue: a SELECT item per document, a WHERE, a SELECT item per group and a
  // HAVING through that item's alias. Before, HOUR / MINUTE / SECOND over a column or an aggregate
  // failed with `Unsupported unit: Hours`, QUARTER did not compile, and a literal failed at row
  // level and, with a time of day, per group.
  // -----------------------------------------------------------------------------------------------

  private val diffIndex = "date_diff_semantics"

  @volatile private var diffIndexCreated = false

  private type DiffDoc = (String, String, Option[String], Option[String])

  /** (id, group, `d` a calendar date, `ts` an instant); `None`: the document lacks it. Group g1 has
    * no value at all, g4 has some.
    */
  private val diffDocs: Seq[DiffDoc] = Seq(
    ("r1", "g1", None, None),
    ("r2", "g1", None, None),
    ("r3", "g2", Some("2024-01-02"), Some("2024-01-01T23:30:00Z")),
    ("r4", "g3", Some("2023-11-15"), Some("2024-01-01T10:30:15Z")),
    ("r5", "g3", Some("2024-02-29"), Some("2024-03-31T00:00:45Z")),
    ("r6", "g4", Some("2024-01-31"), None),
    ("r7", "g4", None, Some("2023-12-31T23:59:59Z")),
    ("r8", "g4", Some("2025-06-15"), Some("2024-06-14T12:00:00Z"))
  )

  private lazy val diffIndexLoaded: Unit = {
    client
      .createIndex(diffIndex, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    diffIndexCreated = true
    client
      .setMapping(
        diffIndex,
        """{"properties": {"id": {"type": "keyword"}, "g": {"type": "keyword"}, "d": {"type": "date"}, "ts": {"type": "date"}}}"""
      )
      .get shouldBe true
    val docs = diffDocs.toList.map { case (id, g, d, ts) =>
      (Seq(s""""id":"$id"""", s""""g":"$g"""") ++ d.map(x => s""""d":"$x"""") ++
      ts.map(x => s""""ts":"$x"""")).mkString("{", ",", "}")
    }
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = diffIndex, logEvery = 10)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(diffIndex)
      case ElasticFailure(error) =>
        fail(s"Bulk indexing into $diffIndex failed: ${error.message}")
    }
  }

  /** An operand's value: a calendar date, or an instant. */
  private sealed trait DiffValue
  private final case class OnDate(date: LocalDate) extends DiffValue
  private final case class AtInstant(instant: Instant) extends DiffValue

  /** An operand of the population, and its value over a document or over a group's documents. */
  private final case class DiffOperand(sql: String, aggregate: Boolean)(
    val value: Seq[DiffDoc] => Option[DiffValue]
  )

  private def constant(sql: String, value: DiffValue): DiffOperand =
    DiffOperand(sql, aggregate = false)(_ => Some(value))

  private val diffLiterals: Seq[DiffOperand] = Seq(
    constant("'2024-01-01'", OnDate(LocalDate.parse("2024-01-01"))),
    constant("'2024-01-01 10:00:00'", AtInstant(Instant.parse("2024-01-01T10:00:00Z"))),
    constant("'2023-12-31T22:15:30Z'", AtInstant(Instant.parse("2023-12-31T22:15:30Z")))
  )

  private val diffRowOperands: Seq[DiffOperand] = Seq(
    DiffOperand("d", aggregate = false)(_.head._3.map(x => OnDate(LocalDate.parse(x)))),
    DiffOperand("ts", aggregate = false)(_.head._4.map(x => AtInstant(Instant.parse(x))))
  ) ++ diffLiterals

  private val diffGroupOperands: Seq[DiffOperand] = Seq(
    DiffOperand("MAX(d)", aggregate = true)(
      _.flatMap(_._3)
        .map(LocalDate.parse(_))
        .reduceOption((x, y) => if (x.isAfter(y)) x else y)
        .map(date => OnDate(date))
    ),
    DiffOperand("MIN(ts)", aggregate = true)(
      _.flatMap(_._4)
        .map(Instant.parse(_))
        .reduceOption((x, y) => if (x.isBefore(y)) x else y)
        .map(instant => AtInstant(instant))
    )
  ) ++ diffLiterals

  private def utcDate(value: DiffValue): LocalDate = value match {
    case OnDate(date)       => date
    case AtInstant(instant) => instant.atZone(ZoneOffset.UTC).toLocalDate
  }

  private def utcSeconds(value: DiffValue): Long = value match {
    case OnDate(date)       => date.toEpochDay * 86400L
    case AtInstant(instant) => instant.getEpochSecond
  }

  /** The complete months from `start` to `end`: a month counts once its day of month is reached. */
  private def wholeMonths(start: LocalDate, end: LocalDate): Long = {
    def packed(date: LocalDate): Long =
      (date.getYear * 12L + date.getMonthValue - 1) * 32L + date.getDayOfMonth
    (packed(end) - packed(start)) / 32L
  }

  /** `end - start` in `unit`, truncated toward zero (Scala's `Long` division). */
  private def documentedDiff(unit: String, start: DiffValue, end: DiffValue): Long = unit match {
    case "SECOND"  => utcSeconds(end) - utcSeconds(start)
    case "MINUTE"  => (utcSeconds(end) - utcSeconds(start)) / 60L
    case "HOUR"    => (utcSeconds(end) - utcSeconds(start)) / 3600L
    case "DAY"     => utcDate(end).toEpochDay - utcDate(start).toEpochDay
    case "WEEK"    => (utcDate(end).toEpochDay - utcDate(start).toEpochDay) / 7L
    case "MONTH"   => wholeMonths(utcDate(start), utcDate(end))
    case "QUARTER" => wholeMonths(utcDate(start), utcDate(end)) / 3L
    case "YEAR"    => wholeMonths(utcDate(start), utcDate(end)) / 12L
  }

  /** A spelling of the family: its SQL over two operands, its unit, and whether it is MySQL's
    * two-argument DATEDIFF (`first - second`).
    */
  private final case class DiffSpelling(unit: String, mysql: Boolean)(
    val sql: (String, String) => String
  )

  private val diffUnits: Seq[String] =
    Seq("YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND")

  private val diffSpellings: Seq[DiffSpelling] = diffUnits.flatMap { u =>
    Seq(
      DiffSpelling(u, mysql = false)((a, b) => s"DATE_DIFF($a, $b, $u)"),
      DiffSpelling(u, mysql = false)((a, b) => s"DATEDIFF($a, $b, $u)"),
      DiffSpelling(u, mysql = false)((a, b) => s"DATE_DIFF($u, $a, $b)"),
      DiffSpelling(u, mysql = false)((a, b) => s"DATEDIFF($u, $a, $b)"),
      DiffSpelling(u, mysql = false)((a, b) => s"TIMESTAMPDIFF($u, $a, $b)")
    )
  } ++ Seq(
    DiffSpelling("DAY", mysql = false)((a, b) => s"DATE_DIFF($a, $b)"),
    DiffSpelling("DAY", mysql = true)((a, b) => s"DATEDIFF($a, $b)")
  )

  /** A statement, the venue it runs in, and the documented answer for each document or group. */
  private final case class DiffStatement(
    sql: String,
    venue: String,
    unit: String,
    expected: Map[String, Option[Long]]
  ) {
    def filters: Boolean = venue == "WHERE" || venue == "HAVING"
    def kept: Set[String] = expected.collect { case (k, Some(v)) if v > 0 => k }.toSet
  }

  private lazy val diffPopulation: Seq[DiffStatement] = {
    val perDocument: Seq[(String, Seq[DiffDoc])] = diffDocs.map(doc => doc._1 -> Seq(doc))
    val perGroup: Seq[(String, Seq[DiffDoc])] = diffDocs.groupBy(_._2).toSeq.sortBy(_._1)
    val rowPairs = for (a <- diffRowOperands; b <- diffRowOperands) yield (a, b)
    // A WHERE over two literals reads no column: it is a constant predicate, rendered as a bare
    // comparison of the function's boxed result, which fails to compile whatever the function is
    // (`WHERE ABS(-3) > 0` too). It is not a DATEDIFF question, so it is left out.
    val wherePairs = rowPairs.filterNot { case (a, b) =>
      diffLiterals.contains(a) && diffLiterals.contains(b)
    }
    val groupPairs =
      for (a <- diffGroupOperands; b <- diffGroupOperands if a.aggregate || b.aggregate)
        yield (a, b)
    for {
      spelling <- diffSpellings
      (venue, pairs, scopes) <- Seq(
        ("SELECT", rowPairs, perDocument),
        ("WHERE", wherePairs, perDocument),
        ("GROUP", groupPairs, perGroup),
        ("HAVING", groupPairs, perGroup)
      )
      (a, b) <- pairs
    } yield {
      val e = spelling.sql(a.sql, b.sql)
      val sql = venue match {
        case "SELECT" => s"SELECT id, $e AS x FROM $diffIndex"
        case "WHERE"  => s"SELECT id FROM $diffIndex WHERE $e > 0"
        case "GROUP"  => s"SELECT g, $e AS x FROM $diffIndex GROUP BY g"
        case _        => s"SELECT g, $e AS x FROM $diffIndex GROUP BY g HAVING x > 0"
      }
      val expected = scopes.map { case (key, docs) =>
        key -> (for {
          first  <- a.value(docs)
          second <- b.value(docs)
        } yield
          if (spelling.mysql) documentedDiff(spelling.unit, second, first)
          else documentedDiff(spelling.unit, first, second))
      }.toMap
      DiffStatement(sql, venue, spelling.unit, expected)
    }
  }

  /** The rows of a statement through the gateway, or why there are none: a streamed result fails
    * while it is consumed, so the whole read is guarded.
    */
  private def diffRows(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Try {
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(QueryRows(rows, _))           => Right(rows)
        case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
        case ElasticSuccess(QueryStream(stream, _)) =>
          Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
        case ElasticSuccess(other) => Left(s"unexpected result $other")
        case ElasticFailure(error) => Left(error.message)
      }
    }.toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}").joinRight

  /** A whole number, from a `script_fields` array, a bucket value or NULL. */
  private def whole(value: Any): Either[String, Option[Long]] = value match {
    case null                       => Right(None)
    case s: Seq[_]                  => s.headOption.map(whole).getOrElse(Right(None))
    case c: java.util.Collection[_] => whole(c.toArray.toSeq)
    case other =>
      Try(BigDecimal(other.toString)).toOption match {
        case Some(n) if n.isWhole => Right(Some(n.toLong))
        case _                    => Left(s"not a whole number: $other")
      }
  }

  "the DATEDIFF family" should "answer its documented semantics for every spelling, unit, operand and venue" in {
    diffIndexLoaded
    val population = diffPopulation
    // Non-vacuity, computed over the material: every spelling in every venue over every operand
    // pair, every unit answering a negative, a zero and a positive somewhere, NULL in every venue,
    // and the filters keeping some and dropping some.
    population.size shouldBe diffSpellings.size * (25 + 16 + 16 + 16)
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))
    diffUnits.foreach { u =>
      val answers = population.filter(_.unit == u).flatMap(_.expected.values.flatten)
      withClue(s"$u: ") {
        Seq(answers.exists(_ < 0), answers.contains(0L), answers.exists(_ > 0)) shouldBe
        Seq(true, true, true)
      }
    }
    Seq("SELECT", "WHERE", "GROUP", "HAVING").foreach { venue =>
      withClue(s"$venue: ") {
        population.filter(_.venue == venue).exists(_.expected.values.exists(_.isEmpty)) shouldBe
        true
      }
    }
    population
      .filter(_.filters)
      .exists(st => st.kept.nonEmpty && st.kept != st.expected.keySet) shouldBe
    true

    val outcomes: Seq[(DiffStatement, Either[String, String])] = population.map { st =>
      st -> diffRows(st.sql).flatMap { rows =>
        val key = if (st.venue == "SELECT" || st.venue == "WHERE") "id" else "g"
        if (st.filters) {
          val actual = rows.map(_.getOrElse(key, "?").toString).toSet
          Right(
            if (actual == st.kept) ""
            else
              s"kept ${actual.toSeq.sorted.mkString(",")}, expected ${st.kept.toSeq.sorted.mkString(",")}"
          )
        } else {
          val values =
            rows.map(r => r.getOrElse(key, "?").toString -> whole(r.getOrElse("x", null)))
          values.collectFirst { case (_, Left(error)) => error } match {
            case Some(error) => Left(error)
            case None =>
              val actual = values.collect { case (k, Right(v)) => k -> v }.toMap
              Right(
                if (actual == st.expected) ""
                else
                  s"answered ${actual.toSeq.sortBy(_._1).mkString(" ")}, expected " +
                  st.expected.toSeq.sortBy(_._1).mkString(" ")
              )
          }
        }
      }
    }
    val wrong = outcomes.collect { case (st, Right(diff)) if diff.nonEmpty => s"[${st.sql}] $diff" }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    info(
      s"DATEDIFF family: ${population.size} statements; wrong: ${wrong.size}; errors: ${errors.size}"
    )
    wrong.foreach(info(_))
    errors.foreach(info(_))
    withClue(
      s"${wrong.size} wrong statements, ${errors.size} errors:\n" +
      (wrong.take(20) ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }
}
