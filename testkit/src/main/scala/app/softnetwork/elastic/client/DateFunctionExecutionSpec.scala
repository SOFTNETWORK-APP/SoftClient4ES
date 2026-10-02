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
    createdTables.foreach(t =>
      Try(Await.result(client.run(s"DROP TABLE IF EXISTS $t"), 60.seconds))
    )
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
  // The DATEDIFF family against the vendors' definitions, computed HERE, independently of the
  // engine:
  //
  //   - DIRECTION, by layout: `end - start`, where the dates first (`DATE_DIFF(a, b[, unit])`,
  //     `DATEDIFF(a, b[, unit])`, `TIMESTAMPDIFF(a, b[, unit])`) is `a - b` and the unit first
  //     (`DATE_DIFF(unit, a, b)`, `DATEDIFF(unit, a, b)`, `TIMESTAMPDIFF(unit, a, b)`) is `b - a`;
  //   - COUNTING, by name: `DATEDIFF` and `DATE_DIFF` count the calendar BOUNDARIES crossed, the
  //     difference of two UTC calendar-unit indices, a week starting on the ISO Monday;
  //     `TIMESTAMPDIFF` counts the whole units ELAPSED, truncated toward zero, a month (quarter,
  //     year) once the same day of month and time of day are reached;
  //   - a DATE operand is the start of its day (UTC); a string literal is the temporal it spells, a
  //     DATE alone or a TIMESTAMP (UTC unless it names a zone); NULL when an operand has no value.
  //
  // The fixture sits where the two countings and the two week starts part: one second (200 ms for
  // a second) across a year, quarter, month, ISO week, day, hour, minute and second end; a Saturday
  // to Sunday second, which is no ISO week; a span with more boundaries than whole units in every
  // unit; the vendors' own 1992-09-15 -> 1992-11-14 (two month boundaries, one month elapsed) and
  // 2003-02-01 -> 2003-05-01 (three months); the same instant; a DATE against a TIMESTAMP. The
  // population is every spelling and unit over every ORDERED operand pair -- so every edge in both
  // directions -- of a date column, a timestamp column, a DATE literal, two TIMESTAMP literals (one
  // with a zone and milliseconds) and a TIMESTAMP-typed one, and of a date and a timestamp aggregate,
  // in every venue: a SELECT item per document, a WHERE, a SELECT item per group and a HAVING
  // through that item's alias.
  // -----------------------------------------------------------------------------------------------

  private val diffIndex = "date_diff_semantics"

  @volatile private var diffIndexCreated = false

  private type DiffDoc = (String, String, Option[String], Option[String])

  /** (id, group, `d` a calendar date, `ts` an instant); `None`: the document lacks it. Group g01
    * has no value at all, g15 several documents with some, g18 a date and no instant; every other
    * group is one document, so its aggregates are that document's edge.
    */
  private val diffDocs: Seq[DiffDoc] = Seq(
    ("r01", "g01", None, None),
    ("r02", "g01", None, None),
    // one second before the date: a year (Tuesday to Wednesday), a quarter (Monday to Tuesday), a
    // month (Friday to Saturday), an ISO week (Sunday to Monday), a Sunday that starts no ISO week
    // (Saturday to Sunday), a day (Wednesday to Thursday)
    ("r03", "g03", Some("2025-01-01"), Some("2024-12-31T23:59:59Z")),
    ("r04", "g04", Some("2025-04-01"), Some("2025-03-31T23:59:59Z")),
    ("r05", "g05", Some("2025-02-01"), Some("2025-01-31T23:59:59Z")),
    ("r06", "g06", Some("2025-01-13"), Some("2025-01-12T23:59:59Z")),
    ("r07", "g07", Some("2025-01-12"), Some("2025-01-11T23:59:59Z")),
    ("r08", "g08", Some("2025-01-16"), Some("2025-01-15T23:59:59Z")),
    // one second before an hour ('2025-01-15 11:00:00'); 200 ms across a minute and a second
    // ('2025-01-15T10:01:00.100Z')
    ("r09", "g09", Some("2025-01-15"), Some("2025-01-15T10:59:59Z")),
    ("r10", "g10", None, Some("2025-01-15T10:00:59.900Z")),
    // more boundaries than whole units, in every unit from SECOND to YEAR
    ("r11", "g11", Some("2025-01-02"), Some("2023-11-30T22:59:30.900Z")),
    // DuckDB's date_diff example, then MySQL's TIMESTAMPDIFF one
    ("r12", "g12", Some("1992-09-15"), Some("1992-11-14T00:00:00Z")),
    ("r13", "g13", Some("2003-02-01"), Some("2003-05-01T00:00:00Z")),
    // the same instant
    ("r14", "g14", Some("2025-01-15"), Some("2025-01-15T00:00:00Z")),
    ("r15", "g15", Some("2025-06-15"), None),
    ("r16", "g15", None, Some("2025-06-14T12:00:00Z")),
    ("r17", "g15", Some("2024-01-31"), Some("2025-07-01T00:00:00Z")),
    ("r18", "g18", Some("2024-02-29"), None)
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
    constant("'2025-01-01'", OnDate(LocalDate.parse("2025-01-01"))),
    constant("'2025-01-15 11:00:00'", AtInstant(Instant.parse("2025-01-15T11:00:00Z"))),
    constant("'2025-01-15T10:01:00.100Z'", AtInstant(Instant.parse("2025-01-15T10:01:00.100Z"))),
    // a typed literal, whose time of day a calendar unit must not count
    constant("'2024-12-31 23:59:59'::TIMESTAMP", AtInstant(Instant.parse("2024-12-31T23:59:59Z")))
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

  /** The instant an operand denotes: a DATE is the start of its day, in UTC. */
  private def instantOf(value: DiffValue): Instant = value match {
    case OnDate(date)       => date.atStartOfDay(ZoneOffset.UTC).toInstant
    case AtInstant(instant) => instant
  }

  /** The UTC calendar unit an instant falls in, as an index: two indices differ by the boundaries
    * crossed between them. 1970-01-01, epoch day 0, is a Thursday, so `epochDay + 3` counts the
    * days from the Monday before it.
    */
  private def unitIndex(unit: String, value: DiffValue): Long = {
    val instant = instantOf(value)
    val utc = instant.atOffset(ZoneOffset.UTC)
    unit match {
      case "YEAR"    => utc.getYear.toLong
      case "QUARTER" => utc.getYear * 4L + (utc.getMonthValue - 1) / 3
      case "MONTH"   => utc.getYear * 12L + utc.getMonthValue - 1
      case "WEEK"    => Math.floorDiv(utc.toLocalDate.toEpochDay + 3, 7L)
      case "DAY"     => utc.toLocalDate.toEpochDay
      case "HOUR"    => Math.floorDiv(instant.toEpochMilli, 3600000L)
      case "MINUTE"  => Math.floorDiv(instant.toEpochMilli, 60000L)
      case "SECOND"  => Math.floorDiv(instant.toEpochMilli, 1000L)
    }
  }

  /** The week a SUNDAY-start count would put an instant in (1970-01-04 is a Sunday). */
  private def sundayWeek(value: DiffValue): Long =
    Math.floorDiv(instantOf(value).atOffset(ZoneOffset.UTC).toLocalDate.toEpochDay + 4, 7L)

  /** The calendar boundaries crossed from `start` to `end`. */
  private def boundaries(unit: String, start: DiffValue, end: DiffValue): Long =
    unitIndex(unit, end) - unitIndex(unit, start)

  /** The whole months from `from` to `to`, `from` not after `to`: a month counts once `to` reaches
    * `from`'s day of month and time of day.
    */
  private def wholeMonths(from: Instant, to: Instant): Long = {
    val (a, b) = (from.atOffset(ZoneOffset.UTC), to.atOffset(ZoneOffset.UTC))
    val months = (b.getYear * 12L + b.getMonthValue) - (a.getYear * 12L + a.getMonthValue)
    val reached = b.getDayOfMonth > a.getDayOfMonth ||
      (b.getDayOfMonth == a.getDayOfMonth && !b.toLocalTime.isBefore(a.toLocalTime))
    if (reached) months else months - 1
  }

  /** The whole units elapsed from `start` to `end`, truncated toward zero. */
  private def elapsed(unit: String, start: DiffValue, end: DiffValue): Long = {
    val (from, to) = (instantOf(start), instantOf(end))
    if (to.isBefore(from)) -elapsed(unit, end, start)
    else {
      val millis = to.toEpochMilli - from.toEpochMilli
      unit match {
        case "YEAR"    => wholeMonths(from, to) / 12L
        case "QUARTER" => wholeMonths(from, to) / 3L
        case "MONTH"   => wholeMonths(from, to)
        case "WEEK"    => millis / 604800000L
        case "DAY"     => millis / 86400000L
        case "HOUR"    => millis / 3600000L
        case "MINUTE"  => millis / 60000L
        case "SECOND"  => millis / 1000L
      }
    }
  }

  /** A spelling of the family: its SQL over two operands in WRITTEN order, its unit, its layout
    * (the dates first: `first - second`; the unit first: `last - middle`) and its counting.
    */
  private final case class DiffSpelling(unit: String, datesFirst: Boolean, countsElapsed: Boolean)(
    val sql: (String, String) => String
  )

  /** What a spelling answers for its two operands, in written order. */
  private def answer(spelling: DiffSpelling, first: DiffValue, second: DiffValue): Long = {
    val (start, end) = if (spelling.datesFirst) (second, first) else (first, second)
    if (spelling.countsElapsed) elapsed(spelling.unit, start, end)
    else boundaries(spelling.unit, start, end)
  }

  private val diffUnits: Seq[String] =
    Seq("YEAR", "QUARTER", "MONTH", "WEEK", "DAY", "HOUR", "MINUTE", "SECOND")

  private val diffSpellings: Seq[DiffSpelling] = diffUnits.flatMap { u =>
    Seq(
      DiffSpelling(u, datesFirst = true, countsElapsed = false)((a, b) => s"DATE_DIFF($a, $b, $u)"),
      DiffSpelling(u, datesFirst = true, countsElapsed = false)((a, b) => s"DATEDIFF($a, $b, $u)"),
      DiffSpelling(u, datesFirst = false, countsElapsed = false)((a, b) =>
        s"DATE_DIFF($u, $a, $b)"
      ),
      DiffSpelling(u, datesFirst = false, countsElapsed = false)((a, b) => s"DATEDIFF($u, $a, $b)"),
      DiffSpelling(u, datesFirst = false, countsElapsed = true)((a, b) =>
        s"TIMESTAMPDIFF($u, $a, $b)"
      )
    )
  } ++ Seq(
    DiffSpelling("DAY", datesFirst = true, countsElapsed = false)((a, b) => s"DATE_DIFF($a, $b)"),
    DiffSpelling("DAY", datesFirst = true, countsElapsed = false)((a, b) => s"DATEDIFF($a, $b)")
  ) ++ diffUnits.map { u =>
    // No vendor writes `TIMESTAMPDIFF` with the dates first, and it is accepted all the same: the
    // two rules give it its meaning, `first - second` counted in whole units elapsed.
    DiffSpelling(u, datesFirst = true, countsElapsed = true)((a, b) => s"TIMESTAMPDIFF($a, $b, $u)")
  } :+ DiffSpelling("DAY", datesFirst = true, countsElapsed = true)((a, b) =>
    s"TIMESTAMPDIFF($a, $b)"
  )

  /** A statement, the venue it runs in, and the answer for each document or group. */
  private final case class DiffStatement(
    sql: String,
    venue: String,
    unit: String,
    expected: Map[String, Option[Long]]
  ) {
    def filters: Boolean = venue == "WHERE" || venue == "HAVING"
    def kept: Set[String] = expected.collect { case (k, Some(v)) if v > 0 => k }.toSet
  }

  private val diffPerDocument: Seq[(String, Seq[DiffDoc])] = diffDocs.map(doc => doc._1 -> Seq(doc))

  private val diffPerGroup: Seq[(String, Seq[DiffDoc])] = diffDocs.groupBy(_._2).toSeq.sortBy(_._1)

  private val diffRowPairs: Seq[(DiffOperand, DiffOperand)] =
    for (a <- diffRowOperands; b <- diffRowOperands) yield (a, b)

  private val diffGroupPairs: Seq[(DiffOperand, DiffOperand)] =
    for (a <- diffGroupOperands; b <- diffGroupOperands if a.aggregate || b.aggregate)
      yield (a, b)

  private lazy val diffPopulation: Seq[DiffStatement] = {
    // A WHERE over two literals reads no column: it is a constant predicate, rendered as a bare
    // comparison of the function's boxed result, which fails to compile whatever the function is
    // (`WHERE ABS(-3) > 0` too). It is not a DATEDIFF question, so it is left out.
    val wherePairs = diffRowPairs.filterNot { case (a, b) =>
      diffLiterals.contains(a) && diffLiterals.contains(b)
    }
    for {
      spelling <- diffSpellings
      (venue, pairs, scopes) <- Seq(
        ("SELECT", diffRowPairs, diffPerDocument),
        ("WHERE", wherePairs, diffPerDocument),
        ("GROUP", diffGroupPairs, diffPerGroup),
        ("HAVING", diffGroupPairs, diffPerGroup)
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
        } yield answer(spelling, first, second))
      }.toMap
      DiffStatement(sql, venue, spelling.unit, expected)
    }
  }

  /** Every pair of operand values the population computes over, in written order. */
  private lazy val diffValuePairs: Seq[(DiffValue, DiffValue)] =
    (for {
      (pairs, scopes) <- Seq(diffRowPairs -> diffPerDocument, diffGroupPairs -> diffPerGroup)
      (a, b)          <- pairs
      (_, docs)       <- scopes
      first           <- a.value(docs)
      second          <- b.value(docs)
    } yield (first, second)).distinct

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

  "the DATEDIFF family" should "answer the vendors' definitions for every spelling, unit, edge, operand and venue" in {
    diffIndexLoaded
    val population = diffPopulation
    // Non-vacuity, computed over the material: every spelling in every venue over every operand
    // pair, every unit answering a negative, a zero and a positive somewhere, NULL in every venue,
    // and the filters keeping some and dropping some.
    population.size shouldBe diffSpellings.size * (36 + 20 + 20 + 20)
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
    // ...and the edges are there, in both directions: in every unit a boundary crossed with no
    // whole unit elapsed, and more boundaries than whole units; a Sunday that a Sunday-start week
    // would count and an ISO week does not; the same instant written as a DATE and a TIMESTAMP.
    val pairs = diffValuePairs
    diffUnits.foreach { u =>
      withClue(s"$u edges: ") {
        Seq(
          pairs.exists { case (x, y) => boundaries(u, x, y) == 1 && elapsed(u, x, y) == 0 },
          pairs.exists { case (x, y) => boundaries(u, x, y) == -1 && elapsed(u, x, y) == 0 },
          pairs.exists { case (x, y) =>
            elapsed(u, x, y) > 0 && boundaries(u, x, y) > elapsed(u, x, y)
          }
        ) shouldBe Seq(true, true, true)
      }
    }
    pairs.exists { case (x, y) =>
      boundaries("WEEK", x, y) == 0 && sundayWeek(y) - sundayWeek(x) == 1
    } shouldBe true
    pairs.exists { case (x, y) =>
      x.isInstanceOf[OnDate] && y.isInstanceOf[AtInstant] && instantOf(x) == instantOf(y)
    } shouldBe true

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

  // -----------------------------------------------------------------------------------------------
  // TIMESTAMPDIFF over an operand of ANOTHER runtime type. The function compares instants, and an
  // operand's declared type does not say which Java type it holds: `'…'::DATETIME` is a
  // `LocalDateTime`, `CURRENT_DATE` and `ts::DATE` a `LocalDate`, `NOW()` a `ZonedDateTime`, and
  // `COALESCE(ts, CURRENT_DATE)` or a CASE over `CURRENT_DATE` hold one type on one row and another
  // on the next. Each is the instant it denotes, in UTC (a DATE is the start of its day), in every
  // venue, for every unit and in both directions. The clock is read where the statement reads it:
  // a query's when its request is built, so the instants that bracket the call bound it; a computed
  // column's on the node at ingest, bounded the same way with a margin for the two clocks.
  // -----------------------------------------------------------------------------------------------

  /** The tables the DDL tests below create, dropped after the suite. */
  @volatile private var createdTables: List[String] = Nil

  /** An operand whose value may read the clock: over a document or a group, at `now`. */
  private final case class ClockOperand(sql: String, aggregate: Boolean)(
    val value: (Seq[DiffDoc], Instant) => Option[DiffValue]
  )

  private def utcDate(instant: Instant): LocalDate = instant.atOffset(ZoneOffset.UTC).toLocalDate

  private def clockFree(operand: DiffOperand): ClockOperand =
    ClockOperand(operand.sql, operand.aggregate)((docs, _) => operand.value(docs))

  private val datetimeLiteral: ClockOperand =
    ClockOperand("'2025-01-20 11:00:00'::DATETIME", aggregate = false)((_, _) =>
      Some(AtInstant(Instant.parse("2025-01-20T11:00:00Z")))
    )

  /** A `LocalDate`, read without the clock. */
  private val dateLiteral: ClockOperand =
    ClockOperand("'2025-06-01'::DATE", aggregate = false)((_, _) =>
      Some(OnDate(LocalDate.parse("2025-06-01")))
    )

  private val currentDate: ClockOperand =
    ClockOperand("CURRENT_DATE", aggregate = false)((_, now) => Some(OnDate(utcDate(now))))

  private val currentInstant: ClockOperand =
    ClockOperand("NOW()", aggregate = false)((_, now) => Some(AtInstant(now)))

  /** A `ZonedDateTime` on a document with `ts`, a `LocalDate` on one without. */
  private val coalesced: ClockOperand =
    ClockOperand("COALESCE(ts, CURRENT_DATE)", aggregate = false)((docs, now) =>
      Some(docs.head._4.map(x => AtInstant(Instant.parse(x))).getOrElse(OnDate(utcDate(now))))
    )

  /** A `LocalDate`: the clock's on one document, a literal's on the others. */
  private val chosen: ClockOperand =
    ClockOperand(
      "CASE WHEN id = 'r03' THEN CURRENT_DATE ELSE '2025-06-01'::DATE END",
      aggregate = false
    )((docs, now) =>
      Some(OnDate(if (docs.head._1 == "r03") utcDate(now) else LocalDate.parse("2025-06-01")))
    )

  /** A column narrowed to its calendar date: a `LocalDate`. */
  private val castToDate: ClockOperand =
    ClockOperand("ts::DATE", aggregate = false)((docs, _) =>
      docs.head._4.map(x => OnDate(utcDate(Instant.parse(x))))
    )

  private val runtimeColumns: Seq[ClockOperand] =
    diffRowOperands.filter(o => o.sql == "d" || o.sql == "ts").map(clockFree)

  private val runtimeAggregates: Seq[ClockOperand] =
    diffGroupOperands.filter(_.aggregate).map(clockFree)

  /** Each anchor against each operand, both ways round. */
  private def runtimePairs(
    anchors: Seq[ClockOperand],
    operands: Seq[ClockOperand]
  ): Seq[(ClockOperand, ClockOperand)] =
    for {
      anchor  <- anchors
      operand <- operands
      pair    <- Seq(anchor -> operand, operand -> anchor)
    } yield pair

  /** A statement, the venue it runs in, and the answer for each document or group at `now`. */
  private final case class RuntimeStatement(sql: String, venue: String, unit: String)(
    val expected: Instant => Map[String, Option[Long]]
  )

  private lazy val runtimePopulation: Seq[RuntimeStatement] = {
    // Per group the operands are constants, whatever function reads them: a COALESCE over an
    // aggregate reads its raw metric, a CASE over one does not parse, and a `bucket_script` is
    // sent without its script parameters, so the request clock (`params.__now__`) is null there
    // and `CURRENT_DATE` and `NOW()` cannot be read.
    val rowPairs = runtimePairs(
      runtimeColumns,
      Seq(datetimeLiteral, currentDate, currentInstant, coalesced, chosen, castToDate)
    )
    val groupPairs = runtimePairs(runtimeAggregates, Seq(datetimeLiteral, dateLiteral))
    for {
      unit <- diffUnits
      (venue, pairs, scopes) <- Seq(
        ("SELECT", rowPairs, diffPerDocument),
        ("WHERE", rowPairs, diffPerDocument),
        ("GROUP", groupPairs, diffPerGroup),
        ("HAVING", groupPairs, diffPerGroup)
      )
      (a, b) <- pairs
    } yield {
      val e = s"TIMESTAMPDIFF($unit, ${a.sql}, ${b.sql})"
      val sql = venue match {
        case "SELECT" => s"SELECT id, $e AS x FROM $diffIndex"
        case "WHERE"  => s"SELECT id FROM $diffIndex WHERE $e > 0"
        case "GROUP"  => s"SELECT g, $e AS x FROM $diffIndex GROUP BY g"
        case _        => s"SELECT g, $e AS x FROM $diffIndex GROUP BY g HAVING x > 0"
      }
      RuntimeStatement(sql, venue, unit)(now =>
        scopes.map { case (key, docs) =>
          key -> (for {
            start <- a.value(docs, now)
            end   <- b.value(docs, now)
          } yield elapsed(unit, start, end))
        }.toMap
      )
    }
  }

  /** The milliseconds clock the engine reads, as an instant. */
  private def clockNow(): Instant = Instant.ofEpochMilli(System.currentTimeMillis())

  /** Does `actual` lie between the answers at the two instants that bracket the statement? An
    * answer that does not read the clock is the same at both.
    */
  private def bracketed(low: Option[Long], high: Option[Long], actual: Option[Long]): Boolean =
    (low, high, actual) match {
      case (Some(l), Some(h), Some(v)) => v >= math.min(l, h) && v <= math.max(l, h)
      case (None, None, None)          => true
      case _                           => false
    }

  "TIMESTAMPDIFF" should "compare instants whatever the runtime type of an operand, in every venue" in {
    diffIndexLoaded
    val population = runtimePopulation
    population.size shouldBe diffUnits.size * (24 + 24 + 8 + 8)
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))
    // Non-vacuity: in every venue a non-zero answer and a NULL, and the filters keep some rows and
    // drop others.
    val probe = clockNow()
    Seq("SELECT", "WHERE", "GROUP", "HAVING").foreach { venue =>
      val answers = population.filter(_.venue == venue).flatMap(_.expected(probe).values)
      withClue(s"$venue: ") {
        Seq(
          answers.exists(_.exists(_ < 0)),
          answers.exists(_.exists(_ > 0)),
          answers.contains(None)
        ) shouldBe
        Seq(true, true, true)
      }
    }

    val outcomes: Seq[(RuntimeStatement, Either[String, String])] = population.map { st =>
      val before = clockNow()
      val result = diffRows(st.sql)
      val after = clockNow()
      val (low, high) = (st.expected(before), st.expected(after))
      st -> result.flatMap { rows =>
        val key = if (st.venue == "SELECT" || st.venue == "WHERE") "id" else "g"
        if (st.venue == "WHERE" || st.venue == "HAVING") {
          val actual = rows.map(_.getOrElse(key, "?").toString).toSet
          val misplaced = (low.keySet.filter { k =>
            val kept = actual.contains(k)
            kept != low(k).exists(_ > 0) && kept != high(k).exists(_ > 0)
          } ++ (actual -- low.keySet)).toSeq.sorted
          Right(
            if (misplaced.isEmpty) ""
            else s"kept ${actual.toSeq.sorted.mkString(",")}, misplaced ${misplaced.mkString(",")}"
          )
        } else {
          val values =
            rows.map(r => r.getOrElse(key, "?").toString -> whole(r.getOrElse("x", null)))
          values.collectFirst { case (_, Left(error)) => error } match {
            case Some(error) => Left(error)
            case None =>
              val actual = values.collect { case (k, Right(v)) => k -> v }.toMap
              val off = (low.keySet ++ actual.keySet).filterNot { k =>
                bracketed(
                  low.getOrElse(k, None),
                  high.getOrElse(k, None),
                  actual.getOrElse(k, None)
                )
              }
              Right(
                if (off.isEmpty) ""
                else
                  s"answered ${actual.toSeq.sortBy(_._1).mkString(" ")}, expected " +
                  low.toSeq.sortBy(_._1).mkString(" ")
              )
          }
        }
      }
    }
    val wrong = outcomes.collect { case (st, Right(diff)) if diff.nonEmpty => s"[${st.sql}] $diff" }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    info(
      s"TIMESTAMPDIFF runtime types: ${population.size} statements; wrong: ${wrong.size}; " +
      s"errors: ${errors.size}"
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

  private val runtimeTable = "timestampdiff_runtime_types"

  it should "compare instants whatever the runtime type of an operand, in a computed column" in {
    // In an ingest script a COALESCE hands on the RAW JSON value of a column it reads, and a cast
    // column is parsed a second time once parsed, whatever function holds either: neither
    // `COALESCE(ts, CURRENT_DATE)` nor `ts::DATE` is one of the operands here.
    val pairs =
      runtimePairs(runtimeColumns, Seq(datetimeLiteral, currentDate, currentInstant, chosen))
    val columns: Seq[(String, String, ClockOperand, ClockOperand)] = for {
      (unit, u)   <- diffUnits.zipWithIndex
      ((a, b), p) <- pairs.zipWithIndex
    } yield (s"c${u}_$p", unit, a, b)
    columns.size shouldBe diffUnits.size * 16
    // a document missing a column leaves the computed column absent, not NULL: both are present
    val docs = diffDocs.filter { case (_, _, d, ts) => d.isDefined && ts.isDefined }
    docs.map(_._1) should contain("r03")
    val ddl =
      s"CREATE TABLE $runtimeTable (id KEYWORD, d DATE, ts TIMESTAMP, " +
      columns
        .map { case (c, unit, a, b) =>
          s"$c BIGINT SCRIPT AS (TIMESTAMPDIFF($unit, ${a.sql}, ${b.sql}))"
        }
        .mkString(", ") + ", PRIMARY KEY (id))"
    Await.result(client.run(ddl), 120.seconds) match {
      case ElasticSuccess(_)     => createdTables = runtimeTable :: createdTables
      case ElasticFailure(error) => fail(s"CREATE TABLE $runtimeTable failed: ${error.message}")
    }
    val values = docs.map { case (id, _, d, ts) => s"('$id', '${d.get}', '${ts.get}')" }
    // the node's clock against this JVM's: two seconds either side
    val before = clockNow().minusSeconds(2)
    Await.result(
      client.run(s"INSERT INTO $runtimeTable (id, d, ts) VALUES ${values.mkString(", ")}"),
      120.seconds
    ) match {
      case ElasticSuccess(dml: DmlResult) => dml.inserted shouldBe docs.size.toLong
      case other                          => fail(s"INSERT INTO $runtimeTable: $other")
    }
    val after = clockNow().plusSeconds(2)
    client.refresh(runtimeTable)
    val stored: Map[String, ListMap[String, Any]] = diffRows(s"SELECT * FROM $runtimeTable") match {
      case Right(rows) => rows.map(r => r.getOrElse("id", "?").toString -> r).toMap
      case Left(error) => fail(s"SELECT * FROM $runtimeTable: $error")
    }
    stored.keySet shouldBe docs.map(_._1).toSet
    val wrong = for {
      (column, unit, a, b) <- columns
      doc                  <- docs
      expectedAt = (now: Instant) =>
        for {
          start <- a.value(Seq(doc), now)
          end   <- b.value(Seq(doc), now)
        } yield elapsed(unit, start, end)
      actual = stored(doc._1).get(column).flatMap(v => whole(v).toOption.flatten)
      if !bracketed(expectedAt(before), expectedAt(after), actual)
    } yield s"$column = TIMESTAMPDIFF($unit, ${a.sql}, ${b.sql}) on ${doc._1}: stored " +
    s"${actual.getOrElse("nothing")}, expected ${expectedAt(before).getOrElse("NULL")}"
    info(
      s"TIMESTAMPDIFF runtime types, computed columns: ${columns.size} columns x ${docs.size} " +
      s"documents; wrong: ${wrong.size}"
    )
    wrong.foreach(info(_))
    withClue(s"${wrong.size} wrong:\n" + wrong.take(20).mkString("\n") + "\n") {
      wrong shouldBe empty
    }
  }

  // -----------------------------------------------------------------------------------------------
  // `DATE_TRUNC(…, WEEK)` is the ISO Monday that starts the week, at 00:00 UTC, in every venue: a
  // SELECT item over a TIMESTAMP and a DATE column, a WHERE, a GROUP BY (Elasticsearch's calendar
  // `date_histogram`) and a computed column. Each weekday, with times of day, across a year end.
  // -----------------------------------------------------------------------------------------------

  private val weekTable = "date_trunc_week"

  private val weekDocs: Seq[(String, String)] = Seq(
    "w0" -> "2024-12-29T23:59:59Z", // a Sunday: the week before
    "w1" -> "2024-12-30T08:00:00Z", // Monday
    "w2" -> "2024-12-31T23:59:59Z", // Tuesday, the last second of 2024
    "w3" -> "2025-01-01T00:00:00Z", // Wednesday, the first second of 2025
    "w4" -> "2025-01-02T12:30:00Z", // Thursday
    "w5" -> "2025-01-03T18:00:00Z", // Friday
    "w6" -> "2025-01-04T00:00:01Z", // Saturday
    "w7" -> "2025-01-05T23:59:59Z", // Sunday
    "w8" -> "2025-01-06T00:00:00Z" // Monday: the week after
  )

  /** The ISO Monday of an instant's UTC date: 1970-01-01, epoch day 0, is a Thursday. */
  private def isoMonday(iso: String): LocalDate = {
    val day = utcDate(Instant.parse(iso)).toEpochDay
    LocalDate.ofEpochDay(day - Math.floorMod(day + 3, 7L))
  }

  private val Midnight = """(\d{4}-\d{2}-\d{2})(T00:00(:00(\.0+)?)?(Z|\+00:00)?)?""".r

  /** A truncated value as its date when it is that date at 00:00 UTC; anything else as it came. */
  private def midnight(value: Any): String =
    scalar(value) match {
      case Midnight(date, _*) => date
      case other              => other
    }

  "DATE_TRUNC(…, WEEK)" should "truncate to the ISO Monday, 00:00 UTC, in every venue" in {
    val expected: Map[String, String] =
      weekDocs.map { case (id, ts) => id -> isoMonday(ts).toString }.toMap
    // every weekday, both years, three Mondays
    weekDocs.map { case (_, ts) => utcDate(Instant.parse(ts)).getDayOfWeek }.toSet.size shouldBe 7
    expected.values.toSet shouldBe Set("2024-12-23", "2024-12-30", "2025-01-06")

    val ddl =
      s"CREATE TABLE $weekTable (id KEYWORD, ts TIMESTAMP, d DATE, " +
      "wt TIMESTAMP SCRIPT AS (DATE_TRUNC(ts, WEEK)), wd DATE SCRIPT AS (DATE_TRUNC(d, WEEK)), " +
      "PRIMARY KEY (id))"
    Await.result(client.run(ddl), 60.seconds) match {
      case ElasticSuccess(_)     => createdTables = weekTable :: createdTables
      case ElasticFailure(error) => fail(s"CREATE TABLE $weekTable failed: ${error.message}")
    }
    val values = weekDocs.map { case (id, ts) => s"('$id', '$ts', '${ts.take(10)}')" }
    Await.result(
      client.run(s"INSERT INTO $weekTable (id, ts, d) VALUES ${values.mkString(", ")}"),
      60.seconds
    ) match {
      case ElasticSuccess(dml: DmlResult) => dml.inserted shouldBe weekDocs.size.toLong
      case other                          => fail(s"INSERT INTO $weekTable: $other")
    }
    client.refresh(weekTable)

    // Every venue is read before anything is asserted, so a failure names each venue that differs.
    def byId(sql: String, column: String): Either[String, Map[String, String]] =
      diffRows(sql).map(
        _.map(r => r.getOrElse("id", "?").toString -> midnight(r.getOrElse(column, null))).toMap
      )

    val mondays = expected.values.toSeq.distinct.sorted
    val counts: Map[String, Either[String, Option[Long]]] =
      expected.values.groupBy(identity).map { case (m, ms) => m -> Right(Some(ms.size.toLong)) }
    val verdicts: Seq[(String, Boolean, String)] =
      Seq(
        s"SELECT id, DATE_TRUNC(ts, WEEK) AS w FROM $weekTable" -> "w",
        s"SELECT id, DATE_TRUNC(WEEK, ts) AS w FROM $weekTable" -> "w",
        s"SELECT id, DATE_TRUNC(d, WEEK) AS w FROM $weekTable"  -> "w",
        s"SELECT id, wt FROM $weekTable"                        -> "wt",
        s"SELECT id, wd FROM $weekTable"                        -> "wd"
      ).map { case (sql, column) =>
        val got = byId(sql, column)
        (sql, got == Right(expected), got.fold(identity, _.toSeq.sorted.mkString(" ")))
      } ++
      // a WHERE keeps each document under its own Monday and under no other
      (for (column <- Seq("ts", "d"); monday <- mondays) yield {
        val sql =
          s"SELECT id FROM $weekTable WHERE DATE_TRUNC($column, WEEK) = CAST('$monday' AS DATE)"
        val kept = diffRows(sql).map(_.map(_.getOrElse("id", "?").toString).toSet)
        val want = expected.collect { case (id, m) if m == monday => id }.toSet
        (sql, kept == Right(want), kept.fold(identity, _.toSeq.sorted.mkString(",")))
      }) ++
      // a GROUP BY buckets on the same Mondays
      Seq("ts", "d").map { column =>
        val sql =
          s"SELECT DATE_TRUNC($column, WEEK) AS w, COUNT(*) AS c FROM $weekTable " +
          s"GROUP BY DATE_TRUNC($column, WEEK)"
        val buckets = diffRows(sql).map(
          _.map(r => midnight(r.getOrElse("w", null)) -> whole(r.getOrElse("c", null))).toMap
        )
        (sql, buckets == Right(counts), buckets.fold(identity, _.toSeq.sortBy(_._1).mkString(" ")))
      }
    verdicts.foreach { case (sql, ok, got) =>
      info(s"${if (ok) "RIGHT" else "WRONG"} [$sql] $got")
    }
    val wrong = verdicts.collect { case (sql, false, got) => s"[$sql] $got" }
    withClue(
      s"expected ${expected.toSeq.sorted.mkString(" ")}; ${wrong.size} venues differ:\n" +
      wrong.mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
    }
  }
}
