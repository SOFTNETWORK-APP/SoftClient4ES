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

import java.time.temporal.ChronoUnit
import java.time.{Instant, LocalDate, LocalDateTime, OffsetDateTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** Date arithmetic EXECUTES and answers what SQL engines answer, in every venue a script runs in: a
  * row SELECT, a WHERE, a per-group SELECT (`bucket_script`), a HAVING over its alias
  * (`bucket_selector`) and a computed column (`SCRIPT AS`, an ingest processor) -- the STORED
  * value.
  *
  * The rules (the lead's ruling of 2026-10-04): DATE - DATE is the calendar days between the two; a
  * difference with a TIMESTAMP on either side is the milliseconds between, in days; a DATE moved by
  * a whole number of days is a DATE, by a fractional one an instant whose fraction is a time of
  * day; a TIMESTAMP moved by any number of days is a TIMESTAMP; a NULL operand gives NULL; and the
  * other combinations are refused by name -- once the column types are known, before anything runs
  * (the lead's ruling of 2026-10-05: no type is refused when a statement is parsed).
  *
  * Every expected value is computed HERE, with `java.time`, from the fixture -- never read from the
  * engine. The fixture holds a group with two dates, groups with one, and a non-empty group where
  * the date columns are absent; DATE and TIMESTAMP columns, the timestamps carrying a time of day;
  * negative differences; January 31 to February 1 (1 day) and 2024-02-28 to 2024-03-01 (2 days,
  * across a leap day); dates before 1970; dates written as epoch-millisecond NUMBERS; and rows
  * where an operand is NULL. The computed-column table also stores a DATE column written as a
  * date-TIME string, which Elasticsearch accepts.
  *
  * Also measured here, each in its own test: an index created by a bulk load with no declaration (a
  * `date` keeps its time), an index PATTERN (it answers like its index), the request clock in a
  * per-group calculation, casts over arithmetic (NULL-safe, applied), and every type rule that
  * waits for the column types (refused by name, never at parse; with no schema, never by core).
  */
trait DateArithmeticSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "date_arithmetic"

  private val computedTable = "date_arithmetic_computed"

  private val rawIndex = "date_arithmetic_raw"

  private val patternA = "date_arithmetic_pat_a"

  private val patternB = "date_arithmetic_pat_b"

  private val disagreeA = "date_arithmetic_dis_a"

  private val disagreeB = "date_arithmetic_dis_b"

  /** The documents of [[table]] with every `d` written as a date-TIME string of its UTC day. */
  private val dateTimeTable = "date_arithmetic_dt"

  /** The comparison rule's fixture (`"the ONE comparison rule"`). */
  private val comparisonTable = "date_arithmetic_cmp"

  /** A quoted number beside an INT, a BIGINT and a DOUBLE (`"a quoted number"`). */
  private val numberTable = "date_arithmetic_num"

  /** An index created by a bulk load, then altered through elasticsql. */
  private val alteredRawIndex = "date_arithmetic_raw_altered"

  /** A table elasticsql created, to which a bulk load later adds a `date` field. */
  private val grownTable = "date_arithmetic_grown"

  /** The indices of the pattern whose cached schema DDL must refresh. */
  private val refreshedA = "date_arithmetic_rf_a"
  private val refreshedB = "date_arithmetic_rf_b"

  /** NULLIF with a date or a timestamp literal (`"NULLIF with a date or a timestamp literal"`). */
  private val nullifTable = "date_arithmetic_nullif"

  /** A simple CASE over a BOOLEAN (`"a simple CASE over a BOOLEAN"`). */
  private val booleanTable = "date_arithmetic_bool"

  /** A BOOLEAN column as a CASE condition (`"a CASE WHEN over a BOOLEAN column"`). */
  private val flagTable = "date_arithmetic_flag"

  private val DayMillis: Long = 86400000L

  /** One document: `None` is a column the document does not carry.
    *
    * @param epoch
    *   the dates and timestamps are written as epoch-millisecond NUMBERS
    * @param dText
    *   `d` is written as this text -- a date-time string in a DATE column -- while `d` is the UTC
    *   day it falls in
    */
  private final case class Row(
    id: String,
    g: String,
    d: Option[LocalDate],
    d2: Option[LocalDate],
    ts: Option[Instant],
    ts2: Option[Instant],
    n: Option[Int],
    x: Option[Double],
    epoch: Boolean = false,
    dText: Option[String] = None
  )

  private def date(s: String): Option[LocalDate] = Some(LocalDate.parse(s))
  private def instant(s: String): Option[Instant] = Some(Instant.parse(s))

  private val rows: Seq[Row] = Seq(
    Row(
      "r1",
      "g1",
      date("2024-01-31"),
      date("2024-02-01"),
      instant("2024-01-31T10:30:00Z"),
      instant("2024-02-01T22:15:00Z"),
      Some(3),
      Some(0.25)
    ),
    Row(
      "r2",
      "g1",
      date("2024-03-01"),
      date("2024-02-28"),
      instant("2024-03-01T06:00:00Z"),
      instant("2024-02-28T18:45:30.500Z"),
      Some(-2),
      Some(1.5)
    ),
    Row(
      "r3",
      "g2",
      date("2023-12-25"),
      None,
      instant("2023-12-25T23:59:59.999Z"),
      None,
      None,
      None
    ),
    Row("r4", "g3", None, None, None, None, Some(1), Some(2.0)),
    Row(
      "r5",
      "g4",
      date("2024-02-29"),
      date("2024-03-01"),
      instant("2024-02-29T12:00:00Z"),
      instant("2024-03-01T00:00:00Z"),
      Some(10),
      Some(-0.5)
    ),
    // before 1970: negative epochs, whose days must FLOOR, not truncate toward zero
    Row(
      "r6",
      "g5",
      date("1969-12-31"),
      date("1960-02-29"),
      instant("1969-12-31T23:00:00Z"),
      instant("1970-01-01T01:00:00Z"),
      Some(-1),
      Some(-0.25)
    ),
    // written as epoch-millisecond NUMBERS, a negative one included
    Row(
      "r7",
      "g6",
      date("2024-01-31"),
      date("1969-12-31"),
      instant("2024-01-31T10:30:00Z"),
      instant("1969-12-31T23:00:00Z"),
      Some(2),
      Some(0.5),
      epoch = true
    )
  )

  /** Stored by the computed-column table only: a DATE column holding a date-TIME string, which
    * Elasticsearch accepts into a `date` field and which a DATE operand reads as its UTC day.
    */
  private val computedOnlyRows: Seq[Row] = Seq(
    Row(
      "r8",
      "g7",
      date("2024-01-31"),
      date("2024-02-01"),
      instant("2024-01-31T22:30:00Z"),
      instant("2024-02-01T01:30:00Z"),
      Some(1),
      Some(1.0),
      dText = Some("2024-01-31T22:30:00Z")
    )
  )

  private val columns =
    "id KEYWORD, g KEYWORD, d DATE, d2 DATE, ts TIMESTAMP, ts2 TIMESTAMP, n INT, x DOUBLE"

  /** The computed columns, each with the type the rules give its expression. */
  private val computed: Seq[(String, String, String)] = Seq(
    ("c_dd", "BIGINT", "d - d2"),
    ("c_dd2", "BIGINT", "d2 - d"),
    ("c_tt", "DOUBLE", "ts - ts2"),
    ("c_dt", "DOUBLE", "d - ts"),
    ("c_dp1", "DATE", "d + 1"),
    ("c_dm1", "DATE", "d - 1"),
    ("c_nd", "DATE", "n + d"),
    ("c_dp15", "TIMESTAMP", "d + 1.5"),
    ("c_dx", "TIMESTAMP", "d + x"),
    ("c_tp1", "TIMESTAMP", "ts + 1"),
    ("c_tm15", "TIMESTAMP", "ts - 1.5"),
    ("c_null", "DATE", "d + NULL"),
    // ---- the fix round: operands that are not bare columns, a numeric string, casts ----
    ("c_coal", "BIGINT", "COALESCE(d, d2) - d2"),
    ("c_nullif", "BIGINT", "NULLIF(d, d2) - d2"),
    ("c_case", "BIGINT", "CASE WHEN n > 0 THEN d ELSE d2 END - d"),
    ("c_castd", "BIGINT", "CAST(ts AS DATE) - d"),
    ("c_str", "DATE", "d + '1'"),
    ("c_tsbig", "BIGINT", "(ts - ts2)::BIGINT"),
    ("c_nxbig", "BIGINT", "(n + x)::BIGINT"),
    ("c_xint", "INT", "(x * 2)::INT"),
    ("c_ddts", "TIMESTAMP", "(d - d2)::TIMESTAMP"),
    ("c_tsts", "TIMESTAMP", "(ts - ts2)::TIMESTAMP"),
    // ---- fix round 2: GREATEST / LEAST over dates, a number cast to a TIMESTAMP ----
    ("c_gr", "DATE", "GREATEST(d, d2)"),
    ("c_le", "TIMESTAMP", "LEAST(d, ts)"),
    ("c_grd", "BIGINT", "GREATEST(d, d2) - d"),
    ("c_nts", "TIMESTAMP", "CAST(n AS TIMESTAMP)"),
    ("c_xts", "TIMESTAMP", "CAST(x AS TIMESTAMP)")
  )

  private def json(r: Row): String = {
    def dateValue(v: LocalDate): String =
      if (r.epoch) s"${start(v).toEpochMilli}" else s""""$v""""
    def instantValue(v: Instant): String = if (r.epoch) s"${v.toEpochMilli}" else s""""$v""""
    (Seq(s""""id":"${r.id}"""", s""""g":"${r.g}"""") ++
    r.dText.map(t => s""""d":"$t"""").orElse(r.d.map(v => s""""d":${dateValue(v)}""")) ++
    r.d2.map(v => s""""d2":${dateValue(v)}""") ++
    r.ts.map(v => s""""ts":${instantValue(v)}""") ++
    r.ts2.map(v => s""""ts2":${instantValue(v)}""") ++
    r.n.map(v => s""""n":$v""") ++ r.x.map(v => s""""x":$v""")).mkString("{", ",", "}")
  }

  private def bulk(index: String, docs: List[String]): Unit = {
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(index)
      case ElasticFailure(error) =>
        fail(s"Bulk indexing into $index failed: ${error.message}")
    }
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    run(s"CREATE TABLE $table ($columns, PRIMARY KEY (id))")
    run(
      s"CREATE TABLE $computedTable ($columns, " +
      computed.map { case (c, t, e) => s"$c $t SCRIPT AS ($e)" }.mkString(", ") +
      ", PRIMARY KEY (id))"
    )
    bulk(table, rows.map(json).toList)
    bulk(computedTable, (rows ++ computedOnlyRows).map(json).toList)
  }

  override def afterAll(): Unit = {
    Seq(
      table,
      computedTable,
      rawIndex,
      patternA,
      patternB,
      disagreeA,
      disagreeB,
      dateTimeTable,
      comparisonTable,
      numberTable,
      alteredRawIndex,
      grownTable,
      refreshedA,
      refreshedB,
      nullifTable,
      booleanTable,
      flagTable
    ).foreach(t => Try(Await.result(client.run(s"DROP TABLE IF EXISTS $t"), 60.seconds)))
    super.afterAll()
  }

  private def run(sql: String): Unit =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    }

  /** The rows of a statement, through `GatewayApi.run` (JDBC / REPL / Flight SQL). */
  private def gatewayRows(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Try {
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(QueryRows(rs, _))             => Right(rs)
        case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
        case ElasticSuccess(QueryStream(stream, _)) =>
          Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
        case ElasticSuccess(other) => Left(s"unexpected result $other")
        case ElasticFailure(error) => Left(error.message)
      }
    }.toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}").joinRight

  // -------------------------------------------------------------------------------------------
  // The oracle
  // -------------------------------------------------------------------------------------------

  /** An expected value: a number, an instant (a DATE is the instant its day starts at), a boolean,
    * a number within a tolerance (a clock read when the statement ran), or NULL.
    */
  private sealed trait Expected
  private final case class Num(value: Double) extends Expected
  private final case class About(value: Double, tolerance: Double) extends Expected
  private final case class At(value: Instant) extends Expected
  private final case class Bool(value: Boolean) extends Expected
  private case object Null extends Expected

  /** An instant read from the clock while the statement ran: between `from` and `to`. */
  private final case class Within(from: Instant, to: Instant) extends Expected

  private def start(d: LocalDate): Instant = d.atStartOfDay(ZoneOffset.UTC).toInstant

  private def utcDay(i: Instant): LocalDate = i.atZone(ZoneOffset.UTC).toLocalDate

  private def daysBetween(from: LocalDate, to: LocalDate): Expected =
    Num(ChronoUnit.DAYS.between(from, to).toDouble)

  private def fractionalDays(from: Instant, to: Instant): Double =
    (to.toEpochMilli - from.toEpochMilli).toDouble / DayMillis

  private def moved(from: Instant, days: Double): Expected =
    At(from.plusMillis(Math.round(days * DayMillis)))

  private def expect(value: Option[Expected]): Expected = value.getOrElse(Null)

  /** The UTC day of the statement's clock, read when the test runs. */
  private def today: LocalDate = LocalDate.now(ZoneOffset.UTC)

  /** Elasticsearch wraps every `script_fields` value in a one-element array. */
  private def scalar(v: Any): Any = v match {
    case s: Seq[_]                  => s.headOption.map(scalar).orNull
    case c: java.util.Collection[_] => scalar(c.toArray.toSeq)
    case other                      => other
  }

  private def asInstant(v: Any): Option[Instant] =
    scalar(v) match {
      case null                => None
      case n: java.lang.Number => Some(Instant.ofEpochMilli(Math.round(n.doubleValue())))
      case i: Instant          => Some(i)
      case z: ZonedDateTime    => Some(z.toInstant)
      case o: OffsetDateTime   => Some(o.toInstant)
      case l: LocalDate        => Some(start(l))
      case l: LocalDateTime    => Some(l.toInstant(ZoneOffset.UTC))
      case d: java.util.Date   => Some(d.toInstant)
      case s =>
        val text = s.toString
        Try(Instant.parse(text))
          .orElse(Try(ZonedDateTime.parse(text).toInstant))
          .orElse(Try(OffsetDateTime.parse(text).toInstant))
          .orElse(Try(LocalDateTime.parse(text).toInstant(ZoneOffset.UTC)))
          .orElse(Try(start(LocalDate.parse(text))))
          .toOption
    }

  private def asNumber(v: Any): Option[Double] =
    scalar(v) match {
      case null                => None
      case n: java.lang.Number => Some(n.doubleValue())
      case s                   => Try(s.toString.toDouble).toOption
    }

  /** Does a returned value answer the expected one? `None` if it does, else what it answered. */
  private def differs(expected: Expected, actual: Any): Option[String] =
    expected match {
      case Null =>
        if (scalar(actual) == null) None else Some(s"$actual instead of NULL")
      case Num(want) =>
        asNumber(actual) match {
          case Some(got) if Math.abs(got - want) <= 1e-9 * Math.max(1.0, Math.abs(want)) => None
          case _ => Some(s"$actual instead of $want")
        }
      case About(want, tolerance) =>
        asNumber(actual) match {
          case Some(got) if Math.abs(got - want) <= tolerance => None
          case _ => Some(s"$actual instead of about $want")
        }
      case At(want) =>
        asInstant(actual) match {
          case Some(got) if got == want => None
          case _                        => Some(s"$actual instead of $want")
        }
      case Within(from, to) =>
        asInstant(actual) match {
          case Some(got) if !got.isBefore(from) && !got.isAfter(to) => None
          case _ => Some(s"$actual instead of an instant between $from and $to")
        }
      case Bool(want) =>
        scalar(actual) match {
          case b: java.lang.Boolean if b.booleanValue() == want      => None
          case s: String if Try(s.toBoolean).toOption.contains(want) => None
          case other => Some(s"$other instead of $want")
        }
    }

  /** A row-level expression and its value for each document. */
  private final case class RowForm(sql: String, value: Row => Expected)

  private def nullIf(a: Option[LocalDate], b: Option[LocalDate]): Option[LocalDate] =
    if (a.isDefined && a == b) None else a

  /** GREATEST / LEAST as SQL engines answer them: a NULL argument is skipped, and the answer is
    * NULL only when every argument is.
    */
  private def latest(values: Option[Instant]*): Option[Instant] =
    values.flatten.reduceOption((a, b) => if (a.isAfter(b)) a else b)

  private def earliest(values: Option[Instant]*): Option[Instant] =
    values.flatten.reduceOption((a, b) => if (a.isBefore(b)) a else b)

  /** A number cast to a TIMESTAMP is the instant that many milliseconds from the epoch, a
    * fractional number truncated as every fractional-to-integral cast is.
    */
  private def epochMillis(value: Double): Expected = At(Instant.ofEpochMilli(value.toLong))

  private val rowForms: Seq[RowForm] = Seq(
    RowForm("d - d2", r => expect(for (a <- r.d; b <- r.d2) yield daysBetween(b, a))),
    RowForm("d2 - d", r => expect(for (a <- r.d; b <- r.d2) yield daysBetween(a, b))),
    RowForm(
      "ts - ts2",
      r => expect(for (a <- r.ts; b <- r.ts2) yield Num(fractionalDays(b, a)))
    ),
    RowForm(
      "d - ts",
      r => expect(for (a <- r.d; b <- r.ts) yield Num(fractionalDays(b, start(a))))
    ),
    RowForm(
      "ts - d",
      r => expect(for (a <- r.ts; b <- r.d) yield Num(fractionalDays(start(b), a)))
    ),
    RowForm("d + 1", r => expect(r.d.map(a => At(start(a.plusDays(1)))))),
    RowForm("d - 1", r => expect(r.d.map(a => At(start(a.minusDays(1)))))),
    RowForm("1 + d", r => expect(r.d.map(a => At(start(a.plusDays(1)))))),
    RowForm("d + n", r => expect(for (a <- r.d; n <- r.n) yield At(start(a.plusDays(n.toLong))))),
    RowForm("n + d", r => expect(for (a <- r.d; n <- r.n) yield At(start(a.plusDays(n.toLong))))),
    RowForm("d + 1.5", r => expect(r.d.map(a => moved(start(a), 1.5)))),
    RowForm("d - 1.5", r => expect(r.d.map(a => moved(start(a), -1.5)))),
    RowForm("d + x", r => expect(for (a <- r.d; x <- r.x) yield moved(start(a), x))),
    RowForm("ts + 1", r => expect(r.ts.map(a => moved(a, 1)))),
    RowForm("ts - 1.5", r => expect(r.ts.map(a => moved(a, -1.5)))),
    RowForm("ts + n", r => expect(for (a <- r.ts; n <- r.n) yield moved(a, n.toDouble))),
    RowForm("ts + x", r => expect(for (a <- r.ts; x <- r.x) yield moved(a, x))),
    RowForm("d + NULL", _ => Null),
    RowForm("NULL - ts", _ => Null),
    RowForm(
      "CAST('2024-02-15' AS DATE) - d",
      r => expect(r.d.map(a => daysBetween(a, LocalDate.parse("2024-02-15"))))
    ),
    RowForm(
      "d + 1 - d2",
      r => expect(for (a <- r.d; b <- r.d2) yield daysBetween(b, a.plusDays(1)))
    ),
    RowForm(
      "(ts - ts2) * 24",
      r =>
        expect(
          for (a <- r.ts; b <- r.ts2)
            yield Num((a.toEpochMilli - b.toEpochMilli) * 24.0 / DayMillis)
        )
    ),
    // ---- the fix round ----
    // operands that are not bare columns
    RowForm(
      "COALESCE(d, d2) - d2",
      r => expect(for (b <- r.d2; a <- r.d.orElse(r.d2)) yield daysBetween(b, a))
    ),
    RowForm(
      "NULLIF(d, d2) - d2",
      r => expect(for (b <- r.d2; a <- nullIf(r.d, r.d2)) yield daysBetween(b, a))
    ),
    RowForm(
      "CASE WHEN n > 0 THEN d ELSE d2 END - d",
      r =>
        expect(
          for (a <- if (r.n.exists(_ > 0)) r.d else r.d2; b <- r.d) yield daysBetween(b, a)
        )
    ),
    RowForm(
      "CAST(ts AS DATE) - d",
      r => expect(for (t <- r.ts; b <- r.d) yield daysBetween(b, utcDay(t)))
    ),
    // a string literal that is a number counts as that number
    RowForm("d + '1'", r => expect(r.d.map(a => At(start(a.plusDays(1)))))),
    RowForm("d - '1.5'", r => expect(r.d.map(a => moved(start(a), -1.5)))),
    // a typed NULL takes its type, and its value is NULL
    RowForm("CAST(NULL AS DATE) - d", _ => Null),
    RowForm("d + CAST(NULL AS INT)", _ => Null),
    // a cast over arithmetic applies core's cast rule to the RESULT, and keeps NULL a NULL
    RowForm(
      "(ts - ts2)::BIGINT",
      r => expect(for (a <- r.ts; b <- r.ts2) yield Num(fractionalDays(b, a).toLong.toDouble))
    ),
    RowForm(
      "(n + x)::BIGINT",
      r => expect(for (n <- r.n; x <- r.x) yield Num((n + x).toLong.toDouble))
    ),
    RowForm("(x * 2)::INT", r => expect(r.x.map(x => Num((x * 2).toInt.toDouble)))),
    RowForm(
      "(d - d2)::TIMESTAMP",
      r =>
        expect(
          for (a <- r.d; b <- r.d2)
            yield At(Instant.ofEpochMilli(ChronoUnit.DAYS.between(b, a)))
        )
    ),
    RowForm(
      "(ts - ts2)::TIMESTAMP",
      r =>
        expect(
          for (a <- r.ts; b <- r.ts2) yield At(Instant.ofEpochMilli(fractionalDays(b, a).toLong))
        )
    ),
    RowForm(
      "(d - d2)::DOUBLE",
      r => expect(for (a <- r.d; b <- r.d2) yield daysBetween(b, a))
    ),
    RowForm("(n + x)::BOOLEAN", r => expect(for (n <- r.n; x <- r.x) yield Bool(n + x != 0))),
    // ---- fix round 2 ----
    // GREATEST / LEAST over dates and timestamps: a DATE is its day's start, a NULL is skipped
    RowForm("GREATEST(d, d2)", r => expect(latest(r.d.map(start), r.d2.map(start)).map(At))),
    RowForm("LEAST(d, ts)", r => expect(earliest(r.d.map(start), r.ts).map(At))),
    RowForm("LEAST(ts, ts2)", r => expect(earliest(r.ts, r.ts2).map(At))),
    RowForm("GREATEST(d, NULL)", r => expect(r.d.map(a => At(start(a))))),
    RowForm(
      "GREATEST(d, d2) - d",
      r =>
        expect(
          for (a <- r.d; m <- latest(r.d.map(start), r.d2.map(start)))
            yield daysBetween(a, utcDay(m))
        )
    ),
    // a number cast to a TIMESTAMP: the instant that many milliseconds from the epoch
    RowForm("CAST(n AS TIMESTAMP)", r => expect(r.n.map(n => epochMillis(n.toDouble)))),
    RowForm("CAST(x AS TIMESTAMP)", r => expect(r.x.map(epochMillis)))
  )

  /** A WHERE predicate and whether each document satisfies it -- `None` (UNKNOWN) keeps none. */
  private final case class WhereForm(sql: String, keeps: Row => Option[Boolean])

  private def on(s: String): LocalDate = LocalDate.parse(s)
  private def at(s: String): Instant = Instant.parse(s)

  private val whereForms: Seq[WhereForm] = Seq(
    WhereForm(
      "d - d2 = -1",
      r => for (a <- r.d; b <- r.d2) yield ChronoUnit.DAYS.between(b, a) == -1
    ),
    WhereForm(
      "d2 - d = 1",
      r => for (a <- r.d; b <- r.d2) yield ChronoUnit.DAYS.between(a, b) == 1
    ),
    WhereForm(
      "d - d2 = 2",
      r => for (a <- r.d; b <- r.d2) yield ChronoUnit.DAYS.between(b, a) == 2
    ),
    WhereForm(
      "ts - ts2 < -1",
      r => for (a <- r.ts; b <- r.ts2) yield fractionalDays(b, a) < -1
    ),
    WhereForm(
      "d - ts > -0.75",
      r => for (a <- r.d; b <- r.ts) yield fractionalDays(b, start(a)) > -0.75
    ),
    WhereForm(
      "d + 1 - d2 = 0",
      r => for (a <- r.d; b <- r.d2) yield ChronoUnit.DAYS.between(b, a.plusDays(1)) == 0
    ),
    WhereForm(
      "d - 1 - d2 = 1",
      r => for (a <- r.d; b <- r.d2) yield ChronoUnit.DAYS.between(b, a.minusDays(1)) == 1
    ),
    WhereForm(
      "n + d - d2 = 2",
      r =>
        for (a <- r.d; b <- r.d2; n <- r.n)
          yield ChronoUnit.DAYS.between(b, a.plusDays(n.toLong)) == 2
    ),
    WhereForm(
      "d + 1.5 - ts > 1",
      r =>
        for (a <- r.d; b <- r.ts)
          yield (start(a).toEpochMilli + 129600000L - b.toEpochMilli).toDouble / DayMillis > 1
    ),
    WhereForm(
      "ts + 1 - ts2 > 0",
      r => for (a <- r.ts; b <- r.ts2) yield a.toEpochMilli + DayMillis - b.toEpochMilli > 0
    ),
    WhereForm(
      "ts - 1.5 - ts2 < -1.5",
      r =>
        for (a <- r.ts; b <- r.ts2)
          yield (a.toEpochMilli - 129600000L - b.toEpochMilli).toDouble / DayMillis < -1.5
    ),
    WhereForm("d + NULL > CAST('2000-01-01' AS DATE)", _ => None),
    // ---- refused at PARSE before the fix round, by a type the column did not have yet ----
    WhereForm(
      "CURRENT_DATE - d > 30",
      r => r.d.map(a => ChronoUnit.DAYS.between(a, today) > 30)
    ),
    WhereForm("d + 7 > CURRENT_DATE", r => r.d.map(a => a.plusDays(7).isAfter(today))),
    WhereForm(
      "NOW() - ts > 1",
      r => r.ts.map(t => fractionalDays(t, Instant.now()) > 1)
    ),
    WhereForm(
      "CAST(ts AS DATE) - d = 0",
      r => for (t <- r.ts; a <- r.d) yield utcDay(t) == a
    ),
    WhereForm(
      "d + 1 = CAST('2024-02-01' AS DATE)",
      r => r.d.map(_.plusDays(1) == on("2024-02-01"))
    ),
    WhereForm("d + 1 > '2024-01-01'", r => r.d.map(_.plusDays(1).isAfter(on("2024-01-01")))),
    // ---- fix round 2 ----
    WhereForm(
      "GREATEST(d, d2) > CAST('2024-02-01' AS DATE)",
      r => latest(r.d.map(start), r.d2.map(start)).map(_.isAfter(start(on("2024-02-01"))))
    ),
    WhereForm(
      "LEAST(ts, ts2) < CAST('2024-02-01T00:00:00Z' AS TIMESTAMP)",
      r => earliest(r.ts, r.ts2).map(_.isBefore(at("2024-02-01T00:00:00Z")))
    ),
    WhereForm(
      "CAST(n AS TIMESTAMP) > CAST('1970-01-01T00:00:00Z' AS TIMESTAMP)",
      r => r.n.map(_ > 0)
    )
  )

  /** The aggregates of one group, `None` where the group has no value. */
  private final case class Group(
    g: String,
    maxD: Option[LocalDate],
    minD: Option[LocalDate],
    maxD2: Option[LocalDate],
    maxTs: Option[Instant],
    minTs: Option[Instant],
    maxN: Option[Int],
    countD: Int,
    distinctD: Int,
    avgN: Option[Double],
    avgX: Option[Double],
    avgD: Option[Double]
  )

  private def mean(values: Seq[Double]): Option[Double] =
    if (values.isEmpty) None else Some(values.sum / values.size)

  private lazy val groups: Seq[Group] = rows.groupBy(_.g).toSeq.sortBy(_._1).map { case (g, rs) =>
    val ds = rs.flatMap(_.d)
    val tss = rs.flatMap(_.ts)
    Group(
      g,
      ds.reduceOption((a, b) => if (a.isAfter(b)) a else b),
      ds.reduceOption((a, b) => if (a.isBefore(b)) a else b),
      rs.flatMap(_.d2).reduceOption((a, b) => if (a.isAfter(b)) a else b),
      tss.reduceOption((a, b) => if (a.isAfter(b)) a else b),
      tss.reduceOption((a, b) => if (a.isBefore(b)) a else b),
      rs.flatMap(_.n).reduceOption(_ max _),
      ds.size,
      ds.distinct.size,
      mean(rs.flatMap(_.n).map(_.toDouble)),
      mean(rs.flatMap(_.x)),
      mean(ds.map(a => start(a).toEpochMilli.toDouble))
    )
  }

  /** A per-group expression and its value for each group. */
  private final case class GroupForm(sql: String, value: Group => Expected)

  private val groupForms: Seq[GroupForm] = Seq(
    GroupForm(
      "MAX(d) - MIN(d)",
      g => expect(for (a <- g.maxD; b <- g.minD) yield daysBetween(b, a))
    ),
    GroupForm(
      "MIN(d) - MAX(d)",
      g => expect(for (a <- g.maxD; b <- g.minD) yield daysBetween(a, b))
    ),
    GroupForm(
      "MAX(ts) - MIN(ts)",
      g => expect(for (a <- g.maxTs; b <- g.minTs) yield Num(fractionalDays(b, a)))
    ),
    GroupForm(
      "MAX(d) - MIN(ts)",
      g => expect(for (a <- g.maxD; b <- g.minTs) yield Num(fractionalDays(b, start(a))))
    ),
    GroupForm(
      "MAX(ts) - MIN(d)",
      g => expect(for (a <- g.maxTs; b <- g.minD) yield Num(fractionalDays(start(b), a)))
    ),
    GroupForm("MAX(d) + 1", g => expect(g.maxD.map(a => At(start(a.plusDays(1)))))),
    GroupForm("MAX(d) - 1", g => expect(g.maxD.map(a => At(start(a.minusDays(1)))))),
    GroupForm("1 + MAX(d)", g => expect(g.maxD.map(a => At(start(a.plusDays(1)))))),
    GroupForm("MAX(d) + 1.5", g => expect(g.maxD.map(a => moved(start(a), 1.5)))),
    GroupForm("MAX(ts) + 1", g => expect(g.maxTs.map(a => moved(a, 1)))),
    GroupForm("MAX(ts) - 1.5", g => expect(g.maxTs.map(a => moved(a, -1.5)))),
    GroupForm(
      "MAX(d) + MAX(n)",
      g => expect(for (a <- g.maxD; n <- g.maxN) yield At(start(a.plusDays(n.toLong))))
    ),
    GroupForm("MAX(d) - NULL", _ => Null),
    GroupForm(
      "MAX(d) - CAST('2024-01-01' AS DATE)",
      g => expect(g.maxD.map(a => daysBetween(LocalDate.parse("2024-01-01"), a)))
    ),
    // ---- the fix round ----
    // an aggregate is the metric it produces: COUNT counts, AVG averages
    GroupForm("COUNT(d) - 1", g => Num(g.countD - 1.0)),
    GroupForm("COUNT(DISTINCT d) * 2", g => Num(g.distinctD * 2.0)),
    GroupForm("AVG(d) - 1", g => expect(g.avgD.map(a => Num(a - 1)))),
    GroupForm(
      "MAX(d) - AVG(n)",
      g => expect(for (a <- g.maxD; n <- g.avgN) yield moved(start(a), -n))
    ),
    // the request clock in a per-group calculation
    GroupForm(
      "CURRENT_DATE - MAX(d)",
      g => expect(g.maxD.map(a => daysBetween(a, today)))
    ),
    GroupForm(
      "DATEDIFF(CURRENT_DATE, MAX(d))",
      g => expect(g.maxD.map(a => daysBetween(a, today)))
    ),
    GroupForm(
      "NOW() - MAX(ts)",
      g => expect(g.maxTs.map(t => About(fractionalDays(t, Instant.now()), 1.0 / 24)))
    ),
    // a cast over a per-group calculation
    GroupForm(
      "(MAX(n) + AVG(x))::BIGINT",
      g => expect(for (n <- g.maxN; x <- g.avgX) yield Num((n + x).toLong.toDouble))
    ),
    // ---- fix round 2: GREATEST / LEAST over aggregates, computed per group ----
    // ⚠️ A SELECT's per-group calculation is NULL in a group missing one of its metrics:
    // Elasticsearch's bucket_script does not run there (its gap policy skips the group), as for
    // every per-group calculation. A HAVING reads every aggregate and skips a NULL one (below).
    GroupForm(
      "GREATEST(MAX(d), MAX(d2))",
      g => expect(for (a <- g.maxD; b <- g.maxD2) yield At(start(if (a.isAfter(b)) a else b)))
    ),
    GroupForm(
      "LEAST(MAX(ts), MAX(d))",
      g =>
        expect(for (t <- g.maxTs; a <- g.maxD; m <- earliest(Some(t), Some(start(a)))) yield At(m))
    ),
    GroupForm(
      "GREATEST(MAX(d), MAX(d2)) - MIN(d)",
      g =>
        expect(
          for (a <- g.maxD; b <- g.maxD2; m <- g.minD)
            yield daysBetween(m, if (a.isAfter(b)) a else b)
        )
    ),
    GroupForm("GREATEST(MAX(n), 2)", g => expect(g.maxN.map(n => Num(Math.max(n, 2).toDouble))))
  )

  /** A HAVING over a per-group calculation and whether each group is kept. */
  private final case class HavingForm(sql: String, keeps: Group => Option[Boolean])

  private val havingForms: Seq[HavingForm] = Seq(
    HavingForm(
      s"SELECT g, MAX(d) - MIN(d) AS x FROM $table GROUP BY g HAVING x > 10",
      g => for (a <- g.maxD; b <- g.minD) yield ChronoUnit.DAYS.between(b, a) > 10
    ),
    HavingForm(
      s"SELECT g, MAX(d) - MIN(d) AS x FROM $table GROUP BY g HAVING x = 0",
      g => for (a <- g.maxD; b <- g.minD) yield ChronoUnit.DAYS.between(b, a) == 0
    ),
    HavingForm(
      s"SELECT g, MAX(ts) - MIN(ts) AS x FROM $table GROUP BY g HAVING x > 0.5",
      g => for (a <- g.maxTs; b <- g.minTs) yield fractionalDays(b, a) > 0.5
    ),
    HavingForm(
      s"SELECT g, MAX(d) - MIN(ts) AS x FROM $table GROUP BY g HAVING x < 0",
      g => for (a <- g.maxD; b <- g.minTs) yield fractionalDays(b, start(a)) < 0
    ),
    HavingForm(
      s"SELECT g, MAX(d) + 1 AS x FROM $table GROUP BY g HAVING x > MAX(d2)",
      g => for (a <- g.maxD; b <- g.maxD2) yield a.plusDays(1).isAfter(b)
    ),
    HavingForm(
      s"SELECT g, MAX(d) + 1.5 AS x FROM $table GROUP BY g HAVING x > MAX(ts)",
      g => for (a <- g.maxD; b <- g.maxTs) yield start(a).plusMillis(129600000L).isAfter(b)
    ),
    HavingForm(
      s"SELECT g, MAX(ts) - 1 AS x FROM $table GROUP BY g HAVING x < MIN(ts)",
      g => for (a <- g.maxTs; b <- g.minTs) yield a.minusMillis(DayMillis).isBefore(b)
    ),
    HavingForm(
      s"SELECT g FROM $table GROUP BY g HAVING MAX(d) > CAST('2024-03-01' AS DATE) - 30",
      g => g.maxD.map(_.isAfter(on("2024-01-31")))
    ),
    // refused at PARSE before the fix round
    HavingForm(
      s"SELECT g, MAX(d) + 1 AS x FROM $table GROUP BY g HAVING x > CAST('2024-01-01' AS DATE)",
      g => g.maxD.map(_.plusDays(1).isAfter(on("2024-01-01")))
    ),
    HavingForm(
      s"SELECT g, COUNT(d) - 1 AS x FROM $table GROUP BY g HAVING x > 0",
      g => Some(g.countD - 1 > 0)
    ),
    // ---- fix round 2: GREATEST over aggregates. Inline, the group filter reads every aggregate
    // and SKIPS a NULL one (g2 has a `d` and no `d2`); through its SELECT alias it reads the
    // per-group calculation, NULL in a group missing one of its metrics ----
    HavingForm(
      s"SELECT g FROM $table GROUP BY g HAVING GREATEST(MAX(d), MAX(d2)) > CAST('2023-12-01' AS DATE)",
      g => latest(g.maxD.map(start), g.maxD2.map(start)).map(_.isAfter(start(on("2023-12-01"))))
    ),
    HavingForm(
      s"SELECT g, GREATEST(MAX(d), MAX(d2)) AS x FROM $table GROUP BY g HAVING x > CAST('2023-12-01' AS DATE)",
      g =>
        for (a <- g.maxD; b <- g.maxD2) yield (if (a.isAfter(b)) a else b).isAfter(on("2023-12-01"))
    )
  )

  /** Run every statement before asserting anything, so a failure names every one that differs. */
  private def verdict(label: String, outcomes: Seq[(String, Either[String, Seq[String]])]): Unit = {
    val wrong = outcomes.collect {
      case (sql, Right(diffs)) if diffs.nonEmpty => s"[$sql] ${diffs.mkString("; ")}"
    }
    val errors = outcomes.collect { case (sql, Left(error)) => s"[$sql] $error" }
    info(s"$label: ${outcomes.size} statements; wrong: ${wrong.size}; errors: ${errors.size}")
    (wrong ++ errors).foreach(info(_))
    withClue(
      s"${wrong.size} wrong, ${errors.size} errors:\n${(wrong ++ errors).take(30).mkString("\n")}\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  private def byKey(rs: Seq[ListMap[String, Any]], key: String): Map[String, Any] =
    rs.map(r => r.getOrElse(key, "?").toString -> r.getOrElse("x", null)).toMap

  // -------------------------------------------------------------------------------------------
  // The venues
  // -------------------------------------------------------------------------------------------

  private def rowVerdicts(
    forms: Seq[RowForm],
    from: String,
    population: Seq[Row]
  ): Seq[(String, Either[String, Seq[String]])] =
    forms.map { f =>
      val sql = s"SELECT id, ${f.sql} AS x FROM $from"
      sql -> gatewayRows(sql).map { rs =>
        val got = byKey(rs, "id")
        population.flatMap { r =>
          differs(f.value(r), got.getOrElse(r.id, null)).map(d => s"${r.id}: $d")
        } ++ (if (got.size == population.size) Nil else Seq(s"${got.size} rows"))
      }
    }

  "date arithmetic in a row SELECT" should "answer the java.time oracle on every document" in {
    rowForms.flatMap(f => rows.map(f.value)).distinct.size should be > 10
    verdict("row SELECT", rowVerdicts(rowForms, table, rows))
  }

  private def whereVerdicts(
    forms: Seq[WhereForm],
    from: String = table
  ): Seq[(String, Either[String, Seq[String]])] =
    forms.map { f =>
      val sql = s"SELECT id FROM $from WHERE ${f.sql}"
      sql -> gatewayRows(sql).map { rs =>
        val kept = rs.map(_.getOrElse("id", "?").toString).toSet
        val want = rows.filter(r => f.keeps(r).contains(true)).map(_.id).toSet
        if (kept == want) Nil
        else
          Seq(
            s"kept ${kept.toSeq.sorted.mkString(",")}, expected ${want.toSeq.sorted.mkString(",")}"
          )
      }
    }

  "date arithmetic in a WHERE" should "keep exactly the documents the java.time oracle keeps" in {
    // the predicates keep some documents and drop others
    whereForms.exists(f => rows.exists(r => f.keeps(r).contains(true))) shouldBe true
    whereForms.exists(f => rows.exists(r => !f.keeps(r).contains(true))) shouldBe true
    verdict("WHERE", whereVerdicts(whereForms))
  }

  /** A date column compared with date arithmetic -- the filter `d > CURRENT_DATE - 7` is written
    * for -- and the neighbouring comparisons a fix for it must keep working. On Elasticsearch 6.8 a
    * `date` doc value is a `JodaCompatibleZonedDateTime`, which `isBefore` / `isAfter` / `isEqual`
    * against a genuine `ZonedDateTime` used to fail with `Class.cast`.
    *
    * ⚠️ A column compared with ANOTHER column (`d2 > d`) is not here: on Elasticsearch 6.8 it is
    * sent as a `range` bounded by the other column's NAME, outside this suite's rules.
    */
  private val columnForms: Seq[WhereForm] = Seq(
    WhereForm("d < CAST('2024-01-07' AS DATE) - 7", r => r.d.map(_.isBefore(on("2023-12-31")))),
    WhereForm(
      "d >= CAST('2024-03-08' AS DATE) - 7",
      r => r.d.map(a => !a.isBefore(on("2024-03-01")))
    ),
    WhereForm(
      "ts > CAST('2024-03-01T00:00:00Z' AS TIMESTAMP) - 1.5",
      r => r.ts.map(_.isAfter(at("2024-02-28T12:00:00Z")))
    ),
    WhereForm("d2 = d + 1", r => for (a <- r.d; b <- r.d2) yield b == a.plusDays(1)),
    WhereForm("d > d2 - 1", r => for (a <- r.d; b <- r.d2) yield a.isAfter(b.minusDays(1))),
    WhereForm(
      "DATE_ADD(d, INTERVAL 1 DAY) = d2",
      r => for (a <- r.d; b <- r.d2) yield a.plusDays(1) == b
    ),
    WhereForm("CAST(ts AS DATE) = d", r => for (t <- r.ts; a <- r.d) yield utcDay(t) == a),
    WhereForm(
      "COALESCE(d, d2) > CAST('2024-01-01' AS DATE)",
      r => r.d.orElse(r.d2).map(_.isAfter(on("2024-01-01")))
    ),
    WhereForm(
      "ts < ts2 + 0.5",
      r => for (a <- r.ts; b <- r.ts2) yield a.isBefore(b.plusMillis(DayMillis / 2))
    ),
    WhereForm(
      "d >= CAST(ts AS DATE) - 1",
      r => for (a <- r.d; t <- r.ts) yield !a.isBefore(utcDay(t).minusDays(1))
    ),
    WhereForm("d + 1 > d2", r => for (a <- r.d; b <- r.d2) yield a.plusDays(1).isAfter(b)),
    WhereForm("d - 1 = d2", r => for (a <- r.d; b <- r.d2) yield a.minusDays(1) == b),
    WhereForm("CURRENT_DATE > d", r => r.d.map(today.isAfter(_))),
    // ---- fix round 2: GREATEST / LEAST over dates against a date column ----
    WhereForm(
      "GREATEST(d, d2) > d",
      r => for (a <- r.d; m <- latest(r.d.map(start), r.d2.map(start))) yield m.isAfter(start(a))
    ),
    WhereForm(
      "LEAST(d, ts) < ts",
      r => for (t <- r.ts; m <- earliest(r.d.map(start), r.ts)) yield m.isBefore(t)
    ),
    // the comparisons that already worked, on every major
    WhereForm("d = CAST('2024-01-31' AS DATE)", r => r.d.map(_ == on("2024-01-31"))),
    WhereForm("d >= '2024-01-01'", r => r.d.map(a => !a.isBefore(on("2024-01-01")))),
    WhereForm(
      "CAST(d AS TIMESTAMP) < ts",
      r => for (a <- r.d; t <- r.ts) yield start(a).isBefore(t)
    ),
    WhereForm(
      "n + d = CAST('2024-02-03' AS DATE)",
      r => for (a <- r.d; n <- r.n) yield a.plusDays(n.toLong) == on("2024-02-03")
    ),
    WhereForm("d > CAST('2024-01-01' AS DATE)", r => r.d.map(_.isAfter(on("2024-01-01")))),
    WhereForm(
      "ts > CAST('2024-01-31T00:00:00Z' AS TIMESTAMP)",
      r => r.ts.map(_.isAfter(at("2024-01-31T00:00:00Z")))
    ),
    WhereForm("d < CURRENT_DATE", r => r.d.map(_.isBefore(today))),
    WhereForm(
      "CAST(d AS DATE) = CAST('2024-01-31' AS DATE)",
      r => r.d.map(_ == on("2024-01-31"))
    ),
    WhereForm(
      "d BETWEEN CAST('2024-01-01' AS DATE) AND CAST('2024-12-31' AS DATE)",
      r => r.d.map(a => !a.isBefore(on("2024-01-01")) && !a.isAfter(on("2024-12-31")))
    ),
    WhereForm(
      "d > CAST('2024-03-08' AS DATE) - INTERVAL 7 DAY",
      r => r.d.map(_.isAfter(on("2024-03-01")))
    )
  )

  "a date column compared with date arithmetic" should "keep exactly the documents the java.time oracle keeps" in {
    columnForms.exists(f => rows.exists(r => f.keeps(r).contains(true))) shouldBe true
    columnForms.exists(f => rows.exists(r => !f.keeps(r).contains(true))) shouldBe true
    verdict("column vs date arithmetic", whereVerdicts(columnForms))
  }

  private def groupVerdicts(
    forms: Seq[GroupForm],
    from: String = table
  ): Seq[(String, Either[String, Seq[String]])] =
    forms.map { f =>
      val sql = s"SELECT g, ${f.sql} AS x FROM $from GROUP BY g"
      sql -> gatewayRows(sql).map { rs =>
        val got = byKey(rs, "g")
        groups.flatMap { g =>
          differs(f.value(g), got.getOrElse(g.g, null)).map(d => s"${g.g}: $d")
        } ++ (if (got.size == groups.size) Nil else Seq(s"${got.size} groups"))
      }
    }

  "date arithmetic per group" should "answer the java.time oracle in every group, NULL where an operand has no value" in {
    // a group with two dates, groups with one, and a non-empty group with none
    groups.map(_.g) shouldBe Seq("g1", "g2", "g3", "g4", "g5", "g6")
    groups.find(_.g == "g3").flatMap(_.maxD) shouldBe None
    verdict("per-group SELECT", groupVerdicts(groupForms))
  }

  "a HAVING over date arithmetic" should "keep exactly the groups the java.time oracle keeps" in {
    havingForms.exists(f => groups.exists(g => f.keeps(g).contains(true))) shouldBe true
    havingForms.exists(f => groups.exists(g => !f.keeps(g).contains(true))) shouldBe true
    verdict(
      "HAVING",
      havingForms.map { f =>
        f.sql -> gatewayRows(f.sql).map { rs =>
          val kept = rs.map(_.getOrElse("g", "?").toString).toSet
          val want = groups.filter(g => f.keeps(g).contains(true)).map(_.g).toSet
          if (kept == want) Nil
          else
            Seq(
              s"kept ${kept.toSeq.sorted.mkString(",")}, expected ${want.toSeq.sorted.mkString(",")}"
            )
        }
      }
    )
  }

  "a DATE aggregate over date-time strings" should
  "compare as its UTC day's start per group, in GREATEST / LEAST as in date arithmetic" in {
    // Elasticsearch accepts a date-time into a `date` field, and MAX / MIN of it keep that time:
    // per group, a DATE operand is still its UTC day -- the oracle is the one of `table`, whose
    // `d` is the same days without a time
    run(s"CREATE TABLE $dateTimeTable ($columns, PRIMARY KEY (id))")
    bulk(
      dateTimeTable,
      rows.map(r => json(r.copy(dText = r.d.map(day => s"${day}T22:30:00Z")))).toList
    )
    val forms = groupForms.filter(f =>
      Set(
        "GREATEST(MAX(d), MAX(d2))",
        "LEAST(MAX(ts), MAX(d))",
        "GREATEST(MAX(d), MAX(d2)) - MIN(d)",
        "MAX(d) + 1",
        "MAX(d) - MIN(ts)"
      ).contains(f.sql)
    )
    forms.size shouldBe 5
    // a group filter reads every aggregate, NULL ones skipped: g1's LEAST is its day's start,
    // 2024-03-01T00:00, kept; read with its time, it would be 06:00, and g1 would be dropped
    val threshold = Instant.parse("2024-03-01T03:00:00Z")
    val having =
      s"SELECT g FROM $dateTimeTable GROUP BY g " +
      s"HAVING LEAST(MAX(ts), MAX(d)) < CAST('$threshold' AS TIMESTAMP)"
    val want = groups
      .filter(g => earliest(g.maxTs, g.maxD.map(start)).exists(_.isBefore(threshold)))
      .map(_.g)
      .toSet
    want should contain("g1")
    verdict(
      s"FROM $dateTimeTable",
      groupVerdicts(forms, dateTimeTable) :+ (having -> gatewayRows(having).map { rs =>
        val kept = rs.map(_.getOrElse("g", "?").toString).toSet
        if (kept == want) Nil
        else
          Seq(
            s"kept ${kept.toSeq.sorted.mkString(",")}, expected ${want.toSeq.sorted.mkString(",")}"
          )
      })
    )
  }

  "a computed column over date arithmetic" should "STORE the java.time oracle's value" in {
    val forms: Map[String, RowForm] = rowForms.map(f => f.sql -> f).toMap
    val stored = rows ++ computedOnlyRows
    val sql = s"SELECT id, ${computed.map(_._1).mkString(", ")} FROM $computedTable"
    verdict(
      "SCRIPT AS",
      Seq(sql -> gatewayRows(sql).map { rs =>
        val byId = rs.map(r => r.getOrElse("id", "?").toString -> r).toMap
        (if (byId.size == stored.size) Nil else Seq(s"${byId.size} rows")) ++
        (for {
          (c, _, expression) <- computed
          r                  <- stored
        } yield {
          val want =
            forms.get(expression).map(_.value(r)).getOrElse(fail(s"no oracle for $expression"))
          differs(want, byId.get(r.id).flatMap(_.get(c)).orNull).map(d =>
            s"$c [$expression] ${r.id}: $d"
          )
        }).flatten
      })
    )
  }

  /** The type DESCRIBE reports for `field` of `index`. */
  private def described(index: String, field: String): Either[String, Option[String]] =
    gatewayRows(s"DESCRIBE TABLE $index").map(
      _.find(_.get("Field").contains(field)).flatMap(_.get("Type")).map(_.toString)
    )

  private val loaded = Seq(
    ("n1", "a", "2024-01-31T10:30:00Z", "2024-02-01T09:15:00Z"),
    ("n2", "a", "2024-01-31T23:00:00Z", "2024-02-01T01:00:00Z"),
    ("n3", "b", "1969-12-31T23:00:00Z", "1970-01-01T01:00:00Z")
  )

  /** The undeclared dates keep their time: `ts + 1`, `ts2 - ts`, `ts - 1.5` over `index`. */
  private def undeclaredVerdicts(index: String): Seq[(String, Either[String, Seq[String]])] = {
    def ts(s: String) = Instant.parse(s)
    Seq(
      s"SELECT id, ts + 1 AS x FROM $index" -> loaded.map { case (id, _, t, _) =>
        id -> moved(ts(t), 1)
      },
      s"SELECT id, ts2 - ts AS x FROM $index" -> loaded.map { case (id, _, t, t2) =>
        id -> Num(fractionalDays(ts(t), ts(t2)))
      },
      s"SELECT id, ts - 1.5 AS x FROM $index" -> loaded.map { case (id, _, t, _) =>
        id -> moved(ts(t), -1.5)
      }
    ).map { case (sql, expected) =>
      sql -> gatewayRows(sql).map { rs =>
        val got = byKey(rs, "id")
        expected.flatMap { case (id, want) =>
          differs(want, got.getOrElse(id, null)).map(d => s"$id: $d")
        }
      }
    }
  }

  "an index created by a bulk load, with no declaration" should
  "hold TIMESTAMPs: its dates keep their time, and DESCRIBE reports them so" in {
    bulk(
      rawIndex,
      loaded.map { case (id, g, ts, ts2) =>
        s"""{"id":"$id","g":"$g","ts":"$ts","ts2":"$ts2"}"""
      }.toList
    )
    // the lead's ruling of 2026-10-05: a `date` field with no elasticsql declaration is a
    // TIMESTAMP everywhere, DESCRIBE included
    described(rawIndex, "ts") shouldBe Right(Some("TIMESTAMP"))
    def ts(s: String) = Instant.parse(s)
    verdict(
      "undeclared date",
      Seq(
        s"SELECT id, ts + 1 AS x FROM $rawIndex" -> loaded.map { case (id, _, t, _) =>
          id -> moved(ts(t), 1)
        },
        s"SELECT id, ts2 - ts AS x FROM $rawIndex" -> loaded.map { case (id, _, t, t2) =>
          id -> Num(fractionalDays(ts(t), ts(t2)))
        },
        s"SELECT id, ts - 1.5 AS x FROM $rawIndex" -> loaded.map { case (id, _, t, _) =>
          id -> moved(ts(t), -1.5)
        }
      ).map { case (sql, expected) =>
        sql -> gatewayRows(sql).map { rs =>
          val got = byKey(rs, "id")
          expected.flatMap { case (id, want) =>
            differs(want, got.getOrElse(id, null)).map(d => s"$id: $d")
          }
        }
      } :+ {
        val sql =
          s"SELECT g.keyword AS g, MAX(ts2) - MIN(ts) AS x FROM $rawIndex GROUP BY g.keyword"
        sql -> gatewayRows(sql).map { rs =>
          val got = byKey(rs, "g")
          loaded.groupBy(_._2).toSeq.flatMap { case (g, members) =>
            val want = Num(
              fractionalDays(members.map(m => ts(m._3)).min, members.map(m => ts(m._4)).max)
            )
            differs(want, got.getOrElse(g, null)).map(d => s"$g: $d")
          }
        }
      }
    )
  }

  it should "stay a TIMESTAMP once an elasticsql ALTER TABLE declares the table, cache refreshed" in {
    bulk(
      alteredRawIndex,
      loaded.map { case (id, g, ts, ts2) =>
        s"""{"id":"$id","g":"$g","ts":"$ts","ts2":"$ts2"}"""
      }.toList
    )
    verdict("before the ALTER", undeclaredVerdicts(alteredRawIndex))
    // an unrelated ALTER writes the table's `_meta`, every column of it: the undeclared dates are
    // declared as what they are, TIMESTAMPs -- never turned into DATEs
    run(s"ALTER TABLE $alteredRawIndex ADD COLUMN note KEYWORD")
    client.invalidateAllSchemas()
    described(alteredRawIndex, "ts") shouldBe Right(Some("TIMESTAMP"))
    described(alteredRawIndex, "note") shouldBe Right(Some("KEYWORD"))
    verdict("after the ALTER and a cache refresh", undeclaredVerdicts(alteredRawIndex))
  }

  "a date field a bulk load adds to a table elasticsql created" should
  "be a TIMESTAMP, while the declared DATE stays a DATE" in {
    run(s"CREATE TABLE $grownTable (id KEYWORD, d DATE, PRIMARY KEY (id))")
    bulk(
      grownTable,
      List("""{"id":"w1","d":"2024-01-31","seen":"2024-01-31T10:30:00Z"}""")
    )
    client.invalidateAllSchemas()
    described(grownTable, "d") shouldBe Right(Some("DATE"))
    described(grownTable, "seen") shouldBe Right(Some("TIMESTAMP"))
    verdict(
      "a dynamic date field",
      Seq(s"SELECT id, seen + 1 AS x, d + 1 AS y FROM $grownTable" -> {
        val sql = s"SELECT id, seen + 1 AS x, d + 1 AS y FROM $grownTable"
        gatewayRows(sql).map { rs =>
          val got = rs.headOption.getOrElse(ListMap.empty[String, Any])
          differs(At(Instant.parse("2024-02-01T10:30:00Z")), got.getOrElse("x", null)).toSeq ++
          differs(At(start(on("2024-02-01"))), got.getOrElse("y", null)).toSeq
        }
      })
    )
  }

  /** The comparison rule's fixture: base's census rows (r1 to r4), and r5, whose `n` = 10 and `s` =
    * "12" tell a number read as a NUMBER from one compared as TEXT ("10" < "2").
    */
  private val comparisonDocs = List(
    """{"id":"r1","g":"g1","d":"2024-01-31","d2":"2024-02-01","ts":"2024-01-31T10:30:00Z","n":3,"s":"a"}""",
    """{"id":"r2","g":"g1","d":"2024-03-01","d2":"2024-02-28","ts":"2024-03-01T06:00:00Z","n":5,"s":"5"}""",
    """{"id":"r3","g":"g2","d":"2023-12-25","ts":"2023-12-25T23:59:59Z","n":-2,"s":"3"}""",
    """{"id":"r4","g":"g3","s":"b"}""",
    """{"id":"r5","g":"g3","d2":"2024-01-15","n":10,"s":"12"}"""
  )

  /** `SELECT id, <expression> AS v` over [[comparisonTable]], against the expected value of each
    * row -- PostgreSQL 16's answer over the same rows, which for r1 to r4 is also base's.
    */
  private def comparisonVerdict(
    expression: String,
    expected: Map[String, Expected],
    from: String = comparisonTable
  ): (String, Either[String, Seq[String]]) = {
    val sql = s"SELECT id, $expression AS x FROM $from"
    sql -> gatewayRows(sql).map { rs =>
      val got = byKey(rs, "id")
      expected.toSeq.sortBy(_._1).flatMap { case (id, want) =>
        differs(want, got.getOrElse(id, null)).map(d => s"$id: $d")
      }
    }
  }

  private def kept(sql: String, want: Set[String]): (String, Either[String, Seq[String]]) =
    sql -> gatewayRows(sql).map { rs =>
      val got = rs.map(r => r.getOrElse("id", r.getOrElse("g", "?")).toString).toSet
      if (got == want) Nil
      else
        Seq(s"kept ${got.toSeq.sorted.mkString(",")}, expected ${want.toSeq.sorted.mkString(",")}")
    }

  private def flags(ones: String*): Map[String, Expected] =
    Seq("r1", "r2", "r3", "r4", "r5").map(id => id -> Num(if (ones.contains(id)) 1 else 0)).toMap

  "the ONE comparison rule" should
  "read a string literal by what it is compared with, in every clause, as PostgreSQL does" in {
    run(
      s"CREATE TABLE $comparisonTable (id KEYWORD, g KEYWORD, d DATE, d2 DATE, ts TIMESTAMP, " +
      "n INT, s KEYWORD, PRIMARY KEY (id))"
    )
    bulk(comparisonTable, comparisonDocs)
    verdict(
      "comparison rule",
      Seq(
        // the review's statements, accurate on base and refused by round 2: base's values on r1
        // to r4, and r5 read as a number
        comparisonVerdict("CASE WHEN COALESCE(n, 0) = '3' THEN 1 ELSE 0 END", flags("r1")),
        comparisonVerdict("CASE WHEN n + 1 = '4' THEN 1 ELSE 0 END", flags("r1")),
        comparisonVerdict(
          "CASE WHEN LENGTH(s) = '1' THEN 1 ELSE 0 END",
          flags("r1", "r2", "r3", "r4")
        ),
        comparisonVerdict(
          "CASE WHEN COALESCE(n, 0) > '2' THEN 1 ELSE 0 END",
          flags("r1", "r2", "r5")
        ),
        comparisonVerdict("CASE WHEN NULLIF(n, 0) = '3' THEN 1 ELSE 0 END", flags("r1")),
        comparisonVerdict("CASE n WHEN '3' THEN 1 ELSE 0 END", flags("r1")),
        kept(
          s"SELECT id FROM $comparisonTable WHERE COALESCE(d, d2) BETWEEN '2024-01-01' AND '2024-02-01'",
          Set("r1", "r5")
        ),
        // main refused this one at parse
        kept(s"SELECT id FROM $comparisonTable WHERE COALESCE(n, 0) = '3'", Set("r1")),
        kept(
          s"SELECT id FROM $comparisonTable WHERE COALESCE(d, d2) IN ('2024-01-31', '2024-01-15')",
          Set("r1", "r5")
        ),
        kept(
          s"SELECT g, MAX(n) AS m FROM $comparisonTable GROUP BY g HAVING MAX(n) > '4'",
          Set("g1", "g3")
        ),
        // a DATE against a TIMESTAMP is comparable, judged on the declared types
        comparisonVerdict(
          "CASE d WHEN CAST('2024-01-31' AS DATE) THEN 1 ELSE 0 END",
          flags("r1")
        ),
        comparisonVerdict(
          "NULLIF(COALESCE(d, d2), CAST('2024-01-31' AS DATE))",
          Map(
            "r1" -> Null,
            "r2" -> At(start(on("2024-03-01"))),
            "r3" -> At(start(on("2023-12-25"))),
            "r4" -> Null,
            "r5" -> At(start(on("2024-01-15")))
          )
        ),
        comparisonVerdict(
          "NULLIF(CAST(d AS DATE), CAST(ts AS TIMESTAMP))",
          Map(
            "r1" -> At(start(on("2024-01-31"))),
            "r2" -> At(start(on("2024-03-01"))),
            "r3" -> At(start(on("2023-12-25"))),
            "r4" -> Null,
            "r5" -> Null
          )
        ),
        // the arithmetic reads it too
        comparisonVerdict(
          "d - '2024-01-31'",
          Map("r1" -> Num(0), "r2" -> Num(30), "r3" -> Num(-37), "r4" -> Null, "r5" -> Null)
        ),
        comparisonVerdict(
          "n + '1'",
          Map("r1" -> Num(4), "r2" -> Num(6), "r3" -> Num(-1), "r4" -> Null, "r5" -> Num(11))
        ),
        // a literal that spells nothing the operand reads stays refused by name
        s"SELECT id, CASE WHEN COALESCE(n, 0) = 'a' THEN 1 ELSE 0 END AS v FROM $comparisonTable" ->
        (Try(
          Await.result(
            client.run(
              s"SELECT id, CASE WHEN COALESCE(n, 0) = 'a' THEN 1 ELSE 0 END AS v FROM $comparisonTable"
            ),
            60.seconds
          )
        ).toEither match {
          case Right(ElasticFailure(error))
              if error.message.contains("'BIGINT' is not compatible with 'VARCHAR'") =>
            Right(Nil)
          case other => Right(Seq(s"answered $other instead of the refusal by name"))
        })
      )
    )
  }

  /** The documents of [[numberTable]]: an INT, a BIGINT past INT's range, a DOUBLE with a fraction,
    * and a row holding none of them.
    */
  private val numberDocs = List(
    """{"id":"r1","n":1,"big":1,"x":1.5}""",
    """{"id":"r2","n":2,"big":10000000000,"x":2.0}""",
    """{"id":"r3","n":-3,"big":-3,"x":-1.25}""",
    """{"id":"r4"}"""
  )

  "a quoted number" should
  "be a value of the other operand's type, as PostgreSQL reads it: any number beside a DOUBLE, a whole one beside an integer" in {
    run(s"CREATE TABLE $numberTable (id KEYWORD, n INT, big BIGINT, x DOUBLE, PRIMARY KEY (id))")
    bulk(numberTable, numberDocs)
    def values(r1: Expected, r2: Expected, r3: Expected, r4: Expected): Map[String, Expected] =
      Map("r1" -> r1, "r2" -> r2, "r3" -> r3, "r4" -> r4)
    def refused(sql: String, refusal: String): (String, Either[String, Seq[String]]) =
      sql -> (Try(Await.result(client.run(sql), 60.seconds)).toEither match {
        case Right(ElasticFailure(error)) if error.message.contains(refusal) => Right(Nil)
        case Right(ElasticFailure(error)) => Right(Seq(s"refused as [${error.message}]"))
        case Right(other) => Right(Seq(s"answered $other instead of the refusal by name"))
        case Left(t)      => Left(t.toString)
      })
    verdict(
      "quoted numbers",
      Seq(
        // beside a DOUBLE, any number: x = 1.5, 2.0, -1.25 and NULL
        comparisonVerdict(
          "CASE WHEN COALESCE(x, 0) = '1.5' THEN 1 ELSE 0 END",
          values(Num(1), Num(0), Num(0), Num(0)),
          numberTable
        ),
        comparisonVerdict(
          "CASE WHEN COALESCE(x, 0) = '2' THEN 1 ELSE 0 END",
          values(Num(0), Num(1), Num(0), Num(0)),
          numberTable
        ),
        comparisonVerdict(
          "CASE x WHEN '1.5' THEN 1 ELSE 0 END",
          values(Num(1), Num(0), Num(0), Num(0)),
          numberTable
        ),
        comparisonVerdict(
          "NULLIF(x, '1.5')",
          values(Null, Num(2.0), Num(-1.25), Null),
          numberTable
        ),
        comparisonVerdict("x + '1.5'", values(Num(3.0), Num(3.5), Num(0.25), Null), numberTable),
        comparisonVerdict(
          "x * '2.5'",
          values(Num(3.75), Num(5.0), Num(-3.125), Null),
          numberTable
        ),
        kept(s"SELECT id FROM $numberTable WHERE COALESCE(x, 0) IN ('1.5', '3')", Set("r1")),
        kept(
          s"SELECT id FROM $numberTable WHERE COALESCE(x, 0) BETWEEN '1.0' AND '1.5'",
          Set("r1")
        ),
        kept(s"SELECT id FROM $numberTable WHERE COALESCE(x, 0) > '1.75'", Set("r2")),
        // beside an integer, a whole number is one -- past INT's range too, beside a BIGINT
        comparisonVerdict(
          "CASE WHEN COALESCE(big, 0) = '10000000000' THEN 1 ELSE 0 END",
          values(Num(0), Num(1), Num(0), Num(0)),
          numberTable
        ),
        comparisonVerdict("n + '1'", values(Num(2), Num(3), Num(-2), Null), numberTable),
        // beside an integer, '1.5' and '1.0' are no integer: refused by name, as PostgreSQL 16
        // refuses them (`invalid input syntax for type integer`)
        refused(
          s"SELECT id, CASE WHEN COALESCE(n, 0) = '1.5' THEN 1 ELSE 0 END AS x FROM $numberTable",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        refused(
          s"SELECT id, CASE WHEN NULLIF(n, 0) = '1.0' THEN 1 ELSE 0 END AS x FROM $numberTable",
          "'INT' is not compatible with 'VARCHAR'"
        ),
        refused(
          s"SELECT id, CASE WHEN COALESCE(big, 0) = '1.5' THEN 1 ELSE 0 END AS x FROM $numberTable",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        refused(
          s"SELECT id, CASE WHEN COALESCE(big, 0) = '1.0' THEN 1 ELSE 0 END AS x FROM $numberTable",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        refused(
          s"SELECT id FROM $numberTable WHERE COALESCE(n, 0) IN ('1', '1.5')",
          "'BIGINT' is not compatible with 'VARCHAR'"
        ),
        refused(
          s"SELECT id FROM $numberTable WHERE COALESCE(big, 0) BETWEEN '1' AND '1.0'",
          "output 'BIGINT' is not compatible with input 'VARCHAR'"
        ),
        refused(s"SELECT id, NULLIF(n, '1.5') AS x FROM $numberTable", "compares INT with VARCHAR"),
        refused(
          s"SELECT id, CASE big WHEN '1.0' THEN 1 ELSE 0 END AS x FROM $numberTable",
          "CASE WHEN conditions must be of the same type as the expression"
        ),
        refused(
          s"SELECT id, n + '1.5' AS x FROM $numberTable",
          "output 'INT' is not compatible with input 'VARCHAR'"
        ),
        refused(
          s"SELECT id, big - '1.0' AS x FROM $numberTable",
          "output 'BIGINT' is not compatible with input 'VARCHAR'"
        )
      )
    )
  }

  "DATE_TRUNC(…, DAY) compared with a date" should "run: a day truncation of a date is the date" in {
    verdict(
      "DATE_TRUNC",
      Seq(
        kept(
          s"SELECT id FROM $comparisonTable WHERE DATE_TRUNC(ts, DAY) = '2024-01-31'",
          Set("r1")
        ),
        kept(
          s"SELECT id FROM $comparisonTable WHERE DATE_TRUNC(ts, DAY) > '2024-01-01'",
          Set("r1", "r2")
        ),
        kept(
          s"SELECT id FROM $comparisonTable WHERE DATE_TRUNC(ts, DAY) = CAST('2024-01-31' AS DATE)",
          Set("r1")
        ),
        comparisonVerdict(
          "CASE WHEN DATE_TRUNC(ts, DAY) = '2024-01-31' THEN 1 ELSE 0 END",
          flags("r1")
        )
      )
    )
  }

  /** The STORED value of each computed column of `from`, for each document. */
  private def storedVerdict(
    from: String,
    expected: Seq[(String, Map[String, Expected])]
  ): (String, Either[String, Seq[String]]) = {
    val sql = s"SELECT id, ${expected.map(_._1).mkString(", ")} FROM $from"
    val ids = expected.flatMap(_._2.keys).toSet
    sql -> gatewayRows(sql).map { rs =>
      val byId = rs.map(r => r.getOrElse("id", "?").toString -> r).toMap
      (if (byId.keySet == ids) Nil else Seq(s"rows ${byId.keySet.toSeq.sorted.mkString(",")}")) ++
      (for {
        (column, values) <- expected
        (id, want)       <- values.toSeq.sortBy(_._1)
        diff             <- differs(want, byId.get(id).flatMap(_.get(column)).orNull)
      } yield s"$column $id: $diff")
    }
  }

  /** The documents of [[nullifTable]], PostgreSQL 16's fixture for the answers below: a DATE that
    * is the literals' day and one that is not, TIMESTAMPs at that day's midnight, later that day
    * and the next midnight, a document with no value at all and one with only `d2`.
    */
  private val nullifDocs = List(
    """{"id":"n1","d":"2024-01-31","d2":"2024-02-01","ts":"2024-01-31T00:00:00Z"}""",
    """{"id":"n2","d":"2024-02-01","d2":"2024-01-31","ts":"2024-01-31T10:00:00Z"}""",
    """{"id":"n3","d":"2024-01-31","d2":"2024-01-30","ts":"2024-02-01T00:00:00Z"}""",
    """{"id":"n4"}""",
    """{"id":"n5","d2":"2024-01-31","ts":"2024-01-31T23:59:59.999Z"}"""
  )

  private def nullifAnswers(
    n1: Expected,
    n2: Expected,
    n3: Expected,
    n4: Expected,
    n5: Expected
  ): Map[String, Expected] =
    Map("n1" -> n1, "n2" -> n2, "n3" -> n3, "n4" -> n4, "n5" -> n5)

  /** A DATE beside a date-TIME literal, a TIMESTAMP beside a date-only one: PostgreSQL reads the
    * literal as the operand's type -- the day it names, the midnight it starts at -- and so does
    * the comparison rule. The literal used to be parsed AGAIN on every document, by the operand's
    * own format, which failed for these two shapes: the query failed and a computed column stored
    * NULL. Every expected value is PostgreSQL 16's answer over the same documents.
    */
  "NULLIF with a date or a timestamp literal of the other shape" should
  "answer PostgreSQL's values in a query and STORE them in a computed column, in either argument order" in {
    val columns = Seq(
      ("c1", "DATE", "NULLIF(d, '2024-01-31 10:00:00')"),
      ("c2", "TIMESTAMP", "NULLIF(ts, '2024-01-31')"),
      ("c3", "DATE", "NULLIF('2024-01-31T10:00:00Z', d)"),
      ("c4", "TIMESTAMP", "NULLIF('2024-01-31', ts)"),
      ("c5", "DATE", "NULLIF(COALESCE(d, d2), '2024-01-31T10:00:00Z')")
    )
    run(
      s"CREATE TABLE $nullifTable (id KEYWORD, d DATE, d2 DATE, ts TIMESTAMP, " +
      columns.map { case (c, t, e) => s"$c $t SCRIPT AS ($e)" }.mkString(", ") +
      ", PRIMARY KEY (id))"
    )
    bulk(nullifTable, nullifDocs)
    val jan30 = At(start(on("2024-01-30")))
    val jan31 = At(start(on("2024-01-31")))
    val feb1 = At(start(on("2024-02-01")))
    val feb2 = At(start(on("2024-02-02")))
    val ts2 = At(at("2024-01-31T10:00:00Z"))
    val ts3 = At(at("2024-02-01T00:00:00Z"))
    val ts5 = At(at("2024-01-31T23:59:59.999Z"))
    val day = At(start(today))
    val answers: Seq[(String, Map[String, Expected])] = Seq(
      // the operand first
      "NULLIF(d, '2024-01-31 10:00:00')"  -> nullifAnswers(Null, feb1, Null, Null, Null),
      "NULLIF(d, '2024-01-31T10:00:00Z')" -> nullifAnswers(Null, feb1, Null, Null, Null),
      "NULLIF(ts, '2024-01-31')"          -> nullifAnswers(Null, ts2, ts3, Null, ts5),
      // the literal first: the answer is the literal, read as the operand's type
      "NULLIF('2024-01-31T10:00:00Z', d)" -> nullifAnswers(Null, jan31, Null, jan31, jan31),
      "NULLIF('2024-01-31 10:00:00', d)"  -> nullifAnswers(Null, jan31, Null, jan31, jan31),
      "NULLIF('2024-01-31', ts)"          -> nullifAnswers(Null, jan31, jan31, jan31, jan31),
      "NULLIF('2024-01-31T10:00:00Z', COALESCE(d, d2))" ->
      nullifAnswers(Null, jan31, Null, jan31, Null),
      // operands that are not columns
      "NULLIF(COALESCE(d, d2), '2024-01-31T10:00:00Z')" ->
      nullifAnswers(Null, feb1, Null, Null, Null),
      "NULLIF(CAST(ts AS DATE), '2024-01-31T10:00:00Z')" ->
      nullifAnswers(Null, Null, feb1, Null, Null),
      "NULLIF(CAST(d AS TIMESTAMP), '2024-01-31')" -> nullifAnswers(Null, feb1, Null, Null, Null),
      // PostgreSQL: `d + interval '1 day'` and `d + 1` give the same answers here
      "NULLIF(DATE_ADD(d, INTERVAL 1 DAY), '2024-02-01')" ->
      nullifAnswers(Null, feb2, Null, Null, Null),
      "NULLIF(DATE_TRUNC(ts, DAY), '2024-01-31')" -> nullifAnswers(Null, Null, feb1, Null, Null),
      "NULLIF(LEAST(d, d2), '2024-01-31T10:00:00Z')" ->
      nullifAnswers(Null, Null, jan30, Null, Null),
      "NULLIF(CURRENT_DATE, '2024-01-31T10:00:00Z')" -> nullifAnswers(day, day, day, day, day)
    )
    // the clock: NOW() is never the literal's midnight, so the answer is the instant it was read
    val clockSql = s"SELECT id, NULLIF(NOW(), '2024-01-31') AS x FROM $nullifTable"
    val before = Instant.now().truncatedTo(ChronoUnit.MILLIS)
    val clockRows = gatewayRows(clockSql)
    val after = Instant.now()
    val clock = clockSql -> clockRows.map { rs =>
      val got = byKey(rs, "id")
      Seq("n1", "n2", "n3", "n4", "n5").flatMap { id =>
        differs(Within(before, after), got.getOrElse(id, null)).map(d => s"$id: $d")
      }
    }
    verdict(
      "NULLIF with a temporal literal",
      answers.map { case (expression, values) =>
        comparisonVerdict(expression, values, nullifTable)
      } ++ Seq(
        clock,
        storedVerdict(
          nullifTable,
          columns.map { case (column, _, expression) =>
            column -> answers.toMap.getOrElse(expression, fail(s"no answer for $expression"))
          }
        )
      )
    )
  }

  /** `CASE b WHEN '1'` compares the BOOLEAN with the boolean the literal spells, as PostgreSQL
    * reads it (`'1'`, `'true'`, `'0'`, `'false'`), and `CASE b WHEN TRUE` with the constant. Each
    * used to render the constant through the per-document conversion, `(true ? 1L : 0L)`, which
    * Elasticsearch 6.8's Painless refuses to compile ("Extraneous conditional statement"): the
    * query failed, and so did the CREATE TABLE of a computed column. A document with no `b` matches
    * no `WHEN`. Every expected value is PostgreSQL 16's answer.
    */
  "a simple CASE over a BOOLEAN, compared with a boolean constant" should
  "answer PostgreSQL's values on every major, in a query and in a computed column" in {
    run(
      s"CREATE TABLE $booleanTable (id KEYWORD, b BOOLEAN, " +
      "w1 INT SCRIPT AS (CASE b WHEN '1' THEN 1 ELSE 0 END), " +
      "w2 INT SCRIPT AS (CASE b WHEN TRUE THEN 1 ELSE 0 END), PRIMARY KEY (id))"
    )
    bulk(
      booleanTable,
      List("""{"id":"t1","b":true}""", """{"id":"t2","b":false}""", """{"id":"t3"}""")
    )
    def answers(t1: Expected, t2: Expected, t3: Expected): Map[String, Expected] =
      Map("t1" -> t1, "t2" -> t2, "t3" -> t3)
    val whenTrue = answers(Num(1), Num(0), Num(0))
    val whenFalse = answers(Num(0), Num(1), Num(0))
    verdict(
      "simple CASE over a BOOLEAN",
      Seq(
        comparisonVerdict("CASE b WHEN '1' THEN 1 ELSE 0 END", whenTrue, booleanTable),
        comparisonVerdict("CASE b WHEN 'true' THEN 1 ELSE 0 END", whenTrue, booleanTable),
        comparisonVerdict("CASE b WHEN 'false' THEN 1 ELSE 0 END", whenFalse, booleanTable),
        comparisonVerdict("CASE b WHEN '0' THEN 1 ELSE 0 END", whenFalse, booleanTable),
        comparisonVerdict("CASE b WHEN TRUE THEN 1 ELSE 0 END", whenTrue, booleanTable),
        comparisonVerdict("CASE b WHEN FALSE THEN 1 ELSE 0 END", whenFalse, booleanTable),
        comparisonVerdict(
          "CASE b WHEN '1' THEN 1.5 ELSE 0 END",
          answers(Num(1.5), Num(0), Num(0)),
          booleanTable
        ),
        storedVerdict(booleanTable, Seq("w1" -> whenTrue, "w2" -> whenTrue))
      )
    )
  }

  /** A BOOLEAN column as a CASE condition is NULL on a document that has no value, and a NULL
    * condition is not true: the branch does not apply, and the ELSE does -- or NULL, with no ELSE.
    * It used to throw on that document (`param1 ? 1 : 0` over a null): the query failed, and a
    * computed column stored NULL where SQL gives the ELSE value. Every expected value is PostgreSQL
    * 16's answer.
    */
  "a CASE WHEN over a BOOLEAN column" should
  "take the ELSE branch on a document with no value, in a query, a WHERE and a computed column" in {
    run(
      s"CREATE TABLE $flagTable (id KEYWORD, flag BOOLEAN, n INT, " +
      "c INT SCRIPT AS (CASE WHEN flag THEN 1 ELSE 0 END), " +
      "c2 INT SCRIPT AS (CASE WHEN flag THEN n ELSE 0 END), PRIMARY KEY (id))"
    )
    bulk(
      flagTable,
      List(
        """{"id":"f1","flag":true,"n":5}""",
        """{"id":"f2","flag":false,"n":6}""",
        """{"id":"f3","n":7}"""
      )
    )
    def answers(f1: Expected, f2: Expected, f3: Expected): Map[String, Expected] =
      Map("f1" -> f1, "f2" -> f2, "f3" -> f3)
    verdict(
      "CASE WHEN over a BOOLEAN column",
      Seq(
        comparisonVerdict(
          "CASE WHEN flag THEN 1 ELSE 0 END",
          answers(Num(1), Num(0), Num(0)),
          flagTable
        ),
        comparisonVerdict(
          "CASE WHEN flag THEN n ELSE 0 END",
          answers(Num(5), Num(0), Num(0)),
          flagTable
        ),
        comparisonVerdict("CASE WHEN flag THEN 1 END", answers(Num(1), Null, Null), flagTable),
        kept(
          s"SELECT id FROM $flagTable WHERE CASE WHEN flag THEN 1 ELSE 0 END = 0",
          Set("f2", "f3")
        ),
        storedVerdict(
          flagTable,
          Seq("c" -> answers(Num(1), Num(0), Num(0)), "c2" -> answers(Num(5), Num(0), Num(0)))
        )
      )
    )
  }

  "a pattern's cached schema" should "be refreshed by an elasticsql DDL statement on an index it matches" in {
    val from = "date_arithmetic_rf_*"
    def typed(sql: String, refusal: String): Boolean =
      Try(Await.result(client.run(sql), 60.seconds)).toEither match {
        case Right(ElasticFailure(error)) => error.message.contains(refusal)
        case _                            => false
      }
    val dTimesTwo = s"SELECT id, d * 2 AS x FROM $from"
    val refusal = "operator * cannot be applied to DATE and BIGINT"
    run(s"CREATE TABLE $refreshedA (id KEYWORD, d DATE, PRIMARY KEY (id))")
    bulk(refreshedA, List("""{"id":"a1","d":"2024-01-31"}"""))
    // the pattern's merged schema is read, and cached: `d` is a DATE
    typed(dTimesTwo, refusal) shouldBe true
    // CREATE: a second index maps `d` as a KEYWORD -- the indices disagree, `d` has no type
    run(s"CREATE TABLE $refreshedB (id KEYWORD, d KEYWORD, PRIMARY KEY (id))")
    typed(dTimesTwo, refusal) shouldBe false
    // DROP: `d` is a DATE again
    run(s"DROP TABLE $refreshedB")
    typed(dTimesTwo, refusal) shouldBe true
    // ALTER: a column added to the one index is known to the pattern at once
    run(s"ALTER TABLE $refreshedA ADD COLUMN m DATE")
    typed(s"SELECT id, m * 2 AS x FROM $from", refusal) shouldBe true
  }

  "an index pattern or a comma list" should "answer like the single index it matches" in {
    run(s"CREATE TABLE $patternA ($columns, PRIMARY KEY (id))")
    run(s"CREATE TABLE $patternB ($columns, PRIMARY KEY (id))")
    bulk(patternA, rows.map(json).toList)
    client.refresh(patternB)
    val samples =
      rowForms.filter(f => Set("d - d2", "d + 1", "ts - ts2", "d + 1.5").contains(f.sql))
    val groupSamples =
      groupForms.filter(f =>
        Set("MAX(d) - MIN(d)", "MAX(ts) - MIN(ts)", "MAX(d) + 1").contains(f.sql)
      )
    Seq("date_arithmetic_pat_*", s"$patternA,$patternB").foreach { from =>
      verdict(
        s"FROM $from",
        rowVerdicts(samples, from, rows) ++ groupVerdicts(groupSamples, from) ++ Seq {
          // a type rule reads the merged schema: refused by name, as over the single index
          val sql = s"SELECT id, d * 2 AS x FROM $from"
          sql -> (Try(Await.result(client.run(sql), 60.seconds)).toEither match {
            case Right(ElasticFailure(error))
                if error.message.contains("operator * cannot be applied to DATE and BIGINT") =>
              Right(Nil)
            case other => Right(Seq(s"answered $other instead of the refusal by name"))
          })
        } :+ {
          // a subquery over the pattern is scoped -- and typed -- by its merged mapping, as one
          // over the single index is
          val sql =
            s"SELECT id FROM $table WHERE g IN (SELECT g FROM $from WHERE d > CAST('2024-01-01' AS DATE))"
          sql -> gatewayRows(sql).map { rs =>
            val kept = rs.map(_.getOrElse("id", "?").toString).toSet
            val groups = rows.filter(_.d.exists(_.isAfter(on("2024-01-01")))).map(_.g).toSet
            val want = rows.filter(r => groups.contains(r.g)).map(_.id).toSet
            if (kept == want) Nil
            else
              Seq(
                s"kept ${kept.toSeq.sorted.mkString(",")}, expected ${want.toSeq.sorted.mkString(",")}"
              )
          }
        }
      )
    }
  }

  "an index pattern whose indices disagree on a field" should
  "type every field they agree on, and refuse nothing on that one: Elasticsearch answers" in {
    run(s"CREATE TABLE $disagreeA ($columns, PRIMARY KEY (id))")
    run(s"CREATE TABLE $disagreeB (id KEYWORD, g KEYWORD, d KEYWORD, PRIMARY KEY (id))")
    bulk(disagreeA, rows.map(json).toList)
    client.refresh(disagreeB)
    val from = "date_arithmetic_dis_*"
    // the indices agree on every field but `d` (DATE and KEYWORD): every other field is typed as
    // over the single index -- the date-arithmetic rules apply, and a type rule refuses by name
    verdict(
      s"FROM $from",
      rowVerdicts(
        rowForms.filter(f =>
          Set("ts - ts2", "ts + 1", "ts - 1.5", "LEAST(ts, ts2)").contains(f.sql)
        ),
        from,
        rows
      ) ++ groupVerdicts(
        groupForms.filter(f => Set("MAX(ts) - MIN(ts)", "MAX(ts) + 1").contains(f.sql)),
        from
      ) :+ {
        val sql = s"SELECT id, ts * 2 AS x FROM $from"
        sql -> (Try(Await.result(client.run(sql), 60.seconds)).toEither match {
          case Right(ElasticFailure(error))
              if error.message.contains("operator * cannot be applied to TIMESTAMP and BIGINT") =>
            Right(Nil)
          case other => Right(Seq(s"answered $other instead of the refusal by name"))
        })
      }
    )
    // the field they disagree on has no type: nothing is refused on it, Elasticsearch answers
    // (MEASURED, reported)
    Seq(
      s"SELECT g, MAX(d) - MIN(d) AS x FROM $from GROUP BY g",
      s"SELECT id, d + 1 AS x FROM $from",
      s"SELECT id, d * 2 AS x FROM $from"
    ).foreach { sql =>
      val answer = Try(Await.result(client.run(sql), 60.seconds)).toEither match {
        case Right(ElasticFailure(error)) =>
          // never core's type refusal: with no type nothing is refused on types
          error.message should not include "cannot be applied to"
          error.message should not include "Type mismatch"
          s"FAILURE ${error.statusCode.getOrElse("-")} ${error.message.take(200)}"
        case Right(ElasticSuccess(_)) =>
          s"SUCCESS ${gatewayRows(sql).map(_.take(3)).fold(identity, _.toString).take(300)}"
        case Left(t) => s"THROW ${t.getMessage.take(200)}"
      }
      info(s"[$sql] -> $answer")
    }
  }

  "a combination the type rules refuse" should "be refused by name once the column types are known, before anything runs" in {
    val remedy = "subtract two dates for the days between them"
    val refused = Seq(
      s"SELECT id, d * 2 AS x FROM $table" -> Seq(
        "operator * cannot be applied to DATE and BIGINT",
        remedy
      ),
      s"SELECT id, d + d2 AS x FROM $table" -> Seq(
        "operator + cannot be applied to DATE and DATE",
        remedy
      ),
      s"SELECT id, 2 - d AS x FROM $table" -> Seq(
        "operator - cannot be applied to BIGINT and DATE",
        remedy
      ),
      s"SELECT id, ts / ts2 AS x FROM $table" -> Seq(
        "operator / cannot be applied to TIMESTAMP and TIMESTAMP",
        remedy
      ),
      s"SELECT g, MAX(d) / 1000 AS x FROM $table GROUP BY g" -> Seq(
        "operator / cannot be applied to DATE and BIGINT",
        remedy
      ),
      s"SELECT g, MAX(d) % MIN(d) AS x FROM $table GROUP BY g" -> Seq(
        "operator % cannot be applied to DATE and DATE",
        remedy
      ),
      s"SELECT id FROM $table WHERE d * 2 > 1" -> Seq(
        "operator * cannot be applied to DATE and BIGINT",
        remedy
      ),
      s"SELECT id, d + 'x' AS x FROM $table" -> Seq(
        "operator + cannot be applied to DATE and VARCHAR",
        remedy
      ),
      s"CREATE TABLE date_arithmetic_refused ($columns, c BIGINT SCRIPT AS (d * 2), PRIMARY KEY (id))" ->
      Seq("operator * cannot be applied to DATE and BIGINT", remedy),
      // every other TYPE rule that waits for the column types (the lead's ruling of 2026-10-05)
      s"SELECT id FROM $table WHERE n + 1 > 'abc'" -> Seq(
        "'BIGINT' is not compatible with 'VARCHAR'"
      ),
      s"SELECT id, n + id AS x FROM $table" -> Seq(
        "output 'INT' is not compatible with input 'KEYWORD'"
      ),
      s"SELECT id, CASE n WHEN 'a' THEN 1 ELSE 0 END AS x FROM $table" -> Seq(
        "CASE WHEN conditions must be of the same type as the expression"
      ),
      s"SELECT id, GREATEST(d, n) AS x FROM $table" -> Seq(
        "GREATEST requires numeric arguments, or date and timestamp arguments, but got DATE for d " +
        "and INT for n"
      ),
      // (`NULLIF(CAST(d AS DATE), CAST(ts AS TIMESTAMP))` compares a DATE with a TIMESTAMP, which
      // the ONE comparison rule accepts: it RUNS, in "the ONE comparison rule" above)
      s"SELECT id, NULLIF(n, 'a') AS x FROM $table" -> Seq("compares INT with VARCHAR"),
      // one true mismatch per rule that waits for the column types
      s"SELECT id FROM $table WHERE id = n" -> Seq("'KEYWORD' is not compatible with 'INT'"),
      s"SELECT id FROM $table WHERE CAST(ts AS TIME) > ts" -> Seq(
        "a TIME value cannot be compared"
      ),
      s"SELECT id FROM $table WHERE CAST(n AS INT) IN ('a', 'b')" -> Seq(
        "'INT' is not compatible with 'VARCHAR'"
      ),
      s"SELECT g FROM $table GROUP BY g HAVING COUNT(n) IN ('a', 'b')" -> Seq(
        "'BIGINT' is not compatible with 'VARCHAR'"
      ),
      s"SELECT id FROM $table WHERE n BETWEEN 1 AND 'a'" -> Seq(
        "output 'BIGINT' is not compatible with input 'VARCHAR'"
      ),
      s"SELECT id FROM $table WHERE CAST(id AS INT) BETWEEN 'a' AND 'b'" -> Seq(
        "output 'INT' is not compatible with input 'VARCHAR'"
      ),
      s"SELECT id, NULLIF(id, 1) AS x FROM $table" -> Seq("compares KEYWORD with BIGINT"),
      s"SELECT id, CASE WHEN n THEN 1 ELSE 0 END AS x FROM $table" -> Seq(
        "CASE WHEN conditions must be of type BOOLEAN"
      ),
      s"SELECT id, LEAST('a', d) AS x FROM $table" -> Seq(
        "LEAST requires numeric arguments, or date and timestamp arguments, but got VARCHAR for 'a'"
      ),
      s"SELECT id, 1 + 'abc' AS x FROM $table" -> Seq("input 'VARCHAR'"),
      s"SELECT id FROM $table UNION ALL SELECT n FROM $table" -> Seq(
        "compatible types at column 1"
      )
    )
    val outcomes = refused.map { case (sql, refusal) =>
      sql -> (Try(Await.result(client.run(sql), 60.seconds)).toEither match {
        case Right(ElasticFailure(error)) if refusal.forall(error.message.contains) =>
          Right(Nil)
        case Right(ElasticFailure(error)) => Right(Seq(s"refused as [${error.message}]"))
        case Right(other)                 => Right(Seq(s"answered $other instead of refusing"))
        case Left(t)                      => Left(t.toString)
      })
    }
    Try(Await.result(client.run("DROP TABLE IF EXISTS date_arithmetic_refused"), 60.seconds))
    verdict("refusals", outcomes)
  }

  "a CREATE OR REPLACE TABLE ... AS SELECT the rules refuse" should
  "be refused before the existing table is touched" in {
    val target = "date_arithmetic_ctas"
    try {
      run(s"CREATE OR REPLACE TABLE $target AS SELECT id, d - d2 AS x FROM $table")
      client.refresh(target)
      gatewayRows(s"SELECT id, x FROM $target").map(_.size) shouldBe Right(rows.size)
      Seq(
        s"SELECT g, MAX(d) / 1000 AS x FROM $table GROUP BY g" ->
        "operator / cannot be applied to DATE and BIGINT",
        // a type rule that no longer refuses at parse, and must still refuse before the replace
        s"SELECT id, n + id AS x FROM $table" ->
        "output 'INT' is not compatible with input 'KEYWORD'"
      ).foreach { case (select, refusal) =>
        Try(
          Await.result(client.run(s"CREATE OR REPLACE TABLE $target AS $select"), 60.seconds)
        ).toEither match {
          case Right(ElasticFailure(error)) => error.message should include(refusal)
          case other => fail(s"[$select] answered $other instead of refusing")
        }
        // the table it would have replaced is still there, with its rows
        gatewayRows(s"SELECT id, x FROM $target").map(_.size) shouldBe Right(rows.size)
      }
    } finally Try(Await.result(client.run(s"DROP TABLE IF EXISTS $target"), 60.seconds))
  }
}
