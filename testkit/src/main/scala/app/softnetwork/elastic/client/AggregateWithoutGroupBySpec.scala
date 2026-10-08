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
import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** Issue #413 -- a calculation over aggregates with NO `GROUP BY` answers ONE row, the whole table
  * being one implicit group: `SELECT MAX(n) - MIN(n) FROM t`, `COUNT(*) * 2`, `MAX(d) + 1`,
  * `GREATEST(MAX(d), MAX(d2))`, `DATEDIFF(MAX(d), MIN(d))`. Elasticsearch refused every one of them
  * on every major (a `bucket_script` at the top level of `aggs`).
  *
  * Every expected value is computed HERE, from the fixture, as DuckDB 1.5.5 and PostgreSQL 16
  * compute it (MEASURED over the same rows), with the date rules of the engine (a DATE moved by
  * whole days is a DATE, DATE - DATE is whole days). The value is read as the per-group calculation
  * of the same expression is -- a date as the instant its day starts at, which a calculation
  * answers as epoch milliseconds -- since the whole table is computed exactly as a group is.
  */
trait AggregateWithoutGroupBySpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "aggregate_without_group_by"

  private final case class Doc(
    id: String,
    g: String,
    n: Option[Int],
    x: Option[Double],
    d: Option[LocalDate],
    d2: Option[LocalDate],
    ts: Option[Instant]
  )

  private def day(s: String): Option[LocalDate] = Some(LocalDate.parse(s))
  private def at(s: String): Option[Instant] = Some(Instant.parse(s))

  private val docs: Seq[Doc] = Seq(
    Doc(
      "r1",
      "g1",
      Some(3),
      Some(0.25),
      day("2024-01-31"),
      day("2024-02-01"),
      at("2024-01-31T10:30:00Z")
    ),
    Doc(
      "r2",
      "g1",
      Some(5),
      Some(1.5),
      day("2024-03-01"),
      day("2024-02-28"),
      at("2024-03-01T06:00:00Z")
    ),
    Doc("r3", "g2", Some(-2), None, day("2023-12-25"), None, at("2023-12-25T23:59:59.999Z")),
    Doc("r4", "g3", None, Some(2.0), None, day("2024-01-31"), None),
    Doc(
      "r5",
      "g3",
      Some(10),
      Some(-1.25),
      day("2024-02-29"),
      day("2024-02-29"),
      at("2024-02-29T00:00:00Z")
    )
  )

  private def json(d: Doc): String = {
    def q(s: String) = "\"" + s + "\""
    Seq(
      Some("id" -> q(d.id)),
      Some("g"  -> q(d.g)),
      d.n.map(v => "n" -> v.toString),
      d.x.map(v => "x" -> v.toString),
      d.d.map(v => "d" -> q(v.toString)),
      d.d2.map(v => "d2" -> q(v.toString)),
      d.ts.map(v => "ts" -> q(v.toString))
    ).flatten.map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $table"), 60.seconds))
    Await.result(
      client.run(
        s"CREATE TABLE $table (id KEYWORD, g KEYWORD, n INT, x DOUBLE, d DATE, d2 DATE, " +
        "ts TIMESTAMP, PRIMARY KEY (id))"
      ),
      60.seconds
    ) match {
      case ElasticSuccess(_)     =>
      case ElasticFailure(error) => fail(s"CREATE TABLE failed: ${error.message}")
    }
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = table, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs.map(json).toList, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(r) if r.failedCount == 0 => client.refresh(table)
      case other                                   => fail(s"bulk failed: $other")
    }
    ()
  }

  override def afterAll(): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $table"), 60.seconds))
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // The oracle
  // -------------------------------------------------------------------------------------------

  private implicit val dayOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isBefore _)

  private def max[T](values: Seq[Option[T]])(implicit o: Ordering[T]): Option[T] =
    values.flatten.reduceOption((a, b) => if (o.gteq(a, b)) a else b)
  private def min[T](values: Seq[Option[T]])(implicit o: Ordering[T]): Option[T] =
    values.flatten.reduceOption((a, b) => if (o.lteq(a, b)) a else b)

  private def where(p: Doc => Boolean): Seq[Doc] = docs.filter(p)

  private def start(d: LocalDate): Instant = d.atStartOfDay(ZoneOffset.UTC).toInstant

  /** An expected cell: a number, a day (the instant it starts at), or NULL. */
  private sealed trait Cell
  private final case class Num(value: Double) extends Cell
  private final case class Day(value: LocalDate) extends Cell
  private case object Null extends Cell

  private def num(v: Option[Double]): Cell = v.map(Num).getOrElse(Null)
  private def date(v: Option[LocalDate]): Cell = v.map(Day).getOrElse(Null)

  private val all = docs
  private val ns = all.map(_.n)
  private val ds = all.map(_.d)

  /** statement -> the ONE row it answers, column by column (`None`: no row, a false HAVING) */
  private val expected: Seq[(String, Option[Seq[(String, Cell)]])] = Seq(
    // the issue's five statements
    s"SELECT MAX(n) - MIN(n) AS v FROM $table" ->
    Some(Seq("v" -> num(for (a <- max(ns); b <- min(ns)) yield (a - b).toDouble))),
    s"SELECT COUNT(*) * 2 AS v FROM $table" -> Some(Seq("v" -> Num(all.size * 2.0))),
    s"SELECT MAX(d) + 1 AS v FROM $table"   -> Some(Seq("v" -> date(max(ds).map(_.plusDays(1))))),
    s"SELECT GREATEST(MAX(d), MAX(d2)) AS v FROM $table" ->
    Some(Seq("v" -> date(max(Seq(max(ds), max(all.map(_.d2))))))),
    s"SELECT DATEDIFF(MAX(d), MIN(d)) AS v FROM $table" ->
    Some(
      Seq("v" -> num(for (a <- max(ds); b <- min(ds)) yield ChronoUnit.DAYS.between(b, a).toDouble))
    ),
    // unaliased, beside a plain aggregate, over a filter, with LIMIT and ORDER BY
    s"SELECT MAX(n) - MIN(n) FROM $table" ->
    Some(Seq("__c1" -> num(for (a <- max(ns); b <- min(ns)) yield (a - b).toDouble))),
    s"SELECT MAX(n) AS m, MAX(n) - MIN(n) AS v FROM $table" ->
    Some(
      Seq(
        "m" -> num(max(ns).map(_.toDouble)),
        "v" -> num(for (a <- max(ns); b <- min(ns)) yield (a - b).toDouble)
      )
    ),
    s"SELECT MAX(n) - MIN(n) AS v FROM $table WHERE g = 'g1'" ->
    Some(Seq("v" -> {
      val g1 = where(_.g == "g1").map(_.n)
      num(for (a <- max(g1); b <- min(g1)) yield (a - b).toDouble)
    })),
    s"SELECT MAX(n) - MIN(n) AS v FROM $table LIMIT 5" ->
    Some(Seq("v" -> num(for (a <- max(ns); b <- min(ns)) yield (a - b).toDouble))),
    s"SELECT MAX(n) - MIN(n) AS v FROM $table ORDER BY v" ->
    Some(Seq("v" -> num(for (a <- max(ns); b <- min(ns)) yield (a - b).toDouble))),
    // a fractional result, a DATE - DATE, a DATE moved back, a LEAST
    s"SELECT MAX(x) - MIN(x) AS v FROM $table" ->
    Some(Seq("v" -> num(for (a <- max(all.map(_.x)); b <- min(all.map(_.x))) yield a - b))),
    s"SELECT AVG(n) * 2 AS v FROM $table" ->
    Some(Seq("v" -> Num(ns.flatten.sum.toDouble / ns.flatten.size * 2))),
    s"SELECT MAX(d) - MIN(d) AS v FROM $table" ->
    Some(
      Seq("v" -> num(for (a <- max(ds); b <- min(ds)) yield ChronoUnit.DAYS.between(b, a).toDouble))
    ),
    s"SELECT MIN(d) - 1 AS v FROM $table" -> Some(Seq("v" -> date(min(ds).map(_.minusDays(1))))),
    s"SELECT LEAST(MIN(d), MIN(d2)) AS v FROM $table" ->
    Some(Seq("v" -> date(min(Seq(min(ds), min(all.map(_.d2))))))),
    s"SELECT COUNT(DISTINCT g) * 10 AS v FROM $table" ->
    Some(Seq("v" -> Num(all.map(_.g).distinct.size * 10.0))),
    // over no document: a MAX is NULL, and so is the calculation
    s"SELECT MAX(n) - MIN(n) AS v FROM $table WHERE g = 'none'" -> Some(Seq("v" -> Null)),
    s"SELECT MAX(d) + 1 AS v FROM $table WHERE g = 'none'"      -> Some(Seq("v" -> Null)),
    // with a HAVING: one row when it holds, none when it does not
    s"SELECT COUNT(*) * 2 AS v FROM $table HAVING COUNT(*) > 1" ->
    Some(Seq("v" -> Num(all.size * 2.0))),
    s"SELECT MAX(n) - MIN(n) AS v FROM $table HAVING MAX(n) - MIN(n) > 100" -> None
  )

  private def asNumber(v: Any): Option[Double] = v match {
    case n: java.lang.Number => Some(n.doubleValue())
    case _                   => None
  }

  private def asInstant(v: Any): Option[Instant] = v match {
    case n: java.lang.Number => Some(Instant.ofEpochMilli(Math.round(n.doubleValue())))
    case l: LocalDate        => Some(start(l))
    case z: ZonedDateTime    => Some(z.toInstant)
    case i: Instant          => Some(i)
    case _                   => None
  }

  private def differs(cell: Cell, actual: Any): Option[String] = cell match {
    case Null => if (actual == null) None else Some(s"$actual instead of NULL")
    case Num(want) =>
      asNumber(actual) match {
        case Some(got) if Math.abs(got - want) <= 1e-9 * Math.max(1.0, Math.abs(want)) => None
        case _ => Some(s"$actual instead of $want")
      }
    case Day(want) =>
      asInstant(actual) match {
        case Some(got) if got == start(want) => None
        case _                               => Some(s"$actual instead of $want")
      }
  }

  private def rows(sql: String): Either[String, Seq[ListMap[String, Any]]] =
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

  /** The Elasticsearch major the client talks to. */
  private lazy val esMajor: Int =
    client.version match {
      case ElasticSuccess(v) => v.takeWhile(_ != '.').toInt
      case ElasticFailure(_) => 0
    }

  /** Over ZERO documents (the lead's ruling of 2026-10-06): `COUNT(*)` is 0 on every major (the
    * bucket's own `_count`); `COUNT(x)` and `COUNT(DISTINCT x)` are 0 from Elasticsearch 7.17
    * (`keep_values`), and NULL on 6.8, which refuses that gap policy -- an Elasticsearch
    * limitation; a `SUM` is core's 0.0 there too; a `MIN`, a `MAX` or an `AVG` is NULL, and so is a
    * calculation reading one, as DuckDB 1.5.5 and PostgreSQL 16 answer (MEASURED). `HAVING COUNT(*)
    * = 0` keeps the one row.
    */
  private def overNothing: Seq[(String, Option[Seq[(String, Cell)]])] = {
    val none = s"FROM $table WHERE g = 'none'"
    def countOfValues(v: Double): Cell = if (esMajor >= 7) Num(v) else Null
    Seq(
      s"SELECT COUNT(*) * 2 AS v $none"                 -> Some(Seq("v" -> Num(0))),
      s"SELECT COUNT(*) AS c, COUNT(*) * 2 AS v $none"  -> Some(Seq("c" -> Num(0), "v" -> Num(0))),
      s"SELECT COUNT(n) * 2 AS v $none"                 -> Some(Seq("v" -> countOfValues(0))),
      s"SELECT COUNT(*) + COUNT(n) AS v $none"          -> Some(Seq("v" -> countOfValues(0))),
      s"SELECT COUNT(DISTINCT g) * 10 AS v $none"       -> Some(Seq("v" -> countOfValues(0))),
      s"SELECT SUM(x) * 2 AS v $none"                   -> Some(Seq("v" -> countOfValues(0))),
      s"SELECT COUNT(*) + MAX(n) AS v $none"            -> Some(Seq("v" -> Null)),
      s"SELECT AVG(n) * 2 AS v $none"                   -> Some(Seq("v" -> Null)),
      s"SELECT DATEDIFF(MAX(d), MIN(d)) AS v $none"     -> Some(Seq("v" -> Null)),
      s"SELECT GREATEST(MAX(d), MAX(d2)) AS v $none"    -> Some(Seq("v" -> Null)),
      s"SELECT COUNT(*) AS c $none HAVING COUNT(*) = 0" -> Some(Seq("c" -> Num(0))),
      s"SELECT COUNT(*) AS c $none HAVING COUNT(*) > 0" -> None,
      s"SELECT COUNT(*) * 2 AS v $none HAVING COUNT(*) = 0" -> Some(Seq("v" -> Num(0))),
      // one document without the column: its count is 0, its MAX NULL, on every major
      s"SELECT COUNT(x) * 2 AS v FROM $table WHERE id = 'r3'"    -> Some(Seq("v" -> Num(0))),
      s"SELECT MAX(x) - MIN(x) AS v FROM $table WHERE id = 'r3'" -> Some(Seq("v" -> Null))
    )
  }

  private def mismatches(statements: Seq[(String, Option[Seq[(String, Cell)]])]): Seq[String] =
    statements.flatMap { case (sql, want) =>
      rows(sql) match {
        case Left(error) => Seq(s"[$sql] failed: $error")
        case Right(rs) =>
          want match {
            case None =>
              if (rs.isEmpty) Nil else Seq(s"[$sql] answered ${rs.size} rows instead of none")
            case Some(cells) =>
              if (rs.size != 1) Seq(s"[$sql] answered ${rs.size} rows instead of one: $rs")
              else
                cells.flatMap { case (column, cell) =>
                  differs(cell, rs.head.getOrElse(column, null)).map(e => s"[$sql] $column: $e")
                }
          }
      }
    }

  "A calculation over aggregates without GROUP BY" should "answer ONE row, with the values SQL engines answer" in {
    mismatches(expected) shouldBe empty
  }

  it should "answer the counts of no document, and NULL for a MIN or a MAX of none" in {
    mismatches(overNothing) shouldBe empty
  }
}
