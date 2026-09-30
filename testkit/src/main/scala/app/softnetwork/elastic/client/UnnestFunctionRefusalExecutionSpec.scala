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
import app.softnetwork.elastic.sql.query.{SelectStatement, UnnestScope}
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/** A function over an UNNEST column that Elasticsearch evaluates against a document that does not
  * hold the column is REFUSED by name -- at the client API and at the gateway (the path the JDBC
  * driver, the REPL and the Flight SQL sidecar take) -- while what already worked keeps its ROWS.
  *
  * Before the rule, MEASURED at RUN level on ES 8.18.3 and 6.8.23: a projected function returned
  * the RAW nested value (a column carrying the chain) or dropped the UNNEST and returned one NULL
  * row per parent (a chain with no named carrier); a WHERE function with no nested carrier returned
  * zero rows, or every row. Every one of those answers came with HTTP 200.
  *
  * Oracles are computed from the fixture in Scala, never from the engine.
  */
trait UnnestFunctionRefusalExecutionSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "unnest_function_refusal"

  /** The five column types of the nested object (`UnnestPopulation.nestedCols`), and a key. */
  private case class Item(k: String, d: String, s: String, n: Long, x: Double, b: Boolean)
  private case class Parent(id: String, name: String, items: Seq[Item])

  /** Today at noon (UTC), for the one row whose answer is relative to the current date. */
  private val today: String =
    _root_.java.time.LocalDate.now(_root_.java.time.ZoneOffset.UTC).toString + "T12:00:00Z"

  /** Two elements per parent at most: how many elements an UNNEST projection returns per parent is
    * not this spec's question (`UnnestInnerHitsSizeExecutionSpec`). `p4` has NO element: an UNNEST
    * is an inner join, it contributes no row.
    */
  private val parents: Seq[Parent] = Seq(
    Parent(
      "p1",
      "Alpha",
      Seq(
        Item("a", "2025-02-10T12:00:00Z", "abc", 7L, 1.5, b = true),
        Item("b", "2024-03-15T08:30:00Z", "xyz", 3L, -2.25, b = false)
      )
    ),
    Parent(
      "p2",
      "beta",
      Seq(
        Item("c", "2025-02-28T00:00:00Z", "ABC", 7L, 0.0, b = true),
        Item("d", "2023-12-31T23:59:59Z", "beta", 1L, 7.0, b = false)
      )
    ),
    Parent(
      "p3",
      "Gamma",
      Seq(
        Item("e", "2025-06-15T10:00:00Z", "mixed Case", 12L, 12.5, b = true),
        Item("f", today, "today", 5L, 3.0, b = false)
      )
    ),
    Parent("p4", "delta", Nil)
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mapping =
      """{
        |  "properties": {
        |    "id":    { "type": "keyword" },
        |    "name":  { "type": "keyword" },
        |    "items": { "type": "nested", "properties": {
        |      "k": { "type": "keyword" },
        |      "d": { "type": "date" },
        |      "s": { "type": "keyword" },
        |      "n": { "type": "long" },
        |      "x": { "type": "double" },
        |      "b": { "type": "boolean" }
        |    } }
        |  }
        |}""".stripMargin
    client
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mapping).get shouldBe true
    val docs = parents.map { p =>
      val items = p.items
        .map(it =>
          s"""{"k":"${it.k}","d":"${it.d}","s":"${it.s}","n":${it.n},"x":${it.x},"b":${it.b}}"""
        )
        .mkString("[", ",", "]")
      s"""{"id":"${p.id}","name":"${p.name}","items":$items}"""
    }.toList
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 10)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_)     => // ok
      case ElasticFailure(error) => fail(s"Bulk indexing into $index failed: ${error.message}")
    }
    client.refresh(index)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    super.afterAll()
  }

  private def u(alias: String): String = s"FROM $index t JOIN UNNEST(t.items) AS $alias"

  /** A failure that surfaced as a thrown exception (a failed future, a stream that fails while it
    * is drained) -- kept as a `Left`, so a caller COLLECTS it instead of aborting the test on it.
    */
  private def thrown(e: Throwable): ElasticError =
    ElasticError(message = s"threw ${e.getClass.getSimpleName}: ${e.getMessage}", cause = Some(e))

  /** The client API: a statement that does not parse is an `ElasticFailure` carrying the parser's
    * reason (`SearchApi.invalidSearchRequest`).
    */
  private def viaClient(sql: String): Either[ElasticError, Seq[ListMap[String, Any]]] =
    Try(client.search(SelectStatement(sql))) match {
      case Success(ElasticSuccess(response)) => Right(response.results)
      case Success(ElasticFailure(error))    => Left(error)
      case Failure(e)                        => Left(thrown(e))
    }

  /** The gateway: every result shape collected to rows. A `QueryStream` fails while it is DRAINED
    * (an Elasticsearch error on a scroll-routed page), so the drain is inside the `Try` too.
    */
  private def viaGateway(sql: String): Either[ElasticError, Seq[ListMap[String, Any]]] = {
    val answered: Try[Either[ElasticError, Seq[ListMap[String, Any]]]] =
      Try(Await.result(client.run(sql), 60.seconds)).flatMap {
        case ElasticSuccess(QueryRows(rows, _))           => Success(Right(rows))
        case ElasticSuccess(QueryStructured(response, _)) => Success(Right(response.results))
        case ElasticSuccess(QueryStream(stream, _)) =>
          Try(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)).map(Right(_))
        case ElasticSuccess(other) =>
          Success(Left(ElasticError(message = s"unexpected result $other")))
        case ElasticFailure(error) => Success(Left(error))
      }
    answered match {
      case Success(result) => result
      case Failure(e)      => Left(thrown(e))
    }
  }

  /** Elasticsearch wraps every `script_fields` value in a per-field array; flattened to the scalar
    * the assertions talk about.
    */
  private def scalar(v: Any): String = v match {
    case s: Seq[_]                         => s.headOption.map(scalar).getOrElse("null")
    case a: _root_.java.util.Collection[_] => scalar(a.toArray.toSeq)
    case null                              => "null"
    case other                             => other.toString
  }

  private def keys(rows: Seq[ListMap[String, Any]], cols: String*): Seq[String] =
    rows.map(r => cols.map(c => scalar(r.getOrElse(c, null))).mkString("|")).sorted

  /** Every statement must be refused by THIS rule at the client API and at the gateway. The wrong
    * ones are COLLECTED and asserted once, so a run on the unfixed tree (T0-R) prints what each
    * statement answered instead -- the BASE answer, row by row -- rather than stopping at the
    * first.
    */
  private def assertAllRefused(clause: String)(sqls: String*): Unit = {
    val phrase = s"${UnnestScope.StablePhrase}$clause: "
    def rows(r: Seq[ListMap[String, Any]]): String =
      s"answered ${r.size} row(s): ${r.map(_.values.map(scalar).mkString("|")).mkString("; ")}"
    val wrong = sqls.flatMap { sql =>
      val client = viaClient(sql) match {
        case Left(e) if e.message.startsWith(phrase) => None
        case Left(e)  => Some(s"client refused otherwise: ${e.message}")
        case Right(r) => Some(s"client ${rows(r)}")
      }
      val gateway = viaGateway(sql) match {
        case Left(e) if e.statusCode.contains(400) && e.message.contains(phrase) => None
        case Left(e)  => Some(s"gateway refused otherwise: ${e.statusCode} ${e.message}")
        case Right(r) => Some(s"gateway ${rows(r)}")
      }
      (client.toSeq ++ gateway.toSeq).map(w => s"[$sql] $w")
    }
    wrong shouldBe empty
  }

  "a function projected over an UNNEST column" should "be refused by name at the client and at the gateway" in {
    assertAllRefused("SELECT")(
      s"SELECT id, YEAR(i.d) AS c ${u("i")}",
      s"SELECT id, UPPER(i.s) AS c ${u("i")}",
      s"SELECT id, i.n + 2 AS c ${u("i")}",
      // right only by coincidence before (the value survives the RAW overwrite): refused uniformly
      s"SELECT id, CAST(i.n AS BIGINT) AS c ${u("i")}",
      s"SELECT id, COALESCE(i.s, 'z') AS c ${u("i")} LIMIT 100",
      // IDENT-1 makes WEEKDAY resolve its receiver; the projection must be refused under BOTH
      // spellings -- a script error under `AS i` before, the RAW value under `AS items`
      s"SELECT id, WEEKDAY(i.d) AS c ${u("i")}",
      s"SELECT id, WEEKDAY(items.d) AS c ${u("items")}",
      // an index PATTERN is resolved once, without a mapping: the rule does not need one
      s"SELECT id, UPPER(i.s) AS c FROM ${index.dropRight(1)}* t JOIN UNNEST(t.items) AS i",
      // beside a window function the rows come from the window-free statement, whose script field
      // is evaluated per parent (MEASURED: NULL for UPPER, the raw dates for YEAR, 6 rows)
      s"SELECT id, i.k AS k, UPPER(i.s) AS c, SUM(i.n) OVER (PARTITION BY id) AS tot ${u("i")}",
      s"SELECT id, YEAR(i.d) AS y, SUM(i.n) OVER (PARTITION BY id) AS tot ${u("i")} LIMIT 100"
    )
  }

  "a WHERE function over an UNNEST column with no nested carrier" should "be refused by name" in {
    assertAllRefused("WHERE")(
      s"SELECT id, i.k AS k ${u("i")} WHERE UPPER(i.s) = 'ABC'",
      s"SELECT id, i.k AS k ${u("i")} WHERE ABS(i.n) = 7",
      // answered EVERY row before (MEASURED): the parent sees no `n`, GREATEST skips the NULL
      s"SELECT id, i.k AS k ${u("i")} WHERE GREATEST(i.n, 1) = 1",
      // one leaf of several is enough
      s"SELECT id, i.k AS k ${u("i")} WHERE i.n > 0 AND UPPER(i.s) = 'ABC'",
      // an explicit NESTED(...) around it was rendered as match_all: EVERY row (MEASURED)
      s"SELECT id, i.k AS k ${u("i")} WHERE NESTED(UPPER(i.s) = 'ABC')",
      // evaluated per element, but it also reads a PARENT column the element does not hold
      // (`p2|d` is the answer: its `s` equals its parent's name)
      s"SELECT id, i.k AS k ${u("i")} WHERE CAST(i.s AS VARCHAR) = t.name"
    )
  }

  "a WHERE function carried by the UNNEST column" should "still filter each element" in {
    def expect(p: Item => Boolean): Seq[String] =
      parents.flatMap(pa => pa.items.filter(p).map(it => s"${pa.id}|${it.k}")).sorted
    def year(it: Item): Int = _root_.java.time.ZonedDateTime.parse(it.d).getYear
    def month(it: Item): Int = _root_.java.time.ZonedDateTime.parse(it.d).getMonthValue
    Seq(
      (s"SELECT id, i.k AS k ${u("i")} WHERE YEAR(i.d) = 2025", expect(year(_) == 2025)),
      (s"SELECT id, i.k AS k ${u("i")} WHERE MONTH(i.d) = 2", expect(month(_) == 2)),
      (s"SELECT id, i.k AS k ${u("i")} WHERE CAST(i.n AS VARCHAR) = '7'", expect(_.n == 7L)),
      // a literal bound names no column: still evaluated per element
      (
        s"SELECT id, i.k AS k ${u("i")} WHERE YEAR(i.d) BETWEEN 2024 AND 2025",
        expect(it => year(it) >= 2024 && year(it) <= 2025)
      ),
      // nor does a function of no column: the compared value is computed once, per element
      (
        s"SELECT id, i.k AS k ${u("i")} WHERE YEAR(i.d) = YEAR(CURRENT_DATE)",
        expect(year(_) == _root_.java.time.LocalDate.now(_root_.java.time.ZoneOffset.UTC).getYear)
      ),
      (
        s"SELECT id, items.k AS k ${u("items")} WHERE YEAR(items.d) = 2025",
        expect(year(_) == 2025)
      )
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ") {
        expected should not be empty // the oracle selects something
        viaClient(sql).map(keys(_, "id", "k")) shouldBe Right(expected)
        viaGateway(sql).map(keys(_, "id", "k")) shouldBe Right(expected)
      }
    }
  }

  /** Every statement must answer `expected` (its `id|k` rows) at the client API and at the gateway;
    * the wrong ones are COLLECTED and asserted once.
    */
  private def assertAllAnswer(cases: (String, Seq[String])*): Unit = {
    val wrong = cases.flatMap { case (sql, expected) =>
      expected should not be empty // the oracle selects something
      Seq("client" -> viaClient(sql), "gateway" -> viaGateway(sql)).collect {
        case (venue, answer) if answer.map(keys(_, "id", "k")) != Right(expected) =>
          s"[$sql] $venue answered ${answer.map(keys(_, "id", "k"))}"
      }
    }
    wrong shouldBe empty
  }

  "a WHERE COALESCE whose UNNEST column follows a non-null literal" should "answer the rows of the same statement without it" in {
    // COALESCE answers the literal, never the column: the condition does not depend on the element
    // and is answered per parent -- correctly, for every column type under both aliases. With a
    // LIMIT, as measured.
    val every = parents.flatMap(p => p.items.map(it => s"${p.id}|${it.k}")).sorted
    val statements = for {
      alias <- Seq("i", "items")
      col   <- Seq("d", "s", "n", "x", "b")
    } yield {
      val without = s"SELECT id, $alias.k AS k ${u(alias)} LIMIT 100"
      (
        without,
        s"SELECT id, $alias.k AS k ${u(alias)} WHERE COALESCE('x', $alias.$col) = 'x' LIMIT 100"
      )
    }
    statements should have size 10
    // the same statement without the condition answers every element, under both aliases
    assertAllAnswer(statements.map(_._1).distinct.map(_ -> every): _*)
    assertAllAnswer(statements.map(_._2 -> every): _*)
  }

  it should "answer per parent wherever it stands at the top of the WHERE" in {
    def expect(p: (Parent, Item) => Boolean): Seq[String] =
      parents.flatMap(pa => pa.items.filter(p(pa, _)).map(it => s"${pa.id}|${it.k}")).sorted
    assertAllAnswer(
      // after a PARENT column and the literal: the parent's name when it has one
      s"SELECT id, i.k AS k ${u("i")} WHERE COALESCE(t.name, 'x', i.s) = 'Alpha' LIMIT 100" ->
      expect((pa, _) => pa.name == "Alpha"),
      // beside a condition evaluated per element
      s"SELECT id, i.k AS k ${u("i")} WHERE i.n > 5 AND COALESCE('x', i.s) = 'x' LIMIT 100" ->
      expect((_, it) => it.n > 5L),
      // against a subquery: `Alpha` is one of the names
      s"SELECT id, i.k AS k ${u("i")} WHERE COALESCE('Alpha', i.s) IN (SELECT name FROM $index) LIMIT 100" ->
      expect((_, _) => true)
    )
  }

  it should "stay refused where the element is lost, the column can be the answer, or the leaf is not answered per parent" in {
    assertAllRefused("SELECT")(
      // one row per parent: the element is lost
      s"SELECT id, COALESCE('x', i.s) AS c ${u("i")} LIMIT 100",
      // the window counts parents
      s"SELECT id, i.k AS k, SUM(COALESCE(1, i.n)) OVER (PARTITION BY id) AS w ${u("i")}"
    )
    assertAllRefused("WHERE")(
      s"SELECT id, i.k AS k ${u("i")} WHERE COALESCE(i.s, 'x') = 'x'",
      s"SELECT id, i.k AS k ${u("i")} WHERE COALESCE(NULL, i.s) = 'x'",
      // `NESTED(..)` scopes no nested element here: the bridge renders it `match_all`, every row
      s"SELECT id, i.k AS k ${u("i")} WHERE NESTED(COALESCE('x', i.s) = 'y')"
    )
  }

  "WEEKDAY in WHERE over an UNNEST alias" should "never be refused by this rule" in {
    // The column carries the function, so it is evaluated per element. Whether it ANSWERS depends
    // on whether WEEKDAY resolves its receiver (story IDENT-1): the refusal must not be this one.
    val sql = s"SELECT id, i.k AS k ${u("i")} WHERE WEEKDAY(i.d) = 0"
    viaClient(sql).left.toOption.foreach(e =>
      e.message should not startWith UnnestScope.StablePhrase
    )
  }

  "a plain UNNEST projection and a parent function beside a nested column" should "keep their rows" in {
    val plain = parents.flatMap(p => p.items.map(it => s"${p.id}|${it.k}|${it.s}")).sorted
    val sql1 = s"SELECT id, i.k AS k, i.s AS s ${u("i")} LIMIT 100"
    viaClient(sql1).map(keys(_, "id", "k", "s")) shouldBe Right(plain)
    viaGateway(sql1).map(keys(_, "id", "k", "s")) shouldBe Right(plain)
    val upper = parents
      .flatMap(p =>
        p.items.map(it => s"${p.id}|${it.k}|${p.name.toUpperCase(_root_.java.util.Locale.ROOT)}")
      )
      .sorted
    val sql2 = s"SELECT id, i.k AS k, UPPER(t.name) AS c ${u("i")} LIMIT 100"
    viaClient(sql2).map(keys(_, "id", "k", "c")) shouldBe Right(upper)
    viaGateway(sql2).map(keys(_, "id", "k", "c")) shouldBe Right(upper)
  }

  "a window function over a function of an UNNEST column with no nested carrier" should "be refused by name" in {
    // Its aggregation is not wrapped in a `nested` one, so it reads the PARENT, where the element's
    // columns are not visible (MEASURED on ES 8.18.3 before the rule: 0.0 on every row, HTTP 200).
    assertAllRefused("SELECT")(
      s"SELECT id, i.k AS k, SUM(ABS(i.n)) OVER (PARTITION BY id) AS tot ${u("i")}"
    )
  }

  "a window over UNNEST columns beside a parent function" should "keep its rows" in {
    // The window's argument is evaluated per element of its nested aggregation, and the parent
    // function reads the parent, where it is visible: the rule accepts both. Over ARITHMETIC of the
    // UNNEST columns -- a window over a BARE one answers NULL (recorded, not this rule's).
    val expected = parents
      .flatMap(p =>
        p.items.map { it =>
          val name = p.name.toUpperCase(_root_.java.util.Locale.ROOT)
          s"${p.id}|${it.k}|$name|${p.items.map(_.n).sum.toDouble}"
        }
      )
      .sorted
    val sql =
      s"SELECT id, i.k AS k, UPPER(t.name) AS c, SUM(i.n * 1) OVER (PARTITION BY t.id) AS tot ${u("i")} LIMIT 100"
    viaClient(sql).map(keys(_, "id", "k", "c", "tot")) shouldBe Right(expected)
    viaGateway(sql).map(keys(_, "id", "k", "c", "tot")) shouldBe Right(expected)
  }
}
