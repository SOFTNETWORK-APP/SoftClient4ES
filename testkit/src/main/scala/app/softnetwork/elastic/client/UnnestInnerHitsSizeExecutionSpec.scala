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
import scala.util.{Failure, Success, Try}

/** An UNNEST projection without a `LIMIT` answers EVERY element of a parent, up to 100 (or up to
  * the index's own lower `index.max_inner_result_window`) — at the client API and at the gateway
  * (the path the JDBC driver, the REPL and the Flight SQL sidecar take).
  *
  * Before, MEASURED at RUN level on ES 8.18.3 and 6.8.23: each parent kept at most 3 elements
  * (`inner_hits`' default size), so the five-element parent answered three rows with HTTP 200. The
  * statement now asks for 100 (Elasticsearch's default `index.max_inner_result_window`), or for the
  * index's own setting when it is lower, and there is deliberately no truncation check (lead
  * ruling): a parent holding more elements than that answers that many of them, which this spec
  * pins so the documentation can state it.
  *
  * Oracles are computed from the fixture in Scala, never from the engine. The index has several
  * shards: a completeness assertion on one shard proves less.
  */
trait UnnestInnerHitsSizeExecutionSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "unnest_inner_hits_size"

  /** The same documents in an index whose `index.max_inner_result_window` is 4: asking it for 100
    * elements per parent, Elasticsearch refuses the whole statement.
    */
  private val lowWindowIndex = "unnest_inner_hits_window"

  private val lowWindow = 4

  private case class Parent(id: String, items: Seq[(String, Long)])

  private def items(id: String, n: Int): Seq[(String, Long)] =
    (0 until n).map(i => f"$id-$i%03d" -> i.toLong)

  /** Five elements (beyond the old 3), two, none (an UNNEST is an inner join: no row), and 101 —
    * one more than the elements requested per parent.
    */
  private val parents: Seq[Parent] = Seq(
    Parent("p5", items("p5", 5)),
    Parent("p2", items("p2", 2)),
    Parent("p0", Nil),
    Parent("p101", items("p101", 101))
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mapping =
      """{
        |  "properties": {
        |    "id":    { "type": "keyword" },
        |    "items": { "type": "nested", "properties": {
        |      "k": { "type": "keyword" },
        |      "n": { "type": "long" }
        |    } }
        |  }
        |}""".stripMargin
    val docs = parents.map { p =>
      val json = p.items.map { case (k, n) => s"""{"k":"$k","n":$n}""" }.mkString("[", ",", "]")
      s"""{"id":"${p.id}","items":$json}"""
    }.toList
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    Seq(
      index -> """{"number_of_shards": 3, "number_of_replicas": 0}""",
      lowWindowIndex -> s"""{"number_of_shards": 3, "number_of_replicas": 0, "max_inner_result_window": $lowWindow}"""
    ).foreach { case (name, settings) =>
      client.createIndex(name, settings = settings).get shouldBe true
      client.setMapping(name, mapping).get shouldBe true
      implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = name, logEvery = 10)
      client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
        case ElasticSuccess(_)     => // ok
        case ElasticFailure(error) => fail(s"Bulk indexing into $name failed: ${error.message}")
      }
      client.refresh(name)
    }
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    client.deleteIndex(lowWindowIndex)
    super.afterAll()
  }

  private def thrown(e: Throwable): ElasticError =
    ElasticError(message = s"threw ${e.getClass.getSimpleName}: ${e.getMessage}", cause = Some(e))

  private def viaClient(sql: String): Either[ElasticError, Seq[ListMap[String, Any]]] =
    Try(client.search(SelectStatement(sql))) match {
      case Success(ElasticSuccess(response)) => Right(response.results)
      case Success(ElasticFailure(error))    => Left(error)
      case Failure(e)                        => Left(thrown(e))
    }

  /** Every result shape collected to rows; a `QueryStream` is drained inside the `Try`. */
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

  /** `id|k` per row, sorted — the row keys an unaliased UNNEST column is projected under. */
  private def keys(rows: Seq[ListMap[String, Any]]): Seq[String] =
    rows.map(r => s"${r.getOrElse("id", null)}|${r.getOrElse("i.k", null)}").sorted

  private def of(ids: String*): Seq[String] =
    parents
      .filter(p => ids.contains(p.id))
      .flatMap(p => p.items.map(i => s"${p.id}|${i._1}"))
      .sorted

  private def statementsOver(name: String): Seq[String] = Seq(
    // the projection's own nested query carries the inner hits
    s"SELECT t.id, i.k FROM $name t JOIN UNNEST(t.items) AS i",
    // a nested filter on the same UNNEST carries them instead
    s"SELECT t.id, i.k FROM $name t JOIN UNNEST(t.items) AS i WHERE i.n >= 0"
  )

  private val statements = statementsOver(index)

  "An UNNEST projection without LIMIT" should "answer every element of a five-element parent" in {
    for {
      sql             <- statements
      (venue, answer) <- Seq("client" -> viaClient(sql), "gateway" -> viaGateway(sql))
    } withClue(s"[$sql] $venue ") {
      answer.map(keys(_).filterNot(_.startsWith("p101|"))) shouldBe Right(of("p5", "p2"))
    }
  }

  it should "answer 100 elements of a 101-element parent" in {
    val all = of("p101")
    all should have size 101
    for {
      sql             <- statements
      (venue, answer) <- Seq("client" -> viaClient(sql), "gateway" -> viaGateway(sql))
    } withClue(s"[$sql] $venue ") {
      val big = answer.map(keys(_).filter(_.startsWith("p101|")))
      big.map(_.size) shouldBe Right(100)
      big.map(_.distinct.size) shouldBe Right(100)
      big.map(_.toSet.subsetOf(all.toSet)) shouldBe Right(true)
      log.info(
        s"[$sql] $venue: the 101-element parent answered ${big.map(_.size)} rows; " +
        s"not returned: ${big.map(all.toSet -- _)}"
      )
    }
  }

  /** The size is read from the schema core attaches before rendering (#306). Exactly `min(4,
    * elements)` rows per parent is the proof that 4 was asked for: 100 would have been refused by
    * Elasticsearch, and a size the index ignored would have answered the five-element parent in
    * full.
    */
  it should "answer up to the index's lower max_inner_result_window per parent, without an error" in {
    for {
      sql             <- statementsOver(lowWindowIndex)
      (venue, answer) <- Seq("client" -> viaClient(sql), "gateway" -> viaGateway(sql))
    } withClue(s"[$sql] $venue ") {
      val rows = answer.map(keys)
      rows.left.map(_.message) shouldBe a[Right[_, _]]
      parents.foreach { p =>
        withClue(s"parent ${p.id}: ") {
          val answered = rows.map(_.filter(_.startsWith(s"${p.id}|")))
          val expected = math.min(lowWindow, p.items.size)
          answered.map(_.size) shouldBe Right(expected)
          answered.map(_.distinct.size) shouldBe Right(expected)
          answered.map(_.toSet.subsetOf(of(p.id).toSet)) shouldBe Right(true)
        }
      }
    }
  }
}
