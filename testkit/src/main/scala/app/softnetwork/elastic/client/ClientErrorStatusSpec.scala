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

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.language.implicitConversions

/** A minimal document shape for the `getAsyncAs` probe — mirrors the call
  * `MaterializedViewExtension.loadMetadata` makes (extensions#40 / SoftClient4ES#184).
  */
case class StatusProbeDocument(id: String)

/** SoftClient4ES#184 — `ElasticError.statusCode` must reflect what Elasticsearch actually returned,
  * so that `IF EXISTS` and other 404-keyed branches work.
  *
  * Asserted on all four ES lines. Before the fix this trait is RED on es8/es9 (their
  * `executeGetAsync` returns a failed `Future`, which `GetApi.getAsync` flattened to a hardcoded
  * 500) and GREEN on es6/es7 (their `executeGetAsync` always returns a successful `Future` holding
  * an `ElasticFailure` that already carries the status). Both cases are wanted: the fix for one,
  * regression protection for the other.
  */
trait ClientErrorStatusSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val ec: ExecutionContextExecutor = system.dispatcher

  // `implicit def formats: Formats` comes from ElasticRestClientTestKit — do NOT redeclare it
  // (it is a concrete member; redeclaring fails with "override modifier required").

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def afterAll(): Unit = {
    if (shardFixtureCreated) client.deleteIndex(shardIndex)
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    super.afterAll()
  }

  /** Never created by this or any other spec — the whole point is that the index is absent. */
  private val absentIndex = "r1fix3_absent_index"

  private val timeout = Duration(30, TimeUnit.SECONDS)

  "get on a non-existent index" should "report HTTP 404, not 500" in {
    val result = client.get("any-id", absentIndex)

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "getAsync on a non-existent index" should "report HTTP 404, not 500" in {
    val result = Await.result(client.getAsync("any-id", absentIndex), timeout)

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "getAsyncAs on a non-existent index" should "report HTTP 404, not 500" in {
    // The exact call shape used by MaterializedViewExtension.loadMetadata (extensions#40).
    val result = Await.result(
      client.getAsyncAs[StatusProbeDocument](id = "any-id", index = Some(absentIndex)),
      timeout
    )

    result.isFailure shouldBe true
    result.error.get.statusCode shouldBe Some(404)
  }

  "a failure on a non-existent index" should "be recognised by ElasticFailure.isNotFound" in {
    client.get("any-id", absentIndex) match {
      case f: ElasticFailure => f.isNotFound shouldBe true
      case other             => fail(s"expected an ElasticFailure, got $other")
    }
  }
  // -- a search whose shards fail -----------------------------------------------------------------

  /** A search on which SOME shards fail answers an error, never the rows of the shards that
    * succeeded.
    *
    * Elasticsearch answers such a search with HTTP 200, `_shards.failed > 0` and the hits / buckets
    * of the surviving shards only. The fixture makes that deterministic: a 3-shard index where ONE
    * document carries a `num` that `CAST(num AS INT)` cannot read (`Integer.parseInt("x")` throws
    * at run time), so exactly the shard holding it fails and the two others succeed — the script
    * error is a property of a VALUE present on one shard, not of a type. The same statements
    * without that document (`WHERE id <> 'bad'`) are the control: they must answer every row.
    *
    * MEASURED before the fix, every row below on 8.18.3 and 6.8.23: HTTP 200 and no error, with the
    * rows of the two surviving shards only (6 of 13 rows on 8.18.3, 10 of 13 on 6.8.23); the
    * aggregation lost the buckets of the failed shard, the UNION ALL the failed leg's rows.
    */
  private val shardIndex = "shard_failure_probe"

  @volatile private var shardFixtureCreated = false

  implicit val conversionContext: ConversionContext = NativeContext

  private val goodDocs: Seq[(String, String)] = (1 to 12).map(i => (f"g$i%02d", i.toString))

  private lazy val shardFixture: Unit = {
    client
      .createIndex(
        shardIndex,
        settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
      )
      .get shouldBe true
    shardFixtureCreated = true
    client
      .setMapping(
        shardIndex,
        """{"properties": {"id": {"type": "keyword"}, "num": {"type": "keyword"}}}"""
      )
      .get shouldBe true

    val docs = (goodDocs :+ ("bad" -> "x")).map { case (id, num) =>
      s"""{"id":"$id","num":"$num"}"""
    }.toList

    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = shardIndex, logEvery = 10)

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing into $shardIndex failed: ${error.message}")
    }
    client.refresh(shardIndex)
    ()
  }

  /** The statement over every document, and its control without the unreadable one. */
  private val rowSql =
    s"SELECT id, CAST(num AS INT) AS n FROM $shardIndex ORDER BY id LIMIT 100"
  private val rowControlSql =
    s"SELECT id, CAST(num AS INT) AS n FROM $shardIndex WHERE id <> 'bad' ORDER BY id LIMIT 100"
  private val groupSql =
    s"SELECT CAST(num AS INT) AS n, COUNT(*) AS c FROM $shardIndex GROUP BY CAST(num AS INT)"
  private val unionSql =
    s"SELECT id, CAST(num AS INT) AS n FROM $shardIndex WHERE id <> 'bad' LIMIT 50 " +
    s"UNION ALL SELECT id, CAST(num AS INT) AS n FROM $shardIndex LIMIT 50"

  /** `1 of 3 shards failed` or `2 of 3`: the failure must be PARTIAL, else Elasticsearch itself
    * answers an error (all shards failed) and the row proves nothing about the HTTP-200 answer.
    */
  private val partialShards = "[12] of 3 shards failed".r

  private def assertShardFailure(sql: String, result: ElasticResult[_], rows: => Seq[_]): Unit =
    result match {
      case ElasticFailure(error) =>
        withClue(s"$sql -> ${error.message}") {
          partialShards.findFirstIn(error.message) should not be empty
          error.message should include("script_exception")
          error.statusCode shouldBe Some(400)
        }
      case ElasticSuccess(_) =>
        fail(
          s"a search on which a shard failed answered ${rows.size} row(s) with no error " +
          s"(a partial answer): $sql -> $rows"
        )
    }

  /** The client venue answers a script field as its one-element list (as
    * `DateFunctionExecutionSpec` reads it).
    */
  private def scalar(v: Any): String = v match {
    case s: Seq[_]                  => s.headOption.map(scalar).getOrElse("null")
    case a: java.util.Collection[_] => scalar(a.toArray.toSeq)
    case null                       => "null"
    case other                      => other.toString
  }

  private def resultsOf(result: ElasticResult[ElasticResponse]): Seq[ListMap[String, Any]] =
    result.toOption.map(_.results).getOrElse(Seq.empty)

  "the control of a search whose shards fail" should "answer every row" in {
    shardFixture
    client.search(SelectStatement(rowControlSql)) match {
      case ElasticSuccess(response) =>
        response.results.map(_("id").toString) shouldBe goodDocs.map(_._1)
        response.results.map(r => scalar(r("n"))) shouldBe goodDocs.map(_._2)
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}\n$rowControlSql")
    }
  }

  "a row search whose shards fail" should "answer an error, not the surviving shards' rows" in {
    shardFixture
    val result = client.search(SelectStatement(rowSql))
    assertShardFailure(rowSql, result, resultsOf(result))
  }

  it should "answer the same error through searchAsync" in {
    shardFixture
    val result = Await.result(client.searchAsync(SelectStatement(rowSql)), timeout)
    assertShardFailure(rowSql, result, resultsOf(result))
  }

  it should "answer the same error through GatewayApi.run" in {
    shardFixture
    val result = Await.result(client.run(rowSql), timeout)
    def rows: Seq[Any] = result match {
      case ElasticSuccess(QueryRows(rows, _))           => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _)) =>
        Await.result(stream.map(_._1).runWith(Sink.seq), timeout)
      case ElasticSuccess(other) => Seq(other)
      case ElasticFailure(_)     => Seq.empty
    }
    assertShardFailure(rowSql, result, rows)
  }

  "an aggregation whose shards fail" should "answer an error, not partial buckets" in {
    shardFixture
    val result = client.search(SelectStatement(groupSql))
    assertShardFailure(groupSql, result, resultsOf(result))
  }

  "a UNION ALL leg whose shards fail" should "answer an error, not the other leg alone" in {
    shardFixture
    val result = client.search(SelectStatement(unionSql))
    assertShardFailure(unionSql, result, resultsOf(result))
  }
}
