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
import app.softnetwork.elastic.client.scroll.ScrollConfig
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
import scala.util.{Failure, Success, Try}

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
    if (relationFixtureCreated) relationIndices.foreach(client.deleteIndex)
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
      case ElasticSuccess(QueryRows(rows, _, _))        => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _, _)) =>
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

  /** A search on which EVERY shard fails answers Elasticsearch's reason, not only its wrapper.
    *
    * No `id` is a number, so `CAST(id AS INT)` throws on every document, and each of the 3 shards
    * holds some: Elasticsearch answers HTTP 400 "all shards failed", and WHY (`For input string`)
    * is in each failed shard's `caused_by` only. Before the fix the ES 8 / 9 Java client answered
    * no more than `search_phase_execution_exception - all shards failed; runtime error` (MEASURED
    * on 8.18.3 for a Painless script that throws).
    */
  "a search whose every shard fails" should "answer Elasticsearch's reason" in {
    shardFixture
    val sql =
      s"SELECT CAST(id AS INT) AS n, COUNT(*) AS c FROM $shardIndex GROUP BY CAST(id AS INT)"
    client.search(SelectStatement(sql)) match {
      case ElasticFailure(error) =>
        withClue(s"$sql -> ${error.message}") {
          error.message should include("For input string")
        }
      case ElasticSuccess(response) =>
        fail(s"a search on which every shard failed answered ${response.results.size} row(s): $sql")
    }
  }

  // -- the same search read page by page ----------------------------------------------------------

  /** Without a LIMIT a row query is read page by page, through the client's paging door: PIT (ES
    * 7.12+) or `search_after` with an ORDER BY, and the classic scroll when the paging API is opted
    * out of. Every one of them fails on a page whose shards failed; the message is the door's own,
    * so only the failure is asserted.
    */
  "an un-LIMITed search whose shards fail" should "answer an error with an ORDER BY (search_after / PIT)" in {
    shardFixture
    val sql = s"SELECT id, CAST(num AS INT) AS n FROM $shardIndex ORDER BY id"
    client.search(SelectStatement(sql)) match {
      case ElasticFailure(_) => // the door's own message
      case ElasticSuccess(response) =>
        fail(
          s"a paged search on which a shard failed answered ${response.results.size} row(s) " +
          s"with no error: $sql"
        )
    }
  }

  it should "answer an error through the classic scroll" in {
    shardFixture
    val sql = s"SELECT id, CAST(num AS INT) AS n FROM $shardIndex"
    Try(
      Await.result(
        client
          .scroll(SelectStatement(sql), ScrollConfig(preferSearchAfter = false))
          .map(_._1)
          .runWith(Sink.seq),
        timeout
      )
    ) match {
      case Failure(_) => // the door's own message
      case Success(rows) =>
        fail(
          s"a classic scroll on which a shard failed answered ${rows.size} row(s) with no " +
          s"error: $sql"
        )
    }
  }

  /** The window values of an un-LIMITed statement are computed first, by one aggregation search;
    * here its partition key `CAST(num AS INT)` throws on the shard holding `bad`, while the rows
    * themselves read nothing that throws. The statement answers an error, not its rows without
    * their window values.
    */
  "an un-LIMITed window whose computation fails on some shards" should "answer an error, not the rows without their window values" in {
    shardFixture
    val sql =
      s"SELECT id, FIRST_VALUE(id) OVER (PARTITION BY CAST(num AS INT) ORDER BY id) AS fv " +
      s"FROM $shardIndex"
    client.search(SelectStatement(sql)) match {
      case ElasticFailure(error) =>
        withClue(error.message) {
          error.message should include("Window function computation failed")
        }
      case ElasticSuccess(response) =>
        fail(
          s"answered ${response.results.size} row(s) without their window values: $sql -> " +
          response.results.take(3).mkString("; ")
        )
    }
  }

  // -- a nested or CHILD/PARENT condition over indices that lack the mapping -----------------------

  import akka.stream.scaladsl.Sink
  import app.softnetwork.elastic.client.result.{
    ElasticSuccess,
    QueryRows,
    QueryStream,
    QueryStructured
  }
  import scala.collection.immutable.ListMap
  import scala.util.Try

  /** A nested or CHILD/PARENT condition over a pattern whose index lacks the mapping answers the
    * other indices' matches.
    *
    * Elasticsearch fails every shard of an index that does not map a nested query's path (`failed
    * to find nested object under path [items]`), or has no join field for a `has_child` /
    * `has_parent` (`no join field has been configured`), even when that index is empty. Such an
    * index holds no element and no relation, so none of its documents can match: when the schema of
    * the pattern maps the path as nested, or has a join field holding the relation in that role,
    * the query now tolerates such an index (`ignore_unmapped`). A path or a relation no index knows
    * still fails.
    *
    * Fixture, 3 shards each: `unm_rel_n_a` and `unm_rel_np_a` map `items` as nested and hold `x1`
    * (`k1`, `k2`), `x2` (`k1`) and `x3` (`k3`); `unm_rel_j_a` and `unm_rel_jp_a` map a join field
    * (`question` -> `answer`) and hold the questions `q1` and `q2` and their answers `c1` (body
    * `x`) and `c2`. `unm_rel_n_e` and `unm_rel_j_e` have no mapping and no document; `unm_rel_np_p`
    * and `unm_rel_jp_p` map `id` only and hold `p1` and `p2`.
    */
  private val relationIndices: Seq[String] = Seq(
    "unm_rel_n_a",
    "unm_rel_n_e",
    "unm_rel_np_a",
    "unm_rel_np_p",
    "unm_rel_j_a",
    "unm_rel_j_e",
    "unm_rel_jp_a",
    "unm_rel_jp_p"
  )

  @volatile private var relationFixtureCreated = false

  private val itemsMapping =
    """{"properties": {"id": {"type": "keyword"}, "items": {"type": "nested",
      | "properties": {"k": {"type": "keyword"}, "q": {"type": "integer"}}}}}""".stripMargin

  private val joinMapping =
    """{"properties": {"id": {"type": "keyword"},
      | "rel": {"type": "join", "relations": {"question": "answer"}},
      | "question": {"properties": {"title": {"type": "keyword"}}},
      | "answer": {"properties": {"body": {"type": "keyword"}}}}}""".stripMargin

  private val idOnlyMapping = """{"properties": {"id": {"type": "keyword"}}}"""

  /** (id, source, routing): an answer is routed to its question's shard, as a join requires. */
  private val itemsDocs: Seq[(String, String, Option[String])] = Seq(
    ("x1", """{"id":"x1","items":[{"k":"k1","q":1},{"k":"k2","q":2}]}""", None),
    ("x2", """{"id":"x2","items":[{"k":"k1","q":3}]}""", None),
    ("x3", """{"id":"x3","items":[{"k":"k3","q":4}]}""", None)
  )

  private val joinDocs: Seq[(String, String, Option[String])] = Seq(
    ("q1", """{"id":"q1","rel":"question","question":{"title":"t1"}}""", Some("q1")),
    ("q2", """{"id":"q2","rel":"question","question":{"title":"t2"}}""", Some("q2")),
    (
      "c1",
      """{"id":"c1","rel":{"name":"answer","parent":"q1"},"answer":{"body":"x"}}""",
      Some("q1")
    ),
    (
      "c2",
      """{"id":"c2","rel":{"name":"answer","parent":"q2"},"answer":{"body":"y"}}""",
      Some("q2")
    )
  )

  private val idOnlyDocs: Seq[(String, String, Option[String])] =
    Seq(("p1", """{"id":"p1"}""", None), ("p2", """{"id":"p2"}""", None))

  private def relationIndex(
    name: String,
    mapping: Option[String],
    docs: Seq[(String, String, Option[String])]
  ): Unit = {
    client
      .createIndex(name, settings = """{"number_of_shards": 3, "number_of_replicas": 0}""")
      .get shouldBe true
    relationFixtureCreated = true
    mapping.foreach(m => client.setMapping(name, m).get shouldBe true)
    docs.foreach { case (id, source, routing) =>
      val request = new org.elasticsearch.client.Request("PUT", s"/$name/_doc/$id")
      routing.foreach(r => request.addParameter("routing", r))
      request.setEntity(
        new org.apache.http.nio.entity.NStringEntity(
          source,
          org.apache.http.entity.ContentType.APPLICATION_JSON
        )
      )
      restClient.performRequest(request)
    }
    client.refresh(name)
    ()
  }

  private lazy val relationFixture: Unit = {
    relationIndex("unm_rel_n_a", Some(itemsMapping), itemsDocs)
    relationIndex("unm_rel_n_e", None, Nil)
    relationIndex("unm_rel_np_a", Some(itemsMapping), itemsDocs)
    relationIndex("unm_rel_np_p", Some(idOnlyMapping), idOnlyDocs)
    relationIndex("unm_rel_j_a", Some(joinMapping), joinDocs)
    relationIndex("unm_rel_j_e", None, Nil)
    relationIndex("unm_rel_jp_a", Some(joinMapping), joinDocs)
    relationIndex("unm_rel_jp_p", Some(idOnlyMapping), idOnlyDocs)
  }

  /** The rows a statement answers through the gateway (the JDBC / REPL / Flight SQL path), whatever
    * its shape: one search, or a read page by page.
    */
  private def answeredRows(sql: String): Seq[ListMap[String, Any]] =
    Await.result(client.run(sql), timeout) match {
      case ElasticSuccess(QueryRows(rows, _, _)) => rows
      case ElasticSuccess(QueryStructured(r, _)) => r.results
      case ElasticSuccess(QueryStream(stream, _, _)) =>
        Try(Await.result(stream.map(_._1).runWith(Sink.seq), timeout)).fold(
          e => fail(s"$sql -> the stream failed: ${e.getMessage}"),
          rows => rows
        )
      case ElasticSuccess(other) => fail(s"$sql -> unexpected result $other")
      case ElasticFailure(error) => fail(s"$sql -> ${error.message}")
    }

  private def idsOf(sql: String): Seq[String] = answeredRows(sql).map(_("id").toString).sorted

  /** Each element of `items` as `id:k`, the four the nested index holds. */
  private def elementsOf(sql: String): Seq[String] =
    answeredRows(sql).map(row => s"${row("id")}:${row("i.k")}").sorted

  private val allElements = Seq("x1:k1", "x1:k2", "x2:k1", "x3:k3")

  "a nested condition over a pattern whose index does not map the path" should
  "answer the other indices' matches" in {
    relationFixture
    idsOf(
      "SELECT t.id FROM unm_rel_np_* t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1' LIMIT 100"
    ) shouldBe Seq("x1", "x2")
  }

  it should "answer every element of an UNNEST, over an empty such index" in {
    relationFixture
    elementsOf(
      "SELECT t.id, i.k FROM unm_rel_n_* t JOIN UNNEST(t.items) AS i LIMIT 100"
    ) shouldBe allElements
  }

  it should "answer every element of an UNNEST without a LIMIT (read page by page)" in {
    relationFixture
    elementsOf("SELECT t.id, i.k FROM unm_rel_np_* t JOIN UNNEST(t.items) AS i") shouldBe
    allElements
  }

  it should "answer as before over the index that maps the path" in {
    relationFixture
    idsOf(
      "SELECT t.id FROM unm_rel_n_a t JOIN UNNEST(t.items) AS i WHERE i.k = 'k1' LIMIT 100"
    ) shouldBe Seq("x1", "x2")
  }

  it should "still fail over a path no index maps" in {
    relationFixture
    Await
      .result(
        client.run(
          "SELECT t.id FROM unm_rel_np_* t JOIN UNNEST(t.itemz) AS i WHERE i.k = 'k1' LIMIT 100"
        ),
        timeout
      )
      .isFailure shouldBe true
  }

  "CHILD(…) and PARENT(…) over a pattern whose index has no join field" should
  "answer the other indices' matches" in {
    relationFixture
    idsOf("SELECT id FROM unm_rel_jp_* WHERE CHILD(answer.body = 'x') LIMIT 100") shouldBe
    Seq("q1")
    idsOf("SELECT id FROM unm_rel_j_* WHERE PARENT(question.title = 't1') LIMIT 100") shouldBe
    Seq("c1")
  }

  it should "still fail over a relation no index holds" in {
    relationFixture
    Await
      .result(
        client.run("SELECT id FROM unm_rel_jp_* WHERE CHILD(answr.body = 'x') LIMIT 100"),
        timeout
      )
      .isFailure shouldBe true
  }

  "an un-LIMITed search over a missing index" should "fail with 404, naming the index" in {
    // Elasticsearch 6.8 names the index beside its reason (`no such index`), later majors in it
    val failure = Try(
      Await.result(
        client.scroll(SelectStatement(s"SELECT id FROM $absentIndex")).runWith(Sink.seq),
        timeout
      )
    ).failed.getOrElse(fail(s"a scroll over $absentIndex answered rows"))
    withClue(s"${failure.getClass.getName} ${failure.getMessage}: ") {
      client.statusOf(failure) shouldBe Some(404)
      failure.getMessage should include(absentIndex)
    }
  }
}
