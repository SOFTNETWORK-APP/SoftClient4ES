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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

case class ShardProbeDoc(id: String)
case class ShardProbeInner(k: String)

/** A search on which SOME shards fail answers an error at every one-shot door.
  *
  * Elasticsearch answers such a search with HTTP 200, `_shards.failed > 0` and the rows of the
  * shards that succeeded. Docker-free: the response trees below are the shapes Elasticsearch
  * returns (the first is the one measured on 6.8.23 for a `CAST` over an unreadable value, its
  * cause reworded), handed to the six one-shot doors through the client seams.
  */
class ShardFailuresSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)
  private implicit val system: ActorSystem = ActorSystem("shard-failures-spec")
  private implicit val ec: ExecutionContext = system.dispatcher
  private implicit val context: ConversionContext = NativeContext
  private implicit val formats: Formats = DefaultFormats

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 20.seconds)
    ()
  }

  private val mapper = new ObjectMapper()

  private def tree(json: String): JsonNode = mapper.readTree(json)

  private val scriptFailure =
    """{"shard":0,"index":"idx","node":"n1","reason":{"type":"script_exception",""" +
    """"reason":"runtime error","lang":"painless","caused_by":{"type":"number_format_exception",""" +
    """"reason":"For input string: \"x\""}}}"""

  private val hits = """"hits":{"total":{"value":1},"hits":[{"_id":"1","_source":{"id":"g01"}}]}"""

  private def response(shards: String): String =
    s"""{"took":1,"timed_out":false,"_shards":$shards,$hits}"""

  private val partial =
    response(s"""{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[$scriptFailure]}""")

  private val healthy = response("""{"total":3,"successful":3,"skipped":1,"failed":0}""")

  private val expected =
    "1 of 3 shards failed: script_exception: runtime error " +
    "(caused by number_format_exception: For input string: \"x\")"

  // -- the helper ---------------------------------------------------------------------------------

  "ShardFailures.of" should "read nothing when no shard failed, skipped shards included" in {
    ShardFailures.of(tree(healthy)) shouldBe None
    ShardFailures.error(tree(healthy)) shouldBe None
    ShardFailures.of(tree(s"{$hits}")) shouldBe None
  }

  it should "answer n of m shards failed, the first failure's type, reason and cause, and 400" in {
    val error = ShardFailures.error(tree(partial)).getOrElse(fail("no error read"))
    error.message shouldBe expected
    error.statusCode shouldBe Some(400)
  }

  it should "read the response measured on Elasticsearch 6.8.23 for DATE_FORMAT over a date" in {
    // HTTP 200, 3 shards, 2 failed, ONE grouped failure, no bucket: answered as 0 rows before
    val measured =
      """{"took":265,"timed_out":false,"_shards":{"total":3,"successful":1,"skipped":0,""" +
      """"failed":2,"failures":[{"shard":0,"index":"fdate_probe","node":"YLLWvnvYQHyfijfzn_Dr3A",""" +
      """"reason":{"type":"script_exception","reason":"runtime error","script_stack":[],""" +
      """"lang":"painless","caused_by":{"type":"class_cast_exception","reason":"Cannot cast """ +
      """org.elasticsearch.script.JodaCompatibleZonedDateTime to """ +
      """java.time.temporal.TemporalAccessor"}}}]},"hits":{"total":0,"max_score":0.0,"hits":[]},""" +
      """"aggregations":{"k":{"doc_count_error_upper_bound":0,"sum_other_doc_count":0,""" +
      """"buckets":[]}}}"""
    val error = ShardFailures.error(tree(measured)).getOrElse(fail("no error read"))
    error.message shouldBe "2 of 3 shards failed: script_exception: runtime error (caused by " +
    "class_cast_exception: Cannot cast org.elasticsearch.script.JodaCompatibleZonedDateTime to " +
    "java.time.temporal.TemporalAccessor)"
    error.statusCode shouldBe Some(400)
  }

  it should "keep the scroll doors' reasons unchanged" in {
    ShardFailures.of(tree(partial)).map(_.reasons) shouldBe Some("runtime error")
    ShardFailures
      .of(tree(response("""{"total":3,"successful":1,"failed":2}""")))
      .map(_.reasons) shouldBe Some("Unknown shard failure")
  }

  it should "count the failed shards without detail when Elasticsearch lists none" in {
    val error = ShardFailures
      .error(tree(response("""{"total":5,"successful":3,"failed":2}""")))
      .getOrElse(fail("no error read"))
    error.message shouldBe "2 of 5 shards failed"
    error.statusCode shouldBe None
  }

  it should "leave the status undetermined unless every failure is the caller's" in {
    val unavailable =
      """{"shard":1,"index":"idx","reason":{"type":"no_shard_available_action_exception",""" +
      """"reason":"No shard available"}}"""
    ShardFailures
      .error(
        tree(response(s"""{"total":3,"successful":2,"failed":1,"failures":[$unavailable]}"""))
      )
      .map(_.statusCode) shouldBe Some(None)
    ShardFailures
      .error(
        tree(
          response(
            s"""{"total":3,"successful":1,"failed":2,"failures":[$scriptFailure,$unavailable]}"""
          )
        )
      )
      .map(_.statusCode) shouldBe Some(None)
  }

  "ShardFailures.shardReason" should "answer the deepest reason of the first failure that says something new" in {
    // as Elasticsearch 7.17.29 answers ASIN outside its domain: the reason is under `caused_by` only
    val failedShards = tree(
      """[{"shard":0,"reason":{"type":"script_exception","reason":"runtime error","caused_by":{"type":"illegal_argument_exception","reason":"ASIN is undefined outside [-1,1]"}}},{"shard":1,"reason":{"type":"script_exception","reason":"runtime error","caused_by":{"type":"illegal_argument_exception","reason":"another"}}}]"""
    )
    ShardFailures.shardReason(failedShards, Seq("runtime error")) shouldBe
    Some("ASIN is undefined outside [-1,1]")
    // a reason already said is not said twice: the next failure's is
    ShardFailures.shardReason(failedShards, Seq("ASIN is undefined outside [-1,1]")) shouldBe
    Some("another")
    // no cause: the failure's own reason, unless it is said already
    val noCause = tree("""[{"shard":0,"reason":{"type":"x_exception","reason":"only"}}]""")
    ShardFailures.shardReason(noCause, Nil) shouldBe Some("only")
    ShardFailures.shardReason(noCause, Seq("only")) shouldBe None
    // not a `failed_shards` array: nothing
    ShardFailures.shardReason(tree("""{"shard":0}"""), Nil) shouldBe None
    ShardFailures.shardReason(tree("""{}""").path("failed_shards"), Nil) shouldBe None
  }

  it should "read a multi search per response, naming the failing one" in {
    val msearch = s"""{"responses":[$healthy,$partial]}"""
    ShardFailures.error(tree(msearch)).map(_.message) shouldBe Some(s"Query 2: $expected")
    ShardFailures.error(tree(s"""{"responses":[$healthy,$healthy]}""")) shouldBe None
  }

  // -- the six one-shot doors ---------------------------------------------------------------------

  private class FixedResponseClient(answer: String) extends NopeClientApi {
    override protected def logger: Logger = testLogger

    private def single: ElasticResult[Option[JsonNode]] = ElasticResult.success(Some(tree(answer)))

    private def multi(queries: ElasticQueries): ElasticResult[Option[JsonNode]] =
      ElasticResult.success(
        Some(
          tree(
            queries.queries.indices
              .map(i => if (i == queries.queries.size - 1) answer else healthy)
              .mkString("""{"responses":[""", ",", "]}")
          )
        )
      )

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = single

    override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] = Future.successful(single)

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] = multi(elasticQueries)

    override private[client] def executeMultiSearchAsync(elasticQueries: ElasticQueries)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] = Future.successful(multi(elasticQueries))
  }

  private val query = ElasticQuery("""{"query":{"match_all":{}}}""", Seq("idx"))
  private val queries = ElasticQueries(List(query, query))

  private def doors(client: SearchApi): Seq[(String, ElasticResult[_])] = Seq(
    "search"      -> client.singleSearch(query, ListMap.empty, ListMap.empty),
    "multiSearch" -> client.multiSearch(queries, ListMap.empty, ListMap.empty),
    "searchAsync" -> Await.result(
      client.singleSearchAsync(query, ListMap.empty, ListMap.empty),
      10.seconds
    ),
    "multiSearchAsync" -> Await.result(
      client.multiSearchAsync(queries, ListMap.empty, ListMap.empty),
      10.seconds
    ),
    "singleSearchWithInnerHits" ->
    client.singleSearchWithInnerHits[ShardProbeDoc, ShardProbeInner](query, "inner"),
    "multisearchWithInnerHits" ->
    client.multisearchWithInnerHits[ShardProbeDoc, ShardProbeInner](queries, "inner")
  )

  "every one-shot search door" should "answer the shard failure as an error, not partial rows" in {
    doors(new FixedResponseClient(partial)).foreach { case (door, result) =>
      withClue(s"$door: ") {
        result match {
          case ElasticFailure(error) =>
            val multi = door.toLowerCase.contains("multi")
            error.message shouldBe (if (multi) s"Query 2: $expected" else expected)
            error.statusCode shouldBe Some(400)
            error.operation should not be empty
          case ElasticSuccess(value) => fail(s"a partial answer was returned: $value")
        }
      }
    }
  }

  "the search doors" should "answer the rows when no shard failed" in {
    val client = new FixedResponseClient(healthy)
    client.singleSearch(query, ListMap.empty, ListMap.empty) match {
      case ElasticSuccess(response) => response.results.map(_("id")) shouldBe Seq("g01")
      case ElasticFailure(error)    => fail(error.message)
    }
    client.multiSearch(queries, ListMap.empty, ListMap.empty) match {
      case ElasticSuccess(response) => response.results.map(_("id")) shouldBe Seq("g01", "g01")
      case ElasticFailure(error)    => fail(error.message)
    }
  }
}
