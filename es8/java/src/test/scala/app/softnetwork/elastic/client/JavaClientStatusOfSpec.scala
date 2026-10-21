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

import app.softnetwork.elastic.client.java.{JavaClientCompanion, JavaClientHelpers}
import co.elastic.clients.elasticsearch._types.{
  ElasticsearchException,
  ErrorCause,
  ErrorResponse,
  ShardStatistics
}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import com.typesafe.config.ConfigFactory
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

// `java` is shadowed by the sibling `app.softnetwork.elastic.client.java` sub-package — the
// `_root_.` prefix is mandatory here (SoftClient4ES#184, Task 10.4).
import _root_.java.util.concurrent.CompletionException

/** SoftClient4ES#184 — proves the ES 8/9 `statusOf` extractor against hand-built real exception
  * instances, without Docker: `ElasticClientCompanion` creates the underlying client lazily, and
  * `apply()` is never called here.
  */
class JavaClientStatusOfSpec extends AnyWordSpec with Matchers {

  private case class Probe(config: ElasticConfig)
      extends JavaClientCompanion
      with JavaClientHelpers {
    override def elasticConfig: ElasticConfig = config
  }

  private val probe = Probe(ElasticConfig(ConfigFactory.load()))

  private def esException(status: Int, errType: String): ElasticsearchException =
    new ElasticsearchException(
      "es/get",
      ErrorResponse.of((r: ErrorResponse.Builder) =>
        r.status(status)
          .error((e: ErrorCause.Builder) => e.`type`(errType).reason("no such index [absent]"))
      )
    )

  "statusOf" should {

    "map an ElasticsearchException to its status" in {
      probe.statusOf(esException(404, "index_not_found_exception")) shouldBe Some(404)
    }

    "unwrap a CompletionException first" in {
      probe.statusOf(
        new CompletionException(esException(409, "version_conflict_engine_exception"))
      ) shouldBe Some(409)
    }

    "return None for a plain transport failure" in {
      probe.statusOf(new _root_.java.net.ConnectException("Connection refused")) shouldBe None
    }

    "treat the primitive 0 sentinel as 'unknown', never as an HTTP status" in {
      // `ElasticsearchException.status()` returns a primitive int, so an absent status is 0.
      // Some(0) would defeat isNotFound and every IF EXISTS check (SoftClient4ES#184).
      probe.statusOf(esException(0, "unknown")) shouldBe None
      probe.statusOrServerError(esException(0, "unknown")) shouldBe Some(500)
    }

    "fall through to the core default for a framework throwable" in {
      probe.statusOf(
        app.softnetwork.elastic.client.result.ElasticError("boom", statusCode = Some(403))
      ) shouldBe Some(403)
    }
  }

  /** An error read by the client's own deserializer, as the transport reads it: `failed_shards` is
    * not a typed field of `ErrorCause`, so it lands in its raw metadata.
    */
  private def readException(json: String): ElasticsearchException = {
    val mapper = new JacksonJsonpMapper()
    val parser = mapper.jsonProvider().createParser(new _root_.java.io.StringReader(json))
    new ElasticsearchException("es/search", ErrorResponse._DESERIALIZER.deserialize(parser, mapper))
  }

  /** The shape Elasticsearch answers when a Painless script throws on every shard: the reason of
    * the throw is in the failed shard's `caused_by` only.
    */
  private def allShardsFailed(shardCause: String, rootCause: String): String =
    s"""{"error":{"root_cause":[{"type":"script_exception","reason":"$rootCause",""" +
    """"lang":"painless"}],"type":"search_phase_execution_exception",""" +
    """"reason":"all shards failed","phase":"query","grouped":true,"failed_shards":[{"shard":0,""" +
    """"index":"idx","node":"n1","reason":{"type":"script_exception","reason":"runtime error",""" +
    """"lang":"painless","caused_by":{"type":"number_format_exception",""" +
    s""""reason":"$shardCause"}}}]},"status":400}"""

  "elasticsearchErrorMessage" should {

    "carry the failed shard's reason, once" in {
      val ex = readException(allShardsFailed("For input string: \\\"x\\\"", "runtime error"))
      probe.elasticsearchErrorMessage("search", ex) shouldBe
      "Elasticsearch error during search: search_phase_execution_exception - all shards failed; " +
      "runtime error: For input string: \"x\""
      probe.statusOf(ex) shouldBe Some(400)
      // a reason the message already says is not said twice
      probe.elasticsearchErrorMessage(
        "search",
        readException(allShardsFailed("boom", "boom"))
      ) shouldBe
      "Elasticsearch error during search: search_phase_execution_exception - all shards failed; boom"
    }
  }

  "searchPageFailure" should {

    "carry Elasticsearch's status and reason for a page it rejects with a 4xx" in {
      val rejected = readException(allShardsFailed("Text 'x' could not be parsed", "runtime error"))
      val failure = probe.searchPageFailure(new CompletionException(rejected))
      failure shouldBe a[JavaClientHelpers.SearchPageFailure]
      failure.getMessage shouldBe
      "Elasticsearch error during search: search_phase_execution_exception - all shards failed; " +
      "runtime error: Text 'x' could not be parsed"
      probe.statusOf(failure) shouldBe Some(400)
      isRetriableError(failure) shouldBe false
    }

    "return any other failure as it is" in {
      val tooMany = esException(429, "es_rejected_execution_exception")
      probe.searchPageFailure(tooMany) should be theSameInstanceAs tooMany
      val unavailable = esException(503, "unavailable_shards_exception")
      probe.searchPageFailure(unavailable) should be theSameInstanceAs unavailable
      val reset = new _root_.java.io.IOException("Connection reset")
      probe.searchPageFailure(reset) should be theSameInstanceAs reset
    }
  }

  /** A page's `_shards`, read by the client's own deserializer as the transport reads it. */
  private def readShards(json: String): ShardStatistics = {
    val mapper = new JacksonJsonpMapper()
    val parser = mapper.jsonProvider().createParser(new _root_.java.io.StringReader(json))
    ShardStatistics._DESERIALIZER.deserialize(parser, mapper)
  }

  /** A page on which ONE of 3 shards failed, as Elasticsearch answers it with HTTP 200. */
  private def oneOfThreeShardsFailed(errType: String): ShardStatistics =
    readShards(
      """{"total":3,"successful":2,"skipped":0,"failed":1,"failures":[{"shard":0,"index":"idx",""" +
      s""""node":"n1","reason":{"type":"$errType","reason":"runtime error","lang":"painless",""" +
      """"caused_by":{"type":"date_time_parse_exception",""" +
      """"reason":"Text 'x' could not be parsed at index 0"}}}]}"""
    )

  private def pitPage(shards: ShardStatistics): Option[Exception] =
    probe.failedShardsPage("PIT search_after failed", shards)(new _root_.java.io.IOException(_))

  "failedShardsPage" should {

    "answer core's shard-failure error and status when every failure is the caller's" in {
      val failure = pitPage(oneOfThreeShardsFailed("script_exception")).get
      failure shouldBe a[JavaClientHelpers.SearchPageFailure]
      failure.getMessage shouldBe
      "PIT search_after failed: 1 of 3 shards failed: script_exception: runtime error " +
      "(caused by date_time_parse_exception: Text 'x' could not be parsed at index 0)"
      probe.statusOf(failure) shouldBe Some(400)
      probe.statusOf(new CompletionException(failure)) shouldBe Some(400)
      isRetriableError(failure) shouldBe false
    }

    "keep the door's own exception, text and retry for any other failure" in {
      val failure = pitPage(oneOfThreeShardsFailed("circuit_breaking_exception")).get
      failure shouldBe an[_root_.java.io.IOException]
      failure.getMessage should startWith("PIT search_after failed: ErrorCause: {")
      probe.statusOf(failure) shouldBe None
      isRetriableError(failure) shouldBe true
    }

    "answer nothing when no shard failed" in {
      pitPage(readShards("""{"total":3,"successful":3,"failed":0}""")) shouldBe None
    }
  }
}
