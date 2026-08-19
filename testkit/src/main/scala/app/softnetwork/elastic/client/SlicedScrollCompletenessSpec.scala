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
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.client.spi.{ElasticClientFactory, ElasticClientSpi}
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import com.typesafe.config.ConfigFactory
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.io.{Source => IoSource}
import scala.language.implicitConversions

/** #238 — sliced PIT row extraction: a no-`ORDER BY` extraction from an N-shard index reads `min(N,
  * max-slices)` slices of ONE PIT concurrently and merges them into the single stream the caller
  * consumes. On real Elasticsearch, for 1 / 3 / 6 shards (+ a 10-shard wildcard): exact row count
  * AND distinct ids on every run, the resolved slice count through `ScrollMetrics.slices`, the
  * sequential guarantees (`ORDER BY`, `maxSlices = Some(1)`, the HOCON opt-out), the quota binding
  * on the merged total, and the single PIT close (no open search context left behind).
  *
  * The `slices == N` expectations apply only where PIT slicing exists (ES >= 7.15); completeness
  * and `slices == 1` on ES 6 / `maxSlices = Some(1)` are asserted unconditionally.
  */
trait SlicedScrollCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  /** A second client over the same cluster with extra HOCON. Straight from the SPI:
    * [[ElasticClientFactory]] caches clients per cluster URL, so a second `create` would return the
    * first client regardless of configuration.
    */
  private def spiClient(hocon: String): ElasticClientApi =
    java.util.ServiceLoader
      .load(classOf[ElasticClientSpi])
      .iterator()
      .next()
      .client(ConfigFactory.parseString(hocon).withFallback(elasticConfig))

  private val oneShard = "sliced_1"
  private val threeShards = "sliced_3"
  private val sixShards = "sliced_6"

  private val oneShardDocs = 2000
  private val threeShardDocs = 6000
  private val sixShardDocs = 12000

  private lazy val pitSlicing: Boolean = client.version match {
    case ElasticSuccess(v) => ElasticsearchVersion.supportsPitSlicing(v)
    case ElasticFailure(_) => false
  }

  /** The slice count core must have resolved: `n` where PIT slicing exists, 1 everywhere else. */
  private def expectedSlices(n: Int): Int =
    if (pitSlicing) math.min(n, ScrollConfig.DefaultMaxSlices) else 1

  private def indexDocs(indexName: String, count: Int, shards: Int): Unit = {
    // explicit number_of_shards is mandatory: the testkit pins a 1-shard wildcard template
    val settings = s"""{"number_of_shards": $shards, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":    { "type": "keyword" },
        |    "value": { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(indexName, settings = settings).get shouldBe true
    client.setMapping(indexName, mapping).get shouldBe true

    val docs = (1 to count).map(i => s"""{"id":"${indexName}_$i","value":$i}""").toList

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = indexName,
      logEvery = 10000
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }

    client.refresh(indexName)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    indexDocs(oneShard, oneShardDocs, 1)
    indexDocs(threeShards, threeShardDocs, 3)
    indexDocs(sixShards, sixShardDocs, 6)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(oneShard)
    client.deleteIndex(threeShards)
    client.deleteIndex(sixShards)
    system.terminate()
    super.afterAll()
  }

  /** Run `body` with a second SPI client and close it afterwards (own pools / transports). */
  private def withSpiClient[T](hocon: String)(body: ElasticClientApi => T): T = {
    val c = spiClient(hocon)
    try body(c)
    finally c.close()
  }

  private type Row = (ListMap[String, Any], ScrollMetrics)

  private def run(
    sql: String,
    config: Option[ScrollConfig],
    c: ElasticClientApi = client
  ): Seq[Row] = {
    val source = config match {
      case Some(cfg) => c.scroll(SelectStatement(sql), cfg)
      case None      => c.scroll(SelectStatement(sql))
    }
    Await.result(source.runWith(Sink.seq), 5.minutes)
  }

  private def assertComplete(rows: Seq[Row], expected: Int, slices: Int, label: String): Unit = {
    rows should have size expected.toLong
    rows.map(_._1("id").toString).toSet should have size expected.toLong
    rows.head._2.slices shouldBe slices
    rows.last._2.slices shouldBe slices
    log.info(s"✓ $label: ${rows.size} rows, ${rows.size} distinct ids, slices = $slices")
  }

  /** Open search contexts across every node — a leaked PIT shows up here. Polled because the close
    * is issued from `watchTermination` right after the stream completes.
    */
  private def openContexts(): Int = {
    val response = restClient.performRequest(
      new Request(
        "GET",
        "/_nodes/stats/indices/search?filter_path=nodes.*.indices.search.open_contexts"
      )
    )
    val stream = response.getEntity.getContent
    val body =
      try IoSource.fromInputStream(stream).mkString
      finally stream.close()
    // the probe must never pass vacuously: the key has to be there
    body should include("open_contexts")
    "\"open_contexts\"\\s*:\\s*(\\d+)".r.findAllMatchIn(body).map(_.group(1).toInt).sum
  }

  /** Positive control for the probe: a PIT opened by the test IS counted as an open context. */
  private def assertProbeCountsPits(): Unit = {
    val open = restClient.performRequest(new Request("POST", s"/$sixShards/_pit?keep_alive=1m"))
    val openStream = open.getEntity.getContent
    val pitId =
      try "\"id\"\\s*:\\s*\"([^\"]+)\"".r
        .findFirstMatchIn(IoSource.fromInputStream(openStream).mkString)
        .map(_.group(1))
        .getOrElse(fail("no PIT id in the open response"))
      finally openStream.close()
    try openContexts() should be > 0
    finally {
      val close = new Request("DELETE", "/_pit")
      close.setJsonEntity(s"""{"id":"$pitId"}""")
      restClient.performRequest(close)
    }
  }

  private def awaitNoOpenContext(): Unit = {
    val deadline = System.currentTimeMillis() + 5000
    var open = openContexts()
    while (open > 0 && System.currentTimeMillis() < deadline) {
      Thread.sleep(100)
      open = openContexts()
    }
    open shouldBe 0
  }

  // ---- (a) one shard -------------------------------------------------------------------------

  "a no-ORDER-BY extraction from a 1-shard index" should "be complete and sequential (slices = 1)" in {
    val rows = run(s"SELECT id, value FROM $oneShard", Some(ScrollConfig(scrollSize = 500)))
    assertComplete(rows, oneShardDocs, 1, "1 shard")
  }

  // ---- (b) three shards -----------------------------------------------------------------------

  "a no-ORDER-BY extraction from a 3-shard index" should "open one slice per primary shard" in {
    val rows = run(s"SELECT id, value FROM $threeShards", Some(ScrollConfig(scrollSize = 500)))
    assertComplete(rows, threeShardDocs, expectedSlices(3), "3 shards")
  }

  // (f) partial last page
  it should "be complete with a page size that does not divide the row count" in {
    // 6000 % 7 != 0 per slice as well — exercises every slice's partial last page
    val rows = run(s"SELECT id FROM $threeShards", Some(ScrollConfig(scrollSize = 7)))
    assertComplete(rows, threeShardDocs, expectedSlices(3), "3 shards, page 7")
  }

  // (h) ORDER BY
  it should "stay sequential and ordered with ORDER BY" in {
    val rows =
      run(
        s"SELECT id, value FROM $threeShards ORDER BY value",
        Some(ScrollConfig(scrollSize = 500))
      )
    assertComplete(rows, threeShardDocs, 1, "3 shards, ORDER BY")
    val values = rows.map(_._1("value").toString.toInt)
    values shouldBe values.sorted
  }

  // ---- (c) six shards -------------------------------------------------------------------------

  "a no-ORDER-BY extraction from a 6-shard index" should "open min(6, max-slices) = 6 slices" in {
    val rows = run(s"SELECT id, value FROM $sixShards", Some(ScrollConfig(scrollSize = 500)))
    assertComplete(rows, sixShardDocs, expectedSlices(6), "6 shards")
  }

  // (d) explicit ceiling below the shard count
  it should "honour maxSlices = Some(2) (still a whole-shard split on the Elasticsearch side)" in {
    // With max < shards ES assigns whole shards to slices (shardIndex % max) — never a
    // per-document filter — so 2 slices over 6 shards is cheap and complete.
    val rows =
      run(s"SELECT id FROM $sixShards", Some(ScrollConfig(scrollSize = 500, maxSlices = Some(2))))
    assertComplete(rows, sixShardDocs, if (pitSlicing) 2 else 1, "6 shards, maxSlices 2")
  }

  // (e) explicit opt-out — asserted UNCONDITIONALLY
  it should "page sequentially with maxSlices = Some(1)" in {
    val rows =
      run(s"SELECT id FROM $sixShards", Some(ScrollConfig(scrollSize = 500, maxSlices = Some(1))))
    assertComplete(rows, sixShardDocs, 1, "6 shards, maxSlices 1")
  }

  // (g) quota on the merged total
  it should "bind maxDocuments on the MERGED total (exactly 2,500 distinct rows)" in {
    val rows = run(
      s"SELECT id FROM $sixShards",
      Some(ScrollConfig(scrollSize = 500, maxDocuments = Some(2500)))
    )
    rows should have size 2500
    rows.map(_._1("id").toString).toSet should have size 2500
    rows.head._2.slices shouldBe expectedSlices(6)
  }

  // (j) mechanical AC 5 — the PIT is closed exactly once and never leaked
  it should "leave no open search context behind" in {
    // positive control first (PIT API exists from 7.10; on ES 6 the path has no PIT to leak)
    if (client.version.toOption.exists(ElasticsearchVersion.supportsPit)) assertProbeCountsPits()
    awaitNoOpenContext()
    val rows = run(s"SELECT id FROM $sixShards", Some(ScrollConfig(scrollSize = 500)))
    rows should have size sixShardDocs.toLong
    awaitNoOpenContext()
    // ...and a cancelled (quota-capped) stream closes its PIT too
    val capped = run(
      s"SELECT id FROM $sixShards",
      Some(ScrollConfig(scrollSize = 500, maxDocuments = Some(700)))
    )
    capped should have size 700
    awaitNoOpenContext()
  }

  // ---- (i) wildcard: several concrete indices, shard SUM, and the cap ---------------------------

  "a no-ORDER-BY extraction over sliced_* (1 + 3 + 6 = 10 shards)" should "sum the shards and cap at max-slices" in {
    val rows = run("SELECT id FROM sliced_*", Some(ScrollConfig(scrollSize = 500)))
    assertComplete(
      rows,
      oneShardDocs + threeShardDocs + sixShardDocs,
      expectedSlices(10),
      "sliced_* (10 shards)"
    )
    if (pitSlicing) rows.head._2.slices shouldBe ScrollConfig.DefaultMaxSlices
  }

  // ---- (k) HOCON opt-out reaches an explicit config ---------------------------------------------

  "elastic.scroll.max-slices = 1" should "disable slicing for an explicit ScrollConfig that leaves maxSlices unset" in {
    withSpiClient("elastic.scroll.max-slices = 1") { optOut =>
      val rows = run(s"SELECT id FROM $sixShards", Some(ScrollConfig(scrollSize = 500)), optOut)
      assertComplete(rows, sixShardDocs, 1, "6 shards, HOCON max-slices = 1")
    }
  }

  // ---- (l) HOCON page size on the no-argument path ---------------------------------------------

  "elastic.scroll.size = 250" should "drive the no-argument scroll path" in {
    withSpiClient("elastic.scroll.size = 250") { paged =>
      val rows = run(s"SELECT id FROM $threeShards", None, paged)
      assertComplete(rows, threeShardDocs, expectedSlices(3), "3 shards, HOCON size = 250")
      // 6,000 rows in 250-row batches on the merged stream
      rows.last._2.totalBatches shouldBe (threeShardDocs / 250).toLong
      rows.last._2.totalDocuments shouldBe threeShardDocs.toLong
    }
  }
}
