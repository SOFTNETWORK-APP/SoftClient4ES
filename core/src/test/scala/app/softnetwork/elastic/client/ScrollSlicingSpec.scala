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
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.sql.query.{SQLAggregation, SelectStatement}
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import java.util.concurrent.atomic.{AtomicInteger, AtomicReference}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

/** #238 — the slice POLICY, decided once per stream in core (`ScrollApi.resolveSlices`) and handed
  * to the client through `ScrollConfig.slices` / `ScrollMetrics.slices`. No Docker: a
  * [[NopeClientApi]] subclass fakes the version, the `_settings` payload and the three page
  * sources, and records the `ScrollConfig` that reaches `pitSearchAfter`.
  */
class ScrollSlicingSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with MockitoSugar
    with ArgumentMatchersSugar {

  implicit val system: ActorSystem = ActorSystem("scroll-slicing-spec")
  implicit val context: ConversionContext = NativeContext

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def shards(index: String, n: Int): String =
    s"""{"$index":{"settings":{"index":{"number_of_shards":"$n"}}}}"""

  private val defaultSettings: Map[String, ElasticResult[String]] = Map(
    "idx_a"    -> ElasticSuccess(shards("idx_a", 3)),
    "idx_one"  -> ElasticSuccess(shards("idx_one", 1)),
    "idx_six"  -> ElasticSuccess(shards("idx_six", 6)),
    "idx_big"  -> ElasticSuccess(shards("idx_big", 12)),
    "idx_fail" -> ElasticFailure(ElasticError("settings unavailable")),
    "idx_bad"  -> ElasticSuccess("this is not json"),
    "idx_forbidden" -> ElasticFailure(
      ElasticError(
        "action [indices:monitor/settings/get] is unauthorized",
        cause = Some(new RuntimeException("security_exception")),
        statusCode = Some(403)
      )
    )
  )

  /** Overrides `version` (NOT `executeVersion`: VersionApi caches the first answer), the
    * `_settings` payload and the page sources. `rows` rows per stream, each carrying the slice
    * count the client saw.
    */
  private class SlicingClient(
    val mockLogger: Logger,
    esVersion: String = "8.18.3",
    ceiling: Int = ScrollConfig.DefaultMaxSlices,
    settings: Map[String, ElasticResult[String]] = defaultSettings,
    rows: Int = 3,
    ttlMs: Long = 5 * 60 * 1000L
  ) extends NopeClientApi {
    override protected def logger: Logger = mockLogger
    override def version: ElasticResult[String] = ElasticSuccess(esVersion)
    override protected def configuredMaxSlices: Int = ceiling
    override protected def shardCountCacheTtlMs: Long = ttlMs

    val pitConfig = new AtomicReference[ScrollConfig]()
    val classicConfig = new AtomicReference[ScrollConfig]()
    val pitCalls = new AtomicInteger(0)
    val settingsCalls = new AtomicInteger(0)

    override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
      settingsCalls.incrementAndGet()
      settings.getOrElse(index, ElasticSuccess("{}"))
    }

    private def page: Source[ListMap[String, Any], NotUsed] =
      Source(List.tabulate(rows)(i => ListMap[String, Any]("id" -> s"doc-$i")))

    override private[client] def pitSearchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = {
      pitCalls.incrementAndGet()
      pitConfig.set(config)
      page
    }

    override private[client] def scrollClassic(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      aggregations: ListMap[String, SQLAggregation],
      config: ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = {
      classicConfig.set(config)
      page
    }
  }

  private def run(
    client: SlicingClient,
    sql: String,
    config: Option[ScrollConfig] = None
  ): Seq[(ListMap[String, Any], ScrollMetrics)] = {
    val source = config match {
      case Some(c) => client.scroll(SelectStatement(sql), c)
      case None    => client.scroll(SelectStatement(sql))
    }
    Await.result(source.runWith(Sink.seq), 30.seconds)
  }

  private def slicedInfo(logger: Logger, times: Int): Unit =
    verify(logger, org.mockito.Mockito.times(times))
      .info(argThat[String]((s: String) => s != null && s.startsWith("Sliced PIT paging:")))

  private def shardWarn(logger: Logger, times: Int): Unit =
    verify(logger, org.mockito.Mockito.times(times))
      .warn(
        argThat[String]((s: String) =>
          s != null && s.startsWith("Could not resolve the primary shard count")
        )
      )

  /** The DEBUG replay of a cached privilege failure. */
  private def shardDebug(logger: Logger, times: Int): Unit =
    verify(logger, org.mockito.Mockito.times(times))
      .debug(
        argThat[String]((s: String) =>
          s != null && s.startsWith("Primary shard count for") && s.contains("still unresolved")
        )
      )

  // ---------------------------------------------------------------------------------------------

  "ScrollApi" should "slice a no-ORDER-BY PIT extraction once per primary shard (3 shards → 3)" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_a")
    rows should have size 3
    client.pitConfig.get().slices shouldBe 3
    client.pitConfig.get().metrics.slices shouldBe 3
    rows.head._2.slices shouldBe 3
    rows.last._2.slices shouldBe 3
    slicedInfo(client.mockLogger, 1)
  }

  it should "never slice above the shard count (1 shard → 1, no INFO line)" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_one")
    client.pitConfig.get().slices shouldBe 1
    rows.head._2.slices shouldBe 1
    slicedInfo(client.mockLogger, 0)
  }

  it should "cap at the default ceiling of 8 (12 shards → 8) and read 6 shards as 6" in {
    val big = new SlicingClient(mock[Logger])
    run(big, "SELECT id FROM idx_big").head._2.slices shouldBe 8
    val six = new SlicingClient(mock[Logger])
    run(six, "SELECT id FROM idx_six").head._2.slices shouldBe 6
  }

  it should "honour an explicit maxSlices = Some(2) (still a whole-shard split on the ES side)" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_a", Some(ScrollConfig(maxSlices = Some(2))))
    rows.head._2.slices shouldBe 2
    client.pitConfig.get().slices shouldBe 2
  }

  it should "page sequentially on maxSlices = Some(1)" in {
    val client = new SlicingClient(mock[Logger])
    run(
      client,
      "SELECT id FROM idx_a",
      Some(ScrollConfig(maxSlices = Some(1)))
    ).head._2.slices shouldBe 1
    slicedInfo(client.mockLogger, 0)
  }

  it should "let the configured ceiling reach an explicit config that leaves maxSlices = None (opt-out)" in {
    val client = new SlicingClient(mock[Logger], ceiling = 1)
    run(
      client,
      "SELECT id FROM idx_a",
      Some(ScrollConfig(scrollSize = 100))
    ).head._2.slices shouldBe 1
    client.pitConfig.get().maxSlices shouldBe None
    slicedInfo(client.mockLogger, 0)
  }

  it should "page sequentially on ES 7.12–7.14 (PIT without slicing)" in {
    val client = new SlicingClient(mock[Logger], esVersion = "7.14.0")
    run(client, "SELECT id FROM idx_a").head._2.slices shouldBe 1
    client.pitCalls.get() shouldBe 1
  }

  it should "slice on ES 7.15" in {
    val client = new SlicingClient(mock[Logger], esVersion = "7.15.0")
    run(client, "SELECT id FROM idx_a").head._2.slices shouldBe 3
  }

  it should "keep ORDER BY sequential" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_a ORDER BY value").head._2.slices shouldBe 1
    client.pitConfig.get().slices shouldBe 1
  }

  it should "keep an explicit LIMIT sequential" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_a LIMIT 5").head._2.slices shouldBe 1
    client.pitConfig.get().maxSlices shouldBe Some(1)
  }

  it should "keep a windowed statement with a LIMIT sequential (clamp before the window branch)" in {
    val client = new SlicingClient(mock[Logger])
    run(
      client,
      "SELECT id, value, ROW_NUMBER() OVER (PARTITION BY id ORDER BY value) AS rn FROM idx_a LIMIT 5"
    )
    val cfg = client.pitConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe Some(1)
    cfg.slices shouldBe 1
  }

  it should "keep a windowed statement WITHOUT a LIMIT sliced" in {
    val client = new SlicingClient(mock[Logger])
    run(
      client,
      "SELECT id, value, ROW_NUMBER() OVER (PARTITION BY id ORDER BY value) AS rn FROM idx_a"
    )
    val cfg = client.pitConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe None
    cfg.slices shouldBe 3
  }

  it should "degrade to sequential with exactly one WARN when the _settings lookup fails" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_fail")
    rows should have size 3
    rows.head._2.slices shouldBe 1
    shardWarn(client.mockLogger, 1)
    slicedInfo(client.mockLogger, 0)
  }

  it should "degrade to sequential with exactly one WARN on a malformed _settings payload, without throwing" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_bad")
    rows should have size 3
    rows.head._2.slices shouldBe 1
    shardWarn(client.mockLogger, 1)
  }

  it should "never consult _settings nor slice when preferSearchAfter = false (classic scroll)" in {
    val client = new SlicingClient(mock[Logger])
    val rows = run(client, "SELECT id FROM idx_a", Some(ScrollConfig(preferSearchAfter = false)))
    rows.head._2.slices shouldBe 1
    client.pitCalls.get() shouldBe 0
    client.classicConfig.get().slices shouldBe 1
    slicedInfo(client.mockLogger, 0)
  }

  it should "hand the resolved count to the client even when the stream yields zero rows" in {
    // (named arg + a later local `rows` trips 2.12's forward-reference parsing — hence `emitted`)
    val client = new SlicingClient(mock[Logger], rows = 0)
    val emitted = run(client, "SELECT id FROM idx_a")
    emitted shouldBe empty
    client.pitConfig.get().slices shouldBe 3
    client.pitConfig.get().metrics.slices shouldBe 3
    slicedInfo(client.mockLogger, 1)
  }

  it should "surface a version failure as a FAILED stream, never a synchronous throw out of scroll()" in {
    val client = new SlicingClient(mock[Logger]) {
      override def version: ElasticResult[String] =
        ElasticFailure(ElasticError("cluster unreachable"))
    }
    // building the source must not throw (gateway / JDBC build it eagerly)…
    val source = client.scroll(SelectStatement("SELECT id FROM idx_a"))
    // …the failure surfaces when the stream runs
    val failure = Await.result(source.runWith(Sink.seq).failed, 30.seconds)
    failure.getMessage should include("Failed to get ES version")
  }

  it should "resolve once per stream (one INFO line per extraction) and bind the quota on the merged total" in {
    val client = new SlicingClient(mock[Logger], rows = 10)
    val emitted = run(client, "SELECT id FROM idx_a", Some(ScrollConfig(maxDocuments = Some(4))))
    emitted should have size 4
    emitted.head._2.slices shouldBe 3
    slicedInfo(client.mockLogger, 1)
  }

  // ---------------------------------------------------------------------------------------------
  // Shard-count cache (#238, review decision 3c)

  it should "serve the primary shard count from the cache within the TTL (one _settings round-trip for two extractions)" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_a").head._2.slices shouldBe 3
    run(client, "SELECT id FROM idx_a").head._2.slices shouldBe 3
    client.settingsCalls.get() shouldBe 1
    slicedInfo(client.mockLogger, 2) // the per-extraction INFO line is NOT cached away
  }

  it should "key the cache by the (sorted, distinct) index set" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_a").head._2.slices shouldBe 3
    run(client, "SELECT id FROM idx_six").head._2.slices shouldBe 6
    client.settingsCalls.get() shouldBe 2
    // a new SET is a new key (one lookup per expression) …
    client.cachedPrimaryShardCount(Seq("idx_six", "idx_a", "idx_a")) shouldBe (
      (
        ElasticSuccess(9),
        false
      )
    )
    client.settingsCalls.get() shouldBe 4
    // … and the same set in another order / with duplicates is the same entry
    client.cachedPrimaryShardCount(Seq("idx_a", "idx_six")) shouldBe ((ElasticSuccess(9), true))
    client.settingsCalls.get() shouldBe 4
  }

  it should "consult _settings again once the TTL has expired" in {
    val client = new SlicingClient(mock[Logger], ttlMs = 0L)
    run(client, "SELECT id FROM idx_a")
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 2
  }

  it should "NOT cache a transient lookup failure: every extraction probes again and WARNs (no RBAC blame)" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_fail").head._2.slices shouldBe 1
    run(client, "SELECT id FROM idx_fail").head._2.slices shouldBe 1
    client.settingsCalls.get() shouldBe 2
    shardWarn(client.mockLogger, 2)
    verify(client.mockLogger, org.mockito.Mockito.times(2))
      .warn(
        argThat[String]((s: String) => s != null && s.contains("retried on the next extraction"))
      )
    verify(client.mockLogger, org.mockito.Mockito.never())
      .warn(argThat[String]((s: String) => s != null && s.contains("view_index_metadata")))
    shardDebug(client.mockLogger, 0)
  }

  it should "cache a PRIVILEGE failure (403): ONE WARN naming the privilege per TTL, the replay at DEBUG, no stack pinned" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_forbidden").head._2.slices shouldBe 1
    run(client, "SELECT id FROM idx_forbidden").head._2.slices shouldBe 1
    client.settingsCalls.get() shouldBe 1
    shardWarn(client.mockLogger, 1)
    verify(client.mockLogger)
      .warn(
        argThat[String]((s: String) =>
          s != null && s.contains("view_index_metadata") && s.contains("HTTP 403")
        )
      )
    shardDebug(client.mockLogger, 1)
    // the cached failure carries no Throwable (the original did)
    val (cached, hit) = client.cachedPrimaryShardCount(Seq("idx_forbidden"))
    hit shouldBe true
    cached match {
      case ElasticFailure(err) => err.cause shouldBe None
      case other               => fail(s"expected a cached failure, got $other")
    }
  }

  it should "NOT cache an expression that matches no index (a later CREATE must be seen at once)" in {
    val client = new SlicingClient(mock[Logger])
    // `idx_empty` is not in the settings map → NopeClientApi-style "{}" → 0 shards → sequential
    run(client, "SELECT id FROM idx_empty").head._2.slices shouldBe 1
    run(client, "SELECT id FROM idx_empty").head._2.slices shouldBe 1
    client.settingsCalls.get() shouldBe 2
    shardWarn(client.mockLogger, 0) // an empty match is not a failure
  }

  it should "clear EVERY cached count on invalidateSchema(index) — wildcard and alias keys cannot be matched by name" in {
    val client = new SlicingClient(mock[Logger])
    run(client, "SELECT id FROM idx_a")
    run(client, "SELECT id FROM idx_six")
    client.settingsCalls.get() shouldBe 2
    client.invalidateSchema("idx_other") // unrelated name — still a full clear, by design
    run(client, "SELECT id FROM idx_a")
    run(client, "SELECT id FROM idx_six")
    client.settingsCalls.get() shouldBe 4
  }

  it should "clear the cache on invalidateAllSchemas(), updateSchema(...) and a successful createIndex" in {
    val client = new SlicingClient(mock[Logger]) {
      // NopeClientApi answers ElasticSuccess(false) ("not created"); make the creation succeed
      override private[client] def executeCreateIndex(
        index: String,
        settings: String,
        mappings: Option[String],
        aliases: Seq[app.softnetwork.elastic.sql.schema.TableAlias]
      ): ElasticResult[Boolean] = ElasticSuccess(true)
    }
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 1
    client.invalidateAllSchemas()
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 2
    client.updateSchema("idx_a", app.softnetwork.elastic.sql.schema.Table("idx_a", Nil))
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 3
    client.createIndex("idx_new").get shouldBe true // NopeClientApi: ElasticSuccess(true)
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 4
    // a no-op confirms the entry is otherwise stable
    run(client, "SELECT id FROM idx_a")
    client.settingsCalls.get() shouldBe 4
  }

  it should "resolve a cold key ONCE under concurrency (one round-trip, one WARN for 8 simultaneous extractions)" in {
    val client = new SlicingClient(mock[Logger])
    val pool = java.util.concurrent.Executors.newFixedThreadPool(8)
    try {
      val start = new java.util.concurrent.CountDownLatch(1)
      val tasks = (1 to 8).map { _ =>
        pool.submit(new java.util.concurrent.Callable[(ElasticResult[Int], Boolean)] {
          def call(): (ElasticResult[Int], Boolean) = {
            start.await()
            client.cachedPrimaryShardCount(Seq("idx_forbidden"))
          }
        })
      }
      start.countDown()
      val outcomes = tasks.map(_.get(30, java.util.concurrent.TimeUnit.SECONDS))
      client.settingsCalls.get() shouldBe 1
      outcomes.count(_._2 == false) shouldBe 1 // exactly one caller saw the miss
      outcomes.forall(_._1.isInstanceOf[ElasticFailure]) shouldBe true
    } finally pool.shutdownNow()
  }
}
