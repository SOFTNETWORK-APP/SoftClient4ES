package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.schema.SchemaCacheTtl
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger

/** The per-index schema-cache TTL (story 21.8 Part D).
  *
  * Since #306 every executed query reads the cached schema, so a stale entry no longer means a
  * stale column list — it means wrong emitted Painless. The TTL therefore stopped being a
  * performance knob, and this spec pins the three things that makes necessary: it is configurable,
  * an index may override it for itself, and the caches that key on an index agree about it.
  *
  * 🔴 Everything here is asserted through OBSERVED FETCHES, never through the resolver's return
  * value alone: a resolver that answers `1h` while the cache re-reads every five minutes would pass
  * a value-only test and fail every user.
  */
class SchemaCacheTtlApiSpec extends AnyFlatSpec with Matchers {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private def mappingJson(ttl: Option[String]): String = {
    val meta = ttl match {
      case Some(v) => s""""_meta":{"${SchemaCacheTtl.MetadataKey}":"$v"},"""
      case None    => ""
    }
    s"""{"mappings":{$meta"properties":{"id":{"type":"keyword"}}},
       | "settings":{"index":{"number_of_shards":"3"}}}""".stripMargin
  }

  /** @param declared
    *   index -> the TTL that index declares in its own `_meta` (None = declares none)
    * @param aliasOf
    *   alias -> the concrete index `GET <alias>` answers with
    */
  private class TtlClient(
    declared: Map[String, Option[String]] = Map.empty,
    hocon: String = "",
    aliasOf: Map[String, String] = Map.empty
  ) extends NopeClientApi {
    override protected def logger: Logger = testLogger
    override def config: Config = ConfigFactory.parseString(hocon)

    val getIndexCalls = new AtomicInteger(0)
    val settingsCalls = new AtomicInteger(0)

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      getIndexCalls.incrementAndGet()
      // `GET <alias>` answers with the TARGET's document, so the metadata read is the target's.
      val body = mappingJson(declared.getOrElse(aliasOf.getOrElse(index, index), None))
      ElasticSuccess(
        Some(aliasOf.get(index).map(target => s"""{"$target":$body}""").getOrElse(body))
      )
    }

    override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
      settingsCalls.incrementAndGet()
      ElasticSuccess("""{"idx":{"settings":{"index":{"number_of_shards":"3"}}}}""")
    }

    // The two resolutions under test, exposed: both are `protected` on the trait.
    def defaultTtl: Long = schemaCacheTtlMs
    def ttlFor(index: String): Long = schemaCacheTtlMsFor(index)
    def missTtl: Long = schemaMissTtlMs
  }

  private def load(client: TtlClient, index: String): Unit =
    client.loadSchema(index) match {
      case ElasticSuccess(_) => ()
      case failure           => fail(s"loadSchema($index) failed: $failure")
    }

  // -- the default -------------------------------------------------------------

  "the default TTL" should "be five minutes when nothing configures it" in {
    val client = new TtlClient()
    client.defaultTtl shouldBe SchemaCacheSettings.DefaultTtlMs
    load(client, "orders")
    load(client, "orders")
    client.getIndexCalls.get() shouldBe 1 // the second read is a cache hit
  }

  it should "come from elastic.schema-cache.ttl" in {
    val client = new TtlClient(hocon = "elastic.schema-cache.ttl = 42m")
    client.defaultTtl shouldBe 42 * 60 * 1000L
  }

  it should "be honoured, not merely reported" in {
    val client = new TtlClient(hocon = "elastic.schema-cache.ttl = 1ms")
    load(client, "orders")
    Thread.sleep(20)
    load(client, "orders")
    client.getIndexCalls.get() shouldBe 2 // expired, so it was read again
  }

  it should "refuse a non-positive value rather than cache forever" in {
    // `ElasticConfig` reports a rejected setting as a ConfigException naming the key — the require
    // in `SchemaCacheSettings` is what produces the message.
    val failure =
      the[Exception] thrownBy new TtlClient(hocon = "elastic.schema-cache.ttl = 0s").defaultTtl
    failure.getMessage should include("elastic.schema-cache.ttl must be positive")
  }

  // -- the per-index override --------------------------------------------------

  "an index that declares its own TTL" should "override the configured default" in {
    // The default has already expired; the index asked to be remembered for an hour.
    val client =
      new TtlClient(
        declared = Map("orders" -> Some("1h")),
        hocon = "elastic.schema-cache.ttl = 1ms"
      )
    load(client, "orders")
    client.ttlFor("orders") shouldBe 3600000L
    Thread.sleep(20)
    load(client, "orders")
    client.getIndexCalls.get() shouldBe 1 // still a hit: the index's own clock governs
  }

  it should "be able to SHORTEN a long default" in {
    val client =
      new TtlClient(declared = Map("logs" -> Some("1ms")), hocon = "elastic.schema-cache.ttl = 1h")
    load(client, "logs")
    client.ttlFor("logs") shouldBe 1L
    Thread.sleep(20)
    load(client, "logs")
    client.getIndexCalls.get() shouldBe 2
  }

  it should "leave every OTHER index on the default" in {
    val client = new TtlClient(
      declared = Map("orders" -> Some("1h")),
      hocon = "elastic.schema-cache.ttl = 30m"
    )
    load(client, "orders")
    load(client, "logs")
    client.ttlFor("orders") shouldBe 3600000L
    client.ttlFor("logs") shouldBe 30 * 60 * 1000L
    load(client, "orders")
    load(client, "logs")
    client.getIndexCalls.get() shouldBe 2 // both still cached, neither re-read
  }

  it should "fall back to the default, with a WARN, when what it declares is not a duration" in {
    val client = new TtlClient(declared = Map("orders" -> Some("soon")))
    load(client, "orders")
    client.ttlFor("orders") shouldBe SchemaCacheSettings.DefaultTtlMs
  }

  "an index whose schema is not cached" should "resolve to the default" in {
    // 🔴 Answering must never cost a round trip — this is called on the paging path.
    val client = new TtlClient(declared = Map("orders" -> Some("1h")))
    client.ttlFor("orders") shouldBe SchemaCacheSettings.DefaultTtlMs
    client.getIndexCalls.get() shouldBe 0
  }

  "an ALIAS" should "inherit the TTL of the index it resolves to" in {
    // D.3.3: the alias entry caches the TARGET's schema, so it carries the target's `_meta`.
    val client = new TtlClient(
      declared = Map("orders_v2" -> Some("1h")),
      hocon = "elastic.schema-cache.ttl = 1ms",
      aliasOf = Map("orders" -> "orders_v2")
    )
    load(client, "orders")
    client.ttlFor("orders") shouldBe 3600000L
    // …and observed, not merely reported: the default has long expired, so a re-read here would
    // mean the alias had NOT inherited its target's clock.
    Thread.sleep(20)
    load(client, "orders")
    client.getIndexCalls.get() shouldBe 1
  }

  // -- the coupled caches ------------------------------------------------------

  "the shard-count cache" should "follow the TTL the index declares" in {
    val client =
      new TtlClient(
        declared = Map("orders" -> Some("1h")),
        hocon = "elastic.schema-cache.ttl = 1ms"
      )
    load(client, "orders")
    client.cachedPrimaryShardCount(Seq("orders"))
    Thread.sleep(20)
    client.cachedPrimaryShardCount(Seq("orders"))
    client.settingsCalls.get() shouldBe 1 // the index asked for an hour; both caches obey it
  }

  it should "expire with a shortened index TTL, not on its own five minutes" in {
    val client =
      new TtlClient(declared = Map("logs" -> Some("1ms")), hocon = "elastic.schema-cache.ttl = 1h")
    load(client, "logs")
    client.cachedPrimaryShardCount(Seq("logs"))
    Thread.sleep(20)
    client.cachedPrimaryShardCount(Seq("logs"))
    client.settingsCalls.get() shouldBe 2
  }

  it should "take the SHORTEST TTL among the indices a key names" in {
    // The key is an index SET: no member may be remembered longer than it asked to be.
    val client = new TtlClient(
      declared = Map("orders" -> Some("1h"), "logs" -> Some("1ms")),
      hocon = "elastic.schema-cache.ttl = 1h"
    )
    load(client, "orders")
    load(client, "logs")
    client.cachedPrimaryShardCount(Seq("orders", "logs"))
    Thread.sleep(20)
    client.cachedPrimaryShardCount(Seq("orders", "logs"))
    // `primaryShardCount` probes `_settings` once per index expression, so a re-probe of this
    // 2-index key is 4 calls in total; the control below is what makes that number mean something.
    client.settingsCalls.get() shouldBe 4
  }

  it should "keep a set whose members ALL asked to be remembered" in {
    val client = new TtlClient(
      declared = Map("orders" -> Some("1h"), "logs" -> Some("1h")),
      hocon = "elastic.schema-cache.ttl = 1ms"
    )
    load(client, "orders")
    load(client, "logs")
    client.cachedPrimaryShardCount(Seq("orders", "logs"))
    Thread.sleep(20)
    client.cachedPrimaryShardCount(Seq("orders", "logs"))
    client.settingsCalls.get() shouldBe 2 // one probe, served from the cache the second time
  }

  "the 404 negative cache" should "use the DEFAULT, never a per-index value" in {
    // D.3.2: it records a MISS — there was no schema, so there is no metadata to read a TTL from.
    val client =
      new TtlClient(declared = Map("orders" -> Some("1h")), hocon = "elastic.schema-cache.ttl = 3m")
    load(client, "orders")
    client.missTtl shouldBe 3 * 60 * 1000L
  }

  it should "follow a SHORTENED default but never be lengthened past five minutes" in {
    // Nothing invalidates a miss and no lookup is attempted while one stands, so an
    // operator-lengthened TTL would leave a table created after a failed probe running with NO
    // schema attached — no conversions, no temporal resolution — for that whole period.
    new TtlClient(hocon = "elastic.schema-cache.ttl = 30s").missTtl shouldBe 30000L
    new TtlClient(hocon = "elastic.schema-cache.ttl = 6h").missTtl shouldBe
    SchemaCacheSettings.DefaultTtlMs
  }

  // -- the bound ---------------------------------------------------------------

  "the schema cache" should "purge expired entries once it grows past its threshold" in {
    // Unbounded while it was cold; since #306 every query writes to it, and a deployment that mints
    // dated indices would grow without bound.
    val client = new TtlClient(hocon = "elastic.schema-cache.ttl = 1ms")
    (1 to 300).foreach(i => load(client, s"logs-$i"))
    Thread.sleep(20)
    load(client, "logs-301")
    client.schemaCacheSize should be < 300
  }

  it should "keep entries that have NOT expired, whatever the map's size" in {
    val client = new TtlClient(hocon = "elastic.schema-cache.ttl = 1h")
    (1 to 300).foreach(i => load(client, s"logs-$i"))
    load(client, "logs-301")
    client.schemaCacheSize shouldBe 301
  }

  it should "drop the cache outright rather than grow without limit on long TTLs" in {
    // Expiry alone bounds nothing when an index asks for a long TTL and the names keep changing
    // (dated or per-tenant indices), and the values here are whole schemas.
    val client = new TtlClient(hocon = "elastic.schema-cache.ttl = 30d")
    (1 to 1100).foreach(i => load(client, s"logs-$i"))
    client.schemaCacheSize should be < 1100
  }

  // -- the expiry rule ---------------------------------------------------------

  "the expiry rule" should "be shared, and treat exactly-the-TTL as expired" in {
    val entry = CachedShardCount(ElasticSuccess(3), cachedAt = 1000L, ttlMs = 100L)
    entry.isExpired(1099L) shouldBe false
    entry.isExpired(1100L) shouldBe true
  }
}
