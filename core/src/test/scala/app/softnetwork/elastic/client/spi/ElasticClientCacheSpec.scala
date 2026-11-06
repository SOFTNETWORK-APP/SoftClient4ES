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

package app.softnetwork.elastic.client.spi

import app.softnetwork.elastic.client.{ElasticClientApi, ElasticClientDelegator, ElasticConfig}
import app.softnetwork.elastic.client.metrics.MetricsCollector
import app.softnetwork.elastic.client.monitoring.{MonitoredClientHooks, MonitoredElasticClient}
import com.typesafe.config.{Config, ConfigFactory}
import org.mockito.MockitoSugar
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.util.{Base64, UUID}
import java.util.concurrent.{ConcurrentLinkedQueue, CyclicBarrier, Executors, TimeUnit}
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import scala.annotation.tailrec
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.util.Try

/** SCHEMA-14 (AC2, AC3, AC4, AC6; P2-P4) -- the client cache behind `ElasticClientFactory`.
  *
  * The core test classpath registers no `ElasticClientSpi` (ElasticClientFactoryIsolationSpec), so
  * the cache is built here with a builder that hands out mocks and counts its calls, on a clock the
  * test moves. Each entry point of the factory is the cache path it delegates to, with the
  * configuration parsed once, as the factory does.
  */
class ElasticClientCacheSpec extends AnyFlatSpec with Matchers with MockitoSugar {

  import ElasticClientCacheSpec._

  // ==================== fixtures ====================

  /** A cache whose builder hands out mocks and counts its calls, on a clock the test moves. */
  private final class Fixture(failBuild: Boolean = false) {
    val clock = new AtomicLong(0L)
    val builds = new AtomicInteger(0)
    private[this] val raws = new ConcurrentLinkedQueue[ElasticClientApi]()
    val cache = new ElasticClientCache(
      _ => {
        builds.incrementAndGet()
        if (failBuild) throw new IllegalStateException("the provider failed to build a client")
        val raw = mock[ElasticClientApi]
        raws.add(raw)
        raw
      },
      () => clock.get()
    )
    def advance(d: FiniteDuration): Unit = { clock.addAndGet(d.toNanos); () }
    def rawList: List[ElasticClientApi] = raws.asScala.toList
  }

  private def using[A](f: Fixture)(body: Fixture => A): A =
    try body(f)
    finally f.cache.shutdown()

  private val PastTtl: FiniteDuration = ElasticClientCache.IdleTtl + 1.minute

  private def basic(user: String, password: String): Identity =
    Identity(
      "BasicAuth",
      s"""username = "$user", password = "$password"""",
      Map("username" -> user, "password" -> password)
    )

  private val admin = basic("elastic", "admin-pw")
  private val reader = basic("s14_ro", "ro-pw")

  private def flags(metrics: Boolean, monitoring: Boolean): String =
    s"metrics { enabled = $metrics, monitoring { enabled = $monitoring } }"

  private def config(
    identity: Identity,
    metrics: Boolean = true,
    monitoring: Boolean = true
  ): (Config, ElasticConfig) = {
    val config = ConfigFactory
      .parseString(
        s"""elastic {
           |  credentials { host = "localhost", port = 9200, ${identity.hocon} }
           |  watcher     { host = "localhost", port = 9200, ${identity.hocon} }
           |  ${flags(metrics, monitoring)}
           |}""".stripMargin
      )
      .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
    (config, ElasticConfig(config))
  }

  /** The same identity as the JDBC driver assembles it (`ConnectionUrl.toConfig`). */
  private def jdbcConfig(
    identity: Identity,
    metrics: Boolean,
    monitoring: Boolean
  ): (Config, ElasticConfig) = {
    val map = new java.util.HashMap[String, Any]()
    Seq("credentials", "watcher").foreach { side =>
      map.put(s"elastic.$side.host", "localhost")
      map.put(s"elastic.$side.port", 9200)
      identity.map.foreach { case (k, v) => map.put(s"elastic.$side.$k", v) }
    }
    map.put("elastic.metrics.enabled", metrics)
    map.put("elastic.metrics.monitoring.enabled", monitoring)
    val config = ConfigFactory
      .parseString("akka.jvm-exit-on-fatal-error = off")
      .withFallback(ConfigFactory.parseString("softclient4es.license.refresh.enabled = false"))
      .withFallback(ConfigFactory.parseMap(map))
      .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
      .withFallback(ConfigFactory.load())
    (config, ElasticConfig(config))
  }

  private def counts(base: Int, metrics: Int, monitored: Int): Map[String, Int] =
    Map("baseClients" -> base, "metricsClients" -> metrics, "monitoredClients" -> monitored)

  private val entryPoints: Seq[EntryPoint] = Seq(
    EntryPoint(
      "create (metrics off)",
      metrics = false,
      monitoring = false,
      cached = true,
      monitored = false,
      ownMonitor = false,
      counts(1, 0, 0),
      (cache, c, ec) => cache.create(c, ec)
    ),
    EntryPoint(
      "create (default)",
      metrics = true,
      monitoring = true,
      cached = true,
      monitored = true,
      ownMonitor = false,
      counts(1, 0, 1),
      (cache, c, ec) => cache.create(c, ec)
    ),
    EntryPoint(
      "createWithMetrics",
      metrics = true,
      monitoring = false,
      cached = true,
      monitored = false,
      ownMonitor = false,
      counts(1, 1, 0),
      (cache, c, ec) => cache.metrics(c, ec)
    ),
    EntryPoint(
      "createWithMonitoring",
      metrics = true,
      monitoring = true,
      cached = true,
      monitored = true,
      ownMonitor = false,
      counts(1, 0, 1),
      (cache, c, ec) => cache.monitored(c, ec)
    ),
    EntryPoint(
      "createWithCustomMetrics",
      metrics = true,
      monitoring = true,
      cached = false,
      monitored = false,
      ownMonitor = false,
      counts(1, 0, 0),
      (cache, c, ec) => cache.customMetrics(c, ec, new MetricsCollector())
    ),
    EntryPoint(
      "createMonitoredWithCustomMetrics",
      metrics = true,
      monitoring = true,
      cached = false,
      monitored = true,
      ownMonitor = true,
      counts(1, 0, 0),
      (cache, c, ec) => cache.customMonitored(c, ec, new MetricsCollector())
    )
  )

  @tailrec
  private def rawOf(client: ElasticClientApi): ElasticClientApi = client match {
    case d: ElasticClientDelegator => rawOf(d.delegate)
    case other                     => other
  }

  private val ShutdownLine = "Shutting down MonitoredElasticClient"

  private def shutdowns(events: Seq[ClientLogCapture.Event]): Int =
    events.count(_.message.contains(ShutdownLine))

  private def creations(events: Seq[ClientLogCapture.Event], prefix: String): Int =
    events.count(_.message.startsWith(prefix))

  private def concurrently[A](n: Int)(task: Int => A): Seq[A] = {
    val pool = Executors.newFixedThreadPool(n)
    implicit val ec: ExecutionContext = ExecutionContext.fromExecutor(pool)
    val barrier = new CyclicBarrier(n)
    try {
      val futures = (0 until n).map { i =>
        Future {
          barrier.await(30, TimeUnit.SECONDS)
          task(i)
        }
      }
      Await.result(Future.sequence(futures), 60.seconds)
    } finally {
      pool.shutdownNow()
      ()
    }
  }

  // ==================== T0-3: the seams ====================

  "The client-cache seams" should "construct a mocked client and record its close (seam)" in {
    val raw = mock[ElasticClientApi]
    raw.close()
    verify(raw, times(1)).close()
  }

  it should "wrap a client in a SharedClient whose close releases instead of closing (seam)" in {
    val raw = mock[ElasticClientApi]
    val released = new AtomicInteger(0)
    val shared = new SharedClient(raw, () => { released.incrementAndGet(); () })
    shared.delegate shouldBe theSameInstanceAs(raw)
    shared.close()
    released.get shouldBe 1
    verify(raw, never).close()
  }

  // ==================== P2: every entry point ====================

  entryPoints.foreach { ep =>
    def cfg(identity: Identity): (Config, ElasticConfig) =
      config(identity, ep.metrics, ep.monitoring)

    behavior of s"ElasticClientCache through ${ep.name}"

    it should "build one client per identity (two identities, two builds)" in using(new Fixture) {
      f =>
        val (ca, ea) = cfg(admin)
        val (cb, eb) = cfg(reader)
        val a = ep.call(f.cache, ca, ea)
        val b = ep.call(f.cache, cb, eb)
        f.builds.get shouldBe 2
        a should not be theSameInstanceAs(b)
        rawOf(a) should not be theSameInstanceAs(rawOf(b))
        f.cache.stats shouldBe ep.stats.map { case (k, v) => k -> 2 * v }
    }

    it should "share one client between equal configurations of different Config objects" in using(
      new Fixture
    ) { f =>
      val (c1, e1) = cfg(admin)
      val (c2, e2) = cfg(admin)
      val (c3, e3) = jdbcConfig(admin, ep.metrics, ep.monitoring)
      c1 should not be theSameInstanceAs(c2)
      e3 shouldBe e1
      val clients =
        Seq(ep.call(f.cache, c1, e1), ep.call(f.cache, c2, e2), ep.call(f.cache, c3, e3))
      f.builds.get shouldBe 1
      clients.foreach { client =>
        rawOf(client) shouldBe theSameInstanceAs(rawOf(clients.head))
        if (ep.cached) client shouldBe theSameInstanceAs(clients.head)
      }
      if (!ep.cached) clients.distinct should have size 3
      // main's getCacheStats on this path
      f.cache.stats shouldBe ep.stats
    }

    it should "never close the client, or stop its monitor, under another caller (A closes, B open)" in using(
      new Fixture
    ) { f =>
      val (c, e) = cfg(admin)
      val (b, events) = ClientLogCapture.capture {
        val a = ep.call(f.cache, c, e)
        val b = ep.call(f.cache, c, e)
        a.close()
        b
      }
      // canary: the capture sees the INFO creation line
      creations(events, "Creating new") should be > 0
      verify(rawOf(b), never).close()
      // the per-call monitored client stops ITS OWN monitor on close; nobody stops B's
      shutdowns(events) shouldBe (if (ep.ownMonitor) 1 else 0)
      b match {
        case m: MonitoredElasticClient => MonitoredClientHooks.removed(m) shouldBe false
        case _                         => succeed
      }
      f.cache.stats shouldBe ep.stats
    }

    it should "build once over sequential create/close cycles inside the idle TTL" in using(
      new Fixture
    ) { f =>
      val (c, e) = cfg(admin)
      val (_, events) = ClientLogCapture.capture {
        (1 to 50).foreach { _ =>
          ep.call(f.cache, c, e).close()
          // sweeps fall due along the way; no client ever stays idle for the TTL
          f.advance(20.seconds)
        }
      }
      f.builds.get shouldBe 1
      creations(events, "Creating new Elasticsearch client") shouldBe 1
      verify(f.rawList.head, never).close()
    }

    it should "close an idle client once after the TTL and build a new one at the next acquisition" in using(
      new Fixture
    ) { f =>
      val (c, e) = cfg(admin)
      val (_, events) = ClientLogCapture.capture {
        val a = ep.call(f.cache, c, e)
        a.close()
        f.advance(PastTtl)
        val b = ep.call(f.cache, c, e)
        b should not be theSameInstanceAs(a)
        rawOf(b) should not be theSameInstanceAs(rawOf(a))
      }
      f.builds.get shouldBe 2
      val List(first, second) = f.rawList
      verify(first, times(1)).close()
      verify(second, never).close()
      creations(events, "Closing idle Elasticsearch client") shouldBe 1
      // the cached monitor is stopped once, at eviction; a per-call one once, at its close
      shutdowns(events) shouldBe (if (ep.monitored) 1 else 0)
      f.cache.stats shouldBe ep.stats
    }

    it should "floor the share count at zero when one caller closes twice" in using(new Fixture) {
      f =>
        val (c, e) = cfg(admin)
        val a = ep.call(f.cache, c, e)
        a.close()
        a.close()
        val b = ep.call(f.cache, c, e)
        f.builds.get shouldBe 1
        verify(rawOf(b), never).close()
        b.close()
        f.advance(PastTtl)
        ep.call(f.cache, c, e)
        verify(f.rawList.head, times(1)).close()
    }

    it should "make a close after shutdown() a no-op" in using(new Fixture) { f =>
      val (c, e) = cfg(admin)
      val a = ep.call(f.cache, c, e)
      f.cache.shutdown()
      noException should be thrownBy a.close()
      verify(f.rawList.head, times(1)).close()
      f.cache.stats.values.sum shouldBe 0
    }

    it should "build once when 8 threads make the first acquisition at once" in using(new Fixture) {
      f =>
        val (c, e) = cfg(admin)
        val clients = concurrently(8)(_ => ep.call(f.cache, c, e))
        f.builds.get shouldBe 1
        clients.foreach { client =>
          rawOf(client) shouldBe theSameInstanceAs(rawOf(clients.head))
          if (ep.cached) client shouldBe theSameInstanceAs(clients.head)
        }
        f.cache.stats shouldBe ep.stats
    }
  }

  // ==================== P2: the sweep under concurrent monitored acquisitions ====================

  private def user(n: String): Identity = basic(s"user-$n", s"pw-$n")

  /** Idle entries in all three maps: a base (metrics off), a metrics and a monitored client. */
  private def idleInEveryMap(f: Fixture, names: Seq[String]): Unit = {
    val Seq(b, m, mon) = names.map(user)
    val (cb, eb) = config(b, metrics = false, monitoring = false)
    val (cm, em) = config(m, metrics = true, monitoring = false)
    val (cmon, emon) = config(mon)
    f.cache.base(cb, eb).close()
    f.cache.metrics(cm, em).close()
    f.cache.monitored(cmon, emon).close()
    f.cache.stats shouldBe counts(3, 1, 1)
    ()
  }

  "ElasticClientCache's sweep" should "close each idle entry exactly once while the first acquisitions after it are monitored ones" in using(
    new Fixture
  ) { f =>
    idleInEveryMap(f, Seq("idle-b", "idle-m", "idle-mon"))
    val idleRaws = f.rawList
    f.advance(PastTtl + ElasticClientCache.SweepInterval)
    val fresh = Seq("fresh-1", "fresh-2", "fresh-3").map(n => config(user(n)))
    val (clients, events) = ClientLogCapture.capture {
      concurrently(8) { i =>
        val (c, e) = fresh(i % 3)
        f.cache.monitored(c, e)
      }
    }
    idleRaws.foreach(raw => verify(raw, times(1)).close())
    shutdowns(events) shouldBe 1
    f.builds.get shouldBe 6
    clients.foreach(client => verify(rawOf(client), never).close())
    f.cache.stats shouldBe counts(3, 0, 3)
  }

  it should "never hand out a closed client when the acquisitions race the eviction of their own keys" in using(
    new Fixture
  ) { f =>
    val idle = Seq("race-1", "race-2", "race-3").map(n => config(user(n)))
    idle.foreach { case (c, e) => f.cache.monitored(c, e).close() }
    f.advance(PastTtl + ElasticClientCache.SweepInterval)
    val clients = concurrently(8) { i =>
      val (c, e) = idle(i % 3)
      f.cache.monitored(c, e)
    }
    clients.foreach { client =>
      verify(rawOf(client), never).close()
      MonitoredClientHooks.removed(client) shouldBe false
    }
    // each raw client is closed at most once
    f.rawList.foreach(raw => verify(raw, atMost(1)).close())
  }

  // ==================== P4 / AC6: lifecycle ====================

  "ElasticClientCache's monitoring system" should "be ONE ActorSystem for every monitored client, across evictions" in using(
    new Fixture
  ) { f =>
    val identities = Seq("sys-1", "sys-2", "sys-3").map(user)
    val systems = (1 to 2).flatMap { _ =>
      val cycle = identities.map { identity =>
        val (c, e) = config(identity)
        val client = f.cache.monitored(c, e).asInstanceOf[SharedMonitoredClient]
        client.close()
        client.actorSystem
      }
      f.advance(PastTtl)
      cycle
    }
    val (c, e) = config(user("sys-custom"))
    val custom = f.cache.customMonitored(c, e, new MetricsCollector())
    val customSystem = custom.asInstanceOf[LeasedMonitoredClient].actorSystem
    custom.close()
    // every monitored client was evicted and re-created once
    f.builds.get shouldBe 7
    (systems :+ customSystem).foreach(_ shouldBe theSameInstanceAs(f.cache.monitoringSystem()))
  }

  it should "be terminated by shutdown() and re-created by the next monitored client" in using(
    new Fixture
  ) { f =>
    val (c, e) = config(admin)
    val first = f.cache.monitored(c, e).asInstanceOf[SharedMonitoredClient].actorSystem
    f.cache.shutdown()
    Await.result(first.whenTerminated, 30.seconds)
    val next = f.cache.monitored(c, e).asInstanceOf[SharedMonitoredClient]
    next.actorSystem should not be theSameInstanceAs(first)
    next.actorSystem.whenTerminated.isCompleted shouldBe false
  }

  "An evicted monitored client" should "have its JVM shutdown hook removed" in using(new Fixture) {
    f =>
      val (c, e) = config(admin)
      val client = f.cache.monitored(c, e)
      client.close()
      MonitoredClientHooks.removed(client) shouldBe false
      f.advance(PastTtl)
      val (co, eo) = config(reader, metrics = false, monitoring = false)
      f.cache.base(co, eo) // any acquisition sweeps
      MonitoredClientHooks.removed(client) shouldBe true
  }

  "ElasticClientCache.shutdown()" should "close every real client exactly once" in using(
    new Fixture
  ) { f =>
    entryPoints.foreach { ep =>
      Seq(admin, reader).foreach { identity =>
        val (c, e) = config(identity, ep.metrics, ep.monitoring)
        ep.call(f.cache, c, e)
      }
    }
    val systemBefore = f.cache.monitoringSystem()
    f.cache.shutdown()
    f.cache.shutdown()
    f.rawList should not be empty
    f.rawList.foreach(raw => verify(raw, times(1)).close())
    f.cache.stats.values.sum shouldBe 0
    Await.result(systemBefore.whenTerminated, 30.seconds)
  }

  // ==================== P3 / AC3: no credential anywhere ====================

  private def b64(s: String): String =
    Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))

  private def chain(t: Throwable): String =
    Iterator
      .iterate(t)(_.getCause)
      .takeWhile(_ != null)
      .map(x => s"${x.getClass.getName}: ${x.getMessage}")
      .mkString("\n")

  "The client cache" should "never log, report or raise a credential, through any entry point" in {
    val salt = UUID.randomUUID().toString
    val password = s"pw-S14-$salt"
    val rawApiKey = s"id-S14:key-S14-$salt"
    val bearer = s"tok-S14-$salt"
    val secrets = Seq(password, rawApiKey, b64(rawApiKey), bearer, b64(s"user-S14:$password"))
    val identities = Seq(
      basic("user-S14", password),
      Identity("ApiKeyAuth", s"""api-key = "$rawApiKey"""", Map("api-key" -> rawApiKey)),
      Identity("BearerTokenAuth", s"""bearer-token = "$bearer"""", Map("bearer-token" -> bearer))
    )
    for {
      ep       <- entryPoints
      identity <- identities
    } {
      val (c, e) = config(identity, ep.metrics, ep.monitoring)
      val (texts, events) = ClientLogCapture.capture {
        val f = new Fixture
        val created = ep.call(f.cache, c, e)
        created.close()
        f.advance(PastTtl)
        val again = ep.call(f.cache, c, e) // an eviction, then a creation
        val stats = f.cache.stats.toString
        val key = ClientKey.of(e).toString
        f.cache.shutdown()
        again.close()
        val failing = new Fixture(failBuild = true)
        val failure = Try(ep.call(failing.cache, c, e)).failed.map(chain).getOrElse("")
        failing.cache.shutdown()
        Seq(stats, key, failure)
      }
      withClue(s"${ep.name} / ${identity.kind}: the creation line is captured (canary): ") {
        events.exists { event =>
          event.message.startsWith("Creating new") &&
          event.message.contains("http://localhost:9200") &&
          event.message.contains(s"(auth: ${identity.kind})")
        } shouldBe true
        creations(events, "Closing idle") should be > 0
        texts.last should include("the provider failed to build a client")
      }
      val everything = events.map(_.text) ++ texts
      for {
        secret <- secrets
        text   <- everything
      } withClue(s"${ep.name} / ${identity.kind}: ")(text should not include secret)
    }
  }
}

object ElasticClientCacheSpec {

  /** An identity, set on `credentials` and `watcher` alike (as the JDBC driver does). */
  final case class Identity(kind: String, hocon: String, map: Map[String, Any])

  /** A factory entry point, as the cache path it delegates to. `stats` = main's `getCacheStats` for
    * one identity on this path.
    */
  final case class EntryPoint(
    name: String,
    metrics: Boolean,
    monitoring: Boolean,
    cached: Boolean,
    monitored: Boolean,
    ownMonitor: Boolean,
    stats: Map[String, Int],
    call: (ElasticClientCache, Config, ElasticConfig) => ElasticClientApi
  )
}
