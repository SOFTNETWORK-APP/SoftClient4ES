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

import app.softnetwork.elastic.client.{
  DiscoveryConfig,
  ElasticConfig,
  ElasticCredentials,
  SchemaCacheSettings,
  ScrollSettings
}
import app.softnetwork.elastic.client.metrics.MetricsConfig
import app.softnetwork.elastic.client.monitoring.MonitoringConfig
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.{Duration => JDuration}
import java.util.Base64
import scala.concurrent.duration._

/** SCHEMA-14 (AC7, P1) -- the client cache key: every leaf of `ElasticConfig` is in it, equal
  * configurations give equal keys however their `Config` was assembled, no two different trees
  * collide, and it carries no secret.
  */
class ClientKeySpec extends AnyFlatSpec with Matchers {

  private val credentials = ElasticCredentials("http", "es.example", 9201, None, "", "", None, None)

  /** Every field given explicitly: the population below changes one leaf at a time from here. */
  private val base: ElasticConfig = ElasticConfig(
    credentials = credentials,
    multithreaded = true,
    discovery = DiscoveryConfig(enabled = false, frequency = JDuration.ofMinutes(5)),
    connectionTimeout = JDuration.ofSeconds(5),
    socketTimeout = JDuration.ofSeconds(30),
    metrics = MetricsConfig(
      enabled = true,
      monitoring = MonitoringConfig(
        enabled = true,
        interval = 30.seconds,
        failureRateThreshold = 10.0,
        latencyThreshold = 1000.0
      )
    ),
    watcher = credentials,
    includeDocumentId = false,
    scroll = ScrollSettings(size = 1000, maxSlices = 8),
    schemaCache = SchemaCacheSettings(ttl = JDuration.ofMinutes(5))
  )

  private def credentialChanges(
    name: String,
    get: ElasticConfig => ElasticCredentials,
    set: (ElasticConfig, ElasticCredentials) => ElasticConfig
  ): Seq[(String, ElasticConfig => ElasticConfig)] =
    Seq[(String, ElasticCredentials => ElasticCredentials)](
      "scheme"      -> (_.copy(scheme = "https")),
      "host"        -> (_.copy(host = "es2.example")),
      "port"        -> (_.copy(port = 9202)),
      "method"      -> (_.copy(method = Some("basic"))),
      "username"    -> (_.copy(username = "someone")),
      "password"    -> (_.copy(password = "secret")),
      "apiKey"      -> (_.copy(apiKey = Some("id:key"))),
      "bearerToken" -> (_.copy(bearerToken = Some("token")))
    ).map { case (field, change) =>
      s"$name.$field" -> ((c: ElasticConfig) => set(c, change(get(c))))
    }

  /** One change per leaf of the tree (P1). */
  private val leafChanges: Seq[(String, ElasticConfig => ElasticConfig)] =
    credentialChanges("credentials", _.credentials, (c, v) => c.copy(credentials = v)) ++
    credentialChanges("watcher", _.watcher, (c, v) => c.copy(watcher = v)) ++
    Seq[(String, ElasticConfig => ElasticConfig)](
      "multithreaded"     -> (c => c.copy(multithreaded = false)),
      "discovery.enabled" -> (c => c.copy(discovery = c.discovery.copy(enabled = true))),
      "discovery.frequency" -> (c =>
        c.copy(discovery = c.discovery.copy(frequency = JDuration.ofMinutes(6)))
      ),
      "connectionTimeout" -> (c => c.copy(connectionTimeout = JDuration.ofSeconds(6))),
      "socketTimeout"     -> (c => c.copy(socketTimeout = JDuration.ofSeconds(31))),
      "metrics.enabled"   -> (c => c.copy(metrics = c.metrics.copy(enabled = false))),
      "metrics.monitoring.enabled" -> (c =>
        c.copy(metrics = c.metrics.copy(monitoring = c.metrics.monitoring.copy(enabled = false)))
      ),
      "metrics.monitoring.interval" -> (c =>
        c.copy(metrics =
          c.metrics.copy(monitoring = c.metrics.monitoring.copy(interval = 31.seconds))
        )
      ),
      "metrics.monitoring.failureRateThreshold" -> (c =>
        c.copy(metrics =
          c.metrics.copy(monitoring = c.metrics.monitoring.copy(failureRateThreshold = 11.0))
        )
      ),
      "metrics.monitoring.latencyThreshold" -> (c =>
        c.copy(metrics =
          c.metrics.copy(monitoring = c.metrics.monitoring.copy(latencyThreshold = 1001.0))
        )
      ),
      "includeDocumentId" -> (c => c.copy(includeDocumentId = true)),
      "scroll.size"       -> (c => c.copy(scroll = c.scroll.copy(size = 500))),
      "scroll.maxSlices"  -> (c => c.copy(scroll = c.scroll.copy(maxSlices = 9))),
      "schemaCache.ttl" -> (c =>
        c.copy(schemaCache = SchemaCacheSettings(ttl = JDuration.ofMinutes(6)))
      )
    )

  /** The leaves of a configuration tree, counted generically: an Option is ONE leaf. */
  private def leaves(value: Any): Int = value match {
    case _: Option[_]                     => 1
    case p: Product if p.productArity > 0 => p.productIterator.map(leaves).sum
    case _                                => 1
  }

  behavior of "ClientKey"

  it should "cover every field of ElasticConfig and ElasticCredentials (arity guard)" in {
    withClue(
      "ElasticConfig or ElasticCredentials gained a field: add its leaves to leafChanges, the " +
      "key already includes it (generic walk) -- "
    ) {
      base.productArity shouldBe 10
      base.credentials.productArity shouldBe 8
    }
    // the population is derived from the tree, not from a hand-kept list of names
    leaves(base) shouldBe 30
    leafChanges.map(_._1).distinct should have size leaves(base).toLong
  }

  it should "give a distinct key when any single leaf changes" in {
    val keys = (("base", base) +: leafChanges.map { case (name, change) =>
      val changed = change(base)
      withClue(s"the change of $name must change the configuration: ")(changed should not be base)
      name -> changed
    }).map { case (name, config) => name -> ClientKey.of(config) }

    keys.foreach { case (name, key) =>
      keys.filter(_._1 != name).foreach { case (other, otherKey) =>
        withClue(s"$name vs $other: ")(key should not be otherKey)
      }
    }
  }

  it should "give equal keys to equal configurations, however their Config was assembled" in {
    val hocon =
      """elastic {
        |  credentials { scheme = "http", host = "es.example", port = 9201, username = "u", password = "p" }
        |  watcher     { scheme = "http", host = "es.example", port = 9201, username = "u", password = "p" }
        |}""".stripMargin

    val viaString = ElasticConfig(ConfigFactory.parseString(hocon))

    // the JDBC driver's form (ConnectionUrl.toConfig): overlays, a parsed map, then the defaults
    val map = new java.util.HashMap[String, Any]()
    Seq("credentials", "watcher").foreach { side =>
      map.put(s"elastic.$side.scheme", "http")
      map.put(s"elastic.$side.host", "es.example")
      map.put(s"elastic.$side.port", 9201)
      map.put(s"elastic.$side.username", "u")
      map.put(s"elastic.$side.password", "p")
    }
    val jdbcShape: Config =
      ConfigFactory
        .parseString("akka.jvm-exit-on-fatal-error = off")
        .withFallback(ConfigFactory.parseString("softclient4es.license.refresh.enabled = false"))
        .withFallback(ConfigFactory.parseMap(map))
        .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
        .withFallback(ConfigFactory.load())
    val viaJdbc = ElasticConfig(jdbcShape)

    // unrelated keys, in a different fallback order
    val viaUnrelatedFirst = ElasticConfig(
      ConfigFactory.parseString("unrelated.a = 1").withFallback(ConfigFactory.parseString(hocon))
    )
    val viaUnrelatedLast = ElasticConfig(
      ConfigFactory
        .parseString(hocon)
        .withFallback(ConfigFactory.parseString("unrelated.b = 2\nunrelated.a = 3"))
    )

    Seq(viaJdbc, viaUnrelatedFirst, viaUnrelatedLast).foreach { config =>
      config shouldBe viaString
      ClientKey.of(config) shouldBe ClientKey.of(viaString)
      ClientKey.of(config).hashCode shouldBe ClientKey.of(viaString).hashCode
    }
    ClientKey.of(viaString) shouldBe ClientKey.of(viaString)
  }

  it should "not let two different trees collide (unambiguous encoding)" in {
    def creds(username: String, password: String): ElasticConfig =
      base.copy(credentials = base.credentials.copy(username = username, password = password))

    val (abC, aBc) = (creds("a,b", "c"), creds("a", "b,c"))
    // control: a digest of toString would give these two identities ONE client
    abC.credentials.toString shouldBe aBc.credentials.toString
    ClientKey.of(abC) should not be ClientKey.of(aBc)

    val (abEmpty, aB) = (creds("ab", ""), creds("a", "b"))
    // control: a plain concatenation would too
    (abEmpty.credentials.username + abEmpty.credentials.password) shouldBe
    (aB.credentials.username + aB.credentials.password)
    ClientKey.of(abEmpty) should not be ClientKey.of(aB)

    // harmless: one extra client for an empty API key
    val emptyApiKey = base.copy(credentials = base.credentials.copy(apiKey = Some("")))
    ClientKey.of(emptyApiKey) should not be ClientKey.of(base)

    // a type tag before every value
    ClientKey.canonical("1") should not equal ClientKey.canonical(1)
    ClientKey.canonical(Some("x")) should not equal ClientKey.canonical("x")
  }

  it should "carry no secret, in toString or anywhere a caller can read" in {
    val apiKey = "id-S14:key-S14-secret"
    val secretConfig = base.copy(credentials =
      base.credentials.copy(
        username = "user-S14",
        password = "pw-S14-secret",
        apiKey = Some(apiKey),
        bearerToken = Some("tok-S14-secret")
      )
    )
    val key = ClientKey.of(secretConfig)
    key.toString shouldBe "ClientKey(http://es.example:9201)"
    key.url shouldBe "http://es.example:9201"
    val b64 = (s: String) => Base64.getEncoder.encodeToString(s.getBytes(StandardCharsets.UTF_8))
    Seq(
      "pw-S14-secret",
      apiKey,
      b64(apiKey),
      "tok-S14-secret",
      b64("user-S14:pw-S14-secret")
    ).foreach(secret => key.toString should not include secret)
  }

  it should "refuse a leaf type it cannot encode as its value, naming the type and never the value" in {
    val failure = intercept[IllegalArgumentException] {
      ClientKey.canonical(ClientKeySpec.WithMap(Map("password" -> "pw-S14-secret")))
    }
    failure.getMessage should include("scala.collection.immutable")
    failure.getMessage should not include "pw-S14-secret"
    an[IllegalArgumentException] should be thrownBy ClientKey.canonical(Array[Byte](1, 2))
  }
}

object ClientKeySpec {

  /** A configuration node whose leaf is not a value type. */
  final case class WithMap(settings: Map[String, String])
}
