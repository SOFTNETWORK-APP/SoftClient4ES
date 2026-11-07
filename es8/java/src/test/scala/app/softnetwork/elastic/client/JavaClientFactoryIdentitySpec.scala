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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.metrics.MetricsCollector
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.{ClientLogCapture, ElasticClientFactory}
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.http.{Header, HttpHost}
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Request, ResponseException, RestClient}
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.nio.charset.StandardCharsets
import _root_.java.util.Base64
import scala.annotation.tailrec
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

/** SCHEMA-14 (AC1, AC4; P5) -- ES 8 with security on: two identities on one URL in one JVM get two
  * clients, each authenticated as itself, through every entry point of `ElasticClientFactory` and
  * in both creation orders; and one caller's `close()` never closes another caller's client.
  *
  * Oracles per client: `version` (`GET /` -- 403 for a read-only identity), a `LIMIT`ed SELECT on
  * an index the read-only role cannot read (403) and one on an index it can (answered). The admin
  * is answered on all three; a wrong password is refused with 401 everywhere. On a factory keyed by
  * URL alone, the second client answers exactly as the first (the control, measured on main).
  *
  * The consumer-shaped cells build each configuration as the JDBC driver (`ConnectionUrl.toConfig`)
  * and the ADBC driver (`ElasticAdbcDriver.buildConfig`) build theirs, and close the client as they
  * do.
  */
class JavaClientFactoryIdentitySpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  import JavaClientFactoryIdentitySpec._

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  override lazy val xpackSecurityEnabled: Boolean = true

  private val AdminPassword = "changeme"

  private def basicHeader(user: String, password: String): String =
    "Basic " + Base64.getEncoder.encodeToString(s"$user:$password".getBytes(StandardCharsets.UTF_8))

  override lazy val restClient: RestClient =
    RestClient
      .builder(HttpHost.create(elasticURL))
      .setDefaultHeaders(
        Array[Header](new BasicHeader("Authorization", basicHeader("elastic", AdminPassword)))
      )
      .build()

  // ==================== identities ====================

  private final class Identity(
    val name: String,
    entries: => Map[String, Any],
    val expected: Seq[Outcome]
  ) {
    def credentials: Map[String, Any] = entries
    def kind: String = if (credentials.contains("api-key")) "ApiKeyAuth" else "BasicAuth"
    override def toString: String = name
  }

  private val ReadOnlyRole = "s14_reader"
  private val OpenIndex = "s14_open"
  private val SecretIndex = "s14_secret"

  private val SecretSql = s"SELECT id FROM $SecretIndex LIMIT 10"
  private val OpenSql = s"SELECT id FROM $OpenIndex LIMIT 10"

  /** id:key of an API key restricted to the read-only role, created in beforeAll. */
  @volatile private var readerApiKey: String = _

  private val admin = new Identity(
    "admin",
    Map("username" -> "elastic", "password" -> AdminPassword),
    Seq(Answered(1), Answered(2), Answered(3))
  )
  private val readOnly = new Identity(
    "ro-basic",
    Map("username" -> "s14_ro", "password" -> "s14-ro-pass"),
    Seq(Refused(403), Refused(403), Answered(3))
  )
  private val readOnlyApiKey = new Identity(
    "ro-apikey",
    Map("api-key" -> readerApiKey),
    Seq(Refused(403), Refused(403), Answered(3))
  )
  private val wrongPassword = new Identity(
    "admin-wrong-password",
    Map("username" -> "elastic", "password" -> "not-the-password"),
    Seq(Refused(401), Refused(401), Refused(401))
  )

  private lazy val url = s"http://localhost:$elasticPort"

  // ==================== configurations ====================

  private def hocon(value: Any): String = value match {
    case s: String => "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
    case other     => other.toString
  }

  /** A plain configuration, as an application writes one. */
  private def plainConfig(identity: Identity, metrics: Boolean, monitoring: Boolean): Config = {
    val credentials = (Map[String, Any]("port" -> elasticPort) ++ identity.credentials)
      .map { case (k, v) => s"$k = ${hocon(v)}" }
      .mkString(", ")
    ConfigFactory
      .parseString(
        s"""elastic {
           |  credentials { $credentials }
           |  watcher     { $credentials }
           |  multithreaded = false
           |  metrics { enabled = $metrics, monitoring { enabled = $monitoring } }
           |}""".stripMargin
      )
      .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
  }

  /** The JDBC driver's form (`ConnectionUrl.toConfig`): overlays, then the URL's parameters for
    * `credentials` and `watcher` alike, then `softnetwork-elastic.conf`, then `load()`.
    */
  private def jdbcConfig(identity: Identity, metricsOff: Boolean = false): Config = {
    val map = new _root_.java.util.HashMap[String, Any]()
    Seq("credentials", "watcher").foreach { side =>
      map.put(s"elastic.$side.scheme", "http")
      map.put(s"elastic.$side.host", "localhost")
      map.put(s"elastic.$side.port", elasticPort)
      identity.credentials.foreach { case (k, v) => map.put(s"elastic.$side.$k", v) }
    }
    val metricsOverlay =
      if (metricsOff) ConfigFactory.parseString("elastic.metrics.enabled = false")
      else ConfigFactory.empty()
    ConfigFactory
      .parseString("akka.jvm-exit-on-fatal-error = off")
      .withFallback(ConfigFactory.parseString("softclient4es.license.refresh.enabled = false"))
      .withFallback(metricsOverlay)
      .withFallback(ConfigFactory.parseMap(map))
      .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
      .withFallback(ConfigFactory.load())
  }

  /** The ADBC driver's form (`ElasticAdbcDriver.buildConfig`): an explicit method, no watcher. */
  private def adbcConfig(identity: Identity): Config = {
    val map = new _root_.java.util.HashMap[String, Any]()
    map.put("elastic.credentials.scheme", "http")
    map.put("elastic.credentials.host", "localhost")
    map.put("elastic.credentials.port", elasticPort)
    map.put(
      "elastic.credentials.method",
      if (identity.kind == "ApiKeyAuth") "apikey" else "basic"
    )
    identity.credentials.foreach { case (k, v) => map.put(s"elastic.credentials.$k", v) }
    ConfigFactory
      .parseMap(map)
      .withFallback(ConfigFactory.load("softnetwork-elastic"))
      .withFallback(ConfigFactory.load())
  }

  // ==================== the factory's entry points ====================

  private val entryPoints = Seq(
    EntryPoint(
      "create (metrics off)",
      metrics = false,
      monitoring = false,
      ElasticClientFactory.create
    ),
    EntryPoint("create (default)", metrics = true, monitoring = true, ElasticClientFactory.create),
    EntryPoint(
      "createWithMetrics",
      metrics = true,
      monitoring = false,
      ElasticClientFactory.createWithMetrics
    ),
    EntryPoint(
      "createWithMonitoring",
      metrics = true,
      monitoring = true,
      ElasticClientFactory.createWithMonitoring
    ),
    EntryPoint(
      "createWithCustomMetrics",
      metrics = true,
      monitoring = true,
      c => ElasticClientFactory.createWithCustomMetrics(c, new MetricsCollector())
    ),
    EntryPoint(
      "createMonitoredWithCustomMetrics",
      metrics = true,
      monitoring = true,
      c => ElasticClientFactory.createMonitoredWithCustomMetrics(c, new MetricsCollector())
    )
  )

  // ==================== oracles ====================

  /** The HTTP status Elasticsearch answered: the error's own status when it carries one, else the
    * status its security reason belongs to (a failure surfaced through a stream carries none).
    */
  private def statusIn(message: String): Option[Int] =
    if (message.contains("unable to authenticate")) Some(401)
    else if (message.contains("is unauthorized for")) Some(403)
    else None

  private def refusal(status: Option[Int], message: String): Outcome =
    status.orElse(statusIn(message)).map(Refused).getOrElse(Unexpected(message.take(300)))

  private def refusal(t: Throwable): Outcome = {
    val messages = Iterator
      .iterate(t)(_.getCause)
      .takeWhile(_ != null)
      .map(x => String.valueOf(x.getMessage))
      .mkString(" | ")
    refusal(None, messages)
  }

  private def rowsOf(result: QueryResult): Int = result match {
    case q: QueryRows       => q.rows.size
    case q: QueryStructured => q.response.results.size
    case q: QueryStream     => Await.result(q.stream.runWith(Sink.seq), 60.seconds).size
    case other              => throw new IllegalStateException(s"unexpected result: $other")
  }

  private def versionOf(client: ElasticClientApi): Outcome =
    Try(client.version) match {
      case Success(ElasticSuccess(_)) => Answered(1)
      case Success(ElasticFailure(e)) => refusal(e.statusCode, e.message)
      case Failure(t)                 => refusal(t)
    }

  private def sqlOf(client: ElasticClientApi, sql: String): Outcome =
    Try(Await.result(client.run(sql), 60.seconds)) match {
      case Success(ElasticSuccess(result)) =>
        Try(rowsOf(result)) match {
          case Success(rows) => Answered(rows)
          case Failure(t)    => refusal(t)
        }
      case Success(ElasticFailure(e)) => refusal(e.statusCode, e.message)
      case Failure(t)                 => refusal(t)
    }

  private def outcomes(client: ElasticClientApi): Seq[Outcome] =
    Seq(versionOf(client), sqlOf(client, SecretSql), sqlOf(client, OpenSql))

  @tailrec
  private def rawOf(client: ElasticClientApi): ElasticClientApi = client match {
    case d: ElasticClientDelegator => rawOf(d.delegate)
    case other                     => other
  }

  private def creationLines(events: Seq[ClientLogCapture.Event]): Int =
    events.count(_.message.startsWith(s"Creating new Elasticsearch client for URL: $url "))

  // ==================== set-up ====================

  private def http(method: String, path: String, body: Option[String] = None): (Int, String) = {
    val request = new Request(method, path)
    body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
    Try {
      val response = restClient.performRequest(request)
      (
        response.getStatusLine.getStatusCode,
        Option(response.getEntity).map(EntityUtils.toString).getOrElse("")
      )
    }.recover { case e: ResponseException =>
      (
        e.getResponse.getStatusLine.getStatusCode,
        Option(e.getResponse.getEntity).map(EntityUtils.toString).getOrElse("")
      )
    }.get
  }

  private def ok(call: (Int, String)): Unit =
    withClue(s"set-up call answered ${call._1}: ${call._2.take(300)}: ")(call._1 should be < 300)

  override def beforeAll(): Unit = {
    super.beforeAll()
    blockUntil("licence active", 60, 1000) { () =>
      http("GET", "/_license")._2.contains("\"active\"")
    }
    Seq(OpenIndex -> 3, SecretIndex -> 2).foreach { case (index, docs) =>
      ok(
        http(
          "PUT",
          s"/$index",
          Some("""{"mappings": {"properties": {"id": {"type": "keyword"}}}}""")
        )
      )
      (1 to docs).foreach { i =>
        ok(http("PUT", s"/$index/_doc/$index-$i?refresh=true", Some(s"""{"id": "$index-$i"}""")))
      }
    }
    val privileges = s"""{"names": ["$OpenIndex*"], "privileges": ["read"]}"""
    ok(http("PUT", s"/_security/role/$ReadOnlyRole", Some(s"""{"indices": [$privileges]}""")))
    ok(
      http(
        "PUT",
        "/_security/user/s14_ro",
        Some(s"""{"password": "s14-ro-pass", "roles": ["$ReadOnlyRole"]}""")
      )
    )
    val (status, body) = http(
      "POST",
      "/_security/api_key",
      Some(
        s"""{"name": "s14-reader-key", "role_descriptors": {"$ReadOnlyRole": {"indices": [$privileges]}}}"""
      )
    )
    ok((status, body))
    val json = parse(body)
    readerApiKey = s"${(json \ "id").extract[String]}:${(json \ "api_key").extract[String]}"
  }

  override def afterAll(): Unit = {
    Try(ElasticClientFactory.shutdown())
    super.afterAll()
  }

  // ==================== AC1: two identities, two clients ====================

  private val pairs = Seq(admin -> readOnly, admin -> readOnlyApiKey, admin -> wrongPassword)

  for {
    ep              <- entryPoints
    (left, right)   <- pairs
    (first, second) <- Seq(left -> right, right -> left)
  } {
    s"ElasticClientFactory.${ep.name}" should s"give $first then $second each its own client" in {
      ElasticClientFactory.shutdown()
      val a = ep.create(plainConfig(first, ep.metrics, ep.monitoring))
      val b = ep.create(plainConfig(second, ep.metrics, ep.monitoring))
      try {
        rawOf(a) should not be theSameInstanceAs(rawOf(b))
        withClue(s"$first (created first): ")(outcomes(a) shouldBe first.expected)
        withClue(s"$second (created second): ")(outcomes(b) shouldBe second.expected)
      } finally {
        a.close()
        b.close()
      }
    }
  }

  // ==================== AC1 / AC4: the JDBC driver's form ====================

  for ((first, second) <- Seq(admin -> readOnly, readOnly -> admin)) {
    "Two JDBC connections of two users" should s"each run as itself and survive the other's close ($first first)" in {
      ElasticClientFactory.shutdown()
      val (_, events) = ClientLogCapture.capture {
        val a = ElasticClientFactory.create(jdbcConfig(first))
        val b = ElasticClientFactory.create(jdbcConfig(second))
        withClue(s"$first: ")(outcomes(a) shouldBe first.expected)
        withClue(s"$second: ")(outcomes(b) shouldBe second.expected)
        a.close()
        withClue(s"$second after $first closed: ")(outcomes(b) shouldBe second.expected)
        // A re-opened three times in sequence
        (1 to 3).foreach { _ =>
          val again = ElasticClientFactory.create(jdbcConfig(first))
          withClue(s"$first re-opened: ")(sqlOf(again, OpenSql) shouldBe first.expected.last)
          again.close()
        }
        b.close()
      }
      // one client per identity, none per connection
      creationLines(events) shouldBe 2
    }
  }

  "Two JDBC connections of one user (default: monitored)" should "never stop each other's monitor" in {
    ElasticClientFactory.shutdown()
    val (_, events) = ClientLogCapture.capture {
      val a = ElasticClientFactory.create(jdbcConfig(admin))
      val b = ElasticClientFactory.create(jdbcConfig(admin))
      sqlOf(b, OpenSql) shouldBe Answered(3)
      a.close()
      sqlOf(b, OpenSql) shouldBe Answered(3)
      b.close()
    }
    creationLines(events) shouldBe 1
    events.count(_.message.contains("Shutting down MonitoredElasticClient")) shouldBe 0
  }

  "Two JDBC connections of one user (metrics off)" should "never close each other's transport" in {
    ElasticClientFactory.shutdown()
    val (_, events) = ClientLogCapture.capture {
      val a = ElasticClientFactory.create(jdbcConfig(admin, metricsOff = true))
      val b = ElasticClientFactory.create(jdbcConfig(admin, metricsOff = true))
      val transport: ElasticClientCompanion[_] = rawOf(b) match {
        case companion: ElasticClientCompanion[_] => companion
        case other => fail(s"expected the raw client to own its transport, got $other")
      }
      transport.apply()
      transport.isInitialized shouldBe true
      a.close()
      transport.isInitialized shouldBe true
      sqlOf(b, OpenSql) shouldBe Answered(3)
      b.close()
    }
    events.count(_.message.startsWith(s"Elasticsearch Client initialized for $url")) shouldBe 1
  }

  // ==================== AC1 / AC4: the ADBC driver's form ====================

  for ((first, second) <- Seq(admin -> readOnlyApiKey, readOnlyApiKey -> admin)) {
    "Two ADBC databases of two users" should s"each run as itself and survive the other's close ($first first)" in {
      ElasticClientFactory.shutdown()
      val a = ElasticClientFactory.createWithMonitoring(adbcConfig(first))
      val b = ElasticClientFactory.createWithMonitoring(adbcConfig(second))
      withClue(s"$first: ")(outcomes(a) shouldBe first.expected)
      withClue(s"$second: ")(outcomes(b) shouldBe second.expected)
      a.asInstanceOf[AutoCloseable].close()
      withClue(s"$second after $first closed: ")(outcomes(b) shouldBe second.expected)
      b.asInstanceOf[AutoCloseable].close()
    }
  }
}

object JavaClientFactoryIdentitySpec {

  sealed trait Outcome
  final case class Answered(rows: Int) extends Outcome
  final case class Refused(status: Int) extends Outcome
  final case class Unexpected(detail: String) extends Outcome

  final case class EntryPoint(
    name: String,
    metrics: Boolean,
    monitoring: Boolean,
    create: Config => ElasticClientApi
  )
}
