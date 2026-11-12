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
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.{ElasticClientFactory, JavaClientSpi}
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import com.typesafe.config.ConfigFactory
import org.apache.http.{Header, HttpHost}
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Request, RestClient}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import _root_.java.util.Base64
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-7 (AC3, P3; D7, D8, D9, A-40, A-41, A-63, A-65, A-67; R-57, R-85) -- Elasticsearch 8 with
  * security on: a user holding only the index `read` privilege (`GET /`, `GET <index>`, `_mapping`,
  * `_settings` answer 403; `_field_caps`, `_search`, `_count`, PIT answer 200), against the
  * privileged client, on one container.
  *
  *   - the read-only user is typed from `_field_caps`, and every P3 statement answers what the
  *     privileged client answers, but for D8's residuals (A-67): `constant_keyword` reads as a
  *     keyword (its projection and `*` read NULL, as main), a runtime field widens `*` with a NULL
  *     column, a several-valued cell is read as its list (A-41);
  *   - its un-LIMITed `SELECT`, pattern and UNION ALL answer every row (`_count`), paged by the
  *     build major (A-63): main failed them ("Failed to get ES version").
  *
  * The read-only client is built from the SPI directly: `ElasticClientFactory` caches clients by
  * URL, whatever their credentials (R-86).
  */
class JavaClientReadOnlyUserSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  override lazy val xpackSecurityEnabled: Boolean = true

  private val admin = ("elastic", "changeme")
  private val reader = ("s7_ro", "s7-ro-pass")

  private def basic(user: (String, String)): String =
    "Basic " + Base64.getEncoder.encodeToString(s"${user._1}:${user._2}".getBytes("UTF-8"))

  override lazy val restClient: RestClient =
    RestClient
      .builder(HttpHost.create(elasticURL))
      .setDefaultHeaders(Array[Header](new BasicHeader("Authorization", basic(admin))))
      .build()

  private def configFor(user: (String, String)): String =
    s"""
       |elastic {
       |  credentials {
       |    port = $elasticPort
       |    username = "${user._1}"
       |    password = "${user._2}"
       |  }
       |  watcher {
       |    port = $elasticPort
       |  }
       |  multithreaded     = false
       |  discovery-enabled = false
       |}
       |""".stripMargin

  override lazy val elasticConfigAsString: String = configFor(admin)

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  lazy val roClient: ElasticClientApi = new JavaClientSpi().client(
    ConfigFactory
      .parseString(configFor(reader))
      .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))
  )

  private val table = "s7_t"
  private val big = "s7_big"
  private val listed = "s7_tags"
  private val indices = Seq(table, big, listed, "s7_p1", "s7_p2")

  private val mapping: String =
    """{"runtime": {"rt": {"type": "keyword", "script": "emit('r')"}},
      | "properties": {
      |  "id": {"type": "keyword"}, "k": {"type": "keyword"}, "d": {"type": "date"},
      |  "n": {"type": "long"}, "x": {"type": "double"},
      |  "sf": {"type": "scaled_float", "scaling_factor": 100}, "hf": {"type": "half_float"},
      |  "ip": {"type": "ip"}, "u": {"type": "unsigned_long"}, "dn": {"type": "date_nanos"},
      |  "ck": {"type": "constant_keyword", "value": "C"}
      |}}""".stripMargin

  private val docs: Seq[(String, String)] = Seq(
    "r1" -> """{"id":"r1","k":"a","d":"2024-01-31","n":1,"x":1.5,"sf":1.25,"hf":0.5,"ip":"10.0.0.1","u":18446744073709551615,"dn":"2024-01-31T10:30:00.123456789Z"}""",
    "r2" -> """{"id":"r2","k":"b","d":"2024-02-01T10:00:00Z","n":2,"x":2.5,"sf":2.5,"hf":1.5,"ip":"10.0.0.2","u":7,"dn":"2024-02-01T00:00:00Z"}""",
    "r3" -> """{"id":"r3","k":"a","n":3}"""
  )

  private def raw(method: String, path: String, body: Option[String] = None): (Int, String) =
    Try {
      val request = new Request(method, path)
      body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
      val response = restClient.performRequest(request)
      (response.getStatusLine.getStatusCode, EntityUtils.toString(response.getEntity))
    }.recover { case e: org.elasticsearch.client.ResponseException =>
      (e.getResponse.getStatusLine.getStatusCode, EntityUtils.toString(e.getResponse.getEntity))
    }.get

  private def load(index: String, mappingJson: String, documents: Seq[(String, String)]): Unit = {
    raw(
      "PUT",
      s"/$index",
      Some(
        s"""{"settings":{"number_of_shards":2,"number_of_replicas":0},"mappings":$mappingJson}"""
      )
    )._1 shouldBe 200
    // raw: the core client's `index` wraps an `unsigned_long` above 2^63-1
    documents.foreach { case (id, source) =>
      raw("PUT", s"/$index/_doc/$id", Some(source))._1 should (be(200) or be(201))
    }
    raw("POST", s"/$index/_refresh")._1 shouldBe 200
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    blockUntil("licence active", 60, 1000) { () =>
      Try(raw("GET", "/_license")._2.contains("\"active\"")).getOrElse(false)
    }
    indices.foreach(index => Try(raw("DELETE", s"/$index")))
    load(table, mapping, docs)
    load(
      big,
      """{"properties": {"id": {"type":"keyword"}, "n": {"type":"long"}}}""",
      (1 to 60).map(i => s"b$i" -> s"""{"id":"b$i","n":$i}""")
    )
    load(
      listed,
      """{"properties": {"id": {"type":"keyword"}, "tags": {"type":"keyword"}}}""",
      Seq("t1" -> """{"id":"t1","tags":["a","b"]}""", "t2" -> """{"id":"t2","tags":"c"}""")
    )
    load("s7_p1", """{"properties": {"k": {"type":"keyword"}}}""", Seq("p1" -> """{"k":"007"}"""))
    load("s7_p2", """{"properties": {"k": {"type":"long"}}}""", Seq("p2" -> """{"k":7}"""))
    raw(
      "PUT",
      "/_security/role/s7_reader",
      Some("""{"indices":[{"names":["s7_*"],"privileges":["read"]}]}""")
    )._1 shouldBe 200
    raw(
      "PUT",
      "/_security/user/s7_ro",
      Some("""{"password":"s7-ro-pass","roles":["s7_reader"]}""")
    )._1 shouldBe 200
  }

  override def afterAll(): Unit = {
    indices.foreach(index => Try(raw("DELETE", s"/$index")))
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------

  private def answer(result: QueryResult): Seq[ListMap[String, Any]] =
    result match {
      case q: QueryRows       => q.rows
      case q: QueryStructured => q.response.results
      case q: QueryStream     => Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case other              => fail(s"unexpected result $other")
    }

  /** The statement's rows, or its failure's message -- a failed stream included. */
  private def outcome(
    on: ElasticClientApi,
    sql: String
  ): Either[String, Seq[ListMap[String, Any]]] =
    Try(Await.result(on.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => Right(answer(result))
      case ElasticFailure(error)  => Left(error.message)
    }).toEither.left
      .map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")
      .joinRight

  private def rows(on: ElasticClientApi, sql: String): Seq[ListMap[String, Any]] =
    outcome(on, sql).fold(error => fail(s"[$sql] failed: $error"), identity)

  /** The rows in a stable order, each as a map (the key order a consumer never reads). */
  private def sorted(rs: Seq[ListMap[String, Any]]): Seq[Map[String, Any]] =
    rs.map(_.toMap).sortBy(_.toSeq.map { case (k, v) => s"$k=$v" }.sorted.mkString(","))

  private def described(on: ElasticClientApi, sql: String): Map[String, String] =
    on.describe(sql) match {
      case ElasticSuccess(Some(schema)) => schema.columns.map(c => c.name -> c.sqlType.typeId).toMap
      case other                        => fail(s"describe [$sql]: $other")
    }

  private def count(index: String): Long = {
    val (_, body) = raw("GET", s"/$index/_count")
    "\"count\":(\\d+)".r.findFirstMatchIn(body).map(_.group(1).toLong).getOrElse(-1L)
  }

  // -------------------------------------------------------------------------------------------
  // The premises (T0-3): what the read-only user may and may not do
  // -------------------------------------------------------------------------------------------

  "The read-only user" should "be refused the mapping and the version, and allowed _field_caps" in {
    val ro = RestClient
      .builder(HttpHost.create(elasticURL))
      .setDefaultHeaders(Array[Header](new BasicHeader("Authorization", basic(reader))))
      .build()
    try {
      def status(method: String, path: String): Int =
        Try(ro.performRequest(new Request(method, path)).getStatusLine.getStatusCode).recover {
          case e: org.elasticsearch.client.ResponseException =>
            e.getResponse.getStatusLine.getStatusCode
        }.get
      status("GET", "/") shouldBe 403
      status("GET", s"/$table") shouldBe 403
      status("GET", s"/$table/_mapping") shouldBe 403
      status("GET", s"/$table/_field_caps?fields=*") shouldBe 200
      status("GET", s"/$table/_count") shouldBe 200
    } finally ro.close()
  }

  // -------------------------------------------------------------------------------------------
  // AC3 / P3 -- the read-only user against the privileged one
  // -------------------------------------------------------------------------------------------

  "A read-only user's schema" should "be typed from _field_caps: the privileged types, and D8's residuals" in {
    val ro = described(roClient, s"SELECT * FROM $table")
    val privileged = described(client, s"SELECT * FROM $table")
    // A-67 (2): a runtime field reads as a mapped keyword, a column the privileged client lacks
    (ro.keySet -- privileged.keySet) shouldBe Set("rt")
    ro("rt") shouldBe "VARCHAR"
    (ro - "rt") shouldBe privileged
    Seq("sf" -> "DOUBLE", "hf" -> "REAL", "u" -> "BIGINT", "dn" -> "TIMESTAMP", "ip" -> "VARCHAR")
      .foreach { case (c, t) => withClue(c)(ro(c) shouldBe t) }
    // A-67 (1): `constant_keyword` is a keyword to this user, typed VARCHAR (main: ANY)
    ro("ck") shouldBe "VARCHAR"
  }

  it should "answer every P3 statement as the privileged client does" in {
    Seq(
      s"SELECT id, k, sf, hf, ip, u, dn, d FROM $table LIMIT 10",
      s"SELECT id FROM $table WHERE ip = '10.0.0.1'",
      s"SELECT id FROM $table WHERE d > '2024-01-01'",
      s"SELECT id FROM $table WHERE ck = 'C'",
      s"SELECT k, COUNT(*) AS c FROM $table GROUP BY k",
      s"SELECT MIN(sf) AS mi, MAX(hf) AS ma, SUM(n) AS s FROM $table",
      s"SELECT id, k FROM $table UNION ALL SELECT id, k FROM $table",
      s"SELECT id, ROW_NUMBER() OVER (PARTITION BY k ORDER BY id) AS rn FROM $table",
      "SELECT k FROM s7_p*"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val ro = outcome(roClient, sql)
        ro.isRight shouldBe true
        ro.map(sorted) shouldBe outcome(client, sql).map(sorted)
      }
    }
    // an explicit path no mapping holds (the D10 reload): the same outcome, whatever it is
    val unmapped = s"SELECT id, nope FROM $table LIMIT 10"
    outcome(roClient, unmapped).map(sorted) shouldBe outcome(client, unmapped).map(sorted)
    // AC1 as the reader: the pattern field is a VARCHAR, both values `String`s
    described(roClient, "SELECT k FROM s7_p*") shouldBe Map("k" -> "VARCHAR")
    rows(roClient, "SELECT k FROM s7_p*").map(_("k")).toSet shouldBe Set("007", "7")
  }

  it should "differ under * by D8's residuals only: a NULL constant_keyword and runtime field (A-67)" in {
    val ro = rows(roClient, s"SELECT * FROM $table LIMIT 10")
    val privileged = rows(client, s"SELECT * FROM $table LIMIT 10")
    ro.map(_.get("ck").orNull).distinct shouldBe Seq(null)
    privileged.map(_.get("ck")).distinct shouldBe Seq(Some("C"))
    sorted(ro.map(r => r - "ck" - "rt")) shouldBe sorted(privileged.map(_ - "ck"))
    // the projection reads `_source` too: NULL, as main answered every user
    rows(roClient, s"SELECT id, ck FROM $table").map(_.get("ck").orNull).distinct shouldBe Seq(null)
  }

  it should "read a several-valued cell as its list, where the privileged client fails with Q1 (A-41)" in {
    val ro = rows(roClient, s"SELECT id, tags FROM $listed LIMIT 10")
      .map(r => r("id") -> r("tags"))
      .toMap
    ro("t1") shouldBe List("a", "b")
    outcome(client, s"SELECT id, tags FROM $listed LIMIT 10").left.getOrElse("") should include(
      "ARRAY<KEYWORD>"
    )
  }

  it should "be refused arithmetic over an ip or a constant_keyword as the privileged client is (A-65)" in {
    Seq(s"SELECT id, ip + 1 AS v FROM $table", s"SELECT id, ck + 1 AS v FROM $table").foreach {
      sql =>
        withClue(s"[$sql] ") {
          val ro = outcome(roClient, sql)
          ro.left.getOrElse("") should include("KEYWORD")
          ro shouldBe outcome(client, sql)
        }
    }
  }

  "A read-only user's JOIN body" should "be typed per table from _field_caps (SCHEMA-15)" in {
    val main = Seq("id", "k", "d", "sf", "hf", "ip", "u", "dn", "ck")
    val joined = Seq("n", "x")
    val body = app.softnetwork.elastic.sql.parser.Parser(
      s"SELECT ${main.map(c => s"a.$c").mkString(", ")}, ${joined.map(c => s"b.$c").mkString(", ")} " +
      s"FROM $table a JOIN $table b ON a.id = b.id"
    ) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"the JOIN body does not parse: ${error.msg}")
    }
    Seq("read-only" -> roClient, "privileged" -> client).foreach { case (who, on) =>
      withClue(s"[$who] ") {
        val single = on.describe(s"SELECT ${(main ++ joined).mkString(", ")} FROM $table")
        single.toOption.flatten should not be empty
        on.describePerTable(body) shouldBe single
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // A-63 (R-85) -- un-LIMITed reads page by the build major
  // -------------------------------------------------------------------------------------------

  "A read-only user's un-LIMITed statement" should "answer every row (main: Failed to get ES version)" in {
    Seq(
      s"SELECT * FROM $table"                                                  -> count(table),
      s"SELECT id FROM $big"                                                   -> count(big),
      "SELECT k FROM s7_p*"                                                    -> count("s7_p*"),
      s"SELECT id FROM $big UNION ALL SELECT id FROM $big"                     -> 2 * count(big),
      s"SELECT id, sf, dn FROM $table UNION ALL SELECT id, sf, dn FROM $table" -> 2 * count(table)
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ") {
        expected should be > 0L
        rows(roClient, sql).size.toLong shouldBe expected
      }
    }
    roClient.asInstanceOf[VersionApi].dateDialectVersion shouldBe Some("8")
    client.asInstanceOf[VersionApi].dateDialectVersion.map(_.takeWhile(_ != '.')) shouldBe Some("8")
  }
}
