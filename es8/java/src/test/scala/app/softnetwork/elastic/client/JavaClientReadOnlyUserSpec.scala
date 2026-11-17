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
  *     column, a several-valued cell of a leaf no index declares to it is read as its list (A-41,
  *     SCHEMA-17 fix R-150) -- one core declared fails as the privileged client's does (A-173), and
  *     several objects fail whatever the declaration (A-183);
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
  private val declared1 = "s7_f1"
  private val declared2 = "s7_f2"

  // SCHEMA-17 fix R-150 (A-182 population): `u` created outside core, `u2` its objects' list,
  // `c` created by core, `cp` core's with a field added by a plain mapping update, `m` created on
  // main, `n1` / `n2` outside core and `c1` / `c2` by core, for the several-index cells
  private val u = "s7_u"
  private val u2 = "s7_u2"
  private val c = "s7_c"
  private val cp = "s7_cp"
  private val m = "s7_m"
  private val n1 = "s7_n1"
  private val n2 = "s7_n2"
  private val c1 = "s7_c1"
  private val c2 = "s7_c2"
  private val r150 = Seq(u, u2, c, cp, m, n1, n2, c1, c2)

  private val indices =
    Seq(table, big, listed, "s7_p1", "s7_p2", declared1, declared2) ++ r150

  /** R-150 -- the leaf kinds (A-182 K), mapped outside core (no field `meta`). */
  private val undeclaredMapping: String =
    """{"properties": {
      |  "id": {"type": "keyword"}, "n": {"type": "integer"},
      |  "k": {"type": "keyword", "fields": {"raw": {"type": "keyword"}}},
      |  "obj": {"properties": {"a": {"type": "integer"}}},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}},
      |  "g": {"type": "geo_point"}, "d": {"type": "date"}
      |}}""".stripMargin

  /** Each kind's own several values (u1), and one value / a list holding one object (u2). */
  private val undeclaredDocs: Seq[(String, String)] = Seq(
    "u1" -> """{"id":"u1","n":[1,2],"k":["x","y"],"obj":{"a":[1,2]},"items":[{"p":[1,2]}],"g":[[1.0,2.0],[3.0,4.0]],"d":["2026-11-16T00:00:00Z","2026-11-17T00:00:00Z"]}""",
    "u2" -> """{"id":"u2","n":3,"k":"z","obj":[{"a":[3,4]}],"items":[{"p":3},{"p":4}],"g":[1.0,2.0],"d":"2026-11-18T00:00:00Z"}"""
  )

  /** The same kinds declared by core (CREATE TABLE writes each leaf's `meta`, A-169). */
  private val coreTable: String =
    "(id KEYWORD, n INT, ln ARRAY<INT>, k KEYWORD, obj STRUCT FIELDS (a INT), " +
    "items ARRAY<STRUCT> FIELDS (p INT), g GEO_POINT, d TIMESTAMP, x INT)"

  /** One document per declared kind holding several values, by id. */
  private val coreDocs: Seq[(String, String)] = Seq(
    "c1" -> """{"id":"c1","n":[1,2]}""",
    "c2" -> """{"id":"c2","ln":[1,2]}""",
    "c3" -> """{"id":"c3","obj":{"a":[1,2]}}""",
    "c4" -> """{"id":"c4","k":["x","y"]}""",
    "c5" -> """{"id":"c5","items":[{"p":[1,2]}]}""",
    "c6" -> """{"id":"c6","g":[[1.0,2.0],[3.0,4.0]]}""",
    "c7" -> """{"id":"c7","d":["2026-11-16T00:00:00Z","2026-11-17T00:00:00Z"]}""",
    "c8" -> """{"id":"c8","obj":[{"a":5},{"a":6}]}"""
  )

  /** SCHEMA-17 (F1) -- a table created on main (a `_meta` declaration, no field `meta`), with a
    * user's own field `meta` key (F2), then altered in place through elasticsql: the first in-place
    * ALTER writes every leaf's declaration into its mapping `meta` (A-171).
    */
  private val declaredMapping: String =
    """{"_meta": {"columns": {"dc": {"data_type": "DATE"}, "tm": {"data_type": "TIME"},
      | "d": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "tags": {"type": "keyword"},
      |  "obj": {"properties": {"a": {"type": "integer"}, "s": {"type": "keyword"}}},
      |  "dc": {"type": "date"}, "tm": {"type": "date", "format": "HH:mm:ss"},
      |  "d": {"type": "date", "format": "dd/MM/yyyy"},
      |  "dz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"},
      |  "ts": {"type": "date", "meta": {"unit": "utc"}}
      |}}""".stripMargin

  private val declaredDocs: Seq[(String, String)] = Seq(
    "f1" -> """{"id":"f1","tags":["a","b"],"obj":[{"a":1,"s":"x"},{"a":2,"s":"y"}],"dc":"2026-11-16","tm":"10:30:00","d":"16/11/2026","dz":"2026-11-16 10:30 UTC","ts":"2026-11-16T10:30:00Z"}""",
    "f2" -> """{"id":"f2","tags":"c","obj":{"a":3,"s":"z"},"dc":"2026-11-17","tm":"11:00:00","d":"17/11/2026","ts":"2026-11-17T00:00:00Z"}"""
  )

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
    put(index, documents)
  }

  /** `documents` indexed raw into `index`, refreshed. */
  private def put(index: String, documents: Seq[(String, String)]): Unit = {
    // raw: the core client's `index` wraps an `unsigned_long` above 2^63-1
    documents.foreach { case (id, source) =>
      raw("PUT", s"/$index/_doc/$id", Some(source))._1 should (be(200) or be(201))
    }
    raw("POST", s"/$index/_refresh")._1 shouldBe 200
    ()
  }

  /** A DDL statement run by the privileged client, which must succeed. */
  private def ddl(statement: String): Unit =
    Await.result(client.run(statement), 60.seconds) match {
      case ElasticSuccess(_) =>
      case other             => fail(s"[$statement] $other")
    }

  override def beforeAll(): Unit = {
    super.beforeAll()
    blockUntil("licence active", 60, 1000) { () =>
      Try(raw("GET", "/_license")._2.contains("\"active\"")).getOrElse(false)
    }
    Seq(c, cp, c1, c2).foreach { t =>
      Try(Await.result(client.run(s"DROP TABLE IF EXISTS $t"), 60.seconds))
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
    Seq(declared1, declared2).foreach { index =>
      load(index, declaredMapping, declaredDocs)
      Seq(
        s"ALTER TABLE $index ALTER COLUMN tags SET DATA TYPE ARRAY<KEYWORD>",
        s"ALTER TABLE $index ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>"
      ).foreach { ddl =>
        Await.result(client.run(ddl), 60.seconds) match {
          case ElasticSuccess(_) =>
          case other             => fail(s"[$ddl] $other")
        }
      }
    }
    load("s7_p2", """{"properties": {"k": {"type":"long"}}}""", Seq("p2" -> """{"k":7}"""))
    // SCHEMA-17 fix R-150
    load(u, undeclaredMapping, undeclaredDocs)
    load(u2, undeclaredMapping, Seq("m1" -> """{"id":"m1","obj":[{"a":5},{"a":6}]}"""))
    ddl(s"CREATE TABLE $c $coreTable")
    put(c, coreDocs)
    ddl(s"CREATE TABLE $cp (id KEYWORD, n INT, x INT)")
    load(
      m,
      """{"_meta": {"columns": {"n": {"data_type": "INT"}, "x": {"data_type": "INT"}}},
        | "properties": {"id": {"type": "keyword"}, "n": {"type": "integer"},
        |  "x": {"type": "integer"}}}""".stripMargin,
      Seq("m1" -> """{"id":"m1","n":[1,2]}""")
    )
    Seq(n1 -> "n1", n2 -> "n2").foreach { case (index, id) =>
      load(
        index,
        """{"properties": {"id": {"type":"keyword"}, "n": {"type":"integer"}}}""",
        Seq(id -> (if (id == "n1") s"""{"id":"$id","n":[1,2]}""" else s"""{"id":"$id","n":3}"""))
      )
    }
    Seq(c1 -> "c1", c2 -> "c2").foreach { case (index, id) =>
      ddl(s"CREATE TABLE $index (id KEYWORD, n INT)")
      put(
        index,
        Seq(id -> (if (id == "c1") s"""{"id":"$id","n":[1,2]}""" else s"""{"id":"$id","n":3}"""))
      )
    }
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

  it should "read a several-valued cell of a field with no core meta as its list, where the privileged client fails naming the ALTER (SCHEMA-17 fix R-150; A-41)" in {
    val sql = s"SELECT id, tags FROM $listed LIMIT 10"
    rows(roClient, sql).map(r => r("id") -> r("tags")).toMap shouldBe Map(
      "t1" -> List("a", "b"),
      "t2" -> "c"
    )
    outcome(client, sql).left.getOrElse("") should include("ARRAY<KEYWORD>")
  }

  "A read-only user's table declared by elasticsql" should "be typed and read as a mapping reader's: lists, objects lists, dates (SCHEMA-17 F1)" in {
    val sql = s"SELECT * FROM $declared1"
    val ro = described(roClient, sql)
    ro shouldBe described(client, sql)
    ro("tags") shouldBe "ARRAY<VARCHAR>"
    ro("obj") shouldBe "ARRAY<STRUCT>"
    ro("dc") shouldBe "DATE"
    ro("tm") shouldBe "TIME"
    ro("d") shouldBe "DATE"
    ro("ts") shouldBe "TIMESTAMP"
    sorted(rows(roClient, sql)) shouldBe sorted(rows(client, sql))
    // one object is a list of one
    rows(roClient, s"SELECT id, obj FROM $declared1")
      .find(_("id") == "f2")
      .map(_("obj")) match {
      case Some(list: Seq[_]) => list.size shouldBe 1
      case other              => fail(s"one object: $other")
    }
  }

  it should "keep the user's own field meta beside core's, which SHOW CREATE TABLE never shows (F2)" in {
    val (_, body) = raw("GET", s"/$declared1/_mapping")
    val ts = new com.fasterxml.jackson.databind.ObjectMapper()
      .readTree(body)
      .path(declared1)
      .path("mappings")
      .path("properties")
      .path("ts")
      .path("meta")
    ts.path("unit").asText() shouldBe "utc"
    ts.path("sql_type").asText().toLowerCase shouldBe "timestamp"
    Await.result(client.run(s"SHOW CREATE TABLE $declared1"), 60.seconds) match {
      case ElasticSuccess(app.softnetwork.elastic.client.result.SQLResult(ddl)) =>
        ddl should not include "sql_"
        ddl should include("unit")
      case other => fail(s"SHOW CREATE TABLE: $other")
    }
  }

  it should "read list-ness only over several indices, and fail a date of no readable format loudly (A-179, LEAD-Q1 b)" in {
    val sql = s"SELECT id, tags, obj FROM $declared1,$declared2"
    val ro = described(roClient, sql)
    ro("tags") shouldBe "ARRAY<VARCHAR>"
    ro("obj") shouldBe "ARRAY<STRUCT>"
    sorted(rows(roClient, sql)) shouldBe sorted(rows(client, sql))
    // a date's format is not applied over several indices: ISO 8601 only, any other spelling refused
    described(roClient, s"SELECT d FROM $declared1,$declared2")("d") shouldBe "TIMESTAMP"
    outcome(roClient, s"SELECT id, d FROM $declared1,$declared2").left.getOrElse("") should include(
      "view_index_metadata"
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

  // -------------------------------------------------------------------------------------------
  // SCHEMA-17 fix R-150 -- a leaf with no core meta reads its own several values as stored, as on
  // ES 6.8; a leaf core declared keeps A-173; objects keep the statement's rule (A-183, A-184)
  // -------------------------------------------------------------------------------------------

  private def point(lat: Double, lon: Double): ListMap[String, Any] =
    ListMap("lat" -> lat, "lon" -> lon)

  /** A date value's instant(s), whatever zone spelling it was answered in. */
  private def instantsOf(value: Any): Any = value match {
    case z: _root_.java.time.ZonedDateTime => z.toInstant
    case s: Seq[_]                         => s.map(instantsOf)
    case other                             => other
  }

  private def instant(day: Int): _root_.java.time.Instant =
    _root_.java.time.Instant.parse(f"2026-11-$day%02dT00:00:00Z")

  /** Every value a cell holds, its lists and objects flattened. */
  private def flat(value: Any): Seq[Any] = value match {
    case m: scala.collection.Map[_, _] => m.values.toSeq.flatMap(flat)
    case s: Seq[_]                     => s.flatMap(flat)
    case other                         => Seq(other)
  }

  /** `sql`'s rows on `on`, by id. */
  private def byId(on: ElasticClientApi, sql: String): Map[Any, ListMap[String, Any]] =
    rows(on, sql).map(r => r("id") -> r).toMap

  /** The failure `sql` answers on `on`, which must fail. */
  private def refusal(on: ElasticClientApi, sql: String): String =
    outcome(on, sql) match {
      case Left(error) => error
      case Right(rs)   => fail(s"[$sql] answered $rs instead of failing")
    }

  "A read-only user's leaf with no core meta" should "answer its own several values as stored, each by its type (R-150 AC1, C1)" in {
    raw("GET", s"/$u/_field_caps?fields=n")._2 should not include "sql_type"
    val r = byId(roClient, s"SELECT id, n, k, obj.a, items, g, d FROM $u LIMIT 10")
    r("u1")("n") shouldBe List(1, 2)
    r("u1")("n") match {
      case values: Seq[_] =>
        values.map(_.getClass).distinct shouldBe Seq(classOf[_root_.java.lang.Integer])
      case other => fail(s"not a list: $other")
    }
    r("u2")("n") shouldBe 3
    r("u1")("k") shouldBe List("x", "y")
    r("u2")("k") shouldBe "z"
    // a leaf in one object, and in a list holding one object
    r("u1")("obj.a") shouldBe List(1, 2)
    r("u2")("obj.a") shouldBe List(3, 4)
    // an element's own leaf of a nested field
    r("u1")("items") shouldBe List(ListMap("p" -> List(1, 2)))
    r("u2")("items") shouldBe List(ListMap("p" -> 3), ListMap("p" -> 4))
    r("u1")("g") shouldBe List(point(2.0, 1.0), point(4.0, 3.0))
    r("u2")("g") shouldBe point(2.0, 1.0)
    instantsOf(r("u1")("d")) shouldBe List(instant(16), instant(17))
    instantsOf(r("u2")("d")) shouldBe instant(18)
    // describe keeps the field's Elasticsearch type, one value: the describe / run residue ES 6.8's
    // read-only users share (R-146 Q2 (a), AC5)
    val types = described(roClient, s"SELECT id, n, d FROM $u")
    types("n") shouldBe "INT"
    types("d") shouldBe "TIMESTAMP"
  }

  it should "answer the same through every consumer: un-LIMITed, *, a top hit, a window's base, UNION ALL legs (C2)" in {
    def ns(sql: String): Map[Any, Any] =
      rows(roClient, sql).map(r => r("id") -> r("n")).toMap
    ns(s"SELECT id, n FROM $u") shouldBe Map("u1" -> List(1, 2), "u2" -> 3)
    val star = byId(roClient, s"SELECT * FROM $u LIMIT 10")
    star("u1")("n") shouldBe List(1, 2)
    star("u1")("obj") shouldBe ListMap("a" -> List(1, 2))
    star("u2")("obj") shouldBe ListMap("a" -> List(3, 4))
    star("u1")("items") shouldBe List(ListMap("p" -> List(1, 2)))
    flat(rows(roClient, s"SELECT ARRAY_AGG(n) AS a FROM $u").head("a")).toSet shouldBe Set(1, 2, 3)
    rows(roClient, s"SELECT id, n, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn FROM $u")
      .map(r => r("id").toString -> flat(r("n")))
      .toMap shouldBe Map("u1" -> Seq(1, 2), "u2" -> Seq(3))
    rows(roClient, s"SELECT id, n FROM $u UNION ALL SELECT id, n FROM $u")
      .map(r => r("id").toString -> flat(r("n")))
      .sortBy(_._1) shouldBe Seq(
      "u1" -> Seq(1, 2),
      "u1" -> Seq(1, 2),
      "u2" -> Seq(3),
      "u2" -> Seq(3)
    )
  }

  it should "keep the statement's rule for objects: several objects, and a path they hold, fail naming the list's ALTER (A-183, C1)" in {
    Seq(roClient, client).foreach { on =>
      refusal(on, s"SELECT id, obj.a FROM $u2 LIMIT 10") should include(
        "Column obj holds several values"
      )
      refusal(on, s"SELECT id, obj FROM $u2 LIMIT 10") should include(
        "SET DATA TYPE ARRAY<STRUCT>"
      )
    }
    // the privileged client keeps refusing the undeclared leaf (SCHEMA-6 Q1, C5)
    refusal(client, s"SELECT id, n FROM $u LIMIT 10") should include("ARRAY<INT>")
  }

  "A read-only user's leaf core declared" should "fail its several values as the privileged client does, naming the ALTER (R-150 AC2, A-173)" in {
    raw("GET", s"/$c/_field_caps?fields=n")._2 should include("sql_type")
    Seq(
      ("c1", "n", "Column n holds several values"),
      ("c3", "obj.a", "Column obj.a holds several values"),
      ("c4", "k", "Column k holds several values"),
      ("c5", "items", "Column items.p holds several values"),
      ("c6", "g", "Column g holds several values"),
      ("c7", "d", "Column d holds several values"),
      ("c8", "obj.a", "Column obj holds several values"),
      ("c8", "obj", "Column obj holds several values")
    ).foreach { case (id, column, message) =>
      val sql = s"SELECT id, $column FROM $c WHERE id = '$id'"
      withClue(s"[$sql] ") {
        refusal(roClient, sql) should include(message)
        refusal(client, sql) should include(message)
      }
    }
    // a column declared a list still reads its list
    described(roClient, s"SELECT id, ln FROM $c")("ln") shouldBe "ARRAY<INT>"
    byId(roClient, s"SELECT id, ln FROM $c WHERE id = 'c2'")("c2")("ln") shouldBe List(1, 2)
  }

  it should "fail through every consumer (C2)" in {
    Seq(
      s"SELECT id, n FROM $c WHERE id = 'c1' LIMIT 10",
      s"SELECT id, n FROM $c WHERE id = 'c1'",
      s"SELECT * FROM $c WHERE id = 'c1'",
      s"SELECT ARRAY_AGG(n) AS a FROM $c WHERE id = 'c1'",
      s"SELECT id, n, ROW_NUMBER() OVER (PARTITION BY id ORDER BY id) AS rn FROM $c WHERE id = 'c1'",
      s"SELECT id, n FROM $c WHERE id = 'c1' UNION ALL SELECT id, n FROM $c WHERE id = 'c1'"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        refusal(roClient, sql) should include("ARRAY<INT>")
      }
    }
  }

  "A read-only user's field added outside core" should "read as stored until an in-place ALTER declares it (R-150 AC3, T0-C, C3)" in {
    def capsOf(index: String, field: String): String =
      raw("GET", s"/$index/_field_caps?fields=$field")._2
    raw(
      "PUT",
      s"/$cp/_mapping",
      Some("""{"properties": {"pn": {"type": "integer"}}}""")
    )._1 shouldBe
    200
    put(cp, Seq("p1" -> """{"id":"p1","n":3,"pn":[1,2]}"""))
    // T0-C before: core's field carries its key, the field added by a plain mapping update none
    capsOf(cp, "n") should include("sql_type")
    capsOf(cp, "pn") should not include "sql_type"
    roClient.invalidateSchema(cp)
    byId(roClient, s"SELECT id, n, pn FROM $cp")("p1")("pn") shouldBe List(1, 2)
    // an in-place ALTER of ANOTHER column rewrites every leaf's meta (A-171): now declared
    ddl(s"ALTER TABLE $cp ALTER COLUMN x SET DATA TYPE ARRAY<INT>")
    capsOf(cp, "pn") should include("sql_type")
    roClient.invalidateSchema(cp)
    refusal(roClient, s"SELECT id, pn FROM $cp") should include("ARRAY<INT>")
    refusal(client, s"SELECT id, pn FROM $cp") should include("ARRAY<INT>")
    // a table created on main carries no field meta until its first in-place ALTER (A-171)
    capsOf(m, "n") should not include "sql_type"
    byId(roClient, s"SELECT id, n FROM $m")("m1")("n") shouldBe List(1, 2)
    ddl(s"ALTER TABLE $m ALTER COLUMN x SET DATA TYPE ARRAY<INT>")
    capsOf(m, "n") should include("sql_type")
    roClient.invalidateSchema(m)
    refusal(roClient, s"SELECT id, n FROM $m") should include("ARRAY<INT>")
  }

  "A read-only user's scalar over several indices" should "read as stored whatever the members' meta: both, one or none (R-150 AC4, A-184, C4)" in {
    Seq(
      s"$n1,$n2" -> Map("n1" -> List(1, 2), "n2" -> 3),
      s"$c1,$c2" -> Map("c1" -> List(1, 2), "c2" -> 3),
      s"$c1,$n2" -> Map("c1" -> List(1, 2), "n2" -> 3)
    ).foreach { case (source, expected) =>
      withClue(s"[$source] ") {
        rows(roClient, s"SELECT id, n FROM $source LIMIT 10")
          .map(r => r("id") -> r("n"))
          .toMap shouldBe expected
        described(roClient, s"SELECT id, n FROM $source")("n") shouldBe "INT"
      }
    }
    // the privileged client reads the mapping: it refuses, naming the ALTER
    refusal(client, s"SELECT id, n FROM $c1,$c2 LIMIT 10") should include("ARRAY<INT>")
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
