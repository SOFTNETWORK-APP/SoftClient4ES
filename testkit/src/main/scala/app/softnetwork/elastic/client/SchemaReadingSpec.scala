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
  ElasticResult,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-2 -- the declared [[ResultSchema]] and the typed hit rows, on a real cluster.
  *
  *   - "analysis rows": table T of the story (AC3) on fixture `s2_t`;
  *   - "classes": every non-null value of a column whose type is not value-typed has D2's class, on
  *     every hit route (AC2): one-shot search, `searchAsync`, scroll / PIT, search_after, the
  *     `_msearch` legs and the per-leg `UNION ALL` route, UNNEST inner hits;
  *   - "q4": `SELECT *` = the cached mapping, an unmapped path read as ANY, an explicit path's one
  *     reload (AC6, Elasticsearch 7+);
  *   - "describe": `describe(sql)` = `run(sql)`'s schema (AC1).
  *
  * SCHEMA-2b -- aggregation values and window statements by type, on fixtures `s2b_t` / `s2b_w`:
  *
  *   - "aggregation rows": Table A (AC3);
  *   - "aggregation classes": every non-null value of an aggregation column has D2's class on every
  *     aggregation route (AC2): one-shot search, `searchAsync`, the `scroll()` API, the `_msearch`
  *     legs of a UNION ALL;
  *   - "window oracle": every partition column of `s2b_w` x {one-shot, async, scroll} x {rn, s, f}
  *     answers the fixture's arithmetic (AC4);
  *   - "describe aggregates": `describe(sql)` = `run(sql)`'s schema over aggregate, top_hits, Case
  *     4 and window statements (AC1).
  *
  * PUBLIC API ONLY but `describe` and the schema: the same fixture runs as the T0 baseline probe on
  * a main clone.
  */
trait SchemaReadingSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  protected val table = "s2_t"
  protected val q4 = "q4_t"
  protected val q4d = "q4d_t"

  protected lazy val major: Int = Try(elasticVersion.takeWhile(_ != '.').toInt).getOrElse(8)

  private val mapping: String =
    """{"_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"},
      |  "ts": {"type": "date"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "c_c1": {"type": "date", "format": "yyyy/MM/dd"},
      |  "dm": {"type": "date", "ignore_malformed": true},
      |  "n": {"type": "integer"}, "x": {"type": "double"},
      |  "b": {"type": "boolean"}, "g": {"type": "geo_point"}, "bin": {"type": "binary"},
      |  "k": {"type": "keyword"}, "kw": {"type": "keyword", "ignore_above": 5},
      |  "nul": {"type": "keyword"},
      |  "obj": {"properties": {"a": {"type": "integer"}}},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}}
      |}}""".stripMargin

  /** One document per value of table T (`_id` = `id`); a point as WKT (`g4`) where the major
    * indexes it -- Elasticsearch 7+: 6.8.23 reads a text point as `lat,lon` or a geohash only.
    */
  protected def docs: Seq[(String, String)] = Seq(
    "t1" ->
    """{"id":"t1","ts":"2024-01-31T10:30:00","c_des":1706697000,"c_c1":"2024/01/31","n":"5.5","x":" 2.5 ","b":"","bin":"aGVsbG8=","k":"10:30","dm":"not-a-date","kw":"abcdefgh","g":{"lat":1.5,"lon":2.5},"obj":{"a":1},"items":[{"p":1},{"p":2}]}""",
    "t3" -> """{"id":"t3","n":3,"g":"1.5,2.5"}""",
    "g3" -> """{"id":"g3","g":[2.5,1.5]}"""
  ) ++ (if (major >= 7) Seq("g4" -> """{"id":"g4","g":"POINT (2.5 1.5)"}""") else Nil) ++ Seq(
    "g5" -> """{"id":"g5","g":"s0"}"""
  )

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(client.deleteIndex(index))
  }

  private def load(index: String, mappingJson: String, documents: Seq[(String, String)]): Unit = {
    client
      .createIndex(index, settings = """{"number_of_shards": 2, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mappingJson).get shouldBe true
    documents.foreach { case (id, source) =>
      client.index(index, id, source) match {
        case ElasticSuccess(true) =>
        case other                => fail(s"indexing $id into $index: $other")
      }
    }
    client.refresh(index)
  }

  // SCHEMA-2b fixtures: Table A (`s2b_t`) and the window oracle (`s2b_w`)
  protected val aggTable = "s2b_t"
  protected val windowTable = "s2b_w"

  private val aggMapping: String =
    """{"_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "b": {"type": "boolean"},
      |  "dc": {"type": "date", "format": "yyyy/MM/dd"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "big": {"type": "long"}, "n": {"type": "integer"}, "k": {"type": "keyword"},
      |  "x": {"type": "double"}, "r": {"type": "float"}
      |}}""".stripMargin

  /** Table A: `big` {42, 1706697000000}, `n` sum 15 max 10, `k` "10:30"; `a3` holds only a
    * whole-day `c_des` (A-25).
    */
  private val aggDocs: Seq[(String, String)] = Seq(
    "a1" -> """{"id":"a1","b":true,"dc":"2024/01/31","c_des":1706697000,"big":42,"n":5,"k":"10:30","x":1.5,"r":0.5}""",
    "a2" -> """{"id":"a2","b":true,"dc":"2024/02/01","big":1706697000000,"n":10,"k":"10:30","x":2.5,"r":1.5}""",
    "a3" -> """{"id":"a3","c_des":1706659200}"""
  )

  /** `pz` (a zone-name format, read as stored) exists from Elasticsearch 7 on: 6.8's Joda-Time
    * cannot parse a zone name, so it would refuse the documents.
    */
  protected lazy val zoneNames: Boolean = major >= 7

  // `pn` is the spec's `pi` (PI is a reserved word)
  private def windowMapping: String =
    """{"_meta": {"columns": {"pd": {"data_type": "DATE"}, "pe": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "n": {"type": "integer"},
      |  "pk1": {"type": "keyword"}, "pk2": {"type": "keyword"}, "pu": {"type": "keyword"},
      |  "pt": {"type": "date"}, "pb": {"type": "boolean"},
      |  "pd": {"type": "date", "format": "epoch_second"}, "pe": {"type": "date"},""".stripMargin +
    (if (zoneNames) """ "pz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"},""" else "") +
    """ "pn": {"type": "integer"}}}"""

  private def windowDocs: Seq[(String, String)] = {
    def pz(text: String): String = if (zoneNames) s""","pz":"$text"""" else ""
    Seq(
      "w1" -> s"""{"id":"w1","n":1,"pk1":"10:30","pk2":"2024-01-31","pu":"x","pt":"2024-01-31T10:30:00.123456Z","pb":true,"pd":1706659200,"pe":"2024-01-31T00:00:00Z"${pz(
        "2024-01-31 10:30 UTC"
      )},"pn":7}""",
      "w2" -> s"""{"id":"w2","n":2,"pk1":"10:30","pk2":"2024-01-31","pu":"x","pt":"2024-01-31T10:30:00.123Z","pb":true,"pd":1706697000,"pe":"2024-01-31T10:30:00Z"${pz(
        "2024-01-31 11:30 CET"
      )},"pn":7}""",
      "w3" -> s"""{"id":"w3","n":4,"pk1":"11:00","pk2":"2024-02-01","pu":"y","pt":"2024-02-01T00:00:00Z","pb":false,"pd":1706709600,"pe":"2024-01-31T00:00:00Z"${pz(
        "2024-02-01 00:00 UTC"
      )},"pn":8}"""
    )
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Seq(table, q4, q4d, aggTable, windowTable).foreach(drop)
    load(table, mapping, docs)
    load(aggTable, aggMapping, aggDocs)
    load(windowTable, windowMapping, windowDocs)
    ()
  }

  override def afterAll(): Unit = {
    Seq(table, q4, q4d, aggTable, windowTable).foreach(drop)
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // Results, routes, the class oracle
  // -------------------------------------------------------------------------------------------

  protected def runOk(sql: String): QueryResult =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error)  => fail(s"[$sql] failed: ${error.message}")
    }

  /** The rows and the schema of a result, as a consumer reads them (`case q: QueryRows`). */
  protected def answer(result: QueryResult): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    result match {
      case q: QueryRows       => (q.rows, q.schema)
      case q: QueryStructured => (q.response.results, q.schema)
      case q: QueryStream =>
        (Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds), q.schema)
      case other => fail(s"unexpected result $other")
    }

  protected def rowsOf(sql: String): Seq[ListMap[String, Any]] = answer(runOk(sql))._1

  /** The routes a hit row is read on (AC2). */
  protected def onRoute(
    route: String,
    sql: String
  ): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    route match {
      case "search" =>
        client.search(SelectStatement(s"$sql LIMIT 100"))(NativeContext) match {
          case ElasticSuccess(r)     => (r.results, r.schema)
          case ElasticFailure(error) => fail(s"[$route] $sql: ${error.message}")
        }
      case "searchAsync"  => answer(runOk(s"$sql LIMIT 100"))
      case "scroll"       => answer(runOk(sql))
      case "search_after" => answer(runOk(s"$sql ORDER BY id"))
      case "msearch"      => answer(runOk(s"$sql LIMIT 100 UNION ALL $sql LIMIT 100"))
      case "per-leg"      => answer(runOk(s"$sql UNION ALL $sql"))
    }

  protected val routes: Seq[String] =
    Seq("search", "searchAsync", "scroll", "search_after", "msearch", "per-leg")

  /** D2's class for a value of `column`, at every depth; `true` for a value-typed column. */
  protected def classOk(column: ResultColumn, value: Any): Boolean =
    value == null || (column.sqlType match {
      case SQLTypes.TinyInt | SQLTypes.SmallInt | SQLTypes.Int => value.isInstanceOf[Integer]
      case SQLTypes.BigInt                                     => value.isInstanceOf[java.lang.Long]
      case SQLTypes.Double  => value.isInstanceOf[java.lang.Double]
      case SQLTypes.Real    => value.isInstanceOf[java.lang.Float]
      case SQLTypes.Boolean => value.isInstanceOf[java.lang.Boolean]
      case SQLTypes.Varchar => value.isInstanceOf[String]
      case SQLTypes.Date    => value.isInstanceOf[LocalDate]
      case SQLTypes.Time    => value.isInstanceOf[LocalTime]
      case SQLTypes.Timestamp =>
        value.isInstanceOf[ZonedDateTime] &&
          value.asInstanceOf[ZonedDateTime].getZone == ZoneOffset.UTC
      case SQLTypes.VarBinary => value.isInstanceOf[Array[Byte]]
      case SQLTypes.GeoPoint | SQLTypes.Struct =>
        value match {
          case m: scala.collection.Map[_, _] =>
            val map = m.asInstanceOf[scala.collection.Map[String, Any]]
            column.fields.forall(f => classOk(f, map.getOrElse(f.name, null)))
          case _ => false
        }
      case SQLTypes.Array(_) =>
        value match {
          case s: Seq[_] => column.element.forall(e => s.forall(classOk(e, _)))
          case _         => false
        }
      case _ => true // NUMERIC, TEMPORAL, ANY: value-typed (R-13)
    })

  protected def classMisses(
    rows: Seq[ListMap[String, Any]],
    schema: ResultSchema
  ): Seq[String] =
    for {
      row    <- rows
      column <- schema.columns
      value = row.getOrElse(column.name, null)
      if !classOk(column, value)
    } yield s"${row.getOrElse("id", "?")}.${column.name}: $value (${value.getClass.getName}) is not ${column.sqlType.typeId}"

  // -------------------------------------------------------------------------------------------
  // Analysis rows (table T)
  // -------------------------------------------------------------------------------------------

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  private def one(sql: String): ListMap[String, Any] =
    rowsOf(sql) match {
      case Seq(row) => row
      case other    => fail(s"[$sql] answered ${other.size} rows: $other")
    }

  "Table T" should "give the analysis rows' answers (AC3)" in {
    val t1 = one(s"SELECT id, ts, c_des, c_c1, n, x, b, bin, nul FROM $table WHERE id = 't1'")
    // 1: a date of the default format, undeclared: TIMESTAMP at UTC
    t1("ts") shouldBe utc(2024, 1, 31, 10, 30)
    // 2: epoch_second declared DATE
    t1("c_des") shouldBe LocalDate.of(2024, 1, 31)
    // 3: a custom format
    t1("c_c1") shouldBe utc(2024, 1, 31)
    // 5: numbers Elasticsearch accepted as text
    t1("n") shouldBe Integer.valueOf(5)
    t1("x") shouldBe java.lang.Double.valueOf(2.5)
    // 6: an empty text in a boolean field
    t1("b") shouldBe java.lang.Boolean.FALSE
    // 8: binary, decoded
    t1("bin").asInstanceOf[Array[Byte]].toSeq shouldBe "hello".getBytes("US-ASCII").toSeq
    // 12: a keyword never set
    t1("nul") shouldBe (null: Any)
    // 7: every spelling of a point
    val points = rowsOf(s"SELECT id, g FROM $table WHERE id IN ('t1', 't3', 'g3', 'g4')")
    points.map(_("g")).distinct shouldBe Seq(ListMap("lat" -> 1.5, "lon" -> 2.5))
    one(s"SELECT g FROM $table WHERE id = 'g5'")("g") match {
      case m: scala.collection.Map[_, _] => m.keySet shouldBe Set("lat", "lon")
      case other                         => fail(s"geohash answered $other")
    }
    // 10: a computed column, unwrapped
    val v = one(s"SELECT id, n + 1 AS v FROM $table WHERE id = 't3'")("v")
    v.asInstanceOf[Number].longValue() shouldBe 4L
    v should not be a[Seq[_]]
    // 13: a DATE beside a TIMESTAMP is a TIMESTAMP in both legs; a keyword is its text
    val union = rowsOf(
      s"SELECT c_des AS d, k FROM $table WHERE id = 't1' UNION ALL " +
      s"SELECT ts AS d, k FROM $table WHERE id = 't1'"
    )
    union.map(_("d")).toSet shouldBe Set(utc(2024, 1, 31), utc(2024, 1, 31, 10, 30))
    union.map(_("k")).distinct shouldBe Seq("10:30")
    // 17: a date Elasticsearch ignored is NULL; a keyword over ignore_above keeps its text
    val t17 = one(s"SELECT dm, kw FROM $table WHERE id = 't1'")
    t17("dm") shouldBe (null: Any)
    t17("kw") shouldBe "abcdefgh"
  }

  // -------------------------------------------------------------------------------------------
  // Classes (AC2)
  // -------------------------------------------------------------------------------------------

  "Every hit route" should "answer every value of a typed column in D2's class (AC2)" in {
    val sql = s"SELECT id, ts, c_des, c_c1, n, x, b, bin, k, nul, obj FROM $table"
    routes.foreach { route =>
      val (rows, schema) = onRoute(route, sql)
      withClue(s"[$route] ") {
        rows should not be empty
        val s = schema.getOrElse(fail("no schema"))
        s.names shouldBe Seq("id", "ts", "c_des", "c_c1", "n", "x", "b", "bin", "k", "nul", "obj")
        classMisses(rows, s) shouldBe empty
        // strict projection: no undeclared key
        rows.foreach(row =>
          (row.keySet -- s.names - ElasticConversion.DocumentIdField) shouldBe empty
        )
      }
    }
    // a point column, its documents' every spelling (several points excluded, D7)
    routes.foreach { route =>
      val (rows, schema) = onRoute(route, s"SELECT id, g FROM $table")
      withClue(s"[$route] g ")(
        classMisses(rows, schema.getOrElse(fail("no schema"))) shouldBe empty
      )
    }
  }

  it should "read an UNNEST element by its column's type (inner hits)" in {
    val (rows, schema) =
      answer(runOk(s"SELECT t.id, i.p FROM $table AS t JOIN UNNEST(t.items) AS i LIMIT 100"))
    rows.map(_("i.p")).toSet shouldBe Set(1, 2)
    schema.foreach(s => classMisses(rows, s) shouldBe empty)
  }

  // -------------------------------------------------------------------------------------------
  // Q4 (AC6)
  // -------------------------------------------------------------------------------------------

  private def rawPut(path: String, body: String): Unit = {
    val request = new Request("PUT", path)
    request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON))
    restClient.performRequest(request)
    ()
  }

  private def typeOf(result: QueryResult, column: String): Option[String] =
    answer(result)._2.flatMap(_.columns.find(_.name == column)).map(_.sqlType.typeId)

  "SELECT * and an unmapped path" should "follow the cached mapping (AC6, q4)" in {
    assume(major >= 7, "Q4 runs on Elasticsearch 7+ (typeless mapping PUT)")
    load(
      q4,
      """{"dynamic": false, "properties": {"id": {"type": "keyword"},
        | "obj": {"properties": {"a": {"type": "integer"}}}}}""".stripMargin,
      Seq("1" -> """{"id":"1","extra":42,"extra2":7,"obj":{"a":1,"zz":1}}""")
    )
    val star = runOk(s"SELECT * FROM $q4")
    val (starRows, starSchema) = answer(star)
    starSchema.map(_.names) shouldBe Some(Seq("id", "obj"))
    starRows.head.contains("extra") shouldBe false
    starRows.head("obj") shouldBe ListMap("a" -> 1)
    // an unmapped column: ANY, as read
    val extra = runOk(s"SELECT extra FROM $q4")
    answer(extra)._1.head("extra") shouldBe 42
    typeOf(extra, "extra") shouldBe Some("ANY")
    // an unmapped path under a mapped parent
    answer(runOk(s"SELECT obj.zz FROM $q4"))._1.head("obj.zz") shouldBe 1
    // a field mapped since (raw PUT, not through core): the memoized `extra` reads no mapping...
    rawPut(
      s"/$q4/_mapping",
      """{"properties": {"extra": {"type": "long"}, "extra2": {"type": "long"}}}"""
    )
    typeOf(runOk(s"SELECT extra FROM $q4"), "extra") shouldBe Some("ANY")
    // ...a path never selected reloads it once: BIGINT, describe too
    val extra2 = runOk(s"SELECT extra2 FROM $q4")
    typeOf(extra2, "extra2") shouldBe Some("BIGINT")
    answer(extra2)._1.head("extra2") shouldBe java.lang.Long.valueOf(7L)
    client.describe(s"SELECT extra2 FROM $q4") match {
      case ElasticSuccess(Some(s)) => s.columns.map(_.sqlType.typeId) shouldBe Seq("BIGINT")
      case other                   => fail(s"$other")
    }
  }

  it should "show a field mapped since only after an explicit selection reloads it (A-17)" in {
    assume(major >= 7, "Q4 runs on Elasticsearch 7+")
    load(q4d, """{"properties": {"id": {"type": "keyword"}}}""", Seq("1" -> """{"id":"1"}"""))
    answer(runOk(s"SELECT * FROM $q4d"))._2.map(_.names) shouldBe Some(Seq("id"))
    client.index(q4d, "2", """{"id":"2","nf":5}""")
    client.refresh(q4d)
    answer(runOk(s"SELECT * FROM $q4d"))._2.map(_.names) shouldBe Some(Seq("id"))
    val nf = runOk(s"SELECT nf FROM $q4d")
    typeOf(nf, "nf") shouldBe Some("BIGINT")
    answer(runOk(s"SELECT * FROM $q4d"))._2.map(_.names.toSet) shouldBe Some(Set("id", "nf"))
  }

  // -------------------------------------------------------------------------------------------
  // describe (AC1)
  // -------------------------------------------------------------------------------------------

  "describe" should "answer run's schema (AC1)" in {
    Seq(
      s"SELECT id, ts, c_des, n FROM $table",
      s"SELECT * FROM $table",
      s"SELECT n AS k, CAST(n AS VARCHAR) AS s FROM $table LIMIT 5",
      s"SELECT k, COUNT(*) AS c FROM $table GROUP BY k",
      s"SELECT id, n + 1 AS v FROM $table LIMIT 5",
      s"SELECT c_des AS d FROM $table LIMIT 5 UNION ALL SELECT ts AS d FROM $table LIMIT 5",
      s"SELECT id FROM $table WHERE n IN (SELECT n FROM $table WHERE id = 't3') LIMIT 5"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val described: ElasticResult[Option[ResultSchema]] = client.describe(sql)
        described shouldBe ElasticSuccess(answer(runOk(sql))._2)
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // SCHEMA-2b -- Table A (AC3)
  // -------------------------------------------------------------------------------------------

  private def byKey(sql: String, key: String): Map[Any, ListMap[String, Any]] =
    rowsOf(sql).map(r => r.getOrElse(key, null) -> r).toMap

  "Table A" should "give the aggregation rows' answers (AC3)" in {
    // 20a
    byKey(s"SELECT b, COUNT(*) AS c FROM $aggTable GROUP BY b", "b").keySet shouldBe
    Set[Any](java.lang.Boolean.TRUE)
    // 20b: a custom-format date key from its epoch millis, at Z
    val dc = byKey(s"SELECT dc, COUNT(*) AS c FROM $aggTable GROUP BY dc", "dc").keySet
    dc shouldBe Set[Any](utc(2024, 1, 31), utc(2024, 2, 1))
    dc.foreach(v => v.asInstanceOf[ZonedDateTime].getZone shouldBe ZoneOffset.UTC)
    // 20c: BIGINT always a Long
    val big = byKey(s"SELECT big, COUNT(*) AS c FROM $aggTable GROUP BY big", "big").keySet
    big shouldBe Set[Any](java.lang.Long.valueOf(42L), java.lang.Long.valueOf(1706697000000L))
    big.map(_.getClass.getName) shouldBe Set("java.lang.Long")
    // 20d (A-25): 10:30 keeps main's reading (a counted residual), the whole day is a LocalDate
    val cDes = byKey(s"SELECT c_des, COUNT(*) AS c FROM $aggTable GROUP BY c_des", "c_des").keySet
    cDes should contain(LocalDate.of(2024, 1, 31))
    cDes.filterNot(_ == LocalDate.of(2024, 1, 31)).map(_.getClass.getName) shouldBe
    Set("java.lang.String")
    // 20e / 20f
    byKey(s"SELECT n, COUNT(*) AS c FROM $aggTable GROUP BY n", "n").keySet shouldBe
    Set[Any](Integer.valueOf(5), Integer.valueOf(10))
    byKey(s"SELECT k, COUNT(*) AS c FROM $aggTable GROUP BY k", "k").keySet shouldBe Set[Any](
      "10:30"
    )
    // 21a-21d
    val metrics = one(
      s"SELECT MAX(dc) AS d, SUM(n) AS s, MAX(n) AS m, COUNT(*) AS c, COUNT(n) AS cn FROM $aggTable"
    )
    metrics("d") shouldBe utc(2024, 2, 1)
    metrics("d").asInstanceOf[ZonedDateTime].getZone shouldBe ZoneOffset.UTC
    metrics("s") shouldBe java.lang.Long.valueOf(15L)
    metrics("m") shouldBe Integer.valueOf(10)
    metrics("c") shouldBe java.lang.Long.valueOf(3L)
    metrics("cn") shouldBe java.lang.Long.valueOf(2L)
  }

  // -------------------------------------------------------------------------------------------
  // SCHEMA-2b -- aggregation classes (AC2)
  // -------------------------------------------------------------------------------------------

  /** The aggregation routes (AC2, B13): one-shot `search`, `run` (`searchAsync`), the `scroll()`
    * API (classic scroll), and the `_msearch` legs of a UNION ALL (aggregation legs never take the
    * per-leg route).
    */
  protected def onAggregationRoute(
    route: String,
    sql: String
  ): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    route match {
      case "search" =>
        client.search(SelectStatement(sql))(NativeContext) match {
          case ElasticSuccess(r)     => (r.results, r.schema)
          case ElasticFailure(error) => fail(s"[$route] $sql: ${error.message}")
        }
      case "searchAsync" => answer(runOk(sql))
      case "scroll" =>
        val (schema, source) = client.scrollWithSchema(SelectStatement(sql))(system, NativeContext)
        (Await.result(source.map(_._1).runWith(Sink.seq), 60.seconds), schema)
      case "msearch" => answer(runOk(s"$sql UNION ALL $sql"))
    }

  protected val aggregationRoutes: Seq[String] = Seq("search", "searchAsync", "scroll", "msearch")

  "Every aggregation route" should "answer every value of a typed column in D2's class (AC2)" in {
    // residuals excluded by construction: no inexact DATE key, no ARRAY_AGG column (A-21), no
    // statistics map
    Seq(
      s"SELECT b, COUNT(*) AS c FROM $aggTable GROUP BY b",
      s"SELECT dc, COUNT(*) AS c, MAX(n) AS m FROM $aggTable GROUP BY dc",
      s"SELECT big, SUM(n) AS s FROM $aggTable GROUP BY big",
      s"SELECT n, MIN(x) AS mx, MAX(r) AS mr FROM $aggTable GROUP BY n",
      s"SELECT k, COUNT(n) AS c, SUM(n) AS s, AVG(n) AS a, MAX(dc) AS d, MAX(b) AS mb FROM $aggTable GROUP BY k",
      s"SELECT k, b, COUNT(*) AS c FROM $aggTable GROUP BY k, b",
      s"SELECT COUNT(*) AS c, SUM(n) AS s, MAX(n) AS m, MIN(big) AS mb, MAX(dc) AS d FROM $aggTable",
      s"SELECT COUNT(*) AS c, SUM(n) AS s, MIN(n) AS m FROM $aggTable WHERE id = 'none'",
      s"SELECT k, SUM(n) AS s FROM $aggTable GROUP BY k HAVING SUM(n) > 1",
      s"SELECT k, MAX(n) - MIN(n) AS d, SUM(n) + 1 AS s1 FROM $aggTable GROUP BY k",
      // Case 4: hits beside global metrics
      s"SELECT id, COUNT(*) OVER () AS c, MAX(pt) OVER () AS m FROM $windowTable LIMIT 5"
    ).foreach { sql =>
      aggregationRoutes.foreach { route =>
        withClue(s"[$route] [$sql] ") {
          val (rows, schema) = onAggregationRoute(route, sql)
          rows should not be empty
          classMisses(rows, schema.getOrElse(fail("no schema"))) shouldBe empty
        }
      }
    }
  }

  it should "read top_hits elements canonically (ARRAY_AGG DISTINCT over a date)" in {
    val row = one(s"SELECT ARRAY_AGG(DISTINCT dc) AS a FROM $aggTable")
    row("a") match {
      case s: Seq[_] =>
        // a3 holds no `dc`: its element is NULL, as SQL keeps it -- DuckDB 1.5.5 `SELECT
        // array_agg(DISTINCT x) FROM (VALUES (DATE '2024-01-31'), (NULL), (DATE '2024-02-01'),
        // (DATE '2024-01-31')) t(x)` = [NULL, 2024-01-31, 2024-02-01]; 0.24.0 answers that NULL too
        s.toSet shouldBe Set[Any](utc(2024, 1, 31), utc(2024, 2, 1), null)
        s.filter(_ != null).foreach(_ shouldBe a[ZonedDateTime])
      case other => fail(s"ARRAY_AGG answered $other")
    }
  }

  // -------------------------------------------------------------------------------------------
  // SCHEMA-2b -- the window oracle (AC4)
  // -------------------------------------------------------------------------------------------

  private def windowSql(p: String, selected: Boolean = true, head: String = "id, n"): String =
    s"SELECT $head${if (selected) s", $p" else ""}, " +
    s"ROW_NUMBER() OVER (PARTITION BY $p ORDER BY id) AS rn, " +
    s"SUM(n) OVER (PARTITION BY $p) AS s, " +
    s"FIRST_VALUE(n) OVER (PARTITION BY $p ORDER BY id) AS f FROM $windowTable"

  /** The window routes: one-shot sync (LIMIT 100), async (`run`, LIMIT 100), scroll (no LIMIT). */
  private def onWindowRoute(route: String, sql: String): Seq[ListMap[String, Any]] =
    route match {
      case "search" =>
        client.search(SelectStatement(s"$sql LIMIT 100"))(NativeContext) match {
          case ElasticSuccess(r)     => r.results
          case ElasticFailure(error) => fail(s"[$route] $sql: ${error.message}")
        }
      case "searchAsync" => answer(runOk(s"$sql LIMIT 100"))._1
      case "scroll"      => answer(runOk(sql))._1
    }

  /** The fixture's arithmetic: id -> (rn, s, f), `null` where the partition is not answered. */
  private type Oracle = Map[String, (Any, Any, Any)]

  private def answered(rn: Long, s: Long, f: Int): (Any, Any, Any) =
    (java.lang.Long.valueOf(rn), java.lang.Long.valueOf(s), Integer.valueOf(f))

  private val none: (Any, Any, Any) = (null, null, null)

  // {w1, w2} share a partition and w3 has its own: n 1 + 2 = 3, and 4
  private val pairs: Oracle =
    Map("w1" -> answered(1, 3, 1), "w2" -> answered(2, 3, 1), "w3" -> answered(1, 4, 4))

  private def oracles: Seq[(String, Oracle)] = Seq(
    "pk1" -> pairs,
    "pk2" -> pairs,
    // w1 / w2 hold the same millisecond (w1's micros truncated as Elasticsearch indexes them)
    "pt" -> pairs,
    "pb" -> pairs,
    "pn" -> pairs,
    // only w1 is exactly a day (A-23): w2 (10:30) and w3 (14:00) answer as main does (NULL)
    "pd" -> Map("w1" -> answered(1, 1, 1), "w2" -> none, "w3" -> none),
    // w1 / w3 at 00:00 share the day; w2 at 10:30 pairs as on main (equal instants)
    "pe" -> Map("w1" -> answered(1, 5, 1), "w2" -> answered(1, 2, 2), "w3" -> answered(2, 5, 1))
  ) ++ (
    // read as stored, partitioned by instant, as Elasticsearch buckets a date: w1 2024-01-31
    // 10:30 UTC = w2 11:30 CET (CET = UTC+1 in January) = 1706697000000 ms, one bucket {w1, w2}
    // (n 1 + 2); w3 2024-02-01 00:00 UTC alone (n 4)
    if (zoneNames) Seq("pz" -> pairs) else Nil
  )

  private def assertOracle(route: String, sql: String, oracle: Oracle, columns: Seq[String]): Unit =
    withClue(s"[$route] [$sql] ") {
      val rows = onWindowRoute(route, sql)
      rows.map(_.keys.toSeq).distinct shouldBe Seq(columns)
      val byId = rows.map(r => r("id").toString -> r).toMap
      byId.keySet shouldBe oracle.keySet
      oracle.foreach { case (id, (rn, s, f)) =>
        withClue(s"$id ") {
          val row = byId(id)
          (row("rn"), row("s"), row("f")) shouldBe ((rn, s, f))
        }
      }
    }

  "The window oracle" should "answer every partition column by the fixture's arithmetic (AC4)" in {
    for {
      (p, oracle) <- oracles
      route       <- Seq("search", "searchAsync", "scroll")
    } assertOracle(route, windowSql(p), oracle, Seq("id", "n", p, "rn", "s", "f"))
  }

  it should "answer an unselected partition column only where the whole document is fetched (B10)" in {
    Seq("search", "searchAsync", "scroll").foreach { route =>
      // its `_source` is not fetched: NULL, as on main (recorded, not fixed)
      assertOracle(
        route,
        windowSql("pu", selected = false),
        Map("w1" -> none, "w2" -> none, "w3" -> none),
        Seq("id", "n", "rn", "s", "f")
      )
    }
    Seq("search", "searchAsync", "scroll").foreach { route =>
      // a computed column alone fetches the whole `_source`: answered, the carried column dropped
      withClue(s"[$route] UPPER(id) ") {
        val rows = onWindowRoute(route, windowSql("pu", selected = false, head = "UPPER(id) AS u"))
        rows.map(_.keys.toSeq).distinct shouldBe Seq(Seq("u", "rn", "s", "f"))
        rows.map(r => r("u").toString -> r("s")).toMap shouldBe Map(
          "W1" -> java.lang.Long.valueOf(3L),
          "W2" -> java.lang.Long.valueOf(3L),
          "W3" -> java.lang.Long.valueOf(4L)
        )
      }
      // `*` fetches it too
      withClue(s"[$route] * ") {
        val rows = onWindowRoute(route, windowSql("pu", selected = false, head = "*"))
        rows.map(r => r("id").toString -> r("s")).toMap shouldBe Map(
          "w1" -> java.lang.Long.valueOf(3L),
          "w2" -> java.lang.Long.valueOf(3L),
          "w3" -> java.lang.Long.valueOf(4L)
        )
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // SCHEMA-2b -- describe = run's schema (AC1)
  // -------------------------------------------------------------------------------------------

  "describe" should "answer run's schema over aggregate, top_hits, Case 4 and window statements (AC1)" in {
    Seq(
      s"SELECT b, COUNT(*) AS c FROM $aggTable GROUP BY b",
      s"SELECT dc, SUM(n) AS s, MAX(n) AS m, MAX(dc) AS d FROM $aggTable GROUP BY dc",
      s"SELECT COUNT(*) AS c, SUM(n) AS s, SUM(x) AS sx, SUM(r) AS sr FROM $aggTable",
      s"SELECT k, MAX(n) - MIN(n) AS d, SUM(n) + 1 AS s1 FROM $aggTable GROUP BY k",
      s"SELECT k, FIRST(dc) AS f FROM $aggTable GROUP BY k",
      s"SELECT id, COUNT(*) OVER () AS c, MAX(pt) OVER () AS m FROM $windowTable LIMIT 5",
      s"${windowSql("pb")} LIMIT 100",
      windowSql("pt")
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val described: ElasticResult[Option[ResultSchema]] = client.describe(sql)
        described shouldBe ElasticSuccess(answer(runOk(sql))._2)
      }
    }
  }
}
