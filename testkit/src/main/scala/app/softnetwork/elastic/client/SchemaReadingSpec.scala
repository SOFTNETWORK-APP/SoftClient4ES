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

  override def beforeAll(): Unit = {
    super.beforeAll()
    Seq(table, q4, q4d).foreach(drop)
    load(table, mapping, docs)
    ()
  }

  override def afterAll(): Unit = {
    Seq(table, q4, q4d).foreach(drop)
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
}
