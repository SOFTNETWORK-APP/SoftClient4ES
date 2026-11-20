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
  QueryStructured,
  SQLResult
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.persistence.generateUUID
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-17 (AC1-AC4, RUN) -- `describe(sql)` declares every column `run(sql)` answers, before any
  * row: the same names, types, fields and elements, at every depth; the row keys among the names;
  * every value of one class per column, by its declared type. Over:
  *
  *   - A1: a concrete index, an alias over one index, over two that agree, over two that disagree,
  *     a pattern, a comma list, a data stream past a rollover (7.9+);
  *   - A2: DDL through an alias over two indices answers as before a query warmed its entry;
  *   - A3: an unknown index (404 on both), a pattern matching none (an empty table, 0 rows), a list
  *     with a concrete part matching none (404 on both);
  *   - U1: `SELECT *` legs of a `UNION ALL`, after a declaring branch and after a `*` one;
  *   - S1: a list of objects declared in place over an `object` field (same index, same documents,
  *     still `object`), `SHOW CREATE TABLE` re-parsing; the same items over a `nested` field;
  *   - F3: the field `meta` core writes from 7.6, none below (CREATE and in-place ALTER succeed on
  *     every major).
  *
  * The one accepted residue, by name (R-146 Q2 (a), SCHEMA-17 fix R-150): a user who may not read
  * the mapping (typed from `_field_caps`), on every major, reads a several-valued cell of a leaf no
  * index declares to it -- no core meta, or a scalar declaration over several indices -- as the
  * `List` of its values, where `describe` declares the field's Elasticsearch type, one value. It is
  * pinned by the ES 8 read-only spec (`JavaClientReadOnlyUserSpec`), not here: this spec runs as
  * the privileged client, whose undeclared cell fails.
  */
trait DescribeRunParitySpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val mapper = new ObjectMapper()

  protected lazy val dataStreams: Boolean =
    Try(ElasticsearchVersion.supportsDataStreams(elasticVersion)).getOrElse(false)

  protected lazy val fieldMeta: Boolean =
    Try(ElasticsearchVersion.supportsFieldMeta(elasticVersion)).getOrElse(false)

  // not `a` / `an`: Matchers' words of those names would be overridden
  private val idxA = "p17x_a"
  private val idxB = "p17x_b"
  private val idxC = "p17c"
  private val dn = "p17dn"
  private val s = "p17s"
  private val sc = "p17sc"
  private val f = "p17f"
  private val g = "p17g"
  private val ds = "p17ds"
  private val al1 = "p17al1"
  private val al2 = "p17al2"
  private val alc = "p17alc"
  // R-169: created by another client after its 404 was remembered
  private val late = "p17late"

  private def indices: Seq[String] = Seq(idxA, idxB, idxC, dn, s, sc, f, g, late)

  private val abMapping =
    """{"_meta": {"columns": {"tags": {"data_type": "ARRAY<KEYWORD>"}}},
      | "properties": {
      |  "k": {"type": "keyword"}, "n": {"type": "integer"}, "x": {"type": "double"},
      |  "d": {"type": "date", "format": "dd/MM/yyyy"}, "ts": {"type": "date"},
      |  "tags": {"type": "keyword"},
      |  "obj": {"properties": {"a": {"type": "integer"}, "s": {"type": "keyword"}}},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}}
      |}}""".stripMargin

  private def raw(method: String, path: String, body: Option[String] = None): (Int, String) =
    Try {
      val request = new Request(method, path)
      body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
      val response = restClient.performRequest(request)
      (response.getStatusLine.getStatusCode, EntityUtils.toString(response.getEntity))
    }.recover { case e: org.elasticsearch.client.ResponseException =>
      (e.getResponse.getStatusLine.getStatusCode, EntityUtils.toString(e.getResponse.getEntity))
    }.get

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(client.deleteIndex(index))
    ()
  }

  private def load(index: String, mappingJson: String, documents: Seq[(String, String)]): Unit = {
    client
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mappingJson).get shouldBe true
    documents.foreach { case (id, source) =>
      client.index(index, id, source) match {
        case ElasticSuccess(true) =>
        case other                => fail(s"indexing $id into $index: $other")
      }
    }
    client.refresh(index)
    ()
  }

  private def alias(name: String, members: String*): Unit = {
    val actions = members.map(m => s"""{"add": {"index": "$m", "alias": "$name"}}""")
    raw("POST", "/_aliases", Some(actions.mkString("""{"actions": [""", ",", "]}")))._1 shouldBe 200
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    (indices :+ ds).foreach(drop)
    Try(raw("DELETE", s"/_data_stream/$ds"))
    Try(raw("DELETE", s"/_index_template/$ds"))
    load(
      idxA,
      abMapping,
      Seq(
        "a1" -> """{"k":"a1","n":1,"x":1.5,"d":"16/11/2026","ts":"2026-11-16T10:00:00Z","tags":["t1","t2"],"obj":{"a":1,"s":"s1"},"items":[{"p":1},{"p":2}]}""",
        "a2" -> """{"k":"a2","n":2,"tags":"t3"}"""
      )
    )
    load(idxB, abMapping, Seq("b1" -> """{"k":"b1","n":3,"x":2.5,"d":"17/11/2026"}"""))
    load(
      idxC,
      """{"properties": {"k": {"type": "keyword"}, "n": {"type": "long"}}}""",
      Seq("c1" -> """{"k":"c1","n":4}""")
    )
    load(
      dn,
      """{"properties": {"k": {"type": "keyword"}, "n": {"type": "date"}}}""",
      Seq("dn1" -> """{"k":"dn1","n":"2026-11-16T00:00:00Z"}""")
    )
    load(
      s,
      """{"properties": {"k": {"type": "keyword"},
        | "obj": {"properties": {"a": {"type": "integer"}, "s": {"type": "keyword"}}},
        | "items": {"type": "nested", "properties": {"p": {"type": "integer"}}}}}""".stripMargin,
      Seq(
        "s1" -> """{"k":"one","obj":{"a":1,"s":"x"},"items":[{"p":1}]}""",
        "s2" -> """{"k":"two","obj":[{"a":2,"s":"y"},{"a":3,"s":"z"}],"items":[{"p":2},{"p":3}]}""",
        "s3" -> """{"k":"empty","obj":[],"items":[]}""",
        "s4" -> """{"k":"none"}"""
      )
    )
    alias(al1, idxA)
    alias(al2, idxA, idxB)
    alias(alc, idxA, idxC)
    if (dataStreams) {
      raw(
        "PUT",
        s"/_index_template/$ds",
        Some(
          s"""{"index_patterns": ["$ds*"], "data_stream": {}, "priority": 500,
             | "template": {"mappings": {"properties": {"@timestamp": {"type": "date"},
             | "k": {"type": "keyword"}, "n": {"type": "integer"}}}}}""".stripMargin
        )
      )._1 shouldBe 200
      raw(
        "POST",
        s"/$ds/_doc?refresh=true",
        Some("""{"@timestamp":"2026-11-16T10:00:00Z","k":"d1","n":1}""")
      )._1 should (be(200) or be(201))
    }
  }

  override def afterAll(): Unit = {
    Seq(al1, al2, alc).foreach(name => Try(raw("DELETE", s"/_all/_alias/$name")))
    indices.foreach(drop)
    if (dataStreams) {
      Try(raw("DELETE", s"/_data_stream/$ds"))
      Try(raw("DELETE", s"/_index_template/$ds"))
    }
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------

  private def rowsOf(result: QueryResult): Seq[ListMap[String, Any]] =
    result match {
      case q: QueryRows       => q.rows
      case q: QueryStructured => q.response.results
      case q: QueryStream     => Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case other              => fail(s"unexpected result $other")
    }

  private def schemaOf(result: QueryResult): Option[ResultSchema] =
    result match {
      case q: QueryRows       => q.schema
      case q: QueryStructured => q.schema
      case q: QueryStream     => q.schema
      case other              => fail(s"unexpected result $other")
    }

  private def run(sql: String): ElasticResult[QueryResult] =
    Await.result(client.run(sql), 60.seconds)

  /** Whether `value` is of the ONE class the declared type `t` answers (null is every type's). */
  private def ofClass(t: SQLType, value: Any): Boolean =
    value == null || (t match {
      case SQLTypes.Any                                        => true
      case SQLTypes.Int | SQLTypes.SmallInt | SQLTypes.TinyInt => value.isInstanceOf[Integer]
      case SQLTypes.BigInt                                     => value.isInstanceOf[java.lang.Long]
      case SQLTypes.Double  => value.isInstanceOf[java.lang.Double]
      case SQLTypes.Real    => value.isInstanceOf[java.lang.Float]
      case SQLTypes.Boolean => value.isInstanceOf[java.lang.Boolean]
      case SQLTypes.Varchar => value.isInstanceOf[String]
      case SQLTypes.Date    => value.isInstanceOf[java.time.LocalDate]
      case SQLTypes.Time    => value.isInstanceOf[java.time.LocalTime]
      case SQLTypes.Timestamp =>
        value.isInstanceOf[java.time.ZonedDateTime] || value.isInstanceOf[java.time.Instant]
      case SQLTypes.Struct | SQLTypes.GeoPoint => value.isInstanceOf[scala.collection.Map[_, _]]
      case SQLTypes.Array(_)                   => value.isInstanceOf[Seq[_]]
      case _                                   => true
    })

  /** Every value of every row of the declared class, at every depth (evidence rule 3). */
  private def classesAgree(
    columns: Seq[ResultColumn],
    row: scala.collection.Map[String, Any]
  ): Unit =
    columns.foreach { column =>
      row.get(column.name).foreach { value =>
        withClue(s"column ${column.name} (${column.sqlType.typeId}) = $value: ") {
          ofClass(column.sqlType, value) shouldBe true
        }
        (column.sqlType, value) match {
          case (SQLTypes.Struct, m: scala.collection.Map[_, _]) if column.fields.nonEmpty =>
            classesAgree(column.fields, m.asInstanceOf[scala.collection.Map[String, Any]])
          case (SQLTypes.Array(_), list: Seq[_]) =>
            column.element.foreach { e =>
              list.foreach { v =>
                withClue(s"element of ${column.name} = $v: ")(ofClass(e.sqlType, v) shouldBe true)
              }
            }
          case _ =>
        }
      }
    }

  /** `describe(sql)` and `run(sql)`: both answer with ONE schema, the rows' keys among its names,
    * every value of its declared class; or both refuse with one status.
    */
  protected def parity(sql: String): Unit =
    withClue(s"[$sql] ") {
      (client.describe(sql), run(sql)) match {
        case (ElasticSuccess(Some(described)), ElasticSuccess(result)) =>
          schemaOf(result) shouldBe Some(described)
          described.columns.map(_.sqlType) should not contain SQLTypes.Numeric
          described.columns.map(_.sqlType) should not contain SQLTypes.Temporal
          rowsOf(result).foreach { row =>
            (row.keySet -- described.names.toSet) shouldBe empty
            classesAgree(described.columns, row)
          }
        case (ElasticFailure(d), ElasticFailure(r)) =>
          d.statusCode shouldBe r.statusCode
        // run answers a stream, which fails once drained: Elasticsearch's status, found on the
        // failure's cause chain, is describe's
        case (ElasticFailure(d), ElasticSuccess(result)) =>
          Try(rowsOf(result)) match {
            case scala.util.Failure(t) => d.statusCode shouldBe statusOf(t)
            case scala.util.Success(rows) =>
              fail(s"describe refused (${d.statusCode}: ${d.message}), run answered $rows")
          }
        case other => fail(s"describe and run disagree: $other")
      }
    }

  /** The HTTP status a failure carries, on its cause chain (at most 8 levels deep). */
  private def statusOf(t: Throwable): Option[Int] = {
    var cur: Throwable = t
    var status: Option[Int] = None
    var depth = 0
    while (cur != null && status.isEmpty && depth < 8) {
      status = Try(client.statusOf(cur)).toOption.flatten
      cur = cur.getCause
      depth += 1
    }
    status
  }

  private def described(sql: String): ResultSchema =
    client.describe(sql) match {
      case ElasticSuccess(Some(schema)) => schema
      case other                        => fail(s"describe [$sql]: $other")
    }

  private def items(source: String): Seq[String] = Seq(
    s"SELECT * FROM $source",
    s"SELECT k, n FROM $source",
    s"SELECT k, n + 1 AS n1 FROM $source",
    s"SELECT k, COUNT(*) AS c FROM $source GROUP BY k"
  )

  // -------------------------------------------------------------------------------------------
  // A1 / A2 -- aliases and data streams over several indices (A-161, A-162)
  // -------------------------------------------------------------------------------------------

  "describe" should "declare what run answers over a concrete index, an alias, a pattern and a list (A1)" in {
    Seq(idxA, al1, al2, alc, "p17x_*", s"$idxA,$idxB", s"$idxA,$idxC")
      .flatMap(items)
      .foreach(parity)
    parity(s"SELECT k, d FROM $al2 WHERE d = '16/11/2026'")
    // the alias over two indices is typed as the comma list of its members
    described(s"SELECT * FROM $al2") shouldBe described(s"SELECT * FROM $idxA,$idxB")
    described(s"SELECT * FROM $alc") shouldBe described(s"SELECT * FROM $idxA,$idxC")
  }

  it should "declare a data stream past a rollover, cached or read again (A1, 7.9+)" in {
    assume(dataStreams, "data streams need Elasticsearch 7.9")
    items(ds).foreach(parity)
    raw("POST", s"/$ds/_rollover")._1 shouldBe 200
    raw(
      "POST",
      s"/$ds/_doc?refresh=true",
      Some("""{"@timestamp":"2026-11-17T10:00:00Z","k":"d2","n":2}""")
    )._1 should (be(200) or be(201))
    // the entry cached before the rollover
    items(ds).foreach(parity)
    // read again: two backing indices, merged
    client.invalidateAllSchemas()
    items(ds).foreach(parity)
    rowsOf(run(s"SELECT k FROM $ds").get).map(_("k")).toSet shouldBe Set("d1", "d2")
  }

  it should "answer DDL through an alias over two indices as before a query warmed it (A2)" in {
    client.invalidateAllSchemas()
    val ddl = s"ALTER TABLE $al2 ALTER COLUMN k SET COMMENT 'c'"
    val cold = run(ddl)
    described(s"SELECT * FROM $al2")
    val warm = run(ddl)
    (cold, warm) match {
      case (ElasticFailure(x), ElasticFailure(y)) =>
        y.statusCode shouldBe x.statusCode
        y.message shouldBe x.message
      case (ElasticSuccess(_), ElasticSuccess(_)) =>
      case other => fail(s"DDL through the alias moved once a query warmed it: $other")
    }
  }

  // -------------------------------------------------------------------------------------------
  // A3 -- no schema found (A-163)
  // -------------------------------------------------------------------------------------------

  it should "answer what run answers where no index matches (A3)" in {
    parity("SELECT k FROM p17nowhere")
    client.describe("SELECT k FROM p17nowhere") match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(404)
      case other                 => fail(s"expected a 404, got $other")
    }
    parity("SELECT * FROM p17nomatch_*")
    parity("SELECT k FROM p17nomatch_*")
    described("SELECT * FROM p17nomatch_*") shouldBe ResultSchema(Nil)
    rowsOf(run("SELECT * FROM p17nomatch_*").get) shouldBe empty
    parity("SELECT * FROM p17nomatch_*,p17gone")
  }

  // -------------------------------------------------------------------------------------------
  // R-169 -- a remembered 404: describe looks the source up again, run keeps its negative cache
  // -------------------------------------------------------------------------------------------

  @scala.annotation.tailrec
  private def baseClientOf(c: ElasticClientApi): ElasticClientApi = c match {
    case delegator: ElasticClientDelegator => baseClientOf(delegator.delegate)
    case other                             => other
  }

  /** Whether `run`'s resolution attaches a schema with columns (on the base client, whose map of
    * remembered misses it reads); false while a miss stands, since `run` makes no lookup then.
    */
  private def typedByRun(sql: String): Boolean =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        baseClientOf(client).resolveWithSchema(single) match {
          case ElasticSuccess(sent) => sent.schema.exists(_.columns.nonEmpty)
          case other                => fail(s"run's resolution of [$sql]: $other")
        }
      case other => fail(s"[$sql] is not a single search: $other")
    }

  it should "type an index another client created after its 404 was remembered, as run then answers (R-169)" in {
    val sql = s"SELECT k, n FROM $late"
    // the 404, remembered by the lookup
    client.describe(sql) match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(404)
      case other                 => fail(s"expected the 404, got $other")
    }
    // another client creates it: an ingest that auto-creates the index, no DDL through this client
    val created = raw("POST", s"/$late/_doc?refresh=true", Some("""{"k":"l1","n":7}"""))._1
    created should (be(200) or be(201))
    // run, while the miss stands, makes no lookup: untyped
    typedByRun(sql) shouldBe false
    // describe looks it up again, by text and by statement
    val declared = described(sql)
    declared.columns.map(_.name) shouldBe Seq("k", "n")
    declared.columns.find(_.name == "n").map(_.sqlType) shouldBe Some(SQLTypes.BigInt)
    Parser(sql) match {
      case Right(statement) => client.describe(statement) shouldBe ElasticSuccess(Some(declared))
      case Left(error)      => fail(s"[$sql] does not parse: ${error.msg}")
    }
    // its success forgot the miss: run is typed, and answers what describe declared
    typedByRun(sql) shouldBe true
    parity(sql)
  }

  it should "type an alias, a pattern and a list whose miss is remembered over their cached schema (R-169)" in {
    Seq(al1, al2, "p17x_*", s"$idxA,$idxC").foreach { source =>
      val sql = s"SELECT k, n FROM $source"
      withClue(s"[$sql] ") {
        val expected = described(sql)
        baseClientOf(client).rememberSchemaMiss(source)
        typedByRun(sql) shouldBe false
        described(sql) shouldBe expected
        typedByRun(sql) shouldBe true
        parity(sql)
      }
    }
  }

  it should "keep a source still absent a 404 on every describe, and a pattern matching none an empty table (R-169)" in {
    val gone = "p17never"
    (1 to 2).foreach { _ =>
      client.describe(s"SELECT k FROM $gone") match {
        case ElasticFailure(error) =>
          error.statusCode shouldBe Some(404)
          error.operation shouldBe Some("describe")
          error.message shouldBe s"no such index [$gone]"
        case other => fail(s"expected the 404, got $other")
      }
    }
    parity(s"SELECT k FROM $gone")
    baseClientOf(client).rememberSchemaMiss("p17nomatch_*")
    described("SELECT * FROM p17nomatch_*") shouldBe ResultSchema(Nil)
    parity("SELECT * FROM p17nomatch_*")
  }

  // -------------------------------------------------------------------------------------------
  // U1 -- SELECT * legs of a UNION ALL (A-165)
  // -------------------------------------------------------------------------------------------

  it should "declare the SELECT * legs of a UNION ALL as their rows are built (U1)" in {
    Seq(
      s"SELECT k, n FROM $idxA UNION ALL SELECT * FROM $idxC",
      s"SELECT * FROM $idxA UNION ALL SELECT * FROM $idxB",
      s"SELECT * FROM $idxA UNION ALL SELECT * FROM $idxC",
      s"SELECT * FROM $idxA UNION ALL SELECT k AS n2 FROM $idxB",
      // a date beside a number under one name: refused by both
      s"SELECT * FROM $idxA UNION ALL SELECT * FROM $dn"
    ).foreach(parity)
    described(s"SELECT * FROM $idxA UNION ALL SELECT * FROM $idxB") shouldBe described(
      s"SELECT * FROM $idxA"
    )
    client.describe(s"SELECT * FROM $idxA UNION ALL SELECT * FROM $dn") match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(400)
      case other                 => fail(s"expected the 400, got $other")
    }
  }

  // -------------------------------------------------------------------------------------------
  // S1 -- a list of objects declared in place over an `object` field (A-166 .. A-168)
  // -------------------------------------------------------------------------------------------

  private def uuidOf(index: String): String =
    mapper
      .readTree(raw("GET", s"/$index/_settings")._2)
      .path(index)
      .path("settings")
      .path("index")
      .path("uuid")
      .asText()

  private def mappingOf(index: String): JsonNode = {
    val root = mapper.readTree(raw("GET", s"/$index/_mapping")._2).path(index).path("mappings")
    if (root.has("_doc")) root.path("_doc") else root
  }

  /** `sql`'s failure -- refused, or a stream failing once drained --: its status (on the cause
    * chain) and every message of the chain.
    */
  private def failureOf(sql: String): (Option[Int], String) =
    run(sql) match {
      case ElasticFailure(error) => (error.statusCode, error.message)
      case ElasticSuccess(result) =>
        Try(rowsOf(result)) match {
          case scala.util.Failure(t) =>
            val chain = Iterator
              .iterate(t)(_.getCause)
              .takeWhile(_ != null)
              .take(8)
              .map(c => String.valueOf(c.getMessage))
            (statusOf(t), chain.mkString(" || "))
          case scala.util.Success(rows) => fail(s"[$sql] answered $rows instead of a failure")
        }
    }

  /** `path`'s cell per document key `k`, NULL where the row holds none. */
  private def cells(sql: String, path: String): Map[Any, Any] =
    rowsOf(run(sql).get).map(r => r("k") -> r.getOrElse(path, null)).toMap

  it should "declare an object field a list in place, and read it as one (S1)" in {
    val listAlter = s"ALTER TABLE $s ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>"
    val unnestObj = s"SELECT t.k, o.a FROM $s AS t JOIN UNNEST(t.obj) AS o"
    // undeclared: a cell of two objects fails, naming the ALTER that declares the list -- read
    // whole, and through a path its objects hold, whose several values come from the list
    // (SCHEMA-17 A-181), never the path's own SET FIELD
    parity(s"SELECT k FROM $s WHERE k = 'one'")
    Seq(s"SELECT obj FROM $s", s"SELECT k, obj.a FROM $s").foreach { sql =>
      withClue(s"[$sql] ") {
        val (_, message) = failureOf(sql)
        message should include(listAlter)
        message should not include "SET FIELD"
      }
    }
    // a path under a nested field is a list already (A-181): one per document
    parity(s"SELECT k, items.p FROM $s")
    // UNNEST over the object field: Elasticsearch's nested refusal, as on main (A-167)
    val (unnestStatus, unnestMessage) = failureOf(unnestObj)
    unnestStatus shouldBe Some(400)
    unnestMessage should include("nested")
    val uuid = uuidOf(s)
    val count = rowsOf(run(s"SELECT k FROM $s").get).size
    run(listAlter) match {
      case ElasticSuccess(_) =>
      case other             => fail(s"the in-place ALTER failed: $other")
    }
    uuidOf(s) shouldBe uuid
    rowsOf(run(s"SELECT k FROM $s").get).size shouldBe count
    mappingOf(s).path("properties").path("obj").path("type").asText("object") shouldBe "object"
    described(s"SELECT obj FROM $s").columns.head.sqlType shouldBe SQLTypes.Array(SQLTypes.Struct)
    Seq(
      s"SELECT * FROM $s",
      s"SELECT k, obj FROM $s",
      s"SELECT k, obj.a FROM $s",
      s"SELECT k, obj, obj.a FROM $s",
      s"SELECT k FROM $s WHERE obj.a = 2",
      s"SELECT obj.a AS a, COUNT(*) AS c FROM $s GROUP BY obj.a",
      s"SELECT k, items FROM $s",
      s"SELECT k, items.p FROM $s",
      s"SELECT k FROM $s WHERE items.p = 2",
      s"SELECT t.k, i.p FROM $s AS t JOIN UNNEST(t.items) AS i"
    ).foreach(parity)
    // one object is a list of one
    rowsOf(run(s"SELECT k, obj FROM $s").get)
      .find(_("k") == "one")
      .map(_("obj")) match {
      case Some(list: Seq[_]) => list.size shouldBe 1
      case other              => fail(s"one object: $other")
    }
    // A-181: a path under a list of objects holds its objects' values, a list -- `object` and
    // `nested` alike, read whole beside it or not; one object a list of one, none NULL
    Seq(
      ("obj.a", s"SELECT k, obj.a FROM $s"),
      ("obj.a", s"SELECT k, obj, obj.a FROM $s"),
      ("items.p", s"SELECT k, items.p FROM $s")
    ).foreach { case (path, sql) =>
      withClue(s"[$sql] ") {
        described(sql).columns.find(_.name == path).map(_.sqlType) shouldBe
        Some(SQLTypes.Array(SQLTypes.Int))
        val byK = cells(sql, path)
        byK("one") shouldBe List(1)
        byK("two") shouldBe List(2, 3)
        byK("empty") shouldBe (null: Any)
        byK("none") shouldBe (null: Any)
      }
    }
    // a GROUP BY key and a filter read each value: one INT bucket per value, the document holding
    // it; an UNNEST element field stays one value
    val grouped = s"SELECT obj.a AS a, COUNT(*) AS c FROM $s GROUP BY obj.a"
    described(grouped).columns.map(_.sqlType) shouldBe Seq(SQLTypes.Int, SQLTypes.BigInt)
    rowsOf(run(grouped).get).map(_("a")).toSet shouldBe Set(1, 2, 3)
    rowsOf(run(s"SELECT k FROM $s WHERE obj.a = 2").get).map(_("k")) shouldBe Seq("two")
    described(s"SELECT t.k, i.p FROM $s AS t JOIN UNNEST(t.items) AS i").columns
      .find(_.name == "i.p")
      .map(_.sqlType) shouldBe Some(SQLTypes.Int)
    // UNNEST over the list of objects declared over `object`: still Elasticsearch's nested refusal
    // (A-167: the field's real type decides)
    val (declaredStatus, declaredMessage) = failureOf(unnestObj)
    declaredStatus shouldBe Some(400)
    declaredMessage should include("nested")
    // a copy writes the SQL type's own mapping, `nested` (A-167), and reads its rows back as lists
    run(s"CREATE TABLE $sc AS SELECT k, obj FROM $s") match {
      case ElasticSuccess(_) =>
      case other             => fail(s"CREATE TABLE AS SELECT: $other")
    }
    client.refresh(sc)
    mappingOf(sc).path("properties").path("obj").path("type").asText() shouldBe "nested"
    parity(s"SELECT k, obj FROM $sc")
    val copied = cells(s"SELECT k, obj FROM $sc", "obj")
    Seq("one" -> 1, "two" -> 2).foreach { case (k, size) =>
      copied(k) match {
        case list: Seq[_] => withClue(k)(list.size shouldBe size)
        case other        => fail(s"$k: $other in the copy")
      }
    }
    // SHOW CREATE TABLE renders the option, and re-parses
    val text = run(s"SHOW CREATE TABLE $s") match {
      case ElasticSuccess(SQLResult(sql)) => sql
      case other                          => fail(s"SHOW CREATE TABLE: $other")
    }
    // the renderer quotes an option's text with double quotes, as every other option it renders
    text should include(""") OPTIONS (type = "object")""")
    Parser(text).isRight shouldBe true
  }

  it should "read a point as one point, and a path beside its selected prefix, under a list of objects (S1)" in {
    load(
      g,
      """{"properties": {"k": {"type": "keyword"},
        | "obj": {"properties": {"loc": {"type": "geo_point"},
        |   "in": {"properties": {"q": {"type": "integer"}}}}},
        | "items": {"type": "nested", "properties": {"loc": {"type": "geo_point"},
        |   "o": {"properties": {"x": {"type": "integer"}}}}}}}""".stripMargin,
      Seq(
        "g1" -> """{"k":"one","obj":{"loc":[1.0,2.0],"in":{"q":1}},"items":{"loc":[1.0,2.0],"o":{"x":1}}}""",
        "g2" -> """{"k":"listone","obj":[{"loc":[1.0,2.0],"in":{"q":1}}],"items":[{"loc":[1.0,2.0],"o":{"x":1}}]}""",
        "g3" -> ("""{"k":"two","obj":[{"loc":[1.0,2.0],"in":{"q":2}},{"loc":[3.0,4.0],"in":{"q":3}}],""" +
        """"items":[{"loc":[1.0,2.0],"o":{"x":2}},{"loc":[3.0,4.0],"o":{"x":3}}]}"""),
        "g4" -> """{"k":"pair","obj":[{"loc":[[1.0,2.0],[3.0,4.0]]}],"items":[{"loc":[[1.0,2.0],[3.0,4.0]]}]}""",
        "g5" -> ("""{"k":"mixed","obj":[{"loc":"2.0,1.0"},{"loc":{"lat":4.0,"lon":3.0}}],""" +
        """"items":[{"loc":"2.0,1.0"},{"loc":{"lat":4.0,"lon":3.0}}]}""")
      )
    )
    run(s"ALTER TABLE $g ALTER COLUMN obj SET DATA TYPE ARRAY<STRUCT>") match {
      case ElasticSuccess(_) =>
      case other             => fail(s"the in-place ALTER failed: $other")
    }
    // A-181: a point stored as [lon, lat] under a list of objects is ONE point, in every spelling,
    // `nested` and declared over `object` alike, read whole beside it or not. A parent read whole
    // reads one element's own field one value, so `pair` (two points in one element) is read by
    // the path alone
    val p12 = ListMap("lat" -> 2.0, "lon" -> 1.0)
    val p34 = ListMap("lat" -> 4.0, "lon" -> 3.0)
    val points: Map[Any, Any] = Map(
      "one"     -> List(p12),
      "listone" -> List(p12),
      "two"     -> List(p12, p34),
      "pair"    -> List(p12, p34),
      "mixed"   -> List(p12, p34)
    )
    Seq("items", "obj").foreach { parent =>
      val path = s"$parent.loc"
      Seq(
        s"SELECT k, $path FROM $g",
        s"SELECT k, $parent, $path FROM $g WHERE k <> 'pair'",
        s"SELECT *, $path FROM $g WHERE k <> 'pair'"
      ).foreach { sql =>
        withClue(s"[$sql] ") {
          parity(sql)
          described(sql).columns.find(_.name == path).map(_.sqlType) shouldBe
          Some(SQLTypes.Array(SQLTypes.GeoPoint))
          val byK = cells(sql, path)
          byK.keySet.foreach(k => withClue(s"$k: ")(byK(k) shouldBe points(k)))
          byK.size shouldBe (if (sql.contains("'pair'")) 4 else 5)
        }
      }
    }
    // A-181: a path selected beside its own prefix reads its objects' values, as alone
    Seq(("obj", "in", "q"), ("items", "o", "x")).foreach { case (parent, child, leaf) =>
      val prefix = s"$parent.$child"
      val path = s"$prefix.$leaf"
      Seq(
        s"SELECT k, $prefix, $path FROM $g",
        s"SELECT k, $parent, $prefix, $path FROM $g WHERE k <> 'pair'",
        s"SELECT k, $path FROM $g"
      ).foreach { sql =>
        withClue(s"[$sql] ") {
          parity(sql)
          described(sql).columns.find(_.name == path).map(_.sqlType) shouldBe
          Some(SQLTypes.Array(SQLTypes.Int))
          val byK = cells(sql, path)
          byK("one") shouldBe List(1)
          byK("listone") shouldBe List(1)
          byK("two") shouldBe List(2, 3)
          byK("mixed") shouldBe (null: Any)
          if (sql.contains(s"$prefix,")) {
            val objects = cells(sql, prefix)
            objects("one") shouldBe List(ListMap(leaf -> 1))
            objects("listone") shouldBe List(ListMap(leaf -> 1))
            objects("two") shouldBe List(ListMap(leaf -> 2), ListMap(leaf -> 3))
          }
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // F3 -- the field `meta` core writes (A-169, A-170)
  // -------------------------------------------------------------------------------------------

  it should "write core's field meta from 7.6, none below, CREATE and in-place ALTER alike (F3)" in {
    run(
      s"CREATE TABLE $f (id KEYWORD, tags ARRAY<KEYWORD>, d DATE OPTIONS (format = 'dd/MM/yyyy'), " +
      "tm TIME, lst ARRAY<STRUCT> FIELDS (p INT) OPTIONS (type = 'object'), n INT)"
    ) match {
      case ElasticSuccess(_) =>
      case other             => fail(s"CREATE TABLE: $other")
    }
    run(s"ALTER TABLE $f ALTER COLUMN n SET DATA TYPE ARRAY<INT>") match {
      case ElasticSuccess(_) =>
      case other             => fail(s"the in-place ALTER: $other")
    }
    val properties = mappingOf(f).path("properties")
    def meta(path: String*): JsonNode =
      path
        .foldLeft(properties: JsonNode)((node, name) =>
          if (node.has("properties")) node.path("properties").path(name)
          else node.path(name)
        )
        .path("meta")
    if (fieldMeta) {
      meta("tags").path("sql_type").asText().toLowerCase shouldBe "array<keyword>"
      meta("d").path("sql_format").asText() shouldBe "dd/MM/yyyy"
      meta("tm").path("sql_type").asText().toLowerCase shouldBe "time"
      meta("n").path("sql_type").asText().toLowerCase shouldBe "array<int>"
      meta("lst", "p").path("sql_list_depths").asText() shouldBe "1"
      properties.path("lst").has("meta") shouldBe false
    } else
      raw("GET", s"/$f/_mapping")._2 should not include "\"meta\""
    // core's keys never surface in SHOW CREATE TABLE
    run(s"SHOW CREATE TABLE $f") match {
      case ElasticSuccess(SQLResult(sql)) => sql should not include "sql_"
      case other                          => fail(s"SHOW CREATE TABLE: $other")
    }
  }
}
