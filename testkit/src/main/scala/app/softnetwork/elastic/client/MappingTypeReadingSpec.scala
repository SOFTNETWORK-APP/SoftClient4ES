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
import app.softnetwork.elastic.sql.Value
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{CreateTable, SelectStatement}
import app.softnetwork.elastic.sql.schema.copyableSettings
import app.softnetwork.persistence.generateUUID
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-7 -- the Elasticsearch mapping types core reads but never writes, on a real cluster (AC0,
  * AC1, AC2, AC4, AC5; P1, P2's RUN subset, A-60..A-62, A-65, A-72, A-73, A-75, R-56).
  *
  *   - `scaled_float` DOUBLE (`Double`), `half_float` REAL (`Float`), `ip` KEYWORD (`String`) on
  *     every major; from Elasticsearch 7 on `unsigned_long` BIGINT (`Long`, a value above 2^63-1
  *     kept as stored: `BigInteger`), `date_nanos` TIMESTAMP (a UTC `ZonedDateTime`, nanoseconds
  *     kept), `constant_keyword` KEYWORD (its value, read from its doc value), `flattened` ANY;
  *   - one class per column on every row of every hit route, `describe` = the reported types;
  *   - an index pattern whose indices disagree on a field (keyword "007", long 7) answers VARCHAR;
  *   - the write direction: ALTER keeps a foreign field's mapping type, a type change reindexes or
  *     is refused, `SHOW CREATE TABLE` re-creates the same mapping types, a `type` written in the
  *     DDL maps the field with it.
  *
  * Documents are indexed raw from Elasticsearch 7 on: the core client's `index` wraps an
  * `unsigned_long` above 2^63-1.
  */
trait MappingTypeReadingSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  protected lazy val major: Int = Try(elasticVersion.takeWhile(_ != '.').toInt).getOrElse(8)

  /** `unsigned_long` (7.10), `date_nanos` (7.0), `constant_keyword` (7.7), `flattened` (7.3). */
  protected lazy val modern: Boolean = major >= 7

  private val mapper = new ObjectMapper()

  protected val table = "s7_t"
  protected val p1 = "s7_p1"
  protected val p2 = "s7_p2"
  private val addIndex = "s7_a_add"
  private val ipIndex = "s7_a_ip"
  private val dnIndex = "s7_a_dn"
  private val ckIndex = "s7_a_ck"
  private val multiIndex = "s7_m"
  private val createdIndex = "s7_a_sc"
  private val copyIndex = "s7_a_sc_copy"
  private val ddlIndex = "s7_a_ddl"
  private val ddlCopyIndex = "s7_a_ddl_copy"
  private val orderDdlIndex = "s7_o1"
  private val orderBareIndex = "s7_o2"

  private def all: Seq[String] =
    Seq(table, p1, p2, addIndex, ipIndex, dnIndex, ckIndex, multiIndex, createdIndex, copyIndex) ++
    Seq(ddlIndex, ddlCopyIndex, orderDdlIndex, orderBareIndex)

  /** The typed fields of this major, with their mappings. */
  protected def typed(flattened: Boolean): Seq[(String, String)] =
    Seq(
      "sf" -> """{"type":"scaled_float","scaling_factor":100}""",
      "hf" -> """{"type":"half_float"}""",
      "ip" -> """{"type":"ip"}"""
    ) ++ (if (modern)
            Seq(
              "u"  -> """{"type":"unsigned_long"}""",
              "dn" -> """{"type":"date_nanos"}""",
              "ck" -> """{"type":"constant_keyword","value":"C"}"""
            ) ++ (if (flattened) Seq("fl" -> """{"type":"flattened"}""") else Nil)
          else Nil)

  protected def mapping(flattened: Boolean = true): String =
    s"""{"properties": {"id": {"type":"keyword"}, "k": {"type":"keyword"}, "d": {"type":"date"},
       | ${typed(flattened).map { case (c, m) => s""""$c": $m""" }.mkString(", ")}}}""".stripMargin

  protected def docs: Seq[(String, String)] = Seq(
    "r1" -> (s"""{"id":"r1","k":"a","d":"2024-01-31T10:30:00Z","sf":1.25,"hf":0.5,"ip":"10.0.0.1"""" +
    (if (modern)
       ""","u":18446744073709551615,"dn":"2024-01-31T10:30:00.123456789Z","fl":{"a":"x"}"""
     else "") + "}"),
    "r2" -> (s"""{"id":"r2","k":"b","d":"2024-02-01T00:00:00Z","sf":2.5,"hf":1.5,"ip":"10.0.0.2"""" +
    (if (modern) ""","u":7,"dn":"2024-02-01T00:00:00Z","fl":{"a":"z"}""" else "") + "}")
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
    put(index, documents)
  }

  private def put(index: String, documents: Seq[(String, String)]): Unit = {
    documents.foreach { case (id, source) =>
      if (modern) {
        val request = new Request("PUT", s"/$index/_doc/$id")
        request.setEntity(new NStringEntity(source, ContentType.APPLICATION_JSON))
        restClient.performRequest(request)
      } else
        client.index(index, id, source) match {
          case ElasticSuccess(true) =>
          case other                => fail(s"indexing $id into $index: $other")
        }
    }
    client.refresh(index)
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    all.foreach(drop)
    load(table, mapping(), docs)
    load(p1, """{"properties": {"k": {"type":"keyword"}}}""", Seq("p1" -> """{"k":"007"}"""))
    load(p2, """{"properties": {"k": {"type":"long"}}}""", Seq("p2" -> """{"k":7}"""))
    Seq(addIndex, ipIndex, dnIndex, ckIndex).foreach(load(_, mapping(), docs))
    load(createdIndex, mapping(flattened = false), docs)
    load(
      multiIndex,
      """{"properties": {"id": {"type":"keyword"}, "ip": {"type":"ip"}}}""",
      Seq(
        "m1" -> """{"id":"m1","ip":["10.0.0.1","10.0.0.2"]}""",
        "m2" -> """{"id":"m2","ip":"10.0.0.3"}"""
      )
    )
  }

  override def afterAll(): Unit = {
    all.foreach(drop)
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // Results
  // -------------------------------------------------------------------------------------------

  private def answer(result: QueryResult): Seq[ListMap[String, Any]] =
    result match {
      case q: QueryRows       => q.rows
      case q: QueryStructured => q.response.results
      case q: QueryStream     => Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case other              => fail(s"unexpected result $other")
    }

  /** The statement's outcome: its rows, or its failure's message -- a failed stream included. */
  protected def outcome(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Try(Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => Right(answer(result))
      case ElasticFailure(error)  => Left(error.message)
    }).toEither.left
      .map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")
      .joinRight

  protected def rowsOf(sql: String): Seq[ListMap[String, Any]] =
    outcome(sql).fold(error => fail(s"[$sql] failed: $error"), identity)

  private def byId(rows: Seq[ListMap[String, Any]]): Map[String, ListMap[String, Any]] =
    rows.map(r => String.valueOf(r.getOrElse("id", "?")) -> r).toMap

  private def ddl(sql: String): Either[String, QueryResult] =
    Try(Await.result(client.run(sql), 120.seconds) match {
      case ElasticSuccess(result) => Right(result)
      case ElasticFailure(error)  => Left(error.message)
    }).toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}").joinRight

  /** `GET <index>/_mapping`'s `properties`, whatever the major's shape. */
  private def properties(index: String): ObjectNode = {
    val response = restClient.performRequest(new Request("GET", s"/$index/_mapping"))
    val mappings =
      mapper.readTree(EntityUtils.toString(response.getEntity)).get(index).get("mappings")
    val typedOrNot: JsonNode =
      if (mappings.has("properties")) mappings else mappings.elements().next()
    typedOrNot.get("properties").asInstanceOf[ObjectNode]
  }

  private def typeOf(index: String, field: String): String =
    properties(index).get(field).get("type").asText()

  /** The hit routes (AC4). */
  private def onRoute(route: String, sql: String): Seq[ListMap[String, Any]] =
    route match {
      case "search" =>
        client.search(SelectStatement(s"$sql LIMIT 100"))(NativeContext) match {
          case ElasticSuccess(r)     => r.results
          case ElasticFailure(error) => fail(s"[$route] $sql: ${error.message}")
        }
      case "searchAsync"  => rowsOf(s"$sql LIMIT 100")
      case "scroll"       => rowsOf(sql)
      case "search_after" => rowsOf(s"$sql ORDER BY id")
      case "msearch"      => rowsOf(s"$sql LIMIT 100 UNION ALL $sql LIMIT 100")
      case "per-leg"      => rowsOf(s"$sql UNION ALL $sql")
    }

  private val routes: Seq[String] =
    Seq("search", "searchAsync", "scroll", "search_after", "msearch", "per-leg")

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  /** AC0's class of a value of `column` (A-12: an `unsigned_long` above 2^63-1 is a `BigInteger`).
    */
  private def classOk(column: String, value: Any): Boolean =
    value == null || (column match {
      case "id" | "k" | "ip" | "ck" => value.isInstanceOf[String]
      case "sf"                     => value.isInstanceOf[java.lang.Double]
      case "hf"                     => value.isInstanceOf[java.lang.Float]
      case "u" =>
        value.isInstanceOf[java.lang.Long] || (value.isInstanceOf[java.math.BigInteger] &&
        value.asInstanceOf[java.math.BigInteger].bitLength() > 63)
      case "d" | "dn" =>
        value.isInstanceOf[ZonedDateTime] &&
          value.asInstanceOf[ZonedDateTime].getZone == ZoneOffset.UTC
      case _ => true
    })

  private def typedColumns: Seq[String] = typed(flattened = false).map(_._1)

  // -------------------------------------------------------------------------------------------
  // AC0 / AC4 -- declared, reported, class per row
  // -------------------------------------------------------------------------------------------

  "The seven mapping types" should "be reported by describe as AC0 declares them (AC0)" in {
    val reported = client.describe(s"SELECT * FROM $table") match {
      case ElasticSuccess(Some(schema)) => schema.columns.map(c => c.name -> c.sqlType.typeId).toMap
      case other                        => fail(s"describe: $other")
    }
    reported("sf") shouldBe "DOUBLE"
    reported("hf") shouldBe "REAL"
    reported("ip") shouldBe "VARCHAR"
    if (modern) {
      reported("u") shouldBe "BIGINT"
      reported("dn") shouldBe "TIMESTAMP"
      reported("ck") shouldBe "VARCHAR"
      reported("fl") shouldBe "ANY"
    }
  }

  it should "answer one class per column on every row of every hit route (AC4)" in {
    val sql = s"SELECT ${("id" +: typedColumns).mkString(", ")} FROM $table"
    routes.foreach { route =>
      withClue(s"[$route] ") {
        val rows = onRoute(route, sql)
        rows should not be empty
        val misses = for {
          row    <- rows
          column <- typedColumns
          value = row.getOrElse(column, null)
          if !classOk(column, value)
        } yield s"${row.getOrElse("id", "?")}.$column: $value (${Option(value).map(_.getClass.getName)})"
        misses shouldBe empty
        val r1 = byId(rows)("r1")
        r1("sf") shouldBe java.lang.Double.valueOf(1.25)
        r1("hf") shouldBe java.lang.Float.valueOf(0.5f)
        r1("ip") shouldBe "10.0.0.1"
        if (modern) {
          r1("u") shouldBe new java.math.BigInteger("18446744073709551615")
          byId(rows)("r2")("u") shouldBe java.lang.Long.valueOf(7L)
          r1("dn") shouldBe ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 123456789, ZoneOffset.UTC)
          rows.map(_.get("ck")).distinct shouldBe Seq(Some("C"))
        }
      }
    }
  }

  "A constant_keyword field" should "answer its value on every row, plain and under * (AC2)" in {
    assume(modern, "constant_keyword exists from Elasticsearch 7.7 on")
    rowsOf(s"SELECT id, ck FROM $table").map(_.get("ck")).distinct shouldBe Seq(Some("C"))
    rowsOf(s"SELECT * FROM $table").map(_.get("ck")).distinct shouldBe Seq(Some("C"))
    rowsOf(s"SELECT id FROM $table WHERE ck = 'C'").map(_("id")).toSet shouldBe Set("r1", "r2")
  }

  "An index pattern whose indices disagree on a field" should "answer it as a VARCHAR (AC1)" in {
    client.describe("SELECT k FROM s7_p*") match {
      case ElasticSuccess(Some(schema)) =>
        schema.columns.map(_.sqlType.typeId) shouldBe Seq("VARCHAR")
      case other => fail(s"describe: $other")
    }
    val values = rowsOf("SELECT k FROM s7_p*").map(_("k"))
    values.toSet shouldBe Set("007", "7")
    values.foreach(_ shouldBe a[String])
  }

  "A JOIN body over a pattern and the SCHEMA-7 types" should "be typed as single-table describe types each side (SCHEMA-15)" in {
    val columns = "id" +: typedColumns
    def body(sql: String) = Parser(sql) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"the JOIN body does not parse: ${error.msg}")
    }
    val pattern = client.describe("SELECT k FROM s7_p*")
    val typed = client.describe(s"SELECT ${columns.mkString(", ")} FROM $table")
    def joined(
      first: ElasticResult[Option[ResultSchema]],
      second: ElasticResult[Option[ResultSchema]]
    ) =
      for {
        f <- first
        s <- second
      } yield for {
        a <- f
        b <- s
      } yield ResultSchema(a.columns ++ b.columns)
    joined(pattern, typed).toOption.flatten should not be empty
    val tItems = columns.map(c => s"t.$c").mkString(", ")
    // the pattern on the FROM side, then on the JOIN side
    client.describePerTable(
      body(s"SELECT p.k, $tItems FROM s7_p* p JOIN $table t ON p.k = t.k")
    ) shouldBe joined(pattern, typed)
    client.describePerTable(
      body(s"SELECT $tItems, p.k FROM $table t JOIN s7_p* p ON t.k = p.k")
    ) shouldBe joined(typed, pattern)
  }

  // -------------------------------------------------------------------------------------------
  // P1 -- the consumers
  // -------------------------------------------------------------------------------------------

  "A typed field" should "be filtered, grouped and aggregated by its type (P1)" in {
    def ids(where: String): Set[Any] =
      rowsOf(s"SELECT id FROM $table WHERE $where").map(_("id")).toSet
    ids("ip = '10.0.0.1'") shouldBe Set("r1")
    ids("hf > 1") shouldBe Set("r2")
    ids("sf = 1.25") shouldBe Set("r1")
    ids("ip IN ('10.0.0.1', '10.0.0.2')") shouldBe Set("r1", "r2")
    if (modern) {
      ids("u > 9223372036854775807") shouldBe Set("r1")
      ids("dn > '2024-01-31 00:00:00'") shouldBe Set("r1", "r2")
    }
    val keys = rowsOf(s"SELECT hf, COUNT(*) AS c FROM $table GROUP BY hf").map(_("hf"))
    keys.toSet shouldBe Set(java.lang.Float.valueOf(0.5f), java.lang.Float.valueOf(1.5f))
    val minMax = rowsOf(s"SELECT MIN(sf) AS mi, MAX(sf) AS ma FROM $table").head
    minMax("mi") shouldBe java.lang.Double.valueOf(1.25)
    minMax("ma") shouldBe java.lang.Double.valueOf(2.5)
    // a UNION ALL slot of an ip beside a keyword: VARCHAR, `String`s
    rowsOf(s"SELECT id, ip AS v FROM $table UNION ALL SELECT id, k AS v FROM $table")
      .map(_("v"))
      .foreach(_ shouldBe a[String])
  }

  it should "compute over a date_nanos field as over a TIMESTAMP (P1, D3)" in {
    assume(modern, "date_nanos exists from Elasticsearch 7 on")
    val plus = byId(rowsOf(s"SELECT id, dn + INTERVAL 1 DAY AS v FROM $table"))
    plus("r2")("v") shouldBe utc(2024, 2, 2)
    // A-72: moved by a number of days (the vendor rule: n days), it keeps its nanoseconds, as
    // under an INTERVAL -- through epoch milliseconds `dn + 1` answered `...T10:30:00.123Z`
    val nanos = ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 123456789, ZoneOffset.UTC)
    Seq(
      "dn + INTERVAL 1 DAY" -> nanos.plusDays(1),
      "dn + 1"              -> nanos.plusDays(1),
      "1 + dn"              -> nanos.plusDays(1),
      "dn - 1"              -> nanos.minusDays(1),
      "dn + 1.5"            -> nanos.plusHours(36),
      "dn + 1 + 1"          -> nanos.plusDays(2)
    ).foreach { case (expr, expected) =>
      withClue(s"[$expr] ") {
        val rows = byId(rowsOf(s"SELECT id, $expr AS v FROM $table"))
        rows("r1")("v") shouldBe expected
        rows("r2")("v") shouldBe a[ZonedDateTime]
      }
    }
    byId(rowsOf(s"SELECT id, dn + 1 AS v FROM $table"))("r2")("v") shouldBe utc(2024, 2, 2)
    byId(rowsOf(s"SELECT id, LAST_DAY(dn) AS v FROM $table"))("r1")("v") shouldBe
    LocalDate.of(2024, 1, 31)
    rowsOf(s"SELECT MAX(dn) AS m FROM $table").head("m") shouldBe a[ZonedDateTime]
  }

  "Arithmetic over an ip or a constant_keyword" should "be refused by type (A-65, R-83)" in {
    val columns = if (modern) Seq("ip", "ck") else Seq("ip")
    for {
      c  <- columns
      op <- Seq("+", "-", "*", "/", "%")
    } {
      val sql = s"SELECT id, $c $op 1 AS v FROM $table"
      withClue(s"[$sql] ") {
        outcome(sql).left.getOrElse("") should include("KEYWORD")
      }
    }
  }

  "A HAVING key filter on a half_float key" should "be refused as a float key's (R-56)" in {
    outcome(s"SELECT hf, COUNT(*) AS c FROM $table GROUP BY hf HAVING hf = 0.1").left.getOrElse(
      ""
    ) should include("HAVING cannot compare the float GROUP BY key hf")
  }

  // -------------------------------------------------------------------------------------------
  // AC5 -- the write direction
  // -------------------------------------------------------------------------------------------

  "ALTER TABLE ADD COLUMN" should "keep every field's mapping type and parameters (AC5)" in {
    val before = properties(addIndex)
    ddl(s"ALTER TABLE $addIndex ADD COLUMN z INT") shouldBe a[Right[_, _]]
    val after = properties(addIndex)
    after.get("z").get("type").asText() shouldBe "integer"
    val without = after.deepCopy()
    without.remove("z")
    without shouldBe before
    // and the rows read as before
    byId(rowsOf(s"SELECT id, sf, ip FROM $addIndex"))("r1")("ip") shouldBe "10.0.0.1"
  }

  "ALTER COLUMN SET DATA TYPE" should "map an ip column as text once it is a VARCHAR (AC5)" in {
    ddl(s"ALTER TABLE $ipIndex ALTER COLUMN ip SET DATA TYPE VARCHAR") shouldBe a[Right[_, _]]
    typeOf(ipIndex, "ip") shouldBe "text"
    byId(rowsOf(s"SELECT id, ip FROM $ipIndex"))("r1")("ip") shouldBe "10.0.0.1"
  }

  it should "reindex a date_nanos column declared a DATE into a date (A-61)" in {
    assume(modern, "date_nanos exists from Elasticsearch 7 on")
    ddl(s"ALTER TABLE $dnIndex ALTER COLUMN dn SET DATA TYPE DATE") shouldBe a[Right[_, _]]
    typeOf(dnIndex, "dn") shouldBe "date"
    val rows = rowsOf(s"SELECT id, dn FROM $dnIndex")
    rows.map(_("id")).toSet shouldBe Set("r1", "r2")
    byId(rows)("r2")("dn") shouldBe LocalDate.of(2024, 2, 1)
  }

  it should "refuse to move a constant_keyword column off its type, leaving it as it was (A-62)" in {
    assume(modern, "constant_keyword exists from Elasticsearch 7.7 on")
    ddl(s"ALTER TABLE $ckIndex ALTER COLUMN ck SET DATA TYPE VARCHAR").left.getOrElse(
      ""
    ) should include("constant_keyword")
    typeOf(ckIndex, "ck") shouldBe "constant_keyword"
    rowsOf(s"SELECT id, ck FROM $ckIndex").map(_.get("ck")).distinct shouldBe Seq(Some("C"))
  }

  "A several-valued ip cell" should "fail with the ARRAY remedy, which keeps the ip mapping (A-60, Q1)" in {
    outcome(s"SELECT id, ip FROM $multiIndex").left.getOrElse("") should include("ARRAY<KEYWORD>")
    ddl(s"ALTER TABLE $multiIndex ALTER COLUMN ip SET DATA TYPE ARRAY<KEYWORD>") shouldBe
    a[Right[_, _]]
    typeOf(multiIndex, "ip") shouldBe "ip"
    val rows = byId(rowsOf(s"SELECT id, ip FROM $multiIndex"))
    rows("m1")("ip") shouldBe Seq("10.0.0.1", "10.0.0.2")
    rows("m2")("ip") shouldBe a[Seq[_]]
  }

  /** `SHOW CREATE TABLE` of `source`, re-parsed, then run as the `CREATE TABLE` of `target`. */
  private def recreate(source: String, target: String): Unit = {
    val text = ddl(s"SHOW CREATE TABLE $source") match {
      case Right(SQLResult(sql)) => sql
      case other                 => fail(s"SHOW CREATE TABLE: $other")
    }
    val stmt = Parser(text) match {
      case Right(ct: CreateTable) => ct
      case other                  => fail(s"[$text] does not re-parse: $other")
    }
    Parser(stmt.sql) shouldBe Right(stmt)
    // the text carries the settings Elasticsearch generated for the source index (`uuid`,
    // `creation_date`, `provided_name`, `version`), which a create request refuses (= main): the
    // copy leaves them out, as ALTER TABLE's reindex and CREATE TABLE ... AS SELECT do
    val options: ListMap[String, Value[_]] =
      if (stmt.settings.isEmpty) stmt.options
      // qualified: this package has its own ObjectValue (an aggregate value), which Scala 2.12
      // resolves before an imported one
      else
        stmt.options.updated(
          "settings",
          app.softnetwork.elastic.sql.ObjectValue(copyableSettings(stmt.settings))
        )
    ddl(stmt.copy(table = target, options = options).sql) shouldBe a[Right[_, _]]
  }

  "SHOW CREATE TABLE of the typed fields" should "re-parse, and re-create the same mapping types (AC5)" in {
    recreate(createdIndex, copyIndex)
    typedColumns.foreach { c =>
      withClue(c)(typeOf(copyIndex, c) shouldBe typeOf(createdIndex, c))
    }
  }

  /** A-73: a `type` written in `CREATE TABLE` maps the field with it, as it did before the types
    * were read, and the field stays so through ALTER and `SHOW CREATE TABLE`: an ALTER re-sent the
    * SQL type's mapping before (`text` over a `keyword`: refused by Elasticsearch).
    */
  "A type written in CREATE TABLE" should "map the field, through ALTER and SHOW CREATE (A-73)" in {
    // `wildcard` from Elasticsearch 7.9 on, `match_only_text` from 7.14 on
    val written: Seq[(String, String, String)] =
      Seq(("c", "VARCHAR", "keyword"), ("i", "KEYWORD", "ip")) ++
      (if (modern) Seq(("w", "KEYWORD", "wildcard"), ("m", "TEXT", "match_only_text")) else Nil)
    val columns = written.map { case (c, sqlType, esType) =>
      s"$c $sqlType OPTIONS (type = '$esType')"
    }
    ddl(s"CREATE TABLE $ddlIndex (id KEYWORD, ${columns.mkString(", ")})") shouldBe a[Right[_, _]]
    def mappedAsWritten(index: String): Unit =
      written.foreach { case (c, _, esType) =>
        withClue(s"$index.$c: ")(typeOf(index, c) shouldBe esType)
      }
    mappedAsWritten(ddlIndex)
    ddl(s"ALTER TABLE $ddlIndex ADD COLUMN z INT") shouldBe a[Right[_, _]]
    typeOf(ddlIndex, "z") shouldBe "integer"
    mappedAsWritten(ddlIndex)
    recreate(ddlIndex, ddlCopyIndex)
    mappedAsWritten(ddlCopyIndex)
  }

  /** A-75: a sort over a declared type a DDL statement wrote another type for (`INT` mapped `long`,
    * `KEYWORD` mapped `wildcard`) carries the `unmapped_type` of the declared type, as it did
    * before the types were read, so `ORDER BY` over a pattern where one index lacks the field
    * answers every document, the one lacking it last.
    */
  "ORDER BY a type written in CREATE TABLE" should "answer over a pattern where an index lacks it (A-75)" in {
    // `wildcard` from Elasticsearch 7.9 on
    val written: Seq[(String, String)] =
      Seq("i" -> "INT OPTIONS (type = 'long')") ++
      (if (modern) Seq("w" -> "KEYWORD OPTIONS (type = 'wildcard')") else Nil)
    val columns = written.map { case (c, definition) => s"$c $definition" }
    ddl(s"CREATE TABLE $orderDdlIndex (id KEYWORD, ${columns.mkString(", ")})") shouldBe
    a[Right[_, _]]
    def source(id: String, i: Int, w: String): String =
      s"""{"id":"$id","i":$i""" + (if (modern) s""","w":"$w"""" else "") + "}"
    put(orderDdlIndex, Seq("o1" -> source("o1", 1, "a"), "o2" -> source("o2", 2, "b")))
    val bare = """{"properties": {"id": {"type":"keyword"}}}"""
    load(orderBareIndex, bare, Seq("o3" -> """{"id":"o3"}"""))
    written.foreach { case (c, _) =>
      val rows = rowsOf(s"SELECT id, $c FROM s7_o* ORDER BY $c LIMIT 10")
      val ids = rows.map(r => String.valueOf(r("id")))
      withClue(s"ORDER BY $c: ")(ids shouldBe Seq("o1", "o2", "o3"))
    }
  }
}
