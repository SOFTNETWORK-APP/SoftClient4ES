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
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.persistence.generateUUID
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.client.Request
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** An entity whose fields a computed-only statement does not all select (D12 (1)). */
case class ComputedProjectionRow(id: Option[String] = None, k: Option[Long] = None)

/** The same, with a required field the statement does not select. */
case class ComputedProjectionRequiredRow(id: String, k: Long)

/** SCHEMA-5 -- computed-only projections and NUMERIC widths (#418), on fixture `s5_t`:
  *
  *   - "a computed-only projection": the request carries `"_source": false`, its rows hold its
  *     columns only, on every hit route, schema path and alias (AC1, D2 / D3);
  *   - "the controls": every other statement keeps its request (AC2);
  *   - "a computed-only window statement": its base asks the `_source` of its partition columns,
  *     its partitions answer as the whole document answers (AC3, D4);
  *   - "widths": DATEDIFF, ROUND, EXP / LOG / LOG10 / SQRT / POW, the date parts and DATE_TRUNC
  *     declare DuckDB's type, one class per column, in every position (AC4);
  *   - "#418": every statement of the issue (AC7);
  *   - "ROUND to a negative scale": DuckDB's values, an integer overflow refused (AC6, A-39);
  *   - "CTAS": the mappings (AC5);
  *   - "INSERT … SELECT": a key-less computed-only SELECT refused before any write (A-38);
  *   - "quoted numbers": a quoted number beside a whole type is the whole number it rounds to (AC9,
  *     D16, A-42), each row equal to its mirror.
  *
  * The oracle is DuckDB 1.5.5 (the story's T0-5, RUN 2026-11-02) and, for what must not change,
  * main's answers (T0-1 on Elasticsearch 8.18.3).
  */
trait ComputedProjectionSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  protected val table = "s5_t"
  protected val alias = "s5_a"

  protected val pk1 = "s5_pk1"
  protected val pk2 = "s5_pk2"
  protected val nokey = "s5_nokey"

  private val ctasTables: Seq[String] = (0 until 8).map(i => s"s5_c$i")

  protected lazy val major: Int = Try(elasticVersion.takeWhile(_ != '.').toInt).getOrElse(8)

  private val mapping: String =
    """{"_meta": {"columns": {"d": {"data_type": "DATE"}, "d2": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"}, "pk": {"type": "keyword"}, "pu": {"type": "keyword"},
      |  "s": {"type": "keyword"}, "b": {"type": "boolean"},
      |  "n": {"type": "integer"}, "big": {"type": "long"}, "ti": {"type": "byte"},
      |  "pos": {"type": "integer"}, "x": {"type": "double"}, "r": {"type": "float"},
      |  "d": {"type": "date"}, "d2": {"type": "date"}, "ts": {"type": "date"},
      |  "name": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
      |  "obj": {"properties": {"a": {"type": "integer"}}},
      |  "items": {"type": "nested", "properties": {"p": {"type": "integer"}}}
      |}}""".stripMargin

  /** NULL (r4), negative, large (r5: ROUND to -5 places, D8) and half values; `n` 2 and 3 tell a
    * rounded quoted number from its fraction; `pos` is positive (SQRT / LOG).
    */
  protected val docs: Seq[(String, String)] = Seq(
    "r1" -> """{"id":"r1","pk":"a","pu":"x","s":"2024-01-31","b":true,"n":2,"big":42,"ti":127,"pos":4,"x":2.5,"r":1.5,"d":"2024-01-31","d2":"2024-01-01","ts":"2024-01-31T10:30:00Z","name":"Alpha","obj":{"a":1},"items":[{"p":1}]}""",
    "r2" -> """{"id":"r2","pk":"a","pu":"x","s":"2024-03-01","b":false,"n":3,"big":-7,"ti":-128,"pos":9,"x":-2.5,"r":0.25,"d":"2024-03-01","d2":"2024-02-28","ts":"2024-03-01T00:00:00Z","name":"Beta","obj":{"a":2}}""",
    "r3" -> """{"id":"r3","pk":"b","pu":"y","s":"2023-12-25","b":true,"n":-2,"big":1706697000000,"ti":0,"pos":16,"x":1.5,"r":-1.5,"d":"2023-12-25","d2":"2023-12-31","ts":"2023-12-25T23:59:59.999Z","name":"Alpha","obj":{"a":1}}""",
    "r4" -> """{"id":"r4","pk":"b","pu":"y"}""",
    "r5" -> """{"id":"r5","pk":"c","pu":"z","s":"2024-02-29","b":false,"n":423789293,"big":1000,"ti":-1,"pos":1,"x":423789293.5,"r":2.5,"d":"2024-02-29","d2":"2024-01-31","ts":"2024-02-29T12:00:00Z","name":"Delta","obj":{"a":3}}""",
    "r6" -> """{"id":"r6","pk":"c","pu":"z","s":"2024-02-01","b":true,"n":1000,"big":2,"ti":1,"pos":25,"x":1000.0,"r":1.0,"d":"2024-02-01","d2":"2024-02-01","ts":"2024-02-01T00:00:00Z","name":"Gamma","obj":{"a":2}}"""
  )

  private def raw(method: String, path: String, body: String = ""): String = {
    val request = new Request(method, path)
    if (body.nonEmpty) request.setEntity(new NStringEntity(body, ContentType.APPLICATION_JSON))
    scala.io.Source
      .fromInputStream(restClient.performRequest(request).getEntity.getContent)
      .mkString
  }

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(client.deleteIndex(index))
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    (Seq(table, pk1, pk2, nokey) ++ ctasTables).foreach(drop)
    client
      .createIndex(table, settings = """{"number_of_shards": 2, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(table, mapping).get shouldBe true
    docs.foreach { case (id, source) =>
      client.index(table, id, source) match {
        case ElasticSuccess(true) =>
        case other                => fail(s"indexing $id into $table: $other")
      }
    }
    client.refresh(table)
    raw("POST", "/_aliases", s"""{"actions":[{"add":{"index":"$table","alias":"$alias"}}]}""")
    ()
  }

  override def afterAll(): Unit = {
    Try(
      raw("POST", "/_aliases", s"""{"actions":[{"remove":{"index":"$table","alias":"$alias"}}]}""")
    )
    (Seq(table, pk1, pk2, nokey) ++ ctasTables).foreach(drop)
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // Results, routes, the class oracle
  // -------------------------------------------------------------------------------------------

  protected def run(sql: String): Either[String, QueryResult] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => Right(result)
      case ElasticFailure(error)  => Left(error.message)
    }

  protected def runOk(sql: String): QueryResult =
    run(sql).fold(e => fail(s"[$sql] failed: $e"), identity)

  protected def answer(result: QueryResult): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    result match {
      case q: QueryRows       => (q.rows, q.schema)
      case q: QueryStructured => (q.response.results, q.schema)
      case q: QueryStream =>
        (Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds), q.schema)
      case other => fail(s"unexpected result $other")
    }

  protected def rowsOf(sql: String): Seq[ListMap[String, Any]] = answer(runOk(sql))._1

  protected def idsOf(sql: String): Set[String] =
    rowsOf(sql).map(r => String.valueOf(r.getOrElse("id", "?"))).toSet

  protected def typeOf(sql: String, column: String): SQLType =
    answer(runOk(sql))._2
      .flatMap(_.columns.find(_.name == column))
      .map(_.sqlType)
      .getOrElse(fail(s"[$sql] no column $column in the schema"))

  /** The `_source` of the request `sql` is sent with, as the client builds it. */
  protected def sourceOf(sql: String): String =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        val resolved = client.resolveWithSchema(single) match {
          case ElasticSuccess(r)     => r
          case ElasticFailure(error) => fail(s"[$sql] ${error.message}")
        }
        val json =
          client.singleSearchToJsonQuery(resolved)(
            System.currentTimeMillis(),
            PainlessContextType.Query
          )
        compact(render(parse(json) \ "_source"))
      case other => fail(s"[$sql] is not one SELECT: $other")
    }

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

  /** SCHEMA-2 D2's class for a value of a column of type `t`. */
  protected def classOk(t: SQLType, value: Any): Boolean =
    value == null || (t match {
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
      case _ => true
    })

  /** `sql`'s rows and schema, or why it failed -- a stream that fails while it is read included. */
  protected def attempt(
    sql: String
  ): Either[String, (Seq[ListMap[String, Any]], Option[ResultSchema])] =
    Try(run(sql).map(answer)).toEither.left.map(e => String.valueOf(e.getMessage)).flatMap(identity)

  /** Every value of every column has its column's class: the misses. */
  protected def classMisses(
    rows: Seq[ListMap[String, Any]],
    schema: Option[ResultSchema]
  ): Seq[String] =
    schema match {
      case None => Seq("no schema")
      case Some(s) =>
        for {
          row    <- rows
          column <- s.columns
          value = row.getOrElse(column.name, null)
          if !classOk(column.sqlType, value)
        } yield s"${row.getOrElse("id", "?")}.${column.name}: $value (${value.getClass.getName}) is not ${column.sqlType.typeId}"
    }

  protected def classMisses(sql: String): Seq[String] =
    attempt(sql).fold(e => Seq(s"failed: $e"), { case (rows, schema) => classMisses(rows, schema) })

  private def utc(y: Int, mo: Int, d: Int): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, 0, 0, 0, 0, ZoneOffset.UTC)

  // -------------------------------------------------------------------------------------------
  // A computed-only projection (AC1, D2 / D3)
  // -------------------------------------------------------------------------------------------

  "A computed-only projection" should "ask for no _source and answer its one column on every route (AC1)" in {
    val sql = s"SELECT 2 AS k FROM $table"
    sourceOf(sql) shouldBe "false"
    sourceOf(s"SELECT n + 1 AS v, UPPER(s) AS u FROM $table") shouldBe "false"
    routes.foreach { route =>
      withClue(s"[$route] ") {
        val (rows, schema) = onRoute(route, sql)
        rows.size shouldBe (if (route == "msearch" || route == "per-leg") 2 * docs.size
                            else docs.size)
        schema.map(_.columns.map(c => c.name -> c.sqlType)) shouldBe Some(Seq("k" -> SQLTypes.Int))
        rows.foreach { row =>
          row.keySet shouldBe Set("k")
          row("k") shouldBe Integer.valueOf(2)
        }
      }
    }
    // the alias (Elasticsearch 8.18.3 attaches the index's mapping to it: measured T0-1)
    val onAlias = rowsOf(s"SELECT 2 AS k FROM $alias")
    onAlias.size shouldBe docs.size
    onAlias.foreach(_.keySet shouldBe Set("k"))
    // a computed column over a field, on every route
    routes.foreach { route =>
      withClue(s"[$route] v ") {
        val (rows, _) = onRoute(route, s"SELECT n + 1 AS v FROM $table")
        rows.map(_("v")).filter(_ != null).foreach(_ shouldBe a[java.lang.Long])
        rows.foreach(_.keySet shouldBe Set("v"))
      }
    }
  }

  it should "answer one row FROM-less, and its WHERE-subquery inner statement as before" in {
    rowsOf("SELECT NULL AS n") shouldBe Seq(ListMap("n" -> null))
    rowsOf("SELECT 2 AS k") shouldBe Seq(ListMap("k" -> Integer.valueOf(2)))
    idsOf(
      s"SELECT id FROM $table WHERE n IN (SELECT n + 0 AS m FROM $table WHERE id = 'r1')"
    ) shouldBe
    Set("r1")
  }

  it should "leave an entity's unselected Option field None, and fail a required one" in {
    // the unselected `id` is no longer read from the whole document (D12 (1)); `k`, a computed
    // column, is read as an entity reads one (recorded: not this story's)
    client.searchAsUnchecked[ComputedProjectionRow](
      SelectStatement(s"SELECT n + 1 AS k FROM $table WHERE id = 'r1'")
    ) match {
      case ElasticSuccess(rows) => rows.map(_.id) shouldBe Seq(None)
      case ElasticFailure(e)    => fail(e.message)
    }
    val required = Try(
      client.searchAsUnchecked[ComputedProjectionRequiredRow](
        SelectStatement(s"SELECT n + 1 AS k FROM $table WHERE id = 'r1'")
      )
    )
    required.toOption.exists(_.isInstanceOf[ElasticSuccess[_]]) shouldBe false
  }

  "The controls" should "keep their request (AC2)" in {
    sourceOf(s"SELECT id, 2 AS k FROM $table") shouldBe """{"includes":["id"]}"""
    sourceOf(s"SELECT _id, 2 AS k FROM $table") shouldBe """{"includes":["_id"]}"""
    sourceOf(s"SELECT * FROM $table") shouldBe """{"includes":["*"]}"""
    sourceOf(s"SELECT * EXCEPT(name) FROM $table") shouldBe
    """{"includes":["*"],"excludes":["name"]}"""
    sourceOf(s"SELECT id, n FROM $table") shouldBe """{"includes":["id","n"]}"""
    sourceOf(s"SELECT COUNT(*) AS c FROM $table") shouldBe "false"
    sourceOf(s"SELECT pk, COUNT(*) AS c FROM $table GROUP BY pk") shouldBe "false"
    sourceOf(s"SELECT t.id, i.p FROM $table AS t JOIN UNNEST(t.items) AS i") should not be "false"
  }

  // -------------------------------------------------------------------------------------------
  // A computed-only window statement (AC3, D4)
  // -------------------------------------------------------------------------------------------

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

  private val windowRoutes = Seq("search", "searchAsync", "scroll")

  "A computed-only window statement" should "answer its unselected partitions (AC3, D4)" in {
    windowRoutes.foreach { route =>
      withClue(s"[$route] ") {
        val rows =
          onWindowRoute(
            route,
            s"SELECT UPPER(id) AS u, SUM(n) OVER (PARTITION BY pk) AS s FROM $table"
          )
        rows.map(_.keys.toSeq).distinct shouldBe Seq(Seq("u", "s"))
        // pk a = {r1, r2}: 2 + 3; b = {r3, r4}: -2; c = {r5, r6}: 423789293 + 1000
        rows.map(r => r("u") -> r("s")).toMap shouldBe Map(
          "R1" -> java.lang.Long.valueOf(5L),
          "R2" -> java.lang.Long.valueOf(5L),
          "R3" -> java.lang.Long.valueOf(-2L),
          "R4" -> java.lang.Long.valueOf(-2L),
          "R5" -> java.lang.Long.valueOf(423790293L),
          "R6" -> java.lang.Long.valueOf(423790293L)
        )
        onWindowRoute(route, s"SELECT 2 AS k, SUM(n) OVER (PARTITION BY pk) AS s FROM $table")
          .map(_("s"))
          .sortBy(_.toString) shouldBe Seq(-2L, -2L, 423790293L, 423790293L, 5L, 5L)
          .map(java.lang.Long.valueOf)
          .sortBy(_.toString)
      }
    }
  }

  it should "answer every partition column kind as the whole document answers it" in {
    // the `_source` subtree each partition key reads is the one `*` fetches: dotted, multi-field
    // sub-path, paired by instant, a DATE
    for (p <- Seq("obj.a", "name.keyword", "ts", "d", "pk"); route <- windowRoutes) {
      withClue(s"[$route] [$p] ") {
        def byId(head: String): Map[String, Any] =
          onWindowRoute(
            route,
            s"SELECT $head, id AS i, SUM(n) OVER (PARTITION BY $p) AS w FROM $table"
          ).map(r => String.valueOf(r("i")) -> r("w")).toMap
        val computedOnly =
          onWindowRoute(
            route,
            s"SELECT CONCAT(id, '') AS i, SUM(n) OVER (PARTITION BY $p) AS w FROM $table"
          )
            .map(r => String.valueOf(r("i")) -> r("w"))
            .toMap
        computedOnly shouldBe byId("*")
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // Widths (AC4)
  // -------------------------------------------------------------------------------------------

  private val extracts: Seq[String] = Seq(
    "YEAR(d)",
    "MONTH(d)",
    "DAY(d)",
    "WEEKDAY(d)",
    "YEARDAY(d)",
    "HOUR(ts)",
    "MINUTE(ts)",
    "SECOND(ts)",
    "MILLISECOND(ts)",
    "MICROSECOND(ts)",
    "NANOSECOND(ts)",
    "EPOCHDAY(d)",
    "OFFSET_SECONDS(ts)",
    "QUARTER(d)",
    "WEEK(d)",
    "EXTRACT(YEAR FROM ts)"
  )

  private val datediffs: Seq[String] = for {
    spelling <- Seq(
      "DATEDIFF(%1$s, %2$s)",
      "DATE_DIFF(%1$s, %2$s, %3$s)",
      "DATEDIFF(%3$s, %2$s, %1$s)",
      "TIMESTAMPDIFF(%3$s, %2$s, %1$s)",
      "TIMESTAMPDIFF(%1$s, %2$s, %3$s)"
    )
    (a, b) <- Seq(("d", "d2"), ("ts", "d2"))
    unit   <- Seq("DAY", "HOUR")
  } yield spelling.format(a, b, unit)

  private val rounds: Seq[(String, SQLType)] =
    (for {
      (operand, t) <- Seq(
        "n"   -> (SQLTypes.Int: SQLType),
        "big" -> SQLTypes.BigInt,
        "x"   -> SQLTypes.Double,
        "r"   -> SQLTypes.Real
      )
      scale <- Seq("", ", 0", ", 1", ", -1", ", -5")
    } yield s"ROUND($operand$scale)" -> t) :+ ("ROUND(ti)" -> SQLTypes.TinyInt)

  private val maths: Seq[String] =
    Seq("EXP(ti)", "LOG(pos)", "LOG10(pos)", "SQRT(pos)", "POW(n, 2)", "POWER(x, 2)")

  /** The cells whose Painless fails on Elasticsearch whatever their type -- its bytes are main's
    * (RENDER, 2026-11-02), so they fail on main too: a `LocalDate` receiver has no `truncatedTo`
    * (HOUR over a date-producing operand), `ChronoUnit.QUARTERS` is not whitelisted (QUARTER over
    * the clock), DATE_PARSE's QUARTER reads its text, and a DATE_ADD over a DATE truncated to the
    * DAY calls `atStartOfDay` on a `ZonedDateTime` (measured on main, T0-1). Recorded, not this
    * story's.
    */
  private val failingTruncs: Set[String] = Set(
    "DATE_TRUNC(CURRENT_TIMESTAMP, QUARTER)",
    "DATE_TRUNC(NOW(), QUARTER)",
    "DATE_TRUNC(CURRENT_DATE, QUARTER)",
    "DATE_TRUNC(CURRENT_DATE, HOUR)",
    "DATE_TRUNC(CAST(ts AS DATE), HOUR)",
    "DATE_TRUNC(DATE_PARSE(s, '%Y-%m-%d'), QUARTER)",
    "DATE_TRUNC(DATE_PARSE(s, '%Y-%m-%d'), HOUR)",
    "DATE_TRUNC(LAST_DAY(ts), HOUR)",
    "DATE_TRUNC(DATE_ADD(d, INTERVAL 1 MONTH), DAY)"
  )

  private val truncs: Seq[String] = (for {
    operand <- Seq(
      "d",
      "ts",
      "CURRENT_TIMESTAMP",
      "NOW()",
      "CURRENT_DATE",
      "CAST(ts AS DATE)",
      "DATE_PARSE(s, '%Y-%m-%d')",
      "LAST_DAY(ts)",
      "DATE_ADD(d, INTERVAL 1 MONTH)"
    )
    unit <- Seq("DAY", "WEEK", "MONTH", "QUARTER", "HOUR")
  } yield s"DATE_TRUNC($operand, $unit)").filterNot(failingTruncs.contains)

  private def items: Seq[(String, SQLType)] =
    extracts.map(_  -> (SQLTypes.BigInt: SQLType)) ++
    datediffs.map(_ -> (SQLTypes.BigInt: SQLType)) ++ rounds ++
    maths.map(_ -> (SQLTypes.Double: SQLType)) ++ truncs.map(_ -> (SQLTypes.Timestamp: SQLType))

  "Every width" should "declare DuckDB's type in describe and run, one class per row (AC4)" in {
    val wrong = items.flatMap { case (e, expected) =>
      val sql = s"SELECT id, $e AS v FROM $table"
      attempt(sql) match {
        case Left(error) => Seq(s"[$sql] failed: ${error.take(300)}")
        case Right((rows, schema)) =>
          val declared = schema.flatMap(_.columns.find(_.name == "v")).map(_.sqlType)
          val described = client.describe(sql) match {
            case ElasticSuccess(s) => s.flatMap(_.columns.find(_.name == "v")).map(_.sqlType)
            case ElasticFailure(f) => Some(SQLTypes.Null).filter(_ => f.message.isEmpty)
          }
          (if (declared != Some(expected)) Seq(s"[$sql] declared $declared, not $expected")
           else Nil) ++
          (if (described != declared) Seq(s"[$sql] described $described, ran $declared")
           else Nil) ++ classMisses(rows, schema).map(m => s"[$sql] $m")
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n")(wrong shouldBe empty)
  }

  private val positioned: Seq[(String, SQLType)] = Seq(
    "DATEDIFF(d, d2)" -> SQLTypes.BigInt,
    "ROUND(n)"        -> SQLTypes.Int,
    "ROUND(big, -1)"  -> SQLTypes.BigInt,
    "ROUND(x, 1)"     -> SQLTypes.Double,
    "ROUND(r)"        -> SQLTypes.Real,
    "SQRT(pos)"       -> SQLTypes.Double,
    "YEAR(d)"         -> SQLTypes.BigInt
  )

  it should "hold one class per column in a UNION ALL, a GROUP BY key, MIN / MAX / SUM, a window and an arm" in {
    val wrong = positioned.flatMap { case (e, t) =>
      val whole = t == SQLTypes.TinyInt || t == SQLTypes.Int || t == SQLTypes.BigInt
      val statements = Seq(
        s"SELECT n AS k FROM $table UNION ALL SELECT $e AS k FROM $table",
        s"SELECT x AS k FROM $table UNION ALL SELECT $e AS k FROM $table",
        s"SELECT $e AS k, COUNT(*) AS c FROM $table GROUP BY $e",
        s"SELECT MIN($e) AS mi, MAX($e) AS ma, SUM($e) AS su FROM $table",
        s"SELECT id, pk, SUM($e) OVER (PARTITION BY pk) AS w FROM $table"
        // GREATEST / COALESCE over a function whose rendering is a NULL-guarded ternary fail to
        // compile in Elasticsearch on main too (`Cannot cast from [int] to [java.lang.Object]`,
        // T0-1): the arm is inlined unparenthesised. Left out, no failure pinned: a pre-existing
        // renderer defect, not this story's (architect ruling A-44).
      )
      statements.flatMap { sql =>
        attempt(sql) match {
          case Left(error) => Seq(s"[$sql] failed: ${error.take(300)}")
          case Right((rows, schema)) =>
            schema.toSeq.flatMap(_.columns).collect {
              case c if c.sqlType == SQLTypes.Numeric || c.sqlType == SQLTypes.Temporal =>
                s"[$sql] ${c.name} has no slot (${c.sqlType.typeId})"
            } ++ classMisses(rows, schema).map(m => s"[$sql] $m")
        }
      } ++ Try {
        // the key level and the aggregates' types
        val key = typeOf(s"SELECT $e AS k, COUNT(*) AS c FROM $table GROUP BY $e", "k")
        val metrics = s"SELECT MIN($e) AS mi, MAX($e) AS ma, SUM($e) AS su FROM $table"
        val sum = if (whole) SQLTypes.BigInt else SQLTypes.Double
        Seq(
          if (key != t) Some(s"[GROUP BY $e] key $key, not $t") else None,
          if (typeOf(metrics, "ma") != t) Some(s"[MAX($e)] ${typeOf(metrics, "ma")}") else None,
          if (typeOf(metrics, "su") != sum) Some(s"[SUM($e)] ${typeOf(metrics, "su")}") else None
        ).flatten
      }.fold(error => Seq(s"[$e types] ${error.getMessage}"), identity) ++ Try {
        // a HAVING key filter keeps the groups SQL keeps
        val all = rowsOf(s"SELECT $e AS k, COUNT(*) AS c FROM $table GROUP BY $e").map(_("k"))
        def numeric(v: Any): BigDecimal = BigDecimal(String.valueOf(v))
        val kept = rowsOf(
          s"SELECT $e AS k, COUNT(*) AS c FROM $table GROUP BY $e HAVING k IN (0, 2, 3, 30, 2024)"
        ).map(_("k")).toSet
        val expected =
          all.filter(v => Set(0, 2, 3, 30, 2024).map(BigDecimal(_)).contains(numeric(v))).toSet
        if (kept != expected) Seq(s"[GROUP BY $e HAVING k IN] $kept, not $expected") else Nil
      }.fold(error => Seq(s"[$e HAVING] ${error.getMessage}"), identity)
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n")(wrong shouldBe empty)
  }

  it should "type a per-group calculation, a DATE_TRUNC beside a DATE and date arithmetic" in {
    // pk a: MAX(d) = 2024-03-01, b: 2023-12-25, c: 2024-02-29; DuckDB date_diff('day', …) BIGINT
    rowsOf(s"SELECT pk, DATEDIFF(MAX(d), '2024-01-01') AS v FROM $table GROUP BY pk")
      .map(r => r("pk") -> r("v"))
      .toMap shouldBe Map[Any, Any](
      "a" -> java.lang.Long.valueOf(60L),
      "b" -> java.lang.Long.valueOf(-7L),
      "c" -> java.lang.Long.valueOf(59L)
    )
    // D15: a TIMESTAMP slot beside a DATE, every row a UTC ZonedDateTime
    Seq("DATE_TRUNC(CURRENT_TIMESTAMP, DAY)", "DATE_TRUNC(CAST(ts AS DATE), DAY)").foreach { e =>
      val sql = s"SELECT d AS k FROM $table UNION ALL SELECT $e AS k FROM $table"
      withClue(s"[$sql] ") {
        typeOf(sql, "k") shouldBe SQLTypes.Timestamp
        classMisses(sql) shouldBe empty
      }
    }
    // D15: over a date-producing operand `- d` is fractional days, `+ 1` a TIMESTAMP
    typeOf(s"SELECT id, DATE_TRUNC(CAST(ts AS DATE), DAY) - d AS v FROM $table", "v") shouldBe
    SQLTypes.Double
    val plusOne =
      rowsOf(s"SELECT id, DATE_TRUNC(CAST(ts AS DATE), DAY) + 1 AS v FROM $table WHERE id = 'r1'")
    plusOne.map(_("v")) shouldBe Seq(utc(2024, 2, 1))
    // D9: a DATE plus DATEDIFF whole days is a DATE (r1: 2024-01-31 + 30)
    rowsOf(s"SELECT id, d + DATEDIFF(d, d2) AS v FROM $table WHERE id = 'r1'").map(_("v")) shouldBe
    Seq(LocalDate.of(2024, 3, 1))
  }

  // -------------------------------------------------------------------------------------------
  // #418 (AC7)
  // -------------------------------------------------------------------------------------------

  "#418" should "answer every statement of its body and comments, one class per column" in {
    val statements = Seq(
      s"SELECT id, n + 1 AS v FROM $table",
      s"SELECT id, 2 AS k FROM $table",
      s"SELECT id, CURRENT_DATE AS v FROM $table",
      s"SELECT id, CAST(d AS DATE) AS v FROM $table",
      s"SELECT id, n FROM $table",
      s"SELECT 2 AS k FROM $table",
      s"SELECT n + 1 AS v FROM $table",
      s"SELECT CAST(d AS DATE) AS k FROM $table",
      s"SELECT CAST(ts AS VARCHAR) AS k FROM $table",
      s"SELECT 2 AS k FROM $table UNION ALL SELECT n AS k FROM $table",
      s"SELECT CURRENT_DATE AS k FROM $table UNION ALL SELECT d AS k FROM $table",
      s"SELECT NULL AS k FROM $table UNION ALL SELECT n AS k FROM $table",
      // comment 2 -- the functions' domain kept (DuckDB refuses SQRT / LOG of a negative too)
      s"SELECT n AS k FROM $table UNION ALL SELECT SQRT(n) AS k FROM $table WHERE n > 0",
      s"SELECT n AS k FROM $table UNION ALL SELECT POWER(n, 2) AS k FROM $table",
      s"SELECT n AS k FROM $table UNION ALL SELECT LOG(x) AS k FROM $table WHERE x > 0",
      s"SELECT n AS k FROM $table UNION ALL SELECT EXP(n) AS k FROM $table WHERE n < 100",
      s"SELECT n AS k FROM $table UNION ALL SELECT ROUND(n) AS k FROM $table",
      s"SELECT n AS k FROM $table UNION ALL SELECT ROUND(x, 1) AS k FROM $table",
      // comment 3 (D15)
      s"SELECT d AS k FROM $table UNION ALL SELECT DATE_TRUNC(CURRENT_TIMESTAMP, DAY) AS k FROM $table",
      s"SELECT ts AS k FROM $table UNION ALL SELECT DATE_TRUNC(CURRENT_TIMESTAMP, DAY) AS k FROM $table"
    )
    val wrong = statements.flatMap { sql =>
      attempt(sql) match {
        case Left(error) => Seq(s"[$sql] failed: ${error.take(300)}")
        case Right((rows, schema)) =>
          val s = schema.getOrElse(fail(s"[$sql] no schema"))
          val described = client.describe(sql) match {
            case ElasticSuccess(d) => d
            case ElasticFailure(f) => fail(s"[$sql] describe: ${f.message}")
          }
          (if (described != schema) Seq(s"[$sql] described $described, ran $schema") else Nil) ++
          s.columns.collect {
            case c if c.sqlType == SQLTypes.Numeric || c.sqlType == SQLTypes.Temporal =>
              s"[$sql] ${c.name} ${c.sqlType.typeId}"
          } ++ rows.flatMap(r => (r.keySet -- s.names).map(k => s"[$sql] undeclared key $k")) ++
          classMisses(rows, schema).map(m => s"[$sql] $m")
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n")(wrong shouldBe empty)
    // comment 2: an INT beside SQRT is a DOUBLE slot; comment 3: TIMESTAMP
    typeOf(
      s"SELECT n AS k FROM $table UNION ALL SELECT SQRT(n) AS k FROM $table WHERE n > 0",
      "k"
    ) shouldBe
    SQLTypes.Double
    typeOf(s"SELECT n AS k FROM $table UNION ALL SELECT ROUND(n) AS k FROM $table", "k") shouldBe
    SQLTypes.Int
  }

  // -------------------------------------------------------------------------------------------
  // ROUND to a negative scale (AC6, D8, A-39)
  // -------------------------------------------------------------------------------------------

  "ROUND to a negative scale" should "answer DuckDB's values and fail an integer overflow (AC6)" in {
    def byId(sql: String): Map[String, Any] =
      rowsOf(sql).map(r => String.valueOf(r("id")) -> r("v")).toMap
    // DuckDB 1.5.5: round(423789293::INTEGER, -5) = 423800000, round(n, -1) over 2, 3, -2, 1000
    byId(s"SELECT id, ROUND(n, -5) AS v FROM $table")("r5") shouldBe Integer.valueOf(423800000)
    byId(s"SELECT id, ROUND(n, -1) AS v FROM $table") shouldBe Map(
      "r1" -> Integer.valueOf(0),
      "r2" -> Integer.valueOf(0),
      "r3" -> Integer.valueOf(0),
      "r4" -> null,
      "r5" -> Integer.valueOf(423789290),
      "r6" -> Integer.valueOf(1000)
    )
    // round(42::BIGINT, -1) = 40, round(-7, -1) = -10, round(1706697000000, -5) = 1706697000000
    val big = byId(s"SELECT id, ROUND(big, -1) AS v FROM $table")
    big("r1") shouldBe java.lang.Long.valueOf(40L)
    big("r2") shouldBe java.lang.Long.valueOf(-10L)
    byId(s"SELECT id, ROUND(big, -5) AS v FROM $table")("r3") shouldBe
    java.lang.Long.valueOf(1706697000000L)
    // round(423789293.5::DOUBLE, -1) = 423789290.0, round(-2.5, -1) = -0.0
    byId(s"SELECT id, ROUND(x, -1) AS v FROM $table")("r5") shouldBe
    java.lang.Double.valueOf(423789290.0)
    // A-39 -- an integer rounded out of its type's range
    Seq(
      s"SELECT ROUND(CAST(127 AS TINYINT), -1) AS v FROM $table",
      // T0-5's `2147483647::INTEGER`: an untyped whole literal is a BIGINT operand in core
      s"SELECT ROUND(CAST(2147483647 AS INT), -1) AS v FROM $table",
      s"SELECT id, ROUND(ti, -1) AS v FROM $table"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        attempt(sql) match {
          case Left(error) => error should include("Overflow in ROUND of integer")
          case Right(rows) => fail(s"answered $rows")
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // CTAS (AC5)
  // -------------------------------------------------------------------------------------------

  private def mappedType(index: String, column: String): String =
    client.getMapping(index) match {
      case ElasticSuccess(js) =>
        val field = ("\"" + java.util.regex.Pattern.quote(column) +
          "\"\\s*:\\s*\\{\\s*\"type\"\\s*:\\s*\"([a-z_]+)\"").r
        field.findFirstMatchIn(js).map(_.group(1)).getOrElse(s"no type in $js")
      case ElasticFailure(e) => fail(s"[$index] ${e.message}")
    }

  "CREATE TABLE … AS SELECT" should "map each width as DuckDB types it (AC5)" in {
    Seq(
      s"SELECT id, DATEDIFF(d, d2) AS v FROM $table"                  -> "long",
      s"SELECT id, ROUND(x) AS v FROM $table"                         -> "double",
      s"SELECT id, ROUND(n) AS v FROM $table"                         -> "integer",
      s"SELECT id, ROUND(r, 1) AS v FROM $table"                      -> "float",
      s"SELECT pk, MAX(DATEDIFF(d, d2)) AS v FROM $table GROUP BY pk" -> "long",
      s"SELECT pk, SUM(DATEDIFF(d, d2)) AS v FROM $table GROUP BY pk" -> "long",
      s"SELECT pk, SUM(ROUND(n)) AS v FROM $table GROUP BY pk"        -> "long",
      s"SELECT id, SQRT(pos) AS v FROM $table"                        -> "double"
    ).zip(ctasTables).foreach { case ((select, expected), target) =>
      withClue(s"[$select] ") {
        runOk(s"CREATE TABLE $target AS $select")
        mappedType(target, "v") shouldBe expected
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // INSERT … SELECT (A-38)
  // -------------------------------------------------------------------------------------------

  private def stored(index: String): Seq[(String, Map[String, Any])] = {
    client.refresh(index)
    val js = parse(raw("GET", s"/$index/_search?size=100"))
    (js \ "hits" \ "hits").children.map { h =>
      val id = (h \ "_id").values.toString
      val source = (h \ "_source").values match {
        case m: Map[_, _] => m.asInstanceOf[Map[String, Any]]
        case _            => Map.empty[String, Any]
      }
      id -> source
    }
  }

  "INSERT … SELECT" should "refuse a computed-only SELECT that omits the target's key, before any write (A-38)" in {
    runOk(s"CREATE TABLE $pk1 (id KEYWORD, k INT, PRIMARY KEY (id))")
    runOk(s"CREATE TABLE $pk2 (id KEYWORD, pk KEYWORD, k INT, PRIMARY KEY (id, pk))")
    runOk(s"CREATE TABLE $nokey (k INT, v INT)")
    Seq(pk1 -> "id", pk2 -> "id,pk").foreach { case (target, keys) =>
      withClue(s"[$target] ") {
        run(s"INSERT INTO $target SELECT n + 1 AS k FROM $table") match {
          case Left(error) =>
            error should include("PRIMARY KEY / PARTITION BY")
            error should include(s"[$keys]")
          case Right(r) => fail(s"answered $r")
        }
        stored(target) shouldBe empty
      }
    }
    // the key projected: one document per source row, holding the projected columns only
    runOk(s"INSERT INTO $pk1 SELECT CONCAT(id, '') AS id, n + 1 AS k FROM $table")
    val keyed = stored(pk1)
    keyed.map(_._1).distinct.size shouldBe docs.size
    keyed.foreach { case (id, source) =>
      withClue(s"$id ")(source.keySet -- Set("id", "k") shouldBe empty)
    }
    // no key: the documents hold the projected columns, no source field
    runOk(s"INSERT INTO $nokey SELECT n + 1 AS k, n * 2 AS v FROM $table")
    stored(nokey).foreach { case (id, source) =>
      withClue(s"$id ")(source.keySet.intersect(Set("n", "big", "name", "pk", "s")) shouldBe empty)
    }
  }

  // -------------------------------------------------------------------------------------------
  // Quoted numbers (AC9, D16, A-42)
  // -------------------------------------------------------------------------------------------

  "A quoted number beside a whole type" should "be the whole number it rounds to, at every site (AC9)" in {
    // (WHERE, ids) -- DuckDB 1.5.5 casts the text to the column's type, half away from zero
    val where = Seq(
      "n = '1.5'"                     -> Set("r1"),
      "n = '2.5'"                     -> Set("r2"),
      "n = '-1.5'"                    -> Set("r3"),
      "n = '-2.5'"                    -> Set.empty[String],
      "n > '1.5'"                     -> Set("r2", "r5", "r6"),
      "n < '2.5'"                     -> Set("r1", "r3"),
      "n <> '1.5'"                    -> Set("r2", "r3", "r5", "r6"),
      "n = '1e3'"                     -> Set("r6"),
      "n = ' 1.5 '"                   -> Set("r1"),
      "n = '2'"                       -> Set("r1"),
      "n IN ('1.5', '3')"             -> Set("r1", "r2"),
      "n NOT IN ('1.5')"              -> Set("r2", "r3", "r5", "r6"),
      "n BETWEEN '1.5' AND '3.4'"     -> Set("r1", "r2"),
      "n NOT BETWEEN '1.5' AND '3.4'" -> Set("r3", "r5", "r6"),
      "big > '41.5'"                  -> Set("r3", "r5"),
      "big >= '41.5'"                 -> Set("r1", "r3", "r5"),
      "ti > '126.5'"                  -> Set.empty[String],
      "ti >= '126.5'"                 -> Set("r1"),
      "ti = '127.5'"                  -> Set.empty[String],
      "NULLIF(n, 0) = '1.5'"          -> Set("r1"),
      "NULLIF(n, 0) > '2.5'"          -> Set("r5", "r6"),
      "DATEDIFF(d, d2) = '29.5'"      -> Set("r1"),
      "ROUND(n) = '2.5'"              -> Set("r2"),
      "YEAR(d) = '2024.4'"            -> Set("r1", "r2", "r5", "r6"),
      "x = '1.5'"                     -> Set("r3"),
      // A-42 -- a literal on the left reads as its mirror
      "'1.5' < n"              -> Set("r2", "r5", "r6"),
      "'2' < n"                -> Set("r2", "r5", "r6"),
      "'2' = n"                -> Set("r1"),
      "'2' <> n"               -> Set("r2", "r3", "r5", "r6"),
      "'1.5' < COALESCE(n, 0)" -> Set("r2", "r5", "r6"),
      "'2024-01-31' = d"       -> Set("r1"),
      "'2024-01-31' < d"       -> Set("r2", "r5", "r6"),
      "'true' = b"             -> Set("r1", "r3", "r6")
    )
    val wrong = where.flatMap { case (w, expected) =>
      run(s"SELECT id FROM $table WHERE $w") match {
        case Left(error) => Seq(s"[$w] failed: $error")
        case Right(r) =>
          val got = answer(r)._1.map(row => String.valueOf(row("id"))).toSet
          if (got != expected) Seq(s"[$w] $got, not $expected") else Nil
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n")(wrong shouldBe empty)
    // each literal-left row equals its mirror
    Seq(
      "'1.5' < n"              -> "n > '1.5'",
      "'2' = n"                -> "n = '2'",
      "'2' <> n"               -> "n <> '2'",
      "'2024-01-31' < d"       -> "d > '2024-01-31'",
      "'true' = b"             -> "b = 'true'",
      "'1.5' < COALESCE(n, 0)" -> "COALESCE(n, 0) > '1.5'"
    ).foreach { case (left, mirror) =>
      withClue(s"[$left] vs [$mirror] ") {
        idsOf(s"SELECT id FROM $table WHERE $left") shouldBe idsOf(
          s"SELECT id FROM $table WHERE $mirror"
        )
      }
    }
  }

  it should "read as that number in arithmetic, NULLIF, CASE and group filters" in {
    def v(sql: String): Map[String, Any] =
      rowsOf(sql).map(r => String.valueOf(r("id")) -> r("v")).toMap
    v(s"SELECT id, n + '1.5' AS v FROM $table WHERE id = 'r1'") shouldBe Map(
      "r1" -> java.lang.Long.valueOf(4L)
    )
    v(s"SELECT id, n * '1.5' AS v FROM $table WHERE id = 'r2'") shouldBe Map(
      "r2" -> java.lang.Long.valueOf(6L)
    )
    v(s"SELECT id, NULLIF(n, '1.5') AS v FROM $table WHERE id IN ('r1', 'r2')") shouldBe Map(
      "r1" -> null,
      "r2" -> Integer.valueOf(3)
    )
    v(s"SELECT id, CASE WHEN n = '1.5' THEN 1 ELSE 0 END AS v FROM $table WHERE id IN ('r1', 'r2')")
      .map { case (k, x) => k -> x.asInstanceOf[Number].intValue } shouldBe Map(
      "r1" -> 1,
      "r2" -> 0
    )
    v(s"SELECT id, CASE n WHEN '1.5' THEN 1 ELSE 0 END AS v FROM $table WHERE id IN ('r1', 'r2')")
      .map { case (k, x) => k -> x.asInstanceOf[Number].intValue } shouldBe Map(
      "r1" -> 1,
      "r2" -> 0
    )
    v(s"SELECT id, CASE WHEN '1.5' < n THEN 1 ELSE 0 END AS v FROM $table WHERE id IN ('r1', 'r2')")
      .map { case (k, x) => k -> x.asInstanceOf[Number].intValue } shouldBe Map(
      "r1" -> 0,
      "r2" -> 1
    )
    // group filters: MAX(n) per pk a 3, b -2, c 423789293; '2.5' is 3, '2.4' is 2
    rowsOf(s"SELECT pk, MAX(n) AS m FROM $table GROUP BY pk HAVING MAX(n) > '2.5'")
      .map(_("pk"))
      .toSet shouldBe
    Set("c")
    rowsOf(s"SELECT pk, MAX(n) AS m FROM $table GROUP BY pk HAVING MAX(n) > '2.4'")
      .map(_("pk"))
      .toSet shouldBe
    Set("a", "c")
    rowsOf(s"SELECT pk, MAX(n) AS m FROM $table GROUP BY pk HAVING '2.4' < MAX(n)")
      .map(_("pk"))
      .toSet shouldBe
    Set("a", "c")
    rowsOf(s"SELECT pk, SUM(n) + '1.5' AS v FROM $table GROUP BY pk")
      .map(r => r("pk") -> r("v"))
      .toMap shouldBe Map[Any, Any](
      "a" -> java.lang.Long.valueOf(7L),
      "b" -> java.lang.Long.valueOf(0L),
      "c" -> java.lang.Long.valueOf(423790295L)
    )
    // HAVING key filters: the group the rounded number names
    def groups(sql: String): Set[Any] = rowsOf(sql).map(_("k")).toSet
    groups(s"SELECT n AS k, COUNT(*) AS c FROM $table GROUP BY n HAVING n = '1.5'") shouldBe
    Set(Integer.valueOf(2))
    groups(s"SELECT n AS k, COUNT(*) AS c FROM $table GROUP BY n HAVING k = '2.5'") shouldBe
    Set(Integer.valueOf(3))
    groups(
      s"SELECT n AS k, COUNT(*) AS c FROM $table GROUP BY n HAVING n IN ('1.5', 'NaN')"
    ) shouldBe
    Set(Integer.valueOf(2))
    groups(
      s"SELECT DATEDIFF(d, d2) AS k, COUNT(*) AS c FROM $table GROUP BY DATEDIFF(d, d2) HAVING k = '29.5'"
    ) shouldBe Set(java.lang.Long.valueOf(30L))
  }
}
