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
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.json4s.jackson.JsonMethods.{compact, render}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** The object an entity extraction reads (`searchAsUnchecked`), for [[DottedColumnSpec]]. */
case class DottedColumnObj(a: Option[Int])

/** One row of `SELECT id, obj.a FROM dotted_cols` read as an entity, for [[DottedColumnSpec]]. */
case class DottedColumnRow(id: String, obj: Option[DottedColumnObj])

/** An un-aliased dotted column (`SELECT obj.a FROM t`) carries the document's value under its
  * output name `obj.a`, on every route core builds rows on, as its aliased form (`obj.a AS k`)
  * carries it under `k` -- where it used to keep the `_source` shape (`obj -> {a: 1}`) beside a
  * null-filled `obj.a`.
  *
  * The documents are written as Elasticsearch stores them (a flat spelling stays flat), one per
  * shape the reader must handle: nested (`dn`), flat (`df`), both spellings (`db`), a flat `null`
  * beside the nested value (`dz`) and its nested twin (`dn4`), an array of objects (`dl`), a
  * sibling only (`ds`), missing (`dm`), a deeper path (`dx`) and a mixed spelling (`dmx`). The
  * index has two shards, so the scroll is sliced on Elasticsearch 8.
  *
  * PUBLIC API ONLY: the same file runs on a main clone, which is how the "census" (main's answers,
  * the canary) and the "population" (every statement on every route, as canonical rows) are
  * measured against the branch -- `DOTTED_CENSUS_OUT` / `DOTTED_POPULATION_OUT` name the files.
  */
trait DottedColumnSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  protected val table = "dotted_cols"
  protected val uniqueTable = "dotted_cols_u"
  protected val copyTable = "dotted_cols_t2"
  protected val scriptedTable = "dotted_cols_t2s"
  protected val changelogTable = "dotted_cl"

  /** The documents of [[table]], as written (`_id` = `id`). */
  protected val docs: Seq[(String, String)] = Seq(
    "dn" ->
    """{"id":"dn","n":1,"dbl":1.5,"a":7,"obj":{"a":1},"children":[{"name":"c1","addr":{"city":"Paris"}},{"name":"c2"}]}""",
    "df"  -> """{"id":"df","n":2,"dbl":2.5,"obj.a":2}""",
    "db"  -> """{"id":"db","n":3,"obj.a":3,"obj":{"a":30}}""",
    "dz"  -> """{"id":"dz","n":4,"obj.a":null,"obj":{"a":4}}""",
    "dn4" -> """{"id":"dn4","n":5,"obj":{"a":4}}""",
    "dl"  -> """{"id":"dl","n":6,"obj":[{"a":5},{"a":50}]}""",
    "ds"  -> """{"id":"ds","n":7,"obj":{"b":6}}""",
    "dm"  -> """{"id":"dm","n":8,"children":[{"name":"c3"}]}""",
    "dx"  -> """{"id":"dx","obj":{"x":{"y":9}}}""",
    "dmx" -> """{"id":"dmx","obj":{"x.y":10}}"""
  )

  /** What `obj.a` holds per document, as Elasticsearch reads it (D-z holds 4: a flat null is no
    * value; D-l is multi-valued, which stays NULL until SCHEMA-6; D-mx is `obj.x.y`).
    */
  protected val valueOfA: Map[String, Any] = Map(
    "dn"  -> 1,
    "df"  -> 2,
    "db"  -> 3,
    "dz"  -> 4,
    "dn4" -> 4,
    "dl"  -> null,
    "ds"  -> null,
    "dm"  -> null,
    "dx"  -> null,
    "dmx" -> null
  )

  /** [[uniqueTable]]: every document carries one `obj.a` (nested or flat, no array, no flat null).
    */
  protected val uniqueDocs: Seq[(String, String)] = Seq(
    "u1" -> """{"id":"u1","n":-2,"obj":{"a":1}}""",
    "u2" -> """{"id":"u2","n":10,"obj.a":2}""",
    "u3" -> """{"id":"u3","n":3,"obj":{"a":4}}""",
    "u4" -> """{"id":"u4","n":5,"obj":{"a":5}}""",
    "u5" -> """{"id":"u5","obj.a":1}"""
  )

  private val mapping: String =
    """{"properties":{
      |  "id":{"type":"keyword"}, "n":{"type":"integer"}, "dbl":{"type":"double"},
      |  "a":{"type":"integer"},
      |  "obj":{"properties":{"a":{"type":"integer"},"b":{"type":"integer"},
      |    "x":{"properties":{"y":{"type":"integer"}}}}},
      |  "children":{"type":"nested","properties":{"name":{"type":"keyword"},
      |    "addr":{"properties":{"city":{"type":"keyword"}}}}}
      |}}""".stripMargin

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(client.deleteIndex(index))
  }

  private def load(index: String, shards: Int, documents: Seq[(String, String)]): Unit = {
    client
      .createIndex(index, settings = s"""{"number_of_shards": $shards, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mapping).get shouldBe true
    documents.foreach { case (id, source) =>
      client.index(index, id, source) match {
        case ElasticSuccess(true) =>
        case other                => fail(s"indexing $id into $index: $other")
      }
    }
    client.refresh(index)
  }

  protected def runOk(sql: String): QueryResult =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error)  => fail(s"[$sql] failed: ${error.message}")
    }

  override def beforeAll(): Unit = {
    super.beforeAll()
    Seq(table, uniqueTable, copyTable, scriptedTable, changelogTable).foreach(drop)
    load(table, 2, docs)
    load(uniqueTable, 1, uniqueDocs)
    runOk(s"CREATE TABLE $copyTable (id KEYWORD, obj STRUCT FIELDS(a INT), PRIMARY KEY (id))")
    Seq(scriptedTable, changelogTable).foreach { t =>
      runOk(
        s"CREATE TABLE $t (id KEYWORD, obj STRUCT FIELDS(a INT), " +
        "twice INT SCRIPT AS (obj.a * 2), PRIMARY KEY (id))"
      )
    }
    ()
  }

  override def afterAll(): Unit = {
    Seq(table, uniqueTable, copyTable, scriptedTable, changelogTable).foreach(drop)
    super.afterAll()
  }

  // -------------------------------------------------------------------------------------------
  // Routes (spec section 4 R) and canonical rows
  // -------------------------------------------------------------------------------------------

  /** Every variant a gateway result can take, as a consumer reads it (arrow's `extractRows`). */
  protected def rowsOfResult(result: QueryResult): Seq[ListMap[String, Any]] = result match {
    case QueryRows(rows, _, _)        => rows
    case QueryStructured(response, _) => response.results
    case QueryStream(stream, _, _) =>
      Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
    case other => fail(s"unexpected result $other")
  }

  /** A row-shaped statement's routes: P1 sync (`search`) and async (`run` -> `searchAsync`, LIMIT
    * => one-shot), P2 sliced (`run`, no LIMIT, 2 shards), P2 sequential (one slice) and P2 sorted
    * (`ORDER BY id` => search_after).
    */
  protected val rowRoutes: Seq[String] = Seq("P1", "P1-async", "P2", "P2-seq", "P2-sorted")

  /** A `UNION ALL`'s: P3 (every leg LIMITed => one `_msearch`), P4 (an un-LIMITed leg => leg by
    * leg), through the gateway and through `search`.
    */
  protected val unionRoutes: Seq[String] = Seq("P3", "P3-search", "P4", "P4-search")

  private def limited(sql: String): String =
    if (sql.contains(" UNION ALL "))
      sql.replace(" UNION ALL ", " LIMIT 100 UNION ALL ") + " LIMIT 100"
    else s"$sql LIMIT 100"

  protected def rowsOn(sql: String, route: String): Either[String, Seq[ListMap[String, Any]]] =
    Try {
      def search(statement: String): Seq[ListMap[String, Any]] =
        client.search(SelectStatement(statement))(NativeContext) match {
          case ElasticSuccess(response) => response.results
          case ElasticFailure(error)    => throw new IllegalStateException(error.message)
        }
      def gateway(statement: String): Seq[ListMap[String, Any]] =
        Await.result(client.run(statement), 60.seconds) match {
          case ElasticSuccess(result) => rowsOfResult(result)
          case ElasticFailure(error)  => throw new IllegalStateException(error.message)
        }
      route match {
        case "P1"        => search(limited(sql))
        case "P1-async"  => gateway(limited(sql))
        case "P2"        => gateway(sql)
        case "P2-sorted" => gateway(s"$sql ORDER BY id")
        case "P2-seq" =>
          Await.result(
            client
              .scroll(SelectStatement(sql), client.defaultScrollConfig.copy(maxSlices = Some(1)))(
                system,
                NativeContext
              )
              .map(_._1)
              .runWith(Sink.seq),
            60.seconds
          )
        case "P3"        => gateway(limited(sql))
        case "P3-search" => search(limited(sql))
        case "P4"        => gateway(sql)
        case "P4-search" => search(sql)
      }
    }.toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}")

  protected def rowsOk(sql: String, route: String): Seq[ListMap[String, Any]] =
    rowsOn(sql, route) match {
      case Right(rows) => rows
      case Left(error) => fail(s"[$route] $sql: $error")
    }

  /** A value with its class, maps and lists in their own order. */
  protected def shown(v: Any): String = v match {
    case null => "null"
    case m: scala.collection.Map[_, _] =>
      m.map { case (k, x) => s"$k=${shown(x)}" }.mkString("{", ",", "}")
    case s: Seq[_] => s.map(shown).mkString("[", ",", "]")
    case other     => s"$other:${other.getClass.getSimpleName}"
  }

  protected def canonical(row: scala.collection.Map[String, Any]): String =
    row.map { case (k, v) => s"$k=${shown(v)}" }.mkString("{", ", ", "}")

  private def tsv(cells: String*): String =
    cells.map(_.replace('\t', ' ').replace('\n', ' ')).mkString("\t")

  /** One line per row (sorted where the route fixes no order), or the error. */
  protected def dumpLines(label: String, sql: String, route: String): Seq[String] =
    rowsOn(sql, route) match {
      case Left(error) => Seq(tsv(route, label, sql, "-", s"ERROR $error"))
      case Right(rows) =>
        val lines = rows.map(canonical)
        // only `P2-sorted` fixes the order of the rows; elsewhere they are compared as a multiset
        val ordered = if (route == "P2-sorted") lines else lines.sorted
        if (ordered.isEmpty) Seq(tsv(route, label, sql, "-", "NO ROWS"))
        else ordered.zipWithIndex.map { case (l, i) => tsv(route, label, sql, i.toString, l) }
    }

  protected def write(env: String, default: String, lines: Seq[String]): Unit = {
    val out = Paths.get(sys.env.getOrElse(env, default))
    Option(out.toAbsolutePath.getParent).foreach(p => Files.createDirectories(p))
    Files.write(out, lines.mkString("", "\n", "\n").getBytes(StandardCharsets.UTF_8))
    log.info(s"wrote ${lines.size} lines to ${out.toAbsolutePath}")
  }

  // -------------------------------------------------------------------------------------------
  // The statements (spec section 4 S and C)
  // -------------------------------------------------------------------------------------------

  protected def rowStatements: Seq[(String, String)] = Seq(
    "S1"  -> s"SELECT obj.a FROM $table",
    "S2"  -> s"SELECT id, obj.a FROM $table",
    "S3"  -> s"SELECT id, obj.x.y FROM $table",
    "S4"  -> s"SELECT id, obj.a, obj.b AS k FROM $table",
    "S5"  -> s"SELECT id, obj, obj.a FROM $table",
    "S6"  -> s"SELECT id, obj.a, obj.a AS k FROM $table",
    "S7"  -> s"""SELECT id, "obj.a" FROM $table""",
    "U1"  -> s"SELECT t.id, t.obj.a FROM $table AS t",
    "U3"  -> s"SELECT *, obj.a FROM $table",
    "U4"  -> s"SELECT id, n AS obj, obj.a FROM $table",
    "U5"  -> s"SELECT id, a AS z, obj.a FROM $table",
    "U6"  -> s"""SELECT id, n AS "obj.a", obj.b FROM $table""",
    "C1"  -> s"SELECT id, obj.a AS k FROM $table",
    "C1x" -> s"SELECT id, obj.x.y AS k FROM $table",
    "C2"  -> s"""SELECT id, obj.a AS "obj.a" FROM $table""",
    "C3a" -> s"SELECT id, n FROM $table",
    "C3b" -> s"SELECT id, n AS m FROM $table",
    "C4a" -> s"SELECT id, n + 1 FROM $table",
    "C4b" -> s"SELECT id, ABS(obj.a) FROM $table",
    "C4c" -> s"SELECT id, obj.a::BIGINT FROM $table",
    "C4d" -> s"SELECT id, obj.a + 1 FROM $table",
    "C5"  -> s"SELECT id, obj FROM $table",
    "C8"  -> s"SELECT * FROM $table"
  )

  /** UNNEST (C7) and GROUP BY (C6) take no `ORDER BY id`. */
  protected def unorderedStatements: Seq[(String, String)] = Seq(
    "C6"  -> s"SELECT obj.a, COUNT(*) AS c FROM $table GROUP BY obj.a",
    "C7a" -> s"SELECT t.id, c.name FROM $table t JOIN UNNEST(t.children) AS c",
    "C7b" -> s"SELECT t.id, t.obj.a, c.name FROM $table t JOIN UNNEST(t.children) AS c",
    "C7c" -> s"SELECT t.id, c.addr.city FROM $table t JOIN UNNEST(t.children) AS c",
    "AGG" -> s"SELECT id, ARRAY_AGG(obj.a) AS l FROM $table GROUP BY id"
  )

  protected def windowStatements: Seq[(String, String)] = Seq(
    "W1" -> s"SELECT id, obj.a, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM $table",
    "W2" -> s"SELECT id, obj.a, ROW_NUMBER() OVER (PARTITION BY obj.a ORDER BY id) AS rn FROM $table",
    "W3" -> s"SELECT id, RANK() OVER (ORDER BY obj.a) AS r FROM $table"
  )

  protected def unionStatements: Seq[(String, String)] = Seq(
    "S8"  -> s"SELECT obj.a FROM $table UNION ALL SELECT n FROM $table",
    "S9"  -> s"SELECT n FROM $table UNION ALL SELECT obj.a FROM $table",
    "S10" -> s"SELECT obj.a FROM $table UNION ALL SELECT dbl FROM $table",
    "C1u" -> s"SELECT obj.a AS k FROM $table UNION ALL SELECT n AS k FROM $table"
  )

  protected def entityStatements: Seq[(String, String)] = Seq(
    // labelled by the subquery's shape: the S1 forms are the ones AC7 changes
    "S1-in-W" -> s"SELECT id FROM $table WHERE n IN (SELECT obj.a FROM $uniqueTable LIMIT 100)",
    "S1-notin-W" -> s"SELECT id FROM $table WHERE n NOT IN (SELECT obj.a FROM $uniqueTable LIMIT 100)",
    "C1-in-W" -> s"SELECT id FROM $table WHERE n IN (SELECT obj.a AS k FROM $uniqueTable LIMIT 100)",
    "C1-notin-W" -> s"SELECT id FROM $table WHERE n NOT IN (SELECT obj.a AS k FROM $uniqueTable LIMIT 100)",
    "C-in-P" -> s"SELECT id FROM $table WHERE n IN (SELECT obj.a FROM $uniqueTable)"
  )

  // -------------------------------------------------------------------------------------------
  // INSERT ... SELECT (spec I) and what it wrote
  // -------------------------------------------------------------------------------------------

  /** Runs the INSERT ... SELECT statements once per suite: the plain copy, the scripted target and
    * the statement extensions generates to preload a materialized-view changelog.
    */
  protected lazy val inserted: Unit = {
    runOk(s"INSERT INTO $copyTable SELECT id, obj.a FROM $table")
    runOk(s"INSERT INTO $scriptedTable SELECT id, obj.a FROM $table")
    runOk(s"INSERT INTO $changelogTable AS SELECT id, obj.a FROM $table;")
    Seq(copyTable, scriptedTable, changelogTable).foreach(client.refresh)
  }

  /** The `_source` of each target document, as stored. */
  protected def storedSource(index: String, id: String): String =
    getDocument(index, id).map(d => compact(render(d \ "_source"))).getOrElse("ABSENT")

  protected def insertLines: Seq[String] = {
    inserted
    Seq(copyTable, scriptedTable, changelogTable).flatMap { target =>
      docs.map(_._1).map(id => tsv("I", target, id, "-", storedSource(target, id))) ++
      dumpLines(
        s"I-agg-$target",
        s"SELECT SUM(obj.a) AS s, COUNT(obj.a) AS c FROM $target",
        "P1-async"
      ) ++ dumpLines(s"S2-read-$target", s"SELECT id, obj.a FROM $target", "P2-sorted")
    }
  }

  protected def entityRows: Either[String, Seq[DottedColumnRow]] =
    client.searchAsUnchecked[DottedColumnRow](
      SelectStatement(s"SELECT id, obj.a FROM $table WHERE n <= 5 ORDER BY id")
    ) match {
      case ElasticSuccess(rows) => Right(rows)
      case ElasticFailure(e)    => Left(e.message)
    }

  // -------------------------------------------------------------------------------------------
  // The census (T0-3) and the population (AC3): dumps, main vs branch
  // -------------------------------------------------------------------------------------------

  "An un-aliased dotted column" should "dump the census of the row paths (T0-3)" in {
    val lines =
      rowStatements
        .filter(s => s._1.startsWith("S") || s._1.startsWith("U") || s._1 == "C1")
        .flatMap { case (l, sql) => Seq("P1", "P2").flatMap(r => dumpLines(l, sql, r)) } ++
      unorderedStatements.flatMap { case (l, sql) => dumpLines(l, sql, "P1-async") } ++
      windowStatements.flatMap { case (l, sql) => dumpLines(l, sql, "P2") } ++
      unionStatements.flatMap { case (l, sql) =>
        Seq("P3", "P4").flatMap(r => dumpLines(l, sql, r))
      } ++
      entityStatements.flatMap { case (l, sql) => dumpLines(l, sql, "P1-async") } ++
      insertLines
    write("DOTTED_CENSUS_OUT", "target/dotted-census.tsv", lines)
    // the canary, read on main: S rows null-filled beside the leaked parent
    val canary = lines.count(l => l.contains("obj.a=null") && l.contains("obj={"))
    log.info(s"census canary: $canary rows carry obj.a=null beside an obj object")
    lines should not be empty
  }

  it should "dump the population of every statement on every route (AC3)" in {
    val lines =
      rowStatements.flatMap { case (l, sql) => rowRoutes.flatMap(r => dumpLines(l, sql, r)) } ++
      unorderedStatements.flatMap { case (l, sql) =>
        Seq("P1", "P1-async", "P2", "P2-seq").flatMap(r => dumpLines(l, sql, r))
      } ++
      windowStatements.flatMap { case (l, sql) =>
        Seq("P1", "P1-async", "P2").flatMap(r => dumpLines(l, sql, r))
      } ++
      unionStatements.flatMap { case (l, sql) => unionRoutes.flatMap(r => dumpLines(l, sql, r)) } ++
      entityStatements.flatMap { case (l, sql) =>
        Seq("P1", "P2").flatMap(r => dumpLines(l, sql, r))
      } ++
      (entityRows match {
        case Right(rows) =>
          rows.zipWithIndex.map { case (r, i) => tsv("E", "searchAs", "-", i.toString, r.toString) }
        case Left(e) => Seq(tsv("E", "searchAs", "-", "-", s"ERROR $e"))
      }) ++
      insertLines
    write("DOTTED_POPULATION_OUT", "target/dotted-population.tsv", lines)
    lines.exists(_.contains("\tS2\t")) shouldBe true
  }

  // -------------------------------------------------------------------------------------------
  // AC1: values on every route, per document, against the aliased control
  // -------------------------------------------------------------------------------------------

  private def byId(rows: Seq[ListMap[String, Any]]): Map[String, ListMap[String, Any]] =
    rows.map(r => String.valueOf(r.getOrElse("id", null)) -> r).toMap

  private def cls(v: Any): String = if (v == null) "null" else v.getClass.getName

  it should "carry the value its aliased form carries, per document, on every route (values)" in {
    rowRoutes.foreach { route =>
      withClue(s"[$route] ") {
        val s2 = byId(rowsOk(s"SELECT id, obj.a FROM $table", route))
        val c1 = byId(rowsOk(s"SELECT id, obj.a AS k FROM $table", route))
        s2.keySet shouldBe docs.map(_._1).toSet
        docs.map(_._1).foreach { id =>
          withClue(s"$id: ") {
            s2(id)("obj.a") shouldBe valueOfA(id)
            // A-3: over D-z the walked column reads ES's 4, where the alias loop keeps the null
            if (id == "dz") c1(id)("k") shouldBe (null: Any)
            else {
              s2(id)("obj.a") shouldBe c1(id)("k")
              cls(s2(id)("obj.a")) shouldBe cls(c1(id)("k"))
            }
          }
        }
        // S1 / S7 / U1 carry the same multiset of values
        val values = docs.map(d => valueOfA(d._1)).map(String.valueOf).sorted
        rowsOk(s"SELECT obj.a FROM $table", route)
          .map(r => String.valueOf(r("obj.a")))
          .sorted shouldBe values
        rowsOk(s"""SELECT id, "obj.a" FROM $table""", route)
          .map(r => String.valueOf(r("obj.a")))
          .sorted shouldBe values
        rowsOk(s"SELECT t.id, t.obj.a FROM $table AS t", route)
          .map(r => String.valueOf(r("obj.a")))
          .sorted shouldBe values
        // S3: a deeper path; a mixed spelling stays NULL (D4)
        val s3 = byId(rowsOk(s"SELECT id, obj.x.y FROM $table", route))
        s3("dx")("obj.x.y") shouldBe 9
        s3("dmx")("obj.x.y") shouldBe (null: Any)
        // S4 / S6 / U4: the dotted cell beside an alias; S6 reads its alias (A-6)
        val s4 = byId(rowsOk(s"SELECT id, obj.a, obj.b AS k FROM $table", route))
        docs.map(_._1).foreach(id => withClue(s"S4 $id: ")(s4(id)("obj.a") shouldBe valueOfA(id)))
        val s6 = byId(rowsOk(s"SELECT id, obj.a, obj.a AS k FROM $table", route))
        docs.map(_._1).foreach(id => withClue(s"S6 $id: ")(s6(id)("obj.a") shouldBe s6(id)("k")))
        s6("dn")("obj.a") shouldBe 1
        val u6 = byId(rowsOk(s"""SELECT id, n AS "obj.a", obj.b FROM $table""", route))
        u6("ds")("obj.b") shouldBe 6
        u6("ds")("obj.a") shouldBe 7
      }
    }
  }

  it should "carry an un-aliased dotted leg's values in a UNION ALL, one-shot and leg by leg (values)" in {
    val dotted = docs.map(d => valueOfA(d._1))
    val n = rowsOk(s"SELECT id, n FROM $table", "P2-sorted").map(_("n"))
    def sortedOf(xs: Seq[Any]): Seq[String] = xs.map(String.valueOf).sorted
    unionRoutes.foreach { route =>
      withClue(s"[$route] ") {
        rowsOk(s"SELECT obj.a FROM $table UNION ALL SELECT n FROM $table", route)
          .map(_("obj.a")) should contain theSameElementsAs (dotted ++ n)
        rowsOk(s"SELECT n FROM $table UNION ALL SELECT obj.a FROM $table", route)
          .map(_("n")) should contain theSameElementsAs (n ++ dotted)
        // S10 converts the INT leg to the result's DOUBLE
        val s10 = rowsOk(s"SELECT obj.a FROM $table UNION ALL SELECT dbl FROM $table", route)
          .map(_("obj.a"))
        s10.filter(_ != null).foreach(v => v shouldBe a[java.lang.Double])
        // the legs come in leg order: the first `docs.size` rows are the dotted leg's
        sortedOf(s10.take(docs.size)) shouldBe sortedOf(dotted.map {
          case null   => null
          case i: Int => i.toDouble
          case other  => other
        })
      }
    }
  }

  it should "find a window's partition and rank by the value it shows (values)" in {
    Seq("P1", "P2").foreach { route =>
      withClue(s"[$route] ") {
        val w1 = byId(rowsOk(windowStatements.head._2, route))
        docs.map(_._1).foreach(id => withClue(s"W1 $id: ")(w1(id)("obj.a") shouldBe valueOfA(id)))
        // W2: the partition key reads the base row's `obj.a`
        val w2 = byId(rowsOk(windowStatements(1)._2, route))
        String.valueOf(w2("dn4")("rn")) shouldBe "1"
        String.valueOf(w2("dz")("rn")) shouldBe "2"
        String.valueOf(w2("dn")("rn")) shouldBe "1"
        // W3: D-z and its nested twin tie (A-3)
        val w3 = byId(rowsOk(windowStatements(2)._2, route))
        w3("dz")("r") shouldBe w3("dn4")("r")
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC2: no leaked parent, but every parent that may carry a value
  // -------------------------------------------------------------------------------------------

  it should "drop the parent it read and keep every other one (parent)" in {
    rowRoutes.foreach { route =>
      withClue(s"[$route] ") {
        val s2 = byId(rowsOk(s"SELECT id, obj.a FROM $table", route))
        Seq("dn", "dz", "dn4").foreach(id =>
          withClue(s"$id: ")(s2(id).keySet shouldBe Set("id", "obj.a"))
        )
        // SCHEMA-2 (D9): where a non-null flat spelling won, or the parent is an array, it is
        // dropped too -- SCHEMA-12 A-1's recorded leak ends: the projection's columns only
        Seq("db", "dl").foreach(id => withClue(s"$id: ")(s2(id).keySet shouldBe Set("id", "obj.a")))
        val s3 = byId(rowsOk(s"SELECT id, obj.x.y FROM $table", route))
        Seq("dx", "dmx").foreach(id =>
          withClue(s"$id: ")(s3(id).keySet shouldBe Set("id", "obj.x.y"))
        )
        // a requested parent and a star keep it: it is one of their columns (A-5, D5)
        Seq(
          s"SELECT id, obj, obj.a FROM $table",
          s"SELECT *, obj.a FROM $table"
        ).foreach { sql =>
          withClue(s"$sql: ")(byId(rowsOk(sql, route))("dn") should contain key "obj")
        }
        // an aliased source under it, an alias collision, a column beside it: SCHEMA-2 (D9) drops
        // it -- the projection's columns only --, each column keeping its value
        val aliased = byId(rowsOk(s"SELECT id, obj.a, obj.b AS k FROM $table", route))
        aliased("dn").keySet shouldBe Set("id", "obj.a", "k")
        String.valueOf(aliased("ds")("k")) shouldBe "6"
        val collision = byId(rowsOk(s"SELECT id, obj.a, obj.a AS k FROM $table", route))
        collision("dn").keySet shouldBe Set("id", "obj.a", "k")
        String.valueOf(collision("dn")("k")) shouldBe "1"
        val beside = byId(rowsOk(s"SELECT id, a AS z, obj.a FROM $table", route))
        beside("dn").keySet shouldBe Set("id", "z", "obj.a")
        String.valueOf(beside("dn")("z")) shouldBe "7"
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC6: INSERT ... SELECT writes main's documents, and they read back with their values
  // -------------------------------------------------------------------------------------------

  it should "write main's documents from an INSERT ... SELECT, which read back with their values (insert)" in {
    inserted
    Seq(copyTable, scriptedTable, changelogTable).foreach { target =>
      withClue(s"[$target] ") {
        // D13: a nested source is written nested, as on main (`{"obj":{"a":1},"id":"dn"}`): the
        // row's null `obj.a` cell is not written, and no flat spelling carries the value
        val dn = org.json4s.jackson.JsonMethods.parse(storedSource(target, "dn"))
        (dn \ "obj.a") shouldBe org.json4s.JNothing
        (dn \ "obj" \ "a") shouldBe org.json4s.JInt(1)
        // the un-aliased read walks to the nested value, which main null-filled
        val back = byId(rowsOk(s"SELECT id, obj.a FROM $target", "P2-sorted"))
        Seq("dn", "df", "db", "dz", "dn4").foreach { id =>
          withClue(s"$id: ")(back(id)("obj.a") shouldBe valueOfA(id))
        }
      }
    }
  }

  // -------------------------------------------------------------------------------------------
  // AC7: the entity context
  // -------------------------------------------------------------------------------------------

  it should "resolve a LIMITed WHERE subquery over the column as its aliased form does (entity)" in {
    def ids(sql: String): Set[String] =
      rowsOk(sql, "P1-async").map(r => String.valueOf(r("id"))).toSet
    val in = ids(entityStatements.head._2)
    in shouldBe ids(entityStatements(2)._2)
    in shouldBe Set("dn", "df", "dz", "dn4")
    val notIn = ids(entityStatements(1)._2)
    notIn shouldBe ids(entityStatements(3)._2)
    notIn should not be empty
    // mode P (no LIMIT) reads bucket rows: the unchanged control
    ids(entityStatements(4)._2) shouldBe in
    // an entity extraction reads the document's shape, as on main
    entityRows shouldBe Right(
      Seq(
        DottedColumnRow("db", Some(DottedColumnObj(Some(30)))),
        DottedColumnRow("df", None),
        DottedColumnRow("dn", Some(DottedColumnObj(Some(1)))),
        DottedColumnRow("dn4", Some(DottedColumnObj(Some(4)))),
        DottedColumnRow("dz", Some(DottedColumnObj(Some(4))))
      )
    )
  }

  // core rendered `CAST(NULL AS VARCHAR)` as `String.valueOf(null)`: the TEXT "null" on every row,
  // alone in the projection (a regression vs 0.23.0) and beside other columns
  "A NULL cast to VARCHAR" should "answer NULL, alone and beside other columns, on every route" in {
    def cell(v: Any): Any = v match {
      case s: Iterable[_] if s.size == 1 => s.head // a script field arrives as a one-element list
      case other                         => other
    }
    rowRoutes.foreach { route =>
      Seq(
        s"SELECT CAST(NULL AS VARCHAR) AS sv FROM $table",
        s"SELECT id, CAST(NULL AS VARCHAR) AS sv, CAST(NULL AS INT) AS iv FROM $table"
      ).foreach { sql =>
        withClue(s"[$route] $sql: ") {
          val rows = rowsOk(sql, route)
          rows should not be empty
          rows.foreach { row =>
            row.keys should contain("sv")
            cell(row("sv")) shouldBe (null: Any)
          }
        }
      }
    }
  }
}
