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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalDate, LocalDateTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

/** What a GROUP BY, an UNNEST and a formatted column answer, executed on Elasticsearch, against the
  * answer of DuckDB 1.5.5 over the same eight rows (the expected values below ARE DuckDB's).
  *
  * The fixture: four groups of `g1` -- `x` (three rows), `y` (two), `z` (two rows with no `n`, no
  * `x` and no `ts`) and `w` (one row) -- so that a group with documents but no value of a field,
  * and a group of one value, are both there.
  */
trait GroupedAggregateSemanticsSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "grouped_aggregate_semantics"

  override def beforeAll(): Unit = {
    super.beforeAll()
    val mapping =
      """{
        |  "properties": {
        |    "id": { "type": "keyword" },
        |    "g1": { "type": "keyword" },
        |    "k":  { "type": "keyword" },
        |    "n":  { "type": "integer" },
        |    "x":  { "type": "double" },
        |    "d":  { "type": "date" },
        |    "ts": { "type": "date" },
        |    "kd": { "type": "keyword" },
        |    "items": {
        |      "type": "nested",
        |      "properties": {
        |        "sku": { "type": "keyword" },
        |        "qty": { "type": "integer" },
        |        "at":  { "type": "date" }
        |      }
        |    }
        |  }
        |}""".stripMargin
    client.createIndex(index, mappings = None, aliases = Nil).get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = List(
      """{"id":"a1","g1":"x","k":"a","n":5,"x":1.5,"d":"2024-01-03","ts":"2025-03-31T10:00:00Z",
        |"kd":"2025-03-31T10:00:00","items":[{"sku":"s1","qty":2,"at":"2024-01-03"},{"sku":"s2","qty":1,"at":"2023-05-01"}]}""",
      """{"id":"a2","g1":"x","k":"b","n":-900,"x":2.5,"d":"2024-01-03","ts":"2024-02-01T00:00:00Z",
        |"items":[{"sku":"s1","qty":3,"at":"2024-02-10"},{"sku":"s1","qty":4,"at":"2024-03-01"}]}""",
      """{"id":"a3","g1":"y","k":"a","n":900,"d":"2024-03-11","ts":"2024-07-04T00:00:00Z",
        |"kd":"2024-07-04T00:00:00","items":[{"sku":"s3","qty":1,"at":"2024-06-01"}]}""",
      """{"id":"a4","g1":"y","k":"c","n":0,"x":0.5,"d":"2024-03-11"}""",
      """{"id":"a5","g1":"z","k":"b","d":"2024-05-20"}""",
      """{"id":"a6","g1":"z","k":"a","d":"2024-05-20"}""",
      """{"id":"a7","g1":"w","k":"c","n":7,"x":3.0,"d":"2024-12-30","ts":"2024-12-30T12:00:00Z"}""",
      """{"id":"a8","g1":"x","k":"a","n":-4,"d":"2024-01-03","ts":"2025-01-15T00:00:00Z"}"""
    ).map(_.stripMargin.replace("\n", ""))

    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 10)

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) =>
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }
    client.refresh(index)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    super.afterAll()
  }

  /** The rows of a statement, through `GatewayApi.run` (JDBC / REPL / Flight SQL). */
  private def rowsOf(sql: String): Seq[ListMap[String, Any]] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _)) =>
        Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case ElasticSuccess(other) => fail(s"[$sql] unexpected result $other")
      case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    }

  /** A cell, unwrapped from the one-element list a script field comes back as. */
  private def cell(row: ListMap[String, Any], column: String): Any =
    row.getOrElse(column, fail(s"no column [$column] in $row")) match {
      case Seq(one) => one
      case other    => other
    }

  private def number(value: Any): Option[Double] = value match {
    case null                => None
    case n: java.lang.Number => Some(n.doubleValue())
    case Seq(one)            => number(one)
    case other               => fail(s"[$other] (${other.getClass.getName}) is not a number")
  }

  private def instant(value: Any): Option[Instant] = value match {
    case null                => None
    case z: ZonedDateTime    => Some(z.toInstant)
    case l: LocalDateTime    => Some(l.toInstant(ZoneOffset.UTC))
    case d: LocalDate        => Some(d.atStartOfDay(ZoneOffset.UTC).toInstant)
    case i: Instant          => Some(i)
    case Seq(one)            => instant(one)
    case other               => fail(s"[$other] (${other.getClass.getName}) is not a timestamp")
  }

  private def at(text: String): Option[Instant] = Some(Instant.parse(text))

  /** The value of `column` per group `g1`. */
  private def byGroup[T](sql: String, column: String)(read: Any => T): Map[String, T] =
    rowsOf(sql).map(r => String.valueOf(cell(r, "g1")) -> read(cell(r, column))).toMap

  // ---------------------------------------------------------------------------------------------
  // An aggregate under a GROUP BY whose first key is an expression
  // ---------------------------------------------------------------------------------------------

  private val caseKey = "CASE WHEN d > '2024-02-01' THEN k ELSE 'x' END"

  private def keyCounts(sql: String): Set[(String, String, Option[Double])] =
    rowsOf(sql)
      .map(r => (String.valueOf(cell(r, "kk")), String.valueOf(cell(r, "g1")), number(cell(r, "c"))))
      .toSet

  "COUNT(*) under an expression key followed by a column key" should
  "answer the count of each group, with or without a HAVING" in {
    // DuckDB: (a,y,1) (a,z,1) (b,z,1) (c,w,1) (c,y,1) (x,x,3)
    val expected = Set(
      ("a", "y", Some(1.0)),
      ("a", "z", Some(1.0)),
      ("b", "z", Some(1.0)),
      ("c", "w", Some(1.0)),
      ("c", "y", Some(1.0)),
      ("x", "x", Some(3.0))
    )
    keyCounts(s"SELECT $caseKey AS kk, g1, COUNT(*) AS c FROM $index GROUP BY kk, g1") shouldBe
    expected
    keyCounts(
      s"SELECT $caseKey AS kk, g1, COUNT(*) AS c FROM $index GROUP BY kk, g1 HAVING kk <> 'x'"
    ) shouldBe expected.filterNot(_._1 == "x")
    // the control: the expression key last
    keyCounts(s"SELECT g1, $caseKey AS kk, COUNT(*) AS c FROM $index GROUP BY g1, kk") shouldBe
    expected
  }

  // ---------------------------------------------------------------------------------------------
  // A scripted GROUP BY key
  // ---------------------------------------------------------------------------------------------

  /** The keys of `sql` in the order they come back, each a number. The NULL group is not asserted
    * here: the levels build no bucket for it (L-7).
    */
  private def numericKeys(sql: String): Seq[Double] =
    rowsOf(sql).map(r => cell(r, "kk")).filter(_ != null).map {
      case n: java.lang.Number => n.doubleValue()
      case other               => fail(s"[$sql] key [$other] (${other.getClass.getName}) is not a number")
    }

  "a scripted GROUP BY key that reports a number" should "come back as a number, in numeric order" in {
    // DuckDB: 900, 7, 5, 0, -4, -900 (then the NULL group)
    numericKeys(
      s"SELECT n + 0 AS kk, COUNT(*) AS c FROM $index GROUP BY n + 0 ORDER BY kk DESC"
    ) shouldBe Seq(900.0, 7.0, 5.0, 0.0, -4.0, -900.0)
    // DuckDB: -1800, -8, 0, 10, 14, 1800
    numericKeys(
      s"SELECT n * 2 AS kk, COUNT(*) AS c FROM $index GROUP BY n * 2 ORDER BY kk ASC"
    ) shouldBe Seq(-1800.0, -8.0, 0.0, 10.0, 14.0, 1800.0)
    // the control: the field key
    numericKeys(
      s"SELECT n AS kk, COUNT(*) AS c FROM $index GROUP BY n ORDER BY n DESC"
    ) shouldBe Seq(900.0, 7.0, 5.0, 0.0, -4.0, -900.0)
  }

  // ---------------------------------------------------------------------------------------------
  // MIN / MAX of a date column
  // ---------------------------------------------------------------------------------------------

  "MIN / MAX of a date column" should "answer the timestamp, and NULL for a group with no value" in {
    // DuckDB: w 2024-12-30 12:00, x 2025-03-31 10:00, y 2024-07-04, z NULL
    byGroup(s"SELECT g1, MAX(ts) AS a FROM $index GROUP BY g1", "a")(instant) shouldBe Map(
      "w" -> at("2024-12-30T12:00:00Z"),
      "x" -> at("2025-03-31T10:00:00Z"),
      "y" -> at("2024-07-04T00:00:00Z"),
      "z" -> None
    )
    // DuckDB: w 2024-12-30, x 2024-01-03, y 2024-03-11, z 2024-05-20
    byGroup(s"SELECT g1, MIN(d) AS a FROM $index GROUP BY g1", "a")(instant) shouldBe Map(
      "w" -> at("2024-12-30T00:00:00Z"),
      "x" -> at("2024-01-03T00:00:00Z"),
      "y" -> at("2024-03-11T00:00:00Z"),
      "z" -> at("2024-05-20T00:00:00Z")
    )
    // DuckDB: the groups whose MAX(ts) is after 2025-01-01 -- x
    byGroup(
      s"SELECT g1, MAX(ts) AS a FROM $index GROUP BY g1 HAVING MAX(ts) > '2025-01-01T00:00:00Z'",
      "a"
    )(instant) shouldBe Map("x" -> at("2025-03-31T10:00:00Z"))
    // over the whole table
    rowsOf(s"SELECT MAX(ts) AS a FROM $index").map(r => instant(cell(r, "a"))) shouldBe
    Seq(at("2025-03-31T10:00:00Z"))
  }

  // ---------------------------------------------------------------------------------------------
  // A group with documents but no value of the field
  // ---------------------------------------------------------------------------------------------

  private def close(expected: Map[String, Option[Double]])(actual: Map[String, Option[Double]]) = {
    actual.keySet shouldBe expected.keySet
    expected.foreach {
      case (g, None) => withClue(s"group $g: ") { actual(g) shouldBe None }
      case (g, Some(v)) =>
        withClue(s"group $g: ") {
          actual(g).getOrElse(fail("NULL")) shouldBe v +- math.max(1e-6, math.abs(v) * 1e-9)
        }
    }
  }

  /** Elasticsearch computes a SAMPLE deviation from 7.7 on: before, every group answers NULL. */
  private lazy val samplesComputed: Boolean = client.asInstanceOf[VersionApi].version match {
    case ElasticSuccess(v) => ElasticsearchVersion.isAtLeast(v, 7, 7)
    case other             => fail(s"$other")
  }

  "STDDEV / VARIANCE of a group" should
  "answer NULL with no value, and NULL for a SAMPLE statistic of one value" in {
    def of(fn: String) = byGroup(s"SELECT g1, $fn(n) AS a FROM $index GROUP BY g1", "a")(number)
    def sampled(expected: Map[String, Option[Double]]) =
      if (samplesComputed) expected else expected.map { case (g, _) => g -> None }
    // DuckDB: w NULL, x 519.9233917928037, y 636.3961030678928, z NULL
    close(
      sampled(
        Map("w" -> None, "x" -> Some(519.9233917928037), "y" -> Some(636.3961030678928), "z" -> None)
      )
    )(of("STDDEV_SAMP"))
    // DuckDB: w NULL, x 270320.3333333333, y 405000.0, z NULL
    close(
      sampled(Map("w" -> None, "x" -> Some(270320.3333333333), "y" -> Some(405000.0), "z" -> None))
    )(of("VAR_SAMP"))
    // DuckDB: w 0.0, x 424.5156717431708, y 450.0, z NULL
    close(Map("w" -> Some(0.0), "x" -> Some(424.5156717431708), "y" -> Some(450.0), "z" -> None))(
      of("STDDEV_POP")
    )
    // DuckDB: w 0.0, x 180213.55555555553, y 202500.0, z NULL
    close(Map("w" -> Some(0.0), "x" -> Some(180213.55555555553), "y" -> Some(202500.0), "z" -> None))(
      of("VAR_POP")
    )
  }

  "a percentile of a group with no value" should "answer NULL, and be kept by HAVING ISNULL" in {
    val p = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY n)"
    // DuckDB: z NULL
    byGroup(s"SELECT g1, $p AS a FROM $index GROUP BY g1 HAVING ISNULL($p)", "a")(number) shouldBe
    Map("z" -> None)
    // the control: the groups with values answer their percentile (DuckDB: w 7.0, x -4.0, y 450.0)
    byGroup(s"SELECT g1, $p AS a FROM $index GROUP BY g1", "a")(number) shouldBe
    Map("w" -> Some(7.0), "x" -> Some(-4.0), "y" -> Some(450.0), "z" -> None)
  }

  // ---------------------------------------------------------------------------------------------
  // A multi-key GROUP BY: LIMIT and ORDER BY
  // ---------------------------------------------------------------------------------------------

  /** DuckDB's groups of `GROUP BY g1, k`. */
  private val twoKeyGroups: Set[(String, String, Double)] = Set(
    ("w", "c", 1.0),
    ("x", "a", 2.0),
    ("x", "b", 1.0),
    ("y", "a", 1.0),
    ("y", "c", 1.0),
    ("z", "a", 1.0),
    ("z", "b", 1.0)
  )

  private def twoKeyRows(sql: String): Seq[(String, String, Double)] =
    rowsOf(sql).map(r =>
      (
        String.valueOf(cell(r, "g1")),
        String.valueOf(cell(r, "k")),
        number(cell(r, "c")).getOrElse(fail(s"[$sql] NULL count in $r"))
      )
    )

  "a multi-key GROUP BY with a LIMIT and no ORDER BY" should "answer at most LIMIT groups" in {
    val limited = twoKeyRows(s"SELECT g1, k, COUNT(*) AS c FROM $index GROUP BY g1, k LIMIT 2")
    limited should have size 2
    limited.toSet.subsetOf(twoKeyGroups) shouldBe true
    // the control: one key
    rowsOf(s"SELECT g1, COUNT(*) AS c FROM $index GROUP BY g1 LIMIT 2") should have size 2
  }

  "a multi-key GROUP BY ordered by a key that is not the first" should
  "answer the groups in the global order" in {
    // DuckDB: c, c, b, b, a, a, a
    val byK = twoKeyRows(s"SELECT g1, k, COUNT(*) AS c FROM $index GROUP BY g1, k ORDER BY k DESC")
    byK.map(_._2) shouldBe Seq("c", "c", "b", "b", "a", "a", "a")
    byK.toSet shouldBe twoKeyGroups
    // DuckDB: (x,a,2) (y,a,1) (z,a,1) (x,b,1) (z,b,1) (w,c,1) (y,c,1)
    twoKeyRows(
      s"SELECT g1, k, COUNT(*) AS c FROM $index GROUP BY g1, k ORDER BY c DESC, k ASC, g1 ASC"
    ) shouldBe Seq(
      ("x", "a", 2.0),
      ("y", "a", 1.0),
      ("z", "a", 1.0),
      ("x", "b", 1.0),
      ("z", "b", 1.0),
      ("w", "c", 1.0),
      ("y", "c", 1.0)
    )
    // DuckDB: (z,a,1) (y,a,1) (x,a,2)
    twoKeyRows(
      s"SELECT g1, k, COUNT(*) AS c FROM $index GROUP BY g1, k ORDER BY k ASC, g1 DESC LIMIT 3"
    ) shouldBe Seq(("z", "a", 1.0), ("y", "a", 1.0), ("x", "a", 2.0))
    // the control: ordered by the first key, then the second
    twoKeyRows(
      s"SELECT g1, k, COUNT(*) AS c FROM $index GROUP BY g1, k ORDER BY g1 ASC, k ASC"
    ) shouldBe twoKeyGroups.toSeq.sortBy(r => (r._1, r._2))
  }

  // ---------------------------------------------------------------------------------------------
  // A WHERE over an UNNEST element
  // ---------------------------------------------------------------------------------------------

  "a WHERE over an UNNEST element" should "answer one row per matching element" in {
    // DuckDB: a1, a2, a2, a3
    rowsOf(
      s"SELECT t.id FROM $index AS t JOIN UNNEST(t.items) AS i WHERE YEAR(i.at) = 2024"
    ).map(r => String.valueOf(cell(r, "id"))).sorted shouldBe Seq("a1", "a2", "a2", "a3")
    // the control: an element column projected (DuckDB: a1 s1, a2 s1, a2 s1, a3 s3)
    rowsOf(
      s"SELECT t.id, i.sku FROM $index AS t JOIN UNNEST(t.items) AS i WHERE YEAR(i.at) = 2024"
    ).map(r => (String.valueOf(cell(r, "id")), String.valueOf(cell(r, "i.sku")))).sorted shouldBe
    Seq(("a1", "s1"), ("a2", "s1"), ("a2", "s1"), ("a3", "s3"))
  }

  // ---------------------------------------------------------------------------------------------
  // A column declared VARCHAR
  // ---------------------------------------------------------------------------------------------

  "a column declared VARCHAR" should "answer its text, never a re-read temporal" in {
    // DuckDB (strftime(CAST(kd AS TIMESTAMP), '%H:%M:%S')): a1 '10:00:00', a3 '00:00:00'
    rowsOf(
      s"SELECT id, DATETIME_FORMAT(CAST(kd AS DATETIME), '%H:%i:%s') AS v FROM $index " +
      "WHERE id IN ('a1', 'a3')"
    ).map(r => String.valueOf(cell(r, "id")) -> cell(r, "v")).toMap shouldBe Map(
      "a1" -> "10:00:00",
      "a3" -> "00:00:00"
    )
  }

  // ---------------------------------------------------------------------------------------------
  // ALTER TABLE on the reindex path
  // ---------------------------------------------------------------------------------------------

  private def rest(method: String, path: String, body: Option[String] = None): (Int, String) = {
    val request = new org.elasticsearch.client.Request(method, path)
    body.foreach(b =>
      request.setEntity(
        new org.apache.http.nio.entity.NStringEntity(
          b,
          org.apache.http.entity.ContentType.APPLICATION_JSON
        )
      )
    )
    scala.util.Try(restClient.performRequest(request)) match {
      case scala.util.Success(response) =>
        (
          response.getStatusLine.getStatusCode,
          org.apache.http.util.EntityUtils.toString(response.getEntity)
        )
      case scala.util.Failure(e: org.elasticsearch.client.ResponseException) =>
        (e.getResponse.getStatusLine.getStatusCode, e.getMessage)
      case scala.util.Failure(e) => throw e
    }
  }

  "ALTER TABLE on the reindex path" should
  "leave the index on its own pipeline, never on the temporary one" in {
    val table = "grouped_alter_restore"
    val pipeline = s"${table}_ddl_default_pipeline"
    val mapper = new com.fasterxml.jackson.databind.ObjectMapper()
    def ddl(sql: String): Unit =
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(_)     =>
        case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
      }
    ddl(s"DROP TABLE IF EXISTS $table")
    ddl(s"DROP PIPELINE IF EXISTS $pipeline")
    ddl(s"CREATE TABLE $table (id KEYWORD, a INT, b INT, c INT SCRIPT AS (a + b), PRIMARY KEY (id))")

    // a processor the table does not declare, with the `separator` a `split` requires: the write
    // of the user's pipeline after the migration is the step whose error used to leave the index on
    // the temporary pipeline (a write that answers an error is restored as a write that throws --
    // the unit row `AlterTablePipelineRewriteSpec` drives that error; this row checks the end state
    // the statement leaves on a real cluster)
    val stored = client.asInstanceOf[PipelineApi].getPipeline(pipeline) match {
      case ElasticSuccess(Some(json)) =>
        mapper.readTree(json).asInstanceOf[com.fasterxml.jackson.databind.node.ObjectNode]
      case other => fail(s"$other")
    }
    stored
      .get("processors")
      .asInstanceOf[com.fasterxml.jackson.databind.node.ArrayNode]
      .add(mapper.readTree("""{"split":{"field":"tags","separator":",","ignore_missing":true}}"""))
    client.asInstanceOf[PipelineApi].createPipeline(pipeline, mapper.writeValueAsString(stored)) shouldBe
    ElasticSuccess(true)

    val altered = Await.result(
      client.run(s"ALTER TABLE $table ADD COLUMN x INT SCRIPT AS (a * 2)"),
      120.seconds
    )
    log.info(s"ALTER TABLE answered $altered")

    // whatever the statement answered, the index runs its own pipeline
    val (_, settings) = rest("GET", s"/$table/_settings")
    mapper
      .readTree(settings)
      .path(table)
      .path("settings")
      .path("index")
      .path("default_pipeline")
      .asText() shouldBe pipeline
    // and the temporary pipeline the migration created is not left behind
    val (_, pipelines) = rest("GET", "/_ingest/pipeline")
    mapper.readTree(pipelines).fieldNames().asScala.filter(_.startsWith(s"${table}_tmp_")).toSeq shouldBe
    empty
    ddl(s"DROP TABLE IF EXISTS $table")
  }
}
