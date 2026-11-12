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
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.Statement
import app.softnetwork.persistence.generateUUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.Request
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-15 (AC1, RUN) -- `describePerTable` types each output column of a JOIN body from its own
  * table's mapping, as single-table `describe` types the same item over that table (one-table
  * items) or over the twin index mapping both tables' columns (two-table items, aggregates), and
  * searches nothing: every index's `_stats/search` `query_total` is unchanged, while one `run` (the
  * positive control) moves it.
  *
  * Fixtures: `pt_a`, `pt_b` (its `k` and `n` collide with `pt_a`'s, `n` with another type, so a
  * wrong attribution is visible) and their twin `pt_ab`; `pz` and `dn` from Elasticsearch 7 on.
  */
trait PerTableDescribeSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  protected lazy val major: Int = Try(elasticVersion.takeWhile(_ != '.').toInt).getOrElse(8)

  /** `date_nanos` (7.0) and a zone-name format (6.8's Joda-Time cannot parse a zone name). */
  protected lazy val modern: Boolean = major >= 7

  private val mapper = new ObjectMapper()

  protected val ptA = "pt_a"
  protected val ptB = "pt_b"
  protected val ptAB = "pt_ab"

  private def indices: Seq[String] = Seq(ptA, ptB, ptAB)

  protected def ptAColumns: Seq[(String, String)] = Seq(
    "aid"   -> """{"type": "keyword"}""",
    "bid"   -> """{"type": "keyword"}""",
    "k"     -> """{"type": "keyword"}""",
    "t"     -> """{"type": "text"}""",
    "n"     -> """{"type": "integer"}""",
    "big_a" -> """{"type": "long"}""",
    "x"     -> """{"type": "double"}""",
    "r"     -> """{"type": "float"}""",
    "ts"    -> """{"type": "date"}""",
    "dc"    -> """{"type": "date", "format": "yyyy/MM/dd"}""",
    "de"    -> """{"type": "date", "format": "epoch_second"}""",
    "b"     -> """{"type": "boolean"}""",
    "g"     -> """{"type": "geo_point"}""",
    "obj"   -> """{"properties": {"a": {"type": "integer"}}}""",
    "items" -> """{"type": "nested", "properties": {"p": {"type": "integer"}}}""",
    "tags"  -> """{"type": "integer"}"""
  ) ++ (if (modern)
          Seq(
            "pz" -> """{"type": "date", "format": "yyyy-MM-dd HH:mm z"}""",
            "dn" -> """{"type": "date_nanos"}"""
          )
        else Nil)

  protected val ptBColumns: Seq[(String, String)] = Seq(
    "bid"   -> """{"type": "keyword"}""",
    "label" -> """{"type": "keyword"}""",
    "name"  -> """{"type": "text"}""",
    "m"     -> """{"type": "integer"}""",
    "big"   -> """{"type": "long"}""",
    "d2"    -> """{"type": "date"}""",
    "addr"  -> """{"properties": {"zip": {"type": "keyword"}}}""",
    "k"     -> """{"type": "keyword"}""",
    "n"     -> """{"type": "long"}"""
  )

  private val meta = """{"de": {"data_type": "DATE"}, "tags": {"data_type": "ARRAY<INT>"}}"""

  private def mappingOf(columns: Seq[(String, String)], withMeta: Boolean): String = {
    val properties = columns.map { case (c, m) => s""""$c": $m""" }.mkString("{", ", ", "}")
    if (withMeta) s"""{"_meta": {"columns": $meta}, "properties": $properties}"""
    else s"""{"properties": $properties}"""
  }

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
    ()
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    indices.foreach(drop)
    load(
      ptA,
      mappingOf(ptAColumns, withMeta = true),
      Seq(
        "a1" -> """{"aid":"a1","bid":"b1","k":"x","n":1,"big_a":10,"x":1.5,"ts":"2024-01-31T10:30:00Z","b":true,"tags":[1,2]}""",
        "a2" -> """{"aid":"a2","bid":"b2","k":"y","n":2}"""
      )
    )
    load(
      ptB,
      mappingOf(ptBColumns, withMeta = false),
      Seq(
        "b1" -> """{"bid":"b1","label":"L1","name":"one","m":3,"big":30,"d2":"2024-01-30T00:00:00Z","k":"x","n":5}""",
        "b2" -> """{"bid":"b2","label":"L2","m":4}"""
      )
    )
    load(
      ptAB,
      mappingOf(
        ptAColumns ++ ptBColumns.filterNot(c => Set("bid", "k", "n").contains(c._1)),
        withMeta = true
      ),
      Seq("ab1" -> """{"aid":"a1","bid":"b1","label":"L1"}""")
    )
  }

  override def afterAll(): Unit = {
    indices.foreach(drop)
    super.afterAll()
  }

  protected def parsed(sql: String): Statement =
    Parser(sql) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"[$sql] does not parse: ${error.msg}")
    }

  /** The item over its own table: the qualifier dropped. */
  protected def unqualified(sql: String): String =
    sql.replaceAll("\\b(?:a2|b2|a|b|pt_a|pt_b)\\.", "")

  protected val onBid = "ON a.bid = b.bid"

  /** `query_total` summed over the three fixtures. */
  protected def queryTotal(): Long = {
    val response =
      restClient.performRequest(new Request("GET", s"/${indices.mkString(",")}/_stats/search"))
    mapper
      .readTree(EntityUtils.toString(response.getEntity))
      .path("_all")
      .path("total")
      .path("search")
      .path("query_total")
      .asLong(-1L)
  }

  /** Both answers equal: the same schema, or the same status and message. */
  protected def sameAnswer(
    perTable: ElasticResult[Option[ResultSchema]],
    oracle: ElasticResult[Option[ResultSchema]]
  ): Unit =
    (perTable, oracle) match {
      case (ElasticSuccess(p), ElasticSuccess(o)) => p shouldBe o
      case (ElasticFailure(p), ElasticFailure(o)) =>
        p.statusCode shouldBe o.statusCode
        p.message shouldBe o.message
      case other => fail(s"describePerTable and its oracle disagree: $other")
    }

  protected def columnsOf(answer: ElasticResult[Option[ResultSchema]]): Seq[(String, String)] =
    answer match {
      case ElasticSuccess(Some(schema)) => schema.columns.map(c => c.name -> c.sqlType.typeId)
      case other                        => fail(s"no schema: $other")
    }

  private def oneTable(q: String, c: String): Seq[String] =
    Seq(s"$q.$c", s"$q.$c AS v", s"CAST($q.$c AS VARCHAR) AS s")

  /** G1 -- (item, the table its oracle reads). */
  protected def g1: Seq[(String, String)] =
    ptAColumns.map(_._1).flatMap(c => oneTable("a", c).map(_ -> ptA)) ++
    ptBColumns.map(_._1).flatMap(c => oneTable("b", c).map(_ -> ptB)) ++
    Seq("a.obj.a" -> ptA, "b.addr.zip AS zip" -> ptB)

  /** G2 -- items over both tables, typed as over the twin. */
  protected val g2: Seq[(String, String)] = Seq(
    "COALESCE(a.n, b.m) AS v",
    "a.n + b.big AS v",
    "GREATEST(a.x, b.m) AS v",
    "CASE WHEN a.b THEN a.ts ELSE b.d2 END AS v",
    "NULLIF(a.k, b.label) AS v",
    "CONCAT(a.k, b.label) AS v",
    "UPPER(b.name) AS v",
    "a.ts - b.d2 AS v",
    "DATEDIFF(a.ts, b.d2) AS v"
  ).map(_ -> "pt_ab")

  /** G3 / B8 -- (select list, group by). */
  protected val g3: Seq[(String, String)] = Seq(
    "b.label, COUNT(*) AS c, COUNT(DISTINCT a.k) AS dk, SUM(a.n) AS s, AVG(a.x) AS av, " +
    "MIN(a.ts) AS mi, MAX(a.dc) AS ma"           -> "b.label",
    "b.label, a.k, COUNT(*) AS c, SUM(a.n) AS s" -> "b.label, a.k"
  )

  /** (JOIN body, the single-table statement whose answer is its oracle). */
  protected def cells: Seq[(String, String)] = {
    def over(from: String, items: Seq[(String, String)]): Seq[(String, String)] =
      items.map { case (item, table) =>
        s"SELECT $item FROM $from" -> s"SELECT ${unqualified(item)} FROM $table"
      }
    val aliased = Seq("INNER JOIN", "LEFT JOIN").flatMap { kind =>
      over(s"$ptA a $kind $ptB b $onBid", g1 ++ g2)
    }
    val unaliased = (g1 ++ g2).map { case (item, table) =>
      val written = item.replaceAll("\\ba\\.", "pt_a.").replaceAll("\\bb\\.", "pt_b.")
      s"SELECT $written FROM pt_a JOIN pt_b ON pt_a.bid = pt_b.bid" ->
      s"SELECT ${unqualified(item)} FROM $table"
    }
    val ptAItems = g1.filter(_._2 == ptA)
    val selfJoin = over(
      s"$ptA a JOIN $ptA a2 ON a.bid = a2.bid",
      ptAItems ++ ptAItems.map { case (item, t) => item.replaceAll("\\ba\\.", "a2.") -> t }
    )
    val aggregates = g3.map { case (items, keys) =>
      s"SELECT $items FROM $ptA a JOIN $ptB b $onBid GROUP BY $keys" ->
        s"SELECT ${unqualified(items)} FROM $ptAB GROUP BY ${unqualified(keys)}"
    }
    aliased ++ unaliased ++ selfJoin ++ aggregates
  }

  "describePerTable" should "type a JOIN body per table, searching nothing (AC1)" in {
    val oracles = cells.map { case (body, oracle) => (body, oracle, client.describe(oracle)) }
    oracles.collect { case (_, oracle, ElasticSuccess(None)) => oracle } shouldBe empty
    val statements = oracles.map { case (body, oracle, answer) =>
      (body, oracle, parsed(body), answer)
    }
    val bare = parsed(s"SELECT n FROM $ptA a JOIN $ptB b $onBid")
    val joinedOnly = parsed(s"SELECT b.aid FROM $ptA a JOIN $ptB b $onBid")
    val unknown = parsed(s"SELECT a.aid, z.n FROM $ptA a JOIN pt_zz z ON a.bid = z.bid")
    val mainN = columnsOf(client.describe(s"SELECT n FROM $ptA"))
    mainN should not be columnsOf(client.describe(s"SELECT n FROM $ptB"))

    val before = queryTotal()
    before should be >= 0L
    statements.foreach { case (body, oracle, statement, answer) =>
      withClue(s"[$body] vs [$oracle] ") {
        sameAnswer(client.describePerTable(statement), answer)
      }
    }
    // G4 (A-110): a bare name is the main table's; a column only the main table maps is ANY under
    // the joined qualifier
    columnsOf(client.describePerTable(bare)) shouldBe mainN
    columnsOf(client.describePerTable(joinedOnly)) shouldBe Seq("aid" -> "ANY")
    // G5: an unknown leg answers None, never the main table's same-named column
    client.describePerTable(unknown) shouldBe ElasticSuccess(None)
    queryTotal() shouldBe before

    // the positive control: one run moves the counter
    Await.result(client.run(s"SELECT aid FROM $ptA LIMIT 10"), 60.seconds) match {
      case ElasticSuccess(_)     =>
      case ElasticFailure(error) => fail(s"positive control: ${error.message}")
    }
    queryTotal() should be > before
  }
}
