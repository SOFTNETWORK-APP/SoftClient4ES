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
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions

/** The BI dialect spellings, EXECUTED — issues #361 / #362.
  *
  * 🔴 This spec exists because `CorpusReplaySpec` scores a row `fixed` only against *"a merged
  * suite asserting that statement's CORRECTNESS"*, and a parse test is not that. The two corpus
  * rows this covers (`tableau.sql92.w8.032`, `tableau.sql92.wx.010`) parsed as soon as the
  * spellings were accepted; whether they ANSWER is a different question, and it is the one the
  * scoreboard actually publishes. Parse is not an answer (PD-3).
  *
  * Each assertion is built so that accepting the spelling is NOT enough to pass it:
  *
  *   - `CHAR_LENGTH` is asserted against a MULTI-BYTE string. `'海豚'` is 2 characters and 6 UTF-8
  *     bytes, so a byte-counting implementation — which is what MySQL's own `LENGTH` does, and the
  *     reason `CHAR_LENGTH` is the spelling that agrees with us — answers 6 and reddens. Asserting
  *     only against ASCII would be satisfied by either.
  *   - `TOP n` is asserted on ROW COUNT and on the ROW SET, against the `LIMIT n` spelling of the
  *     same statement, over an index with more documents than `n` AND more than Elasticsearch's
  *     default of 10. A `TOP` that was parsed and then dropped returns every row and reddens.
  */
trait BiDialectExecutionSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "bi_dialect"

  /** 24 documents on 3 shards — above the default 10 hits, so a dropped bound is visible. */
  private val docCount = 24

  /** name -> its CHARACTER count. `海豚` is the load-bearing row: 2 characters, 6 UTF-8 bytes.
    *
    * 🔴 None of these may be `padding`, the name the 24 bulk documents carry: a collision would
    * make 25 documents match `name = 'padding'` and quietly change every count oracle below.
    */
  private val names: Map[String, Int] =
    Map("grace" -> 5, "海豚" -> 2, "café" -> 4, "ada" -> 3, "" -> 0)

  override def beforeAll(): Unit = {
    super.beforeAll()
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":     { "type": "keyword" },
        |    "name":   { "type": "keyword" },
        |    "amount": { "type": "integer" }
        |  }
        |}""".stripMargin
    client.createIndex(index, settings = settings).get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = (1 to docCount).map { i =>
      s"""{"id":"doc_$i","name":"padding","amount":$i}"""
    }.toList ++ names.keys.map { n =>
      s"""{"id":"name_${n.hashCode}","name":"$n","amount":0}"""
    }.toList

    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 1000)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing failed: ${error.message}")
    }
    client.refresh(index)
    ()
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    super.afterAll()
  }

  private def rowsOf(sql: String): Seq[ListMap[String, Any]] = {
    implicit val ctx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"[$sql] failed: ${error.message}")
    }
  }

  /** A `script_fields` value comes back wrapped in Elasticsearch's per-field ARRAY on the row path
    * (`n -> List(3)`), while the same expression under an aggregation comes back as a scalar. That
    * asymmetry is pre-existing and documented against issue #209; it is not what this spec tests,
    * so it is unwrapped here rather than asserted around.
    */
  private def longAt(row: ListMap[String, Any], key: String): Long = row(key) match {
    case n: Number          => n.longValue()
    case Seq(n: Number)     => n.longValue()
    case (n: Number) :: Nil => n.longValue()
    case other              => fail(s"expected a number at '$key', got ${other.getClass}: $other")
  }

  // ───────────────────────────────── CHAR_LENGTH (issue #361) ─────────────────────────────────

  "CHAR_LENGTH" should "count CHARACTERS, not bytes, on a real index" in {
    names.foreach { case (name, expected) =>
      val rows = rowsOf(
        s"SELECT CHAR_LENGTH(name) AS n FROM $index WHERE id = 'name_${name.hashCode}'"
      )
      withClue(s"CHAR_LENGTH('$name') (${name.getBytes("UTF-8").length} UTF-8 bytes) ") {
        rows should have size 1L
        longAt(rows.head, "n") shouldBe expected.toLong
      }
    }
  }

  it should "answer exactly what LENGTH answers, for every name" in {
    names.keys.foreach { name =>
      val where = s"WHERE id = 'name_${name.hashCode}'"
      val viaAlias = rowsOf(s"SELECT CHAR_LENGTH(name) AS n FROM $index $where")
      val viaCanonical = rowsOf(s"SELECT LENGTH(name) AS n FROM $index $where")
      withClue(s"['$name'] ") { viaAlias shouldBe viaCanonical }
    }
  }

  it should "work inside an aggregate, which is the shape the capture used" in {
    // tableau.sql92.w8.032 wraps it in SUM(...). 24 documents named `padding`, 7 characters each.
    val rows = rowsOf(s"SELECT SUM(CHAR_LENGTH(name)) AS total FROM $index WHERE name = 'padding'")
    rows should have size 1L
    longAt(rows.head, "total") shouldBe (docCount * 7).toLong
  }

  // ─────────────────────────────────── SELECT TOP n (issue #361) ───────────────────────────────

  "SELECT TOP n" should "return exactly n rows, not every row" in {
    Seq(1, 5, 7).foreach { n =>
      withClue(s"TOP $n ") {
        rowsOf(s"SELECT TOP $n id FROM $index WHERE name = 'padding'") should have size n.toLong
      }
    }
  }

  it should "return the same rows as the LIMIT spelling of the same statement" in {
    Seq(1, 5, 7).foreach { n =>
      val viaTop =
        rowsOf(s"SELECT TOP $n id FROM $index WHERE name = 'padding' ORDER BY amount ASC")
      val viaLimit =
        rowsOf(s"SELECT id FROM $index WHERE name = 'padding' ORDER BY amount ASC LIMIT $n")
      withClue(s"TOP $n vs LIMIT $n ") { viaTop shouldBe viaLimit }
    }
  }

  /** The control that makes the two assertions above mean something: without a bound the same
    * statement returns all 24 documents. A `TOP` that parsed and was then dropped would return this
    * number instead of `n`, and 24 > Elasticsearch's default page of 10, so the difference cannot
    * be an artefact of the default.
    */
  it should "differ from the unbounded statement, which returns every document" in {
    rowsOf(s"SELECT id FROM $index WHERE name = 'padding'") should have size docCount.toLong
  }

  it should "bind the rows for the exact captured statement shape" in {
    // tableau.sql92.wx.010: SELECT TOP 1 * FROM "elastic"."bi_events"
    rowsOf(s"""SELECT TOP 1 * FROM "$index"""") should have size 1L
  }

  // ─────────────────────────── TIMESTAMPADD / CHAR_LENGTH round trip ───────────────────────────

  "TIMESTAMPADD" should "execute as DATETIME_ADD does, on the same statement" in {
    val viaAlias = rowsOf(
      s"SELECT TIMESTAMPADD(DAY, 1, CURRENT_DATE) AS d FROM $index WHERE name = 'padding' LIMIT 1"
    )
    val viaCanonical = rowsOf(
      s"SELECT DATETIME_ADD(DAY, 1, CURRENT_DATE) AS d FROM $index WHERE name = 'padding' LIMIT 1"
    )
    viaAlias should have size 1L
    viaAlias shouldBe viaCanonical
  }
}
