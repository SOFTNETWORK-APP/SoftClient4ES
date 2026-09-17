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

/** Story 22.6 AD-6 — issue #209's family, one venue over.
  *
  * An `_msearch` sends every leg as its own ONE-SHOT request, so a `UNION ALL` of two un-LIMITed
  * row legs over 30- and 24-document indices used to return 20 rows with HTTP 200 and no truncation
  * flag. This spec asserts EXACT row counts on MULTI-SHARD indices — the failure mode is silent, so
  * nothing here may be an inequality.
  *
  * It also pins the other half of the story at the plain client: a de-duplicating `UNION` must be
  * REFUSED rather than executed as its cheaper cousin.
  */
trait UnionAllCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val leftIndex = "union_left"
  private val rightIndex = "union_right"

  /** `union_left` = 10 categories x 3 docs = 30; `union_right` = 8 categories x 3 docs = 24. Both
    * are far above the Elasticsearch default of 10 hits, and the overlap `cat_06..cat_10` is what
    * makes a distinct `UNION` differ from `UNION ALL` at the engine venues.
    */
  private val leftCategories = 10
  private val rightCategories = 8
  private val docsPerCategory = 3
  private val leftDocs = leftCategories * docsPerCategory // 30
  private val rightDocs = rightCategories * docsPerCategory // 24

  private def createIndex(index: String, firstCategory: Int, categories: Int): Unit = {
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "category": { "type": "keyword" },
        |    "amount":   { "type": "integer" }
        |  }
        |}""".stripMargin

    client.createIndex(index, settings = settings).get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    val docs = (for {
      c <- firstCategory until (firstCategory + categories)
      d <- 1 to docsPerCategory
    } yield {
      val category = f"cat_$c%02d"
      s"""{"id":"${index}_${category}_$d","category":"$category","amount":${c * 100 + d}}"""
    }).toList

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

  override def beforeAll(): Unit = {
    super.beforeAll()
    createIndex(leftIndex, firstCategory = 1, categories = leftCategories)
    createIndex(rightIndex, firstCategory = 6, categories = rightCategories)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(leftIndex)
    client.deleteIndex(rightIndex)
    super.afterAll()
  }

  /** `client.search(SelectStatement(...))` and NOT `searchAs`: the macro types a `UNION ALL` from
    * the FIRST leg, which is a separate concern from row completeness.
    */
  private def rowCountOf(sql: String): Int = rowsOf(sql).size

  private def rowsOf(sql: String): Seq[ListMap[String, Any]] = {
    implicit val ctx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"[$sql] failed: ${error.message}")
    }
  }

  "UNION ALL of two un-LIMITed row legs" should "return every row of both legs" in {
    rowCountOf(
      s"SELECT category, amount FROM $leftIndex UNION ALL SELECT category, amount FROM $rightIndex"
    ) shouldBe (leftDocs + rightDocs)
  }

  "UNION ALL with one LIMITed and one un-LIMITed leg" should
  "bound the first and complete the second" in {
    rowCountOf(
      s"SELECT category, amount FROM $leftIndex LIMIT 5 " +
      s"UNION ALL SELECT category, amount FROM $rightIndex"
    ) shouldBe (5 + rightDocs)
  }

  "UNION ALL of two LIMITed legs" should "stay one-shot and bounded" in {
    rowCountOf(
      s"SELECT category, amount FROM $leftIndex LIMIT 7 " +
      s"UNION ALL SELECT category, amount FROM $rightIndex LIMIT 7"
    ) shouldBe 14
  }

  "UNION ALL of two aggregation legs" should "stay one-shot and return one row per bucket" in {
    rowCountOf(
      s"SELECT category, COUNT(*) AS n FROM $leftIndex GROUP BY category " +
      s"UNION ALL SELECT category, COUNT(*) AS n FROM $rightIndex GROUP BY category"
    ) shouldBe (leftCategories + rightCategories)
  }

  // ── issue #354: SQL-92 §7.10 matches branches BY POSITION, on EVERY route ─────────────────

  /** The result's column NAMES come from the first branch and the branches are matched by ORDINAL
    * POSITION — names play no part in it (`CORRESPONDING` is the opt-in for name matching, and its
    * existence is the proof). The engine used to look the first branch's names up IN THE LEG'S ROW,
    * which answered NULL for a column the branch names differently.
    *
    * `cat_01` exists only in `union_left` and `cat_13` only in `union_right`, so each branch
    * contributes a set of `id`s that names its own index — an oracle independent of the mapping
    * under test. Row ORDER within a leg is not guaranteed on a 3-shard index, so the assertions are
    * on SETS.
    */
  "UNION ALL with an alias per branch" should "take the FIRST branch's column name, one-shot" in {
    val rows = rowsOf(
      s"SELECT id AS x FROM $leftIndex WHERE category = 'cat_01' LIMIT 5 " +
      s"UNION ALL SELECT id AS y FROM $rightIndex WHERE category = 'cat_13' LIMIT 5"
    )
    rows should have size (2 * docsPerCategory).toLong
    // 🔴 ONE column, called `x`, on EVERY row — branch 1's own included. Before the fix
    // `MultiSearch.fieldAliases` merged the two aliases keyed by the SOURCE field `id`, one
    // survived, and every row came back `{x -> null, y -> …}`.
    rows.map(_.keys.toSeq).distinct shouldBe Seq(Seq("x"))
    rows.map(_("x")).toSet shouldBe
    ((1 to docsPerCategory).map(d => s"${leftIndex}_cat_01_$d") ++
    (1 to docsPerCategory).map(d => s"${rightIndex}_cat_13_$d")).toSet
  }

  it should "answer identically when the legs are paged per leg" in {
    val rows = rowsOf(
      s"SELECT id AS x FROM $leftIndex WHERE category = 'cat_01' " +
      s"UNION ALL SELECT id AS y FROM $rightIndex WHERE category = 'cat_13'"
    )
    rows should have size (2 * docsPerCategory).toLong
    rows.map(_.keys.toSeq).distinct shouldBe Seq(Seq("x"))
    rows.map(_("x")).toSet shouldBe
    ((1 to docsPerCategory).map(d => s"${leftIndex}_cat_01_$d") ++
    (1 to docsPerCategory).map(d => s"${rightIndex}_cat_13_$d")).toSet
  }

  /** Reordering the projection is how an analyst aligns two differently-shaped sources, and a
    * by-name lookup silently undid it: column 1 held branch 2's `category` because that is what
    * branch 1 called column 1.
    */
  "UNION ALL with a REORDERED second branch" should "bind column i to column i of that branch" in {
    val rows = rowsOf(
      s"SELECT category, id FROM $leftIndex WHERE category = 'cat_01' LIMIT 5 " +
      s"UNION ALL SELECT id, category FROM $rightIndex WHERE category = 'cat_13' LIMIT 5"
    )
    rows should have size (2 * docsPerCategory).toLong
    rows.map(_.keys.toSeq).distinct shouldBe Seq(Seq("category", "id"))
    val (left, right) = rows.splitAt(docsPerCategory)
    left.map(_("category")).toSet shouldBe Set("cat_01")
    left.map(_("id")).toSet shouldBe (1 to docsPerCategory)
      .map(d => s"${leftIndex}_cat_01_$d")
      .toSet
    // 🔴 branch 2 declared `id` FIRST, so the result's first column — the one branch 1 called
    // `category` — holds branch 2's ids, and `id` holds its category.
    right.map(_("category")).toSet shouldBe
    (1 to docsPerCategory).map(d => s"${rightIndex}_cat_13_$d").toSet
    right.map(_("id")).toSet shouldBe Set("cat_13")
  }

  "A distinct UNION on the plain client" should
  "be refused with 400 naming the extension, never executed as UNION ALL" in {
    implicit val ctx: ConversionContext = NativeContext
    val sql = s"SELECT category FROM $leftIndex UNION SELECT category FROM $rightIndex"
    client.search(SelectStatement(sql)) match {
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(400)
        error.message should include("A set operation")
        error.message should include("softclient4es-arrow-extensions")
      case ElasticSuccess(response) =>
        fail(s"must not execute: got ${response.results.size} rows")
    }
  }

  "INTERSECT and EXCEPT on the plain client" should "be refused the same way" in {
    implicit val ctx: ConversionContext = NativeContext
    Seq("INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL").foreach { op =>
      val sql = s"SELECT category FROM $leftIndex $op SELECT category FROM $rightIndex"
      withClue(s"[$op] ") {
        client.search(SelectStatement(sql)) match {
          case ElasticFailure(error) => error.statusCode shouldBe Some(400)
          case ElasticSuccess(r)     => fail(s"must not execute: got ${r.results.size} rows")
        }
      }
    }
  }
}
