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

import scala.language.implicitConversions

case class CategoryCount(category: String, cnt: Long)

// Issue #253 result shapes: an aggregate-free GROUP BY projects the bucket keys only.
case class CategoryOnly(category: String)
case class CategoryAmount(category: String, amount: Int)
case class CategoryAliased(cat: String)

/** Regression test for issue #205: `GROUP BY` with no `LIMIT` must return EVERY group.
  *
  * The failure mode this guards against is SILENT: without an explicit `size` on the `terms`
  * aggregation, Elasticsearch returns its default 10 buckets with HTTP 200 and no truncation flag,
  * so a 37-category index silently reports 10 plausible-looking groups. Every fixture in the other
  * suites has cardinality <= 10, which is exactly why this went unnoticed — this spec asserts group
  * count AND total doc coverage at cardinality > 10, on a multi-shard index.
  */
trait GroupByCompletenessSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "group_by_completeness"

  /** 37 categories — far above the ES `terms` default of 10 buckets. Category `cat_i` holds exactly
    * `i` docs, so both the group count and every per-group count are exact oracles.
    */
  private val categories = 37

  private val totalDocs = (1 to categories).sum

  override def beforeAll(): Unit = {
    super.beforeAll()

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
      c <- 1 to categories
      d <- 1 to c
    } yield {
      val category = f"cat_$c%02d"
      s"""{"id":"${category}_$d","category":"$category","amount":$d}"""
    }).toList

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = index,
      logEvery = 1000
    )

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
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

  "GROUP BY without LIMIT" should "return every group and account for every document" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong

        val counts = rows.map(r => r.category -> r.cnt).toMap
        counts should have size categories.toLong
        (1 to categories).foreach { c =>
          counts(f"cat_$c%02d") shouldBe c.toLong
        }

        rows.map(_.cnt).sum shouldBe totalDocs.toLong

        log.info(s"✓ ${rows.size} groups covering ${rows.map(_.cnt).sum} docs from $index")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "GROUP BY with LIMIT" should "still bound the number of groups" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category LIMIT 5"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size 5

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "GROUP BY with ORDER BY on the bucket key and LIMIT" should "return the exact top groups" in {
    // Key ordering is exact on a multi-shard index; ordering by the metric (ORDER BY cnt DESC)
    // is NOT — the pushed-down terms top-N is shard-approximate by design (doc_count_error),
    // so a metric-ordered assertion here would flake on routing skew.
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category ORDER BY category DESC LIMIT 3"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(r => r.category -> r.cnt) shouldBe (0 until 3).map { i =>
          val c = categories - i
          f"cat_$c%02d" -> c.toLong
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ------------------------------------------------------------------
  // Issue #253 -- GROUP BY with NO aggregate in the SELECT list returns
  // one row per GROUP, not one row per DOCUMENT.
  //
  // Before the fix the statement was row-shaped (`returnsRows` was true because `sqlAggregations`
  // was empty), so it was routed to the scroll / capped-scroll path and came back with
  // per-document rows -- 703 here, and 10,000 on the reporter's 200k fixture. The oracle below is
  // the GROUP count (37), deliberately different from BOTH the document count (703) and the ES
  // terms default (10), so neither failure mode can pass.
  // ------------------------------------------------------------------

  "GROUP BY with no aggregate" should "return exactly one row per group" in {
    client.searchAs[CategoryOnly](
      "SELECT category FROM group_by_completeness GROUP BY category"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong // 37 groups, not 703 documents
        rows.map(_.category).distinct should have size categories.toLong
        rows.map(_.category).toSet shouldBe (1 to categories).map(c => f"cat_$c%02d").toSet
        log.info(s"OK ${rows.size} groups from $index (documents indexed: $totalDocs)")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "still bound the group count with an explicit LIMIT" in {
    // LIMIT on a GROUP BY is the terms SIZE (a bucket count), aggregate or not -- unchanged
    // semantics, pinned so the #253 fix cannot drift it into a row count.
    client.searchAs[CategoryOnly](
      "SELECT category FROM group_by_completeness GROUP BY category LIMIT 5"
    ) match {
      case ElasticSuccess(rows)  => rows should have size 5
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "return one row per COMBINATION for a multi-column aggregate-free GROUP BY" in {
    // `cat_i` holds exactly i docs with amounts 1..i, so every document IS a distinct
    // (category, amount) pair: the exact oracle is `totalDocs` = sum(1..37) = 703.
    client.searchAs[CategoryAmount](
      "SELECT category, amount FROM group_by_completeness GROUP BY category, amount"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size totalDocs.toLong
        rows.map(r => (r.category, r.amount)).distinct should have size totalDocs.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "order an aggregate-free GROUP BY by the bucket key" in {
    client.searchAs[CategoryOnly](
      "SELECT category FROM group_by_completeness GROUP BY category ORDER BY category DESC LIMIT 3"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.category) shouldBe (0 until 3).map(i => f"cat_${categories - i}%02d")
      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "resolve an ordinal GROUP BY / ORDER BY to the n-th SELECT item (OQ-1)" in {
    // Tableau's own shape: `... GROUP BY 1 ORDER BY 1 ASC`.
    // ⚠️ RED pre-fix for a NON-obvious reason, which is what makes it a strong gate: the
    // `GROUP BY 1` already resolved, but the `ORDER BY 1` was silently dropped, so the terms
    // aggregation fell back to its default doc_count-descending order and `LIMIT 3` returned the
    // THREE BIGGEST categories (cat_37, cat_36, cat_35 -- `cat_i` holds `i` docs) instead of the
    // three smallest by key. The assertion is on the KEY order, which is exact on a multi-shard
    // index; a doc_count-ordered top-N would be shard-approximate.
    client.searchAs[CategoryOnly](
      "SELECT category FROM group_by_completeness GROUP BY 1 ORDER BY 1 ASC LIMIT 3"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.category) shouldBe Seq("cat_01", "cat_02", "cat_03")
      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "resolve a GROUP BY select-alias to the aliased field (FOLD-IN 2)" in {
    // Pre-fix failure mode: terms on the non-existent field "cat" => ZERO rows with HTTP 200 --
    // the strongest possible RED (an empty success, not an error). The 37-group oracle is the same
    // as the plain #253 test's, so the two must agree exactly. The result binds the ALIAS (the
    // bucket is named "cat" -- name = identifier.fieldAlias), hence CategoryAliased.
    client.searchAs[CategoryAliased](
      "SELECT category AS cat FROM group_by_completeness GROUP BY cat"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong
        rows.map(_.cat).toSet shouldBe (1 to categories).map(c => f"cat_$c%02d").toSet
      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "group by a row-invariant constant, which is exactly ONE group (FOLD-IN 1)" in {
    // The Tableau capability-probe shape (`SELECT SUM(1) AS COL, 2 AS COL2 ... GROUP BY 2`) reduced
    // to what makes it work: a bucket whose identifier is a CONSTANT has no field to name, so it
    // must be emitted as a SCRIPTED `terms`. Before that, the emitted aggregation carried neither
    // `field` nor `script` and Elasticsearch rejects such a request outright -- which is why this
    // assertion has to run against a real cluster and not against the generated JSON alone.
    // A constant is the same for every document, so the oracle is ONE group over all 703 of them.
    // ⚠️ Asserted on the RAW row, not through `searchAs`: the macro types a bare integer literal
    // as BIGINT and therefore demands a `Long` field, while the value arrives as Jackson's
    // smallest type (an Integer), so the generated decoder rejects it. That binding mismatch is
    // pre-existing for any literal column and is recorded separately -- it must not be allowed to
    // hide whether the AGGREGATION is right, which is what this test is for.
    implicit val ctx: ConversionContext = NativeContext
    client.search(
      SelectStatement("SELECT 2 AS flag FROM group_by_completeness GROUP BY flag")
    ) match {
      case ElasticSuccess(response) =>
        response.results should have size 1L
        response.results.head.keys.toSeq shouldBe Seq("flag")
        response.results.head("flag").toString shouldBe "2"

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }
}
