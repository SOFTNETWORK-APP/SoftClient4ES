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
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch}
import app.softnetwork.persistence.generateUUID
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

case class CategoryCount(category: String, cnt: Long)

// Issue #253 result shapes: an aggregate-free GROUP BY projects the bucket keys only.
case class CategoryOnly(category: String)
case class CategoryAmount(category: String, amount: Int)
case class CategoryAliased(cat: String)
case class AmountOnly(amount: Int)

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
    client.deleteIndex(nullIndex)
    client.deleteIndex(coalesceIndex)
    client.deleteIndex(dateDiffIndex)
    client.deleteIndex(keyIndex)
    client.deleteIndex(nestedKeyIndex)
    client.deleteIndex(nestedMetricIndex)
    client.deleteIndex(mixedIntIndex)
    client.deleteIndex(mixedLongIndex)
    client.deleteIndex(datesIndex)
    client.deleteIndex(oneResolutionIndex)
    client.deleteIndex(oneResolutionDates)
    client.deleteIndex(im10OneResolution)
    client.deleteIndex(dateKeyIndex)
    client.deleteIndex(dateKeyEmptyIndex)
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
    // 🔴 The oracle has to be strictly LESS than the document count, or it cannot fail: grouping by
    // (category, amount) yields exactly one pair per document here (`cat_i` holds `i` docs with
    // amounts 1..i), so a row-shaped -- i.e. UNFIXED -- execution returns the same 703 rows and the
    // test is green either way. Slicing to `amount <= 3` makes the amount REPEAT across categories:
    // cat_01 contributes 1 pair, cat_02 two, every cat_i (i >= 3) three, so the combination count
    // is 1 + 2 + 3*35 = 108 while the DOCUMENT count in the slice is the same 108... so the slice
    // alone is not enough either. Grouping by `amount` ONLY is what separates them: 3 groups
    // against 108 documents.
    client.searchAs[CategoryAmount](
      "SELECT category, amount FROM group_by_completeness WHERE amount <= 3 GROUP BY category, amount"
    ) match {
      case ElasticSuccess(rows) =>
        val expected = 1 + 2 + 3 * (categories - 2) // 108 distinct (category, amount) pairs
        rows should have size expected.toLong
        rows.map(r => (r.category, r.amount)).distinct should have size expected.toLong

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "return one row per GROUP, not per document, when the group key repeats" in {
    // The gate the test above cannot be: `amount <= 3` selects 108 documents but only THREE
    // distinct amounts, so a row-shaped execution returns 108 rows and an aggregation-shaped one
    // returns 3. Nothing about the fixture can make those numbers coincide.
    client.searchAs[AmountOnly](
      "SELECT amount FROM group_by_completeness WHERE amount <= 3 GROUP BY amount"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.amount).sorted shouldBe Seq(1, 2, 3)

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

  it should "project a row-invariant constant beside the group key (FOLD-IN 1, case B)" in {
    // 🔴 The pre-fix failure mode is a NULL column, not an error: an aggregation response has no
    // hits, so the `script_fields` entry the constant is emitted as is never fetched under
    // `"size": 0`. MEASURED on real ES 8.18 before the projection landed:
    // `List((cat_37,None), (cat_36,None), (cat_35,None))`.
    //
    // ⚠️ Asserted on the RAW row, not through `searchAs`: the macro types a bare integer literal
    // as BIGINT (so an `Int` field is a compile error) while the value arrives as Jackson's
    // smallest type at run time (so a `Long` field fails to decode). That binding mismatch is
    // pre-existing for ANY literal column and is recorded separately -- it must not be allowed to
    // hide whether the projection works, which is what this test is for.
    implicit val projCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement(
        "SELECT category, 2 AS flag FROM group_by_completeness GROUP BY category"
      )
    ) match {
      case ElasticSuccess(response) =>
        response.results should have size categories.toLong
        response.results.foreach { row =>
          row.keys.toSeq should contain("flag")
          row("flag").toString shouldBe "2"
        }
        response.results.map(_("category").toString).toSet shouldBe
        (1 to categories).map(c => f"cat_$c%02d").toSet

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // 🔴 PINS A KNOWN DEFECT, not a contract -- delete this test when the defect is fixed.
  //
  // The same constant reaches the caller DIFFERENTLY on the two paths, MEASURED on real ES 8.18:
  //   aggregation path (GROUP BY): `flag -> 2`         -- a clean scalar, projected from the AST
  //   row path        (no GROUP BY): `flag -> List(2)` -- array-wrapped
  //
  // The wrap is the pre-existing `script_fields` defect (`jsonNodeToAny` keeps an ES per-field
  // array as a List and nothing unwraps it), recorded in
  // docs/issues/local-21.3-script-fields-values-are-array-wrapped.md. The aggregation side is the
  // CORRECT one and is deliberately NOT wrapped to match: matching would spread a defect to a path
  // that does not have it. Pinned so the divergence is visible rather than discovered.
  it should "expose the known row-path array wrap, which the aggregation path does not share" in {
    implicit val wrapCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement("SELECT id, 2 AS flag FROM group_by_completeness LIMIT 2")
    ) match {
      case ElasticSuccess(response) =>
        response.results.foreach { row =>
          withClue(s"row path, row=$row: ") { row("flag") shouldBe List(2) }
        }
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "project an UN-ALIASED constant under the column the row actually asks for" in {
    // 🔴 Every other constant test uses an alias, which is why this shipped broken: with no alias
    // the requested column is the COMPUTED `__c2`, and keying the projection by the rendered name
    // (`2`) left that column NULL while inventing a bogus `2` beside it.
    implicit val bareCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement("SELECT category, 2 FROM group_by_completeness GROUP BY category")
    ) match {
      case ElasticSuccess(response) =>
        response.results should have size categories.toLong
        response.results.foreach { row =>
          withClue(s"row=$row: ") {
            row.keys.toSeq should contain("__c2")
            row("__c2").toString shouldBe "2"
            row.keys.toSeq should not contain "2"
          }
        }
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "project each UNION ALL leg's OWN constant" in {
    // 🔴 The multi-search arm never projected at all: both legs are aggregation-shaped and each
    // carries a different constant, so the rows came back with `flag = null`. The legs are
    // concatenated by the time a caller sees them, so the projection has to happen per response.
    implicit val unionCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement(
        "SELECT category, 2 AS flag FROM group_by_completeness WHERE amount >= 2 GROUP BY category" +
        " UNION ALL " +
        "SELECT category, 3 AS flag FROM group_by_completeness WHERE amount >= 3 GROUP BY category"
      )
    ) match {
      case ElasticSuccess(response) =>
        // 🔴 Pair the constant with a leg-distinguishing ROW COUNT: `Set("2","3")` alone would pass
        // if the legs' seeds were swapped (an off-by-one in the per-leg lookup). `cat_i` holds
        // amounts 1..i, so `amount >= 2` matches cat_02..cat_37 (36 groups) and `amount >= 3`
        // matches cat_03..cat_37 (35) -- counts that differ, and differ from each other.
        val byFlag = response.results.groupBy(_("flag").toString).map { case (k, v) => k -> v.size }
        withClue(s"rows=${response.results.take(4)}: ") {
          byFlag shouldBe Map("2" -> (categories - 1), "3" -> (categories - 2))
        }
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "return NO row when the grouping matches nothing" in {
    // 🔴 The aggregation fold seeded itself with one EMPTY row, so a `WHERE` that excludes
    // everything produced a phantom all-NULL row -- and with a seeded constant that phantom carried
    // REAL values. Asserted for the aggregate-free, constant-bearing and aggregate-bearing
    // spellings alike: the guard takes no "except".
    implicit val emptyCtx: ConversionContext = NativeContext
    Seq(
      "SELECT category FROM group_by_completeness WHERE amount > 9999 GROUP BY category",
      "SELECT category, 2 AS flag FROM group_by_completeness WHERE amount > 9999 GROUP BY category",
      "SELECT category, COUNT(*) AS c FROM group_by_completeness WHERE amount > 9999 GROUP BY category"
    ).foreach { sql =>
      client.search(SelectStatement(sql)) match {
        case ElasticSuccess(response) =>
          withClue(s"[$sql] rows=${response.results}: ") { response.results shouldBe empty }
        case ElasticFailure(error) => fail(s"[$sql] Query failed: ${error.message}")
      }
    }
  }

  it should "order an aggregate-free GROUP BY by a SELECT ALIAS of the grouped column" in {
    // 🔴 Regression guard for the alias/bucket key desync: `SingleSearch.sorts` is keyed by the
    // sort's name while `buildBuckets` looks the direction up under the bucket's resolved column,
    // so an ORDER BY naming the alias silently produced NO terms `order` at all -- with a LIMIT
    // that is a DIFFERENT SET of groups (the top 3 by doc_count instead of the 3 smallest keys),
    // returned with HTTP 200. No integration test covered the alias spelling, which is why it
    // shipped. `cat_i` holds `i` docs, so doc_count order and key order disagree by construction.
    client.searchAs[CategoryAliased](
      "SELECT category AS cat FROM group_by_completeness GROUP BY cat ORDER BY cat ASC LIMIT 3"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.cat) shouldBe Seq("cat_01", "cat_02", "cat_03")
      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "apply a HAVING however the GROUP BY spelled the column" in {
    // 🔴 M-2: the repair originally covered only a SUBSTITUTED bucket, so whether a HAVING was
    // honoured depended on whether the GROUP BY named the alias or the column -- worse than
    // uniformly broken. Both spellings must filter.
    // ⚠️ Unrolled, not looped: `searchAs` is a macro and needs a compile-time constant SQL string.
    def check(label: String, result: ElasticResult[Seq[CategoryAliased]]): Unit =
      result match {
        case ElasticSuccess(rows) =>
          withClue(s"[$label] ") {
            rows should have size (categories - 1).toLong
            rows.map(_.cat) should not contain "cat_01"
          }
        case ElasticFailure(error) => fail(s"[$label] Query failed: ${error.message}")
      }

    check(
      "HAVING names the alias",
      client.searchAs[CategoryAliased](
        "SELECT category AS cat FROM group_by_completeness GROUP BY category HAVING cat <> 'cat_01'"
      )
    )
    check(
      "HAVING names the column",
      client.searchAs[CategoryAliased](
        "SELECT category AS cat FROM group_by_completeness GROUP BY category HAVING category <> 'cat_01'"
      )
    )
  }

  it should "apply a HAVING over an alias-resolved bucket" in {
    // 🔴 Regression guard for the same desync on the HAVING path: the bucket `Identifier.update`
    // attaches is matched against the real one by DISPLAY NAME, so an aliased grouped column made
    // `Expression.includes` stop matching and the terms `exclude` vanished -- the user's filter
    // silently dropped and MORE rows came back.
    client.searchAs[CategoryAliased](
      "SELECT category AS cat FROM group_by_completeness GROUP BY cat HAVING cat <> 'cat_01'"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size (categories - 1).toLong
        rows.map(_.cat) should not contain "cat_01"
      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "group by a row-invariant constant, which is exactly ONE group (FOLD-IN 1, case A)" in {
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

  // ------------------------------------------------------------------
  // Story 21.6 AC-6 -- the CORPUS shapes, proven end to end on a real 3-shard cluster.
  //
  // Not extra GROUP BY coverage. These are the shapes that, WITHOUT story 21.3, flip from a LOUD
  // parse error to a SILENT wrong answer the moment 21.1/21.2's quoting lands:
  //   21 of the 99 captured BI statements are aggregate-free GROUP BY with NO LIMIT (#253), and
  //   tableau.sql92.w5.028 carries ORDER BY 1, which parses TODAY and is discarded silently.
  //
  // The tests above already cover the BARE spellings. What is new here is that the statements are
  // written the way Tableau actually writes them -- quoted, qualified, aliased -- which is the only
  // spelling the corpus contains and therefore the only one the scoreboard's `scored = fixed` claim
  // can rest on. The corpus witnesses, verbatim:
  //
  //   tableau.mysql.w2.028   SELECT `bi_events`.`category` AS `category`
  //                          FROM `elastic`.`bi_events` `bi_events` GROUP BY `bi_events`.`category`
  //   tableau.sql92.w5.026   the ANSI double-quoted spelling of the same shape
  //   tableau.sql92.w5.028   ... the same, plus ORDER BY 1 ASC
  //
  // superset.flightsql.w1.001 is deliberately NOT the oracle: it carries LIMIT 100 and `id` in the
  // grouping key, so it is a weak witness for both defects.
  //
  // NOTE the deliberate asymmetry with the tests above: these assert the GROUP KEYS and (for the
  // ORDER BY case) their ORDER, and nothing about doc counts. An aggregate-free projection HAS no
  // count column -- that is the shape #253 is about -- and adding COUNT(*) to obtain one would
  // destroy the thing under test. Doc coverage is already pinned by "GROUP BY without LIMIT" above.
  //
  // `CategoryOnly` is the file-top-level case class story 21.3 already added; `searchAs` is a macro
  // and binds a top-level case class, so it is reused rather than duplicated.
  // ------------------------------------------------------------------

  private val expectedCategories: Set[String] = (1 to categories).map(c => f"cat_$c%02d").toSet

  "corpus shape: aggregate-free GROUP BY, qualified backtick name, NO LIMIT" should
  "return one row per group" in {
    client.searchAs[CategoryOnly](
      "SELECT `g`.`category` AS `category` FROM `group_by_completeness` `g` GROUP BY `g`.`category`"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong // 37 groups, not 703 documents and not ES's 10
        rows.map(_.category).distinct should have size categories.toLong
        rows.map(_.category).toSet shouldBe expectedCategories
      case ElasticFailure(error) =>
        fail(s"aggregate-free GROUP BY over a backticked qualified name failed: ${error.message}")
    }
  }

  it should "do the same with the ANSI double-quoted spelling" in {
    client.searchAs[CategoryOnly](
      "SELECT \"g\".\"category\" AS \"category\" FROM \"group_by_completeness\" \"g\" " +
      "GROUP BY \"g\".\"category\""
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong
        rows.map(_.category).distinct should have size categories.toLong
        rows.map(_.category).toSet shouldBe expectedCategories
      case ElasticFailure(error) =>
        fail(s"aggregate-free GROUP BY over a quoted qualified name failed: ${error.message}")
    }
  }

  "corpus shape: a catalog prefix is captured and IGNORED" should
  "read the bare index, not a catalog-qualified one" in {
    // The corpus's real FROM is `elastic`.`bi_events` -- `elastic` is the schema OUR OWN driver
    // advertised, never part of the index name (21.2 finding 1; story 21.2 preserves the qualifier
    // in `Table.parts` and does NOT interpret it). This is the only end-to-end proof, against a real
    // cluster, that the prefix does not move the read: an index named `elastic.group_by_completeness`
    // does not exist, so if the qualifier reached the request this would fail or return nothing.
    client.searchAs[CategoryOnly](
      "SELECT `g`.`category` AS `category` " +
      "FROM `elastic`.`group_by_completeness` `g` GROUP BY `g`.`category`"
    ) match {
      case ElasticSuccess(rows) =>
        rows should have size categories.toLong
        rows.map(_.category).toSet shouldBe expectedCategories
      case ElasticFailure(error) =>
        fail(s"catalog-prefixed read failed: ${error.message}")
    }
  }

  "corpus shape: GROUP BY <qualified quoted name> ORDER BY 1 ASC" should
  "return the groups in ascending order, not merely be accepted" in {
    client.searchAs[CategoryOnly](
      "SELECT \"g\".\"category\" AS \"category\" FROM \"group_by_completeness\" \"g\" " +
      "GROUP BY \"g\".\"category\" ORDER BY 1 ASC"
    ) match {
      case ElasticSuccess(rows) =>
        // 🔴 ORDER, not acceptance. `ORDER BY 1` parses TODAY and was silently discarded, so without
        // story 21.3's ordinal resolution this statement flips from parse_error straight to
        // parses-and-answers-in-arbitrary-order -- which PD-3 forbids scoring as a fix. The
        // assertion is the EXACT ascending sequence rather than `values == values.sorted`, and it is
        // falsifiable: with the ORDER BY dropped the terms aggregation falls back to its default
        // doc_count-DESCENDING order, and `cat_i` holds exactly `i` docs, so the fallback order is
        // the exact REVERSE of this. Key order is exact on a multi-shard index; a doc_count-ordered
        // assertion would be shard-approximate.
        rows.map(_.category) shouldBe (1 to categories).map(c => f"cat_$c%02d")
      case ElasticFailure(error) =>
        fail(s"ORDER BY 1 over a quoted qualified GROUP BY failed: ${error.message}")
    }
  }

  /** `HAVING` with NO `GROUP BY`: standard SQL's implicit whole-table group.
    *
    * Four of the 99 captured BI statements are this shape, verbatim `SELECT SUM(1) AS `COL` FROM
    * `elastic`.`bi_events` `bi_events` HAVING COUNT(1)>0` -- it is Tableau's data-source
    * row-existence probe, so it fires on the most basic interaction there is. Story 21.6 measured
    * it here rather than scoring it off a parse verdict, and found TWO defects; these tests were
    * the pins that recorded them, and they are now the contract:
    *
    *   - `COUNT(<literal>)` reached Elasticsearch as a `value_count` with NEITHER `field` nor
    *     `script` and was rejected outright (`illegal_argument_exception`). `COUNT(1)` is
    *     `COUNT(*)` in ANSI SQL -- counting a constant counts rows -- and the operand is now
    *     rewritten to `*` at parse time, so it takes exactly the path `COUNT(*)` already took.
    *   - the `HAVING` itself EVAPORATED. A whole-table aggregation has no buckets, so the
    *     `bucket_selector` the GROUP BY path attaches to each bucket had nothing to attach to:
    *     `HAVING COUNT(*) > 10000` over this 703-document fixture returned 703, HTTP 200. The
    *     metric aggregations are now wrapped in a synthetic single-bucket keyed `filters`
    *     aggregation that the SAME `having_filter` selector hangs from, so a false predicate
    *     removes the only bucket and the statement returns no row.
    *
    * 🔴 THE FALSE-PREDICATE HALF IS THE WHOLE POINT. A `HAVING` that is silently DISCARDED still
    * satisfies the true-predicate case -- the aggregate is correct either way -- so asserting only
    * `COUNT(1) > 0` would be a test that cannot fail for the reason it exists. That is the
    * #205/#209/#224/#253 silent-wrong-answer family. The halves are asserted as a PAIR and neither
    * is evidence alone.
    *
    * Asserted on the RAW row via `client.search`, not through `searchAs`, for the reason the four
    * constant-projection tests above already give: the macro types a bare integer literal as BIGINT
    * while the value arrives as Jackson's smallest type, so `SUM(1)` over a literal cannot be bound
    * to a case-class field in either spelling. That mismatch is pre-existing and recorded
    * separately; it must not be allowed to decide whether the HAVING works.
    */
  "corpus shape: HAVING with no GROUP BY" should
  "answer COUNT(<literal>) as a row count -- the corpus spelling" in {
    implicit val havingCtx: ConversionContext = NativeContext
    // The corpus spelling verbatim, quoted and qualified as Tableau emits it. `COUNT(1)` is
    // `COUNT(*)` (ANSI), and `SUM(1)` over the whole table is the document count.
    client.search(
      SelectStatement(
        "SELECT SUM(1) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(1) > 0"
      )
    ) match {
      case ElasticSuccess(response) =>
        withClue(s"rows=${response.results}: ") {
          response.results should have size 1L
          response.results.head("COL").toString.toDouble shouldBe totalDocs.toDouble
          // The synthetic whole-table bucket must not surface: it is an emission device, not a
          // column. `rowNormalizer` APPENDS keys nobody requested, so a leak would be visible here.
          response.results.head.keys.toList shouldBe List("COL")
        }
      case ElasticFailure(error) => fail(s"the corpus HAVING shape failed: ${error.message}")
    }
  }

  it should "honour the predicate, both when it is TRUE and when it is FALSE" in {
    implicit val havingCtx: ConversionContext = NativeContext

    // 🔴 THE TWO HALVES ARE ONE TEST. The true-predicate half passes whether or not the predicate
    // is honoured -- the aggregate is correct either way -- which is exactly how a dropped HAVING
    // stayed invisible. Only the FALSE half distinguishes "the filter was applied" from "the
    // filter was discarded"; neither half is evidence alone.
    def rowsOf(sql: String): Seq[scala.collection.immutable.ListMap[String, Any]] =
      client.search(SelectStatement(sql)) match {
        case ElasticSuccess(response) => response.results
        case ElasticFailure(error)    => fail(s"[$sql] failed: ${error.message}")
      }

    // (a) TRUE: one row carrying the fixture total.
    val trueCount =
      rowsOf("SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(*) > 0")
    withClue(s"true count predicate, rows=$trueCount: ") {
      trueCount should have size 1L
      trueCount.head("COL").toString.toDouble shouldBe totalDocs.toDouble
    }

    // (b) FALSE: the whole table holds exactly `totalDocs` (703) documents, so `> 10000` is
    // unsatisfiable BY CONSTRUCTION and standard SQL returns NO row.
    withClue("false count predicate: ") {
      rowsOf(
        "SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(*) > 10000"
      ) shouldBe empty
    }

    // (c) the same over a metric rather than a count, so neither half is an artefact of COUNT(*).
    // `amount` is `d` for the d-th document of `cat_c`, so SUM(amount) = sum(c(c+1)/2) = 9139.
    val trueSum = rowsOf(
      "SELECT SUM(amount) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING SUM(amount) > 9000"
    )
    withClue(s"true metric predicate, rows=$trueSum: ") {
      trueSum should have size 1L
      trueSum.head("COL").toString.toDouble shouldBe 9139.0
    }
    withClue("false metric predicate: ") {
      rowsOf(
        "SELECT SUM(amount) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING SUM(amount) > 99999"
      ) shouldBe empty
    }

    // (d) a composite predicate, to prove the whole criteria tree reaches Elasticsearch and not
    // just its first term: TRUE AND FALSE must select nothing.
    withClue("composite predicate with one false term: ") {
      rowsOf(
        "SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" " +
        "HAVING COUNT(*) > 0 AND SUM(amount) > 99999"
      ) shouldBe empty
    }
  }

  // ------------------------------------------------------------------
  // Issue #389 -- a HAVING over a FUNCTION of an aggregate must FILTER, or FAIL.
  //
  // MEASURED on `main`: every shape below parsed, ran, and returned EVERY group with HTTP 200 --
  // the `bucket_selector` was never emitted because a function does not look inside its own
  // arguments. These rows EXECUTE the emitted script against a real cluster, because the claim is
  // that Elasticsearch COMPILES and RUNS it: `cat_i` holds exactly `i` documents, so the filtered
  // answer and the unfiltered one differ by construction (7 groups against 37).
  //
  // 🔴 EXECUTION is what set the emittable population, not reading. `ABS(COUNT(*)) > 1` renders a
  // perfectly good-looking boolean expression and Elasticsearch REFUSES to compile it
  // (`Double.valueOf` in a bucket-pipeline script). It is in the refusal list below, where the
  // cluster put it.
  // ------------------------------------------------------------------

  private val over30 = 7 // cat_31 .. cat_37

  "a HAVING over a function of an aggregate" should "filter the groups it names" in {
    // ⚠️ Unrolled, not looped: `searchAs` is a macro and needs a compile-time constant SQL string.
    def check(label: String, result: ElasticResult[Seq[CategoryCount]], expected: Int): Unit =
      result match {
        case ElasticSuccess(rows) =>
          withClue(s"[$label] rows=${rows.map(_.category).sorted}: ") {
            rows should have size expected.toLong
            rows.map(_.category).toSet shouldBe
            (categories - expected + 1 to categories).map(c => f"cat_$c%02d").toSet
          }
        case ElasticFailure(error) => fail(s"[$label] Query failed: ${error.message}")
      }

    check(
      "GREATEST over an aggregate",
      client.searchAs[CategoryCount](
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING GREATEST(COUNT(*), 0) > 30"
      ),
      over30
    )
    check(
      "COALESCE over an aggregate",
      client.searchAs[CategoryCount](
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING COALESCE(COUNT(*), 0) > 30"
      ),
      over30
    )
    check(
      "the aggregate on the RIGHT of the comparison",
      client.searchAs[CategoryCount](
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING 30 < GREATEST(COUNT(*), 0)"
      ),
      over30
    )
    check(
      "BETWEEN over a function of an aggregate",
      client.searchAs[CategoryCount](
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING GREATEST(COUNT(*), 0) BETWEEN 31 AND 37"
      ),
      over30
    )
  }

  it should "emit BOTH halves of a conjunction" in {
    // 🔴 On `main` the second conjunct was DROPPED, so this returned 37 groups: a PARTIAL filter,
    // which looks filtered and is wrong. The oracle (cat_31..cat_34) differs from BOTH the
    // unfiltered answer (37) and from either conjunct alone (7 and 34).
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > 30 AND GREATEST(COUNT(*), 0) < 35"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.category).toSet shouldBe Set("cat_31", "cat_32", "cat_33", "cat_34")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "create the aggregation when the aggregate appears ONLY in the HAVING function" in {
    // No SELECT aggregate at all: on `main` there was no `count_all` aggregation for the script to
    // read, so even a corrected script would have had nothing to compare.
    client.searchAs[CategoryOnly](
      "SELECT category FROM group_by_completeness GROUP BY category HAVING GREATEST(COUNT(*), 0) > 30"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.category).toSet shouldBe
          (categories - over30 + 1 to categories).map(c => f"cat_$c%02d").toSet
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "filter the implicit whole-table group too" in {
    implicit val wholeCtx: ConversionContext = NativeContext
    // 703 documents in total: the TRUE predicate keeps the single row, the FALSE one removes it.
    client.search(
      SelectStatement(
        "SELECT COUNT(*) AS c FROM group_by_completeness HAVING GREATEST(COUNT(*), 0) > 700"
      )
    ) match {
      case ElasticSuccess(response) => response.results should have size 1
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}")
    }
    client.search(
      SelectStatement(
        "SELECT COUNT(*) AS c FROM group_by_completeness HAVING GREATEST(COUNT(*), 0) > 9999"
      )
    ) match {
      case ElasticSuccess(response) => response.results shouldBe empty
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}")
    }
  }

  it should "REFUSE the shapes it cannot express, rather than return every group" in {
    // 🔴 The whole point of the issue: a predicate the engine cannot emit used to come back as
    // HTTP 200 over UNFILTERED groups. Each of these must now be an error.
    //
    // ⚠️ Asserted through `run`, not `search(SelectStatement(...))`: `SelectStatement` yields
    // `statement = None` on a rejection and the client reports its own generic "does not contain a
    // valid search request" message, which would hide WHICH rule fired. `run` is the REPL / JDBC /
    // Flight route and relays the parser's reason (#262).
    Seq(
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING ABS(COUNT(*)) > 30",
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING NULLIF(COUNT(*), 0) > 30",
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING NULLIF(cnt, 0) > 30",
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING CASE WHEN COUNT(*) > 30 THEN 1 ELSE 0 END = 1",
      "SELECT category, SUM(amount) AS s FROM group_by_completeness GROUP BY category " +
      "HAVING ROUND(SUM(amount), 2) > 30",
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > 30 AND NULLIF(COUNT(*), 0) > 1",
      // 🔴 The RIGHT-operand family (F1): the left operand is a bare aggregate, so the gate used to
      // be skipped entirely and these reached Elasticsearch as HTTP 400 / an internal-error label.
      "SELECT category, SUM(amount) AS s FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > ROUND(SUM(amount), 2)",
      "SELECT category, SUM(amount) AS s FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > NULLIF(SUM(amount), 0)",
      "SELECT category, SUM(amount) AS s FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) BETWEEN 1 AND ABS(SUM(amount))",
      "SELECT category, SUM(amount) AS s FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > CASE WHEN SUM(amount) > 1 THEN 1 ELSE 0 END"
    ).foreach { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(result) => fail(s"[$sql] was accepted and answered $result")
        case ElasticFailure(error) =>
          withClue(s"[$sql] ") { error.message should include("HAVING cannot") }
      }
    }
  }

  it should "REFUSE a key condition the terms filter cannot express, rather than answer wrongly" in {
    // 🔴 ROUND 3. Round 2 pushed these into the QUERY as a document filter. That is unsound twice,
    // both MEASURED on real Elasticsearch: with a key that is a FUNCTION of the column a SURVIVING
    // bucket's count changed (`GROUP BY DAY(d) HAVING YEAR(d) = 2025` kept bucket 3 and moved its
    // count 3 -> 1), and on a MULTI-VALUED field a bucket that FAILS the predicate survived,
    // because a document in several buckets is kept or dropped whole. The `terms` `include` path
    // answers such a grouping correctly, so the push-down was strictly LESS sound than the
    // mechanism it was meant to generalise.
    Seq(
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING UPPER(category) = 'CAT_37'",
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING SUBSTRING(category, 5, 2) = '37'"
    ).foreach { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticSuccess(result) => fail(s"[$sql] was accepted and answered $result")
        case ElasticFailure(error) =>
          withClue(s"[$sql] ") { error.message should include("HAVING cannot filter on") }
      }
    }
  }

  it should "still apply a key condition the terms filter DOES express" in {
    // The complement: this is the mechanism the refusal above defers to, and it must keep working.
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING category = 'cat_37'"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(r => r.category -> r.cnt) shouldBe Seq("cat_37" -> categories.toLong)
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "combine a key condition with an aggregate condition as an AND" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > 35 AND category <> 'cat_36'"
    ) match {
      case ElasticSuccess(rows)  => rows.map(_.category).toSet shouldBe Set("cat_37")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "REFUSE an OR whose branches need different mechanisms" in {
    // 🔴 PRE-EXISTING silent wrong answer: the key became a terms `include` and the aggregate a
    // `bucket_selector`, so the OR was EXECUTED AS AN AND. Measured on `main` over a 4-document
    // fixture: `HAVING COUNT(*) > 1 OR status = 'b'` returned NO groups where the disjunction is
    // two of them.
    Await.result(
      client.run(
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING COUNT(*) > 35 OR category = 'cat_01'"
      ),
      60.seconds
    ) match {
      case ElasticSuccess(result) => fail(s"the OR was accepted and answered $result")
      case ElasticFailure(error)  => error.message should include("different mechanisms")
    }
  }

  it should "still accept an OR the terms filter unions into one include list" in {
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING category = 'cat_36' OR category = 'cat_37'"
    ) match {
      case ElasticSuccess(rows) =>
        rows.map(_.category).toSet shouldBe Set("cat_36", "cat_37")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  // -----------------------------------------------------------------------------------------
  // The include/exclude CHANNEL, EXECUTED (rule b2)
  //
  // 🔴 `excludes` IS `includes(bucket, !not, …)` -- one method, two senses -- and every other row
  // in this file exercises the INCLUDE sense. These are the exclude-side cells, each against a
  // bucket set computed by hand from the fixture: `cat_i` holds exactly `i` documents, 37 groups.
  // -----------------------------------------------------------------------------------------

  private def categoriesOf(having: String): Seq[String] =
    client.searchAsUnchecked[CategoryCount](
      SelectStatement(
        s"SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category $having"
      )
    ) match {
      case ElasticSuccess(rows)  => rows.map(_.category).sorted
      case ElasticFailure(error) => fail(s"[$having] failed: ${error.message}")
    }

  private def categoryRefusal(having: String): String =
    Await.result(
      client.run(
        s"SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category $having"
      ),
      60.seconds
    ) match {
      case ElasticSuccess(result) => fail(s"[$having] was accepted and answered $result")
      case ElasticFailure(error)  => error.message
    }

  private lazy val allCategories: Seq[String] = (1 to categories).map(c => f"cat_$c%02d").sorted

  "an AND of inequalities on the GROUP BY key" should "remove exactly those groups" in {
    // NOT-in-A and NOT-in-B is NOT-in-(A ∪ B): the exclude list IS that union, so this is correct
    // on `455433ae` and must stay byte-identical.
    categoriesOf("HAVING category <> 'cat_36' AND category <> 'cat_37'") shouldBe
    allCategories.filterNot(Set("cat_36", "cat_37"))
    categoriesOf(
      "HAVING category NOT IN ('cat_36','cat_37') AND category <> 'cat_35'"
    ) shouldBe allCategories.filterNot(Set("cat_35", "cat_36", "cat_37"))
    // three leaves, the same union one level deeper
    categoriesOf(
      "HAVING category <> 'cat_35' AND category <> 'cat_36' AND category <> 'cat_37'"
    ) shouldBe allCategories.filterNot(Set("cat_35", "cat_36", "cat_37"))
  }

  it should "still combine with an aggregate condition, which is a different mechanism" in {
    categoriesOf("HAVING category <> 'cat_37' AND COUNT(*) > 35") shouldBe Seq("cat_36")
  }

  "an OR of inequalities" should "be REFUSED, because the exclude list would execute it as an AND" in {
    // 🔴 MEASURED on `455433ae` and on this cluster: it emitted the SAME `exclude:["cat_36",
    // "cat_37"]` as the AND above. The disjunction is true for EVERY one of the 37 groups, and two
    // were dropped -- HTTP 200, no error. Live on `main` today.
    categoryRefusal(
      "HAVING category <> 'cat_36' OR category <> 'cat_37'"
    ) should include("cannot combine")
    categoryRefusal(
      "HAVING category NOT IN ('cat_36','cat_37') OR category <> 'cat_35'"
    ) should include("cannot combine")
  }

  "an AND of equalities" should "be REFUSED, because the include list means OR" in {
    // `include:["cat_36","cat_37"]` keeps two groups; the SQL means none.
    categoryRefusal(
      "HAVING category = 'cat_36' AND category = 'cat_37'"
    ) should include("cannot combine")
  }

  "a MIXED pair under OR" should "be REFUSED, because the two lists are ANDed" in {
    categoryRefusal(
      "HAVING category = 'cat_37' OR category <> 'cat_36'"
    ) should include("cannot combine")
  }

  it should "still honour one include and one exclude under AND" in {
    // Both lists carry SEVERAL values. The ONE-value forms of this shape used to fail on ES 6.8 (a
    // one-entry list was written as a bare string, which 6.8 reads as a REGEX, and a regex beside a
    // set is `Cannot mix ...`, HTTP 400); the one-value key-filter rows below pin them.
    categoriesOf(
      "HAVING category IN ('cat_35','cat_36','cat_37') AND category NOT IN ('cat_35','cat_36')"
    ) shouldBe Seq("cat_37")
  }

  "a pattern meeting a value list on the same key" should "be REFUSED, not half-applied" in {
    // 🔴 MEASURED on ES 8.18.3 over a three-bucket fixture: `= 'a' OR LIKE 'b%'` emitted
    // `include:"b.*"` and returned only the pattern's bucket -- the value list is DISCARDED by the
    // emission when a pattern is present, and a second pattern is lost to `orElse`.
    // ⚠️ the client caps a relayed parse message at 200 characters (#262), so the assertion
    // anchors on the TAIL of the reason, not its middle.
    categoryRefusal(
      "HAVING category = 'cat_37' OR category LIKE 'cat_3%'"
    ) should include("Use a single RLIKE")
    // ... and the pattern ALONE still works: cat_30 .. cat_37.
    categoriesOf("HAVING category LIKE 'cat_3%'") shouldBe
    (30 to 37).map(c => f"cat_$c%02d")
  }

  "a LIKE pattern on the key" should "treat _ as a wildcard, like every other LIKE" in {
    // 🔴 EXECUTED. The terms channel had its own LIKE translation that left `_` literal, so this
    // matched NOTHING on `455433ae` -- there is no category named `cat_0_`. With the shared
    // translation `_` is one character, so it selects cat_01 .. cat_09.
    categoriesOf("HAVING category LIKE 'cat_0_'") shouldBe (1 to 9).map(c => f"cat_0$c%d")
    // ... and the plain wildcard form is unchanged.
    categoriesOf("HAVING category LIKE 'cat_3%'") shouldBe (30 to 37).map(c => f"cat_$c%02d")
  }

  "an OR across TWO grouping levels" should "be REFUSED, because Elasticsearch NESTS them" in {
    // 🔴 The two `terms` aggregations are nested, which IS a conjunction: an OR across them
    // returned the intersection. Measured on a two-key fixture; here the refusal is asserted
    // end-to-end, and the AND -- which is exactly what the nesting means -- still answers.
    Await.result(
      client.run(
        "SELECT category, id, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category, id " +
        "HAVING category = 'cat_02' OR id = 'cat_02_1'"
      ),
      60.seconds
    ) match {
      case ElasticSuccess(result) => fail(s"the OR was accepted and answered $result")
      case ElasticFailure(error)  => error.message should include("DIFFERENT GROUP BY keys")
    }
    client.searchAsUnchecked[CategoryCount](
      SelectStatement(
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category, id " +
        "HAVING category = 'cat_02' AND id = 'cat_02_1'"
      )
    ) match {
      case ElasticSuccess(rows)  => rows.map(_.category) shouldBe Seq("cat_02")
      case ElasticFailure(error) => fail(s"the AND failed: ${error.message}")
    }
  }

  it should "REFUSE a HAVING on a column that is neither grouped nor aggregated" in {
    Await.result(
      client.run(
        "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
        "HAVING UPPER(id) = 'X'"
      ),
      60.seconds
    ) match {
      case ElasticSuccess(result) => fail(s"accepted and answered $result")
      case ElasticFailure(error) =>
        error.message should include("can only filter on a GROUP BY key")
    }
  }

  it should "still return every group when the un-expressible predicate is NOT there" in {
    // 🔴 Non-vacuity for the refusals above: the same statements WITHOUT the offending function
    // are accepted, so the rejection is about the predicate and not about the fixture.
    client.searchAs[CategoryCount](
      "SELECT category, COUNT(*) AS cnt FROM group_by_completeness GROUP BY category " +
      "HAVING COUNT(*) > 30"
    ) match {
      case ElasticSuccess(rows)  => rows should have size over30.toLong
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "refuse -- never discard -- a HAVING it cannot evaluate over the implicit group" in {
    implicit val havingCtx: ConversionContext = NativeContext
    // `category` is not aggregated and there is no GROUP BY, so there is no group to filter. The
    // predicate must not be dropped: the statement is rejected.
    val res = client.search(
      SelectStatement(
        "SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING category = 'cat_01'"
      )
    )
    withClue(s"a non-aggregated HAVING was accepted: $res: ") {
      res.isSuccess shouldBe false
    }
  }

  // ------------------------------------------------------------------
  // COUNT(DISTINCT *) and COUNT(DISTINCT _id) count DOCUMENTS, exactly as COUNT(*) does (an `_id`
  // is unique within its index, and this fixture is one index).
  //
  // 🔴 The search bridge emitted both as the `cardinality` of `_index`: the number of distinct
  // INDICES, so every group answered 1 -- and so did the whole table -- with HTTP 200. The oracle is
  // the fixture, not a second query through the engine: `cat_i` holds exactly `i` documents (703
  // in all), so the old answer is right for one group of 37 and wrong for the table.
  // ------------------------------------------------------------------

  private val documentCounts = Seq("n" -> "COUNT(DISTINCT *)", "d" -> "COUNT(DISTINCT _id)")

  "COUNT(DISTINCT *) and COUNT(DISTINCT _id)" should "count the documents of every group, as COUNT(*) does" in {
    implicit val distinctCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement(
        "SELECT category, COUNT(DISTINCT *) AS n, COUNT(DISTINCT _id) AS d, COUNT(*) AS c " +
        "FROM group_by_completeness GROUP BY category"
      )
    ) match {
      case ElasticSuccess(response) =>
        documentCounts.foreach { case (column, count) =>
          withClue(s"[$count] rows=${response.results.take(4)}: ") {
            response.results
              .map(row => row("category").toString -> row(column).toString.toDouble)
              .toMap shouldBe (1 to categories).map(c => f"cat_$c%02d" -> c.toDouble).toMap
            // Read back exactly as COUNT(*) is: the same value, of the same runtime type (the typed
            // ES 8 / 9 models read a `value_count` as a double, a `cardinality` as a long).
            response.results.foreach { row =>
              row(column) shouldBe row("c")
              row(column).getClass shouldBe row("c").getClass
            }
          }
        }
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "count the documents of the whole table" in {
    implicit val distinctCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement(
        "SELECT COUNT(DISTINCT *) AS n, COUNT(DISTINCT _id) AS d, COUNT(*) AS c " +
        "FROM group_by_completeness"
      )
    ) match {
      case ElasticSuccess(response) =>
        withClue(s"rows=${response.results}: ") {
          response.results should have size 1
          val row = response.results.head
          documentCounts.foreach { case (column, count) =>
            withClue(s"[$count] ") {
              row(column).toString.toDouble shouldBe totalDocs.toDouble
              row(column) shouldBe row("c")
              row(column).getClass shouldBe row("c").getClass
            }
          }
        }
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "filter the groups on their document count in a HAVING" in {
    // cat_31 .. cat_37 hold more than 30 documents; with the count of indices no group did.
    implicit val distinctCtx: ConversionContext = NativeContext
    documentCounts.foreach { case (_, count) =>
      client.search(
        SelectStatement(
          s"SELECT category FROM group_by_completeness GROUP BY category HAVING $count > 30"
        )
      ) match {
        case ElasticSuccess(response) =>
          withClue(s"[$count] ") {
            response.results.map(_("category").toString).toSet shouldBe
            (categories - over30 + 1 to categories).map(c => f"cat_$c%02d").toSet
          }
        case ElasticFailure(error) => fail(s"[$count] Query failed: ${error.message}")
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // F1 -- an aggregate reads in HAVING exactly as SELECT returns it.
  //
  // In a group where no document has the aggregated column, SELECT answers NULL for MIN, MAX,
  // AVG, the percentiles and arithmetic over them (`ClientAggregation.nullOverEmptyInput`), and
  // SQL keeps a group only when its condition is TRUE -- a comparison with NULL is UNKNOWN, and
  // so is its NOT. The `bucket_selector` was handed Elasticsearch's sentinel instead (`NaN`), so
  // `MAX(v) <> 5` KEPT such a group and `ISNULL(MAX(v))` dropped it, HTTP 200.
  //
  // The population is DERIVED, not listed: the aggregates from that rule, every comparison, every
  // NOT the grammar accepts, ISNULL / ISNOTNULL, each of them under AND / OR with COUNT(*), on
  // both venues of the aggregate (HAVING only, and published by the SELECT list), plus the
  // implicit whole-table group. The expected groups are computed HERE, from the fixture, with
  // three-valued logic -- never from the engine. Every statement runs through the gateway.
  // ---------------------------------------------------------------------------------------------

  private val nullIndex = "having_null_groups"

  /** Each group and the `v` of each of its documents (`None`: the document has no `v`): absent from
    * every document, partly present, fully present. The present values of a group are all equal, so
    * every percentile of it is exact on every major -- the oracle never models a digest.
    */
  private val nullGroups: Seq[(String, Seq[Option[Int]])] = Seq(
    "a1" -> Seq(None, None),
    "a2" -> Seq(None, None, None),
    "p1" -> Seq(Some(5), None),
    "p2" -> Seq(Some(7), Some(7), None),
    "f1" -> Seq(Some(5), Some(5)),
    "f2" -> Seq(Some(7), Some(7), Some(7))
  )

  private lazy val nullGroupsLoaded: Unit = {
    client
      .createIndex(nullIndex, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client
      .setMapping(
        nullIndex,
        """{"properties": {"id": {"type": "keyword"}, "g": {"type": "keyword"}, "v": {"type": "integer"}}}"""
      )
      .get shouldBe true
    val docs = for {
      (g, vs)     <- nullGroups.toList
      (v, offset) <- vs.zipWithIndex
    } yield v.fold(s"""{"id":"$g-$offset","g":"$g"}""")(x =>
      s"""{"id":"$g-$offset","g":"$g","v":$x}"""
    )
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = nullIndex, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_)     => client.refresh(nullIndex)
      case ElasticFailure(error) => fail(s"Bulk indexing into $nullIndex failed: ${error.message}")
    }
  }

  /** The aggregates SELECT answers NULL over a group with no value, DERIVED from the rule the
    * response parser applies (`ClientAggregation.nullOverEmptyInput`) over every aggregation type.
    */
  private lazy val nullOverEmptyTypes: Seq[AggregationType.AggregationType] =
    AggregationType.values.toSeq.filter(t =>
      ClientAggregation(
        aggName = "a",
        aggType = t,
        distinct = false,
        sourceField = "v",
        windowing = false,
        bucketPath = "",
        bucketRoot = ""
      ).nullOverEmptyInput
    )

  /** The derived aggregates a HAVING cannot answer at all -- MEASURED on main, every form failing
    * in Elasticsearch before any group is filtered, and recorded as their own defects.
    */
  private val notAnsweredByHaving: Map[AggregationType.AggregationType, String] = Map(
    AggregationType.Stddev     -> "extended_stats read without its key by the bucket_selector",
    AggregationType.StddevSamp -> "extended_stats read without its key by the bucket_selector",
    AggregationType.StddevPop  -> "extended_stats read without its key by the bucket_selector",
    AggregationType.Variance   -> "extended_stats read without its key by the bucket_selector",
    AggregationType.VarSamp    -> "extended_stats read without its key by the bucket_selector",
    AggregationType.VarPop     -> "extended_stats read without its key by the bucket_selector",
    AggregationType.FirstValue -> "top_hits: no number a bucket_selector can read",
    AggregationType.LastValue  -> "top_hits: no number a bucket_selector can read"
  )

  /** How a HAVING names an aggregate of `nullOverEmptyTypes` over `v`, and what SELECT answers for
    * it over a group's present values. Arithmetic over aggregates is named by its SELECT alias.
    */
  private final case class NullableAggregate(
    kind: AggregationType.AggregationType,
    having: String,
    selectItem: String,
    value: Seq[Int] => Double
  )

  private def nullableAggregate(t: AggregationType.AggregationType): NullableAggregate = {
    def single(present: Seq[Int]): Double = {
      present.distinct should have size 1L
      present.head.toDouble
    }
    t match {
      case AggregationType.Min => NullableAggregate(t, "MIN(v)", "MIN(v) AS a", _.min.toDouble)
      case AggregationType.Max => NullableAggregate(t, "MAX(v)", "MAX(v) AS a", _.max.toDouble)
      case AggregationType.Avg =>
        NullableAggregate(t, "AVG(v)", "AVG(v) AS a", vs => vs.sum.toDouble / vs.size)
      case AggregationType.PercentileCont =>
        val p = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY v)"
        NullableAggregate(t, p, s"$p AS a", single)
      case AggregationType.PercentileDisc =>
        val p = "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY v)"
        NullableAggregate(t, p, s"$p AS a", single)
      case AggregationType.BucketScript =>
        NullableAggregate(t, "d", "MAX(v) - MIN(v) AS d", vs => (vs.max - vs.min).toDouble)
      case other => fail(s"$other is NULL over an empty group and has no HAVING spelling here")
    }
  }

  /** A HAVING condition over the aggregate `a`, rendered as SQL and evaluated as SQL does: `None`
    * is UNKNOWN.
    */
  private sealed trait NullCond {
    def sql(a: String): String
    def eval(v: Option[Double], rows: Int): Option[Boolean]
  }

  private final case class Cmp(op: String, k: Int) extends NullCond {
    def sql(a: String): String = s"$a $op $k"
    def eval(v: Option[Double], rows: Int): Option[Boolean] = v.map { x =>
      op match {
        case ">"         => x > k
        case "<"         => x < k
        case ">="        => x >= k
        case "<="        => x <= k
        case "="         => x == k
        case "<>" | "!=" => x != k
      }
    }
  }

  private final case class NotCmp(cmp: Cmp) extends NullCond {
    def sql(a: String): String = s"NOT ${cmp.sql(a)}"
    def eval(v: Option[Double], rows: Int): Option[Boolean] = cmp.eval(v, rows).map(!_)
  }

  private final case class Between(lo: Int, hi: Int, not: Boolean) extends NullCond {
    def sql(a: String): String = s"$a ${if (not) "NOT " else ""}BETWEEN $lo AND $hi"
    def eval(v: Option[Double], rows: Int): Option[Boolean] =
      v.map(x => (x >= lo && x <= hi) != not)
  }

  private final case class In(ks: Seq[Int], not: Boolean) extends NullCond {
    def sql(a: String): String = s"$a ${if (not) "NOT " else ""}IN (${ks.mkString(", ")})"
    def eval(v: Option[Double], rows: Int): Option[Boolean] =
      v.map(x => ks.exists(_.toDouble == x) != not)
  }

  private final case class NullTest(isNull: Boolean) extends NullCond {
    def sql(a: String): String = if (isNull) s"ISNULL($a)" else s"ISNOTNULL($a)"
    def eval(v: Option[Double], rows: Int): Option[Boolean] = Some(v.isEmpty == isNull)
  }

  private final case class RowsAbove(n: Int) extends NullCond {
    def sql(a: String): String = s"COUNT(*) > $n"
    def eval(v: Option[Double], rows: Int): Option[Boolean] = Some(rows > n)
  }

  /** `left AND|OR [NOT] right`: the grammar's `NOT` after an operator negates the RIGHT operand. */
  private final case class Junction(
    left: NullCond,
    and: Boolean,
    notRight: Boolean,
    right: NullCond
  ) extends NullCond {
    def sql(a: String): String =
      s"${left.sql(a)} ${if (and) "AND" else "OR"}${if (notRight) " NOT" else ""} ${right.sql(a)}"
    def eval(v: Option[Double], rows: Int): Option[Boolean] = {
      val l = left.eval(v, rows)
      val r = right.eval(v, rows).map(b => if (notRight) !b else b)
      if (and) {
        (l, r) match {
          case (Some(false), _) | (_, Some(false)) => Some(false)
          case (Some(true), Some(true))            => Some(true)
          case _                                   => None
        }
      } else {
        (l, r) match {
          case (Some(true), _) | (_, Some(true)) => Some(true)
          case (Some(false), Some(false))        => Some(false)
          case _                                 => None
        }
      }
    }
  }

  private val comparisons: Seq[Cmp] =
    Seq(
      Cmp(">", 6),
      Cmp("<", 6),
      Cmp(">=", 7),
      Cmp("<=", 5),
      Cmp("=", 5),
      Cmp("<>", 5),
      Cmp("!=", 7)
    )

  private val positives: Seq[NullCond] =
    comparisons ++ Seq(Between(4, 6, not = false), In(Seq(5, 8), not = false)) ++
    Seq(NullTest(isNull = true), NullTest(isNull = false))

  /** Every NOT the grammar accepts over an aggregate: before a comparison, inside BETWEEN / IN, and
    * after AND / OR (below). `NOT ISNULL(...)` at the start of a clause is not accepted.
    */
  private val negations: Seq[NullCond] =
    comparisons.map(c => NotCmp(c)) ++ Seq(Between(4, 6, not = true), In(Seq(5, 8), not = true))

  private val rowsAbove = RowsAbove(2)

  private val nullForms: Seq[NullCond] = {
    val alone = positives ++ negations
    val withCount = alone.flatMap(c =>
      Seq(
        Junction(rowsAbove, and = true, notRight = false, c),
        Junction(rowsAbove, and = false, notRight = false, c),
        Junction(c, and = true, notRight = false, rowsAbove),
        Junction(c, and = false, notRight = false, rowsAbove)
      )
    )
    val negatedAfterOperator = positives.flatMap(c =>
      Seq(
        Junction(rowsAbove, and = true, notRight = true, c),
        Junction(rowsAbove, and = false, notRight = true, c)
      )
    )
    alone ++ withCount ++ negatedAfterOperator
  }

  private def readsNullTest(c: NullCond): Boolean = c match {
    case _: NullTest          => true
    case Junction(l, _, _, r) => readsNullTest(l) || readsNullTest(r)
    case _                    => false
  }

  /** `wholeTable`: the one group the WHERE selects, for a statement with no GROUP BY. */
  private final case class NullStatement(
    sql: String,
    expected: Set[String],
    wholeTable: Option[String] = None
  )

  private lazy val nullCandidates: Seq[NullStatement] = {
    val answered = nullOverEmptyTypes.filterNot(notAnsweredByHaving.contains).map(nullableAggregate)
    val oracle: Seq[(String, Option[Seq[Int]], Int)] = nullGroups.map { case (g, vs) =>
      (g, Some(vs.flatten).filter(_.nonEmpty), vs.size)
    }
    def keep(agg: NullableAggregate, c: NullCond): Set[String] =
      oracle.collect {
        case (g, present, rows) if c.eval(present.map(agg.value), rows).contains(true) => g
      }.toSet
    val grouped = for {
      agg <- answered
      c   <- nullForms
      // `ISNULL(d)` over the alias of arithmetic over aggregates reads the OPERANDS, which its
      // selector does not declare -- it fails on main, before any group is filtered.
      if !(agg.kind == AggregationType.BucketScript && readsNullTest(c))
      select <-
        if (agg.kind == AggregationType.BucketScript) Seq(s"g, ${agg.selectItem}")
        else Seq("g, COUNT(*) AS c", s"g, ${agg.selectItem}")
    } yield NullStatement(
      s"SELECT $select FROM $nullIndex GROUP BY g HAVING ${c.sql(agg.having)}",
      keep(agg, c)
    )
    // The implicit whole-table group, one group of each kind at a time.
    val wholeTable = for {
      agg     <- answered.filterNot(_.kind == AggregationType.BucketScript)
      c       <- positives ++ negations
      (g, vs) <- nullGroups.filter { case (g, _) => Set("a1", "p1", "f1").contains(g) }
    } yield NullStatement(
      s"SELECT ${agg.selectItem} FROM $nullIndex WHERE g = '$g' HAVING ${c.sql(agg.having)}",
      if (c.eval(Some(vs.flatten).filter(_.nonEmpty).map(agg.value), vs.size).contains(true))
        Set(g)
      else Set.empty,
      wholeTable = Some(g)
    )
    grouped ++ wholeTable
  }

  /** The forms the grammar accepts -- and the ones it does not, which must be ONLY `ISNULL(...)` /
    * `ISNOTNULL(...)` over the WITHIN GROUP spelling of a percentile (asserted in the test).
    */
  private lazy val nullPartition: (Seq[NullStatement], Seq[NullStatement]) =
    nullCandidates.partition(st => Parser(st.sql).isRight)

  private def nullPopulation: Seq[NullStatement] = nullPartition._1

  private def nullPopulationNotParsed: Seq[NullStatement] = nullPartition._2

  /** The rows of a statement, through `GatewayApi.run` (JDBC / REPL / Flight SQL). */
  private def gatewayRows(sql: String): Either[String, Seq[ListMap[String, Any]]] =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => Right(rows)
      case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
      case ElasticSuccess(QueryStream(stream, _)) =>
        Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
      case ElasticSuccess(other) => Left(s"unexpected result $other")
      case ElasticFailure(error) => Left(error.message)
    }

  /** The groups a statement keeps, through the gateway. */
  private def keptGroups(statement: NullStatement): Either[String, Set[String]] =
    gatewayRows(statement.sql).map { rs =>
      statement.wholeTable match {
        case None => rs.map(_.getOrElse("g", "?").toString).toSet
        // The implicit group is ONE row or none; any other count is a wrong answer of its own.
        case Some(g) =>
          if (rs.isEmpty) Set.empty[String]
          else if (rs.size == 1) Set(g)
          else Set(g, s"${rs.size} rows")
      }
    }

  "a HAVING over an aggregate a group has no value for" should
  "keep exactly the groups SQL's three-valued logic keeps" in {
    nullGroupsLoaded
    // Non-vacuity, computed over the material: the population must hold the shapes the defect
    // lives in, and the oracle must both keep and drop the groups that have no value.
    val population = nullPopulation
    population.size should be >= 1000
    Seq(
      " <> ",
      " != ",
      "HAVING NOT ",
      " NOT BETWEEN ",
      " NOT IN ",
      "ISNULL(",
      "ISNOTNULL(",
      " AND NOT ",
      " OR NOT "
    )
      .foreach(shape =>
        withClue(s"[$shape] ")(population.exists(_.sql.contains(shape)) shouldBe true)
      )
    population.exists(_.expected.contains("a1")) shouldBe true
    population.exists(st => !st.expected.contains("a1") && st.expected.contains("f1")) shouldBe true
    // The parser trimmed only `ISNULL` / `ISNOTNULL` over the WITHIN GROUP spelling.
    nullPopulationNotParsed
      .map(_.sql)
      .filterNot(sql =>
        sql.contains("ISNULL(PERCENTILE_") || sql.contains("ISNOTNULL(PERCENTILE_")
      ) shouldBe empty

    val outcomes = population.map(st => st -> keptGroups(st))
    val wrong = outcomes.collect {
      case (st, Right(actual)) if actual != st.expected => (st, actual)
    }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    val wrongGroups = wrong.map { case (st, actual) =>
      ((st.expected -- actual) ++ (actual -- st.expected)).size
    }.sum
    info(
      s"HAVING over valueless groups: ${population.size} statements over ${nullOverEmptyTypes.size} derived " +
      s"aggregates (${notAnsweredByHaving.keys.toSeq.map(_.toString).sorted.mkString(", ")} not " +
      s"answered by HAVING); wrong: ${wrong.size} statements / $wrongGroups groups; " +
      s"errors: ${errors.size}"
    )
    val report = wrong.take(20).map { case (st, actual) =>
      s"[${st.sql}] kept ${actual.toSeq.sorted.mkString(",")}, expected ${st.expected.toSeq.sorted.mkString(",")}"
    }
    withClue(
      s"${wrong.size} wrong statements ($wrongGroups wrong groups), ${errors.size} errors:\n" +
      (report ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------------------------
  // A COALESCE of aggregates in HAVING is guarded on its RESULT: the comparison is UNKNOWN only
  // when every argument is NULL. It was guarded on the argument its rendering does not test, the
  // LAST one, so once an empty group's aggregate read as NULL, `COALESCE(MAX(a), MIN(b)) > 1`
  // dropped a group whose documents have `a` and no `b`.
  //
  // The fixture is the review's: a group with `a` only, `b` only, both, neither, and a `0` on
  // either side. The population is a COALESCE in every position its guard is derived for --
  // compared, under NOT / BETWEEN / IN, on the right of an aggregate or of a literal,
  // null-tested, nested, with three arguments, as the argument of a function -- on both venues
  // of the aggregates, plus the review's four statements in the implicit whole-table group. The
  // expected groups are computed HERE, with three-valued logic, never from the engine.
  // ---------------------------------------------------------------------------------------------

  private val coalesceIndex = "having_coalesce_groups"

  /** Each group and the `(a, b)` of each of its documents (`None`: the document lacks it). */
  private val coalesceGroups: Seq[(String, Seq[(Option[Int], Option[Int])])] = Seq(
    "g1" -> Seq((Some(5), None), (Some(5), None)),
    "g2" -> Seq((None, Some(5))),
    "g3" -> Seq((Some(5), Some(5))),
    "g4" -> Seq((None, None)),
    "g5" -> Seq((Some(0), None)),
    "g6" -> Seq((None, Some(0)))
  )

  private lazy val coalesceGroupsLoaded: Unit = {
    client
      .createIndex(coalesceIndex, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client
      .setMapping(
        coalesceIndex,
        """{"properties": {"id": {"type": "keyword"}, "g": {"type": "keyword"}, "a": {"type": "integer"}, "b": {"type": "integer"}}}"""
      )
      .get shouldBe true
    val docs = for {
      (g, rows)        <- coalesceGroups.toList
      ((a, b), offset) <- rows.zipWithIndex
    } yield (Seq(s""""id":"$g-$offset"""", s""""g":"$g"""") ++ a.map(x => s""""a":$x""") ++
    b.map(x => s""""b":$x""")).mkString("{", ",", "}")
    implicit val bulkOptions: BulkOptions =
      BulkOptions(defaultIndex = coalesceIndex, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(coalesceIndex)
      case ElasticFailure(error) =>
        fail(s"Bulk indexing into $coalesceIndex failed: ${error.message}")
    }
  }

  /** The present `a` and `b` values of a group, and its document count. */
  private final case class CoalesceGroup(rows: Int, as: Seq[Int], bs: Seq[Int])

  /** An operand of the HAVING comparison, and its value over a group as SELECT answers it (#336):
    * NULL over no value, except `SUM`, which is `0`.
    */
  private sealed trait CoalesceOperand {
    def sql: String
    def value(group: CoalesceGroup): Option[Double]
  }

  private final case class Aggregate(sql: String) extends CoalesceOperand {
    def value(group: CoalesceGroup): Option[Double] = sql match {
      case "MAX(a)" => group.as.reduceOption(_ max _).map(_.toDouble)
      case "MIN(b)" => group.bs.reduceOption(_ min _).map(_.toDouble)
      case "MAX(b)" => group.bs.reduceOption(_ max _).map(_.toDouble)
      case "SUM(a)" => Some(group.as.sum.toDouble)
      case other    => fail(s"no value for $other")
    }
  }

  private final case class Literal(k: Int) extends CoalesceOperand {
    def sql: String = k.toString
    def value(group: CoalesceGroup): Option[Double] = Some(k.toDouble)
  }

  private final case class CoalesceOf(arguments: CoalesceOperand*) extends CoalesceOperand {
    def sql: String = arguments.map(_.sql).mkString("COALESCE(", ", ", ")")
    def value(group: CoalesceGroup): Option[Double] =
      arguments.flatMap(_.value(group)).headOption
  }

  private final case class SignOf(argument: CoalesceOperand) extends CoalesceOperand {
    def sql: String = s"SIGN(${argument.sql})"
    def value(group: CoalesceGroup): Option[Double] = argument.value(group).map(math.signum)
  }

  private def compares(left: Double, op: String, right: Double): Boolean = op match {
    case ">"  => left > right
    case "<"  => left < right
    case ">=" => left >= right
    case "<=" => left <= right
    case "="  => left == right
    case "<>" => left != right
  }

  /** `COUNT(*) <op> X`: an aggregate on the LEFT of the operand. */
  private final case class CountVs(op: String) extends NullCond {
    def sql(a: String): String = s"COUNT(*) $op $a"
    def eval(v: Option[Double], rows: Int): Option[Boolean] =
      v.map(x => compares(rows.toDouble, op, x))
  }

  /** `k <op> X`: a literal on the LEFT of the operand. */
  private final case class LiteralVs(k: Int, op: String) extends NullCond {
    def sql(a: String): String = s"$k $op $a"
    def eval(v: Option[Double], rows: Int): Option[Boolean] =
      v.map(x => compares(k.toDouble, op, x))
  }

  private val maxA = Aggregate("MAX(a)")
  private val minB = Aggregate("MIN(b)")
  private val maxB = Aggregate("MAX(b)")

  /** The review's four statements, over the review's fixture. */
  private val reviewForms: Seq[(CoalesceOperand, NullCond)] = Seq(
    CoalesceOf(maxA, minB)       -> Cmp(">", 1),
    CoalesceOf(maxA, maxB)       -> Cmp(">", 1),
    CoalesceOf(maxA, Literal(5)) -> Cmp(">", 1),
    CoalesceOf(maxA, minB)       -> Junction(RowsAbove(0), and = true, notRight = true, Cmp(">", 1))
  )

  private val coalesceOperands: Seq[CoalesceOperand] = Seq(
    CoalesceOf(maxA, minB),
    CoalesceOf(maxA, maxB),
    // never NULL
    CoalesceOf(maxA, Literal(5)),
    CoalesceOf(minB, maxA),
    CoalesceOf(maxA, minB, maxB),
    CoalesceOf(maxA, minB, Literal(5)),
    // SUM is never NULL either (#336)
    CoalesceOf(Aggregate("SUM(a)"), minB),
    CoalesceOf(CoalesceOf(maxA, minB), Literal(5)),
    CoalesceOf(maxB, CoalesceOf(maxA, minB)),
    SignOf(CoalesceOf(maxA, minB))
  )

  private val coalesceForms: Seq[NullCond] = Seq(
    Cmp(">", 1),
    Cmp("<", 1),
    Cmp("=", 5),
    Cmp("<>", 5),
    NotCmp(Cmp(">", 1)),
    NotCmp(Cmp("=", 0)),
    Between(1, 5, not = false),
    Between(1, 5, not = true),
    In(Seq(0, 5), not = false),
    In(Seq(0, 5), not = true),
    CountVs(">"),
    CountVs("<="),
    LiteralVs(1, "<"),
    NullTest(isNull = true),
    NullTest(isNull = false),
    Junction(RowsAbove(0), and = true, notRight = true, Cmp(">", 1)),
    Junction(RowsAbove(0), and = true, notRight = false, Cmp(">", 1)),
    Junction(Cmp(">", 1), and = false, notRight = false, RowsAbove(1)),
    Junction(RowsAbove(0), and = true, notRight = true, NullTest(isNull = true))
  )

  private lazy val coalescePopulation: Seq[NullStatement] = {
    val oracle: Seq[(String, CoalesceGroup)] = coalesceGroups.map { case (g, rows) =>
      g -> CoalesceGroup(rows.size, rows.flatMap(_._1), rows.flatMap(_._2))
    }
    def keep(operand: CoalesceOperand, c: NullCond): Set[String] =
      oracle.collect {
        case (g, group) if c.eval(operand.value(group), group.rows).contains(true) => g
      }.toSet
    val grouped = for {
      operand <- coalesceOperands
      c       <- coalesceForms
      // `ISNULL(SIGN(...))` fails to compile in Elasticsearch on main already (`Cannot cast from
      // [int] to [java.lang.Object]`): the null test of a function rendering a primitive.
      if !(operand.isInstanceOf[SignOf] && readsNullTest(c))
      select <- Seq("g, COUNT(*) AS c", "g, MAX(a) AS ma, MIN(b) AS mb, MAX(b) AS xb, SUM(a) AS sa")
    } yield NullStatement(
      s"SELECT $select FROM $coalesceIndex GROUP BY g HAVING ${c.sql(operand.sql)}",
      keep(operand, c)
    )
    // The implicit whole-table group, one group at a time.
    val wholeTable = for {
      (operand, c) <- reviewForms
      (g, group)   <- oracle
    } yield NullStatement(
      s"SELECT COUNT(*) AS c FROM $coalesceIndex WHERE g = '$g' HAVING ${c.sql(operand.sql)}",
      if (c.eval(operand.value(group), group.rows).contains(true)) Set(g) else Set.empty,
      wholeTable = Some(g)
    )
    grouped ++ wholeTable
  }

  "a HAVING over a COALESCE of aggregates" should
  "keep exactly the groups SQL's three-valued logic keeps" in {
    coalesceGroupsLoaded
    val population = coalescePopulation
    // Non-vacuity, computed over the material: the review's four statements are in it, and the
    // oracle keeps and drops each group whose `a` or `b` has no value.
    reviewForms.foreach { case (operand, c) =>
      val sql =
        s"SELECT g, COUNT(*) AS c FROM $coalesceIndex GROUP BY g HAVING ${c.sql(operand.sql)}"
      withClue(s"[$sql] ")(population.exists(_.sql == sql) shouldBe true)
    }
    Seq("g1", "g2", "g4", "g5", "g6").foreach { g =>
      withClue(s"[$g] ") {
        population.exists(st => st.wholeTable.isEmpty && st.expected.contains(g)) shouldBe true
        population.exists(st => st.wholeTable.isEmpty && !st.expected.contains(g)) shouldBe true
      }
    }
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))

    val outcomes = population.map(st => st -> keptGroups(st))
    val wrong = outcomes.collect {
      case (st, Right(actual)) if actual != st.expected => (st, actual)
    }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    val wrongGroups = wrong.map { case (st, actual) =>
      ((st.expected -- actual) ++ (actual -- st.expected)).size
    }.sum
    info(
      s"HAVING over a COALESCE of aggregates: ${population.size} statements; wrong: " +
      s"${wrong.size} statements / $wrongGroups groups; errors: ${errors.size}"
    )
    val report = wrong.map { case (st, actual) =>
      s"[${st.sql}] kept ${actual.toSeq.sorted.mkString(",")}, expected ${st.expected.toSeq.sorted.mkString(",")}"
    }
    report.foreach(info(_))
    errors.foreach(info(_))
    withClue(
      s"${wrong.size} wrong statements ($wrongGroups wrong groups), ${errors.size} errors:\n" +
      (report.take(20) ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The spellings a HAVING used to refuse and now answers: a SELECT alias inside a function, read
  // as the aggregate it names at every depth (`COALESCE(ma, mb) > 1` is `COALESCE(MAX(a), MIN(b))
  // > 1`), and ISNULL / ISNOTNULL over a QUALIFIED aggregate, whose operand is now the aggregate
  // every other position builds -- it was refused as "a different aggregate" than its own SELECT
  // item.
  //
  // The population is DERIVED from the COALESCE one above: every operand of it in its alias
  // spelling, under every form, plus the same operands on the VALUE side of a published aggregate,
  // over the same fixture and the same three-valued oracle -- and, under the same forms, the alias
  // of a SELECT bucket_script item (`MAX(a) - MIN(b) AS d`) under SIGN, NULL whenever an operand
  // is. Every statement runs through the gateway; every one of them was refused before.
  // ---------------------------------------------------------------------------------------------

  private val coalesceSelect = "g, MAX(a) AS ma, MIN(b) AS mb, MAX(b) AS xb, SUM(a) AS sa"

  /** An operand in its SELECT-alias spelling. */
  private def aliasSql(operand: CoalesceOperand): String = operand match {
    case Aggregate(sql) =>
      Map("MAX(a)" -> "ma", "MIN(b)" -> "mb", "MAX(b)" -> "xb", "SUM(a)" -> "sa")
        .getOrElse(sql, fail(s"no SELECT alias for $sql"))
    case Literal(k)          => k.toString
    case CoalesceOf(as @ _*) => as.map(aliasSql).mkString("COALESCE(", ", ", ")")
    case SignOf(argument)    => s"SIGN(${aliasSql(argument)})"
    case GreatestOf(as @ _*) => as.map(aliasSql).mkString("GREATEST(", ", ", ")")
    case LeastOf(as @ _*)    => as.map(aliasSql).mkString("LEAST(", ", ", ")")
  }

  private lazy val respelledPopulation: Seq[NullStatement] = {
    val oracle: Seq[(String, CoalesceGroup)] = coalesceGroups.map { case (g, rows) =>
      g -> CoalesceGroup(rows.size, rows.flatMap(_._1), rows.flatMap(_._2))
    }
    def keep(eval: CoalesceGroup => Option[Boolean]): Set[String] =
      oracle.collect { case (g, group) if eval(group).contains(true) => g }.toSet
    // the alias spelling of every COALESCE-family statement above
    val aliasInFunction = for {
      operand <- coalesceOperands
      c       <- coalesceForms
      if !(operand.isInstanceOf[SignOf] && readsNullTest(c))
    } yield NullStatement(
      s"SELECT $coalesceSelect FROM $coalesceIndex GROUP BY g HAVING ${c.sql(aliasSql(operand))}",
      keep(group => c.eval(operand.value(group), group.rows))
    )
    // the same operands on the VALUE side of a published aggregate, alias spelling on both sides
    val valueSide = for {
      operand <- coalesceOperands ++ Seq(CoalesceOf(minB, Literal(1)), SignOf(minB))
      op      <- Seq(">", "<=")
    } yield NullStatement(
      s"SELECT $coalesceSelect FROM $coalesceIndex GROUP BY g HAVING ma $op ${aliasSql(operand)}",
      keep { group =>
        for {
          left  <- maxA.value(group)
          right <- operand.value(group)
        } yield compares(left, op, right)
      }
    )
    // the alias of a SELECT bucket_script item under SIGN, NULL whenever an operand is: the filter
    // reads the operands and declares them
    val bucketScriptInFunction = for {
      c <- coalesceForms
      // the null test of SIGN, as above
      if !readsNullTest(c)
    } yield {
      val having = c.sql("SIGN(d)")
      NullStatement(
        s"SELECT g, MAX(a) AS ma, MIN(b) AS mb, MAX(a) - MIN(b) AS d FROM $coalesceIndex GROUP BY g HAVING $having",
        keep { group =>
          c.eval(
            for {
              x <- maxA.value(group)
              y <- minB.value(group)
            } yield math.signum(x - y),
            group.rows
          )
        }
      )
    }
    // ISNULL / ISNOTNULL over a qualified aggregate
    val qualifiedNullTests = for {
      aggregate <- Seq(maxA, minB, maxB)
      isNull    <- Seq(true, false)
    } yield {
      val qualified = aggregate.sql.replace("(", "(r.")
      val test = if (isNull) s"ISNULL($qualified)" else s"ISNOTNULL($qualified)"
      NullStatement(
        s"SELECT r.g, $qualified AS m FROM $coalesceIndex AS r GROUP BY r.g HAVING $test",
        keep(group => Some(aggregate.value(group).isEmpty == isNull))
      )
    }
    aliasInFunction ++ valueSide ++ bucketScriptInFunction ++ qualifiedNullTests
  }

  "a HAVING spelling that used to be refused" should
  "keep exactly the groups SQL's three-valued logic keeps" in {
    coalesceGroupsLoaded
    val population = respelledPopulation
    // Non-vacuity: every statement parses now, and the oracle keeps and drops a group whose `a`
    // or `b` has no value.
    population.size should be >= 200
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))
    Seq("g1", "g2", "g4").foreach { g =>
      withClue(s"[$g] ") {
        population.exists(_.expected.contains(g)) shouldBe true
        population.exists(st => !st.expected.contains(g)) shouldBe true
      }
    }

    val outcomes = population.map(st => st -> keptGroups(st))
    val wrong = outcomes.collect {
      case (st, Right(actual)) if actual != st.expected => (st, actual)
    }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    val wrongGroups = wrong.map { case (st, actual) =>
      ((st.expected -- actual) ++ (actual -- st.expected)).size
    }.sum
    info(
      s"HAVING spellings that used to be refused: ${population.size} statements; wrong: " +
      s"${wrong.size} statements / $wrongGroups groups; errors: ${errors.size}"
    )
    val report = wrong.map { case (st, actual) =>
      s"[${st.sql}] kept ${actual.toSeq.sorted.mkString(",")}, expected ${st.expected.toSeq.sorted.mkString(",")}"
    }
    report.foreach(info(_))
    errors.foreach(info(_))
    withClue(
      s"${wrong.size} wrong statements ($wrongGroups wrong groups), ${errors.size} errors:\n" +
      (report.take(20) ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------------------------
  // GREATEST / LEAST over aggregates in HAVING skip a NULL argument -- what the docs state and
  // WHERE does: the result is NULL only when every argument is. The group filter emitted
  // `Math.max(params.m, k)` with no NULL skip, behind a guard on EVERY argument, so a group was
  // dropped as soon as ONE argument was NULL: MEASURED on Elasticsearch 8.18.3, 5 of the 6 shapes
  // below kept the wrong groups (`LEAST(MIN(b), 1) < 3` dropped every group with no `b`).
  //
  // The population is DERIVED from the COALESCE one above -- the same fixture, the same forms and
  // the same three-valued oracle, NULL arguments skipped -- over several aggregates and an
  // aggregate with a literal, in the bare, alias and qualified spellings, on the value side of an
  // aggregate, and in the implicit whole-table group. Every statement runs through the gateway.
  // ---------------------------------------------------------------------------------------------

  private final case class GreatestOf(arguments: CoalesceOperand*) extends CoalesceOperand {
    def sql: String = arguments.map(_.sql).mkString("GREATEST(", ", ", ")")
    def value(group: CoalesceGroup): Option[Double] =
      arguments.flatMap(_.value(group)).reduceOption(_ max _)
  }

  private final case class LeastOf(arguments: CoalesceOperand*) extends CoalesceOperand {
    def sql: String = arguments.map(_.sql).mkString("LEAST(", ", ", ")")
    def value(group: CoalesceGroup): Option[Double] =
      arguments.flatMap(_.value(group)).reduceOption(_ min _)
  }

  private val reducerOperands: Seq[CoalesceOperand] = Seq(
    GreatestOf(maxA, minB),
    LeastOf(maxA, minB),
    GreatestOf(minB, Literal(1)),
    LeastOf(minB, Literal(1)),
    GreatestOf(maxA, minB, maxB),
    LeastOf(Literal(5), maxA, minB),
    // SUM is never NULL (#336)
    GreatestOf(Aggregate("SUM(a)"), minB),
    LeastOf(GreatestOf(maxA, minB), maxB),
    CoalesceOf(GreatestOf(maxA, minB), Literal(5)),
    GreatestOf(CoalesceOf(maxA, Literal(0)), minB),
    SignOf(LeastOf(maxA, minB))
  )

  /** The measured shapes with the reducer on the LEFT, in the form they were measured in; the two
    * on the value side (`MAX(a) > GREATEST(MIN(b), 1)`, `... LEAST(...)`) are in `valueSide`.
    */
  private val reducerMeasured: Seq[(CoalesceOperand, NullCond)] = Seq(
    GreatestOf(maxA, minB)       -> Cmp(">", 1),
    GreatestOf(minB, Literal(1)) -> Cmp(">", 1),
    LeastOf(maxA, minB)          -> Cmp("<", 3),
    LeastOf(minB, Literal(1))    -> Cmp("<", 3)
  )

  private val reducerForms: Seq[NullCond] = coalesceForms :+ Cmp("<", 3)

  /** A GREATEST / LEAST with a literal argument: never NULL, and its rendering is a primitive. */
  private def primitiveReducer(operand: CoalesceOperand): Boolean = operand match {
    case GreatestOf(as @ _*) => as.exists(_.isInstanceOf[Literal])
    case LeastOf(as @ _*)    => as.exists(_.isInstanceOf[Literal])
    case _                   => false
  }

  private lazy val reducerPopulation: Seq[NullStatement] = {
    val oracle: Seq[(String, CoalesceGroup)] = coalesceGroups.map { case (g, rows) =>
      g -> CoalesceGroup(rows.size, rows.flatMap(_._1), rows.flatMap(_._2))
    }
    def keep(eval: CoalesceGroup => Option[Boolean]): Set[String] =
      oracle.collect { case (g, group) if eval(group).contains(true) => g }.toSet
    def qualified(operand: CoalesceOperand): String =
      operand.sql.replace("(a)", "(r.a)").replace("(b)", "(r.b)")
    val qualifiedSelect = "r.g, MAX(r.a) AS ma, MIN(r.b) AS mb, MAX(r.b) AS xb, SUM(r.a) AS sa"
    val grouped = for {
      operand <- reducerOperands
      c       <- reducerForms
      // As for COALESCE: the null test of a function rendering a primitive fails to compile in
      // Elasticsearch on main already (`Cannot cast from [double] to [java.lang.Object]`) -- SIGN,
      // and a GREATEST / LEAST with a literal argument, which is never NULL.
      if !((operand.isInstanceOf[SignOf] || primitiveReducer(operand)) && readsNullTest(c))
      (select, from, key, spelled) <- Seq(
        ("g, COUNT(*) AS c", coalesceIndex, "g", operand.sql),
        (coalesceSelect, coalesceIndex, "g", operand.sql),
        (coalesceSelect, coalesceIndex, "g", aliasSql(operand)),
        (qualifiedSelect, s"$coalesceIndex AS r", "r.g", qualified(operand))
      )
    } yield NullStatement(
      s"SELECT $select FROM $from GROUP BY $key HAVING ${c.sql(spelled)}",
      keep(group => c.eval(operand.value(group), group.rows))
    )
    // on the VALUE side of an aggregate, bare and alias spellings
    val valueSide = for {
      operand         <- reducerOperands
      op              <- Seq(">", "<=")
      (left, spelled) <- Seq(("MAX(a)", operand.sql), ("ma", aliasSql(operand)))
    } yield NullStatement(
      s"SELECT $coalesceSelect FROM $coalesceIndex GROUP BY g HAVING $left $op $spelled",
      keep { group =>
        for {
          l <- maxA.value(group)
          r <- operand.value(group)
        } yield compares(l, op, r)
      }
    )
    // The implicit whole-table group, one group at a time.
    val wholeTable = for {
      (operand, c) <- reducerMeasured
      (g, group)   <- oracle
    } yield NullStatement(
      s"SELECT COUNT(*) AS c FROM $coalesceIndex WHERE g = '$g' HAVING ${c.sql(operand.sql)}",
      if (c.eval(operand.value(group), group.rows).contains(true)) Set(g) else Set.empty,
      wholeTable = Some(g)
    )
    grouped ++ valueSide ++ wholeTable
  }

  "a HAVING over GREATEST / LEAST of aggregates" should
  "keep exactly the groups SQL's three-valued logic keeps, NULL arguments skipped" in {
    coalesceGroupsLoaded
    val population = reducerPopulation
    // Non-vacuity, computed over the material: the measured shapes are in it, in both spellings,
    // and the oracle keeps and drops each group whose `a` or `b` has no value.
    reducerMeasured.foreach { case (operand, c) =>
      Seq(
        s"SELECT g, COUNT(*) AS c FROM $coalesceIndex GROUP BY g HAVING ${c.sql(operand.sql)}",
        s"SELECT $coalesceSelect FROM $coalesceIndex GROUP BY g HAVING ${c.sql(aliasSql(operand))}"
      ).foreach(sql => withClue(s"[$sql] ")(population.exists(_.sql == sql) shouldBe true))
    }
    Seq("g1", "g2", "g4", "g5", "g6").foreach { g =>
      withClue(s"[$g] ") {
        population.exists(st => st.wholeTable.isEmpty && st.expected.contains(g)) shouldBe true
        population.exists(st => st.wholeTable.isEmpty && !st.expected.contains(g)) shouldBe true
      }
    }
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))

    val outcomes = population.map(st => st -> keptGroups(st))
    val wrong = outcomes.collect {
      case (st, Right(actual)) if actual != st.expected => (st, actual)
    }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    val wrongGroups = wrong.map { case (st, actual) =>
      ((st.expected -- actual) ++ (actual -- st.expected)).size
    }.sum
    info(
      s"HAVING over GREATEST / LEAST of aggregates: ${population.size} statements; wrong: " +
      s"${wrong.size} statements / $wrongGroups groups; errors: ${errors.size}"
    )
    val report = wrong.map { case (st, actual) =>
      s"[${st.sql}] kept ${actual.toSeq.sorted.mkString(",")}, expected ${st.expected.toSeq.sorted.mkString(",")}"
    }
    report.foreach(info(_))
    errors.foreach(info(_))
    withClue(
      s"${wrong.size} wrong statements ($wrongGroups wrong groups), ${errors.size} errors:\n" +
      (report.take(20) ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------------------------
  // DATEDIFF / DATE_DIFF / TIMESTAMPDIFF over aggregates, computed per group. The calculation is a
  // bucket_script, which reads a date aggregate as the epoch millis Elasticsearch computed: it failed
  // the search for every statement below -- `java.lang.Double cannot be cast to ... Temporal` over
  // two aggregates, `Cannot cast from [java.lang.String] to [java.time.temporal.Temporal]` over an
  // aggregate and a literal.
  //
  // The population is every spelling of the family (MySQL's DATEDIFF with and without a unit,
  // DATEDIFF / DATE_DIFF / TIMESTAMPDIFF unit-first, DATE_DIFF date-first), over an aggregate and a
  // literal and over two aggregates, bare and table-qualified: as a SELECT item, whose value is
  // checked in every group, and through its SELECT alias in HAVING -- the route the "alias it in
  // SELECT" remedy of an inline DATEDIFF leads to. The fixture holds a group with no date, a group
  // with some and groups with all of them; the expected values are computed HERE, NULL when an
  // operand has no value.
  // ---------------------------------------------------------------------------------------------

  private val dateDiffIndex = "having_datediff_groups"

  /** Each group and the `d` of each of its documents (`None`: the document lacks it). */
  private val dateDiffGroups: Seq[(String, Seq[Option[String]])] = Seq(
    "s1" -> Seq(None, None),
    "s2" -> Seq(Some("2023-12-01")),
    "s3" -> Seq(Some("2024-03-01"), Some("2024-01-01")),
    "s4" -> Seq(Some("2023-06-01"), None, Some("2024-02-15")),
    "s5" -> Seq(Some("2024-01-02")),
    "s6" -> Seq(Some("2024-01-01"), Some("2024-01-02"))
  )

  private lazy val dateDiffGroupsLoaded: Unit = {
    client
      .createIndex(dateDiffIndex, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client
      .setMapping(
        dateDiffIndex,
        """{"properties": {"id": {"type": "keyword"}, "sensor_id": {"type": "keyword"}, "d": {"type": "date"}}}"""
      )
      .get shouldBe true
    val docs = for {
      (s, ds)     <- dateDiffGroups.toList
      (d, offset) <- ds.zipWithIndex
    } yield (Seq(s""""id":"$s-$offset"""", s""""sensor_id":"$s"""") ++ d.map(x => s""""d":"$x""""))
      .mkString("{", ",", "}")
    implicit val bulkOptions: BulkOptions =
      BulkOptions(defaultIndex = dateDiffIndex, logEvery = 100)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(dateDiffIndex)
      case ElasticFailure(error) =>
        fail(s"Bulk indexing into $dateDiffIndex failed: ${error.message}")
    }
  }

  /** A spelling of the family over two operands, and the days it answers for them: `end - start`,
    * where the dates first is `first - second` and the unit first `last - middle`. Between two
    * calendar dates, the days crossed and the whole days elapsed are the same number.
    */
  private final case class DateDiffForm(
    sql: (String, String) => String,
    days: (LocalDate, LocalDate) => Long
  )

  private val dateDiffForms: Seq[DateDiffForm] = {
    def between(start: LocalDate, end: LocalDate): Long = ChronoUnit.DAYS.between(start, end)
    Seq(
      DateDiffForm((a, b) => s"DATEDIFF($a, $b)", (a, b) => between(b, a)),
      DateDiffForm((a, b) => s"DATEDIFF($a, $b, DAY)", (a, b) => between(b, a)),
      DateDiffForm((a, b) => s"DATEDIFF(DAY, $a, $b)", between),
      DateDiffForm((a, b) => s"DATE_DIFF($a, $b, DAY)", (a, b) => between(b, a)),
      DateDiffForm((a, b) => s"DATE_DIFF(DAY, $a, $b)", between),
      DateDiffForm((a, b) => s"TIMESTAMPDIFF(DAY, $a, $b)", between)
    )
  }

  /** A statement of the population: its value of `x_inline` in each group (`None`: NULL) for a
    * SELECT, or the groups it keeps for a HAVING.
    */
  private final case class DateDiffStatement(
    sql: String,
    values: Option[Map[String, Option[Long]]],
    kept: Option[Set[String]]
  )

  private lazy val dateDiffPopulation: Seq[DateDiffStatement] = {
    val literal = LocalDate.parse("2024-01-01")
    val groups: Seq[(String, Seq[LocalDate])] = dateDiffGroups.map { case (s, ds) =>
      s -> ds.flatten.map(x => LocalDate.parse(x))
    }
    def maxOf(ds: Seq[LocalDate]) = ds.reduceOption((x, y) => if (x.isAfter(y)) x else y)
    def minOf(ds: Seq[LocalDate]) = ds.reduceOption((x, y) => if (x.isBefore(y)) x else y)
    for {
      spelling <- dateDiffForms
      (second, readsMin, secondValue) <- Seq(
        ("'2024-01-01'", false, (_: Seq[LocalDate]) => Option(literal)),
        ("MIN(d)", true, minOf _)
      )
      qualified <- Seq(false, true)
      (form, filter) <- Seq[(String, Option[Long => Boolean])](
        ("", None),
        (" HAVING x_inline > 1", Some((v: Long) => v > 1)),
        (" HAVING x_inline < -1", Some((v: Long) => v < -1))
      )
    } yield {
      val q = if (qualified) "r." else ""
      val items = s"MAX(${q}d) AS max_d" + (if (readsMin) s", MIN(${q}d) AS min_d" else "")
      val expr = spelling.sql(s"MAX(${q}d)", second.replace("(d)", s"(${q}d)"))
      val from = if (qualified) s"$dateDiffIndex AS r" else dateDiffIndex
      val sql =
        s"SELECT ${q}sensor_id, $items, $expr AS x_inline FROM $from GROUP BY ${q}sensor_id$form"
      val values = groups.map { case (s, ds) =>
        s -> (for {
          first <- maxOf(ds)
          other <- secondValue(ds)
        } yield spelling.days(first, other))
      }.toMap
      filter match {
        case None => DateDiffStatement(sql, Some(values), None)
        case Some(f) =>
          DateDiffStatement(
            sql,
            None,
            Some(values.collect { case (s, Some(v)) if f(v) => s }.toSet)
          )
      }
    }
  }

  "DATEDIFF / DATE_DIFF / TIMESTAMPDIFF over aggregates" should
  "answer per group, NULL where an operand has no value" in {
    dateDiffGroupsLoaded
    val population = dateDiffPopulation
    // Non-vacuity, computed over the material: every spelling in both venues, the group with no
    // date is NULL in every SELECT and kept by no HAVING, and the HAVINGs keep and drop groups.
    population.size shouldBe dateDiffForms.size * 2 * 2 * 3
    population.foreach(st => withClue(s"[${st.sql}] ")(Parser(st.sql).isRight shouldBe true))
    population.flatMap(_.values).foreach(values => values("s1") shouldBe None)
    population.flatMap(_.kept).foreach(kept => kept should not contain "s1")
    population.flatMap(_.kept).exists(_.nonEmpty) shouldBe true
    population.flatMap(_.kept).exists(_.size < dateDiffGroups.size - 1) shouldBe true

    def number(v: Any): Option[Double] = Option(v).map(_.toString.toDouble)
    val outcomes: Seq[(DateDiffStatement, Either[String, String])] = population.map { st =>
      st -> gatewayRows(st.sql).map { rows =>
        (st.values, st.kept) match {
          case (Some(expected), _) =>
            val actual = rows
              .map(r => r.getOrElse("sensor_id", "?").toString -> r.get("x_inline").flatMap(number))
              .toMap
            val wanted = expected.map { case (s, v) => s -> v.map(_.toDouble) }
            if (actual == wanted) ""
            else s"answered ${actual.toSeq.sortBy(_._1)}, expected ${wanted.toSeq.sortBy(_._1)}"
          case (_, Some(expected)) =>
            val actual = rows.map(_.getOrElse("sensor_id", "?").toString).toSet
            if (actual == expected) ""
            else
              s"kept ${actual.toSeq.sorted.mkString(",")}, expected ${expected.toSeq.sorted.mkString(",")}"
          case _ => "no expectation"
        }
      }
    }
    val wrong = outcomes.collect { case (st, Right(diff)) if diff.nonEmpty => s"[${st.sql}] $diff" }
    val errors = outcomes.collect { case (st, Left(error)) => s"[${st.sql}] $error" }
    info(
      s"DATEDIFF family over aggregates: ${population.size} statements; wrong: ${wrong.size}; " +
      s"errors: ${errors.size}"
    )
    wrong.foreach(info(_))
    errors.foreach(info(_))
    withClue(
      s"${wrong.size} wrong statements, ${errors.size} errors:\n" +
      (wrong.take(20) ++ errors.take(10)).mkString("\n") + "\n"
    ) {
      wrong shouldBe empty
      errors shouldBe empty
    }
  }

  // ---------------------------------------------------------------------------------------------
  // F2 -- a MATCH in HAVING over an aggregate, or over a column that is neither an aggregate nor a
  // GROUP BY key, is refused by name. Before, it was DROPPED: the group filter has no MATCH, the
  // key filter reads only the key, so every group came back.
  // ---------------------------------------------------------------------------------------------

  "a MATCH in HAVING over an aggregate or a non-key column" should
  "be refused by name through the gateway" in {
    nullGroupsLoaded
    Seq(
      s"SELECT g, COUNT(*) AS c FROM $nullIndex GROUP BY g HAVING MATCH (MAX(g)) AGAINST ('zzz')" ->
      "HAVING cannot apply MATCH to the aggregate MAX(g)",
      s"SELECT g, COUNT(*) AS c FROM $nullIndex GROUP BY g HAVING COUNT(*) > 0 AND MATCH (MAX(g)) AGAINST ('zzz')" ->
      "HAVING cannot apply MATCH to the aggregate MAX(g)",
      s"SELECT g, COUNT(*) AS c FROM $nullIndex GROUP BY g HAVING MATCH (id) AGAINST ('zzz')" ->
      "HAVING cannot apply MATCH to the column id",
      s"SELECT g, COUNT(*) AS c FROM $nullIndex GROUP BY g HAVING COUNT(*) > 0 OR MATCH (id) AGAINST ('zzz')" ->
      "HAVING cannot apply MATCH to the column id"
    ).foreach { case (sql, refusal) =>
      withClue(s"[$sql] ") {
        gatewayRows(sql) match {
          case Left(error) =>
            error should include(refusal)
            error should include("Put the MATCH in WHERE.")
          case Right(rows) => fail(s"answered ${rows.size} rows instead of refusing")
        }
      }
    }
  }

  // =============================================================================================
  // A ONE-value `terms` include / exclude is an EXACT value, on every major.
  //
  // Elasticsearch 6.8 used to read a one-value key filter as a REGEX: the es6 bridge's elastic4s
  // 6.7.8 writes a one-entry list as a bare JSON string and 6.8 compiles a string `include` as a
  // Lucene RegExp. The fixture above holds only `cat_NN` keys and one integer, so every earlier row
  // passed on 6.8 by construction. This index is built so the REGEX reading and the EXACT reading
  // give DIFFERENT groups for every value under test (`a.b` beside `axb`, `a*` beside `a`, `aa`
  // and the empty key, ...). The oracle is the SQL meaning computed from the documents below --
  // never the engine -- and the same rows run on every major. The es6 bridge's
  // `TermsIncludeExcludeSpec` sweeps the same alphabet through Elasticsearch 6.8.23's own code.
  // =============================================================================================

  private val keyIndex = "group_by_key_filter"

  private val valuesUnderTest: Seq[String] =
    Seq("a.b", "a*", "a+b", "(x)", "[a]", "a|b", "a@b", "a\"b", "a\\b", "it's", "^a$", "é", "")

  /** Keys the REGEX reading of some value above also keeps -- they make the readings disagree. */
  private val decoys: Seq[String] = Seq("axb", "a", "aa", "ab", "aab", "x", "b", "its")

  private val stringKeys: Seq[String] = valuesUnderTest ++ decoys

  private final case class KeyDoc(id: String, fields: Seq[(String, AnyRef)])

  /** `sval` key #i holds i + 1 documents; the numeric and boolean keys carry their own counts; four
    * documents carry a keyword AND a long key, for the two-level row.
    */
  private lazy val keyDocs: Seq[KeyDoc] =
    stringKeys.zipWithIndex.flatMap { case (k, i) =>
      (0 to i).map(j => KeyDoc(s"s$i-$j", Seq("sval" -> k)))
    } ++
    Seq(1L -> 1, 2L -> 2, 10L -> 3, 21L -> 4).flatMap { case (v, n) =>
      (1 to n).map(j => KeyDoc(s"l$v-$j", Seq("lval" -> java.lang.Long.valueOf(v))))
    } ++
    Seq(1.5 -> 1, 2.5 -> 2, 10.5 -> 3).flatMap { case (v, n) =>
      (1 to n).map(j => KeyDoc(s"d$v-$j", Seq("dval" -> java.lang.Double.valueOf(v))))
    } ++
    Seq(true -> 2, false -> 3).flatMap { case (v, n) =>
      (1 to n).map(j => KeyDoc(s"b$v-$j", Seq("bval" -> java.lang.Boolean.valueOf(v))))
    } ++
    Seq("a.b" -> 1L, "a.b" -> 1L, "axb" -> 1L, "a.b" -> 2L).zipWithIndex.map { case ((s, l), j) =>
      KeyDoc(s"p$j", Seq("sval" -> s, "lval" -> java.lang.Long.valueOf(l)))
    }

  private val keyJson = new ObjectMapper()

  private def jsonOf(doc: KeyDoc): String = {
    val fields = new java.util.LinkedHashMap[String, AnyRef]()
    fields.put("id", doc.id)
    doc.fields.foreach { case (k, v) => fields.put(k, v) }
    keyJson.writeValueAsString(fields) // escapes `"` and `\` in the keys -- never interpolate them
  }

  private lazy val keyFilterLoaded: Unit = {
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":   { "type": "keyword" },
        |    "sval": { "type": "keyword" },
        |    "lval": { "type": "long" },
        |    "dval": { "type": "double" },
        |    "bval": { "type": "boolean" }
        |  }
        |}""".stripMargin
    client.createIndex(keyIndex, settings = settings).get shouldBe true
    client.setMapping(keyIndex, mapping).get shouldBe true
    implicit val keyBulkOptions: BulkOptions = BulkOptions(defaultIndex = keyIndex, logEvery = 1000)
    implicit def keyListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](keyDocs.map(jsonOf).toList, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(keyIndex)
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $keyIndex failed: ${error.message}")
    }
  }

  /** An UNNEST grouping key: `items` is nested, and its `qty` values group as 1, 2, 10 and 21. */
  private val nestedKeyIndex = "group_by_nested_key_filter"

  private lazy val nestedKeyFilterLoaded: Unit = {
    val settings = """{"number_of_shards": 2, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "category": { "type": "keyword" },
        |    "items":    { "type": "nested", "properties": { "qty": { "type": "long" } } }
        |  }
        |}""".stripMargin
    client.createIndex(nestedKeyIndex, settings = settings).get shouldBe true
    client.setMapping(nestedKeyIndex, mapping).get shouldBe true
    implicit val nestedBulkOptions: BulkOptions =
      BulkOptions(defaultIndex = nestedKeyIndex, logEvery = 1000)
    implicit def nestedListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    val docs = Seq(
      ("n1", "a", Seq(1L, 2L)),
      ("n2", "b", Seq(2L, 10L)),
      ("n3", "a", Seq(21L)),
      ("n4", "c", Seq(1L))
    ).map { case (id, category, items) =>
      val quantities = items.map(q => s"""{"qty": $q}""").mkString(", ")
      s"""{"id": "$id", "category": "$category", "items": [$quantities]}"""
    }.toList
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(nestedKeyIndex)
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $nestedKeyIndex failed: ${error.message}")
    }
  }

  private def unnestedKey(predicate: String): String =
    s"SELECT inner_items.qty AS q, MIN(inner_items.qty) AS m FROM $nestedKeyIndex tt " +
    s"JOIN UNNEST(tt.items) AS inner_items GROUP BY inner_items.qty HAVING $predicate"

  private lazy val esMajor: Int =
    client.version match {
      case ElasticSuccess(v) => v.takeWhile(_ != '.').toInt
      case ElasticFailure(error) =>
        fail(s"Failed to retrieve the Elasticsearch version: ${error.message}")
    }

  /** A number as ONE spelling, whatever type a client materialises it as (`1`, `1L`, `1.0`). */
  private def numericText(v: Any): String =
    BigDecimal(v.toString).bigDecimal.stripTrailingZeros.toPlainString

  /** The numeric key columns, `l` being `lval` under its SELECT alias. */
  private val numericKeys: Set[String] = Set("lval", "dval", "l")

  /** A key as the oracle and the client answer are compared: numeric keys by value. */
  private def keyText(field: String, v: Any): String =
    if (numericKeys(field)) numericText(v) else v.toString

  /** The SQL meaning, from the documents: kept key -> its document count. */
  private def expectedCounts(field: String, keep: String => Boolean): Map[String, Long] = {
    val column = if (field == "l") "lval" else field
    keyDocs
      .flatMap(_.fields.collect { case (`column`, v) => keyText(field, v) })
      .filter(keep)
      .groupBy(identity)
      .map { case (k, vs) => k -> vs.size.toLong }
  }

  /** Key -> count as the client answers; `Left` carries a failure's message. */
  private def countsBy(sql: String, field: String): Either[String, Map[String, Long]] = {
    keyFilterLoaded
    implicit val keyCtx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        Right(response.results.map { row =>
          row
            .get(field)
            .map(v => Option(v).map(keyText(field, _)).getOrElse("<null>"))
            .getOrElse("<absent>") ->
          row.get("c").map(_.toString.toDouble.toLong).getOrElse(-1L)
        }.toMap)
      case ElasticFailure(error) => Left(error.message)
    }
  }

  /** The group KEYS the client answers -- and only them: these rows are about which groups a key
    * condition keeps; the metrics of an UNNEST grouping have rows of their own below.
    */
  private def keysBy(sql: String, field: String): Either[String, Set[String]] = {
    nestedKeyFilterLoaded
    implicit val keyCtx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        Right(
          response.results
            .map(_.get(field).map(v => Option(v).map(numericText).getOrElse("<null>")))
            .map(_.getOrElse("<absent>"))
            .toSet
        )
      case ElasticFailure(error) => Left(error.message)
    }
  }

  private def sqlString(v: String): String = "'" + v.replace("'", "''") + "'"

  /** `field <op> literal` for the four one-value operators, and whether each KEEPS the value. */
  private val oneValueOps: Seq[(String, Boolean, (String, String) => String)] = Seq(
    ("=", true, (f, l) => s"$f = $l"),
    ("IN", true, (f, l) => s"$f IN ($l)"),
    ("<>", false, (f, l) => s"$f <> $l"),
    ("NOT IN", false, (f, l) => s"$f NOT IN ($l)")
  )

  "a one-value filter on a keyword GROUP BY key" should
  "keep or remove EXACTLY that value -- never its regex reading" in {
    val cells = for {
      v                        <- valuesUnderTest
      (op, include, predicate) <- oneValueOps
      // `= ''` / `<> ''` never reach the key filter: refused before emission on every major
      if !(v.isEmpty && (op == "=" || op == "<>"))
    } yield (v, include, predicate("sval", sqlString(v)))
    cells should have size (valuesUnderTest.size * oneValueOps.size - 2).toLong
    val wrong = cells.flatMap { case (v, include, predicate) =>
      val sql = s"SELECT sval, COUNT(*) AS c FROM $keyIndex GROUP BY sval HAVING $predicate"
      val expected = expectedCounts("sval", k => if (include) k == v else k != v)
      countsBy(sql, "sval") match {
        case Right(actual) if actual == expected => None
        case other => Some(s"[$sql]\n    expected $expected\n    got      $other")
      }
    }
    withClue(s"${wrong.size} of ${cells.size} cells wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  it should "refuse the empty string compared with = or <> before emission (a CONTROL)" in {
    keyFilterLoaded
    val wrong = Seq("sval = ''", "sval <> ''").flatMap { predicate =>
      val sql = s"SELECT sval, COUNT(*) AS c FROM $keyIndex GROUP BY sval HAVING $predicate"
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error) if error.statusCode.contains(400) => None
        case other => Some(s"[$sql] must be refused before emission, got $other")
      }
    }
    withClue(s"${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "compare a numeric key as a number and a boolean key as a boolean -- never fail" in {
    val numericWrong = for {
      (field, v)               <- Seq("lval" -> "1", "dval" -> "1.5")
      (op, include, predicate) <- oneValueOps
      sql = s"SELECT $field, COUNT(*) AS c FROM $keyIndex GROUP BY $field HAVING ${predicate(field, v)}"
      expected = expectedCounts(field, k => if (include) k == v else k != v)
      actual = countsBy(sql, field)
      if actual != Right(expected)
    } yield s"[$sql] expected $expected, got $actual"
    // `=` / `<>` only -- no boolean IN grammar. A boolean key's spelling in a row differs by client,
    // so the COUNT identifies the group: 2 documents are true, 3 are false.
    val booleanWrong = Seq("bval = true" -> 2L, "bval <> true" -> 3L).flatMap {
      case (predicate, count) =>
        val sql = s"SELECT bval, COUNT(*) AS c FROM $keyIndex GROUP BY bval HAVING $predicate"
        val actual = countsBy(sql, "bval").map(_.values.toSeq)
        if (actual == Right(Seq(count))) None
        else Some(s"[$sql] expected Right(List($count)), got $actual")
    }
    val wrong = numericWrong ++ booleanWrong
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "filter each of two grouping levels exactly" in {
    keyFilterLoaded
    implicit val levelsCtx: ConversionContext = NativeContext
    val sql =
      s"SELECT sval, lval, COUNT(*) AS c FROM $keyIndex GROUP BY sval, lval " +
      "HAVING sval = 'a.b' AND lval = 1"
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        response.results.map(r =>
          (r("sval").toString, numericText(r("lval")), r("c").toString.toDouble.toLong)
        ) shouldBe Seq(("a.b", "1", 2L))
      case ElasticFailure(error) => fail(s"[$sql] ${error.message}")
    }
  }

  "the kept and removed lists of one key filter" should
  "combine exactly -- and on Elasticsearch 6 refuse, by name, only a pattern beside several values" in {
    keyFilterLoaded
    val aKeys: Set[String] = stringKeys.filter(_.startsWith("a")).toSet
    val cells: Seq[(String, String, Set[String])] = Seq(
      ("V1+V1", "sval = 'a.b' AND sval <> 'axb'", Set("a.b")),
      ("V1+Vn", "sval = 'a.b' AND sval NOT IN ('axb','b')", Set("a.b")),
      ("V1+P", "sval = 'a.b' AND sval NOT LIKE 'x%'", Set("a.b")),
      ("Vn+V1", "sval IN ('a.b','axb') AND sval <> 'axb'", Set("a.b")),
      ("Vn+Vn", "sval IN ('a.b','axb','b') AND sval NOT IN ('axb','x')", Set("a.b", "b")),
      ("Vn+P", "sval IN ('a.b','axb') AND sval NOT LIKE 'x%'", Set("a.b", "axb")),
      ("P+V1", "sval LIKE 'a%' AND sval <> 'a.b'", aKeys - "a.b"),
      ("P+Vn", "sval LIKE 'a%' AND sval NOT IN ('a.b','axb')", aKeys -- Set("a.b", "axb")),
      ("P+P", "sval LIKE 'a%' AND sval NOT LIKE 'a.%'", aKeys - "a.b")
    )
    val refusedOnSix = Set("Vn+P", "P+Vn")
    def namedRefusal(e: ElasticError): Boolean =
      e.statusCode.contains(400) &&
      e.message.contains("HAVING cannot filter the GROUP BY key 'sval' on Elasticsearch 6") &&
      e.message.contains("RLIKE") && e.message.contains("escaping") && e.message.contains("7.11")
    // COLLECTED, never `foreach { ... shouldBe ... }`: a red run must read as the per-cell map.
    val wrong = cells.flatMap { case (label, predicate, kept) =>
      val sql = s"SELECT sval, COUNT(*) AS c FROM $keyIndex GROUP BY sval HAVING $predicate"
      if (esMajor == 6 && refusedOnSix(label)) {
        // the gateway route (REPL / JDBC / Arrow): an honest 400 naming the key and the remedy
        val gateway = Await.result(client.run(sql), 60.seconds) match {
          case ElasticFailure(error) if namedRefusal(error) => None
          case other =>
            Some(s"[$label] gateway: must be refused by name on Elasticsearch 6, got $other")
        }
        // the direct client API: the same ElasticError, THROWN or returned; an answer is not
        implicit val directCtx: ConversionContext = NativeContext
        val direct = Try(client.search(SelectStatement(sql))) match {
          case Failure(e: ElasticError) if namedRefusal(e)   => None
          case Success(ElasticFailure(e)) if namedRefusal(e) => None
          case other => Some(s"[$label] direct API: must be refused by name, got $other")
        }
        gateway.toSeq ++ direct.toSeq
      } else {
        val expected = expectedCounts("sval", kept)
        countsBy(sql, "sval") match {
          case Right(actual) if actual == expected => Nil
          case other => Seq(s"[$label] $sql\n    expected $expected\n    got      $other")
        }
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "answer the remedy the refusal names -- on every major, with the groups meant" in {
    val aKeys: Set[String] = stringKeys.filter(_.startsWith("a")).toSet
    val wrong = Seq(
      "sval LIKE 'a%' AND sval NOT RLIKE 'a\\.b|axb'" -> (aKeys -- Set("a.b", "axb")),
      "sval RLIKE 'a\\.b|axb' AND sval NOT LIKE 'x%'" -> Set("a.b", "axb")
    ).flatMap { case (predicate, kept) =>
      val sql = s"SELECT sval, COUNT(*) AS c FROM $keyIndex GROUP BY sval HAVING $predicate"
      val expected = Right(expectedCounts("sval", kept))
      val actual = countsBy(sql, "sval")
      if (actual == expected) None else Some(s"[$sql] expected $expected, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  "a key filter on the completeness fixture" should "filter an INTEGER grouping key exactly" in {
    implicit val intCtx: ConversionContext = NativeContext
    client.search(
      SelectStatement(
        "SELECT amount, COUNT(*) AS c FROM group_by_completeness GROUP BY amount HAVING amount = 2"
      )
    ) match {
      case ElasticSuccess(response) =>
        // amount 2 exists in cat_02 .. cat_37: 36 documents, one group
        response.results.map(r =>
          numericText(r("amount")) -> r("c").toString.toDouble.toLong
        ) shouldBe
          Seq("2" -> (categories - 1).toLong)
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}")
    }
  }

  it should "honour a multi-value include beside a ONE-value exclude" in {
    categoriesOf(
      "HAVING category IN ('cat_35','cat_36','cat_37') AND category <> 'cat_35'"
    ) shouldBe Seq("cat_36", "cat_37")
  }

  "a fractional number compared with an INTEGER GROUP BY key" should
  "be refused by name on every major -- never rounded by Elasticsearch" in {
    keyFilterLoaded
    nestedKeyFilterLoaded
    // (statement, the key column in the rows, the key as the refusal names it). The SELECT-alias
    // rows are the spelling whose HAVING leaf carries no column of its own: the rule must read the
    // BUCKET's type.
    val rows = Seq(
      (
        s"SELECT lval, COUNT(*) AS c FROM $keyIndex GROUP BY lval HAVING lval = 1.5",
        "lval",
        "lval"
      ),
      (
        s"SELECT lval, COUNT(*) AS c FROM $keyIndex GROUP BY lval HAVING lval <> 1.5",
        "lval",
        "lval"
      ),
      (
        s"SELECT lval, COUNT(*) AS c FROM $keyIndex GROUP BY lval HAVING lval IN (1.5, 2.5)",
        "lval",
        "lval"
      ),
      (s"SELECT lval AS l, COUNT(*) AS c FROM $keyIndex GROUP BY l HAVING l = 1.5", "l", "l"),
      // a STRING reaches the filter as the same text, and Elasticsearch parses it as a number
      (s"SELECT lval AS l, COUNT(*) AS c FROM $keyIndex GROUP BY l HAVING l = '1.5'", "l", "l"),
      // an UNNEST key, whose predicate the resolution wraps in its nested relation
      (unnestedKey("inner_items.qty <> 1.5"), "q", "inner_items.qty"),
      (unnestedKey("inner_items.qty NOT IN (1.5)"), "q", "inner_items.qty")
    )
    // COLLECTED, and a row that is answered prints WHAT Elasticsearch answered: that is the premise
    // of the rule (`lval = 1.5` keeps the group 1, `lval <> 1.5` drops it).
    val wrong = rows.flatMap { case (sql, column, key) =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error)
            if error.statusCode.contains(400) &&
              error.message.contains(s"HAVING cannot compare the integer GROUP BY key $key") =>
          None
        case other =>
          Some(
            s"[$sql] must be refused, got $other -- the groups answered: ${countsBy(sql, column)}"
          )
      }
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "still accept a whole number with a decimal point, and a fraction on a DOUBLE key" in {
    val wrong = Seq(
      (s"SELECT lval, COUNT(*) AS c FROM $keyIndex GROUP BY lval HAVING lval = 2.0", "lval") ->
      expectedCounts("lval", _ == "2"),
      (s"SELECT dval, COUNT(*) AS c FROM $keyIndex GROUP BY dval HAVING dval = 1.5", "dval") ->
      expectedCounts("dval", _ == "1.5"),
      (s"SELECT lval AS l, COUNT(*) AS c FROM $keyIndex GROUP BY l HAVING l = 2.0", "l") ->
      expectedCounts("l", _ == "2")
    ).flatMap { case ((sql, field), expected) =>
      val actual = countsBy(sql, field)
      if (actual == Right(expected)) None else Some(s"[$sql] expected $expected, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "still accept a whole number on an UNNEST key -- asserted on the group KEYS" in {
    val wrong = Seq(
      unnestedKey("inner_items.qty = 2.0") -> Set("2"),
      unnestedKey("inner_items.qty <> 2")  -> Set("1", "10", "21")
    ).flatMap { case (sql, expected) =>
      val actual = keysBy(sql, "q")
      if (actual == Right(expected)) None else Some(s"[$sql] expected $expected, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  /** The keys fixture under an index PATTERN: it matches that one index. */
  private val keyPattern = "group_by_key_filt*"

  /** Two indices mapping `lval` with DIFFERENT numeric types: integer in one, long in the other. */
  private val mixedIntIndex = "group_by_key_mixed_int"
  private val mixedLongIndex = "group_by_key_mixed_long"

  private lazy val mixedKeyLoaded: Unit =
    Seq(mixedIntIndex -> "integer", mixedLongIndex -> "long").foreach { case (index, lvalType) =>
      val settings = """{"number_of_shards": 1, "number_of_replicas": 0}"""
      val mapping =
        s"""{"properties": {"id": {"type": "keyword"}, "lval": {"type": "$lvalType"}}}"""
      client.createIndex(index, settings = settings).get shouldBe true
      client.setMapping(index, mapping).get shouldBe true
      implicit val mixedBulkOptions: BulkOptions =
        BulkOptions(defaultIndex = index, logEvery = 1000)
      implicit def mixedListToSource[T](list: List[T]): Source[T, NotUsed] =
        Source.fromIterator(() => list.iterator)
      // lval 1: one document in the integer index, two in the long one
      val values = if (lvalType == "integer") Seq(1L, 2L) else Seq(1L, 1L, 3L)
      val docs = values.zipWithIndex.map { case (v, j) =>
        s"""{"id": "$lvalType-$j", "lval": $v}"""
      }.toList
      client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
        case ElasticSuccess(_) => client.refresh(index)
        case ElasticFailure(error) =>
          error.cause.foreach(_.printStackTrace())
          fail(s"Bulk indexing $index failed: ${error.message}")
      }
    }

  it should "be refused over an INDEX PATTERN too, whose merged mapping types the key" in {
    keyFilterLoaded
    val sql = s"SELECT lval, COUNT(*) AS c FROM $keyPattern GROUP BY lval HAVING lval = 1.5"
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticFailure(error) =>
        withClue(s"[$sql] ") {
          error.statusCode shouldBe Some(400)
          error.message should include("HAVING cannot compare the integer GROUP BY key lval")
        }
      case other => fail(s"[$sql] must be refused, got $other")
    }
  }

  it should "be left to Elasticsearch where the indices map the key with different types" in {
    // the merged mapping gives `lval` no type, so no check runs: Elasticsearch rounds 1.5 down
    // and keeps the group 1 -- the documented boundary of the check
    mixedKeyLoaded
    val sql =
      s"SELECT lval, COUNT(*) AS c FROM $mixedIntIndex, $mixedLongIndex GROUP BY lval " +
      "HAVING lval = 1.5"
    countsBy(sql, "lval") shouldBe Right(Map("1" -> 3L))
  }

  // ---------------------------------------------------------------------------------------------
  // An aggregate over an UNNEST GROUP BY key, grouped by that key: one value PER GROUP
  // ---------------------------------------------------------------------------------------------

  /** `items` is nested; each entry is (qty, sku), and `w`, a number of its own
    * ([[nestedMetricWeight]]). `qty` 1 has three entries, 2 two, 10 and 21 one; the (sku, qty) pair
    * (x, 1) has three entries and every other pair one. `category` is a root key and `rank` a root
    * number.
    */
  private val nestedMetricIndex = "group_by_nested_key_metric"

  private val nestedMetricDocs: Seq[(String, Seq[(Long, String)])] = Seq(
    "m1" -> Seq(1L -> "x", 2L -> "x"),
    "m2" -> Seq(2L -> "y", 10L -> "x"),
    "m3" -> Seq(21L -> "y", 1L -> "x"),
    "m4" -> Seq(1L -> "x"),
    "m5" -> Seq.empty
  )

  private val nestedMetricCategory: Map[String, String] =
    Map("m1" -> "a", "m2" -> "b", "m3" -> "a", "m4" -> "c", "m5" -> "b")

  private val nestedMetricRank: Map[String, Long] =
    Map("m1" -> 3L, "m2" -> 5L, "m3" -> 7L, "m4" -> 11L, "m5" -> 13L)

  /** The `w` of an entry: a field of the array other than the key. */
  private def nestedMetricWeight(qty: Long, sku: String): Long =
    qty * 10 + (if (sku == "x") 1 else 2)

  private lazy val nestedMetricLoaded: Unit = {
    val settings = """{"number_of_shards": 2, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "category": { "type": "keyword" },
        |    "rank":     { "type": "long" },
        |    "items": {
        |      "type": "nested",
        |      "properties": {
        |        "qty": { "type": "long" }, "sku": { "type": "keyword" }, "w": { "type": "long" }
        |      }
        |    }
        |  }
        |}""".stripMargin
    client.createIndex(nestedMetricIndex, settings = settings).get shouldBe true
    client.setMapping(nestedMetricIndex, mapping).get shouldBe true
    implicit val metricBulkOptions: BulkOptions =
      BulkOptions(defaultIndex = nestedMetricIndex, logEvery = 1000)
    implicit def metricListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    val docs = nestedMetricDocs.map { case (id, items) =>
      val entries = items
        .map { case (q, k) =>
          s"""{"qty": $q, "sku": "$k", "w": ${nestedMetricWeight(q, k)}}"""
        }
        .mkString(", ")
      s"""{"id": "$id", "category": "${nestedMetricCategory(id)}", """ +
      s""""rank": ${nestedMetricRank(id)}, "items": [$entries]}"""
    }.toList
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(nestedMetricIndex)
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $nestedMetricIndex failed: ${error.message}")
    }
  }

  private val unnestItems = s"FROM $nestedMetricIndex o JOIN UNNEST(o.items) AS i"

  /** Every aggregate over the key, one column each. */
  private val keyAggregates =
    "MIN(i.qty) AS mn, MAX(i.qty) AS mx, SUM(i.qty) AS s, AVG(i.qty) AS a, COUNT(i.qty) AS c, " +
    "COUNT(DISTINCT i.qty) AS d"

  private val keyColumns = Seq("q", "mn", "mx", "s", "a", "c", "d")

  /** The SQL meaning, from the documents: per kept `qty`, MIN = MAX = AVG = the key, COUNT = its
    * entries, SUM = key x entries, COUNT(DISTINCT) = 1.
    */
  private def perQty(keep: Long => Boolean): Seq[Seq[String]] =
    nestedMetricDocs
      .flatMap(_._2)
      .groupBy(_._1)
      .toSeq
      .collect {
        case (q, entries) if keep(q) =>
          val n = entries.size.toLong
          Seq(q, q, q, q * n, q, n, 1L).map(_.toString)
      }
      .sortBy(_.mkString("|"))

  /** The rows as the client answers them, in its order; `Left` carries a failure's message. */
  private def metricRows(sql: String, columns: Seq[String]): Either[String, Seq[Seq[String]]] = {
    nestedMetricLoaded
    implicit val metricCtx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        Right(response.results.map { row =>
          columns.map(c =>
            row
              .get(c)
              .map(v =>
                Option(v).map(x => Try(numericText(x)).getOrElse(x.toString)).getOrElse("<null>")
              )
              .getOrElse("<absent>")
          )
        })
      case ElasticFailure(error) => Left(error.message)
    }
  }

  private def sortedByKey(
    rows: Either[String, Seq[Seq[String]]]
  ): Either[String, Seq[Seq[String]]] =
    rows.map(_.sortBy(_.mkString("|")))

  "an aggregate over an UNNEST GROUP BY key, grouped by that key" should
  "answer one value per group, for every aggregate" in {
    // Was NULL on every group: the metric was emitted beside the buckets, one value over all of them.
    sortedByKey(
      metricRows(s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty", keyColumns)
    ) shouldBe Right(perQty(_ => true))
  }

  it should "filter the groups on it in a HAVING" in {
    sortedByKey(
      metricRows(
        s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty HAVING MIN(i.qty) > 1",
        keyColumns
      )
    ) shouldBe Right(perQty(_ > 1))
  }

  it should "order the groups by it" in {
    metricRows(
      s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty ORDER BY MIN(i.qty) DESC",
      keyColumns
    ) shouldBe Right(perQty(_ => true).sortBy(row => -row.head.toLong))
  }

  it should "still answer per group beside a HAVING on the key, with two values and with one" in {
    // the one-value spelling is the one Elasticsearch 6.8 used to refuse with an HTTP 500
    val wrong = Seq(
      s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty HAVING i.qty IN (1, 10)" ->
      perQty(Set(1L, 10L)),
      s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty HAVING i.qty = 1" ->
      perQty(_ == 1L)
    ).flatMap { case (sql, expected) =>
      val actual = sortedByKey(metricRows(sql, keyColumns))
      if (actual == Right(expected)) None else Some(s"[$sql] expected $expected, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "answer per group under a LIMIT" in {
    metricRows(
      s"SELECT i.qty AS q, $keyAggregates $unnestItems GROUP BY i.qty LIMIT 2",
      keyColumns
    ) match {
      case Right(rows) =>
        rows should have size 2
        rows.foreach(row => perQty(_ => true) should contain(row))
      case Left(error) => fail(error)
    }
  }

  it should "answer per pair over two keys of the same array, in both orders" in {
    val perPair: Seq[Seq[String]] =
      nestedMetricDocs
        .flatMap(_._2)
        .groupBy(identity)
        .toSeq
        .map { case ((q, k), entries) => Seq(k, q.toString, q.toString, entries.size.toString) }
        .sortBy(_.mkString("|"))
    val wrong = Seq("i.sku, i.qty", "i.qty, i.sku").flatMap { keys =>
      val sql =
        s"SELECT i.sku AS k, i.qty AS q, MIN(i.qty) AS mn, COUNT(i.qty) AS c $unnestItems " +
        s"GROUP BY $keys"
      val actual = sortedByKey(metricRows(sql, Seq("k", "q", "mn", "c")))
      if (actual == Right(perPair)) None else Some(s"[$sql] expected $perPair, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "answer per pair grouped by a root key, then the UNNEST key" in {
    val perPair: Seq[Seq[String]] =
      nestedMetricDocs
        .flatMap { case (id, items) => items.map(e => nestedMetricCategory(id) -> e._1) }
        .groupBy(identity)
        .toSeq
        .map { case ((cat, q), entries) => Seq(cat, q.toString, q.toString, entries.size.toString) }
        .sortBy(_.mkString("|"))
    sortedByKey(
      metricRows(
        s"SELECT o.category AS cat, i.qty AS q, MIN(i.qty) AS mn, COUNT(i.qty) AS c $unnestItems " +
        "GROUP BY o.category, i.qty",
        Seq("cat", "q", "mn", "c")
      )
    ) shouldBe Right(perPair)
  }

  it should "leave an aggregate over ANOTHER field of the array as it was -- per group" in {
    // the canary: grouped by `sku`, the aggregate over `qty` always was a sub-aggregation
    sortedByKey(
      metricRows(
        s"SELECT i.sku AS k, MIN(i.qty) AS mn, MAX(i.qty) AS mx $unnestItems GROUP BY i.sku",
        Seq("k", "mn", "mx")
      )
    ) shouldBe Right(Seq(Seq("x", "1", "10"), Seq("y", "2", "21")))
  }

  // ---------------------------------------------------------------------------------------------
  // An UNNEST key listed before a root key: answered as the same GROUP BY with the root key first
  // ---------------------------------------------------------------------------------------------

  /** Every aggregate of the key, and of `w` and `sku`, two other fields of the array. */
  private val pairAggregates =
    s"$keyAggregates, MIN(i.w) AS wn, MAX(i.w) AS wx, SUM(i.w) AS ws, AVG(i.w) AS wa, " +
    "COUNT(i.w) AS wc, COUNT(i.sku) AS kc, COUNT(DISTINCT i.sku) AS kd"

  private val pairColumns =
    Seq("cat", "q", "mn", "mx", "s", "a", "c", "d", "wn", "wx", "ws", "wa", "wc", "kc", "kd")

  /** The SQL meaning, from the documents: per kept (category, qty) pair, every aggregate of
    * [[pairAggregates]].
    */
  private def perCategoryQty(keep: Long => Boolean): Seq[Seq[String]] =
    nestedMetricDocs
      .flatMap { case (id, items) => items.map { case (q, k) => (nestedMetricCategory(id), q, k) } }
      .groupBy { case (cat, q, _) => (cat, q) }
      .toSeq
      .collect {
        case ((cat, q), entries) if keep(q) =>
          val n = entries.size.toLong
          val ws = entries.map { case (_, qty, sku) => BigDecimal(nestedMetricWeight(qty, sku)) }
          val distinctSkus = entries.map(_._3).distinct.size.toLong
          Seq(cat) ++ Seq(q, q, q, q * n, q, n, 1L).map(_.toString) ++
          Seq(ws.min, ws.max, ws.sum, ws.sum / BigDecimal(n)).map(numericText) ++
          Seq(n, n, distinctSkus).map(_.toString)
      }
      .sortBy(_.mkString("|"))

  private val pairSelect = s"SELECT o.category AS cat, i.qty AS q, $pairAggregates $unnestItems"

  "a GROUP BY that lists an UNNEST key before a root key" should
  "answer per pair as the root-first GROUP BY does, for every aggregate and key condition" in {
    // Was NULL for every aggregate of the array, which the request left out, on every major; and on
    // Elasticsearch 6.8 a one-value HAVING on the key failed with HTTP 500.
    val wrong = Seq(
      ""                         -> perCategoryQty(_ => true),
      " HAVING i.qty = 1"        -> perCategoryQty(_ == 1L),
      " HAVING i.qty <> 2"       -> perCategoryQty(_ != 2L),
      " HAVING i.qty NOT IN (2)" -> perCategoryQty(_ != 2L),
      " HAVING i.qty = 2.0"      -> perCategoryQty(_ == 2L),
      " HAVING i.qty IN (1, 10)" -> perCategoryQty(Set(1L, 10L)),
      " HAVING MIN(i.qty) > 1"   -> perCategoryQty(_ > 1L)
    ).flatMap { case (clause, expected) =>
      val written = s"$pairSelect GROUP BY i.qty, o.category$clause"
      val actual = metricRows(written, pairColumns)
      val twin = metricRows(s"$pairSelect GROUP BY o.category, i.qty$clause", pairColumns)
      Seq(
        if (sortedByKey(actual) == Right(expected)) None
        else Some(s"[$written] expected $expected, got $actual"),
        if (actual == twin) None else Some(s"[$written] answered $actual, the root-first $twin")
      ).flatten
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "order and limit the pairs as the root-first GROUP BY does" in {
    val all = perCategoryQty(_ => true)
    val wrong = Seq(" ORDER BY MIN(i.qty) DESC", " ORDER BY SUM(i.w) DESC", " LIMIT 2").flatMap {
      clause =>
        val written = s"$pairSelect GROUP BY i.qty, o.category$clause"
        val actual = metricRows(written, pairColumns)
        val twin = metricRows(s"$pairSelect GROUP BY o.category, i.qty$clause", pairColumns)
        val rowsRight = actual match {
          case Right(rows) if clause.contains("LIMIT") => rows.nonEmpty && rows.forall(all.contains)
          case Right(rows)                             => rows.sortBy(_.mkString("|")) == all
          case Left(_)                                 => false
        }
        Seq(
          if (rowsRight) None else Some(s"[$written] expected rows of $all, got $actual"),
          if (actual == twin) None else Some(s"[$written] answered $actual, the root-first $twin")
        ).flatten
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "answer a root aggregate, and the keys alone, as the root-first GROUP BY does" in {
    // A root aggregate beside an UNNEST key is wrong in EVERY order, a recorded defect: written UNNEST
    // key first it answered one row per category with the key NULL. It now answers what the
    // root-first spelling answers, which is asserted here -- never a value.
    val wrong = for {
      aggregate <- Seq("COUNT(o.id) AS m", "SUM(o.rank) AS m", "COUNT(*) AS m", "")
      clause    <- Seq("", " HAVING i.qty = 1", " HAVING i.qty IN (1, 10)")
      columns = if (aggregate.isEmpty) Seq("q", "cat") else Seq("q", "cat", "m")
      select =
        if (aggregate.isEmpty) "SELECT i.qty AS q, o.category AS cat"
        else s"SELECT i.qty AS q, o.category AS cat, $aggregate"
      written = s"$select $unnestItems GROUP BY i.qty, o.category$clause"
      actual = metricRows(written, columns)
      twin = metricRows(s"$select $unnestItems GROUP BY o.category, i.qty$clause", columns)
      if actual != twin
    } yield s"[$written] answered $actual, the root-first $twin"
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  // =============================================================================================
  // Story IDENT-4 -- a QUALIFIED GROUP BY key filters groups exactly as the bare spelling does.
  //
  // BI tools always qualify (`SELECT g.category ... FROM idx g GROUP BY g.category HAVING
  // g.category = 'x'`). Every single-table row runs at TWO venues because they resolve the
  // statement a different number of times: `group_by_completeness g` (one concrete index -- the
  // client resolves the statement again against its mapping) and `group_by_complet* g` (an index
  // pattern -- the client sends the parse-time resolution as it is). The witnesses are MAX and SUM,
  // which Elasticsearch cannot answer from `doc_count`: a metric aggregation left off its level
  // cannot hide behind COUNT(*). Oracle, from the fixture alone: `cat_i` holds i documents with
  // amounts 1..i.
  // =============================================================================================

  private val qualifiedVenues: Seq[String] = Seq("group_by_completeness g", "group_by_complet* g")

  /** A metric as one number: -1 when the column is ABSENT (its aggregation never reached the
    * level), -2 when it is NULL.
    */
  private def metricOf(row: Map[String, Any], column: String): Long =
    row.get(column) match {
      case None        => -1L
      case Some(null)  => -2L
      case Some(value) => BigDecimal(numericText(value)).toLongExact
    }

  /** key `k` -> (count, max, sum), as the client answers them. */
  private def maxSumRows(sql: String): Either[String, Map[String, (Long, Long, Long)]] = {
    implicit val rowCtx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        Right(response.results.map { row =>
          row.get("k").map(v => String.valueOf(v)).getOrElse("<absent>") ->
          (metricOf(row, "c"), metricOf(row, "mx"), metricOf(row, "s"))
        }.toMap)
      case ElasticFailure(error) => Left(error.message)
    }
  }

  /** (count, max, sum) of every group, sorted -- for statements that project no key. */
  private def metricTriples(sql: String): Either[String, Seq[(Long, Long, Long)]] = {
    implicit val tripleCtx: ConversionContext = NativeContext
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) =>
        Right(
          response.results
            .map(row => (metricOf(row, "c"), metricOf(row, "mx"), metricOf(row, "s")))
            .sorted
        )
      case ElasticFailure(error) => Left(error.message)
    }
  }

  private def expectedGroups(keep: String => Boolean): Map[String, (Long, Long, Long)] =
    (1 to categories)
      .map { i =>
        f"cat_$i%02d" -> ((i.toLong, i.toLong, i.toLong * (i + 1) / 2))
      }
      .filter { case (k, _) => keep(k) }
      .toMap

  private def refusalOf(sql: String): String =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => fail(s"[$sql] was accepted and answered $result")
      case ElasticFailure(error)  => error.message
    }

  /** (label, SELECT key, GROUP BY key, HAVING operand): the key bare or qualified on each side. */
  private val fourSpellings: Seq[(String, String, String, String)] = Seq(
    ("bare / bare", "category", "category", "category"),
    ("bare / qualified", "category", "category", "g.category"),
    ("qualified / bare", "g.category", "g.category", "category"),
    ("qualified / qualified", "g.category", "g.category", "g.category")
  )

  private val qualifiedOps: Seq[(String, String => String, String => Boolean)] = Seq(
    ("=", k => s"$k = 'cat_37'", _ == "cat_37"),
    ("<>", k => s"$k <> 'cat_01'", _ != "cat_01"),
    ("IN", k => s"$k IN ('cat_36', 'cat_37')", Set("cat_36", "cat_37")),
    ("NOT IN", k => s"$k NOT IN ('cat_01', 'cat_02')", k => !Set("cat_01", "cat_02")(k)),
    ("LIKE", k => s"$k LIKE 'cat_3%'", _.startsWith("cat_3")),
    ("NOT LIKE", k => s"$k NOT LIKE 'cat_0%'", k => !k.startsWith("cat_0")),
    ("RLIKE", k => s"$k RLIKE 'cat_3[0-2]'", Set("cat_30", "cat_31", "cat_32")),
    ("MATCH", k => s"MATCH ($k) AGAINST ('cat_37')", _ == "cat_37")
  )

  /** The 4 spellings x {`AS k`, unaliased} x 8 operators at the given venues (the unaliased
    * qualified / qualified spelling is the one that used to be refused), each against the oracle --
    * by output name when the key is projected, through `metricTriples` when it is not (the triples
    * of `cat_i` are distinct, so they name the groups). COLLECTED: the wrong cells.
    */
  private def qualifiedMatrix(venues: Seq[String]): (Int, Seq[String]) = {
    val cells = for {
      venue                            <- venues
      (label, select, groupBy, having) <- fourSpellings
      aliased                          <- Seq(true, false)
      (op, predicate, keep)            <- qualifiedOps
    } yield (
      s"$venue | $label | ${if (aliased) "AS k" else "unaliased"} | $op",
      (if (aliased) s"SELECT $select AS k, " else "SELECT ") +
      s"COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $venue GROUP BY $groupBy HAVING ${predicate(having)}",
      aliased,
      expectedGroups(keep)
    )
    val wrong = cells.flatMap { case (label, sql, aliased, expected) =>
      val (actual, wanted) =
        if (aliased) (maxSumRows(sql), Right(expected))
        else (metricTriples(sql), Right(expected.values.toSeq.sorted))
      if (actual == wanted) None
      else
        Some(s"[$label] $sql\n    expected ${wanted
          .map(_.toString.take(300))}\n    got      ${actual.map(_.toString.take(300))}")
    }
    (cells.size, wrong)
  }

  "IDENT-4: a qualified GROUP BY key" should
  "filter groups exactly as the bare spelling does -- four spellings, two alias forms, eight operators, two venues" in {
    val (size, wrong) = qualifiedMatrix(qualifiedVenues)
    size shouldBe qualifiedVenues.size * fourSpellings.size * 2 * qualifiedOps.size
    // COLLECTED, never `foreach { … shouldBe … }`: the wrong cells are reported together.
    withClue(s"${wrong.size} of $size cells wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  it should "put its metrics on their level at BOTH venues" in {
    // At a ONE-resolution venue the metric's bucket path used to be the parsed `g.category`, the
    // level's `category`, so `mx` and `s` never reached a level -- the columns came back NULL (-2)
    // under `NativeContext`. Since #412 both venues here resolve twice;
    // the one-resolution venue has its own block below.
    val wrong = qualifiedVenues.flatMap { venue =>
      val sql =
        s"SELECT g.category AS k, COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $venue GROUP BY g.category"
      maxSumRows(sql) match {
        case Right(actual) if actual == expectedGroups(_ => true) => None
        case other                                                => Some(s"[$sql] $other")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  it should "answer its SELECT-alias spelling at both venues (the remedy if the story were killed)" in {
    val wrong = qualifiedVenues.flatMap { venue =>
      val sql =
        s"SELECT g.category AS k, COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $venue " +
        "GROUP BY g.category HAVING k = 'cat_37'"
      maxSumRows(sql) match {
        case Right(actual) if actual == expectedGroups(_ == "cat_37") => None
        case other                                                    => Some(s"[$sql] $other")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  it should "answer GROUP BY <select alias> over a qualified column at both venues" in {
    // Accepted and answered before this story at a venue that resolves twice (at a one-resolution
    // venue its body lacked the metrics).
    // Executable in core, so it is held at RUN, not only at RENDER.
    val wrong = qualifiedVenues.flatMap { venue =>
      val sql =
        s"SELECT g.category AS k, COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $venue " +
        "GROUP BY k HAVING k = 'cat_37'"
      maxSumRows(sql) match {
        case Right(actual) if actual == expectedGroups(_ == "cat_37") => None
        case other                                                    => Some(s"[$sql] $other")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  it should "select exactly the groups it names inside a WHERE subquery" in {
    maxSumRows(
      "SELECT category AS k, COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM group_by_completeness " +
      "WHERE category IN (SELECT g.category FROM group_by_completeness g GROUP BY g.category " +
      "HAVING g.category = 'cat_37') GROUP BY category"
    ) shouldBe Right(expectedGroups(_ == "cat_37"))
  }

  it should "refuse a table's own name once aliased, and an undeclared qualifier -- neither is a key" in {
    Seq("group_by_completeness.category", "zz.category").foreach { operand =>
      val sql =
        "SELECT g.category AS k, COUNT(*) AS c FROM group_by_completeness g GROUP BY g.category " +
        s"HAVING $operand = 'cat_37'"
      withClue(s"[$sql] ") { refusalOf(sql) should include("can only filter on a GROUP BY key") }
    }
  }

  it should "carry Elasticsearch 6's refusal set on the qualified spelling" in {
    // IDENT-3 contract item 4: a pattern in one list beside two or more values in the other.
    val sql =
      "SELECT g.category AS k, COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s " +
      "FROM group_by_completeness g GROUP BY g.category " +
      "HAVING g.category LIKE 'cat_3%' AND g.category NOT IN ('cat_30', 'cat_31')"
    if (esMajor == 6) refusalOf(sql) should include("on Elasticsearch 6")
    else
      maxSumRows(sql) shouldBe Right(
        expectedGroups(k => k.startsWith("cat_3") && !Set("cat_30", "cat_31")(k))
      )
  }

  it should "refuse a QUALIFIED integer key compared with a fractional number, like the bare one" in {
    keyFilterLoaded
    val wrong = Seq(
      s"SELECT k.lval AS l, COUNT(*) AS c FROM $keyIndex k GROUP BY k.lval HAVING k.lval = 1.5",
      s"SELECT k.lval AS l, COUNT(*) AS c FROM $keyIndex k GROUP BY k.lval HAVING lval = 1.5",
      s"SELECT lval AS l, COUNT(*) AS c FROM $keyIndex k GROUP BY lval HAVING k.lval = 1.5"
    ).flatMap { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error)
            if error.statusCode.contains(400) &&
              // the refusal names the key as the condition spells it (`lval` or `k.lval`)
              error.message.contains("HAVING cannot compare the integer GROUP BY key") =>
          None
        case other =>
          Some(s"[$sql] must be refused, got $other -- the groups: ${countsBy(sql, "l")}")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
    countsBy(
      s"SELECT k.lval AS l, COUNT(*) AS c FROM $keyIndex k GROUP BY k.lval HAVING k.lval = 2.0",
      "l"
    ) shouldBe
    Right(expectedCounts("lval", _ == "2"))
  }

  // ---- the date fixture: two functions of one column, and the date-histogram suspect ----------

  private val datesIndex = "group_by_qualified_dates"

  /** 2024-01: 1 + 2 documents, 2024-02: 3, 2025-01: 4 -- amounts 1..10 in that order. */
  private val datedDocs: Seq[(String, Int)] = Seq(
    "2024-01-03" -> 1,
    "2024-01-20" -> 2,
    "2024-01-20" -> 3,
    "2024-02-03" -> 4,
    "2024-02-03" -> 5,
    "2024-02-03" -> 6,
    "2025-01-05" -> 7,
    "2025-01-05" -> 8,
    "2025-01-05" -> 9,
    "2025-01-05" -> 10
  )

  private lazy val qualifiedDatesLoaded: Unit = indexDatesFixture(datesIndex)

  private def indexDatesFixture(target: String): Unit = {
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":     { "type": "keyword" },
        |    "d":      { "type": "date" },
        |    "amount": { "type": "integer" }
        |  }
        |}""".stripMargin
    client.createIndex(target, settings = settings).get shouldBe true
    client.setMapping(target, mapping).get shouldBe true
    implicit val datesBulkOptions: BulkOptions = BulkOptions(defaultIndex = target, logEvery = 1000)
    implicit def datesListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    val docs = datedDocs.zipWithIndex.map { case ((d, amount), i) =>
      s"""{"id":"q$i","d":"$d","amount":$amount}"""
    }.toList
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) =>
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $target failed: ${error.message}")
    }
    client.refresh(target)
  }

  "IDENT-4: two functions of one date column as two keys" should
  "REFUSE an OR across the two levels, which Elasticsearch would run as an AND" in {
    qualifiedDatesLoaded
    // Answered until now, with the conjunction `[(3,3,6)]` at a one-resolution venue.
    val wrong = Seq(
      s"SELECT COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $datesIndex " +
      "GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 OR MONTH(d) = 1",
      s"SELECT COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $datesIndex q " +
      "GROUP BY YEAR(q.d), MONTH(q.d) HAVING YEAR(q.d) = 2024 OR MONTH(q.d) = 1"
    ).flatMap { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error) if error.message.contains("DIFFERENT GROUP BY keys") => None
        case _ => Some(s"[$sql] must be refused, got ${metricTriples(sql)}")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // The date-histogram key-filter refusal (required).
  "IDENT-4: a key predicate on a DATE grouping level" should
  "be refused by name -- and the remedy the refusal names must answer" in {
    // Answered until now with the unfiltered groups: the date histogram dropped the condition, at
    // every venue.
    qualifiedDatesLoaded
    val wrong = Seq(
      s"SELECT COUNT(*) AS c FROM $datesIndex GROUP BY DATE_TRUNC(d, MONTH) " +
      "HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'",
      s"SELECT COUNT(*) AS c FROM $datesIndex q GROUP BY DATE_TRUNC(q.d, MONTH) " +
      "HAVING DATE_TRUNC(q.d, MONTH) = '2024-01-01'",
      s"SELECT COUNT(*) AS c FROM $datesPattern q GROUP BY DATE_TRUNC(q.d, MONTH) " +
      "HAVING DATE_TRUNC(q.d, MONTH) = '2024-01-01'"
    ).flatMap { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error)
            if error.message.contains("HAVING cannot filter a DATE grouping level") =>
          None
        case _ => Some(s"[$sql] must be refused, got ${dateGroups(sql)}")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
    metricTriples(
      s"SELECT COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $datesIndex q " +
      "WHERE q.d >= '2024-01-01' AND q.d < '2024-02-01' GROUP BY DATE_TRUNC(q.d, MONTH)"
    ) shouldBe Right(Seq((3L, 3L, 6L)))
  }

  // The HAVING rules seeing a MATCH (required).
  "IDENT-4: a MATCH on the GROUP BY key" should "be refused where the rules cannot honour it" in {
    // COLLECTED, both venues and both spellings: answered until now with the intersection
    // `{cat_37}` where SQL means `{cat_36, cat_37}`.
    val combinations = for {
      venue         <- qualifiedVenues
      (select, key) <- Seq("category" -> "category", "g.category" -> "g.category")
    } yield s"SELECT $select AS k, COUNT(*) AS c FROM $venue GROUP BY $key " +
    s"HAVING MATCH ($key) AGAINST ('cat_37') OR COUNT(*) > 35"
    // The UNALIASED form too (metric-only, groups told apart by their triples).
    val unaliased = for {
      venue <- qualifiedVenues
      key   <- Seq("category", "g.category")
    } yield s"SELECT COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s FROM $venue GROUP BY $key " +
    s"HAVING MATCH ($key) AGAINST ('cat_37') OR COUNT(*) > 35"
    val wrong = ((combinations ++ unaliased).map(_ -> "different mechanisms") :+ (
      // since #408 a MATCH over a column that is neither a key nor an aggregate is refused by the
      // MATCH rule itself (not a witness of the HAVING rules seeing a MATCH)
      "SELECT category AS k, COUNT(*) AS c FROM group_by_completeness GROUP BY category " +
      "HAVING MATCH (id) AGAINST ('cat_37_1')" -> "HAVING cannot apply MATCH"
    )).flatMap { case (sql, reason) =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error) if error.message.contains(reason) => None
        case _ =>
          val answered =
            if (sql.contains(" AS k,")) maxSumRows(sql).map(_.keys.toSeq.sorted)
            else metricTriples(sql)
          Some(s"[$sql] must be refused ($reason), got $answered")
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // ---- shared rows of the date fixture ---------------------------------------------------------

  private val datesPattern = "group_by_qualified_d*"

  /** The document count of every group, sorted -- each date and each month of the date fixture has
    * its own count, so the counts name the groups.
    */
  private def dateGroups(sql: String): Either[String, Seq[Long]] =
    metricTriples(sql).map(_.map(_._1))

  // ---- a temporal WHERE comparison of a GROUP BY key column ----------------------------------
  //
  // Two date fields; the right operand of `q.d > DATE_ADD(q.e, INTERVAL 1 DAY)` is temporal without
  // a mapping, so the comparison is a date one at every resolution. SQL: `d > e + 1 day` keeps r1
  // only. Run at the one-resolution venue below.

  private def indexIm10Fixture(target: String): Unit = {
    val settings = """{"number_of_shards": 1, "number_of_replicas": 0}"""
    val mapping =
      """{"properties": {"id": {"type": "keyword"}, "d": {"type": "date"}, "e": {"type": "date"}}}"""
    client.createIndex(target, settings = settings).get shouldBe true
    client.setMapping(target, mapping).get shouldBe true
    implicit val im10BulkOptions: BulkOptions = BulkOptions(defaultIndex = target, logEvery = 1000)
    implicit def im10ListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    val docs = List(
      """{"id":"r1","d":"2024-01-10","e":"2024-01-01"}""",
      """{"id":"r2","d":"2024-01-02","e":"2024-01-01"}""",
      """{"id":"r3","d":"2024-01-01","e":"2024-01-05"}"""
    )
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(target)
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $target failed: ${error.message}")
    }
  }

  private def im10Rows(index: String): Seq[(String, Seq[Long])] = Seq(
    s"SELECT COUNT(*) AS c FROM $index q WHERE q.d > DATE_ADD(q.e, INTERVAL 1 DAY) GROUP BY q.d" -> Seq(
      1L
    ),
    s"SELECT COUNT(*) AS c FROM $index WHERE d > DATE_ADD(e, INTERVAL 1 DAY) GROUP BY d" -> Seq(1L)
  )

  // ---- the fractional key check over an index pattern (since #412 a pattern is typed) ---------

  "IDENT-4: an integer key compared with a fractional number over an index pattern" should
  "be refused in every spelling" in {
    keyFilterLoaded
    val wrong = Seq(
      "SELECT lval AS l, COUNT(*) AS c FROM group_by_key_filter* k GROUP BY lval HAVING lval = 1.5",
      "SELECT k.lval AS l, COUNT(*) AS c FROM group_by_key_filter* k GROUP BY k.lval HAVING k.lval = 1.5"
    ).flatMap { sql =>
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error)
            if error.message.contains("HAVING cannot compare the integer GROUP BY key") =>
          None
        case other =>
          Some(
            s"[$sql] must be refused, got ${other.toString.take(300)} -- groups ${countsBy(sql, "l")}"
          )
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // ---- the ONE-resolution venue: a remembered schema miss -------------------------------------
  //
  // Every one-update path of `resolveWithSchema` sends the same unresolved statement -- a failed or
  // denied mapping read, a remembered 404, a client that is not an `IndicesApi` -- so a FROM queried
  // BEFORE its index exists runs, while the miss is remembered (`schema-cache.ttl`, at most five
  // minutes), the bytes a user without the mapping privilege runs. Each index below is armed that
  // way, then created with the mapping and documents of its twin above; that the rows ran at ONE
  // resolution is proven by the MECHANISM, before and after them: the statement comes back out of
  // the resolution as the SAME instance (no mapping was attached).

  private val oneResolutionIndex = "group_by_one_resolution"

  private val oneResolutionDates = "group_by_one_resolution_dates"

  private val im10OneResolution = "group_by_im10_one"

  @scala.annotation.tailrec
  private def baseClientOf(c: ElasticClientApi): ElasticClientApi = c match {
    case delegator: ElasticClientDelegator => baseClientOf(delegator.delegate)
    case other                             => other
  }

  /** True when the statement is sent as parsed: no mapping was looked up. */
  private def resolvedOnce(sql: String): Boolean =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        baseClientOf(client).resolveWithSchema(single) match {
          case ElasticSuccess(sent) => sent eq single
          case ElasticFailure(_)    => false
        }
      case _ => false
    }

  private def armSchemaMiss(index: String): Unit = {
    implicit val armCtx: ConversionContext = NativeContext
    // the index does not exist yet: the lookup answers 404 and the miss is remembered
    client.search(SelectStatement(s"SELECT COUNT(*) AS c FROM $index g")) match {
      case ElasticSuccess(response) =>
        fail(s"[$index] exists already: ${response.results.size} rows")
      case ElasticFailure(_) => ()
    }
  }

  private def indexCategoriesFixture(target: String): Unit = {
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{
        |  "properties": {
        |    "id":       { "type": "keyword" },
        |    "category": { "type": "keyword" },
        |    "amount":   { "type": "integer" }
        |  }
        |}""".stripMargin
    client.createIndex(target, settings = settings).get shouldBe true
    client.setMapping(target, mapping).get shouldBe true
    val docs = (for {
      c <- 1 to categories
      d <- 1 to c
    } yield {
      val category = f"cat_$c%02d"
      s"""{"id":"${category}_$d","category":"$category","amount":$d}"""
    }).toList
    implicit val categoriesBulkOptions: BulkOptions =
      BulkOptions(defaultIndex = target, logEvery = 1000)
    implicit def categoriesListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => client.refresh(target)
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $target failed: ${error.message}")
    }
  }

  "IDENT-4 at a ONE-resolution venue (a remembered schema miss)" should
  "answer every qualified row as the bare spelling does" in {
    val armed = Seq(oneResolutionIndex, oneResolutionDates, im10OneResolution)
    armed.foreach(armSchemaMiss)
    indexCategoriesFixture(oneResolutionIndex)
    indexDatesFixture(oneResolutionDates)
    indexIm10Fixture(im10OneResolution)
    val started = System.currentTimeMillis()
    def witnesses: Seq[(String, Boolean)] =
      armed.map(i => i -> resolvedOnce(s"SELECT COUNT(*) AS c FROM $i g"))
    val before = witnesses
    val venue = s"$oneResolutionIndex g"
    val dates = s"$oneResolutionDates q"
    val metrics = "COUNT(*) AS c, MAX(amount) AS mx, SUM(amount) AS s"
    val (matrixSize, matrixWrong) = qualifiedMatrix(Seq(venue))
    def row(sql: String, expected: Any, answered: => Any): Option[String] = {
      val got = answered
      if (got == expected) None
      else Some(s"[$sql] expected $expected, got ${got.toString.take(400)}")
    }
    def refused(sql: String, reason: String, answered: => Any): Option[String] =
      Await.result(client.run(sql), 60.seconds) match {
        case ElasticFailure(error) if error.message.contains(reason) => None
        case _ => row(sql, s"refused ($reason)", answered)
      }
    val named = Seq(
      // A qualified key's metrics, and a HAVING over them
      row(
        s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category",
        Right(expectedGroups(_ => true)),
        maxSumRows(s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category")
      ),
      row(
        s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category HAVING COUNT(*) > 35",
        Right(expectedGroups(Set("cat_36", "cat_37"))),
        maxSumRows(
          s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category HAVING COUNT(*) > 35"
        )
      ),
      // the SELECT-alias remedy
      row(
        s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category HAVING k = 'cat_37'",
        Right(expectedGroups(_ == "cat_37")),
        maxSumRows(
          s"SELECT g.category AS k, $metrics FROM $venue GROUP BY g.category HAVING k = 'cat_37'"
        )
      ),
      row(
        s"SELECT g.category AS k, $metrics FROM $venue GROUP BY k HAVING k = 'cat_37'",
        Right(expectedGroups(_ == "cat_37")),
        maxSumRows(s"SELECT g.category AS k, $metrics FROM $venue GROUP BY k HAVING k = 'cat_37'")
      )
    ) ++ (for {
      // A MATCH on the key: aliased and unaliased, bare and qualified
      key <- Seq("category", "g.category")
      sql <- Seq(
        s"SELECT $key AS k, $metrics FROM $venue GROUP BY $key HAVING MATCH ($key) AGAINST ('cat_37') OR COUNT(*) > 35",
        s"SELECT $metrics FROM $venue GROUP BY $key HAVING MATCH ($key) AGAINST ('cat_37') OR COUNT(*) > 35"
      )
    } yield refused(sql, "different mechanisms", metricTriples(sql))) ++ Seq(
      // Two functions of one column keeping their own filters, bare and qualified
      row(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING MONTH(d) = 1",
        Right(Seq((3L, 3L, 6L), (4L, 10L, 34L))),
        metricTriples(s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING MONTH(d) = 1")
      ),
      row(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING MONTH(q.d) = 1",
        Right(Seq((3L, 3L, 6L), (4L, 10L, 34L))),
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING MONTH(q.d) = 1"
        )
      ),
      // The AND of the two levels, each half on its own level (refused until now)
      row(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 AND MONTH(d) = 1",
        Right(Seq((3L, 3L, 6L))),
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 AND MONTH(d) = 1"
        )
      ),
      row(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING YEAR(q.d) = 2024 AND MONTH(q.d) = 1",
        Right(Seq((3L, 3L, 6L))),
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING YEAR(q.d) = 2024 AND MONTH(q.d) = 1"
        )
      ),
      // An OR across the two levels, bare and qualified (answered until now as the AND)
      refused(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 OR MONTH(d) = 1",
        "DIFFERENT GROUP BY keys",
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY YEAR(d), MONTH(d) HAVING YEAR(d) = 2024 OR MONTH(d) = 1"
        )
      ),
      refused(
        s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING YEAR(q.d) = 2024 OR MONTH(q.d) = 1",
        "DIFFERENT GROUP BY keys",
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY YEAR(q.d), MONTH(q.d) HAVING YEAR(q.d) = 2024 OR MONTH(q.d) = 1"
        )
      ),
      // A DATE_TRUNC key filter refused at every venue, bare and qualified; a DATE key at
      // this venue is a terms level on a date field -- its filter applies (the one date of count 1)
      refused(
        s"SELECT $metrics FROM $dates GROUP BY DATE_TRUNC(d, MONTH) HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'",
        "HAVING cannot filter a DATE grouping level",
        metricTriples(
          s"SELECT $metrics FROM $dates GROUP BY DATE_TRUNC(d, MONTH) HAVING DATE_TRUNC(d, MONTH) = '2024-01-01'"
        )
      ),
      refused(
        s"SELECT COUNT(*) AS c FROM $dates GROUP BY DATE_TRUNC(q.d, MONTH) HAVING DATE_TRUNC(q.d, MONTH) = '2024-01-01'",
        "HAVING cannot filter a DATE grouping level",
        dateGroups(
          s"SELECT COUNT(*) AS c FROM $dates GROUP BY DATE_TRUNC(q.d, MONTH) HAVING DATE_TRUNC(q.d, MONTH) = '2024-01-01'"
        )
      ),
      row(
        s"SELECT COUNT(*) AS c FROM $dates GROUP BY q.d HAVING q.d = '2024-01-03'",
        Right(Seq(1L)),
        dateGroups(s"SELECT COUNT(*) AS c FROM $dates GROUP BY q.d HAVING q.d = '2024-01-03'")
      ),
      row(
        s"SELECT COUNT(*) AS c FROM $dates GROUP BY d HAVING d = '2024-01-03'",
        Right(Seq(1L)),
        dateGroups(s"SELECT COUNT(*) AS c FROM $dates GROUP BY d HAVING d = '2024-01-03'")
      ),
      row(
        s"SELECT COUNT(*) AS c FROM $dates GROUP BY d",
        Right(Seq(1L, 2L, 3L, 4L)),
        dateGroups(s"SELECT COUNT(*) AS c FROM $dates GROUP BY d")
      )
    ) ++ (
      // a temporal WHERE comparison of a GROUP BY key column, qualified or bare (Elasticsearch 7 and
      // up, which hand a script a genuine `ZonedDateTime`)
      if (esMajor < 7) Nil
      else
        im10Rows(im10OneResolution).map { case (sql, expected) =>
          row(sql, Right(expected), dateGroups(sql))
        }
    )
    val after = witnesses
    val elapsed = System.currentTimeMillis() - started
    val wrong = matrixWrong ++ named.flatten
    withClue(s"the schema miss did not stand for every row: before $before, after $after") {
      (before ++ after).forall(_._2) shouldBe true
    }
    elapsed should be < SchemaCacheSettings.DefaultTtlMs
    withClue(s"${wrong.size} wrong ($matrixSize matrix cells):\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  // ---- GROUP BY over a date column, or over a function of a column ----------------------------
  //
  // A level is a `date_histogram` only for a `DATE_TRUNC` key; every other key is a `terms` level
  // -- a bare column on its field, a function of a column on its script. Before, the level followed
  // the COLUMN's type: with the mapping attached (a concrete index, and since #412 an index pattern
  // or list as well) every key over a date column was a `date_histogram` with no interval, which
  // Elasticsearch refuses (HTTP 400). And a function that does not script itself (`ISNULL`,
  // `WEEKDAY`, `DATE_FORMAT`) was a `terms` on the bare FIELD: one group per raw value, HTTP 200.
  //
  // Every expected value is computed HERE, from the fixture, as DuckDB 1.5.5 answers it (MEASURED).
  // Keys are compared by a class-agnostic normalised value: a numeric script key reads back as text
  // today, and a DATE column's key as a timestamp at midnight -- the values are what is asserted.
  // No row orders by a numeric script key (it sorts as text) and no row asserts an ORDER BY on an
  // inner key or a LIMIT over several keys (each level is ordered and sized inside its parent).

  private val dateKeyIndex = "group_by_date_keys"

  /** A second index with the same mapping and no documents: the index list's second member. */
  private val dateKeyEmptyIndex = "group_by_date_keys_empty"

  private val dateKeyPattern = "group_by_date_key*"

  private val dateKeyList = s"$dateKeyIndex,$dateKeyEmptyIndex"

  /** (id, d, ts, n, s) */
  private val dateKeyDocs: Seq[(String, LocalDate, java.time.Instant, Int, String)] = Seq(
    ("r1", "2024-01-03", "2024-01-03T10:15:00Z", 1, "a"),
    ("r2", "2024-01-03", "2024-01-03T10:45:00Z", -1, "b"),
    ("r3", "2024-01-20", "2024-01-20T23:59:59Z", 2, "a"),
    ("r4", "2024-02-05", "2024-02-05T00:00:01Z", -2, "b"),
    ("r5", "2024-03-11", "2024-03-11T10:15:00Z", 3, "A"),
    ("r6", "2024-03-11", "2025-03-11T11:00:00Z", 3, "A"),
    ("r7", "2025-03-31", "2025-03-31T10:20:00Z", 5, "bb")
  ).map { case (id, d, ts, n, s) => (id, LocalDate.parse(d), java.time.Instant.parse(ts), n, s) }

  private lazy val dateKeysLoaded: Unit = {
    val settings = """{"number_of_shards": 3, "number_of_replicas": 0}"""
    val mapping =
      """{"properties":{"id":{"type":"keyword"},"d":{"type":"date"},"ts":{"type":"date"},""" +
      """"n":{"type":"integer"},"s":{"type":"keyword"}}}"""
    Seq(dateKeyIndex, dateKeyEmptyIndex).foreach { target =>
      client.createIndex(target, settings = settings).get shouldBe true
      client.setMapping(target, mapping).get shouldBe true
    }
    val docs = dateKeyDocs.map { case (id, d, ts, n, s) =>
      s"""{"id":"$id","d":"$d","ts":"$ts","n":$n,"s":"$s"}"""
    }.toList
    implicit val dateKeyBulkOptions: BulkOptions =
      BulkOptions(defaultIndex = dateKeyIndex, logEvery = 1000)
    implicit def dateKeyListToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) =>
        client.refresh(dateKeyIndex)
        client.refresh(dateKeyEmptyIndex)
      case ElasticFailure(error) => fail(s"Bulk indexing $dateKeyIndex failed: ${error.message}")
    }
  }

  /** A value as the oracle and the client answer are compared, whatever class the client reads it
    * as: a number as one spelling, a temporal at UTC (its date alone at midnight).
    */
  private def dateKeyText(v: Any): String = {
    def local(t: java.time.LocalDateTime): String =
      if (t.toLocalTime == java.time.LocalTime.MIDNIGHT) t.toLocalDate.toString else t.toString
    v match {
      case null => "<null>"
      case z: java.time.ZonedDateTime =>
        local(z.withZoneSameInstant(java.time.ZoneOffset.UTC).toLocalDateTime)
      case o: java.time.OffsetDateTime =>
        local(o.atZoneSameInstant(java.time.ZoneOffset.UTC).toLocalDateTime)
      case i: java.time.Instant       => local(i.atZone(java.time.ZoneOffset.UTC).toLocalDateTime)
      case t: java.time.LocalDateTime => local(t)
      case d: LocalDate               => d.toString
      case n: java.lang.Number        => numericText(n)
      case Seq(one)                   => dateKeyText(one) // the row path's computed-column wrap
      case other                      => Try(numericText(other)).getOrElse(other.toString)
    }
  }

  private def utc(i: java.time.Instant): String = dateKeyText(i)

  /** The rows a statement answers, each its `columns` in order, sorted; `Left` is a failure. */
  private def dateKeyRows(sql: String, columns: Seq[String]): Either[String, Seq[Seq[String]]] = {
    dateKeysLoaded
    gatewayRows(sql).map(
      _.map(row => columns.map(c => row.get(c).map(dateKeyText).getOrElse("<absent>")))
        .sortBy(_.mkString("|"))
    )
  }

  /** The oracle: the documents grouped by `key`, each group with its document count. */
  private def countsOf(
    key: ((String, LocalDate, java.time.Instant, Int, String)) => Seq[String]
  ): Seq[Seq[String]] =
    dateKeyDocs
      .groupBy(key)
      .map { case (k, docs) => k :+ docs.size.toString }
      .toSeq
      .sortBy(_.mkString("|"))

  private def d(doc: (String, LocalDate, java.time.Instant, Int, String)): String = doc._2.toString

  /** The statements whose answer is the same at every venue, with their oracle. */
  private def dateKeyStatements(venue: String): Seq[(String, Seq[String], Seq[Seq[String]])] =
    Seq(
      (
        s"SELECT d, COUNT(*) AS c FROM $venue GROUP BY d",
        Seq("d", "c"),
        countsOf(x => Seq(d(x)))
      ),
      (
        s"SELECT ts, COUNT(*) AS c FROM $venue GROUP BY ts",
        Seq("ts", "c"),
        countsOf(x => Seq(utc(x._3)))
      ),
      (
        s"SELECT YEAR(d) AS k, COUNT(*) AS c FROM $venue GROUP BY YEAR(d)",
        Seq("k", "c"),
        countsOf(x => Seq(x._2.getYear.toString))
      ),
      (
        s"SELECT MONTH(d) AS k, COUNT(*) AS c FROM $venue GROUP BY MONTH(d)",
        Seq("k", "c"),
        countsOf(x => Seq(x._2.getMonthValue.toString))
      ),
      (
        s"SELECT s, d, COUNT(*) AS c FROM $venue GROUP BY s, d",
        Seq("s", "d", "c"),
        countsOf(x => Seq(x._5, d(x)))
      )
    )

  private def wrongDateKeyRows(
    statements: Seq[(String, Seq[String], Seq[Seq[String]])]
  ): Seq[String] =
    statements.flatMap { case (sql, columns, expected) =>
      val actual = dateKeyRows(sql, columns)
      if (actual == Right(expected)) None
      else Some(s"[$sql] expected $expected, got ${actual.toString.take(400)}")
    }

  "a GROUP BY over a date column or a function of one, FROM an index pattern or an index list" should
  "answer as 0.23.0 did" in {
    // Since #412 a pattern or a list resolves its merged mapping, and these statements failed with
    // HTTP 400 there ("Invalid interval specified" on Elasticsearch 8, "Zero or negative time
    // interval not supported" on 6.8), where 0.23.0 answered them.
    val wrong =
      Seq(dateKeyPattern, dateKeyList).flatMap(v => wrongDateKeyRows(dateKeyStatements(v)))
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  "a GROUP BY over a date column or a function of one, FROM a concrete index" should
  "answer instead of failing with HTTP 400" in {
    val v = dateKeyIndex
    val extra = Seq(
      (
        s"SELECT DAY(d) AS k, COUNT(*) AS c FROM $v GROUP BY DAY(d)",
        Seq("k", "c"),
        countsOf(x => Seq(x._2.getDayOfMonth.toString))
      ),
      (
        s"SELECT YEAR(ts) AS k, COUNT(*) AS c FROM $v GROUP BY YEAR(ts)",
        Seq("k", "c"),
        countsOf(x => Seq(x._3.atZone(java.time.ZoneOffset.UTC).getYear.toString))
      ),
      (
        s"SELECT LAST_DAY(d) AS k, COUNT(*) AS c FROM $v GROUP BY LAST_DAY(d)",
        Seq("k", "c"),
        countsOf(x => Seq(x._2.withDayOfMonth(x._2.lengthOfMonth).toString))
      ),
      (
        s"SELECT d, s, COUNT(*) AS c FROM $v GROUP BY d, s",
        Seq("d", "s", "c"),
        countsOf(x => Seq(d(x), x._5))
      ),
      (
        s"SELECT d, MIN(n) AS mn FROM $v GROUP BY d",
        Seq("d", "mn"),
        dateKeyDocs
          .groupBy(d)
          .map { case (k, docs) => Seq(k, docs.map(_._4).min.toString) }
          .toSeq
          .sortBy(_.mkString("|"))
      ),
      (
        s"SELECT d, COUNT(*) AS c FROM $v GROUP BY d HAVING COUNT(*) > 1",
        Seq("d", "c"),
        countsOf(x => Seq(d(x))).filter(_.last.toInt > 1)
      )
    )
    val wrong = wrongDateKeyRows(dateKeyStatements(v) ++ extra)
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  "a GROUP BY over a function of a column" should
  "group by the function's value, never by the column's raw values" in {
    // These functions do not script themselves, and the level used to be a `terms` on the bare
    // field: `GROUP BY ISNULL(s)` answered one group per value of `s`, with HTTP 200.
    val statements = Seq(dateKeyIndex, dateKeyPattern).flatMap { v =>
      Seq(
        (
          s"SELECT ISNULL(s) AS k, COUNT(*) AS c FROM $v GROUP BY ISNULL(s)",
          Seq("k", "c"),
          countsOf(_ => Seq("false"))
        ),
        (
          s"SELECT ISNULL(n) AS k, COUNT(*) AS c FROM $v GROUP BY ISNULL(n)",
          Seq("k", "c"),
          countsOf(_ => Seq("false"))
        )
      ) ++ (if (esMajor >= 7)
              Seq(
                (
                  s"SELECT DATE_FORMAT(d, '%Y-%m') AS k, COUNT(*) AS c FROM $v " +
                  "GROUP BY DATE_FORMAT(d, '%Y-%m')",
                  Seq("k", "c"),
                  countsOf(x => Seq(f"${x._2.getYear}%04d-${x._2.getMonthValue}%02d"))
                )
              )
            else Nil)
    }
    val wrong = wrongDateKeyRows(statements)
    // WEEKDAY: the groups are the function's own values, as its SELECT answers them per document
    // (the numbering is not this row's subject).
    val weekday = Seq(dateKeyIndex, dateKeyPattern).flatMap { v =>
      val perDocument = dateKeyRows(s"SELECT id, WEEKDAY(d) AS w FROM $v", Seq("id", "w"))
      val expected = perDocument.map(
        _.groupBy(_(1))
          .map { case (w, docs) => Seq(w, docs.size.toString) }
          .toSeq
          .sortBy(_.mkString("|"))
      )
      val sql = s"SELECT WEEKDAY(d) AS k, COUNT(*) AS c FROM $v GROUP BY WEEKDAY(d)"
      val actual = dateKeyRows(sql, Seq("k", "c"))
      if (expected.isRight && actual == expected && actual.exists(_.size == 3)) None
      else Some(s"[$sql] expected $expected (3 weekdays), got $actual")
    }
    val all = wrong ++ weekday
    withClue(s"${all.size} wrong:\n${all.mkString("\n")}\n") { all shouldBe empty }
  }

  "a HAVING key filter on a GROUP BY key whose script returns a date" should
  "be refused by name, and the same condition in WHERE answer" in {
    // Such a level is a `terms` aggregation on the key's script, and Elasticsearch compares the
    // filter with the text the script's date prints ("2024-01-31T00:00Z"), which a date literal
    // does not match: `=` / `IN` answered no group and `<>` every group, HTTP 200.
    def lastDay(x: (String, LocalDate, java.time.Instant, Int, String)): String =
      x._2.withDayOfMonth(x._2.lengthOfMonth).toString
    val groups = countsOf(x => Seq(lastDay(x)))
    val outcomes = Seq(dateKeyIndex, dateKeyPattern).flatMap { v =>
      Seq(
        ("= '2024-01-31'", groups.filter(_.head == "2024-01-31")),
        (
          "IN ('2024-01-31', '2024-03-31')",
          groups.filter(g => Set("2024-01-31", "2024-03-31")(g.head))
        ),
        ("<> '2024-01-31'", groups.filterNot(_.head == "2024-01-31"))
      ).flatMap { case (condition, expected) =>
        val having =
          s"SELECT LAST_DAY(d) AS k, COUNT(*) AS c FROM $v GROUP BY LAST_DAY(d) " +
          s"HAVING LAST_DAY(d) $condition"
        val where =
          s"SELECT LAST_DAY(d) AS k, COUNT(*) AS c FROM $v WHERE LAST_DAY(d) $condition " +
          "GROUP BY LAST_DAY(d)"
        val refused = dateKeyRows(having, Seq("k", "c")) match {
          case Left(message)
              if message.contains("HAVING cannot filter a DATE grouping level") &&
                message.contains("with the same condition on LAST_DAY(d).") =>
            None
          case other => Some(s"[$having] expected the refusal, got ${other.toString.take(400)}")
        }
        val remedied = dateKeyRows(where, Seq("k", "c")) match {
          case Right(rows) if rows == expected => None
          case other                           => Some(s"[$where] expected $expected, got $other")
        }
        refused.toSeq ++ remedied
      }
    }
    withClue(s"${outcomes.size} wrong:\n${outcomes.mkString("\n")}\n") { outcomes shouldBe empty }
  }

  "a GROUP BY key formatting a date column as stored" should
  "be refused by name on Elasticsearch 6, and its remedy answer on every major" in {
    // Elasticsearch 6.8 cannot run such a script (MEASURED): it receives the column's value as a
    // JodaCompatibleZonedDateTime there, which the formatter cannot read, and every shard holding a
    // document fails. The refusal names the column before the query is sent, at an outer level and
    // at an inner one alike.
    def month(i: java.time.Instant): String = {
      val z = i.atZone(java.time.ZoneOffset.UTC)
      f"${z.getYear}%04d-${z.getMonthValue}%02d"
    }
    val byDate = countsOf(x => Seq(f"${x._2.getYear}%04d-${x._2.getMonthValue}%02d"))
    val byTimestamp = countsOf(x => Seq(month(x._3)))
    val cases = Seq(
      // (key, column, its remedy, the groups)
      ("DATE_FORMAT(d, '%Y-%m')", "d", "DATE_FORMAT(DATE_TRUNC(d, DAY), '%Y-%m')", byDate),
      (
        "DATETIME_FORMAT(ts, '%Y-%m')",
        "ts",
        "DATETIME_FORMAT(DATE_TRUNC(ts, SECOND), '%Y-%m')",
        byTimestamp
      )
    )
    val outcomes = for {
      v                             <- Seq(dateKeyIndex, dateKeyPattern)
      (key, column, remedy, groups) <- cases
      outcome <- {
        val formatted = s"SELECT $key AS k, COUNT(*) AS c FROM $v GROUP BY $key"
        val remedied = s"SELECT $remedy AS k, COUNT(*) AS c FROM $v GROUP BY $remedy"
        val formattedOutcome = dateKeyRows(formatted, Seq("k", "c")) match {
          case Left(message)
              if esMajor < 7 && message.contains(s"Elasticsearch $esMajor") &&
                message.contains(s"'$column'") && message.contains("DATE_TRUNC(") =>
            None
          case Right(rows) if esMajor >= 7 && rows == groups => None
          case other => Some(s"[$formatted] on Elasticsearch $esMajor: ${other.toString.take(400)}")
        }
        val remedyOutcome = dateKeyRows(remedied, Seq("k", "c")) match {
          case Right(rows) if rows == groups => None
          case other                         => Some(s"[$remedied] expected $groups, got $other")
        }
        formattedOutcome.toSeq ++ remedyOutcome
      }
    } yield outcome
    // the same key at an INNER level, beside a keyword key
    val inner = Seq(dateKeyIndex, dateKeyPattern).flatMap { v =>
      val sql =
        s"SELECT s, DATE_FORMAT(d, '%Y-%m') AS k, COUNT(*) AS c FROM $v " +
        "GROUP BY s, DATE_FORMAT(d, '%Y-%m')"
      val groups = countsOf(x => Seq(x._5, f"${x._2.getYear}%04d-${x._2.getMonthValue}%02d"))
      dateKeyRows(sql, Seq("s", "k", "c")) match {
        case Left(message)
            if esMajor < 7 && message.contains(s"Elasticsearch $esMajor") &&
              message.contains("'d'") =>
          None
        case Right(rows) if esMajor >= 7 && rows == groups => None
        case other => Some(s"[$sql] on Elasticsearch $esMajor: ${other.toString.take(400)}")
      }
    }
    // a function reading the column first is no raw doc value: answered on every major
    val cast = Seq(dateKeyIndex, dateKeyPattern).flatMap { v =>
      val key = "DATE_FORMAT(CAST(d AS DATE), '%Y-%m')"
      val sql = s"SELECT $key AS k, COUNT(*) AS c FROM $v GROUP BY $key"
      dateKeyRows(sql, Seq("k", "c")) match {
        case Right(rows) if rows == byDate => None
        case other                         => Some(s"[$sql] expected $byDate, got $other")
      }
    }
    val all = outcomes ++ inner ++ cast
    withClue(s"${all.size} wrong:\n${all.mkString("\n")}\n") { all shouldBe empty }
  }

}
