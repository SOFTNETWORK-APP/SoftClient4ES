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
  ElasticResult,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

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
    // ⚠️ Both lists carry SEVERAL values on purpose. RESIDUAL, pre-existing and ES-6-ONLY, found
    // by this matrix: the es6 bridge renders a SINGLE-element list as a bare string
    // (`"exclude":"c"`), which ES 6.8 reads as a REGEX, and mixing a set-based include with a
    // regex-based exclude is rejected with HTTP 400 `Cannot mix a set-based include with a
    // regex-based method`. The emission is byte-identical to `455433ae`; it is recorded, not fixed
    // here.
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
}
