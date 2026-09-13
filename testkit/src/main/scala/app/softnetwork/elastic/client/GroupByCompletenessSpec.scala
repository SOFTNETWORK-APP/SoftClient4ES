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
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
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

  /** Story 21.6, lead ruling 1 -- `HAVING` with NO `GROUP BY`: the whole table is one implicit
    * group.
    *
    * Four of the 99 captured BI statements are this shape, verbatim `SELECT SUM(1) AS `COL` FROM
    * `elastic`.`bi_events` `bi_events` HAVING COUNT(1)>0` (`tableau.mysql.w1.023`, `w7.048`,
    * `w7.051`, `tableau.sql92.wx.015`). They are scored as Epic 21 fixes, and the lead ruled that
    * the score must be EARNED by a correctness test rather than taken on the parse verdict: before
    * this test, a repo-wide search found `HAVING` covered only in combination with a `GROUP BY`, so
    * nothing anywhere answered what this shape returns.
    *
    * 🔴 THE SECOND HALF IS THE WHOLE POINT. A `HAVING` that is silently DISCARDED still satisfies
    * the true-predicate case -- the aggregate is correct either way -- so asserting only `COUNT(1)
    * > 0` would be a test that cannot fail for the reason it exists. The false-predicate case is
    * what distinguishes "the filter was applied" from "the filter was dropped", which is the
    * #205/#209/#224/#253 silent-wrong-answer family this whole story is built to catch. The two
    * halves are asserted as a PAIR and neither is meaningful alone.
    *
    * Asserted on the RAW row via `client.search`, not through `searchAs`, for the reason the four
    * constant-projection tests above already give: the macro types a bare integer literal as BIGINT
    * while the value arrives as Jackson's smallest type, so `SUM(1)` over a literal cannot be bound
    * to a case-class field in either spelling. That mismatch is pre-existing and recorded
    * separately; it must not be allowed to decide whether the HAVING works.
    */
  // 🔴 PINS TWO KNOWN DEFECTS, not a contract -- delete this test when they are fixed.
  //
  // Story 21.6, lead ruling 1. Four of the 99 captured BI statements are `HAVING` with NO `GROUP BY`,
  // verbatim `SELECT SUM(1) AS `COL` FROM `elastic`.`bi_events` `bi_events` HAVING COUNT(1)>0`
  // (tableau.mysql.w1.023, w7.048, w7.051, tableau.sql92.wx.015). They PARSE after Epic 21, and the
  // lead required the `scored = fixed` claim to be EARNED by a correctness test rather than taken on
  // the parse verdict. It was not earned: the test MEASURED two defects, so the four rows are scored
  // `residual` and the published headline is 56/99, not 60/99.
  //
  // 🔴 DEFECT 1 -- the corpus shape ERRORS on every client. `COUNT(<literal>)` emits an aggregation
  // with neither `field` nor `script`, which Elasticsearch rejects outright
  // (illegal_argument_exception, "Required one of fields [field, script]"). Measured independent of
  // the SELECT list: `SELECT SUM(amount) ... HAVING COUNT(1) > 0` fails identically, so the blocker is
  // `COUNT(1)` itself. Pre-existing, and already recorded by story 21.3 as a known defect; Epic 21
  // only made the statements that carry it reach execution.
  //
  // 🔴 DEFECT 2 -- and this is the dangerous one, found only because the lead mandated a FALSIFIABLE
  // PAIR rather than a happy path: with a field-bearing aggregate the statement succeeds and the
  // `HAVING` is SILENTLY DISCARDED. A whole-table aggregate has no buckets, so there is nothing for a
  // `bucket_selector` to select, and the predicate evaporates with HTTP 200. Measured on ES 8.18:
  //   SELECT COUNT(*) AS COL ... HAVING COUNT(*)  > 10000  => 703   (expected: NO rows)
  //   SELECT SUM(amount) AS COL ... HAVING SUM(amount) > 99999  => 9139  (expected: NO rows)
  // Both predicates are FALSE and both returned the unfiltered aggregate. That is the
  // #205/#209/#224/#253 silent-wrong-answer family, on a shape the corpus samples four times.
  //
  // The assertions below pin what the engine DOES, with the correct answer named beside each. The
  // true-predicate halves are deliberately kept: they are what makes the pair evidence rather than a
  // single observation -- on their own they pass whether or not the predicate is honoured, which is
  // exactly why a happy-path-only test would have certified this shape as working.
  "corpus shape: HAVING with no GROUP BY" should
  "fail on COUNT(<literal>) -- defect 1, the corpus spelling" in {
    implicit val havingCtx: ConversionContext = NativeContext
    // The corpus spelling, quoted and qualified as Tableau emits it. Asserted as a FAILURE, without
    // pinning the message text (a rejection message is never a contract): only that it does not
    // succeed, plus the message in the clue for the next reader.
    val result = client.search(
      SelectStatement(
        "SELECT SUM(1) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(1) > 0"
      )
    )
    withClue(
      s"the corpus HAVING shape now SUCCEEDS -- defect 1 is fixed, delete this pin: $result: "
    ) {
      result.isSuccess shouldBe false
    }
  }

  it should "silently DISCARD the predicate when the aggregate is field-bearing -- defect 2" in {
    implicit val havingCtx: ConversionContext = NativeContext

    // (a) TRUE predicate: one row, the known fixture total. Correct -- and note it would be correct
    // even with the predicate dropped, which is the point of pairing it with (b).
    client.search(
      SelectStatement(
        "SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(*) > 0"
      )
    ) match {
      case ElasticSuccess(response) =>
        withClue(s"true predicate, rows=${response.results}: ") {
          response.results should have size 1L
          response.results.head("COL").toString.toDouble shouldBe totalDocs.toDouble
        }
      case ElasticFailure(error) => fail(s"true predicate failed: ${error.message}")
    }

    // (b) FALSE predicate: the whole table holds exactly `totalDocs` (703) documents, so `> 10000` is
    // unsatisfiable BY CONSTRUCTION and standard SQL returns NO row. The engine returns the
    // unfiltered aggregate instead. `shouldBe` the WRONG value, because that is what is true today.
    client.search(
      SelectStatement(
        "SELECT COUNT(*) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING COUNT(*) > 10000"
      )
    ) match {
      case ElasticSuccess(response) =>
        withClue(
          "the FALSE predicate is now honoured -- defect 2 is fixed and this pin must be replaced by " +
          s"`response.results shouldBe empty`: rows=${response.results}: "
        ) {
          response.results should have size 1L
          response.results.head("COL").toString.toDouble shouldBe totalDocs.toDouble
        }
      case ElasticFailure(error) => fail(s"false predicate failed: ${error.message}")
    }

    // (c) the same, over a metric rather than a count, so the pin is not an artefact of COUNT(*):
    // SUM(amount) is 9139 and `> 99999` is unsatisfiable.
    client.search(
      SelectStatement(
        "SELECT SUM(amount) AS \"COL\" FROM \"group_by_completeness\" \"g\" HAVING SUM(amount) > 99999"
      )
    ) match {
      case ElasticSuccess(response) =>
        withClue(
          "the FALSE metric predicate is now honoured -- defect 2 is fixed, replace this pin with " +
          s"`response.results shouldBe empty`: rows=${response.results}: "
        ) {
          response.results should have size 1L
        }
      case ElasticFailure(error) => fail(s"false metric predicate failed: ${error.message}")
    }
  }
}
