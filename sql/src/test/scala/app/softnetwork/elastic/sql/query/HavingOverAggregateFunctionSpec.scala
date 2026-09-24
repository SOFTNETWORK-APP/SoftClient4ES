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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.OptionValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #389 -- a `HAVING` over a FUNCTION of an aggregate must FILTER, or FAIL, never vanish.
  *
  * This file measures the `sql` half: the DETECTION (does the engine see the aggregate hidden in a
  * function argument, a `CASE` branch, or the right-hand operand?), the REPRESENTABILITY gate (is
  * the rendering a single boolean expression?) and the REFUSAL each un-expressible shape earns. The
  * emission itself is pinned on the GENERATED query, in the bridge suites.
  *
  * 🔴 Every assertion here is at the PARSE/RENDER level. `Parser.apply` runs `update()` and then
  * `validate()`, so a `Left` here is what every venue sees -- REPL, JDBC, Flight, the
  * materialized-view extension and the bridge's own `SelectStatement(sql).query` alike.
  */
class HavingOverAggregateFunctionSpec extends AnyFlatSpec with Matchers with OptionValues {

  private def parsed(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def rejection(sql: String): String = Parser(sql) match {
    case Left(e)  => e.msg
    case Right(s) => fail(s"[$sql] expected a rejection, got $s")
  }

  /** The AST of a statement `validate()` may REFUSE -- `Parser.single` runs `update()` inside its
    * own combinator action, so everything the detection reads is populated exactly as `apply` would
    * have populated it. Needed because half the shapes below are (correctly) rejected.
    */
  private def unvalidated(sql: String): SingleSearch = Parser.parseUnvalidated(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def having(sql: String): Criteria =
    parsed(sql).having.flatMap(_.criteria).getOrElse(fail(s"[$sql] has no HAVING criteria"))

  private def havingOf(s: SingleSearch, sql: String): Criteria =
    s.having.flatMap(_.criteria).getOrElse(fail(s"[$sql] has no HAVING criteria"))

  private def script(sql: String): String =
    MetricSelectorScript.metricSelector(having(sql))

  private val group = "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING "

  // -------------------------------------------------------------------------------------------
  // Detection -- `Identifier.bucketMetrics` is the ONE derivation behind the selector, the
  // aggregations that get created, and the `buckets_path` that publishes them.
  // -------------------------------------------------------------------------------------------

  private def metricKeys(sql: String): Seq[String] =
    havingOf(unvalidated(sql), sql).referencedIdentifiers
      .flatMap(_.bucketMetrics)
      .flatMap(_.metricName)
      .distinct

  "the aggregate inside a function argument" should "be seen through every wrapper" in {
    // Measured on `main`: every one of these answered `hasAggregation = false` and the whole
    // condition degenerated to `1 == 1`.
    metricKeys(group + "ABS(COUNT(*)) > 1") shouldBe Seq("c")
    metricKeys(group + "COALESCE(COUNT(*), 0) > 1") shouldBe Seq("c")
    metricKeys(group + "NULLIF(COUNT(*), 0) > 1") shouldBe Seq("c")
    metricKeys(group + "GREATEST(COUNT(*), 0) > 1") shouldBe Seq("c")
    metricKeys(group + "FLOOR(COUNT(*)) > 1") shouldBe Seq("c")
    metricKeys(group + "COALESCE(NULLIF(COUNT(*), 0), 5) > 1") shouldBe Seq("c")
  }

  it should "be seen in a CASE branch and in a CASE condition" in {
    // 🔴 The two live in DIFFERENT places and no single walk reaches both: `Case.args` excludes the
    // WHEN conditions on purpose, so `funIdentifiers` finds the THEN result and misses the
    // condition. Measured: the condition form yielded NO aggregate from `funIdentifiers` alone.
    metricKeys(group + "CASE WHEN status = 'a' THEN COUNT(*) ELSE 0 END = 1") shouldBe Seq("c")
    metricKeys(group + "CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1") shouldBe Seq("c")
  }

  it should "be seen on the RIGHT operand of the comparison" in {
    // `1 < ABS(COUNT(*))` reads the metric through the VALUE, not the identifier.
    metricKeys(group + "1 < ABS(COUNT(*))") shouldBe Seq("c")
    metricKeys(
      "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING COUNT(*) > ABS(MAX(x))"
    ) shouldBe Seq("c", "max_x")
  }

  "a predicate that references no aggregate" should "still contribute no metric" in {
    metricKeys(group + "status = 'A'") shouldBe empty
    metricKeys(group + "UPPER(status) = 'A'") shouldBe empty
  }

  "a SELECT bucket_script referenced by its alias" should "publish the alias, never its operands" in {
    // The control the third arm must not swallow: the selector reads the sibling pipeline
    // aggregation `d` by name; publishing `max_x` / `min_x` here would change a shipped query.
    val sql = "SELECT status, MAX(x) - MIN(x) AS d FROM t GROUP BY status HAVING d > 3"
    having(sql).extractAllMetricsPath shouldBe Map("d" -> "d")
    script(sql) shouldBe "(params.d == null ? false : (params.d > 3))"
  }

  // -------------------------------------------------------------------------------------------
  // Emission -- what the selector renders once the aggregate is seen
  //
  // 🔴 The emittable population is the one that MEASURABLY RUNS. `GREATEST` / `LEAST` / `COALESCE`
  // emit; `ABS` and the rest of the boxing family are REFUSED, because their rendering does not
  // compile in the bucket-pipeline script context -- see the disqualifier tests below.
  // -------------------------------------------------------------------------------------------

  "a null-PROPAGATING function of an aggregate" should "render guarded, exactly as a bare aggregate does" in {
    // SQL says `GREATEST(NULL, 0) > 1` is UNKNOWN, so the group is excluded -- which is the `false`
    // this guard supplies. Without it `Math.max(null, 0)` fails the whole search.
    script(group + "GREATEST(COUNT(*), 0) > 1") shouldBe
    "(params.c == null ? false : (Math.max(params.c, 0) > 1))"
    script(group + "LEAST(COUNT(*), 99) > 1") shouldBe
    "(params.c == null ? false : (Math.min(params.c, 99) > 1))"
    script(group + "1 < GREATEST(COUNT(*), 0)") shouldBe
    "(params.c == null ? false : (1 < Math.max(params.c, 0)))"
  }

  "a null-ABSORBING function of an aggregate" should "render UNguarded" in {
    // 🔴 `COALESCE` exists precisely to decide what a null means. Forcing `false` on it would make
    // `COALESCE(MAX(x), 99) > 1` answer `false` where SQL says `true`.
    script(group + "COALESCE(COUNT(*), 0) > 1") shouldBe "(params.c != null ? params.c : 0) > 1"
  }

  "BETWEEN and IN over a function of an aggregate" should "render as one guarded boolean" in {
    script(group + "GREATEST(COUNT(*), 0) BETWEEN 1 AND 5") shouldBe
    "(params.c == null ? false : ((Math.max(params.c, 0) >= 1 && Math.max(params.c, 0) <= 5)))"
    script(group + "GREATEST(COUNT(*), 0) IN (1, 2)") shouldBe
    "(params.c == null ? false : ((Math.max(params.c, 0) == 1 || Math.max(params.c, 0) == 2)))"
  }

  "NOT over a function of an aggregate" should "push the negation into the comparison" in {
    script(group + "NOT GREATEST(COUNT(*), 0) > 1") shouldBe
    "(params.c == null ? false : (Math.max(params.c, 0) <= 1))"
  }

  "a conjunction of two expressible predicates" should "emit BOTH" in {
    // 🔴 On `main` the second conjunct was DROPPED and the answer looked filtered while being
    // wrong -- a partial filter is a wrong answer, not a lesser fix.
    script(group + "COUNT(*) > 1 AND GREATEST(COUNT(*), 0) > 2") shouldBe
    "(params.c == null ? false : (params.c > 1)) && " +
    "(params.c == null ? false : (Math.max(params.c, 0) > 2))"
  }

  "a disjunction of two expressible predicates" should "emit BOTH" in {
    // The OR form is WORSE than the AND form when a branch is dropped: the surviving filter is
    // STRICTER than what was written, so rows silently disappear.
    script(group + "GREATEST(COUNT(*), 0) > 1 OR COUNT(*) > 5") shouldBe
    "(params.c == null ? false : (Math.max(params.c, 0) > 1)) || " +
    "(params.c == null ? false : (params.c > 5))"
  }

  "an aggregate reached only through a HAVING function" should "be created and published" in {
    // Issue #389 AC-5: with no SELECT alias there was no aggregation for the script to read.
    val sql = "SELECT status FROM t GROUP BY status HAVING GREATEST(COUNT(*), 0) > 1"
    parsed(sql).sqlAggregations.keys.toList shouldBe List("count_all")
    having(sql).extractAllMetricsPath shouldBe Map("count_all" -> "count_all")
  }

  "an aggregate reached only through the RIGHT operand" should "be created and guarded" in {
    // Measured on `main`: the script read `params.max_x`, `buckets_path` declared only `c`, and NO
    // `max_x` aggregation existed anywhere.
    val sql =
      "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING COUNT(*) > GREATEST(MAX(x), 0)"
    parsed(sql).sqlAggregations.keys.toList shouldBe List("c", "max_x")
    having(sql).extractAllMetricsPath shouldBe Map("c" -> "c", "max_x" -> "max_x")
    script(sql) shouldBe
    "(params.c == null || params.max_x == null ? false : " +
    "(params.c > Math.max(params.max_x, 0)))"
  }

  // -------------------------------------------------------------------------------------------
  // The representability gate -- one refusal per disqualifier, each naming its own reason
  // -------------------------------------------------------------------------------------------

  "a rendering that can evaluate to NULL" should "be refused, naming the boolean requirement" in {
    val msg = rejection(group + "NULLIF(COUNT(*), 0) > 1")
    msg should include("HAVING cannot be applied to NULLIF(COUNT(*), 0) > 1")
    msg should include("can evaluate to NULL")
  }

  it should "be refused through an outer function too" in {
    rejection(group + "COALESCE(NULLIF(COUNT(*), 0), 5) > 1") should include("can evaluate to NULL")
  }

  "a rendering that needs a local declaration" should "be refused, naming the expression requirement" in {
    val msg = rejection(
      "SELECT status, MAX(amount) AS m FROM t GROUP BY status HAVING ROUND(MAX(amount), 2) > 1"
    )
    msg should include("HAVING cannot be applied to ROUND(MAX(amount), 2) > 1")
    msg should include("needs a local declaration")
  }

  "a rendering that BOXES a number" should "be refused, naming the script context" in {
    // 🔴 The disqualifier no amount of reading could have produced, and the reason this issue's
    // acceptance bar was EXECUTION. `Double.valueOf(Math.abs(params.c)) > 1` is syntactically a
    // perfectly good boolean expression -- and on a real Elasticsearch 8.18.3 it is a COMPILE error
    // (`Cannot cast from [java.lang.Double] to [int]`) that fails the whole search. The same call
    // compiles in the QUERY script context, so it is the bucket-pipeline context that refuses it.
    // Derived, not guessed: eighteen shapes were rendered and executed, and `.valueOf(` separates
    // the nine that run from the nine that do not, exactly.
    Seq("ABS", "FLOOR", "CEIL", "SQRT", "EXP", "LOG").foreach { fn =>
      withClue(s"[$fn] ") {
        rejection(group + s"$fn(COUNT(*)) > 1") should include("boxes a number")
      }
    }
    rejection(group + "POWER(COUNT(*), 2) > 1") should include("boxes a number")
  }

  "a function whose rendering boxes nothing" should "still emit" in {
    // The complement that proves the rule above is not simply "refuse every math function":
    // `SIGN` renders a bare ternary, and it runs.
    script(group + "SIGN(COUNT(*)) > 0") shouldBe
    "(params.c == null ? false : ((params.c > 0 ? 1 : (params.c < 0 ? -1 : 0)) > 0))"
  }

  "a rendering that needs a document" should "be refused" in {
    // A `CASE` renders only against a Painless context; a bucket pipeline has no document.
    rejection(group + "CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1") should include(
      "cannot be rendered without a document"
    )
  }

  "an aggregate ALIAS used inside a function argument" should "be refused by name" in {
    // `Having.resolveAggregateAliases` substitutes an OPERAND only, so `c` inside `NULLIF(c, 0)`
    // stays a bare column and renders `arg0`, a context parameter nothing binds.
    val msg = rejection(group + "NULLIF(c, 0) > 1")
    msg should include("aggregate alias 'c'")
    msg should include("NULLIF(c, 0)")
  }

  "a conjunction mixing an expressible and an un-expressible predicate" should "refuse the whole statement" in {
    // 🔴 The alternative is a PARTIAL filter, which is a wrong answer dressed as a right one.
    rejection(group + "COUNT(*) > 1 AND NULLIF(COUNT(*), 0) > 2") should include(
      "can evaluate to NULL"
    )
    rejection(group + "NULLIF(COUNT(*), 0) > 2 AND COUNT(*) > 1") should include(
      "can evaluate to NULL"
    )
    rejection(group + "COUNT(*) > 1 OR NULLIF(COUNT(*), 0) > 2") should include(
      "can evaluate to NULL"
    )
  }

  "the whole-table HAVING" should "follow the same rule" in {
    script("SELECT COUNT(*) AS c FROM t HAVING GREATEST(COUNT(*), 0) > 1") shouldBe
    "(params.c == null ? false : (Math.max(params.c, 0) > 1))"
    rejection("SELECT COUNT(*) AS c FROM t HAVING NULLIF(COUNT(*), 0) > 1") should include(
      "can evaluate to NULL"
    )
  }

  "a nested-relation HAVING" should "follow the same rule" in {
    val sql = "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
      "HAVING GREATEST(COUNT(e.address), 0) > 1"
    parsed(sql).sqlAggregations.keys.toList shouldBe List("e.filtered_agg.count_e_address")
    script(sql) shouldBe
    "(params.count_e_address == null ? false : (Math.max(params.count_e_address, 0) > 1))"
  }

  it should "be REFUSED inside the relation too, not only at the flat level" in {
    // The walk recurses through `ElasticRelation`, so a nested predicate the engine cannot express
    // is refused rather than silently dropped -- the nested level is where the ORIGINAL drop was
    // most damaging (measured on `main`: the whole aggregation tree vanished and raw documents came
    // back).
    rejection(
      "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
      "HAVING NULLIF(COUNT(e.address), 0) > 1"
    ) should include("can evaluate to NULL")
  }

  // -------------------------------------------------------------------------------------------
  // Each gate rule, falsified on its OWN input
  //
  // 🔴 The rules OVERLAP on real SQL -- every statement that trips the top-level-conditional rule
  // also renders a `? null :`, and every statement carrying a free local also carries one. A
  // mutation matrix driven by statements alone therefore reports live rules as unguarded, which is
  // the "a guard never seen RED is a hypothesis" trap. `disqualifyRendering` is a pure function of
  // a string, so each rule gets an input only IT can classify.
  // -------------------------------------------------------------------------------------------

  private def reason(rendering: String): Option[String] =
    MetricSelectorScript.disqualifyRendering(rendering)

  "the statement rule" should "refuse a declaration and accept a call chain" in {
    reason("def x = 1; x > 1").value should include("local declaration")
    reason("params.c > 1") shouldBe None
  }

  "the NULL rule" should "refuse a null-producing ternary anywhere in the rendering" in {
    reason("(params.c) == 0 ? null : (params.c) > 1").value should include("evaluate to NULL")
    reason("((params.c) == 0 ? null : 5) > 1").value should include("evaluate to NULL")
  }

  "the top-level-conditional rule" should "refuse a rendering whose outermost operator is `?`" in {
    // No SQL statement reaches this rule today -- every conditional the renderer emits is either
    // parenthesised (`SIGN`) or null-producing (`NULLIF`), so the two rules above catch them
    // first. It is kept because the gate's contract is to prove a BOOLEAN, and a bare ternary is
    // not one; this row is the only thing that can hold it honest.
    reason("params.c > 0 ? 1 : 2").value should include("outermost operator is a conditional")
    reason("(params.c > 0 ? 1 : 2) > 1") shouldBe None
  }

  "the boxing rule" should "refuse a valueOf and accept the same call without it" in {
    reason("Double.valueOf(Math.abs(params.c)) > 1").value should include("boxes a number")
    reason("Math.abs(params.c) > 1") shouldBe None
  }

  "the text-comparison rule" should "refuse a metric compared as text and accept a numeric one" in {
    // 🔴 MEASURED on Elasticsearch 8.18.3: a date metric arrives from `buckets_path` as epoch
    // millis, so `(params.max_d != null ? params.max_d : "2020-01-01").compareTo("2019-01-01") > 0`
    // fails with `cannot explicitly cast def [java.lang.String] to java.lang.Double`.
    reason(
      """(params.max_d != null ? params.max_d : "2020-01-01").compareTo("2019-01-01") > 0"""
    ).value should
    include("compares a metric as text")
    reason("params.c > 1") shouldBe None
  }

  "the free-local rule" should "refuse an unbound name and accept methods, classes and literals" in {
    reason("arg0 > 1").value should include("reads 'arg0'")
    reason("Double.parseDouble(params.c) > 1") shouldBe None
    // `null` / `true` / `false` are Painless LITERALS, not free names -- allowing them is not a
    // whitelist of the renderer's vocabulary, which is the thing this rule must not become.
    reason("(params.c == null ? false : true) && params.c != null") shouldBe None
  }

  "every rule" should "read CODE and never the inside of a string literal" in {
    // 🔴 Without the literal blanking, each of these is refused by a DIFFERENT rule, and each of
    // them is a perfectly good script. Measured on a real Elasticsearch 8.18.3: the first form
    // runs.
    // ⚠️ Deliberately NOT a `.compareTo("...")` payload: that is itself a disqualifier now (a
    // metric arrives as a number), so such a row could never isolate the blanking.
    reason("""params.c > 1 && params.c != "a; b"""") shouldBe None
    reason("""params.c > 1 && params.c != "a ? null : b"""") shouldBe None
    reason("""params.c > 1 && params.c != "def x"""") shouldBe None
    reason("""params.c > 1 && params.c != 'arg0'""") shouldBe None
  }

  "an un-expressible criterion" should "make the selector THROW, never return a partial script" in {
    // 🔴 The second line of defence. `validate()` refuses every such statement inside
    // `Parser.apply`, so this is reachable only for a `SingleSearch` assembled in code and emitted
    // without validation -- exactly the route on which `""` used to mean "nothing to filter".
    val sql = group + "COUNT(*) > 1 AND NULLIF(COUNT(*), 0) > 2"
    val criteria = havingOf(unvalidated(sql), sql)
    val thrown = intercept[IllegalStateException] {
      MetricSelectorScript.metricSelector(criteria)
    }
    thrown.getMessage should include("HAVING cannot be applied to")
    // ... and it must not have quietly emitted the OTHER half instead.
    thrown.getMessage should not include "params.c > 1"
  }

  it should "be reported by name from either side of a conjunction" in {
    Seq(
      group + "COUNT(*) > 1 AND NULLIF(COUNT(*), 0) > 2",
      group + "NULLIF(COUNT(*), 0) > 2 AND COUNT(*) > 1",
      group + "COUNT(*) > 1 OR NULLIF(COUNT(*), 0) > 2"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val u = unvalidated(sql).having.map(_.unrepresentable).getOrElse(Nil)
        u should have size 1
        u.head.reason should include("evaluate to NULL")
      }
    }
  }

  "a function of an aggregate over a plain COLUMN" should "be refused by name" in {
    // The free-local rule's own SQL witness: the second operand is a document field, which a
    // bucket pipeline cannot read at all.
    rejection(group + "GREATEST(COUNT(*), status) > 1") should include("reads 'arg1'")
  }

  // -------------------------------------------------------------------------------------------
  // Controls -- shapes that must NOT move
  // -------------------------------------------------------------------------------------------

  "a bare aggregate predicate" should "render exactly as it always did" in {
    script(group + "COUNT(*) > 1") shouldBe "(params.c == null ? false : (params.c > 1))"
    script(group + "c > 1") shouldBe "(params.c == null ? false : (params.c > 1))"
  }

  "a predicate over the bucket KEY" should "still contribute nothing to the selector" in {
    // It is honoured by the `terms` include / exclude, which is a DIFFERENT mechanism. Refusing it
    // here would break every `HAVING status = 'A'` shipped today.
    script(group + "status = 'A'") shouldBe "1 == 1"
    Parser(group + "status = 'A'").isRight shouldBe true
  }

  // -------------------------------------------------------------------------------------------
  // The GROUP BY key population (round 3 -- the document push-down is GONE)
  // -------------------------------------------------------------------------------------------

  "a key predicate the terms filter expresses" should "be classified TermsFilter and left to it" in {
    // The ONE classifier both emission paths read. `TermsFilter` means the existing `terms`
    // include/exclude call applies it, so the selector sees nothing.
    val st = parsed(group + "status = 'a'")
    val leaf = st.havingLeaves.head
    st.keyPredicateOutcome(leaf) shouldBe st.KeyPredicateOutcome.TermsFilter
    script(group + "status = 'a'") shouldBe "1 == 1"
  }

  "a key predicate neither bucket mechanism expresses" should "be REFUSED, never applied to documents" in {
    // 🔴 ROUND 3. This used to be pushed into the QUERY as a document filter, and that is unsound
    // twice over -- MEASURED on Elasticsearch 8.18.3:
    //   `GROUP BY DAY(d) HAVING YEAR(d) = 2025` kept bucket `3` and changed its count 3 -> 1;
    //   `GROUP BY tags HAVING UPPER(tags) = 'A'` kept bucket `b`, which FAILS the predicate,
    //   because a multi-valued document is kept or dropped whole.
    // The second is strictly less sound than the `include` path it was meant to generalise.
    Seq("UPPER(status) = 'A'", "LENGTH(status) = 1", "SUBSTRING(status, 1, 1) = 'a'").foreach { p =>
      withClue(s"[$p] ") {
        rejection(group + p) should include("terms filter, which can only express")
      }
    }
  }

  it should "be refused on a FUNCTION-of-column key too, where the push-down silently miscounted" in {
    rejection(
      "SELECT DAY(d) AS dd, COUNT(*) AS c FROM t GROUP BY DAY(d) HAVING YEAR(d) = 2025"
    ) should include("HAVING cannot filter on")
  }

  it should "name the nested case separately" in {
    rejection(
      "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name HAVING UPPER(e.name) = 'X'"
    ) should include("nested object")
  }

  "a HAVING that reads neither an aggregate nor a GROUP BY key" should "be refused by name" in {
    rejection(group + "UPPER(name) = 'X'") should include("can only filter on a GROUP BY key")
    rejection(group + "name = 'x'") should include("can only filter on a GROUP BY key")
  }

  it should "be refused with NO GROUP BY as well" in {
    // 🔴 Review MEDIUM-4: the rule was gated on `groupBy.isDefined`, so S8's original hole stayed
    // open for a FUNCTION of a column. MEASURED: `SELECT COUNT(*) AS c FROM t HAVING
    // UPPER(status) = 'A'` answered `{"c":{"value":4}}` with the predicate gone, while the bare
    // `HAVING status = 'a'` was correctly refused.
    rejection("SELECT COUNT(*) AS c FROM t HAVING UPPER(status) = 'A'") should include(
      "can only filter on a GROUP BY key"
    )
  }

  // -------------------------------------------------------------------------------------------
  // The include/exclude CHANNEL -- it unions, it never intersects (rule b2)
  //
  // 🔴 `Criteria.excludes` IS `Criteria.includes(bucket, !not, …)`: one method read in two senses.
  // A rule about these channels written for one sense is wrong for the other by construction --
  // that is exactly how an abandoned round shipped an inverted rule that turned `<> a OR <> b`
  // from a dropped predicate into a WRONG answer. So the matrix below is DERIVED and covers both
  // senses; the population, not the assertion, is what was missing.
  // -------------------------------------------------------------------------------------------

  /** Both halves of the matrix, as the predicate text and which channel the leaf feeds. */
  private val includeSide = Seq(
    "status = 'a'"        -> "status = 'b'",
    "status IN ('a','x')" -> "status = 'b'",
    "status LIKE 'a%'"    -> "status LIKE 'b%'"
  )
  private val excludeSide = Seq(
    "status <> 'a'"           -> "status <> 'b'",
    "status NOT IN ('a','x')" -> "status <> 'b'",
    "status NOT LIKE 'a%'"    -> "status NOT LIKE 'b%'"
  )

  "the terms channel" should "express every DERIVED combination of key predicates, or refuse it" in {
    // The verdict is DERIVED from the principle, never listed by hand: the include list is a union
    // and therefore a DISJUNCTION; the exclude list is a union of negations and therefore a
    // CONJUNCTION. So two leaves are expressible only as an OR of includes or an AND of excludes.
    val cells =
      for {
        (leftSide, leftName)   <- Seq(includeSide -> "include", excludeSide -> "exclude")
        (left, right)          <- leftSide
        (rightSide, rightName) <- Seq(includeSide -> "include", excludeSide -> "exclude")
        combiner               <- Seq("AND", "OR")
      } yield {
        // pair each left with the OTHER side's matching right, so mixed cells exist too
        val other = rightSide(leftSide.indexOf((left, right)))._2
        val rhs = if (rightName == leftName) right else other
        val sql = s"$left $combiner $rhs"
        val sameChannel = leftName == rightName
        val expressible =
          // same channel: the include list is a disjunction, the exclude list a conjunction ...
          (sameChannel && leftName == "include" && combiner == "OR") ||
          (sameChannel && leftName == "exclude" && combiner == "AND") ||
          // ... and the two channels are themselves ANDed by Elasticsearch, so one of each under
          // a conjunction is exactly what they mean together.
          (!sameChannel && combiner == "AND")
        (sql, expressible)
      }
    cells.distinct.size should be >= 20
    cells.distinct.foreach { case (predicate, expressible) =>
      withClue(s"[$predicate] expressible=$expressible ") {
        Parser(group + predicate).isRight shouldBe expressible
      }
    }
  }

  it should "leave a SINGLE key predicate of either sense alone" in {
    (includeSide ++ excludeSide).flatMap { case (l, r) => Seq(l, r) }.distinct.foreach { p =>
      withClue(s"[$p] ")(Parser(group + p).isRight shouldBe true)
    }
  }

  it should "keep the AND of inequalities that works on `455433ae`, byte for byte" in {
    // 🔴 The row this rule must NOT move: `exclude:["a","b"]` is CORRECT, because NOT-in-A and
    // NOT-in-B is NOT-in-(A ∪ B). An earlier, inverted rule refused it -- a regression against
    // `main` that the docs then contradicted.
    val st = parsed(group + "status <> 'a' AND status <> 'b'")
    st.buckets
      .map(b => havingOf(st, "and").excludes(b, not = false, BucketIncludesExcludes()).values)
      .toSet shouldBe Set(Set("a", "b"))
    // ... and the three-leaf chain, which is the same union one level deeper.
    val three = parsed(group + "status <> 'a' AND status <> 'b' AND status <> 'c'")
    three.buckets
      .map(b => havingOf(three, "and3").excludes(b, not = false, BucketIncludesExcludes()).values)
      .toSet shouldBe Set(Set("a", "b", "c"))
  }

  it should "refuse the OR of inequalities that is WRONG on `455433ae`" in {
    // 🔴 MEASURED on `455433ae` and on ES 8.18.3: `<> 'a' OR <> 'b'` emitted the SAME
    // `exclude:["a","b"]` as the AND. The disjunction is true for EVERY bucket, and two were
    // dropped, HTTP 200. Live on `main` today.
    rejection(group + "status <> 'a' OR status <> 'b'") should include("cannot combine")
    rejection(group + "status NOT IN ('a','b') OR status <> 'c'") should include("cannot combine")
  }

  it should "refuse the AND of equalities, whose include list means OR" in {
    // `include:["a","b"]` keeps two buckets; the SQL means none. DECISION: refused, not
    // "corrected" to an empty result -- a loud refusal is the contract this issue is about.
    rejection(group + "status = 'a' AND status = 'b'") should include("cannot combine")
  }

  it should "refuse a MIXED pair under OR, which the emission would AND" in {
    // `= 'a' OR <> 'b'` emitted `include:["a"]` AND `exclude:["b"]` on `455433ae` -- a conjunction
    // where the SQL says disjunction.
    rejection(group + "status = 'a' OR status <> 'b'") should include("cannot combine")
  }

  it should "not refuse a conjunction the channels express SEPARATELY" in {
    // One include and one exclude under AND is exactly what the two lists mean together.
    Parser(group + "status = 'a' AND status <> 'b'").isRight shouldBe true
    Parser(group + "status IN ('a','b') AND status <> 'c'").isRight shouldBe true
    // ... and a key predicate beside a METRIC is a different mechanism, not a second channel.
    Parser(group + "status <> 'a' AND COUNT(*) > 1").isRight shouldBe true
  }

  it should "still place a NOT before a LEAF in the right channel" in {
    // The polarity threading that IS reachable: `NOT = 'b'` feeds the EXCLUDE list, so the pair is
    // one include and one exclude under a conjunction -- expressible.
    val st = parsed(group + "status = 'a' AND NOT status = 'b'")
    st.buckets
      .map(b => havingOf(st, "notleaf").includes(b, not = false, BucketIncludesExcludes()).values)
      .toSet shouldBe Set(Set("a"))
    st.buckets
      .map(b => havingOf(st, "notleaf").excludes(b, not = false, BucketIncludesExcludes()).values)
      .toSet shouldBe Set(Set("b"))
    // 🔴 And the shape that would need De Morgan does not exist: `NOT ( … )` around a group is
    // rejected by the grammar, here and on `455433ae`. MEASURED -- which is why the rule carries
    // no arm for it.
    Parser(group + "NOT (status = 'x' OR status = 'y')").isLeft shouldBe true
  }

  // -------------------------------------------------------------------------------------------
  // The OR rule -- homogeneity of MECHANISM (round 3)
  // -------------------------------------------------------------------------------------------

  "an OR whose branches need different mechanisms" should "be refused, because it executes as an AND" in {
    // 🔴 MEASURED on `main`: `HAVING COUNT(*) > 1 OR status = 'b'` returned NO buckets where the
    // disjunction is two of them -- the key became a terms `include` (removing one) and the metric
    // a `bucket_selector` (removing the other), so the OR answered the empty AND.
    val msg = rejection(group + "COUNT(*) > 1 OR status = 'b'")
    msg should include("different mechanisms")
    msg should include("key")
    msg should include("metric")
  }

  it should "name the mechanisms it actually found, not an aggregate that is not there" in {
    // 🔴 Review MEDIUM-5: the old message said "cannot OR a GROUP BY key with an aggregate" even
    // for a statement containing NO aggregate. The message now names what it found.
    rejection(group + "COUNT(*) > 1 OR status = 'b'") should include("mixes key and metric")
    // ... and a key predicate neither mechanism expresses is refused for its OWN reason first,
    // which is the more actionable one.
    rejection(group + "status = 'a' OR UPPER(status) = 'B'") should include(
      "terms filter, which can only express"
    )
  }

  it should "close the NESTED pair too, which the round-2 rule left executing as an AND" in {
    // 🔴 The nested filter aggregation is a THIRD mechanism. The round-2 rule dropped `_.nested`
    // leaves before counting, so this pair was invisible to it and still executed as an AND.
    rejection(
      "SELECT e.name, COUNT(*) AS c FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
      "HAVING COUNT(*) > 1 OR e.name = 'x'"
    ) should include("mixes metric and nested")
  }

  it should "accept an OR the terms filter UNIONS into one include list" in {
    // 🔴 The complement, and the mutation-killer for the rule's second clause: `include: [a, b]`
    // IS the disjunction.
    Parser(group + "status = 'a' OR status = 'b'").isRight shouldBe true
  }

  it should "accept an OR of two metric conditions" in {
    Parser(group + "COUNT(*) > 1 OR COUNT(*) > 5").isRight shouldBe true
  }

  "Having.script" should "answer None for a clause the gate refuses, never a partial filter" in {
    // 🔴 NOT dead code: `softclient4es-extensions` reads it to build a materialized view's
    // `TransformBucketSelectorConfig` (`graph/Stage.scala:291`). Round 1 recorded it as having no
    // production caller and that was WRONG -- it is a FIFTH HAVING mechanism, outside this repo.
    //
    // The contract core can offer: never hand the extension a script for a clause the engine
    // cannot express (a PARTIAL filter inside an MV), and never THROW (a 500 inside the extension).
    // `unrepresentable` is public so the extension can refuse for itself.
    val refused = unvalidated(group + "NULLIF(COUNT(*), 0) > 1").having.get
    refused.unrepresentable should not be empty
    refused.script shouldBe None
    val ok = unvalidated(group + "COUNT(*) > 1").having.get
    ok.unrepresentable shouldBe empty
    ok.script shouldBe Some("(params.c == null ? false : (params.c > 1))")
  }

  "a metric computed outside the nested grouping" should "be refused rather than vanish" in {
    // 🔴 The issue's SECOND conflation site. MEASURED on the branch before this rule: the statement
    // answered HTTP 200 with `{"aggs":{"max_amount":{"max":{"field":"amount"}}}}` -- no
    // `having_filter` AND no grouping at all. (The vanished GROUP BY is a separate pre-existing
    // defect: the control answers `{"query":{"match_all":{}},"_source":true}` for the same input.)
    rejection(
      "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
      "HAVING COALESCE(MAX(amount), 0) > 1"
    ) should include("computed outside the nested grouping")
  }

  "an aggregate written in WHERE" should "still be rejected, now through the wider walk" in {
    Parser("SELECT status FROM t WHERE COUNT(*) > 1 GROUP BY status").isLeft shouldBe true
    // NEW: the function-wrapped form is rejected too. It used to reach the bridge, where an
    // aggregate has no document-level query form at all.
    rejection("SELECT status FROM t WHERE ABS(COUNT(*)) > 1 GROUP BY status") should include(
      "Aggregate functions are not allowed in WHERE"
    )
  }
}
