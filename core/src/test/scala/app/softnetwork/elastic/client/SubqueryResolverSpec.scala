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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.operator.{DIFF, GT, NOT}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story 22.2 — the two-phase rewrite, Docker-free, through the resolver's injected executor.
  *
  * Every statement the resolver DERIVES (mode P's GROUP BY, mode W's bound, the NULL probe) is
  * asserted on its SHAPE and pinned as a parser fixed point, so a derived statement that renders
  * into something the engine could not re-read fails here rather than at a customer's cluster.
  */
class SubqueryResolverSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] $other")
  }

  private def rows(column: String, values: Any*): ElasticResult[ElasticResponse] =
    ElasticSuccess(
      ElasticResponse(
        None,
        "{}",
        values.map(v => ListMap[String, Any](column -> v)),
        ListMap.empty,
        ListMap.empty
      )
    )

  /** Records every inner statement the resolver executes and answers from the table. */
  private class Recording(answers: PartialFunction[SingleSearch, ElasticResult[ElasticResponse]]) {
    val executed = scala.collection.mutable.ArrayBuffer.empty[SingleSearch]
    val execute: SingleSearch => ElasticResult[ElasticResponse] = { s =>
      executed += s
      answers.applyOrElse(s, (x: SingleSearch) => fail(s"unexpected inner statement: ${x.sql}"))
    }
  }

  private def whereOf(r: ElasticResult[SingleSearch]): Criteria =
    r match {
      case ElasticSuccess(s) =>
        s.where.flatMap(_.criteria).getOrElse(fail(s"no WHERE in ${s.sql}"))
      case other => fail(s"expected a success, got $other")
    }

  private def errorOf(r: ElasticResult[_]): ElasticError = r match {
    case ElasticFailure(e) => e
    case other             => fail(s"expected a failure, got $other")
  }

  private def valuesOf(c: Criteria): Values[_, _] = c match {
    case in: InExpr[_, _] => in.values
    case other            => fail(s"expected an InExpr, got $other")
  }

  // ── IN ───────────────────────────────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - IN"

  it should "run a bare-column body as a GROUP BY with bucket size MaxTerms+1 (mode P)" in {
    val outer =
      single("SELECT id FROM orders WHERE customer_id IN (SELECT id FROM c WHERE r = 'EU')")
    val rec = new Recording({ case s if s.groupBy.isDefined => rows("id", 3L, 1L, 2L, 3L) })
    val res = SubqueryResolver.resolve(outer, rec.execute)
    val inner = rec.executed.head
    inner.groupBy.map(_.buckets.map(_.identifier.name)) shouldBe Some(Seq("id"))
    inner.limit shouldBe Some(Limit(SubqueryResolver.MaxTerms + 1, None))
    inner.where.map(_.sql.trim) shouldBe Some("WHERE r = 'EU'")
    inner.returnsRows shouldBe false
    Parser(inner.sql) shouldBe Right(inner) // the DERIVED statement is a parser fixed point
    // distinct, order-preserving, typed on the WHOLE list
    valuesOf(whereOf(res)) shouldBe LongValues(Seq(LongValue(3), LongValue(1), LongValue(2)))
  }

  it should "widen a mixed integral/floating column to DoubleValues (never typed by the first value)" in {
    val outer = single("SELECT id FROM t WHERE amount IN (SELECT amount FROM u)")
    val res = SubqueryResolver.resolve(
      outer,
      new Recording({ case _ => rows("amount", 1L, 2.5d) }).execute
    )
    valuesOf(whereOf(res)) shouldBe DoubleValues(Seq(DoubleValue(1.0), DoubleValue(2.5)))
  }

  it should "emit StringValues and BooleanValues for string and boolean columns" in {
    val s = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE name IN (SELECT name FROM u)"),
      new Recording({ case _ => rows("name", "a", "b") }).execute
    )
    valuesOf(whereOf(s)) shouldBe StringValues(Seq(StringValue("a"), StringValue("b")))
    val b = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE flag IN (SELECT flag FROM u)"),
      new Recording({ case _ =>
        rows("flag", java.lang.Boolean.TRUE, java.lang.Boolean.FALSE)
      }).execute
    )
    valuesOf(whereOf(b)) shouldBe BooleanValues(Seq(BooleanValue(true), BooleanValue(false)))
  }

  /** 🔴 FIXED-WIDTH milliseconds, not `ISO_INSTANT`. `ISO_INSTANT` varies its fraction width, so
    * `"…T00:00:00.500Z"` sorts BEFORE `"…T00:00:00Z"` (`'.' < 'Z'`) and `TermValues.sorted` — which
    * reads that order to pick the min/max a quantified comparison reduces to — would have inverted
    * the bound for any millisecond-bearing date column. Three digits always present makes
    * lexicographic order chronological, and `strict_date_optional_time` accepts the spelling.
    */
  it should "render TEMPORAL cells as fixed-width ISO-8601 instants, chronologically ordered" in {
    val zdt = java.time.ZonedDateTime.parse("2024-01-01T00:00:00Z")
    val res = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE created IN (SELECT created FROM u)"),
      new Recording({ case _ => rows("created", zdt, zdt.toInstant) }).execute
    )
    // both cells are the SAME instant, so the set collapses to one value
    valuesOf(whereOf(res)) shouldBe StringValues(Seq(StringValue("2024-01-01T00:00:00.000Z")))
    // and the ordering the quantified reduction reads IS chronological across fraction widths
    val half = zdt.plusNanos(500000000L)
    TermValues.sorted(Seq(half, zdt).map(TermValues.canonical)) shouldBe
    Seq("2024-01-01T00:00:00.000Z", "2024-01-01T00:00:00.500Z")
    val sc = single("SELECT id FROM t WHERE created > (SELECT MAX(created) AS m FROM u)")
    whereOf(
      SubqueryResolver.resolve(sc, new Recording({ case _ => rows("m", zdt) }).execute)
    ) shouldBe GenericExpression(
      sc.whereSubqueries.head.asInstanceOf[ScalarSubquery].identifier,
      GT,
      StringValue("2024-01-01T00:00:00.000Z"),
      None
    )
  }

  it should "run a `text` inner column in mode W, and an unknown mapping in mode P" in {
    val outer = single("SELECT id FROM t WHERE name IN (SELECT name FROM u)")
    val r1 = new Recording({ case _ => rows("name", "x") })
    SubqueryResolver.resolve(outer, r1.execute, columnType = (_, _) => Some(SQLTypes.Text))
    r1.executed.head.groupBy shouldBe None // a terms aggregation on `text` is an ES 400
    val r2 = new Recording({ case _ => rows("name", "x") })
    SubqueryResolver.resolve(outer, r2.execute)
    r2.executed.head.groupBy shouldBe defined // unknown mapping: the cheap default
  }

  it should "fail LOUDLY when the projected column is absent from the rows (EntityContext keeps it absent)" in {
    // the headline UN-ALIASED aggregate: under NativeContext this would null-fill and answer
    // MatchNone, a silent wrong answer
    val outer = single("SELECT id FROM t WHERE amount > (SELECT AVG(amount) FROM u)")
    // 🔴 The column the CONVERTER produces, which for an UN-ALIASED aggregate is the SYNTHETIC
    // alias `__c1`, NOT the source field `amount`. Measured on real ES 8.18: reading
    // `select.fields.head.outputName` here asked for `amount` and the resolver answered "no column"
    // for a statement that had run perfectly.
    val column = SubqueryResolver.columnNameOf(outer.whereSubqueries.head.inner.get)
    column shouldBe "__c1"
    val err = errorOf(
      SubqueryResolver.resolve(outer, new Recording({ case _ => rows("other_key", 1.5d) }).execute)
    )
    err.statusCode shouldBe Some(400)
    err.message should include(column)
    err.message should include("other_key")
    // the same shape with the RIGHT key resolves — the resolver reads the ONE name it requests
    whereOf(
      SubqueryResolver.resolve(outer, new Recording({ case _ => rows(column, 100.5d) }).execute)
    ) shouldBe a[GenericExpression]
  }

  it should "fail LOUDLY on a null produced by mode P (a terms bucket never yields one)" in {
    val err = errorOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a IN (SELECT a FROM u)"),
        new Recording({ case _ => rows("a", null) }).execute
      )
    )
    err.message should include("naming defect")
  }

  it should "flatten the row-path array wrap and multi-valued cells" in {
    val res = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE tag IN (SELECT UPPER(tag) AS up FROM u)"),
      new Recording({ case _ => rows("up", List("A"), List("B", "C")) }).execute
    )
    valuesOf(whereOf(res)) shouldBe StringValues(Seq("A", "B", "C").map(StringValue))
  }

  it should "keep a body's own LIMIT and bound a row-shaped no-LIMIT body at MaxTerms+1" in {
    val r1 = new Recording({ case _ => rows("a", "x") })
    SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE a IN (SELECT a FROM u ORDER BY score DESC LIMIT 100)"),
      r1.execute
    )
    r1.executed.head.limit shouldBe Some(Limit(100, None))
    val r2 = new Recording({ case _ => rows("up", "X") })
    SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE a IN (SELECT UPPER(a) AS up FROM u)"),
      r2.execute
    )
    r2.executed.head.groupBy shouldBe None // a scripted projection is not a bare column: mode W
    r2.executed.head.limit shouldBe Some(Limit(SubqueryResolver.MaxTerms + 1, None))
  }

  it should "resolve an EMPTY set to match_none for IN and match_all for NOT IN" in {
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a IN (SELECT a FROM u)"),
        new Recording({ case _ => rows("a") }).execute
      )
    ) shouldBe MatchNoneCriteria()
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a NOT IN (SELECT a FROM u)"),
        // mode P cannot see a NULL, so `NOT IN` probes for one: the probe answers no row here
        new Recording({
          case s if s.groupBy.isDefined              => rows("a")
          case s if s.limit.contains(Limit(1, None)) => rows("a")
        }).execute
      )
    ) shouldBe MatchAllCriteria()
  }

  // ── NULL semantics (PD-5) ────────────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - ANSI NULL semantics"

  it should "ignore inner NULLs for IN" in {
    val res = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE a IN (SELECT a FROM u LIMIT 10)"),
      new Recording({ case _ => rows("a", 1L, null, 2L) }).execute
    )
    valuesOf(whereOf(res)) shouldBe LongValues(Seq(LongValue(1), LongValue(2)))
  }

  it should "resolve NOT IN to match_none when a mode-W set contains NULL" in {
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a NOT IN (SELECT a FROM u LIMIT 10)"),
        new Recording({ case _ => rows("a", 1L, null) }).execute
      )
    ) shouldBe MatchNoneCriteria()
  }

  it should "probe for NULL with `... AND a IS NULL LIMIT 1` for NOT IN in mode P, and honour it" in {
    val outer = single("SELECT id FROM t WHERE a NOT IN (SELECT a FROM u WHERE b = 1)")
    val rec = new Recording({
      case s if s.groupBy.isDefined              => rows("a", 1L, 2L)
      case s if s.limit.contains(Limit(1, None)) => rows("a", null) // one row = a NULL exists
    })
    whereOf(SubqueryResolver.resolve(outer, rec.execute)) shouldBe MatchNoneCriteria()
    val probe = rec.executed(1)
    probe.where.map(_.sql.trim) shouldBe Some("WHERE (b = 1 AND a IS NULL)")
    Parser(probe.sql) shouldBe Right(probe)
    // and with NO null the predicate is the ordinary NOT IN
    val rec2 = new Recording({
      case s if s.groupBy.isDefined => rows("a", 1L, 2L)
      case _                        => rows("a")
    })
    whereOf(SubqueryResolver.resolve(outer, rec2.execute))
      .asInstanceOf[InExpr[_, _]]
      .maybeNot shouldBe Some(NOT)
  }

  // ── EXISTS ───────────────────────────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - EXISTS"

  it should "cap a row-shaped body at LIMIT 1, keep LIMIT 0, and run an aggregate body as written" in {
    val r1 = new Recording({ case _ => rows("id", "x") })
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE EXISTS (SELECT 1 FROM u)"),
        r1.execute
      )
    ) shouldBe MatchAllCriteria()
    r1.executed.head.limit shouldBe Some(Limit(1, None))
    val r2 = new Recording({ case _ => rows("id") })
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE EXISTS (SELECT id FROM u LIMIT 0)"),
        r2.execute
      )
    ) shouldBe MatchNoneCriteria()
    r2.executed.head.limit shouldBe Some(Limit(0, None))
    val r3 = new Recording({ case _ => rows("n", 0L) })
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE EXISTS (SELECT COUNT(*) AS n FROM u)"),
        r3.execute
      )
    ) shouldBe MatchAllCriteria() // a metric-only SELECT always yields one row
  }

  it should "cap a GROUP BY body without HAVING at one bucket and run a HAVING body as written" in {
    val r1 = new Recording({ case _ => rows("a", "x") })
    SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE EXISTS (SELECT a FROM u GROUP BY a)"),
      r1.execute
    )
    r1.executed.head.limit shouldBe Some(Limit(1, None))
    val r2 = new Recording({ case _ => rows("a", "x") })
    SubqueryResolver.resolve(
      single(
        "SELECT id FROM t WHERE EXISTS (SELECT a, COUNT(*) AS n FROM u GROUP BY a HAVING n > 1)"
      ),
      r2.execute
    )
    r2.executed.head.limit shouldBe None
  }

  it should "negate under NOT" in {
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u)"),
        new Recording({ case _ => rows("id", "x") }).execute
      )
    ) shouldBe MatchNoneCriteria()
    whereOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE NOT EXISTS (SELECT 1 FROM u)"),
        new Recording({ case _ => rows("id") }).execute
      )
    ) shouldBe MatchAllCriteria()
  }

  // ── scalar ───────────────────────────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - scalar"

  it should "rewrite one row into a literal comparison, zero rows into match_none, two rows into a 400" in {
    val outer = single("SELECT id FROM t WHERE amount > (SELECT AVG(amount) AS a FROM u)")
    whereOf(
      SubqueryResolver.resolve(outer, new Recording({ case _ => rows("a", 12.5d) }).execute)
    ) shouldBe GenericExpression(
      outer.whereSubqueries.head.asInstanceOf[ScalarSubquery].identifier,
      GT,
      DoubleValue(12.5),
      None
    )
    // 0 rows -> the subquery is NULL -> UNKNOWN -> no row, NOT negated by an outer NOT
    whereOf(
      SubqueryResolver.resolve(outer, new Recording({ case _ => rows("a") }).execute)
    ) shouldBe MatchNoneCriteria()
    val many = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE amount > (SELECT amount AS a FROM u LIMIT 1)"),
      new Recording({ case _ => rows("a", 1L, 2L) }).execute
    )
    errorOf(many).statusCode shouldBe Some(400)
    errorOf(many).message should include("returned 2 rows")
  }

  it should "unwrap a one-element scalar cell and reject a multi-valued one" in {
    val outer = single("SELECT id FROM t WHERE tag = (SELECT UPPER(tag) AS up FROM u LIMIT 1)")
    whereOf(
      SubqueryResolver.resolve(outer, new Recording({ case _ => rows("up", List("X")) }).execute)
    ) shouldBe a[GenericExpression]
    errorOf(
      SubqueryResolver
        .resolve(outer, new Recording({ case _ => rows("up", List("X", "Y")) }).execute)
    ).message should include("multi-valued")
  }

  // ── quantified (lead ruling OQ-4) ────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - quantified comparison"

  /** An `ALL` form in mode P probes for an inner NULL exactly as `NOT IN` does, so the NULL probe
    * (the only inner statement with `LIMIT 1`) is answered with NO row unless a row explicitly
    * passes `null` among its values.
    */
  private def quantified(sql: String, values: Any*): Criteria =
    whereOf(
      SubqueryResolver.resolve(
        single(sql),
        new Recording({
          case s if s.limit.contains(Limit(1, None)) =>
            if (values.contains(null)) rows("a", null) else rows("a")
          case _ => rows("a", values: _*)
        }).execute
      )
    )

  private def idOf(sql: String): Identifier =
    single(sql).whereSubqueries.head.asInstanceOf[QuantifiedSubquery].identifier

  /** 🔴 The full reduction table, every row, values `{1, 5, 9}` so `min != max`. */
  it should "reduce every ordering combination to a MIN/MAX comparison" in {
    val table = Seq(
      "> ANY"  -> (GT                                      -> 1L),
      ">= ANY" -> (app.softnetwork.elastic.sql.operator.GE -> 1L),
      "< ANY"  -> (app.softnetwork.elastic.sql.operator.LT -> 9L),
      "<= ANY" -> (app.softnetwork.elastic.sql.operator.LE -> 9L),
      "> ALL"  -> (GT                                      -> 9L),
      ">= ALL" -> (app.softnetwork.elastic.sql.operator.GE -> 9L),
      "< ALL"  -> (app.softnetwork.elastic.sql.operator.LT -> 1L),
      "<= ALL" -> (app.softnetwork.elastic.sql.operator.LE -> 1L)
    )
    table.foreach { case (spelling, (op, bound)) =>
      val sql = s"SELECT id FROM t WHERE amount $spelling (SELECT a FROM u)"
      withClue(s"$spelling: ") {
        quantified(sql, 5L, 1L, 9L) shouldBe
        GenericExpression(idOf(sql), op, LongValue(bound), None)
      }
    }
  }

  it should "reduce = ALL and <> ANY, which need BOTH ends" in {
    val eqAll = "SELECT id FROM t WHERE amount = ALL (SELECT a FROM u)"
    quantified(eqAll, 7L, 7L) shouldBe
    GenericExpression(idOf(eqAll), app.softnetwork.elastic.sql.operator.EQ, LongValue(7), None)
    quantified(eqAll, 7L, 8L) shouldBe MatchNoneCriteria() // not a singleton: no row can equal all
    val neAny = "SELECT id FROM t WHERE amount <> ANY (SELECT a FROM u)"
    quantified(neAny, 7L, 7L) shouldBe
    GenericExpression(idOf(neAny), app.softnetwork.elastic.sql.operator.NE, LongValue(7), None)
    quantified(neAny, 7L, 8L) shouldBe MatchAllCriteria() // some value differs from every x
    quantified("SELECT id FROM t WHERE amount != ANY (SELECT a FROM u)", 7L, 8L) shouldBe
    MatchAllCriteria()
  }

  /** 🔴 The two empty-set rows are OPPOSITE — ANSI: an existential over an empty set is FALSE, a
    * universal over an empty set is TRUE. The single most likely thing to ship backwards.
    */
  it should "answer FALSE for ANY and TRUE for ALL over an EMPTY subquery" in {
    quantified("SELECT id FROM t WHERE amount > ANY (SELECT a FROM u)") shouldBe MatchNoneCriteria()
    quantified("SELECT id FROM t WHERE amount < ANY (SELECT a FROM u)") shouldBe MatchNoneCriteria()
    quantified("SELECT id FROM t WHERE amount = ALL (SELECT a FROM u)") shouldBe MatchAllCriteria()
    quantified("SELECT id FROM t WHERE amount > ALL (SELECT a FROM u)") shouldBe MatchAllCriteria()
    quantified("SELECT id FROM t WHERE amount <> ANY (SELECT a FROM u)") shouldBe
    MatchNoneCriteria()
  }

  /** 🔴 An inner NULL makes every `ALL` form UNKNOWN for every row — and UNKNOWN negated is still
    * UNKNOWN, so an outer NOT does NOT flip it. `ANY` ignores inner NULLs.
    */
  it should "answer no rows for ALL when the inner set carries a NULL, and ignore it for ANY" in {
    quantified("SELECT id FROM t WHERE amount > ALL (SELECT a FROM u LIMIT 9)", 1L, null) shouldBe
    MatchNoneCriteria()
    quantified(
      "SELECT id FROM t WHERE NOT amount > ALL (SELECT a FROM u LIMIT 9)",
      1L,
      null
    ) shouldBe MatchNoneCriteria()
    val anySql = "SELECT id FROM t WHERE amount > ANY (SELECT a FROM u LIMIT 9)"
    quantified(anySql, 5L, null, 1L) shouldBe
    GenericExpression(idOf(anySql), GT, LongValue(1), None)
  }

  it should "probe for a NULL in mode P for an ALL form, exactly as NOT IN does" in {
    val outer = single("SELECT id FROM t WHERE amount > ALL (SELECT a FROM u WHERE b = 1)")
    val rec = new Recording({
      case s if s.groupBy.isDefined              => rows("a", 1L, 9L)
      case s if s.limit.contains(Limit(1, None)) => rows("a", null)
    })
    whereOf(SubqueryResolver.resolve(outer, rec.execute)) shouldBe MatchNoneCriteria()
    rec.executed should have size 2
    rec.executed(1).where.map(_.sql.trim) shouldBe Some("WHERE (b = 1 AND a IS NULL)")
  }

  it should "flip a genuine truth value under NOT, and a comparison's own maybeNot" in {
    quantified("SELECT id FROM t WHERE NOT amount > ANY (SELECT a FROM u)") shouldBe
    MatchAllCriteria() // NOT FALSE
    quantified("SELECT id FROM t WHERE NOT amount > ALL (SELECT a FROM u)") shouldBe
    MatchNoneCriteria() // NOT TRUE
    val sql = "SELECT id FROM t WHERE NOT amount > ANY (SELECT a FROM u)"
    quantified(sql, 1L, 9L) shouldBe GenericExpression(idOf(sql), GT, LongValue(1), Some(NOT))
  }

  // ── bounds, status, recursion ────────────────────────────────────────────────────────────────

  behavior of "SubqueryResolver - bounds, status, purity"

  it should "fail loudly above MaxTerms distinct values, naming the setting and the JOIN rewrite" in {
    val outer = single("SELECT id FROM t WHERE a IN (SELECT a FROM u)")
    val err = errorOf(
      SubqueryResolver.resolve(
        outer,
        new Recording({ case _ =>
          rows("a", (1L to (SubqueryResolver.MaxTerms + 1).toLong).map(Long.box): _*)
        }).execute
      )
    )
    err.statusCode shouldBe Some(400)
    err.message should include("index.max_terms_count")
    err.message should include("JOIN")
    // exactly MaxTerms values is fine
    SubqueryResolver.resolve(
      outer,
      new Recording({ case _ =>
        rows("a", (1L to SubqueryResolver.MaxTerms.toLong).map(Long.box): _*)
      }).execute
    ) shouldBe a[ElasticSuccess[_]]
  }

  it should "translate an inner too_many_buckets_exception into the same bound message" in {
    val err = errorOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a IN (SELECT a FROM u)"),
        new Recording({ case _ =>
          ElasticFailure(
            ElasticError(
              "all shards failed: too_many_buckets_exception: Trying to create too many buckets",
              None,
              Some(400)
            )
          )
        }).execute
      )
    )
    err.message should include("index.max_terms_count")
    err.message should include("JOIN")
  }

  it should "propagate an inner failure with ITS status and cause (#184)" in {
    val boom = new RuntimeException("shard down")
    val err = errorOf(
      SubqueryResolver.resolve(
        single("SELECT id FROM t WHERE a IN (SELECT a FROM u)"),
        new Recording({ case _ =>
          ElasticFailure(ElasticError("unavailable", Some(boom), Some(503)))
        }).execute
      )
    )
    err.statusCode shouldBe Some(503)
    err.cause shouldBe Some(boom)
    err.message should include("Subquery")
  }

  it should "return the SAME instance when the statement carries no subquery" in {
    val s = single("SELECT id FROM t WHERE a = 1")
    SubqueryResolver
      .resolve(s, new Recording({ case _ => fail("must not execute") }).execute)
      .asInstanceOf[ElasticSuccess[SingleSearch]]
      .value should be theSameInstanceAs s
  }

  it should "reject a correlated bare name through the injected mapping check" in {
    val outer = single("SELECT id FROM orders WHERE cid IN (SELECT id FROM customers WHERE r = 1)")
    val res = SubqueryResolver.resolve(
      outer,
      new Recording({ case _ => rows("id", 1L) }).execute,
      _ => Some("Correlated subquery: 'r' is not a column of 'customers'")
    )
    errorOf(res).statusCode shouldBe Some(400)
    errorOf(res).message should include("Correlated subquery")
  }

  it should "rewrite BOTH sides of a predicate and keep every untouched criteria instance" in {
    val outer = single(
      "SELECT id FROM t WHERE a = 1 AND b IN (SELECT b FROM u) OR EXISTS (SELECT 1 FROM v)"
    )
    val rec = new Recording({
      case s if s.sources.contains("u") => rows("b", 7L)
      case s if s.sources.contains("v") => rows("id", "x")
    })
    val rewritten = SubqueryResolver.resolve(outer, rec.execute) match {
      case ElasticSuccess(s) => s
      case other             => fail(s"$other")
    }
    rec.executed should have size 2
    rewritten.whereSubqueries shouldBe empty
    rewritten.where.map(_.sql.trim) shouldBe Some("WHERE a = 1 AND b IN (7) OR 1 = 1")
  }

  it should "refuse LOUDLY a quantified combination the parser cannot build" in {
    // `= ANY` and `<> ALL` collapse to InSubquery at PARSE time, so no parsed statement reaches this
    // arm — but `GatewayApi.run(statement)` takes a programmatically built AST, where answering
    // `match_none` would be a silent wrong answer.
    val node = single("SELECT id FROM t WHERE amount > ANY (SELECT a FROM u)").whereSubqueries.head
      .asInstanceOf[QuantifiedSubquery]
    val res = SubqueryResolver.reduceQuantified(
      node.copy(operator = DIFF, quantifier = app.softnetwork.elastic.sql.operator.ALL),
      Seq(1L, 2L),
      nullSeen = false
    )
    res.isLeft shouldBe true
    res.swap.toOption.get.message should include("Unsupported quantified comparison")
  }

  /** 🔴 Independent-review H4: a NEGATED `ANY` over a NULL-bearing set is UNKNOWN, not
    * `must_not(min)`. With `S = {1, NULL}` and `x = 0`, `x > ANY S` is `FALSE OR UNKNOWN` =
    * UNKNOWN, so its negation is UNKNOWN and NO row matches — while `must_not(range gt 1)` would
    * return it.
    */
  it should "answer no rows for a NEGATED ANY when the inner set carries a NULL" in {
    quantified(
      "SELECT id FROM t WHERE NOT amount > ANY (SELECT a FROM u LIMIT 9)",
      1L,
      null
    ) shouldBe
    MatchNoneCriteria()
    // and it PROBES for that NULL in mode P, exactly as NOT IN and the ALL family do
    val outer = single("SELECT id FROM t WHERE NOT amount > ANY (SELECT a FROM u WHERE b = 1)")
    val rec = new Recording({
      case s if s.groupBy.isDefined              => rows("a", 1L, 9L)
      case s if s.limit.contains(Limit(1, None)) => rows("a", null)
    })
    whereOf(SubqueryResolver.resolve(outer, rec.execute)) shouldBe MatchNoneCriteria()
    rec.executed should have size 2
  }

  /** 🔴 Independent-review M1: a written window BEYOND the bound is REFUSED, never silently
    * replaced — executing `LIMIT 65537 OFFSET 0` in place of `LIMIT 10 OFFSET 100000` reads a
    * completely different window and the count check would never notice.
    */
  it should "refuse a body whose own LIMIT window exceeds the bound" in {
    val res = SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE a IN (SELECT a FROM u ORDER BY a LIMIT 10 OFFSET 100000)"),
      new Recording({ case _ => fail("must not execute") }).execute
    )
    errorOf(res).statusCode shouldBe Some(400)
    errorOf(res).message should include("beyond 65536 rows")
  }

  /** 🔴 Independent-review H3: on the ROW path a document that carries no value for the projected
    * column yields a row WITHOUT the key, and that is an ordinary SQL NULL — not the naming defect
    * mode P's contract makes it. Turning it into a 400 broke the exact ANSI shape PD-5 exists for.
    */
  it should "read an ABSENT key on the row path as a NULL, and stay loud when NO row carries it" in {
    val notIn = single("SELECT id FROM t WHERE a NOT IN (SELECT a FROM u LIMIT 10)")
    val mixed = ElasticSuccess(
      ElasticResponse(
        None,
        "{}",
        Seq(ListMap[String, Any]("a" -> 1L), ListMap.empty[String, Any]),
        ListMap.empty,
        ListMap.empty
      )
    )
    whereOf(SubqueryResolver.resolve(notIn, new Recording({ case _ => mixed }).execute)) shouldBe
    MatchNoneCriteria() // the absent key IS the NULL that makes NOT IN empty
    val none = ElasticSuccess(
      ElasticResponse(
        None,
        "{}",
        Seq(ListMap[String, Any]("other" -> 1L)),
        ListMap.empty,
        ListMap.empty
      )
    )
    errorOf(
      SubqueryResolver.resolve(notIn, new Recording({ case _ => none }).execute)
    ).message should include("returned no column 'a'")
  }

  /** 🔴 Independent-review M4: a scalar body is bounded at two rows — enough to report the ANSI
    * cardinality violation, impossible to run away even when `validate()` was bypassed.
    */
  it should "cap a row-shaped scalar body at LIMIT 2" in {
    val rec = new Recording({ case _ => rows("a", 1L) })
    SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE amount > (SELECT a FROM u LIMIT 1)"),
      rec.execute
    )
    rec.executed.head.limit shouldBe Some(Limit(1, None)) // its own smaller LIMIT is kept
    val rec2 = new Recording({ case _ => rows("a", 1L) })
    SubqueryResolver.resolve(
      single("SELECT id FROM t WHERE amount > (SELECT MAX(a) AS a FROM u)"),
      rec2.execute
    )
    rec2.executed.head.limit shouldBe None // an aggregate body is bounded by its own shape
  }
}
