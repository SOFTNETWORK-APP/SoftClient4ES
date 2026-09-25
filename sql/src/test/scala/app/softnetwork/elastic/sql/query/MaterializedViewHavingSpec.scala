package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A MATERIALIZED VIEW may not carry a `HAVING` an Elasticsearch TRANSFORM cannot express.
  *
  * SIX shapes were MEASURED at RENDER level (the generated `TransformConfig`) either silently
  * dropping the clause or deploying a `bucket_selector` that cannot run, on `origin/main` at
  * `04b1c694`:
  *
  *   - a condition on a GROUPING key -- a search rides the `terms` `include` / `exclude` channel, a
  *     transform's `group_by` has none (`TermsGroupBy` emits `{"terms":{"field":…}}` and nothing
  *     else), so the whole condition vanished. With a metric conjunct beside it, only the key half
  *     vanished: a PARTIAL filter, which is a wrong answer that answers 200;
  *   - a `HAVING` with no `GROUP BY` -- `FinalTransformStage.toTransformConfig` builds no pivot, so
  *     the `bucket_selector` is never even attempted;
  *   - an aggregate NO transform can compute (`STDDEV`, `VARIANCE`, `PERCENTILE_CONT`, …) -- even
  *     WITH the aggregate in the SELECT list, `Stage.buildAggregations()` drops it because
  *     `AggregateConversion.toTransformAggregation` answers `None`, while
  *     `Stage.extractAggregatePaths` still names it: a `buckets_path` pointing at an aggregation
  *     that does not exist;
  *   - a SELECT `bucket_script` alias (`MAX(x) - MIN(x) AS d`) -- a transform's pivot has no
  *     `bucket_script` channel and such an item is not an aggregate, so `buckets_path` came back
  *     EMPTY and the clause was dropped;
  *   - an aggregate on the RIGHT of a comparison -- `Stage.extractAggregatePaths` walks
  *     `expr.identifier` ONLY and never `expr.maybeValue`, while core's own
  *     `Expression.extractAllMetricsPath` walks BOTH, so publishing it puts it in
  *     `buildAggregations` and NEVER in `buckets_path`: the selector IS built, reads a null
  *     parameter, the guard short-circuits and EVERY bucket is rejected -- an EMPTY view at 200;
  *   - an aggregate a transform COULD compute but the SELECT list does not publish -- the selector
  *     reads a metric the view never creates: `buckets_path` empty (dropped) or, beside a published
  *     metric, PARTIAL, so the script reads a `params.*` the path never declares.
  *
  * 🔴 MV-ONLY. Every one of those bodies is CORRECT as a plain search and stays accepted --
  * asserted here in both directions, and executed against real Elasticsearch by the testkit's
  * `GroupByCompletenessSpec`.
  */
class MaterializedViewHavingSpec extends AnyFlatSpec with Matchers {

  /** What a statement's verdict is, and why. `Refused` carries the reason so a row can assert the
    * RULE that fired and not merely that something failed.
    */
  private sealed trait Verdict
  private case object Accepted extends Verdict
  private case class Refused(reason: String) extends Verdict

  private def verdict(sql: String): Verdict = Parser(sql) match {
    case Right(_)  => Accepted
    case Left(err) => Refused(err.msg)
  }

  private def asView(body: String): String = s"CREATE MATERIALIZED VIEW mv_probe AS $body"

  /** Which of the six rules a refusal came from -- keyed on the PROPERTY the message carries, never
    * on an internal identifier, so message wording stays free to change
    * (`feedback_assert_the_mechanism_not_a_proxy`).
    */
  private object Rule {
    val NoGroupBy = "no GROUP BY"
    val GroupKey = "grouping key"
    val NoTransformAgg = "computes only MIN, MAX, SUM, AVG"
    val BucketScript = "expression over aggregates"
    val TwoAggregates = "compare two aggregates"
    val Unpublished = "to the SELECT list"
    val NoMatchingAgg = "matches none of the aggregations this view creates"
    val all: Seq[String] =
      Seq(
        NoGroupBy,
        GroupKey,
        NoTransformAgg,
        BucketScript,
        TwoAggregates,
        Unpublished,
        NoMatchingAgg
      )
  }

  private def refusalOf(sql: String): String = verdict(sql) match {
    case Refused(reason) => reason
    case Accepted        => fail(s"expected a refusal, got ACCEPTED: [$sql]")
  }

  private def parsedSearch(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"expected a SingleSearch, got $other: [$sql]")
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // The POPULATION. Every cell of the spec's §3 carries a verdict for BOTH venues, so the MV-only
  // separation is read off the table rather than argued. `mv` is the verdict of
  // `CREATE MATERIALIZED VIEW … AS <body>`; `select` is the verdict of `<body>` on its own.
  // ─────────────────────────────────────────────────────────────────────────────────────────────
  private case class Cell(id: String, body: String, mvRefusedBy: Option[String])

  private val population: Seq[Cell] = Seq(
    // (A) a metric the SELECT publishes: the `bucket_selector` a transform CAN build.
    Cell(
      "A1 metric in SELECT",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1",
      None
    ),
    Cell(
      "A2 metric via SELECT alias",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING c > 1",
      None
    ),
    Cell(
      "A3 SUM in SELECT",
      "SELECT city, SUM(amount) AS s FROM customers GROUP BY city HAVING SUM(amount) > 1",
      None
    ),
    Cell(
      "A4 WHERE + GROUP BY + metric",
      "SELECT city, COUNT(*) AS c FROM customers WHERE amount > 0 GROUP BY city HAVING COUNT(*) > 1",
      None
    ),
    // (B) a condition on the GROUP BY key -- the terms include/exclude channel, which a transform lacks.
    Cell(
      "B1 key =",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B2 key <>",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city <> 'Paris'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B3 key IN",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city IN ('Paris','Lyon')",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B4 key LIKE",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city LIKE 'Par%'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B5 key OR key",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris' OR city = 'Lyon'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B6 key on 2nd grouping level",
      "SELECT city, status, COUNT(*) AS c FROM customers GROUP BY city, status HAVING status = 'a'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "B7 key NOT IN",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city NOT IN ('Paris','Lyon')",
      Some(Rule.GroupKey)
    ),
    // (C) the PARTIAL shape: only the key half vanished, which answers 200 with wrong groups.
    Cell(
      "C1 metric AND key",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1 AND city = 'Paris'",
      Some(Rule.GroupKey)
    ),
    Cell(
      "C2 key AND metric",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris' AND COUNT(*) > 1",
      Some(Rule.GroupKey)
    ),
    // (D) an aggregate the view does not publish.
    Cell(
      "D1 MAX not in SELECT",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1",
      Some(Rule.Unpublished)
    ),
    Cell(
      "D2 MIN not in SELECT",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MIN(amount) > 1",
      Some(Rule.Unpublished)
    ),
    Cell(
      "D3 one published, one not",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1 AND MAX(amount) > 2",
      Some(Rule.Unpublished)
    ),
    Cell(
      "D4 aggregate IS in SELECT",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > 1",
      None
    ),
    Cell(
      "D5 remedy: add it to SELECT",
      "SELECT city, COUNT(*) AS c, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > 1",
      None
    ),
    // (E) a HAVING with no GROUP BY -- no pivot, so no selector.
    Cell(
      "E1 no GROUP BY, metric",
      "SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1",
      Some(Rule.NoGroupBy)
    ),
    Cell(
      "E2 no GROUP BY, metric alias",
      "SELECT COUNT(*) AS c FROM customers HAVING c > 1",
      Some(Rule.NoGroupBy)
    ),
    Cell(
      "E3 no GROUP BY, SUM",
      "SELECT SUM(amount) AS s FROM customers HAVING SUM(amount) > 1",
      Some(Rule.NoGroupBy)
    ),
    // (H) no HAVING at all: the shapes a transform has always been able to build.
    Cell("H1 no HAVING, GROUP BY", "SELECT city, COUNT(*) AS c FROM customers GROUP BY city", None),
    Cell("H2 no HAVING, no GROUP BY", "SELECT id, name FROM customers", None),
    Cell("H3 WHERE only", "SELECT id, name FROM customers WHERE city = 'Paris'", None),
    // (J) the repo's one existing materialized-view HAVING fixture, and its neighbours.
    Cell(
      "J1 JOIN, metric HAVING (existing fixture)",
      "SELECT c.city, SUM(o.amount) AS total FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.city HAVING SUM(o.amount) > 10000",
      None
    ),
    Cell(
      "J2 JOIN, no HAVING",
      "SELECT c.city, SUM(o.amount) AS total FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.city",
      None
    ),
    Cell(
      "J3 JOIN, unpublished aggregate",
      "SELECT c.city, SUM(o.amount) AS total FROM customers c INNER JOIN orders o ON c.id = o.customer_id GROUP BY c.city HAVING MAX(o.amount) > 1",
      Some(Rule.Unpublished)
    ),
    // (N) an aggregate NO transform can compute. `AggregateConversion.toTransformAggregation`
    // maps MIN / MAX / SUM / AVG / COUNT and answers None for everything else, and
    // `Stage.buildAggregations()` drops that None while `Stage.extractAggregatePaths` still names
    // the field -- so these deployed a `buckets_path` pointing at an aggregation that is never
    // created, even though the SELECT list "publishes" them.
    Cell(
      "N1 STDDEV in SELECT and HAVING",
      "SELECT city, STDDEV(amount) AS sd FROM customers GROUP BY city HAVING STDDEV(amount) > 1",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N2 VARIANCE in SELECT and HAVING",
      "SELECT city, VARIANCE(amount) AS vr FROM customers GROUP BY city HAVING VARIANCE(amount) > 1",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N3 PERCENTILE_CONT in SELECT and HAVING",
      "SELECT city, PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) AS p95 FROM customers " +
      "GROUP BY city HAVING PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount) > 1",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N4 STDDEV via SELECT alias",
      "SELECT city, STDDEV(amount) AS sd FROM customers GROUP BY city HAVING sd > 1",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N5 STDDEV beside a published COUNT",
      "SELECT city, COUNT(*) AS c, STDDEV(amount) AS sd FROM customers GROUP BY city " +
      "HAVING STDDEV(amount) > 1 AND COUNT(*) > 2",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N6 STDDEV NOT in SELECT",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING STDDEV(amount) > 1",
      Some(Rule.NoTransformAgg)
    ),
    Cell(
      "N7 bucket_script alias",
      "SELECT city, MAX(amount) - MIN(amount) AS d FROM customers GROUP BY city HAVING d > 3",
      Some(Rule.BucketScript)
    ),
    // 🔴 The non-over-reach row: an aggregate no transform can compute is fine in the SELECT list
    // as long as the HAVING does not read it. Without this, rule 3 could refuse the whole family.
    Cell(
      "N8 STDDEV in SELECT, HAVING reads only COUNT",
      "SELECT city, COUNT(*) AS c, STDDEV(amount) AS sd FROM customers GROUP BY city HAVING COUNT(*) > 1",
      None
    ),
    Cell(
      "N9 MIN is computable",
      "SELECT city, MIN(amount) AS mn FROM customers GROUP BY city HAVING MIN(amount) > 1",
      None
    ),
    Cell(
      "N10 COUNT DISTINCT is computable",
      "SELECT city, COUNT(DISTINCT id) AS cd FROM customers GROUP BY city HAVING COUNT(DISTINCT id) > 1",
      None
    ),
    // (R) an aggregate on the RIGHT of the comparison. 🔴 The population had NO row comparing two
    // aggregates -- every earlier cell is `<agg> <op> <literal>` or `<key> <op> <literal>` -- which
    // is why a whole rule could be added without one assertion moving.
    //
    // extensions' `Stage.extractAggregatePaths` walks `expr.identifier` ONLY and never
    // `expr.maybeValue`, while core's own `Expression.extractAllMetricsPath` walks BOTH. So
    // publishing the right-hand aggregate puts it in `buildAggregations` and NEVER in
    // `buckets_path`. MEASURED: the selector IS built, reads `params.<right>` which is null, the
    // null guard short-circuits, and EVERY bucket is rejected -- an EMPTY view at HTTP 200.
    Cell(
      "R1 MAX > MIN, both published",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R2 same, alias spelling",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city HAVING mx > mn",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R3 reversed operand order",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MIN(amount) < MAX(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R4 inequality",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MAX(amount) <> MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R5 SUM > AVG",
      "SELECT city, SUM(amount) AS sm, AVG(amount) AS av FROM customers GROUP BY city " +
      "HAVING SUM(amount) > AVG(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R6 beside a well-formed metric, AND",
      "SELECT city, COUNT(*) AS c, MAX(amount) AS mx, MIN(amount) AS mn FROM customers " +
      "GROUP BY city HAVING COUNT(*) > 1 AND MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "R7 beside a well-formed metric, OR",
      "SELECT city, COUNT(*) AS c, MAX(amount) AS mx, MIN(amount) AS mn FROM customers " +
      "GROUP BY city HAVING COUNT(*) > 1 OR MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    // 🔴 The other direction: the right-hand aggregate is NOT published either. Both rules apply;
    // the ordering test below pins which one speaks, and it must be this one -- rule 5's remedy
    // ("add it to the SELECT list") is MEASURED to produce R1.
    Cell(
      "R8 right-hand aggregate NOT published",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    // (V) 🔴 A right-hand aggregate is undeclared ONLY when its name appears on no leaf's LEFT
    // side anywhere in the clause -- `Stage.extractAggregatePaths` declares a parameter for the
    // left identifier of EVERY leaf in the whole tree. A sibling conjunct that names the same
    // aggregate therefore declares it, and the transform deploys and runs correctly.
    // MEASURED on the control: UNDECLARED = none, selector built, both aggregations created.
    // These MUST stay accepted; refusing them is a regression against main.
    Cell(
      "V1 sibling conjunct declares it, AND",
      "SELECT city, SUM(amount) AS m, MAX(amount) AS m2 FROM customers GROUP BY city " +
      "HAVING SUM(amount) > MAX(amount) AND MAX(amount) > 1",
      None
    ),
    Cell(
      "V2 sibling conjunct declares it, OR",
      "SELECT city, SUM(amount) AS m, MAX(amount) AS m2 FROM customers GROUP BY city " +
      "HAVING SUM(amount) > MAX(amount) OR MAX(amount) > 1",
      None
    ),
    Cell(
      "V3 alias spelling on the left",
      "SELECT city, SUM(amount) AS m, MAX(amount) AS m2 FROM customers GROUP BY city " +
      "HAVING m > MAX(amount) AND m2 > 1",
      None
    ),
    Cell(
      "V4 two grouping keys",
      "SELECT city, status, SUM(amount) AS m, MAX(amount) AS m2 FROM customers " +
      "GROUP BY city, status HAVING SUM(amount) > MAX(amount) AND MAX(amount) > 1",
      None
    ),
    Cell(
      "V5 MIN / AVG pair",
      "SELECT city, MIN(amount) AS mn, AVG(amount) AS av FROM customers GROUP BY city " +
      "HAVING MIN(amount) > AVG(amount) AND AVG(amount) > 1",
      None
    ),
    Cell(
      "V6 the declaring sibling comes FIRST",
      "SELECT city, SUM(amount) AS m, MAX(amount) AS m2 FROM customers GROUP BY city " +
      "HAVING MAX(amount) > 1 AND SUM(amount) > MAX(amount)",
      None
    ),
    // family B: both operands are the SAME aggregate. Tautologically false, but that is what the
    // SQL means and the plain search renders the identical script -- the path declares it.
    Cell(
      "V7 the same aggregate on both sides",
      "SELECT city, MAX(amount) AS m2 FROM customers GROUP BY city HAVING MAX(amount) > MAX(amount)",
      None
    ),
    Cell(
      "V8 the same aggregate, alias spelling",
      "SELECT city, MAX(amount) AS m2 FROM customers GROUP BY city HAVING m2 > m2",
      None
    ),
    // …and the negatives that keep rule 5 alive: nothing declares the right-hand name.
    Cell(
      "V9 no sibling declares it",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "V10 right-hand aggregate not published at all",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > MIN(amount)",
      Some(Rule.TwoAggregates)
    ),
    // (W) 🔴 Which of rule 5 and rule 6 speaks when BOTH apply -- the right-hand aggregate is
    // undeclared AND the SELECT list does not publish it. The discriminator is the COUNTERFACTUAL:
    // would publishing it make the selector declare it? `Stage.extractAggregatePaths` declares the
    // LEFT identifier of EVERY leaf, so the answer is yes exactly when some leaf names it on the
    // left. When it does, rule 6's remedy ends the journey (W5 / W8 are W1 / W3 with the remedy
    // applied literally, and they are ACCEPTED); when nothing names it on the left, publishing it
    // lands on V9 and rule 5 must speak instead (W6, R8/V10 above).
    Cell(
      "W1 a sibling leaf names it on the left, unpublished",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city " +
      "HAVING MIN(amount) > 1 AND MAX(amount) > MIN(amount)",
      Some(Rule.Unpublished)
    ),
    Cell(
      "W2 the same, with a further well-formed conjunct",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city " +
      "HAVING MIN(amount) > 1 AND MAX(amount) > 2 AND MAX(amount) > MIN(amount)",
      Some(Rule.Unpublished)
    ),
    Cell(
      "W3 the leaf that names it on the left is the leaf itself",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MIN(amount) > MIN(amount)",
      Some(Rule.Unpublished)
    ),
    Cell(
      "W4 COUNT(*) named on the left by a sibling",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city " +
      "HAVING COUNT(*) > 1 AND MAX(amount) > COUNT(*)",
      Some(Rule.Unpublished)
    ),
    Cell(
      "W5 W1 with rule 6's remedy applied literally",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MIN(amount) > 1 AND MAX(amount) > MIN(amount)",
      None
    ),
    Cell(
      "W6 nothing names it on the left: publishing it would not help",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city " +
      "HAVING MIN(amount) > 1 AND MIN(amount) > MAX(amount)",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "W7 both published, alias spelling on the right",
      "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MAX(amount) > mn",
      Some(Rule.TwoAggregates)
    ),
    Cell(
      "W8 W3 with rule 6's remedy applied literally",
      "SELECT city, COUNT(*) AS c, MIN(amount) AS mn FROM customers GROUP BY city " +
      "HAVING MIN(amount) > MIN(amount)",
      None
    ),
    // (U) the BACKSTOP population: a HAVING leaf whose selector parameter names no aggregation the
    // pivot creates. Two families, both PRE-EXISTING (accepted on the control too) and both still
    // correct as a plain SELECT.
    //
    // U-1: the aggregate carries NO SELECT alias. `Select.fieldAliases` mints a generated internal
    // alias for an unaliased item and `Identifier.update` copies it, so the script reads that name
    // -- while `RequiredField.apply` names the aggregation `fieldAlias.getOrElse(sourceField)`, the
    // USER alias only. The two never match, `buckets_path` comes back EMPTY and the clause is
    // SILENTLY DROPPED. MEASURED invariant across every computable aggregate, every connective,
    // one and two grouping keys, and a JOIN body. It survived because the estate's only MV+HAVING
    // fixture aliases everything.
    Cell(
      "U1 SUM with no SELECT alias",
      "SELECT city, SUM(amount) FROM customers GROUP BY city HAVING SUM(amount) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U2 COUNT(*) with no SELECT alias",
      "SELECT city, COUNT(*) FROM customers GROUP BY city HAVING COUNT(*) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U3 COUNT(col) with no SELECT alias",
      "SELECT city, COUNT(id) FROM customers GROUP BY city HAVING COUNT(id) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U4 COUNT DISTINCT with no SELECT alias",
      "SELECT city, COUNT(DISTINCT id) FROM customers GROUP BY city HAVING COUNT(DISTINCT id) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U5 MIN with no SELECT alias",
      "SELECT city, MIN(qty) FROM customers GROUP BY city HAVING MIN(qty) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U6 two unaliased aggregates, AND",
      "SELECT city, SUM(amount), MIN(qty) FROM customers GROUP BY city " +
      "HAVING SUM(amount) > 5 AND MIN(qty) > 1",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U7 two grouping keys, unaliased aggregate",
      "SELECT city, status, SUM(amount) FROM customers GROUP BY city, status HAVING SUM(amount) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U8 JOIN body, unaliased aggregate",
      "SELECT c.city, SUM(o.amount) FROM customers c INNER JOIN orders o ON c.id = o.customer_id " +
      "GROUP BY c.city HAVING SUM(o.amount) > 5",
      Some(Rule.NoMatchingAgg)
    ),
    Cell(
      "U9 BETWEEN, unaliased aggregate",
      "SELECT city, SUM(amount) FROM customers GROUP BY city HAVING SUM(amount) BETWEEN 1 AND 9",
      Some(Rule.NoMatchingAgg)
    ),
    // U-2: a relation predicate BESIDE a metric conjunct. `extractAggregatePaths` falls to its
    // `case _ => acc` for an `ElasticRelation`, and `havingLeaves` deliberately excludes relation
    // predicates -- so the grouping-key and two-aggregate rules are STRUCTURALLY blind to it. The
    // selector IS built and reads the metric only: a PARTIAL filter answering 200 with the wrong
    // groups, which is exactly the harm the grouping-key rule's own docstring names.
    Cell(
      "U10 relation predicate beside a metric",
      "SELECT city, SUM(amount) AS s FROM customers GROUP BY city HAVING s > 5 AND child(c.x = 1)",
      Some(Rule.NoMatchingAgg)
    ),
    // (P) the ONE materialized-view example in the shipped documentation that carries a HAVING --
    // `documentation/sql/materialized_views.md` "Materialized View with Aggregations", verbatim.
    // A published example the engine rejects is the issue-#302 family, so it is PINNED rather than
    // reasoned about: every aggregate its HAVING names is in the SELECT list, so it still creates.
    Cell(
      "P1 published MV example",
      "SELECT c.city, c.country, COUNT(*) AS order_count, SUM(o.amount) AS total_amount, " +
      "AVG(o.amount) AS avg_amount, MAX(o.amount) AS max_amount " +
      "FROM orders o JOIN customers c ON o.customer_id = c.id " +
      "WHERE o.status = 'completed' GROUP BY c.city, c.country " +
      "HAVING SUM(o.amount) > 10000 ORDER BY total_amount DESC LIMIT 100",
      None
    )
  )

  "every cell of the population" should "carry the verdict the spec names, in BOTH venues" in {
    population.foreach { cell =>
      withClue(s"[${cell.id}] MATERIALIZED VIEW: ") {
        (verdict(asView(cell.body)), cell.mvRefusedBy) match {
          case (Accepted, None)                  => succeed
          case (Refused(reason), Some(property)) => reason should include(property)
          case (Accepted, Some(property)) =>
            fail(s"expected a refusal naming '$property', got ACCEPTED")
          case (Refused(reason), None) => fail(s"expected ACCEPTED, got refused: $reason")
        }
      }
      // 🔴 The other direction of AC-2: the SAME body, as a plain search, is ALWAYS accepted.
      withClue(s"[${cell.id}] plain SELECT: ") {
        verdict(cell.body) shouldBe Accepted
      }
    }
  }

  it should "exercise every rule and both outcomes" in {
    // Non-vacuity, computed over the table the rows are drawn from -- not a literal count.
    val refusals = population.flatMap(_.mvRefusedBy).distinct
    refusals should contain theSameElementsAs Rule.all
    population.count(_.mvRefusedBy.isEmpty) should be > 0
  }

  it should "key each rule on a property no OTHER rule's message carries" in {
    // 🔴 The population above reads a rule off a substring. If two messages shared a key, a row
    // could pass while a DIFFERENT rule fired -- so the keys are proved mutually exclusive
    // against the real messages, not eyeballed.
    val canonical: Map[String, String] = Map(
      Rule.NoGroupBy -> refusalOf(
        asView("SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1")
      ),
      Rule.GroupKey -> refusalOf(
        asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'")
      ),
      Rule.NoTransformAgg -> refusalOf(
        asView(
          "SELECT city, STDDEV(amount) AS sd FROM customers GROUP BY city HAVING STDDEV(amount) > 1"
        )
      ),
      Rule.BucketScript -> refusalOf(
        asView(
          "SELECT city, MAX(amount) - MIN(amount) AS d FROM customers GROUP BY city HAVING d > 3"
        )
      ),
      Rule.TwoAggregates -> refusalOf(
        asView(
          "SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city " +
          "HAVING MAX(amount) > MIN(amount)"
        )
      ),
      Rule.Unpublished -> refusalOf(
        asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1")
      ),
      Rule.NoMatchingAgg -> refusalOf(
        asView("SELECT city, SUM(amount) FROM customers GROUP BY city HAVING SUM(amount) > 5")
      )
    )
    canonical.foreach { case (key, message) =>
      withClue(s"[$key] ") {
        message should include(key)
        Rule.all.filterNot(_ == key).foreach { other =>
          withClue(s"also carries the key of another rule ($other): ") {
            message should not include other
          }
        }
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // Rule 3's oracle: the verdict must agree with `AggregateConversion.toTransformAggregation`
  // ITSELF -- the function the transform emitter calls -- never with a list of function names.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "the unbuildable-aggregate rule" should "agree with toTransformAggregation on every spelling" in {
    import app.softnetwork.elastic.sql.transform.AggregateConversion
    // The SPELLINGS are written (one must write SQL); no EXPECTATION is. Each row's verdict is
    // computed by asking the emitter's own mapping, so the rule and the emitter cannot disagree.
    val spellings = Seq(
      "COUNT(*)",
      "COUNT(id)",
      "COUNT(DISTINCT id)",
      "MIN(amount)",
      "MAX(amount)",
      "SUM(amount)",
      "AVG(amount)",
      "STDDEV(amount)",
      "STDDEV_POP(amount)",
      "STDDEV_SAMP(amount)",
      "VARIANCE(amount)",
      "VAR_POP(amount)",
      "VAR_SAMP(amount)",
      "PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY amount)",
      "PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY amount)"
    )
    var computable, refused, notExercised = 0
    val skipped = Seq.newBuilder[String]
    spellings.foreach { agg =>
      val body = s"SELECT city, $agg AS m FROM customers GROUP BY city HAVING $agg > 1"
      val oracle = Parser(body) match {
        case Right(s: SingleSearch) =>
          s.having
            .flatMap(_.criteria)
            .map(_.extractAggregationFields)
            .getOrElse(Seq.empty)
            .flatMap(_.identifier.aggregateFunction)
            .headOption
            .map(_.toTransformAggregation.isDefined)
        case _ => None
      }
      oracle match {
        case None =>
          // Three outcomes, never two: an unparseable or non-aggregate spelling is REPORTED.
          notExercised += 1
          skipped += agg
        case Some(true) =>
          computable += 1
          withClue(s"[$agg] toTransformAggregation says computable, so the view must create: ") {
            verdict(asView(body)) shouldBe Accepted
          }
        case Some(false) =>
          refused += 1
          withClue(s"[$agg] toTransformAggregation says None, so the view must refuse: ") {
            refusalOf(asView(body)) should include(Rule.NoTransformAgg)
          }
      }
    }
    info(s"computable=$computable refused=$refused notExercised=$notExercised ${skipped.result()}")
    // Both outcomes must actually occur, or the oracle proved nothing.
    computable should be > 0
    refused should be > 0
  }

  it should "leave no AggregateFunction leaf silently unconsidered" in {
    // Completeness, walked off the SEALED hierarchy rather than a hand list. Leaves the spellings
    // above do not reach are REPORTED with a count -- the third outcome that otherwise becomes
    // "fine" by default. Window ranking functions legitimately live here and are not aggregates a
    // pivot ever computes.
    import scala.reflect.runtime.{universe => ru}
    def leaves(sym: ru.ClassSymbol): Set[ru.ClassSymbol] = {
      val subs = sym.knownDirectSubclasses.map(_.asClass)
      if (subs.isEmpty) Set(sym) else subs.flatMap(leaves)
    }
    val names =
      leaves(
        ru.typeOf[app.softnetwork.elastic.sql.function.aggregate.AggregateFunction]
          .typeSymbol
          .asClass
      )
        .map(_.name.toString)
    names.size should be > 5
    info(s"AggregateFunction leaves (${names.size}): ${names.toSeq.sorted.mkString(", ")}")
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // The MV-only separation, at the MECHANISM the emission reads (AC-2, RENDER level).
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "a key condition refused in a view" should "still reach the terms include/exclude channel in a search" in {
    // `Expression.includes` / `excludes` IS what the bridge calls to build the `terms` filter --
    // the same function `SingleSearch.keyExpressibleByTerms` asks, never a copy of its rules.
    val empty = BucketIncludesExcludes()
    Seq(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'" -> Set(
        "Paris"
      ),
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city IN ('Paris','Lyon')" -> Set(
        "Paris",
        "Lyon"
      ),
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris' OR city = 'Lyon'" -> Set(
        "Paris",
        "Lyon"
      )
    ).foreach { case (sql, expected) =>
      withClue(s"[$sql] ") {
        val s = parsedSearch(sql)
        val bucket = s.buckets.headOption.getOrElse(fail("no bucket"))
        val criteria = s.having.flatMap(_.criteria).getOrElse(fail("no HAVING"))
        criteria.includes(bucket, not = false, empty).values shouldBe expected
      }
    }
    val excluded = parsedSearch(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city <> 'Paris'"
    )
    val bucket = excluded.buckets.headOption.getOrElse(fail("no bucket"))
    excluded.having
      .flatMap(_.criteria)
      .getOrElse(fail("no HAVING"))
      .excludes(bucket, not = false, empty)
      .values shouldBe Set("Paris")
  }

  "an unpublished aggregate refused in a view" should "still become an auxiliary aggregation in a search" in {
    // A search CREATES the aggregation the HAVING needs; a transform does not. That asymmetry is
    // the whole reason the rule is MV-only, so it is asserted rather than described.
    val s = parsedSearch(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1"
    )
    s.havingOnlyAggs.map(_.identifier.sql) shouldBe Seq("MAX(amount)")
    s.sqlAggregations.keys.toSeq should contain("max_amount")
  }

  "a whole-table HAVING refused in a view" should "still be evaluated by a search" in {
    parsedSearch(
      "SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1"
    ).wholeTableHaving shouldBe true
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // Message contract and rule precedence.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "each refusal" should "name the clause, the reason and a remedy" in {
    val key = refusalOf(
      asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'")
    )
    key should include("MATERIALIZED VIEW")
    key should include("city = 'Paris'")
    key should include("include")
    key should include("WHERE")

    val noGroupBy = refusalOf(asView("SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1"))
    noGroupBy should include("MATERIALIZED VIEW")
    noGroupBy should include("no GROUP BY")
    noGroupBy should include("Add a GROUP BY")

    val unpublished = refusalOf(
      asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1")
    )
    unpublished should include("MATERIALIZED VIEW")
    unpublished should include("MAX(amount)")
    unpublished should include("SELECT list")
  }

  it should "carry no internal work-item identifier" in {
    Seq(
      asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'"),
      asView("SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1"),
      asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1")
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val reason = refusalOf(sql)
        reason should not include "#"
        reason.toLowerCase should not include "story"
      }
    }
  }

  "the no-GROUP-BY rule" should "win over every rule that needs a pivot" in {
    // The structural reason is the more useful one, and the precedence is pinned so it cannot
    // drift silently. Two rows, because a single assertion is the whole guard otherwise.
    refusalOf(asView("SELECT COUNT(*) AS c FROM customers HAVING MAX(amount) > 1")) should
    include(Rule.NoGroupBy)
    refusalOf(asView("SELECT STDDEV(amount) AS sd FROM customers HAVING STDDEV(amount) > 1")) should
    include(Rule.NoGroupBy)
  }

  "the grouping-key rule" should "win over the aggregate rules" in {
    refusalOf(
      asView(
        "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris' AND MAX(amount) > 1"
      )
    ) should include(Rule.GroupKey)
    refusalOf(
      asView(
        "SELECT city, STDDEV(amount) AS sd FROM customers GROUP BY city " +
        "HAVING city = 'Paris' AND STDDEV(amount) > 1"
      )
    ) should include(Rule.GroupKey)
  }

  "the unbuildable-aggregate rule" should "win over the unpublished-aggregate rule" in {
    // 🔴 Order matters for a REASON, not for taste: the unpublished rule's remedy is "add it to
    // the SELECT list", and for an aggregate no transform can compute that remedy DOES NOT WORK --
    // MEASURED, adding it produces the buckets_path-names-a-missing-aggregation state that the
    // unbuildable rule refuses. A refusal whose remedy leads somewhere worse is the #389 trap.
    val notInSelect =
      asView("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING STDDEV(amount) > 1")
    refusalOf(notInSelect) should include(Rule.NoTransformAgg)
    refusalOf(notInSelect) should not include Rule.Unpublished
    // and the remedy the OTHER rule would have prescribed is itself refused, which is the proof
    // that the ordering is load-bearing rather than cosmetic.
    refusalOf(
      asView(
        "SELECT city, COUNT(*) AS c, STDDEV(amount) AS sd FROM customers GROUP BY city " +
        "HAVING STDDEV(amount) > 1"
      )
    ) should include(Rule.NoTransformAgg)
  }

  "the two-aggregate rule" should "win over the unpublished-aggregate rule" in {
    // 🔴 Same reason as rule 3's precedence, one operand over: rule 5's remedy is "add it to the
    // SELECT list", and MEASURED, applying it to this statement produces R1 -- a selector that
    // reads an undeclared parameter and rejects every group. A refusal whose remedy leads
    // somewhere worse is a defect, not a nicety.
    val reason = refusalOf(
      asView(
        "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > MIN(amount)"
      )
    )
    reason should include(Rule.TwoAggregates)
    reason should not include Rule.Unpublished
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // Each message must name what actually fired.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "the grouping-key message" should "not call a window PARTITION BY key a GROUP BY key" in {
    // `SingleSearch.buckets` is `bucketTree.allBuckets`, which carries window PARTITION BY keys
    // too, so this reaches the key arm with `status` -- which is NOT a GROUP BY key. The refusal
    // is right (a transform expresses neither channel); the noun had to stop being wrong.
    val reason = refusalOf(
      asView(
        "SELECT city, COUNT(*) AS c, MAX(amount) OVER (PARTITION BY status) AS mo " +
        "FROM customers GROUP BY city HAVING status = 'a'"
      )
    )
    reason should include("grouping key")
    reason should not include "GROUP BY key"
  }

  "the unpublished-aggregate message" should "not tell the user to add something already there" in {
    // `SELECT COUNT(id) AS c … HAVING COUNT(`id`)` really does build a second metric, so the
    // refusal is right -- but a bare "add COUNT(\"id\") to the SELECT list" reads as nonsense
    // when COUNT(id) is visibly in the SELECT list. The message has to say WHY.
    refusalOf(
      asView(
        "SELECT `category`, COUNT(id) AS c FROM customers GROUP BY `category` HAVING COUNT(`id`) > 1"
      )
    ) should include("spelling the aggregate exactly as the HAVING spells it")
  }

  /** 🔴 A remedy must not steer the user into another refusal. Three rounds in a row a remedy WAS
    * the defect, so each one is now applied LITERALLY here and its result asserted.
    *
    * The trap this closes: every remedy that puts an aggregate in the SELECT list has to say `AS
    * <alias>`, because a view's HAVING can only read an aggregate that carries a SELECT alias
    * -- and a HAVING never spells an alias, so "spell it exactly as the HAVING spells it" was, on
    * its own, the instruction that produced the broken artefact.
    */
  "every remedy" should "name the SELECT alias, and be accepted when applied literally" in {
    Seq(
      "R1 add a GROUP BY" ->
      ("SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 5",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 5"),
      "R2 filter the key in WHERE" ->
      ("SELECT city, SUM(amount) AS s FROM customers GROUP BY city HAVING city = 'Paris'",
      "SELECT city, SUM(amount) AS s FROM customers WHERE city = 'Paris' GROUP BY city HAVING SUM(amount) > 5"),
      "R5 compare with a constant" ->
      ("SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city HAVING MAX(amount) > MIN(amount)",
      "SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > 5"),
      "R6 add it to the SELECT list" ->
      ("SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MIN(qty) > 5",
      "SELECT city, COUNT(*) AS c, MIN(qty) AS mq FROM customers GROUP BY city HAVING MIN(qty) > 5")
    ).foreach { case (label, (refusedBody, remedyApplied)) =>
      withClue(s"[$label] the refusal must prescribe a SELECT alias: ") {
        // The PROPERTY, not one spelling: the message tells the user to write `AS <something>`
        // and says the word alias. Both halves matter -- an `AS` with no explanation reads as
        // noise, and "alias" with no `AS` does not show what to type.
        val reason = refusalOf(asView(refusedBody))
        // 🔴 `include("alias")` alone is satisfied by the `AS <alias>` PLACEHOLDER itself, so a
        // mutation that deletes the explanation stayed green. Assert the EXPLANATION: the message
        // has to say that a view's HAVING can only read an aliased aggregate.
        reason should include("AS ")
        reason should include("SELECT alias")
      }
      withClue(s"[$label] the remedy applied literally must be ACCEPTED: ") {
        verdict(asView(remedyApplied)) shouldBe Accepted
      }
    }
  }

  "the backstop message" should "teach the alias rule and name the relation case" in {
    // 🔴 Same weakness M14 exposed one rule over: the population keys on "matches none of the
    // aggregations this view creates", which survives deleting everything that TEACHES. The
    // backstop is the message most users will see for the commonest mistake (no SELECT alias),
    // so its remedy is asserted, not just its diagnosis.
    val noAlias = refusalOf(
      asView("SELECT city, SUM(amount) FROM customers GROUP BY city HAVING SUM(amount) > 5")
    )
    noAlias should include("SELECT list with an alias")
    noAlias should include("SUM(x) AS s")
    val relation = refusalOf(
      asView(
        "SELECT city, SUM(amount) AS s FROM customers GROUP BY city HAVING s > 5 AND child(c.x = 1)"
      )
    )
    relation should include("nested, child or parent predicate")
  }

  it should "refuse the SAME remedy applied WITHOUT an alias" in {
    // The paired negative: drop the `AS` the remedy prescribes and the statement is refused by the
    // backstop rather than silently deployed. This is what makes the alias clause load-bearing.
    Seq(
      "SELECT city, COUNT(*) FROM customers GROUP BY city HAVING COUNT(*) > 5",
      "SELECT city, SUM(amount) FROM customers WHERE city = 'Paris' GROUP BY city HAVING SUM(amount) > 5",
      "SELECT city, MAX(amount) FROM customers GROUP BY city HAVING MAX(amount) > 5",
      "SELECT city, COUNT(*) AS c, MIN(qty) FROM customers GROUP BY city HAVING MIN(qty) > 5"
    ).foreach { body =>
      withClue(s"[$body] ") { refusalOf(asView(body)) should include(Rule.NoMatchingAgg) }
      withClue(s"[$body] still correct as a plain SELECT: ") { verdict(body) shouldBe Accepted }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // The existing three arms keep their precedence, and the existing HAVING rules keep theirs.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "the set-operation, derived-table and WHERE-subquery arms" should "still win" in {
    refusalOf(
      "CREATE MATERIALIZED VIEW mv_probe AS SELECT city FROM customers UNION ALL SELECT city FROM prospects"
    ) should include("set operation")
    refusalOf(
      "CREATE MATERIALIZED VIEW mv_probe AS SELECT d.city FROM (SELECT city FROM customers) AS d"
    ) should include("derived table")
    refusalOf(
      "CREATE MATERIALIZED VIEW mv_probe AS SELECT city FROM customers WHERE id IN (SELECT customer_id FROM orders)"
    ) should include("WHERE subquery")
  }

  /** 🔴 The reason there is no fourth arm relaying `Having.unrepresentable`.
    *
    * `SingleSearch.validate()` already refuses every un-expressible HAVING, and it runs inside
    * `dql.validate()`, i.e. BEFORE the materialized-view rules. An MV arm reading `unrepresentable`
    * would therefore be unsatisfiable for every possible input -- dead code beside live code. These
    * rows are what makes that claim falsifiable: if the shared rule ever stopped firing, they would
    * report the MV message (or an acceptance) instead.
    */
  "an un-expressible HAVING" should "be refused by the shared rule, not by a view-specific one" in {
    Seq(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING ROUND(COUNT(*), 2) > 1",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING ABS(COUNT(*)) > 1",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING NULLIF(COUNT(*), 0) > 1"
    ).foreach { body =>
      withClue(s"[$body] ") {
        val viewReason = refusalOf(asView(body))
        viewReason should include("HAVING cannot be applied to")
        viewReason should not include "MATERIALIZED VIEW"
        // The SAME reason in both venues: the rule is shared, not duplicated.
        refusalOf(body) shouldBe viewReason
      }
    }
  }

  /** 🔴 Why the `isAggregation` guard on `havingValueSideAggs` has a GREEN mutation.
    *
    * A value side that is NOT an aggregate never reaches the view rules: the shared rules inside
    * `dql.validate()` refuse it first. These rows pin THAT, which is what makes the guard dead from
    * SQL -- if a shared rule ever stopped firing, the guard would become live and its mutation
    * would start to matter. Recording the reason beats leaving a green cell unexplained.
    */
  "a non-aggregate on the value side" should "be refused by the shared rules, before the view rules" in {
    Seq(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > amount" ->
      "HAVING cannot be applied to",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > city" ->
      "HAVING cannot be applied to",
      "SELECT city, status, COUNT(*) AS c FROM customers GROUP BY city, status HAVING city = status" ->
      "HAVING cannot filter on"
    ).foreach { case (body, sharedRule) =>
      withClue(s"[$body] ") {
        val reason = refusalOf(asView(body))
        reason should include(sharedRule)
        reason should not include "MATERIALIZED VIEW"
        refusalOf(body) shouldBe reason
      }
    }
    // …and a SELECT alias of an aggregate IS substituted before the guard sees it, so the guard
    // admits it as the aggregate it is rather than excluding it.
    refusalOf(
      asView(
        "SELECT city, COUNT(*) AS c, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > c"
      )
    ) should include(Rule.TwoAggregates)
  }

  "the shared HAVING rules" should "still fire inside a view, unchanged" in {
    Seq(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING UPPER(city) = 'PARIS'" ->
      "HAVING cannot filter on",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING amount > 1" ->
      "HAVING can only filter on",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1 OR city = 'Paris'" ->
      "HAVING cannot OR conditions",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris' AND city = 'Lyon'" ->
      "HAVING cannot combine the conditions"
    ).foreach { case (body, expected) =>
      withClue(s"[$body] ") {
        refusalOf(asView(body)) should include(expected)
        refusalOf(body) should include(expected)
      }
    }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // The refusal reaches every CREATE spelling.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "the refusal" should "reach OR REPLACE, IF NOT EXISTS, REFRESH EVERY and WITH options" in {
    val body = "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'"
    Seq(
      s"CREATE OR REPLACE MATERIALIZED VIEW mv_probe AS $body",
      s"CREATE MATERIALIZED VIEW IF NOT EXISTS mv_probe AS $body",
      s"CREATE MATERIALIZED VIEW mv_probe REFRESH EVERY 60 SECONDS AS $body",
      s"CREATE MATERIALIZED VIEW mv_probe WITH (delay = '1s') AS $body"
    ).foreach { sql =>
      withClue(s"[$sql] ") { refusalOf(sql) should include(Rule.GroupKey) }
    }
  }

  "a view that still creates" should "keep every CREATE spelling too" in {
    val body = "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1"
    Seq(
      s"CREATE OR REPLACE MATERIALIZED VIEW mv_probe AS $body",
      s"CREATE MATERIALIZED VIEW IF NOT EXISTS mv_probe AS $body",
      s"CREATE MATERIALIZED VIEW mv_probe REFRESH EVERY 60 SECONDS AS $body"
    ).foreach { sql => withClue(s"[$sql] ") { verdict(sql) shouldBe Accepted } }
  }

  // ─────────────────────────────────────────────────────────────────────────────────────────────
  // The rules are MV-only by construction: no OTHER statement kind inherits them.
  // ─────────────────────────────────────────────────────────────────────────────────────────────

  "a CREATE TABLE AS SELECT over the same body" should "not inherit the view rules" in {
    // CTAS renders through the ordinary search path, which HAS the include/exclude channel.
    verdict(
      "CREATE TABLE t2 AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'"
    ) shouldBe Accepted
  }
}
