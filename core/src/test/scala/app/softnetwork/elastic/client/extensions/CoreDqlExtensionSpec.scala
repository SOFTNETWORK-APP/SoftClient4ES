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

package app.softnetwork.elastic.client.extensions

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.licensing.metrics.MetricsApi
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{Limit, SearchStatement, SelectStatement, SingleSearch}
import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

/** Unit tests for [[CoreDqlExtension]] — the Community-baseline single-index result cap (Story
  * P0.5). No Docker / no Elasticsearch: a recording [[NopeClientApi]] captures the forwarded
  * [[SearchStatement]] and the [[ScrollConfig]] so we assert the cap DECISION and the attached
  * [[ResultTruncation]] directly.
  *
  * Seam (OQ-2 resolution): the production `CoreDqlExtension` drives `client.scroll(single, config)`
  * for the no-LIMIT cap path and `client.dqlExecutor.execute(single)` (→ `SearchExecutor` →
  * `scroll`/`searchAsync`) for the passthrough paths. The test client overrides BOTH `scroll` and
  * `searchAsync` to record, so every branch is observable without a full ES stub or a `dqlExecutor`
  * spy (which is a concrete `lazy val DqlRouterExecutor` and not cleanly mockable).
  */
class CoreDqlExtensionSpec extends AnyFlatSpec with Matchers {

  implicit val system: ActorSystem = ActorSystem("core-dql-extension-test")

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  // ---- license-manager + strategy stubs (mirror LicenseExecutorSpec) ----

  private def managerWithQuota(q: Quota, tier: LicenseType): LicenseManager =
    new LicenseManager {
      override def validate(key: String): Either[LicenseError, LicenseKey] =
        Left(InvalidLicense("test"))
      override def hasFeature(feature: Feature): Boolean = true
      override def quotas: Quota = q
      override def licenseType: LicenseType = tier
    }

  private def strategy(
    mgr: LicenseManager,
    collector: TelemetryCollector = TelemetryCollector.Noop
  ): LicenseRefreshStrategy =
    new LicenseRefreshStrategy {
      override def initialize(): LicenseKey = LicenseKey.Community
      override def refresh(): Either[LicenseError, LicenseKey] = Left(RefreshNotSupported)
      override def licenseManager: LicenseManager = mgr
      override def telemetryCollector: TelemetryCollector = collector
    }

  /** Records every statement forwarded to the scroll / searchAsync seam plus the scroll config, so
    * tests can assert the cap decision (which path, what maxDocuments).
    */
  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = testLogger

    val scrolledStatement = new AtomicReference[SearchStatement]()
    val scrolledConfig = new AtomicReference[ScrollConfig]()
    val searchedStatement = new AtomicReference[SearchStatement]()

    override def scroll(
      statement: SearchStatement,
      config: ScrollConfig = ScrollConfig()
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[(ListMap[String, Any], ScrollMetrics), NotUsed] = {
      scrolledStatement.set(statement)
      scrolledConfig.set(config)
      Source.empty[(ListMap[String, Any], ScrollMetrics)]
    }

    override def searchAsync(
      statement: SearchStatement
    )(implicit
      ec: scala.concurrent.ExecutionContext,
      context: ConversionContext
    ): scala.concurrent.Future[ElasticResult[ElasticResponse]] = {
      searchedStatement.set(statement)
      scala.concurrent.Future.successful(
        ElasticSuccess(
          ElasticResponse(
            sql = None,
            query = "{}",
            results = Seq(ListMap[String, Any]("ok" -> 1)),
            fieldAliases = ListMap.empty,
            aggregations = ListMap.empty
          )
        )
      )
    }
  }

  /** Story 22.6 — the running-budget client. `RecordingClient` above keeps only the LAST scroll (an
    * `AtomicReference`) and serves no rows, so neither the per-leg budget nor the concatenation is
    * observable through it. This one records EVERY scroll with its config and serves `rowsPerLeg`
    * rows, which is what makes "leg 2 was bounded by what leg 1 left" a measurable claim rather
    * than a narrated one.
    */
  private class BudgetClient(rowsPerLeg: Int, failExecute: Boolean = false) extends NopeClientApi {
    override protected def logger: Logger = testLogger

    @volatile var scrolls: Seq[(SearchStatement, ScrollConfig)] = Seq.empty
    @volatile var executed: Seq[SearchStatement] = Seq.empty

    override def scroll(
      statement: SearchStatement,
      config: ScrollConfig = ScrollConfig()
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[(ListMap[String, Any], ScrollMetrics), NotUsed] = {
      synchronized { scrolls = scrolls :+ ((statement, config)) }
      // 🔴 `rowsPerLeg` rows REGARDLESS of the statement's own LIMIT — because that is exactly what
      // the real `ScrollApi.scroll(statement, config)` does (issue O1, pre-existing: it honours
      // `config.maxDocuments` and IGNORES the statement's `LIMIT`). A stub that honoured the LIMIT
      // would hide the defect this fixture exists to catch.
      val served = config.maxDocuments.map(_.toInt.min(rowsPerLeg)).getOrElse(rowsPerLeg)
      Source(
        (1 to served)
          .map(i => (ListMap[String, Any]("a" -> i), ScrollMetrics()))
          .toList
      )
    }

    /** The route a LIMITed row leg and an aggregation leg take. It HONOURS the statement's LIMIT,
      * which is the whole reason such a leg must not be scrolled.
      */
    override def searchAsync(
      statement: SearchStatement
    )(implicit
      ec: scala.concurrent.ExecutionContext,
      context: ConversionContext
    ): scala.concurrent.Future[ElasticResult[ElasticResponse]] = {
      // 🔴 A leg bounded ABOVE `index.max_result_window` must reach the REAL routing, which sends
      // it through `scroll` and answers `QueryStream`. Intercepting every statement here made that
      // arm unreachable, so the test written for it passed with the arm disabled.
      val needsPaging = statement match {
        case s: SingleSearch => s.returnsRows && s.limit.exists(_.limit > 10000)
        case _               => false
      }
      if (needsPaging) return super.searchAsync(statement)
      synchronized { executed = executed :+ statement }
      if (failExecute)
        return scala.concurrent.Future.successful(
          ElasticFailure(ElasticError(message = "branch blew up", statusCode = Some(503)))
        )
      val bound = statement match {
        case s: SingleSearch => s.limit.map(_.limit).getOrElse(rowsPerLeg)
        case _               => rowsPerLeg
      }
      scala.concurrent.Future.successful(
        ElasticSuccess(
          ElasticResponse(
            sql = None,
            query = "{}",
            results = (1 to bound.min(rowsPerLeg)).map(i => ListMap[String, Any]("a" -> i)),
            fieldAliases = ListMap.empty,
            aggregations = ListMap.empty
          )
        )
      )
    }
  }

  private def runWith(
    sql: String,
    quota: Quota,
    client: ElasticClientApi,
    tier: LicenseType = LicenseType.Community
  ): ElasticResult[QueryResult] = {
    val parsed = Parser(sql) match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    Await.result(newExtension(quota, tier).execute(parsed, client), 10.seconds)
  }

  private def newExtension(quota: Quota, tier: LicenseType): CoreDqlExtension = {
    val ext = new CoreDqlExtension()
    ext.initialize(ConfigFactory.empty(), strategy(managerWithQuota(quota, tier)))
    ext
  }

  private def run(
    sql: String,
    quota: Quota,
    tier: LicenseType = LicenseType.Community
  ): (RecordingClient, ElasticResult[QueryResult]) = {
    val parsed = Parser(sql) match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    val client = new RecordingClient()
    val ext = newExtension(quota, tier)
    val res = Await.result(ext.execute(parsed, client), 5.seconds)
    (client, res)
  }

  private def truncationOf(res: ElasticResult[QueryResult]): Option[ResultTruncation] =
    res.asInstanceOf[ElasticSuccess[QueryResult]].value match {
      case q: QueryRows       => q.truncation
      case q: QueryStream     => q.truncation
      case q: QueryStructured => q.truncation
      case other              => fail(s"unexpected result variant: $other")
    }

  // ---- AC-6: routing verdict guard ----

  behavior of "CoreDqlExtension routing"

  it should "parse a single-index SELECT to a bare SingleSearch (not SelectStatement)" in {
    Parser("SELECT a, b FROM idx") match {
      case Right(_: SingleSearch) => succeed
      case Right(_: SelectStatement) =>
        fail("parser wrapped SELECT in SelectStatement — routing assumption broken")
      case other => fail(s"unexpected parse result: $other")
    }
  }

  // ---- AC-1 + AC-2: no-LIMIT on Community → capped scroll + truncation ----

  behavior of "CoreDqlExtension no-LIMIT cap (Community)"

  it should "drive a capped scroll (maxDocuments = quota) and flag truncation for a no-LIMIT single-index query" in {
    val (client, res) = run("SELECT a, b FROM idx", Quota.Community)

    res shouldBe a[ElasticSuccess[_]]

    // AC-1: the scroll was driven bounded by maxDocuments = the Community quota.
    client.scrolledStatement.get() shouldBe a[SingleSearch]
    client.scrolledConfig.get().maxDocuments shouldBe Some(10000L)
    // searchAsync must NOT be used for the over-quota path (would hit max_result_window on Pro).
    client.searchedStatement.get() shouldBe null

    // AC-2: truncation present and non-silent.
    val trunc = truncationOf(res)
    trunc.map(_.truncated) shouldBe Some(true)
    trunc.map(_.limit) shouldBe Some(10000L)
    trunc.flatMap(t => Some(t.warning)).getOrElse("") should not be empty
  }

  /** Story 22.2 (AD-8 / PD-4) — ROUTING and the licensed cap do not move for a WHERE subquery.
    *
    * ⚠️ Scope of this row, stated so it is not read as more than it is: `RecordingClient` overrides
    * `scroll` wholesale and therefore never crosses `resolveWithSchema`, so nothing here exercises
    * the subquery PHASE — it asserts only that the statement is claimed by `canHandle` as an
    * ordinary `DqlStatement`, takes the scroll arm rather than `searchAsync`, and is capped at the
    * OUTER quota (the inner statement is not quota-checked, PD-4). The phase itself is proved
    * Docker-free in `SubqueryResolverSpec` and `RelationalClosureGuardSpec`, and on a real cluster
    * in `WhereSubqueryCompletenessSpec`.
    */
  it should "route an uncorrelated WHERE subquery exactly like a plain SELECT (story 22.2)" in {
    val (client, res) =
      run("SELECT a, b FROM idx WHERE a IN (SELECT a FROM other)", Quota.Community)
    res shouldBe a[ElasticSuccess[_]]
    client.scrolledStatement.get() shouldBe a[SingleSearch]
    client.scrolledConfig.get().maxDocuments shouldBe Some(10000L) // the OUTER cap, unchanged
    client.searchedStatement.get() shouldBe null
    truncationOf(res).map(_.limit) shouldBe Some(10000L)
  }

  it should "cap a no-LIMIT query at the Pro quota (1,000,000) via scroll, never searchAsync" in {
    val (client, res) = run("SELECT a, b FROM idx", Quota.Pro, LicenseType.Pro)

    res shouldBe a[ElasticSuccess[_]]
    client.scrolledConfig.get().maxDocuments shouldBe Some(1000000L)
    client.searchedStatement.get() shouldBe null
    truncationOf(res).map(_.limit) shouldBe Some(1000000L)
  }

  // ---- AC-3: explicit LIMIT within quota → untouched, no flag ----

  behavior of "CoreDqlExtension explicit LIMIT within quota"

  it should "forward an explicit LIMIT <= quota unchanged with no truncation flag" in {
    val (client, res) = run("SELECT a FROM idx LIMIT 50", Quota.Community)

    res shouldBe a[ElasticSuccess[_]]
    // limit defined → SearchExecutor routes to searchAsync, not the capped scroll.
    client.searchedStatement.get() match {
      case s: SingleSearch => s.limit.map(_.limit) shouldBe Some(50)
      case other           => fail(s"expected SingleSearch via searchAsync, got $other")
    }
    client.scrolledConfig.get() shouldBe null
    truncationOf(res) shouldBe None
  }

  // ---- searchAsync-branch carve-out: no-LIMIT aggregation / empty-fields query is NOT capped ----

  behavior of "CoreDqlExtension searchAsync-branch carve-out"

  it should "NOT cap a no-LIMIT aggregation query (empty fields → searchAsync, bounded by max_result_window)" in {
    // GROUP BY ⇒ SingleSearch.fields is empty ⇒ SearchExecutor routes to searchAsync, which ES
    // bounds by index.max_result_window (≤10k ≤ quota). Capping it would mishandle aggregation
    // buckets and re-route to scroll, so the cap MUST fall through to plain execution.
    val (client, res) = run("SELECT category, count(*) FROM idx GROUP BY category", Quota.Community)

    res shouldBe a[ElasticSuccess[_]]
    // executed via searchAsync, NOT the capped scroll, and NOT truncation-flagged.
    client.searchedStatement.get() shouldBe a[SingleSearch]
    client.scrolledConfig.get() shouldBe null
    truncationOf(res) shouldBe None
  }

  // ---- issue #253: an aggregate-free GROUP BY is aggregation-shaped, so the row cap must NOT
  //      fire and the statement must NOT be re-routed to a capped scroll ----

  it should "NOT cap a no-LIMIT aggregate-free GROUP BY (issue #253)" in {
    // Pre-fix `returnsRows` was true (no aggregate anywhere ⇒ `sqlAggregations` empty), so rule (2)
    // drove a `cappedScroll` at maxDocuments = 10,000 and the caller got per-DOCUMENT rows. This is
    // the aggregate-free twin of the carve-out test above and must behave identically to it.
    val (client, res) = run("SELECT category FROM idx GROUP BY category", Quota.Community)

    res shouldBe a[ElasticSuccess[_]]
    client.scrolledConfig.get() shouldBe null
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe a[SingleSearch]
    truncationOf(res) shouldBe None
  }

  it should "still reject an explicit LIMIT over quota on an aggregate-free GROUP BY (no bypass)" in {
    // Rule (1) does not key on `returnsRows`, so the 402 asymmetry is unchanged by the #253 fix.
    // Expected GREEN before the fix too -- this is a no-regression pin for the licence argument,
    // not a RED.
    val (client, res) =
      run("SELECT category FROM idx GROUP BY category LIMIT 20000", Quota.Community)

    res shouldBe a[ElasticFailure]
    res.asInstanceOf[ElasticFailure].elasticError.statusCode shouldBe Some(402)
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  // ---- AC-4: explicit LIMIT over quota → 402 (intentional asymmetry) ----

  behavior of "CoreDqlExtension explicit LIMIT over quota"

  it should "reject an explicit LIMIT > quota with HTTP 402 (no truncation, no execution)" in {
    val (client, res) = run("SELECT a FROM idx LIMIT 20000", Quota.Community)

    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(402)
    err.operation shouldBe Some("license")
    // never forwarded to any execution seam
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  // ---- AC-5: Enterprise (unlimited) → uncapped ----

  behavior of "CoreDqlExtension Enterprise unlimited"

  it should "not cap a no-LIMIT query when maxQueryResults is None (Enterprise)" in {
    val (client, res) = run("SELECT a, b FROM idx", Quota.Enterprise, LicenseType.Enterprise)

    res shouldBe a[ElasticSuccess[_]]
    // no-LIMIT + unlimited quota → plain passthrough scroll with the DEFAULT config (no cap).
    client.scrolledStatement.get() shouldBe a[SingleSearch]
    client.scrolledConfig.get().maxDocuments shouldBe None
    truncationOf(res) shouldBe None
  }

  it should "not reject an arbitrarily large explicit LIMIT on Enterprise" in {
    val (client, res) =
      run("SELECT a FROM idx LIMIT 50000000", Quota.Enterprise, LicenseType.Enterprise)
    res shouldBe a[ElasticSuccess[_]]
    client.searchedStatement.get() match {
      case s: SingleSearch => s.limit.map(_.limit) shouldBe Some(50000000)
      case other           => fail(s"expected SingleSearch via searchAsync, got $other")
    }
  }

  // ---- AC (join-leg carve-out): suppress flag bypasses the cap ----

  behavior of "CoreDqlExtension join-leg suppression"

  it should "NOT cap a no-LIMIT query when ResultCapContext is suppressed (a JOIN leg)" in {
    val parsed = Parser("SELECT a, b FROM idx") match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    val client = new RecordingClient()
    val ext = newExtension(Quota.Community, LicenseType.Community)

    val res = ResultCapContext.suppressed {
      Await.result(ext.execute(parsed, client), 5.seconds)
    }

    res shouldBe a[ElasticSuccess[_]]
    // suppressed → falls through to the plain passthrough scroll (default config, no cap), and
    // carries NO truncation flag, so the JOIN input is not silently truncated.
    client.scrolledStatement.get() shouldBe a[SingleSearch]
    client.scrolledConfig.get().maxDocuments shouldBe None
    truncationOf(res) shouldBe None
  }

  // ---- Issue #157: cross-index JOIN must fail loudly on a join-incapable handler ----

  behavior of "CoreDqlExtension cross-index JOIN guard (#157)"

  it should "reject a cross-index JOIN with a clear 400 instead of silently executing the left table only" in {
    val (client, res) = run(
      "SELECT e.name, d.dept_name FROM emp e JOIN dept d ON e.dept_id = d.dept_id",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("join")
    err.message should include("softclient4es-arrow-extensions")
    // never forwarded to any execution seam — no silent left-table-only search.
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  // `UNION ALL`, not `UNION`: the token is `Expr("UNION ALL")`, so a bare `UNION` never matched
  // the separator. Until #213 made `Parser.apply` require the whole input, this statement parsed
  // as its first leg alone and everything from `UNION` on was silently discarded — so the rejection
  // asserted below was firing on a single-leg SELECT, not on a union.
  it should "reject a UNION whose leg carries a cross-index JOIN" in {
    val (client, res) = run(
      "SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id UNION ALL SELECT name FROM others",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("join")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  it should "NOT reject a JOIN UNNEST (nested-field unnest, handled natively)" in {
    val (_, res) = run(
      "SELECT o.id, oi.quantity FROM orders o JOIN UNNEST(o.items) AS oi",
      Quota.Community
    )
    res shouldBe a[ElasticSuccess[_]]
  }

  it should "reject an INSERT ... SELECT whose inner SELECT carries a cross-index JOIN" in {
    val (client, res) = run(
      "INSERT INTO target SELECT e.name, d.dept_name FROM emp e JOIN dept d ON e.dept_id = d.dept_id",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("join")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  it should "reject a CREATE TABLE ... AS SELECT whose inner SELECT carries a cross-index JOIN" in {
    val (client, res) = run(
      "CREATE TABLE target AS SELECT e.name, d.dept_name FROM emp e JOIN dept d ON e.dept_id = d.dept_id",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("join")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  it should "claim write-with-JOIN statements in canHandle but leave join-less writes to core executors" in {
    val ext = new CoreDqlExtension()
    def parsed(sql: String) = Parser(sql) match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    ext.canHandle(
      parsed("INSERT INTO target SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id")
    ) shouldBe true
    ext.canHandle(
      parsed("CREATE TABLE target AS SELECT e.name FROM emp e JOIN dept d ON e.dept_id = d.dept_id")
    ) shouldBe true
    // join-less writes keep their existing routing (core DML/DDL executors).
    ext.canHandle(parsed("INSERT INTO target SELECT id, name FROM old_users")) shouldBe false
    ext.canHandle(parsed("CREATE TABLE target AS SELECT id, name FROM old_users")) shouldBe false
  }

  // ---- Story 22.1: the guard widens from "cross-index JOIN" to "relational closure" ----

  behavior of "CoreDqlExtension relational-closure guard (story 22.1)"

  /** AC 6's proof, and the reason `returnsRows` needed no change: a derived-table statement reports
    * `returnsRows == true` (it has no aggregate and no GROUP BY), yet NOTHING routes on that,
    * because the closure arm runs before `checkQuotasAndExecute`. Asserted through the
    * RecordingClient seam rather than argued.
    */
  it should "never reach scroll or searchAsync for a FROM-derived statement" in {
    val (client, res) = run("SELECT COL FROM (SELECT 1 AS COL) AS d", Quota.Community)
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.operation shouldBe Some("join")
    err.message should include("softclient4es-arrow-extensions")
    err.message should include("derived table")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  it should "reject a JOIN-derived statement the same way" in {
    val (client, res) = run(
      "SELECT o.id, d.cid FROM orders o JOIN (SELECT cid FROM orders) AS d ON o.id = d.cid",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    res.asInstanceOf[ElasticFailure].elasticError.message should include("derived table")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  /** Story 22.3b — the CORRELATED shape reaches the SAME guard through the SAME predicate, with
    * zero edit in this extension: `relationalClosureRequired` gained one disjunct and every venue
    * inherited it. The falsifiable half is that NEITHER client seam is touched — a correlated
    * statement that leaked past the guard would have been scrolled against the OUTER index alone,
    * silently dropping the correlation (HTTP 200, wrong answer).
    */
  it should "reject a CORRELATED WHERE subquery, naming the shape, and never execute it" in {
    val (client, res) = run(
      "SELECT c.id FROM customers c WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.message should include("A correlated subquery")
    err.message should include("softclient4es-arrow-extensions")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  /** Story 22.5 — a CTE statement reaches the SAME guard through the SAME predicate, with zero edit
    * in this extension: `relationalClosureRequired` gained one disjunct and every venue inherited
    * it.
    *
    * The falsifiable half is the pair of `null` seam assertions. A CTE statement reports
    * `returnsRows == true` (no aggregate in the OUTER select), so had it slipped past the closure
    * arm it would have been SCROLLED against an index named after the CTE's alias — `monthly` is
    * not an index, and the alias is what `sources` yields.
    */
  it should "reject a CTE statement, naming the WITH clause, and never reach scroll or search" in {
    val (client, res) = run(
      "WITH monthly AS (SELECT category, SUM(amount) AS total FROM bi_events GROUP BY category) " +
      "SELECT * FROM monthly",
      Quota.Community
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(400)
    err.message should include("WITH clause")
    err.message should include("softclient4es-arrow-extensions")
    // It must be named as the construct the analyst WROTE, not as the derived table it becomes.
    err.message should not include "derived table"
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  it should "reject a CTE statement even when no CTE is referenced" in {
    // The arrow regex classifier keys on the leading token and cannot count references; the AST
    // predicate must agree with it or the two venues disagree about who owns the statement.
    val (client, res) = run("WITH u AS (SELECT 1 AS x) SELECT a FROM t", Quota.Community)
    res shouldBe a[ElasticFailure]
    res.asInstanceOf[ElasticFailure].elasticError.message should include("WITH clause")
    client.scrolledStatement.get() shouldBe null
    client.searchedStatement.get() shouldBe null
  }

  /** Story 22.6 — a set operation reaches the SAME guard through the SAME predicate, with zero edit
    * in this extension. The falsifiable half is the pair of `null` seam assertions PLUS the `UNION
    * ALL` control below: a distinct `UNION` that leaked past the guard would have been executed as
    * an `_msearch`, i.e. as `UNION ALL` — returning DUPLICATES with HTTP 200, which is exactly the
    * silent degradation this operator family invites.
    */
  it should "reject UNION / INTERSECT / EXCEPT, naming the set operation, and never execute it" in {
    Seq(
      "SELECT a FROM t UNION SELECT a FROM u",
      "SELECT a FROM t UNION DISTINCT SELECT a FROM u",
      "SELECT a FROM t INTERSECT SELECT a FROM u",
      "SELECT a FROM t INTERSECT ALL SELECT a FROM u",
      "SELECT a FROM t EXCEPT SELECT a FROM u",
      "SELECT a FROM t EXCEPT ALL SELECT a FROM u"
    ).foreach { sql =>
      val (client, res) = run(sql, Quota.Community)
      withClue(s"[$sql] ") {
        res shouldBe a[ElasticFailure]
        val err = res.asInstanceOf[ElasticFailure].elasticError
        err.statusCode shouldBe Some(400)
        err.message should include("A set operation")
        err.message should include("softclient4es-arrow-extensions")
        client.scrolledStatement.get() shouldBe null
        client.searchedStatement.get() shouldBe null
      }
    }
  }

  /** The CONTROL. Without it the row above is satisfied by a guard that refuses every
    * `MultiSearch`, which would take the ES-native fast path down with it.
    */
  it should "still execute a plain UNION ALL (the ES-native fast path is untouched)" in {
    val (client, res) = run(
      "SELECT a FROM t LIMIT 5 UNION ALL SELECT a FROM u LIMIT 5",
      Quota.Community
    )
    res shouldBe a[ElasticSuccess[_]]
    // it reached the client, i.e. it was EXECUTED rather than refused
    client.searchedStatement.get() should not be null
  }

  // ---- story 22.6: the licensed cap moves with the routing (capUnionAll) --------------------

  /** Rule (2): the cap binds on the CONCATENATED total with a RUNNING budget.
    *
    * With `maxQueryResults = 7` and a stub serving 5 rows per leg, a per-LEG cap would return 10
    * rows (5 + 5) and a `n × quota` cap would allow 14. Seven is the only answer a running budget
    * gives — and leg 2's recorded `maxDocuments` (2, not 7) is the mechanism itself rather than a
    * proxy for it.
    */
  it should "cap a UNION ALL on the concatenated total with a running budget, and count ONE cap-hit" in {
    val collector = new TelemetryCollector
    val client = new BudgetClient(rowsPerLeg = 5)
    val ext = new CoreDqlExtension()
    ext.initialize(
      ConfigFactory.empty(),
      strategy(
        managerWithQuota(
          Quota.Community.copy(maxQueryResults = Some(7)),
          LicenseType.Community
        ),
        collector
      )
    )
    val parsed = Parser("SELECT a FROM x UNION ALL SELECT a FROM y") match {
      case Right(st) => st
      case Left(e)   => fail(s"parse failed: ${e.msg}")
    }
    val res = Await.result(ext.execute(parsed, client), 10.seconds)
    res shouldBe a[ElasticSuccess[_]]
    val rows = res.asInstanceOf[ElasticSuccess[QueryResult]].value match {
      case q: QueryRows => q
      case other        => fail(s"expected QueryRows, got $other")
    }
    rows.rows should have size 7
    rows.truncation.map(_.truncated) shouldBe Some(true)
    rows.truncation.map(_.limit) shouldBe Some(7L)
    // the running budget, observed at the client
    client.scrolls should have size 2
    client.scrolls.head._2.maxDocuments shouldBe Some(7L)
    client.scrolls(1)._2.maxDocuments shouldBe Some(2L)
    // ONE cap-hit for the STATEMENT, never one per leg
    capHits(collector)("max_query_results") shouldBe 1L
  }

  it should "never open a scroll for a leg the budget cannot pay for" in {
    val client = new BudgetClient(rowsPerLeg = 5)
    runWith(
      "SELECT a FROM x UNION ALL SELECT a FROM y UNION ALL SELECT a FROM z",
      Quota.Community.copy(maxQueryResults = Some(5)),
      client
    )
    // leg 1 alone spends the whole budget, so legs 2 and 3 are never executed at all
    client.scrolls should have size 1
  }

  /** 🔴 THE REGRESSION THIS STORY INTRODUCED, and the row that would have caught it.
    *
    * `cappedUnionAllRows` scrolled EVERY row-shaped leg, and `ScrollApi.scroll(statement, config)`
    * does not honour the statement's own `LIMIT` — so a branch's `LIMIT` was silently DROPPED.
    * MEASURED on real ES 8.18 over 30- and 24-document indices: `SELECT id FROM a LIMIT 3 UNION ALL
    * SELECT id FROM b` returned 54 rows where SQL says 27, and where `main` (with the #209
    * truncation still in place) returned 13. `QueryRows`, HTTP 200, `truncated = false`.
    *
    * The fixture is the mechanism, not a proxy: `BudgetClient.scroll` ignores the statement LIMIT
    * exactly as the real one does, so a leg that is scrolled yields `rowsPerLeg` and a leg that is
    * EXECUTED yields its own LIMIT. 5 vs 2 is what distinguishes the two routes.
    */
  it should "honour a branch's own LIMIT under the cap, never scroll a LIMITed leg" in {
    val client = new BudgetClient(rowsPerLeg = 5)
    val res = runWith(
      "SELECT a FROM x LIMIT 2 UNION ALL SELECT a FROM y",
      Quota.Community.copy(maxQueryResults = Some(50)),
      client
    )
    val rows = res.asInstanceOf[ElasticSuccess[QueryResult]].value match {
      case q: QueryRows => q.rows
      case other        => fail(s"expected QueryRows, got $other")
    }
    // 2 (the LIMITed leg, honoured) + 5 (the un-LIMITed leg, scrolled) — never 5 + 5.
    rows should have size 7L
    withClue("a LIMITed row leg must NOT be scrolled — scroll drops its LIMIT: ") {
      client.scrolls.map(_._1.sql) shouldBe Seq("SELECT a FROM y")
    }
    client.executed.map(_.sql) shouldBe Seq("SELECT a FROM x LIMIT 2")
  }

  /** 🔴 A branch that FAILS while the cap is being applied must fail the STATEMENT. Dropping its
    * rows and answering HTTP 200 with a short result is the silent-wrong-answer mode this whole
    * story exists to close, reintroduced one layer down — and a `case _ =>` in the fold is exactly
    * how it gets reintroduced.
    *
    * 🔴 An earlier version of this row could NOT fail for the reason it names: it relied on
    * `NopeClientApi`'s cluster-less executor, whose failure is an `ElasticFailure` caught by the
    * dedicated arm — so reinstating a `case _ => success(acc)` before the catch-all left the whole
    * core suite green (measured by the independent review). The failure is now INJECTED at the
    * client, and the assertions below name which arm answered.
    */
  it should "fail the whole statement when a capped UNION ALL branch fails, never truncate silently" in {
    val client = new BudgetClient(rowsPerLeg = 5, failExecute = true)
    val res = runWith(
      "SELECT a FROM x LIMIT 2 UNION ALL SELECT a FROM y",
      Quota.Community.copy(maxQueryResults = Some(50)),
      client
    )
    res shouldBe a[ElasticFailure]
    res.asInstanceOf[ElasticFailure].elasticError.message should include("branch blew up")
    // the LIMITed branch DID reach the client — so the failure comes from executing it, not from a
    // guard that refused the statement before anything ran
    client.executed.map(_.sql) shouldBe Seq("SELECT a FROM x LIMIT 2")
  }

  /** 🔴 The `QueryStream` arm of the cap fold is DEFENCE IN DEPTH, and this row is what says so —
    * and what will tell the next reader the day it stops being true.
    *
    * The review predicted that not scrolling a LIMITed row leg would make `QueryStream` reachable
    * through `dqlExecutor.execute`. MEASURED here, on the shape most likely to produce one (a leg
    * bounded ABOVE `index.max_result_window`, which the router does send through `scroll`): the
    * executor COLLECTS that stream and answers `QueryStructured`. So the arm is not reachable today
    * — it is kept because the alternative to an unreachable arm is a `case _` that would drop a
    * branch's rows the day the executor's contract changes, and this assertion is the tripwire for
    * exactly that change.
    */
  it should "pin which result variants the cap fold can actually receive" in {
    implicit val ctx: ConversionContext = NativeContext
    val client = new BudgetClient(rowsPerLeg = 5)
    def variantOf(sql: String): String = {
      val leg = Parser(sql) match {
        case Right(st: SingleSearch) => st
        case other                   => fail(s"expected a SingleSearch, got $other")
      }
      Await.result(client.dqlExecutor.execute(leg), 10.seconds) match {
        case ElasticSuccess(v)     => v.getClass.getSimpleName
        case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
      }
    }
    // the two shapes the fold hands to `execute`: a LIMITed row leg (bounded, and bounded above
    // the result window) and an aggregation leg
    variantOf("SELECT a FROM x LIMIT 2") shouldBe "QueryStructured"
    variantOf("SELECT a FROM x LIMIT 20000") shouldBe "QueryStructured"
    variantOf("SELECT COUNT(*) AS a FROM x") shouldBe "QueryStructured"
  }

  /** …and the fold handles that variant end to end, with the branch LIMIT honoured. */
  it should "materialise a bounded branch under the cap, never refuse or drop it" in {
    val client = new BudgetClient(rowsPerLeg = 5)
    val res = runWith(
      "SELECT a FROM x LIMIT 20000 UNION ALL SELECT a FROM y",
      Quota.Community.copy(maxQueryResults = Some(50000)),
      client
    )
    res shouldBe a[ElasticSuccess[_]]
    val rows = res.asInstanceOf[ElasticSuccess[QueryResult]].value match {
      case q: QueryRows => q.rows
      case other        => fail(s"expected QueryRows, got $other")
    }
    // both branches contributed; neither was refused and neither was silently dropped
    rows should have size 10L
  }

  /** 🔴 F6 — the METER must fire only when the cap actually BIT. Incrementing before the run
    * recorded a cap-hit for every bounded `UNION ALL`, beside a `truncation` saying `truncated =
    * false` — the meter and the flag contradicting each other on one statement, which is
    * extensions#46's false-increment class.
    */
  it should "record NO cap-hit for a UNION ALL that fits inside the quota" in {
    val collector = new TelemetryCollector()
    val client = new BudgetClient(rowsPerLeg = 5)
    val ext = new CoreDqlExtension()
    ext.initialize(
      ConfigFactory.empty(),
      strategy(
        managerWithQuota(
          Quota.Community.copy(maxQueryResults = Some(10000)),
          LicenseType.Community
        ),
        collector
      )
    )
    val parsed = Parser("SELECT a FROM x UNION ALL SELECT a FROM y") match {
      case Right(st) => st
      case Left(e)   => fail(s"parse failed: ${e.msg}")
    }
    val res = Await.result(ext.execute(parsed, client), 10.seconds)
    val result = res.asInstanceOf[ElasticSuccess[QueryResult]].value match {
      case q: QueryRows => q
      case other        => fail(s"expected QueryRows, got $other")
    }
    // 10 rows against a 10 000 quota: the cap did not bite…
    result.rows should have size 10L
    result.truncation.map(_.truncated) shouldBe Some(false)
    // …so the meter must agree with the flag.
    capHits(collector)("max_query_results") shouldBe 0L
    // and the warning must not claim a capping that did not happen
    result.truncation.map(_.warning) shouldBe Some("")
  }

  /** Rule (1): an explicit LIMIT above the quota on ANY leg is a 402, not a silent cap — the same
    * asymmetry the single-statement rule encodes.
    */
  it should "reject a UNION ALL whose branch LIMIT exceeds the quota with 402" in {
    val client = new BudgetClient(rowsPerLeg = 5)
    val res = runWith(
      "SELECT a FROM x LIMIT 20 UNION ALL SELECT a FROM y",
      Quota.Community.copy(maxQueryResults = Some(10)),
      client
    )
    res shouldBe a[ElasticFailure]
    val err = res.asInstanceOf[ElasticFailure].elasticError
    err.statusCode shouldBe Some(402)
    err.message should include("20")
    err.message should include("10")
    // and NOTHING executed
    client.scrolls shouldBe empty
  }

  /** Rule (3), and the CONTROL for the two rows above: every leg bounded within quota executes
    * unchanged, uncapped and untruncated.
    */
  it should "leave a UNION ALL whose legs are all within quota alone" in {
    val (client, res) = run(
      "SELECT a FROM x LIMIT 5 UNION ALL SELECT a FROM y LIMIT 5",
      Quota.Community.copy(maxQueryResults = Some(10))
    )
    res shouldBe a[ElasticSuccess[_]]
    truncationOf(res) shouldBe None
    client.scrolledStatement.get() shouldBe null
  }

  /** 🔴 The suppressed-leg discipline: a derived leg whose BODY is a `UNION ALL` is executed by the
    * relational engine through `gateway.run` under `ResultCapContext.suppressed`. Capping it here
    * would truncate a JOIN INPUT, not a user-visible result.
    */
  it should "NOT cap a UNION ALL executed as a suppressed join leg" in {
    val collector = new TelemetryCollector
    val client = new BudgetClient(rowsPerLeg = 5)
    val ext = new CoreDqlExtension()
    ext.initialize(
      ConfigFactory.empty(),
      strategy(
        managerWithQuota(Quota.Community.copy(maxQueryResults = Some(7)), LicenseType.Community),
        collector
      )
    )
    val parsed = Parser("SELECT a FROM x UNION ALL SELECT a FROM y") match {
      case Right(st) => st
      case Left(e)   => fail(s"parse failed: ${e.msg}")
    }
    val res = ResultCapContext.suppressed {
      Await.result(ext.execute(parsed, client), 10.seconds)
    }
    res shouldBe a[ElasticSuccess[_]]
    truncationOf(res) shouldBe None
    capHits(collector)("max_query_results") shouldBe 0L
  }

  it should "reject INSERT ... SELECT and CTAS carrying a set operation, and claim them" in {
    Seq(
      "INSERT INTO target SELECT a FROM t EXCEPT SELECT a FROM u",
      "CREATE TABLE target AS SELECT a FROM t INTERSECT SELECT a FROM u"
    ).foreach { sql =>
      val (client, res) = run(sql, Quota.Community)
      withClue(s"[$sql] ") {
        res shouldBe a[ElasticFailure]
        val err = res.asInstanceOf[ElasticFailure].elasticError
        err.statusCode shouldBe Some(400)
        err.message should include("A set operation")
        client.scrolledStatement.get() shouldBe null
        client.searchedStatement.get() shouldBe null
      }
    }
  }

  it should "reject INSERT ... SELECT and CTAS carrying a derived table, and claim them" in {
    Seq(
      "INSERT INTO target SELECT COL FROM (SELECT 1 AS COL) AS d",
      "CREATE TABLE target AS SELECT COL FROM (SELECT 1 AS COL) AS d"
    ).foreach { sql =>
      val (client, res) = run(sql, Quota.Community)
      withClue(s"[$sql] ") {
        res shouldBe a[ElasticFailure]
        res.asInstanceOf[ElasticFailure].elasticError.statusCode shouldBe Some(400)
        client.scrolledStatement.get() shouldBe null
        client.searchedStatement.get() shouldBe null
      }
    }
    val ext = new CoreDqlExtension()
    def parsed(sql: String) = Parser(sql) match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    ext.canHandle(parsed("INSERT INTO target SELECT COL FROM (SELECT 1 AS COL) AS d")) shouldBe true
    ext.canHandle(
      parsed("CREATE TABLE target AS SELECT COL FROM (SELECT 1 AS COL) AS d")
    ) shouldBe true
  }

  it should "still execute a plain statement — the guard did not widen past closure shapes" in {
    val (client, res) = run("SELECT a FROM t LIMIT 5", Quota.Community)
    res shouldBe a[ElasticSuccess[_]]
    client.searchedStatement.get() should not be null
  }

  // ---- Story P0.6: QueryResults cap-hit recorded on BOTH reject branches ----

  behavior of "CoreDqlExtension cap-hit instrumentation (P0.6)"

  private def runWithCollector(
    sql: String,
    quota: Quota,
    tier: LicenseType,
    suppress: Boolean = false
  ): (TelemetryCollector, ElasticResult[QueryResult]) = {
    val parsed = Parser(sql) match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    val collector = new TelemetryCollector
    val client = new RecordingClient()
    val ext = new CoreDqlExtension()
    ext.initialize(ConfigFactory.empty(), strategy(managerWithQuota(quota, tier), collector))
    val res =
      if (suppress)
        ResultCapContext.suppressed(Await.result(ext.execute(parsed, client), 5.seconds))
      else Await.result(ext.execute(parsed, client), 5.seconds)
    (collector, res)
  }

  private def capHits(c: TelemetryCollector): Map[String, Long] =
    c.collect(MetricsApi.Noop).capHitsByKind

  it should "increment the QueryResults cap-hit on the explicit-LIMIT 402 branch" in {
    val (collector, res) =
      runWithCollector("SELECT a FROM idx LIMIT 20000", Quota.Community, LicenseType.Community)
    res shouldBe a[ElasticFailure]
    res.asInstanceOf[ElasticFailure].elasticError.statusCode shouldBe Some(402)
    capHits(collector)("max_query_results") shouldBe 1L
    // no other bucket bumped
    capHits(collector)("max_joins") shouldBe 0L
    capHits(collector)("max_clusters") shouldBe 0L
    capHits(collector)("max_materialized_views") shouldBe 0L
  }

  it should "increment the QueryResults cap-hit on the P0.5 no-LIMIT truncation branch (OQ-2)" in {
    val (collector, res) =
      runWithCollector("SELECT a, b FROM idx", Quota.Community, LicenseType.Community)
    res shouldBe a[ElasticSuccess[_]]
    truncationOf(res).map(_.truncated) shouldBe Some(true)
    capHits(collector)("max_query_results") shouldBe 1L
  }

  it should "NOT increment any cap-hit when an explicit LIMIT is within quota" in {
    val (collector, res) =
      runWithCollector("SELECT a FROM idx LIMIT 50", Quota.Community, LicenseType.Community)
    res shouldBe a[ElasticSuccess[_]]
    capHits(collector).values.toSet shouldBe Set(0L)
  }

  it should "NOT increment a cap-hit when a no-LIMIT query is a suppressed JOIN leg" in {
    val (collector, res) =
      runWithCollector(
        "SELECT a, b FROM idx",
        Quota.Community,
        LicenseType.Community,
        suppress = true
      )
    res shouldBe a[ElasticSuccess[_]]
    capHits(collector).values.toSet shouldBe Set(0L)
  }

  it should "NOT increment a cap-hit for an unlimited (Enterprise) no-LIMIT query" in {
    val (collector, res) =
      runWithCollector("SELECT a, b FROM idx", Quota.Enterprise, LicenseType.Enterprise)
    res shouldBe a[ElasticSuccess[_]]
    capHits(collector).values.toSet shouldBe Set(0L)
  }

  // ---- Story 20.9 / issue #251: FROM-less SELECT takes the no-cap arm (AC 9) ----

  behavior of "CoreDqlExtension FROM-less handshake routing (story 20.9)"

  it should "route a FromlessSelect down the structural no-cap arm — no quota, no cap-hit, never scroll" in {
    // Pinned so a future cap refactor cannot silently start metering handshakes: FromlessSelect
    // is neither SingleSearch nor SelectStatement, so checkQuotasAndExecute must fall through to
    // `case _ => client.dqlExecutor.execute(dql)` — no cap applied, no CapHitKind.QueryResults
    // increment. The INTERNAL rewrite then executes BELOW this seam as an ordinary SingleSearch
    // carrying its own LIMIT 1 (never re-capped, never re-metered, can never reach scroll).
    val parsed = Parser("SELECT 1") match {
      case Right(s) => s
      case Left(e)  => fail(s"parse failed: ${e.msg}")
    }
    val collector = new TelemetryCollector
    val client = new RecordingClient()
    val ext = new CoreDqlExtension()
    ext.initialize(
      ConfigFactory.empty(),
      strategy(managerWithQuota(Quota.Community, LicenseType.Community), collector)
    )
    val res = Await.result(ext.execute(parsed, client), 5.seconds)

    res shouldBe a[ElasticSuccess[_]] // RecordingClient's canned searchAsync answers the rewrite
    // never the scroll path — the rewrite's own LIMIT 1 keeps requiresScrollPaging false
    client.scrolledStatement.get() shouldBe null
    client.scrolledConfig.get() shouldBe null
    // what reached the search seam is the INTERNAL rewrite: handshake index, LIMIT 1 — proof the
    // statement crossed the extension seam uncapped and the rewrite was built below it
    client.searchedStatement.get() match {
      case s: SingleSearch =>
        s.from.tables.map(_.name) shouldBe Seq(GatewayApi.HandshakeIndex)
        s.limit shouldBe Some(Limit(1, None))
      case other => fail(s"expected the handshake rewrite via searchAsync, got $other")
    }
    // no cap-hit of ANY kind
    capHits(collector).values.toSet shouldBe Set(0L)
  }
}
