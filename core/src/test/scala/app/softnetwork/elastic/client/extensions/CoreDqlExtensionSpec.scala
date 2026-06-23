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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SearchStatement, SelectStatement, SingleSearch}
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

  private def strategy(mgr: LicenseManager): LicenseRefreshStrategy =
    new LicenseRefreshStrategy {
      override def initialize(): LicenseKey = LicenseKey.Community
      override def refresh(): Either[LicenseError, LicenseKey] = Left(RefreshNotSupported)
      override def licenseManager: LicenseManager = mgr
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
}
