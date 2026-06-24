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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.sql.query._
import com.typesafe.config.Config
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

//format:off
/** Core extension for DQL quota enforcement.
  *
  * {{{
  * ✅ OSS (always loaded)
  * ✅ Applies Community quotas by default
  * ✅ Can be upgraded with Pro license
  * }}}
  *
  * Story P0.5 — this is the '''Community baseline''' handler (priority 100, Apache-OSS, always
  * loaded). It applies two single-index result-boundary rules at the licensed `maxQueryResults`:
  *
  *   1. explicit `LIMIT` > finite quota → hard `402` reject (unchanged, intentional asymmetry); 2.
  *      no `LIMIT` + finite quota + NOT a JOIN leg → drive the scroll bounded by
  *      `ScrollConfig.maxDocuments = maxQueryResults` (enforced by the existing `.take` at
  *      `ScrollApi`), and attach an additive [[ResultTruncation]] so the truncation is never
  *      silent.
  *
  * The cap is '''suppressed for JOIN legs''' ([[ResultCapContext.isSuppressed]]) so a cross-index
  * JOIN — local or federated — never has a per-leg input truncated (which would silently corrupt
  * the join). The JOINED output is capped downstream (arrow `JoinExecutor` /
  * `FederationFlightProducer`).
  *
  * The quota '''source''' is exposed via the overridable [[resolveQuota]] seam: the closed
  * `EnforcedDqlExtension` (priority &lt; 100, in `softclient4es-extensions`) extends this class and
  * overrides it to the licensed `LicenseManager`, making the paid enforcement un-forkable while
  * this OSS baseline keeps working for the Community tier.
  */
//format:on
class CoreDqlExtension extends ExtensionSpi {

  private[this] var licenseManager: Option[LicenseManager] = None
  private[this] val logger: Logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** Story P0.6 -- the cap-hit collector captured at `initialize`. It is the SAME per-strategy
    * `TelemetryCollector` that `GatewayApi.run` increments for `queriesTotal`, so a `QueryResults`
    * cap-hit recorded here rides the existing `InstancePing` delta to the license-server. Defaults
    * to `Noop` until initialized (or if no strategy carries a real collector). The closed
    * `EnforcedDqlExtension` (priority &lt; 100) extends this class and inherits the increment via
    * the shared `capOrReject`, so the paid enforcement path counts cap-hits too.
    */
  private[this] var capHitCollector: TelemetryCollector = TelemetryCollector.Noop

  override def extensionId: String = "core-dql"
  override def extensionName: String = "Core DQL Quotas"
  override def version: String = "0.1.0"

  override def initialize(
    config: Config,
    licenseRefreshStrategy: LicenseRefreshStrategy
  ): Either[String, Unit] = {
    logger.info("🔌 Initializing Core DQL extension")
    licenseManager = Some(licenseRefreshStrategy.licenseManager)
    capHitCollector = licenseRefreshStrategy.telemetryCollector
    Right(())
  }

  // ✅ Priority: 100 (lower = higher priority)
  // Built-in extensions should have lower priority than premium ones
  override def priority: Int = 100

  override def canHandle(statement: Statement): Boolean = statement match {
    case _: DqlStatement => true
    case _               => false
  }

  override def execute(
    statement: Statement,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    statement match {
      case dql: DqlStatement =>
        licenseManager match {
          case Some(manager) =>
            checkQuotasAndExecute(dql, manager, client)
          case None =>
            // No license manager, execute without checks
            client.dqlExecutor.execute(dql)
        }

      case _ =>
        Future.successful(
          ElasticFailure(
            ElasticError(
              message = "Statement not supported by this extension",
              statusCode = Some(400),
              operation = Some("extension")
            )
          )
        )
    }
  }

  override def supportedSyntax: Seq[String] = Seq(
    "SELECT ... FROM ... WHERE ... GROUP BY ... HAVING ... LIMIT ..."
  )

  // ════════════════════════════════════════════════════════════════════
  // ✅ QUOTA CHECK LOGIC (in extension, not in core)
  // ════════════════════════════════════════════════════════════════════

  /** The quota source for the result cap. OSS baseline = the initialized `LicenseManager`'s quotas
    * (Community by default). The closed `EnforcedDqlExtension` overrides this to the '''licensed'''
    * `LicenseManager` so the paid cap is un-forkable.
    */
  protected def resolveQuota(manager: LicenseManager): Quota = manager.quotas

  protected def checkQuotasAndExecute(
    dql: DqlStatement,
    manager: LicenseManager,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    val quota = resolveQuota(manager)

    dql match {
      case single: SingleSearch =>
        capOrReject(single, quota, client)

      // Defensive: a typed-DSL SelectStatement wrapping a SingleSearch. The parser never emits this
      // for `run(sql)` (it yields a bare SingleSearch — see the ROUTING VERDICT), but
      // ExtensionSpi.execute takes a Statement; if some caller hands us a SelectStatement we still
      // enforce, rather than fall through uncapped.
      case select: SelectStatement =>
        select.statement match {
          case Some(single: SingleSearch) =>
            capOrReject(single.copy(score = select.score), quota, client)
          case _ =>
            client.dqlExecutor.execute(dql)
        }

      case _ =>
        // Other DQL types (SHOW…, MultiSearch/UNION, etc.) — no single-index result cap. JOIN paths
        // are enforced downstream (arrow); UNION is multi-index and out of scope for this story.
        client.dqlExecutor.execute(dql)
    }
  }

  /** Apply the single-index result-boundary rule (ADR D4) at the (licensed) quota.
    *   - explicit LIMIT > finite quota → 402 reject (intentional asymmetry)
    *   - no LIMIT, finite quota, NOT a join leg → cap the scroll + flag truncated
    *   - explicit LIMIT ≤ quota / unlimited / join leg → execute unchanged
    */
  protected def capOrReject(
    single: SingleSearch,
    quota: Quota,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    (single.limit, quota.maxQueryResults) match {
      // (1) Explicit LIMIT over a finite quota → hard 402 (UNCHANGED).
      case (Some(l), Some(max)) if max < l.limit =>
        // Story P0.6 — the meter bit: record a QueryResults cap-hit BEFORE building the 402
        // (side-effect only; the reject itself is unchanged — AC 8).
        capHitCollector.incrementCapHit(TelemetryCollector.CapHitKind.QueryResults)
        logger.warn(
          s"⚠️ Query result limit (${l.limit}) exceeds license quota ($max)"
        )
        Future.successful(
          ElasticFailure(
            ElasticError(
              message =
                s"Query result limit (${l.limit}) exceeds license quota ($max). Upgrade to Pro license.",
              statusCode = Some(402),
              operation = Some("license")
            )
          )
        )

      // (2) No LIMIT, finite quota, the query would take the UNBOUNDED scroll branch, and NOT a
      //     JOIN leg → cap the scroll stream at `max` via ScrollConfig.maxDocuments (enforced by
      //     ScrollApi's `.take`) and flag truncation.
      //
      //     `single.fields.nonEmpty` MIRRORS `SearchExecutor` (GatewayApi.scala:146): only a
      //     no-LIMIT query with explicit projection fields takes the unbounded `scroll` branch —
      //     the ONLY over-quota path. A no-LIMIT query with empty `fields` (aggregations / GROUP BY
      //     / `SELECT *`) takes the `searchAsync` branch, which ES bounds by
      //     `index.max_result_window` (≤10k ≤ every tier quota) → can never exceed, needs no cap,
      //     and must NOT be re-routed to scroll (it would mishandle aggregation buckets). JOIN legs
      //     (ResultCapContext.isSuppressed) also skip the cap so the join input is not truncated.
      case (None, Some(max)) if single.fields.nonEmpty && !ResultCapContext.isSuppressed =>
        // Story P0.6 (OQ-2) — the no-LIMIT truncation IS the meter biting (non-fatally): count it
        // as a QueryResults cap-hit, the SAME kind as the explicit-LIMIT 402 above. The cap is
        // suppressed for JOIN legs (the `!isSuppressed` guard), so a per-leg input truncation is
        // never counted — only a genuine single-index result cap. (JOIN-output truncation is
        // counted on the Joins axis downstream, not here — no double-count.)
        capHitCollector.incrementCapHit(TelemetryCollector.CapHitKind.QueryResults)
        logger.info(
          s"ℹ️ No LIMIT on single-index scroll query; capping the stream at license quota ($max rows) and flagging truncation"
        )
        cappedScroll(single, max, client)

      // (3) Explicit LIMIT within quota, OR unlimited quota (Enterprise), OR the bounded
      //     searchAsync branch (no-LIMIT + empty fields / aggregations), OR a suppressed JOIN leg
      //     → execute unchanged, no truncation flag.
      case _ =>
        client.dqlExecutor.execute(single)
    }
  }

  /** Drive the single-index scroll bounded by `maxDocuments = max` and attach a
    * [[ResultTruncation]] to the produced [[QueryStream]]. `client` is a full `ElasticClientApi`,
    * which mixes `ScrollApi`, so `scroll(statement, config)` is directly available. The
    * `searchAsync` branch is intentionally NOT used: it would issue a single `size = max` search,
    * which Elasticsearch rejects when `max > index.max_result_window` (default 10k) — i.e. for Pro
    * (1M). Scroll / PIT pages past `max_result_window` natively, serving any N up to the quota
    * across ES 6/7/8/9.
    */
  protected def cappedScroll(
    single: SingleSearch,
    max: Int,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val context: ConversionContext = NativeContext
    val source =
      client.scroll(single, ScrollConfig(maxDocuments = Some(max.toLong)))
    val warning =
      s"Result capped to $max rows (license quota). " +
      s"Add an explicit LIMIT <= $max, or upgrade for more rows."
    val signal = ResultTruncation(
      truncated = true,
      limit = max.toLong,
      totalRows = None, // capped at source — pre-cap total intentionally not scanned (OQ-4)
      warning = warning
    )
    Future.successful(ElasticSuccess(QueryStream(source, truncation = Some(signal))))
  }
}
