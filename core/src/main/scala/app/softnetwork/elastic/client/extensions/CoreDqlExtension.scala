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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.licensing._
import app.softnetwork.elastic.sql.query._
import com.typesafe.config.Config
import org.slf4j.Logger

import scala.collection.immutable.ListMap
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
    // Issue #157 — also claim write-with-JOIN statements (INSERT … SELECT / CREATE TABLE … AS
    // SELECT with a cross-index JOIN, mirroring the arrow JoinExtension's canHandle) so they
    // fail loudly in execute instead of falling through to the core DML/DDL executors, which
    // would silently run the inner SELECT as a single-index search and write wrong data.
    // Issue #157 / story 22.1 — also claim write-with-CLOSURE statements (INSERT … SELECT /
    // CREATE TABLE … AS SELECT carrying a cross-index JOIN or a derived table, mirroring the arrow
    // JoinExtension's canHandle) so they fail loudly in execute instead of falling through to the
    // core DML/DDL executors, which would silently run the inner SELECT as a single-index search
    // and write wrong data.
    case other => relationalClosureRequired(other)
  }

  override def execute(
    statement: Statement,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    statement match {
      // Issue #157 — this baseline handler (and the closed EnforcedDqlExtension inheriting it)
      // translates only the first FROM table: a cross-index JOIN reaching it would silently
      // execute as a single-index search (joined columns NULL, orphans kept) — or, for
      // INSERT … SELECT / CTAS, write that wrong data. With a join-capable extension
      // registered ahead (priority < 100) join statements never get here; without one, fail
      // loudly instead of returning wrong data.
      case s if relationalClosureRequired(s) =>
        Future.successful(ElasticFailure(RelationalClosureGuard.rejection(s)))

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

  // Story 22.1 — `joinRequired` DELETED. The predicate now lives in the `sql` module
  // (`app.softnetwork.elastic.sql.query.relationalClosureRequired`, folded over `closureSearches`)
  // so core, the `searchAs` macro and the arrow venue read ONE definition with ONE list of
  // statement arms. It covers the same shapes it always did — a cross-index JOIN, embedded in
  // INSERT … SELECT / CTAS, with UNNEST excluded by construction — plus derived tables.

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

      // Story 22.6 — a `UNION ALL` is capped on the CONCATENATED total. Before this story the
      // arm below executed it uncapped, which was harmless ONLY because `_msearch` truncated every
      // un-LIMITed leg to 10 rows anyway. That truncation is the defect this story fixed
      // (`SearchApi.unionAllByLeg`), so the licensed cap has to move with the routing — otherwise
      // a `UNION ALL` of N un-LIMITed legs would return every row of every leg, uncapped.
      //
      // A `UNION` / `INTERSECT` / `EXCEPT` never reaches here: the `relationalClosureRequired` arm
      // above refuses it first.
      case multi: MultiSearch =>
        capUnionAll(multi, quota, client)

      case _ =>
        // Other DQL types (SHOW…, etc.) — no single-index result cap. JOIN paths are enforced
        // downstream (arrow).
        client.dqlExecutor.execute(dql)
    }
  }

  /** Story 22.6 — [[capOrReject]] 's rules applied to a `UNION ALL`, on the CONCATENATED total.
    *
    *   - (1) any leg whose explicit LIMIT exceeds a finite quota → 402, first leg wins. Checked
    *     over EVERY leg before anything executes, and it is the same asymmetry rule (1) encodes for
    *     a single statement: an explicit over-limit is a refusal, not a silent cap.
    *   - (2) otherwise, if any leg is row-shaped with no LIMIT under a finite quota AND the cap is
    *     not suppressed, the legs run SEQUENTIALLY with a RUNNING budget: leg `i` is bounded by
    *     what the earlier legs left. The cap therefore binds on the total, never on `n × quota`,
    *     and never more than one scroll is open at a time. Exactly ONE cap-hit is recorded for the
    *     statement, never one per leg.
    *   - (3) otherwise → execute unchanged (Enterprise, a suppressed leg, or every leg bounded
    *     within quota). ⚠️ Rule (3) is PER LEG, not aggregate: three legs at `LIMIT 80` under a
    *     100-row quota return 240 rows. Pre-existing and shared with `capOrReject`'s rule (3) —
    *     recorded here rather than changed, because tightening it is a behaviour change on the
    *     single-statement path too.
    *
    * 🔴 `!ResultCapContext.isSuppressed` carries the same meaning it does in [[capOrReject]]: a
    * derived leg whose body is a `UNION ALL` is executed by the relational engine through
    * `gateway.run` under `ResultCapContext.suppressed`, and capping it INSIDE the engine would
    * truncate a join input rather than a user-visible result.
    *
    * ⚠️ Result-VARIANT change on the licensed gateway: under rule (2) a `UNION ALL` answers
    * `QueryRows` where every path answered `QueryStructured` before. Every consumer already matches
    * all three variants.
    */
  protected def capUnionAll(
    multi: MultiSearch,
    quota: Quota,
    client: ElasticClientApi
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val context: ConversionContext = NativeContext

    val overLimit = quota.maxQueryResults.flatMap { max =>
      multi.requests.zipWithIndex.collectFirst {
        case (r, i) if r.limit.exists(_.limit > max) => (i, r.limit.get.limit, max)
      }
    }

    overLimit match {
      // (1) — the same meter bit and the same 402 as the single-statement rule.
      case Some((i, limit, max)) =>
        capHitCollector.incrementCapHit(TelemetryCollector.CapHitKind.QueryResults)
        logger.warn(
          s"⚠️ Query result limit ($limit) on branch ${i + 1} exceeds license quota ($max)"
        )
        Future.successful(
          ElasticFailure(
            ElasticError(
              message =
                s"Query result limit ($limit) exceeds license quota ($max). Upgrade to Pro license.",
              statusCode = Some(402),
              operation = Some("license")
            )
          )
        )

      case None =>
        val needsCap = quota.maxQueryResults.exists(_ =>
          multi.requests.exists(r => r.returnsRows && r.limit.isEmpty)
        ) && !ResultCapContext.isSuppressed

        if (!needsCap) client.dqlExecutor.execute(multi) // (3)
        else {
          // (2) — sequential legs, running budget, ONE cap-hit.
          val max = quota.maxQueryResults.get
          // 🔴 THE SEAM, run here too. Rule (3) reaches it through `search(multi)`; this fold
          // executes leg by leg and never does, so AD-4's cross-branch TYPE re-check — the one
          // that can only be made once every branch has its schema attached — was live on the
          // plain client and bypassed on the licensed gateway, which is the surface the REPL, the
          // JDBC driver and the Flight sidecar actually use. MEASURED by the independent review:
          // `SELECT category FROM l UNION ALL SELECT amount FROM r` answered 400 through
          // `client.search` and HTTP 200 with 54 rows through here. The resolved statement is
          // DISCARDED on purpose: every leg is re-resolved by its own `search`, idempotently
          // (`SchemaAttachSpec`), and re-using it here would make this fold a second owner of a
          // resolution it does not perform.
          val seam = client.resolveWithSchema(multi)
          logger.info(
            s"ℹ️ No LIMIT on a UNION ALL branch; bounding the concatenated result at license " +
            s"quota ($max rows)"
          )
          (seam match {
            case ElasticFailure(error) =>
              Future.successful(ElasticResult.failure[(Seq[ListMap[String, Any]], Boolean)](error))
            case ElasticSuccess(_) => cappedUnionAllRows(multi, max, client)
          }).map {
            case ElasticFailure(error)          => ElasticFailure(error)
            case ElasticSuccess((rows, capBit)) =>
              // 🔴 The METER fires only when the cap actually BIT, and the flag is computed from
              // the same fact. Incrementing before the run recorded a cap-hit for every bounded
              // `UNION ALL` — a `truncation` saying `truncated = false` beside a telemetry bucket
              // saying the quota was hit, contradicting each other on the same statement. That is
              // extensions#46's false-increment class. (`capOrReject` over-reports the same way,
              // but there `truncated` is unconditionally true so the two at least agree; it is
              // deliberately left alone — it is not on this story's path.)
              // The fold reads ONE row past the budget and says whether it found it, so a
              // statement whose result is exactly `max` rows long is not reported as capped.
              // `rows.size >= max` could not tell those two apart and fired a cap-hit beside
              // `truncated = true` for a result nothing had truncated — F6's class, one boundary
              // further in.
              val truncated = capBit
              if (truncated)
                capHitCollector.incrementCapHit(TelemetryCollector.CapHitKind.QueryResults)
              val warning =
                if (truncated)
                  s"Result capped to $max rows (license quota). " +
                  s"Add an explicit LIMIT <= $max to each branch, or upgrade for more rows."
                else ""
              ElasticSuccess(
                QueryRows(
                  rows,
                  truncation = Some(
                    ResultTruncation(
                      truncated = truncated,
                      limit = max.toLong,
                      totalRows = None,
                      warning = warning
                    )
                  )
                )
              )
          }
        }
    }
  }

  /** The running budget itself: leg `i` reads at most what legs `0..i-1` left unread, and once the
    * budget is spent the remaining legs are never executed at all.
    *
    * 🔴 ONLY an UN-LIMITED row leg is scrolled, and the `limit.isEmpty` test is the whole point:
    * `ScrollApi.scroll(statement, config)` does NOT honour the statement's own `LIMIT` (a
    * pre-existing defect — `cappedScroll` only ever scrolls `limit.isEmpty` statements, so nothing
    * reached it). Scrolling every row leg made that defect REACHABLE and DROPPED the branch's
    * LIMIT: MEASURED on real ES 8.18, `SELECT id FROM a LIMIT 3 UNION ALL SELECT id FROM b` over
    * 30- and 24-document indices returned 54 rows where SQL says 27 and where `main` — with the
    * #209 truncation still in place — returned 13. A silent wrong answer introduced inside the
    * commit written to close one.
    *
    * So: an un-LIMITed row leg is scrolled bounded by the remaining budget (the same mechanism
    * [[cappedScroll]] uses, so paging past `index.max_result_window` works on every ES major). The
    * budget is `max + 1`: the extra row is what tells "exactly `max` rows exist" apart from "the
    * result was cut", and it is dropped before the rows are returned. EVERY other leg — a LIMITed
    * row leg, and an aggregation-shaped leg, which scrolling would page hits a bucket query never
    * returns for — is executed as itself and its rows are taken from the result, bounded by the
    * budget.
    */
  private def cappedUnionAllRows(
    multi: MultiSearch,
    max: Int,
    client: ElasticClientApi
  )(implicit
    system: ActorSystem,
    context: ConversionContext
  ): Future[ElasticResult[(Seq[ListMap[String, Any]], Boolean)]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // 🔴 Each leg is ASKED for one row more than the budget can pay for and KEEPS at most the
    // budget. That one probe row is what tells "the result is exactly `max` rows long" apart from
    // "the result was cut at `max`" — `rows.size >= max` cannot, and fired a cap-hit beside a
    // `truncated` flag for a statement nothing had truncated. Raising the BUDGET instead would
    // have been the obvious edit and a regression: `acc` would reach `max + 1`, so a later leg
    // would still have one row of budget left and would be executed — losing the property that
    // legs past the spent budget never run at all.
    val capBit = new java.util.concurrent.atomic.AtomicBoolean(false)
    // The row contract, hoisted ONCE for the whole statement — the same function the per-leg and
    // one-shot routes apply, so all three routes answer with one row shape. Per-row work on this
    // path is per-row over an UN-LIMITED extraction (`feedback_no_per_row_hot_path_work`).
    val normalise = client.unionAllRowNormalizer(multi)
    val zero: Future[ElasticResult[Seq[ListMap[String, Any]]]] =
      Future.successful(ElasticResult.success(Seq.empty[ListMap[String, Any]]))
    multi.requests
      .foldLeft(zero) { (accF, leg) =>
        accF.flatMap {
          case failure @ ElasticFailure(_) => Future.successful(failure)
          case ElasticSuccess(acc) =>
            val remaining = max.toLong - acc.size.toLong
            // what the leg is ASKED for; what it may CONTRIBUTE is `remaining`
            val probe = remaining + 1L
            def keep(rows: Seq[ListMap[String, Any]]): Seq[ListMap[String, Any]] = {
              if (rows.size.toLong > remaining) capBit.set(true)
              acc ++ rows.take(remaining.toInt).map(normalise)
            }
            if (remaining <= 0L) Future.successful(ElasticResult.success(acc))
            else if (leg.returnsRows && leg.limit.isEmpty)
              client
                .scroll(leg, client.defaultScrollConfig.copy(maxDocuments = Some(probe)))
                .map(_._1)
                .runWith(Sink.seq)
                .map(rows => ElasticResult.success(keep(rows)))
            else
              client.dqlExecutor.execute(leg).flatMap {
                case ElasticSuccess(q: QueryStructured) =>
                  Future.successful(ElasticResult.success(keep(q.response.results)))
                case ElasticSuccess(q: QueryRows) =>
                  Future.successful(ElasticResult.success(keep(q.rows)))
                // 🔴 REACHABLE, and my earlier claim that it was not is REFUTED. I wrote that
                // `dqlExecutor.execute` collects every stream it opens and answers
                // `QueryStructured`; it does not. `DqlRouterExecutor` routes a `SingleSearch` on
                // `limit.isDefined || fields.isEmpty`, and `fields` is empty only under GROUP BY or
                // windowing — so a leg that is NOT row-shaped, carries no LIMIT and projects
                // fields (`SELECT amount, MAX(amount) AS m FROM r`, `SELECT category FROM l ORDER
                // BY COUNT(*)`) is answered as `QueryStream(api.scroll(single))`. The arm is what
                // keeps those statements working; the pin below it now uses one of those two
                // shapes, so it fails for the reason it names.
                case ElasticSuccess(q: QueryStream) =>
                  q.stream.map(_._1).take(probe).runWith(Sink.seq).map { rows =>
                    ElasticResult.success(keep(rows))
                  }
                case ElasticFailure(error) => Future.successful(ElasticResult.failure(error))
                // 🔴 TOTAL, and loud. `case _ => acc` here would DROP a branch's rows and answer
                // HTTP 200 with a short result — the silent-wrong-answer mode this whole story
                // exists to close, reintroduced one layer down. Anything reaching here is a
                // contract change in `dqlExecutor`, and the operator must hear about it. Pinned by
                // a stub executor answering a variant no leg can produce today, because the three
                // variants above are exactly the ones it CAN produce and a test that never reaches
                // this arm leaves the silent-drop mutation green (it did).
                // `ElasticSuccess(other)`, not a bare `other`: the wrapper's class name is
                // `ElasticSuccess` for every possible value, so the message named nothing an
                // operator could act on. The arm is still TOTAL — `ElasticResult` is sealed and
                // its failure half is matched above.
                case ElasticSuccess(other) =>
                  Future.successful(
                    ElasticResult.failure(
                      ElasticError(
                        message = "A UNION ALL branch returned an unexpected result variant " +
                          s"(${other.getClass.getSimpleName}) while the licensed result cap was " +
                          "being applied; the branch's rows would otherwise be dropped silently.",
                        statusCode = Some(500),
                        operation = Some("license")
                      )
                    )
                  )
              }
        }
      }
      .map(_.map(rows => (rows, capBit.get())))
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

      // (2) No LIMIT, finite quota, a ROW-shaped query, and NOT a JOIN leg → cap the scroll
      //     stream at `max` via ScrollConfig.maxDocuments (enforced by ScrollApi's `.take`) and
      //     flag truncation.
      //
      //     `single.returnsRows` covers every unbounded path: the gateway's `scroll` branch
      //     (plain projections) AND the row queries that reach `searchAsync` (windowed /
      //     script-field-only projections), which since issue #209 routes them through scroll
      //     internally and therefore returns EVERY row — so they must be capped here too.
      //     Aggregation-shaped queries (`returnsRows` false: GROUP BY / metric-only SELECT) are
      //     bounded by the aggregation sizes, need no row cap, and must NOT be re-routed to
      //     scroll (it would mishandle aggregation buckets). JOIN legs
      //     (ResultCapContext.isSuppressed) also skip the cap so the join input is not truncated.
      case (None, Some(max)) if single.returnsRows && !ResultCapContext.isSuppressed =>
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
    // The client's configured defaults (page size, slice ceiling — #238) plus the quota cap; the
    // cap is `.take(max)` on the MERGED stream, so it binds on the total whatever the slicing.
    val source =
      client.scroll(single, client.defaultScrollConfig.copy(maxDocuments = Some(max.toLong)))
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
