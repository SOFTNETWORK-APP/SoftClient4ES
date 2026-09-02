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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.query.{FromlessSelect, SingleSearch}
import org.slf4j.Logger

import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/** THE SEAM (issue #251, AD-4′): "evaluate this FROM-less select-list's Painless against the
  * cluster and give me ONE row". Today's backend searches the dedicated handshake index
  * (script_fields on the seeded doc); the recorded future backend is the Painless execute API
  * (`_scripts/painless/_execute`, no index needed) — swapping it must touch nothing but this
  * trait's implementation wiring in GatewayApi.
  */
trait HandshakeEvaluator {
  def evaluateHandshake(statement: FromlessSelect)(implicit
    system: ActorSystem
  ): Future[ElasticResult[ListMap[String, Any]]]
}

/** Search-backed implementation: lazily ensures the handshake index (probe-before-act, race-safe,
  * memoized — AD-9), rewrites the statement to `SELECT <items> FROM <handshake> LIMIT 1` (AD-3′)
  * and executes it through the UNMODIFIED FROM-ful pipeline, then assembles one row: script_fields
  * arrays unwrapped, `__cN` keys renamed to the PD-2 output names (AD-11).
  */
class SearchHandshakeEvaluator(
  api: SearchApi with IndicesApi with IndexApi with RefreshApi with VersionApi,
  logger: Logger
) extends HandshakeEvaluator {

  import GatewayApi._

  @volatile private[this] var ready: Boolean = false

  /** Test seam only. */
  private[client] def isReady: Boolean = ready

  override def evaluateHandshake(statement: FromlessSelect)(implicit
    system: ActorSystem
  ): Future[ElasticResult[ListMap[String, Any]]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    implicit val context: ConversionContext = NativeContext
    ensureHandshakeIndex() match {
      case ElasticFailure(error) => Future.successful(ElasticFailure(error))
      case ElasticSuccess(_)     => runSearch(statement, retriesLeft = 1)
    }
  }

  private def runSearch(statement: FromlessSelect, retriesLeft: Int)(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ListMap[String, Any]]] = {
    val single = statement.toSingleSearch(HandshakeIndex)
    api.searchAsync(single).flatMap {
      case ElasticSuccess(response) =>
        response.results.headOption match {
          case Some(raw) =>
            Future.successful(ElasticSuccess(assembleRow(statement, single, raw)))
          case None if retriesLeft > 0 =>
            // Pre-created-but-unseeded index, or the seed doc was deleted out-of-band:
            // re-seed ONCE, then loud (never an empty-but-successful answer — #253 family).
            ready = false
            ensureHandshakeIndex(forceSeed = true) match {
              case ElasticFailure(error) => Future.successful(ElasticFailure(error))
              case _                     => runSearch(statement, retriesLeft - 1)
            }
          case None =>
            Future.successful(ElasticFailure(handshakeCorruptError()))
        }
      case ElasticFailure(error) if indexNotFound(error) && retriesLeft > 0 =>
        // Out-of-band index delete: reset the memo, re-ensure, retry ONCE.
        ready = false
        ensureHandshakeIndex() match {
          case ElasticFailure(e) => Future.successful(ElasticFailure(e))
          case _                 => runSearch(statement, retriesLeft - 1)
        }
      case ElasticFailure(error) =>
        // The connection check doing its job: propagate the client/cluster failure verbatim.
        Future.successful(ElasticFailure(error))
    }
  }

  /** Probe-before-act (project_mv_metadata_index_contract): an ElasticFailure from the probe is
    * NEVER read as "absent". A failed create re-probes ONCE (lost cross-process race => proceed)
    * and otherwise propagates the ORIGINAL failure (a 403 stays a 403 — PD-6/OQ-6). Memoized per
    * client; concurrent ensures are idempotent (PUT same doc id). Runs synchronously on the caller
    * thread — the established extension-path shape, once per client lifecycle.
    */
  private[client] def ensureHandshakeIndex(forceSeed: Boolean = false): ElasticResult[Unit] = {
    if (ready && !forceSeed) ElasticResult.success(())
    else {
      api.indexExists(HandshakeIndex, pattern = false) match {
        case ElasticFailure(error) if error.statusCode.contains(403) =>
          // A read-only account can be denied the EXISTS probe itself (ES security answers the
          // exists action with 403 for an index the user has no privilege on) — the OQ-6/PD-6
          // guidance must reach THIS failure too, not only the create/seed 403s, or exactly the
          // read-only BI session the guidance exists for gets a bare security_exception.
          withGuidance(ElasticFailure(error))
        case ElasticFailure(error) =>
          ElasticFailure(error) // outage != absence — propagate verbatim (never "absent")
        case ElasticSuccess(true) if !forceSeed =>
          ready = true
          ElasticResult.success(())
        case ElasticSuccess(existsNow) =>
          val created: ElasticResult[_] =
            if (existsNow) ElasticResult.success(true)
            else
              // mappings MUST be passed: without it the seed doc dynamic-maps `dummy` as
              // text+keyword instead of the lead-mandated single keyword field (AC 5/AD-9),
              // and the HandshakeMapping constant is dead code.
              api.createIndex(
                HandshakeIndex,
                settings = handshakeSettings(),
                mappings = Some(HandshakeMapping)
              ) match {
                case f @ ElasticFailure(_) =>
                  api.indexExists(HandshakeIndex, pattern = false) match {
                    case ElasticSuccess(true) => ElasticResult.success(true) // lost the race
                    case _                    => withGuidance(f)
                  }
                case ok => ok
              }
          created match {
            case ElasticFailure(error) => ElasticFailure(error)
            case _ =>
              api.index(HandshakeIndex, HandshakeDocId, HandshakeDoc) match {
                case ElasticFailure(error) => withGuidance(ElasticFailure(error))
                case _ =>
                  api.refresh(HandshakeIndex) match {
                    case ElasticFailure(error) => ElasticFailure(error)
                    case _ =>
                      ready = true
                      logger.info(s"✅ Handshake index '$HandshakeIndex' ready")
                      ElasticResult.success(())
                  }
              }
          }
      }
    }
  }

  /** index.hidden exists only from ES 7.7 (AD-9); `api.version` caches successes. A version lookup
    * failure — or an unparseable version string — falls back to the un-hidden settings (the create
    * itself will surface any real outage) — defense-in-depth must not add a failure mode.
    */
  private def handshakeSettings(): String =
    api.version match {
      case ElasticSuccess(v) if Try(ElasticsearchVersion.isAtLeast(v, 7, 7)).getOrElse(false) =>
        HandshakeSettingsHidden
      case _ => HandshakeSettings
    }

  /** Bounded self-heal trigger. statusCode None != 404 (project_elastic_error_status_semantics) —
    * the message probe covers status-less transports; a wrong trigger costs one retry, never a
    * wrong answer.
    */
  private def indexNotFound(error: ElasticError): Boolean =
    error.statusCode.contains(404) ||
    Option(error.message).exists(_.contains("index_not_found"))

  private def handshakeCorruptError(): ElasticError =
    ElasticError(
      message =
        s"FROM-less SELECT handshake found index '$HandshakeIndex' present but empty and could " +
        s"not re-seed it. Seed it once: PUT /$HandshakeIndex/_doc/$HandshakeDocId $HandshakeDoc",
      statusCode = Some(500),
      index = Some(HandshakeIndex),
      operation = Some("handshake")
    )

  /** PD-6/OQ-6 recommendation (lead-confirmed default): keep the original failure — status
    * included, never invented — and append the pre-creation guidance for read-only BI service
    * accounts. Both routes are named (lead review of PR #268): the SQL one for an administrator
    * connected through SoftClient4ES itself, the REST one for curl/Kibana.
    */
  private def withGuidance(f: ElasticFailure): ElasticFailure =
    ElasticFailure(
      f.elasticError.copy(
        message = s"${f.elasticError.message} — FROM-less SELECT executes a Painless handshake " +
          s"against index '$HandshakeIndex'. If this client must stay read-only, create it " +
          s"once as an administrator — via SQL: CREATE TABLE IF NOT EXISTS $HandshakeIndex " +
          s"""(dummy KEYWORD) OPTIONS (settings = (number_of_shards = "1", """ +
          s"""number_of_replicas = "0")); INSERT INTO $HandshakeIndex (dummy) VALUES ('dummy'); """ +
          s"or via REST: PUT /$HandshakeIndex " +
          s"""{"settings": {"number_of_shards": 1, "number_of_replicas": 0}, """ +
          s""""mappings": $HandshakeMapping} then PUT /$HandshakeIndex/_doc/$HandshakeDocId """ +
          s"$HandshakeDoc — the driver then uses it read-only.",
        index = Some(HandshakeIndex),
        operation = Some("handshake")
      )
    )

  /** Project EXACTLY the select-list outputs (PD-2 names), unwrapping the ES per-field
    * script_fields array (AD-11): the generic parseSimpleHits row keeps the wrap AND appends the
    * `dummy` _source entry (normalizeRow "extra fields"). Response keys = the rewrite's computed
    * aliases (`__cN`) or explicit aliases — positionally zipped with the statement's output names
    * (same Select instance, same order; key alignment is by construction: SingleSearch.scriptFields
    * = fieldsWithComputedAliases.filter(_.isScriptField)).
    */
  private def assembleRow(
    statement: FromlessSelect,
    single: SingleSearch,
    raw: ListMap[String, Any]
  ): ListMap[String, Any] = {
    val responseKeys =
      single.select.fieldsWithComputedAliases.map(f =>
        f.fieldAlias.map(_.alias).getOrElse(f.sourceField)
      )
    ListMap(statement.columnNames.zip(responseKeys).map { case (out, key) =>
      out -> (raw.get(key) match {
        case Some(l: Seq[_]) if l.size == 1 => l.head // the ES per-field array wrapper
        case Some(l: Seq[_]) if l.isEmpty   => null
        case Some(v)                        => v // defensive passthrough — never guess
        case None                           => null // script returned null -> key absent
      })
    }: _*)
  }
}
