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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.function.aggregate.{PercentileAgg, RankingWindow}
import app.softnetwork.elastic.sql.macros.SQLQueryMacros
import app.softnetwork.elastic.sql.query.{
  Limit,
  MultiSearch,
  SQLAggregation,
  SearchStatement,
  SelectStatement,
  SingleSearch,
  SubqueryScope
}
import app.softnetwork.elastic.sql.`type`.SQLType
import app.softnetwork.elastic.sql.schema.Schema
import app.softnetwork.elastic.sql.query.TemporalLiterals
import com.fasterxml.jackson.databind.JsonNode
import com.typesafe.config.ConfigFactory
import org.json4s.Formats

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.jdk.CollectionConverters._
import scala.language.experimental.macros
import scala.reflect.{classTag, ClassTag}
import scala.util.{Failure, Success, Try}

//format:off
/** Elasticsearch search API with unified error handling via ElasticResult.
  *
  * @example
  * {{{
  *   class MyClient extends SearchApi {
  *     // Implementation of abstract methods
  *   }
  *
  *   val client = new MyClient()
  *   val result = client.searchAs[User]("SELECT * FROM users WHERE age > 30")
  * }}}
  */
//format:on
trait SearchApi extends ElasticConversion with ElasticClientHelpers with SchemaCacheTtlApi {

  /** Extract output field names from a SingleSearch in SQL SELECT order. For each field, uses the
    * alias if present, otherwise the source field name. Returns empty Seq for SELECT * queries.
    */
  protected def extractOutputFieldNames(single: SingleSearch): Seq[String] = {
    val fields = single.select.fieldsWithComputedAliases
    // `Field.outputName` is the SHARED definition: `SingleSearch.rowInvariantProjection` keys a
    // projected constant off the very same expression, so the column this asks for and the column
    // that supplies its value are the same string by construction (#253 FOLD-IN 1).
    if (fields.size == 1 && fields.head.identifier.identifierName == "*") Seq.empty
    else fields.map(_.outputName)
  }

  /** Attach the index schema to the AST, and resolve the string literals a WHERE clause compares
    * against `date`-mapped columns (issue #276; see [[TemporalLiterals]]) -- both BEFORE the
    * statement is rendered into an Elasticsearch query. Runs ONCE per statement, on the AST --
    * never per hit.
    *
    * Covered entry points: every `SingleSearch -> ElasticQuery` seam -- [[search]] /
    * [[searchAsync]] (the single statement and each `UNION ALL` request), the inner-hits search and
    * `ScrollApi.scroll` -- hence `GatewayApi.run` (REPL, JDBC driver, Flight SQL sidecar and its
    * cross-index JOIN legs, which re-enter `gateway.run`) transitively.
    *
    * 🔴 The attach arrived with issue #306 (see the body); it is why this method is named for the
    * schema rather than for the literals, and why the lookup is NO LONGER skipped for a statement
    * without a temporal candidate — `baseType` is needed for every statement, and skipping would
    * have left the attach dead for nearly every query. Cost: one lookup per index per cache TTL.
    *
    * Schema-absent path = verbatim (#276 AD-S4-1.2): the lookup is skipped when the FROM does not
    * name exactly ONE concrete index (several sources, a `*` wildcard or a `,` list), when this
    * client is not an [[IndicesApi]], and when the schema cannot be loaded (alias without a
    * template, unknown index, cluster error or a thrown lookup)
    * -- the literal is then forwarded exactly as before. The schema comes from
    * [[IndicesApi.loadSchema]] 's 5-minute cache, so a miss costs one `GET <index>` per index per
    * TTL. A literal that cannot be a date under a fully-understood mapping format is a `400` that
    * names the literal and the field, instead of Elasticsearch's
    * `search_phase_execution_exception`.
    *
    * `loadSchema` caches successes only, so a source it cannot resolve -- an ALIAS on the es8/es9
    * clients (`executeGetIndex` looks the alias up as a key and finds nothing), an unknown index
    * without a template -- would otherwise cost one failed lookup (two round trips + WARN lines)
    * per statement. [[schemaMisses]] remembers a 404 miss for [[schemaMissTtlMs]] and skips the
    * lookup; the literal is verbatim either way. A NON-404 failure (a transient 5xx, a thrown
    * lookup) is never remembered, so a cluster blip cannot disable the resolution for the TTL.
    *
    * `private[client]`: `IndicesApi` reuses it for the DELETE / UPDATE by-query search bodies.
    *
    * ==Cost==
    * MEASURED (Apple silicon, JDK 11; medians over warmed runs) — two costs, different profiles:
    *
    *   - the LOAD is amortised by the schema cache: a HIT is `0.1 us`. A MISS additionally costs
    *     one `GET <index>` round trip plus the mapping parse, which scales with mapping WIDTH — `74
    *     us` at 5 fields, `170 us` at 50, `962 us` at 300 — once per index per TTL. Concurrent
    *     first-touch queries do not stampede: `loadSchema` populates under `compute`.
    *   - the ATTACH (`update(Some(schema))`) never amortises. It scales with STATEMENT size, not
    *     mapping width: `1-2 us` trivial, `7-11 us` for several scripted fields plus a CASE. It
    *     runs ONCE per statement, TWICE for an un-LIMITed row query (`search` -> `scrollRows` ->
    *     `ScrollApi.scroll` resolves again) — and NOT per page, so a 10M-row extraction pays it
    *     twice in total. Verified by counting resolutions: 1 for a LIMITed statement, 2 for a
    *     scroll-routed one.
    *
    * Steady-state overhead is therefore ~1-25 us per statement against millisecond-scale statement
    * latency — under 0.1%. That is why resolution is UNCONDITIONAL rather than filtered on
    * statement content. Documented for operators in `documentation/client/search.md`.
    *
    * 🔴 If a filter is ever revisited, the predicate must NOT be "the statement contains a cast":
    * `coerce` also serves FunctionN argument and CASE branch coercion, so a cast-scoped attach
    * would make `UPPER(col)` emit differently depending on whether a cast happened to appear
    * ELSEWHERE in the same statement — a #205-family inconsistency driven by incidental content
    * (AD-17). The only self-consistent predicate is "this statement emits Painless over a column",
    * i.e. `shouldBeScripted` across the clauses.
    */
  private[client] def resolveWithSchema(single: SingleSearch): ElasticResult[SingleSearch] = {
    // Story 22.1 (epic 22 AD-5) — THE seam. Every direct-API path crosses it BEFORE routing
    // (`search`, `searchAsync`, `searchWithInnerHits`, `ScrollApi.scroll`, and `IndicesApi`'s
    // by-query DELETE/UPDATE bodies through `resolveDmlWithSchema`), and none of them passes
    // through `CoreDqlExtension` — so a guard placed here cannot be bypassed by a routing
    // decision, and two guards would be two places for it to go missing.
    //
    // A derived table has no index to resolve a schema against and `single.sources` yields its
    // ALIAS; a cross-index JOIN resolves only its FIRST table. MEASURED on `origin/main` before
    // this story: `client.search("SELECT o.id, c.name FROM orders o JOIN customers c ON …")`
    // returned `ElasticSuccess` over index list `[orders]` with a `match_all` body — the JOIN leg
    // silently dropped, HTTP 200, wrong answer. That is the #157 residual on the raw client API
    // (no sibling production code calls this API — every one goes through `gateway.run` — which is
    // why it survived), and the lead's ruling for this story is to close it here rather than file
    // it.
    if (single.relationalClosureRequired)
      return ElasticResult.failure(
        RelationalClosureGuard.rejection(single, operation = "search")
      )
    // Story 22.2 — PHASE ONE. Every UNCORRELATED WHERE subquery is executed and its predicate is
    // rewritten into the literal form Elasticsearch already runs (`terms` / a literal comparison /
    // `match_all` / `match_none`) BEFORE the schema attach and the temporal resolution below, so
    // the values it injects get EXACTLY the treatment a hand-written literal list gets (an ISO-8601
    // instant string is normalised against the OUTER column's mapped format, custom formats
    // included). It runs for EVERY source shape — the single-concrete-source guard below is about
    // the OUTER schema, not about this, and `FROM a, b WHERE x IN (SELECT …)` must be rewritten too.
    // `hasWhereSubqueries` is ONE boolean per statement: zero cost for every statement without one.
    val phaseOne: SingleSearch =
      if (!single.hasWhereSubqueries) single
      else
        SubqueryResolver.resolve(single, this) match {
          case ElasticSuccess(rewritten) => rewritten
          case ElasticFailure(error)     => return ElasticResult.failure(error)
        }
    // #306 -- this used to return early for a statement whose WHERE carried no temporal literal.
    // That was correct while the only job was rewriting those literals, and is WRONG now that the
    // schema is also attached to the AST: almost no statement carries a temporal WHERE literal,
    // so the lookup would be skipped for almost every query and `baseType` would stay `Any`.
    phaseOne.sources.distinct match {
      case Seq(source) if !source.contains("*") && !source.contains(",") =>
        this match {
          case indices: IndicesApi if !schemaMissed(source) =>
            Try(indices.loadSchema(source)) match {
              case Success(ElasticSuccess(schema)) =>
                schemaMisses.remove(source)
                TemporalLiterals(phaseOne, schema) match {
                  case Right(literalsResolved) =>
                    // #306 -- ATTACH the schema to the AST. `GenericIdentifier.baseType` is
                    // `col.map(_.dataType)` and `col` is populated only here, inside `update`, from
                    // `request.schema`; until this call existed the ONLY production caller of
                    // `update(Some(schema))` was `Table.mergeWithSearch` (CTAS column inference,
                    // which returns a Table rather than a statement to execute), so every executing
                    // query reached `SQLTypeUtils.coerce` with `baseType = SQLTypes.Any` and NO cast
                    // over a column ever emitted a conversion -- for any source type.
                    //
                    // Unconditional within this method's precondition, deliberately NOT scoped to
                    // statements that contain a cast: `coerce` also serves `FunctionN` argument
                    // coercion and CASE branch coercion, so a cast-scoped attach would make
                    // `UPPER(col)` emit differently depending on whether the SAME statement
                    // happened to carry a cast elsewhere -- a #205-family inconsistency that
                    // depends on incidental statement content.
                    //
                    // Idempotent by necessity, not by luck: `SearchApi.search` resolves and then
                    // routes a row query through `scrollRows` -> `ScrollApi.scroll`, which resolves
                    // AGAIN, so this runs twice on that path (pinned in SchemaAttachSpec).
                    val resolved = literalsResolved.update(Some(schema))
                    if (resolved ne phaseOne)
                      logger.debug(
                        s"Temporal literals resolved against the mapping of '$source':${resolved.where
                          .map(_.sql)
                          .getOrElse("")}"
                      )
                    // 🔴 Issue #384 — the rules that need the SCHEMA are checked HERE, because this
                    // is the only place a resolved statement exists. `Parser.apply` validates the
                    // statement it parsed, where every column is `Any`, so a TIME compared with a
                    // TIMESTAMP is indistinguishable from a legal comparison at parse time. Without
                    // this the mismatch reaches Elasticsearch and fails the shard with
                    // `Cannot cast java.time.ZonedDateTime to java.time.LocalTime`, naming neither
                    // the column nor the SQL. NOT a second `validate()` — see `validateResolved`.
                    resolved.validateResolved() match {
                      case Left(reason) =>
                        logger.error(s"❌ $reason")
                        ElasticResult.failure(
                          ElasticError(
                            message = reason,
                            statusCode = Some(400),
                            index = Some(source),
                            operation = Some("search")
                          )
                        )
                      case Right(_) => ElasticResult.success(resolved)
                    }
                  case Left(reason) =>
                    logger.error(s"❌ $reason")
                    ElasticResult.failure(
                      ElasticError(
                        message = reason,
                        statusCode = Some(400),
                        index = Some(source),
                        operation = Some("search")
                      )
                    )
                }
              case Success(ElasticFailure(error)) =>
                if (error.statusCode.contains(404)) rememberSchemaMiss(source)
                logger.debug(
                  s"Schema of '$source' unavailable (${error.message}) - temporal literals forwarded verbatim"
                )
                ElasticResult.success(phaseOne)
              case Failure(e) =>
                logger.debug(
                  s"Schema lookup for '$source' failed with ${e.getClass.getName} - temporal literals forwarded verbatim"
                )
                ElasticResult.success(phaseOne)
            }
          case _ => ElasticResult.success(phaseOne)
        }
      case _ => ElasticResult.success(phaseOne)
    }
  }

  /** The mapped type of a column PROJECTED BY AN INNER STATEMENT, when it can be known for free
    * (story 22.2).
    *
    * `None` under every #306 skip condition — several sources, a wildcard, a client that is not an
    * `IndicesApi`, a remembered miss, a failed or absent schema. The caller (`SubqueryResolver`)
    * reads it for ONE decision: a `text` column cannot carry a terms aggregation, so mode P is
    * declined for it. An unknown mapping therefore keeps the cheap default, and an Elasticsearch
    * refusal propagates loudly with the inner statement's own message.
    */
  private[client] def innerColumnType(inner: SingleSearch, name: String): Option[SQLType] =
    resolvedSchema(inner).flatMap(_.find(name).map(_.dataType))

  /** Story 22.3 (AD-3) — the SCHEMA-aware SCOPE check, generalised from story 22.2's
    * `bareNameCorrelation`. Reached only for a statement the closure guard did NOT already route to
    * the relational engine, so it answers the two questions parse time provably cannot:
    *
    *   1. a BARE name the inner index does not map while the outer one DOES. SQL resolves a bare
    *      name innermost-first, so `cid` inside the subquery is the INNER column whenever the inner
    *      index has it — the right reading in the overwhelming majority of statements — but when
    *      BOTH mappings are in hand the other case is a correlated reference beyond reasonable
    *      doubt, and executing it as uncorrelated would send an unknown field to Elasticsearch and
    *      answer zero rows with HTTP 200. It is refused with the remedy that makes it EXECUTABLE
    *      since story 22.3b: qualify it, and the statement routes. 2. a QUALIFIED name that
    *      resolves in NO scope of the chain and is not a mapped object field of the inner index
    *      either. At parse time those two are indistinguishable (story 22.2's PD-2 assumes the
    *      object path, which is why no arm rejects it there); with the mapping in hand they are
    *      not, and the message NAMES every scope it searched.
    *
    * Any schema-absent condition answers `None` (assume inner — PD-2's documented boundary), so
    * this never turns a schema outage into a rejection.
    */
  private[client] def scopeCorrelation(
    outer: SingleSearch,
    inner: SingleSearch
  ): Option[String] =
    bareNameCorrelation(outer, inner).orElse(unresolvedQualifier(outer, inner))

  /** Question 2 of [[scopeCorrelation]]. `Schema.find(head)` answers for an OBJECT field too (it
    * returns the object column itself), so one lookup separates `address.city` from a typo.
    */
  private def unresolvedQualifier(
    outer: SingleSearch,
    inner: SingleSearch
  ): Option[String] =
    // 🔴 An EMPTY mapping is a mapping GAP, never evidence that a name does not exist: an index
    // created but not yet written to, or one whose dynamic mapping has not caught up, would
    // otherwise turn every dotted object path in a subquery body into a 400. Same posture as the
    // schema-absent conditions in `resolvedSchema` — a mapping we do not have must never become a
    // rejection.
    resolvedSchema(inner).filter(_.columns.nonEmpty).flatMap { innerMapping =>
      val chain = SubqueryScope.chain(inner, Seq(outer))
      inner.referencedIdentifiers.iterator
        .filter(id =>
          id.tableAlias.isEmpty && id.table.isEmpty && !id.nested && id.name.contains(".")
        )
        .flatMap { id =>
          SubqueryScope.resolve(id, chain) match {
            // 🔴 The resolver sees a name the DETECTOR did not report. That is possible because the
            // two used to read different maps, and it is exactly the shape that answers HTTP 200
            // with zero rows if it executes: Elasticsearch reads `o.region` as an object path.
            // `correlationNames` now derives from `scopeOf`, so this should be unreachable —
            // it is the belt for the day the two drift again, and it fails LOUD, never silent.
            case SubqueryScope.Resolved(depth, _, _) if depth >= 1 =>
              Some(
                SubqueryScope.bareCorrelatedMessage(
                  id.name,
                  inner.sources.headOption.getOrElse("the subquery's table"),
                  outer.sources.headOption.getOrElse("the outer table")
                )
              )
            // Unresolved: a typo, or an object path. Only the mapping can tell, and it does.
            case SubqueryScope.Unresolved
                if innerMapping.find(id.name.split("\\.", 2)(0)).isEmpty =>
              Some(SubqueryScope.unresolvedMessage(id, chain, inner.sql))
            case _ => None
          }
        }
        .toSeq
        .headOption
    }

  private def bareNameCorrelation(
    outer: SingleSearch,
    inner: SingleSearch
  ): Option[String] =
    for {
      innerMapping <- resolvedSchema(inner)
      outerMapping <- resolvedSchema(outer)
      // 🔴 A name the INNER SELECT list defines as an alias is an inner name, whatever the mappings
      // say: `IN (SELECT id AS cid FROM customers ORDER BY cid)` references `cid`, which no mapping
      // carries — and rejecting it because the OUTER index happens to have a `cid` column would
      // refuse a perfectly self-contained subquery. `referencedIdentifiers` covers ORDER BY and
      // GROUP BY, which is exactly where such an alias is referenced.
      innerAliases = inner.select.fieldAliases.values.toSet
      offender <- inner.referencedIdentifiers.find { id =>
        id.tableAlias.isEmpty && !id.nested && !id.name.contains(".") &&
        id.functions.isEmpty && id.name.nonEmpty && id.name != "*" &&
        !innerAliases.contains(id.name) &&
        innerMapping.find(id.name).isEmpty && outerMapping.find(id.name).isDefined
      }
    } yield SubqueryScope.bareCorrelatedMessage(
      offender.name,
      innerMapping.name,
      outerMapping.name
    )

  private def resolvedSchema(s: SingleSearch): Option[Schema] =
    s.sources.distinct match {
      case Seq(source) if !source.contains("*") && !source.contains(",") =>
        this match {
          case indices: IndicesApi if !schemaMissed(source) =>
            Try(indices.loadSchema(source)) match {
              case Success(ElasticSuccess(schema)) => Some(schema)
              case _                               => None
            }
          case _ => None
        }
      case _ => None
    }

  /** Sources whose schema answered 404, with the time of the miss (see [[resolveWithSchema]]). Per
    * client instance, like the schema cache it shadows. Keys are caller-supplied FROM names, so the
    * map is bounded the way #238's `shardCountCache` is: above [[schemaMissPurgeThreshold]] entries
    * an insert also purges the expired ones, and if it is STILL above four times the threshold (a
    * client probing thousands of distinct unknown names inside one TTL) it is cleared -- the worst
    * case is then today's cost, one failed lookup per statement, never unbounded memory.
    */
  private val schemaMisses =
    new java.util.concurrent.ConcurrentHashMap[String, java.lang.Long]()

  private val schemaMissPurgeThreshold = 256

  /** How long a failed schema lookup is remembered: the schema cache's DEFAULT TTL
    * (`elastic.schema-cache.ttl`), never longer than the built-in five minutes.
    *
    * 🔴 Deliberately the default and never a per-index value (story 21.8 D.3.2): this map records a
    * MISS. There is no metadata to read a TTL from, because there was no schema — an index that
    * does not exist cannot ask to be forgotten sooner.
    *
    * 🔴 And deliberately CAPPED, which the two positive caches are not. Nothing invalidates a miss:
    * it is cleared only by a later successful load, and `resolveWithSchema` does not even attempt
    * the lookup while one stands. So a miss remembered for an operator-chosen hour would mean that
    * querying a table before it exists, then creating it, leaves every statement against it running
    * with NO schema attached for that hour — no `SQLTypeUtils.coerce`, no temporal-literal
    * resolution, silently (#306's skip conditions). Shortening the TTL still shortens this; making
    * the knob able to LENGTHEN it would turn a five-minute nuisance into an unbounded one.
    */
  protected def schemaMissTtlMs: Long =
    math.min(schemaCacheTtlMs, SchemaCacheSettings.DefaultTtlMs)

  /** Current size of the negative cache (tests). */
  private[client] def schemaMissCount: Int = schemaMisses.size()

  private def schemaMissed(source: String): Boolean =
    Option(schemaMisses.get(source)).exists { missedAt =>
      System.currentTimeMillis() - missedAt < schemaMissTtlMs
    }

  private def rememberSchemaMiss(source: String): Unit = {
    val now = System.currentTimeMillis()
    schemaMisses.put(source, now)
    if (schemaMisses.size() > schemaMissPurgeThreshold) {
      val ttl = schemaMissTtlMs
      schemaMisses
        .entrySet()
        .removeIf((e: java.util.Map.Entry[String, java.lang.Long]) => now - e.getValue >= ttl)
      if (schemaMisses.size() > schemaMissPurgeThreshold * 4)
        schemaMisses.clear()
    }
  }

  /** [[resolveWithSchema]] over every request of a `UNION ALL`; the first rejection wins. */
  private[client] def resolveWithSchema(multiple: MultiSearch): ElasticResult[MultiSearch] = {
    // Story 22.6 — only `UNION ALL` is an Elasticsearch `_msearch`; UNION (de-duplicating),
    // UNION DISTINCT, INTERSECT [ALL] and EXCEPT [ALL] all need the relational engine. Refuse HERE,
    // before any branch is resolved or any request is built: this is the ONE seam every direct-API
    // path crosses, so a de-duplicating UNION can never degrade into a UNION ALL that returns
    // duplicates with HTTP 200 — the silent-wrong-answer mode this operator family invites.
    if (!multiple.isUnionAllOnly)
      return ElasticResult.failure(
        RelationalClosureGuard.rejection(multiple, operation = "search")
      )
    val zero: ElasticResult[Seq[SingleSearch]] = ElasticResult.success(Seq.empty)
    multiple.requests.foldLeft(zero) {
      case (ElasticSuccess(acc), request) =>
        resolveWithSchema(request) match {
          case ElasticSuccess(resolved) => ElasticResult.success(acc :+ resolved)
          case ElasticFailure(error)    => ElasticResult.failure(error)
        }
      case (failure, _) => failure
    } match {
      case ElasticSuccess(resolved) =>
        // Story 22.6 — with each branch's schema attached, bare columns are TYPED: re-run the
        // pairwise compatibility check that passed vacuously at parse time (where a bare column is
        // `Any`). This is the only type guard the family has — DuckDB casts implicitly across
        // branches, so the relational engine is never a backstop either.
        MultiSearch.branchTypes(resolved) match {
          case Left(reason) =>
            ElasticResult.failure(
              ElasticError(
                message = reason,
                statusCode = Some(400),
                operation = Some("search")
              )
            )
          case Right(()) =>
            val unchanged = resolved.zip(multiple.requests).forall { case (a, b) => a eq b }
            ElasticResult.success(if (unchanged) multiple else multiple.copy(requests = resolved))
        }
      case ElasticFailure(error) => ElasticResult.failure(error)
    }
  }

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** The client's refusal for a statement that did not parse -- WITH the parser's own reason when
    * there is one (issue #389 / F7).
    *
    * 🔴 `SelectStatement.statement` is an `Option`, so a rejection arriving through it used to be
    * reported as the generic sentence below and the reason -- computed, formatted, and naming the
    * clause and its remedy -- was thrown away. Tolerable while the rejections were syntax errors
    * the user can see; a whole family of SEMANTIC refusals now lands here.
    */
  private def invalidSearchRequest(statement: SearchStatement, query: String): String =
    (statement match {
      case s: SelectStatement => s.parseError
      case _                  => None
    }).getOrElse(s"SQL query does not contain a valid search request\n$query")

  /** Search for documents / aggregations matching the SQL query.
    *
    * @param statement
    *   the SQL query to execute
    * @return
    *   the Elasticsearch response
    */
  def search(
    statement: SearchStatement
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    val query = statement.sql
    statement match {
      case select: SelectStatement =>
        select.statement match {
          case Some(statement: SingleSearch) =>
            search(statement.copy(score = select.score))
          case Some(statement: MultiSearch) =>
            search(statement)
          case None =>
            logger.error(
              s"❌ Failed to execute search for query \n${statement.sql}"
            )
            ElasticResult.failure(
              ElasticError(
                message = invalidSearchRequest(statement, query),
                operation = Some("search")
              )
            )
        }
      case parsed: SingleSearch =>
        // #276 -- resolve temporal literals against the mapped `date` columns BEFORE rendering
        resolveWithSchema(parsed) match {
          case ElasticSuccess(single) => searchResolved(single, query)
          case ElasticFailure(error)  => ElasticResult.failure(error)
        }

      case parsed: MultiSearch =>
        val multiple = resolveWithSchema(parsed) match {
          case ElasticSuccess(resolved) => resolved
          case ElasticFailure(error)    => return ElasticResult.failure(error)
        }
        // Story 22.6 — an `_msearch` leg is ONE-SHOT: a row-shaped leg with no bounding LIMIT
        // gets Elasticsearch's default 10 hits with HTTP 200. That is issue #209's family, one
        // venue over, and this story pins this path as "the fast path" — pinning a silently
        // truncating request byte-for-byte would pin a defect as a contract. Such a leg is
        // executed through `search(leg)`, which routes it through scroll/PIT exactly as the same
        // statement executes on its own, and the legs are concatenated IN ORDER. Every other
        // shape (every leg LIMITed within `max_result_window`, or aggregation-shaped) keeps the
        // single `_msearch`, byte-for-byte the request emitted before this story.
        if (unionAllNeedsPerLeg(multiple))
          return unionAllByLeg(multiple, query)
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*),
              explodeNested = multiple.explodeNested
            )
          }.toList,
          sql = Some(query),
          explodeNested = multiple.explodeNested
        )
        multiSearch(
          elasticQueries,
          multiple.fieldAliases,
          multiple.sqlAggregations,
          // The result's column NAMES come from the FIRST leg (SQL-92 §7.10). Everything a leg
          // needs to build its OWN rows — aliases, projection, nested-hits mapping — travels
          // per leg below, as do the row-invariant CONSTANTS (#253 FOLD-IN 1).
          unionAllOutputFieldNames(multiple),
          multiple.requests.headOption.map(_.nestedHitsMappings).getOrElse(Map.empty),
          multiple.requests.map(rowInvariantsOf),
          unionAllLegProjections(multiple)
        )

      case _ =>
        logger.error(
          s"❌ Failed to execute search for query \n${statement.sql}"
        )
        ElasticResult.failure(
          ElasticError(
            message = invalidSearchRequest(statement, query),
            operation = Some("search")
          )
        )
    }
  }

  /** Convert SELECT aggregations to ClientAggregations, applying percentile coalescing: columns
    * sharing a value column / `cont` flag / partition become delegates of the first (owner) column
    * and read their value from the owner's shared ES `percentiles` response node. Mirrors the
    * bridge's percent-merge (both call [[PercentileAgg.coalescePlan]] on the same SELECT-ordered
    * items, so they always pick the same owner).
    */
  private def toClientAggregations(
    aggregations: ListMap[String, SQLAggregation]
  ): ListMap[String, ClientAggregation] = {
    val aggs0 = aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
    val percentileItems = aggregations.toSeq.collect {
      case (name, sa) if sa.aggType.isInstanceOf[PercentileAgg] =>
        name -> sa.aggType.asInstanceOf[PercentileAgg]
    }
    if (percentileItems.size < 2) aggs0
    else {
      val plan = PercentileAgg.coalescePlan(percentileItems)
      aggs0.map { case (name, ca) =>
        name -> (if (plan.isDelegate(name)) ca.copy(sourceAgg = plan.ownerOf.get(name)) else ca)
      }
    }
  }

  /** The ROW-INVARIANT constants a statement contributes to its own rows (#253 FOLD-IN 1).
    *
    * Handed to the parse layer as ordinary statement-derived data, exactly like `fieldAliases` and
    * the output `fields`, and SEEDED into `parseAggregations`' `parentContext` so each constant is
    * placed once as the rows are built rather than by a second pass over them.
    *
    * No `returnsRows` guard is needed: `parseAggregations` IS the aggregation path by construction,
    * so a row-shaped statement never reaches the seed. The empty default is what every scroll-page
    * caller gets.
    */
  private def rowInvariantsOf(single: SingleSearch): ListMap[String, Any] =
    single.rowInvariantProjection

  /** Search for documents / aggregations matching the Elasticsearch query.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   the Elasticsearch response
    */
  def singleSearch(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: ListMap[String, Any] = ListMap.empty
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] =
    singleSearchInternal(
      elasticQuery,
      fieldAliases,
      aggregations,
      fields,
      nestedHits,
      rowInvariants
    )

  /** [[singleSearch]] with an explicit document-id retention decision. `retainDocumentId = true` is
    * reserved for the window-enrichment base query, which matches rows to their ranking ordinals by
    * document id AFTER parsing (the id is stripped again after enrichment).
    */
  private[client] def singleSearchInternal(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: ListMap[String, Any] = ListMap.empty,
    retainDocumentId: Boolean = false
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
    validateJson("search", elasticQuery.query) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid query: ${error.message}",
            statusCode = Some(400),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("search")
          )
        )
      case None => // continue
    }

    val sql = elasticQuery.sql
    val query = elasticQuery.query
    val indices = elasticQuery.indices.mkString(",")

    logger.info(
      s"🔍 Searching with query \n$elasticQuery\nin indices '$indices'"
    )

    executeSingleSearch(elasticQuery) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed search for query \n$elasticQuery\nin indices '$indices'"
        )
        val aggs = toClientAggregations(aggregations)
        ElasticResult.fromTry(
          parseResponseTree(
            response,
            fieldAliases,
            aggs,
            fields,
            nestedHits,
            elasticQuery.explodeNested,
            retainDocumentId,
            Seq(rowInvariants)
          )
        ) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed search results for query \n$elasticQuery\nin indices '$indices'"
            )
            ElasticResult.success(
              ElasticResponse(
                sql,
                query,
                success.value,
                fieldAliases,
                aggs
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse search results for query \n${sql
                .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("search"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute search for query \n$elasticQuery\nin indices '$indices'",
            index = Some(indices),
            operation = Some("search")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute search for query \n${sql
            .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
        )
        ElasticResult.failure(
          enrichBoundError(error).copy(
            operation = Some("search"),
            index = Some(elasticQuery.indices.mkString(","))
          )
        )
    }

  }

  /** Multi-search with Elasticsearch queries.
    *
    * @param elasticQueries
    *   Elasticsearch queries
    * @param fieldAliases
    *   field aliases
    * @param aggregations
    *   SQL aggregations
    * @return
    *   the combined Elasticsearch response
    */
  def multiSearch(
    elasticQueries: ElasticQueries,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: Seq[ListMap[String, Any]] = Seq.empty,
    legProjections: Seq[LegProjection] = Seq.empty
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {
    elasticQueries.queries.flatMap { elasticQuery =>
      validateJson("search", elasticQuery.query).map(error =>
        elasticQuery.indices.mkString(",") -> error.message
      )
    } match {
      case Nil => // continue
      case errors =>
        return ElasticResult.failure(
          ElasticError(
            message = s"Invalid queries: ${errors.map(_._2).mkString(",")}",
            statusCode = Some(400),
            index = Some(errors.map(_._1).mkString(",")),
            operation = Some("multiSearch")
          )
        )
    }

    val query = elasticQueries.queries.map(_.query).mkString("\n")
    val sql = elasticQueries.sql.orElse(
      Option(elasticQueries.queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
    )

    logger.debug(
      s"🔍 Multi-searching with query \n$elasticQueries"
    )

    executeMultiSearch(elasticQueries) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed multi-search for query \n$elasticQueries"
        )
        val aggs = toClientAggregations(aggregations)
        ElasticResult.fromTry(
          parseResponseTree(
            response,
            fieldAliases,
            aggs,
            fields,
            nestedHits,
            elasticQueries.explodeNested,
            rowInvariants = rowInvariants,
            legProjections = legProjections
          )
        ) match {
          case success @ ElasticSuccess(_) =>
            logger.info(
              s"✅ Successfully parsed multi-search results for query '$elasticQueries'"
            )
            ElasticResult.success(
              ElasticResponse(
                sql,
                query,
                success.value,
                fieldAliases,
                aggs
              )
            )
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse multi-search results for query \n$elasticQueries\n -> ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("multiSearch")
              )
            )
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message = s"Failed to execute multi-search for query \n$elasticQueries",
            operation = Some("multiSearch")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute multi-search for query \n$elasticQueries\n -> ${error.message}"
        )
        ElasticResult.failure(
          enrichBoundError(error).copy(
            operation = Some("multiSearch")
          )
        )
    }
  }

  // ========================================================================
  // ASYNCHRONOUS SEARCH METHODS
  // ========================================================================

  /** Asynchronous search for documents / aggregations matching the SQL query.
    *
    * @param sqlQuery
    *   the SQL query
    * @return
    *   a Future containing the Elasticsearch response
    */
  def searchAsync(
    statement: SearchStatement
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    statement match {
      case select: SelectStatement =>
        select.statement match {
          case Some(statement: SingleSearch) =>
            searchAsync(statement.copy(score = select.score))
          case Some(statement: MultiSearch) =>
            searchAsync(statement)
          case None =>
            logger.error(
              s"❌ Failed to execute asynchronous search for query '${statement.sql}'"
            )
            Future.successful(
              ElasticResult.failure(
                ElasticError(
                  message = invalidSearchRequest(statement, statement.sql),
                  operation = Some("searchAsync")
                )
              )
            )
        }

      case parsed: SingleSearch =>
        // #276 -- resolve temporal literals against the mapped `date` columns BEFORE rendering
        resolveWithSchema(parsed) match {
          case ElasticSuccess(single) => searchResolvedAsync(single)
          case ElasticFailure(error)  => Future.successful(ElasticResult.failure(error))
        }

      case parsed: MultiSearch =>
        val multiple = resolveWithSchema(parsed) match {
          case ElasticSuccess(resolved) => resolved
          case ElasticFailure(error)    => return Future.successful(ElasticResult.failure(error))
        }
        // Story 22.6 — the async twin of the per-leg route; see `search`'s arm.
        if (unionAllNeedsPerLeg(multiple))
          return unionAllByLegAsync(multiple, statement.sql)
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multiSearchAsync(
          elasticQueries,
          multiple.fieldAliases,
          multiple.sqlAggregations,
          // See `search`'s arm: first leg names the columns, every leg builds its own rows.
          unionAllOutputFieldNames(multiple),
          multiple.requests.headOption.map(_.nestedHitsMappings).getOrElse(Map.empty),
          multiple.requests.map(rowInvariantsOf),
          unionAllLegProjections(multiple)
        )

      case _ =>
        val query = statement.sql
        logger.error(
          s"❌ Failed to execute asynchronous search for query '$query'"
        )
        Future.successful(
          ElasticResult.failure(
            ElasticError(
              message = invalidSearchRequest(statement, query),
              operation = Some("searchAsync")
            )
          )
        )
    }
  }

  /** Asynchronous search for documents / aggregations matching the Elasticsearch query.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   a Future containing the Elasticsearch response
    */
  def singleSearchAsync(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: ListMap[String, Any] = ListMap.empty
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val sql = elasticQuery.sql
    val query = elasticQuery.query
    val indices = elasticQuery.indices.mkString(",")
    executeSingleSearchAsync(elasticQuery)
      .flatMap {
        case ElasticSuccess(Some(response)) =>
          logger.info(
            s"✅ Successfully executed asynchronous search for query \n$elasticQuery\nin indices '$indices'"
          )
          val aggs = toClientAggregations(aggregations)
          ElasticResult.fromTry(
            parseResponseTree(
              response,
              fieldAliases,
              aggs,
              fields,
              nestedHits,
              elasticQuery.explodeNested,
              rowInvariants = Seq(rowInvariants)
            )
          ) match {
            case success @ ElasticSuccess(_) =>
              logger.info(
                s"✅ Successfully parsed search results for query \n$elasticQuery\nin indices '$indices'"
              )
              Future.successful(
                ElasticResult.success(
                  ElasticResponse(
                    sql,
                    query,
                    success.value,
                    fieldAliases,
                    aggs
                  )
                )
              )
            case ElasticFailure(error) =>
              logger.error(
                s"❌ Failed to parse search results for query \n${sql
                  .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
              )
              Future.successful(
                ElasticResult.failure(
                  error.copy(
                    operation = Some("searchAsync"),
                    index = Some(indices)
                  )
                )
              )
          }
        case ElasticSuccess(_) =>
          val error =
            ElasticError(
              message =
                s"Failed to execute asynchronous search for query \n$elasticQuery\nin indices '$indices'",
              index = Some(elasticQuery.indices.mkString(",")),
              operation = Some("searchAsync")
            )
          logger.error(s"❌ ${error.message}")
          Future.successful(ElasticResult.failure(error))
        case ElasticFailure(error) =>
          logger.error(
            s"❌ Failed to execute asynchronous search for query \n${sql
              .getOrElse(query)}\nin indices '$indices' -> ${error.message}"
          )
          Future.successful(
            ElasticResult.failure(
              enrichBoundError(error).copy(
                operation = Some("searchAsync"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
          )
      }
      .recover {
        // Issue #224 — some client implementations surface an execution failure as a FAILED future
        // rather than an ElasticFailure; without this recover the raw Throwable propagates to the
        // consumer, which typically flattens it into an opaque generic error. Honor the
        // ElasticResult contract here (and translate a max_result_window rejection on the way).
        case t: Throwable =>
          logger.error(
            s"❌ Failed to execute asynchronous search for query \n${sql
              .getOrElse(query)}\nin indices '$indices' -> ${t.getMessage}"
          )
          ElasticResult.failure(
            enrichBoundError(
              ElasticError(
                message = s"Failed to execute search: ${t.getMessage}",
                cause = Some(t),
                operation = Some("searchAsync"),
                index = Some(indices)
              )
            )
          )
      }
  }

  /** Asynchronous multi-search with Elasticsearch queries.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @return
    *   a Future containing the combined Elasticsearch response
    */
  def multiSearchAsync(
    elasticQueries: ElasticQueries,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    fields: Seq[String] = Seq.empty,
    nestedHits: Map[String, Seq[(String, String)]] = Map.empty,
    rowInvariants: Seq[ListMap[String, Any]] = Seq.empty,
    legProjections: Seq[LegProjection] = Seq.empty
  )(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val query = elasticQueries.queries.map(_.query).mkString("\n")
    val sql = elasticQueries.sql.orElse(
      Option(elasticQueries.queries.flatMap(_.sql).mkString("\nUNION ALL\n"))
    )

    executeMultiSearchAsync(elasticQueries)
      .flatMap {
        case ElasticSuccess(Some(response)) =>
          logger.info(
            s"✅ Successfully executed asynchronous multi-search for query \n$elasticQueries"
          )
          val aggs = toClientAggregations(aggregations)
          ElasticResult.fromTry(
            parseResponseTree(
              response,
              fieldAliases,
              aggs,
              fields,
              nestedHits,
              elasticQueries.explodeNested,
              rowInvariants = rowInvariants,
              legProjections = legProjections
            )
          ) match {
            case success @ ElasticSuccess(_) =>
              logger.info(
                s"✅ Successfully parsed multi-search results for query '$elasticQueries'"
              )
              Future.successful(
                ElasticResult.success(
                  ElasticResponse(
                    sql,
                    query,
                    success.value,
                    fieldAliases,
                    aggs
                  )
                )
              )
            case ElasticFailure(error) =>
              logger.error(
                s"❌ Failed to parse multi-search results for query \n$elasticQueries\n -> ${error.message}"
              )
              Future.successful(
                ElasticResult.failure(
                  error.copy(
                    operation = Some("multiSearchAsync")
                  )
                )
              )
          }
        case ElasticSuccess(_) =>
          val error =
            ElasticError(
              message = s"Failed to execute asynchronous multi-search for query \n$elasticQueries",
              operation = Some("multiSearchAsync")
            )
          logger.error(s"❌ ${error.message}")
          Future.successful(ElasticResult.failure(error))
        case ElasticFailure(error) =>
          logger.error(
            s"❌ Failed to execute asynchronous multi-search for query \n$elasticQueries\n -> ${error.message}"
          )
          Future.successful(
            ElasticResult.failure(
              enrichBoundError(error).copy(
                operation = Some("multiSearchAsync")
              )
            )
          )
      }
      .recover {
        // Issue #224 — same contract repair as singleSearchAsync: a client implementation may fail
        // the future instead of returning an ElasticFailure.
        case t: Throwable =>
          logger.error(
            s"❌ Failed to execute asynchronous multi-search for query \n$elasticQueries\n -> ${t.getMessage}"
          )
          ElasticResult.failure(
            enrichBoundError(
              ElasticError(
                message = s"Failed to execute multi-search: ${t.getMessage}",
                cause = Some(t),
                operation = Some("multiSearchAsync")
              )
            )
          )
      }
  }

  // ========================================================================
  // SEARCH METHODS WITH CONVERSION
  // ========================================================================

  /** Searches and converts results into typed entities from an SQL query.
    *
    * @note
    *   This method uses compile-time macros to validate the SQL query against the type U.
    *
    * @param query
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def searchAs[U](
    query: String
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] =
    macro SQLQueryMacros.searchAsImpl[U]

  /** Searches and converts results into typed entities from an SQL query.
    *
    * @note
    *   This method is a variant of searchAs without compile-time SQL validation.
    *
    * @param sqlQuery
    *   the SQL query containing fieldAliases and aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def searchAsUnchecked[U](
    sqlQuery: SelectStatement
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
    for {
      response <- search(sqlQuery.withoutNestedExplosion)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  /** Searches and converts results into typed entities.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the query
    */
  def singleSearchAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    formats: Formats
  ): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
    for {
      response <- singleSearch(elasticQuery, fieldAliases, aggregations)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  /** Multi-search with conversion to typed entities.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   the entities matching the queries
    */
  def multisearchAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    implicit val context: ConversionContext = EntityContext
    for {
      response <- multiSearch(elasticQueries, fieldAliases, aggregations)
      entities <- convertToEntities[U](response)
    } yield entities
  }

  // ========================================================================
  // ASYNCHRONOUS SEARCH METHODS WITH CONVERSION
  // ========================================================================

  /** Asynchronous search with conversion to typed entities.
    *
    * @note
    *   This method uses compile-time macros to validate the SQL query against the type U.
    *
    * @param query
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def searchAsyncAs[U](
    query: String
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] =
    macro SQLQueryMacros.searchAsyncAsImpl[U]

  /** Asynchronous search with conversion to typed entities.
    *
    * @note
    *   This method is a variant of searchAsyncAs without compile-time SQL validation.
    *
    * @param sqlQuery
    *   the SQL query
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def searchAsyncAsUnchecked[U](
    sqlQuery: SelectStatement
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
    searchAsync(sqlQuery.withoutNestedExplosion).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${sqlQuery.query}': ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query '${sqlQuery.query}'"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  /** Asynchronous search with conversion to typed entities.
    *
    * @param elasticQuery
    *   the Elasticsearch query
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def singleSearchAsyncAs[U](
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
    singleSearchAsync(elasticQuery, fieldAliases, aggregations).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous search for query '${elasticQuery.query}': ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous search for query '${elasticQuery.query}'"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  /** Asynchronous multi-search with conversion to typed entities.
    *
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param fieldAliases
    *   the field aliases
    * @param aggregations
    *   the SQL aggregations
    * @tparam U
    *   the type of entities to return
    * @return
    *   a Future containing the entities
    */
  def multiSearchAsyncAs[U](
    elasticQueries: ElasticQueries,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    ec: ExecutionContext,
    formats: Formats
  ): Future[ElasticResult[Seq[U]]] = {
    implicit val context: ConversionContext = EntityContext
    multiSearchAsync(elasticQueries, fieldAliases, aggregations).flatMap {
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute asynchronous multi-search with ${elasticQueries.queries.size} queries: ${error.message}"
        )
        Future.successful(ElasticResult.failure(error))
      case ElasticSuccess(response) =>
        logger.info(
          s"✅ Successfully executed asynchronous multi-search with ${elasticQueries.queries.size} queries"
        )
        Future.successful(convertToEntities[U](response))
    }
  }

  // ========================================================================
  // SEARCH METHODS WITH INNER HITS
  // ========================================================================

  @deprecated("Use `search` instead.", "v0.10")
  /** Search with inner hits from an SQL query.
    *
    * @deprecated
    *   Use `search` instead.
    * @param sql
    *   the SQL query
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   tuples (main entity, inner hits)
    */
  def searchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    sql: SelectStatement,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    sql.statement match {
      case Some(parsed: SingleSearch) =>
        val single = resolveWithSchema(parsed) match {
          case ElasticSuccess(resolved) => resolved
          case ElasticFailure(error)    => return ElasticResult.failure(error)
        }
        val elasticQuery = ElasticQuery(
          single,
          collection.immutable.Seq(single.sources: _*)
        )
        singleSearchWithInnerHits[U, I](elasticQuery, innerField)

      case Some(parsed: MultiSearch) =>
        val multiple = resolveWithSchema(parsed) match {
          case ElasticSuccess(resolved) => resolved
          case ElasticFailure(error)    => return ElasticResult.failure(error)
        }
        val elasticQueries = ElasticQueries(
          multiple.requests.map { query =>
            ElasticQuery(
              query,
              collection.immutable.Seq(query.sources: _*)
            )
          }.toList
        )
        multisearchWithInnerHits[U, I](elasticQueries, innerField)

      case None =>
        logger.error(
          s"❌ Failed to execute search with inner hits for query '${sql.query}'"
        )
        ElasticResult.failure(
          ElasticError(
            message = invalidSearchRequest(sql, sql.query),
            operation = Some("searchWithInnerHits")
          )
        )
    }
  }

  @deprecated("Use `search` instead.", "v0.10")
  /** Search with inner hits from an Elasticsearch query.
    *
    * @deprecated
    *   Use `search` instead.
    * @param elasticQuery
    *   the Elasticsearch query
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   tuples (main entity, inner hits)
    */
  def singleSearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQuery: ElasticQuery,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    validateJson("search", elasticQuery.query) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid query: ${error.message}",
            statusCode = Some(400),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("singleSearchWithInnerHits")
          )
        )
      case None => // continue
    }

    logger.debug(
      s"🔍 Searching inner hits with query '${elasticQuery.query}' in indices '${elasticQuery.indices
        .mkString(",")}'"
    )

    executeSingleSearch(elasticQuery) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed search with inner hits in indices '${elasticQuery.indices.mkString(",")}'"
        )
        ElasticResult.attempt(parseInnerHits[U, I](response, innerField)) match {
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse Elasticsearch response for search with inner hits in indices '${elasticQuery.indices
                .mkString(",")}': ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("singleSearchWithInnerHits"),
                index = Some(elasticQuery.indices.mkString(","))
              )
            )
          case success => success
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute search with inner hits in indices '${elasticQuery.indices.mkString(",")}'",
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("singleSearchWithInnerHits")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute search with inner hits in indices '${elasticQuery.indices
            .mkString(",")}': ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("singleSearchWithInnerHits"),
            index = Some(elasticQuery.indices.mkString(","))
          )
        )
    }
  }

  @deprecated("Use `multisearch` instead.", "v0.10")
  /** Multisearch with inner hits from Elasticsearch queries.
    *
    * @deprecated
    *   Use `multisearch` instead.
    * @param elasticQueries
    *   the Elasticsearch queries
    * @param innerField
    *   the field for inner hits
    * @tparam U
    *   the type of the main entity
    * @tparam I
    *   the type of inner hits
    * @return
    *   a sequence of results with inner hits
    */
  def multisearchWithInnerHits[U: Manifest: ClassTag, I: Manifest: ClassTag](
    elasticQueries: ElasticQueries,
    innerField: String
  )(implicit
    formats: Formats
  ): ElasticResult[Seq[(U, Seq[I])]] = {
    elasticQueries.queries.flatMap { elasticQuery =>
      validateJson("search", elasticQuery.query).map(error =>
        elasticQuery.indices.mkString(",") -> error.message
      )
    } match {
      case Nil => // continue
      case errors =>
        return ElasticResult.failure(
          ElasticError(
            message = s"Invalid queries: ${errors.map(_._2).mkString(",")}",
            statusCode = Some(400),
            index = Some(errors.map(_._1).mkString(",")),
            operation = Some("multisearchWithInnerHits")
          )
        )
    }

    logger.debug(
      s"🔍 Multi-searching inner hits with ${elasticQueries.queries.size} queries"
    )

    executeMultiSearch(elasticQueries) match {
      case ElasticSuccess(Some(response)) =>
        logger.info(
          s"✅ Successfully executed multi-search inner hits with ${elasticQueries.queries.size} queries"
        )
        ElasticResult.attempt(parseInnerHits[U, I](response, innerField)) match {
          case ElasticFailure(error) =>
            logger.error(
              s"❌ Failed to parse Elasticsearch response for multi-search inner hits with ${elasticQueries.queries.size} queries: ${error.message}"
            )
            ElasticResult.failure(
              error.copy(
                operation = Some("multisearchWithInnerHits")
              )
            )
          case success => success
        }
      case ElasticSuccess(_) =>
        val error =
          ElasticError(
            message =
              s"Failed to execute multi-search inner hits with ${elasticQueries.queries.size} queries",
            operation = Some("multisearchWithInnerHits")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to execute multi-search inner hits with ${elasticQueries.queries.size} queries: ${error.message}"
        )
        ElasticResult.failure(
          error.copy(
            operation = Some("multisearchWithInnerHits")
          )
        )
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  /** Execute the search and hand back the response as an already-parsed Jackson tree (#228).
    *
    * The tree contract kills the historical double parse: implementations must materialize the
    * Elasticsearch response as the Jackson tree core consumes — parsing the raw response bytes
    * exactly once, or re-parenting `_source` trees the transport already parsed — never by
    * serializing a typed response back to a JSON String for core to re-parse.
    */
  private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[JsonNode]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[JsonNode]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[JsonNode]]]

  /** @see [[executeSingleSearch]] for the single-parse tree contract (#228). */
  private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[JsonNode]]]

  // ================================================================================
  // IMPLICIT CONVERSIONS
  // ================================================================================

  /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query serialization.
    *
    * @param sqlSearch
    *   the SQL search request to convert
    * @return
    *   JSON string representation of the query
    */
  private[client] implicit def singleSearchToJsonQuery(sqlSearch: SingleSearch)(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String

  private def parseInnerHits[M: Manifest: ClassTag, I: Manifest: ClassTag](
    searchResult: JsonNode,
    innerField: String
  )(implicit formats: Formats): Seq[(M, Seq[I])] = {
    val mManifest = implicitly[Manifest[M]]
    val iManifest = implicitly[Manifest[I]]
    val mClass = classTag[M].runtimeClass
    val iClass = classTag[I].runtimeClass

    logger.info(
      s"🔍 Processing inner hits with types: M=${mClass.getSimpleName}, I=${iClass.getSimpleName}"
    )

    def innerHits(result: JsonNode): Iterator[JsonNode] = {
      val hits = result
        .path("inner_hits")
        .path(innerField)
        .path("hits")
        .path("hits")
      if (!hits.isArray) {
        throw new IllegalStateException(
          s"No inner hits found for field '$innerField' in search response"
        )
      }
      hits.elements().asScala
    }

    val hits = searchResult.path("hits").path("hits")
    if (!hits.isArray) {
      throw new IllegalStateException("No hits found in search response")
    }

    (for (result <- hits.elements().asScala)
      yield (
        result match {
          case obj if obj.isObject =>
            Try {
              val source = mapper.writeValueAsString(obj.get("_source"))
              logger.debug(
                s"Deserializing main entity ${mClass.getSimpleName} from source: $source"
              )
              serialization.read[M](source)(formats, mManifest)
            } match {
              case Success(s) => s
              case Failure(f) =>
                logger.error(s"❌ Failed to deserialize main entity: ${f.getMessage}", f)
                throw f
            }
          case other => serialization.read[M](other.asText())(formats, mManifest)
        },
        (for (innerHit <- innerHits(result)) yield innerHit match {
          case obj if obj.isObject =>
            Try {
              val source = mapper.writeValueAsString(obj.get("_source"))
              logger.debug(
                s"Deserializing inner hit entity ${iClass.getSimpleName} from source: $source"
              )
              serialization.read[I](source)(formats, iManifest)
            } match {
              case Success(s) => s
              case Failure(f) =>
                logger.error(s"❌ Failed to deserialize inner hit entity: ${f.getMessage}")
                throw f
            }
          case other => serialization.read[I](other.asText())(formats, iManifest)
        }).toList
      )).toList
  }

  // ========================================================================
  // PRIVATE HELPERS
  // ========================================================================

  /** Converts an Elasticsearch response to typed entities.
    *
    * @param response
    *   the Elasticsearch response
    * @tparam U
    *   the type of entities to convert to
    * @return
    *   ElasticResult containing the entities or an error
    */
  private def convertToEntities[U](
    response: ElasticResponse
  )(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]] = {
    val results = ElasticResult.fromTry(convertTo[U](response))
    results
      .fold(
        onFailure = error => {
          logger.error(
            s"❌ Conversion to entities failed: ${error.message} with query \n${response.query}\n and results:\n ${response.results}"
          )
          ElasticResult.failure(
            ElasticError(
              message = s"Failed to convert search results to ${m.runtimeClass.getSimpleName}",
              cause = error.cause,
              operation = Some("convertToEntities")
            )
          )
        },
        onSuccess = entities => ElasticResult.success(entities)
      )
  }

  // ========================================================================
  // WINDOW FUNCTION SEARCH
  // ========================================================================

  /** Search with window function enrichment
    * {{{
    * Strategy:
    *   1. Execute aggregation query to compute window values
    *   2. Execute main query (without window functions)
    *   3. Enrich results with window values
    * }}}
    */
  private def searchWithWindowEnrichment(
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[ElasticResponse] = {

    logger.info(s"🪟 Detected ${request.windowFunctions.size} window functions")

    for {
      // Step 1: Execute window aggregations
      windowCache <- executeWindowAggregations(request)

      // Step 2: Execute base query (without window functions)
      baseResponse <- executeBaseQuery(request)

      // Step 3: Enrich results
      enrichedResponse <- enrichResponseWithWindowValues(baseResponse, windowCache, request)

    } yield enrichedResponse
  }

  // ========================================================================
  // WINDOW AGGREGATION EXECUTION
  // ========================================================================

  /** Execute aggregation queries for all window functions Returns a cache of partition key ->
    * window values
    */
  protected def executeWindowAggregations(
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[WindowCache] = {
    val (aggRequest, elasticQuery) = windowAggregationQuery(request)
    executeWindowAggregations(request, aggRequest, elasticQuery)
  }

  /** The window-aggregation request of `request` and its TRANSLATION to an Elasticsearch body.
    *
    * Kept separate from the execution (issue #222): the translation is where a client module may
    * REFUSE the statement (a status-bearing `ElasticError`, e.g. STDDEV / VARIANCE over a
    * transformed expression on ES 6 / ES 7), and a refusal known at translation time must be
    * answered at `GatewayApi.run` -- so a caller that executes asynchronously translates here
    * FIRST, on the calling thread, and only then schedules the execution.
    */
  private[client] def windowAggregationQuery(
    request: SingleSearch
  )(implicit timestamp: Long): (SingleSearch, ElasticQuery) = {
    val aggRequest = buildWindowAggregationRequest(request)
    val elasticQuery = ElasticQuery(
      aggRequest,
      collection.immutable.Seq(aggRequest.sources: _*),
      sql = Some(aggRequest.sql)
    )
    (aggRequest, elasticQuery)
  }

  /** Execute an already-translated window-aggregation request -- see [[windowAggregationQuery]]. */
  private[client] def executeWindowAggregations(
    request: SingleSearch,
    aggRequest: SingleSearch,
    elasticQuery: ElasticQuery
  )(implicit context: ConversionContext): ElasticResult[WindowCache] = {

    logger.info(
      s"🔍 Executing window aggregation query:\n${aggRequest.sql}"
    )

    for {
      // Use singleSearch to execute aggregation
      aggResponse <- singleSearch(
        elasticQuery,
        aggRequest.fieldAliases,
        aggRequest.sqlAggregations,
        extractOutputFieldNames(aggRequest),
        aggRequest.nestedHitsMappings
      )

      // Parse aggregation results into cache
      cache <- parseWindowAggregationsToCache(aggResponse, request)

    } yield cache
  }

  /** Build aggregation request for window functions
    */
  private def buildWindowAggregationRequest(
    request: SingleSearch
  ): SingleSearch = {

    // Create modified request with:
    // - Only window buckets in GROUP BY
    // - Only window aggregations in SELECT
    // - No LIMIT (need all partitions)
    // - Same WHERE clause (to match base query filtering)
    request
      .copy(
        select = request.select.copy(fields = request.windowFields.map(_.update(request))),
        groupBy = None, //request.groupBy.map(_.copy(buckets = request.windowBuckets)),
        orderBy = None, // Not needed for aggregations
        limit = None // Need all buckets
      )
      .update()
  }

  /** Parse aggregation response into window cache Uses your existing
    * ElasticConversion.parseResponse
    */
  private def parseWindowAggregationsToCache(
    response: ElasticResponse,
    request: SingleSearch
  ): ElasticResult[WindowCache] = {

    logger.info(
      s"🔍 Parsing window aggregations to cache for query \n${response.sql.getOrElse(response.query)}"
    )

    val aggRows = response.results

    logger.info(s"✅ Parsed ${aggRows.size} aggregation buckets")

    // Ranking-style windows in the original request, paired with their
    // SELECT-field alias (which is the key under which the top_hits
    // sub-aggregation surfaces in the parsed agg row). Ranking windows have
    // an empty positional identifier, so we can't recover the alias from
    // the AST alone — pull it from the field that wraps them.
    val rankingWindows: Seq[(String, RankingWindow)] =
      request.windowFields.flatMap { f =>
        f.identifier.windows.collect { case r: RankingWindow =>
          f.outputName -> r
        }
      }

    val cache = aggRows.map { row =>
      val partitionKey = extractPartitionKey(row, request)
      val windowValues = extractWindowValues(row, response.aggregations)
      val rankings = extractRankings(row, rankingWindows)
      partitionKey -> windowValues.copy(rankings = rankings)
    }

    ElasticResult.success(WindowCache(ListMap(cache: _*)))
  }

  /** Read each ranking window's top_hits results from the parsed aggregation row, compute ordinals
    * via the window's `assignOrdinals` (per its tie rule), and return a `fieldAlias -> (rowId ->
    * rank)` map per partition.
    *
    * Each `(name, rw)` pair carries the SELECT-field alias (the key under which the top_hits
    * sub-agg surfaces in `row`) and the ranking window itself.
    */
  /** Resolve an OVER ORDER BY column value from a top_hits inner-source map. The inner-source map
    * keeps nested objects un-flattened, so a dotted column (e.g. `address.salary`) is not a
    * top-level key. Fall back to walking the dotted path into nested maps. This is purely additive
    * — a flat column hits the direct lookup and behaves exactly as before.
    */
  private def resolveSortKey(h: Map[String, Any], col: String): Any =
    h.get(col) match {
      case Some(v) => v
      case None =>
        col
          .split('.')
          .foldLeft(Option[Any](h)) {
            case (Some(m: Map[_, _]), part) =>
              m.asInstanceOf[Map[String, Any]].get(part)
            case _ => None
          }
          .orNull
    }

  private def extractRankings(
    row: ListMap[String, Any],
    rankingWindows: Seq[(String, RankingWindow)]
  ): Map[String, Map[String, Long]] = {
    if (rankingWindows.isEmpty) Map.empty
    else {
      rankingWindows.flatMap { case (name, rw) =>
        val orderByCols: Seq[String] =
          rw.orderBy.toSeq.flatMap(_.sorts.map(_.field.name))
        val hits: Seq[Map[String, Any]] = row.get(name) match {
          case Some(l: List[_]) =>
            l.collect { case m: Map[_, _] =>
              m.asInstanceOf[Map[String, Any]]
            }
          case _ => Seq.empty
        }
        if (hits.isEmpty) None
        else {
          val ordered: Seq[(String, Seq[Any])] = hits.map { h =>
            val rowId = h.getOrElse("_id", "").toString
            val key = orderByCols.map(c => resolveSortKey(h, c))
            rowId -> key
          }
          Some(name -> rw.assignOrdinals(ordered).toMap)
        }
      }.toMap
    }
  }

  // ========================================================================
  // BASE QUERY EXECUTION
  // ========================================================================

  /** Execute base query without window functions
    */
  private def executeBaseQuery(
    request: SingleSearch
  )(implicit timestamp: Long, context: ConversionContext): ElasticResult[ElasticResponse] = {

    val baseQuery = createBaseQuery(request)

    logger.info(s"🔍 Executing base query without window functions ${baseQuery.sql}")

    // Retain `_id` on the parsed rows: the enrichment step matches each base row to its
    // ranking ordinals by document id. The id is stripped after enrichment.
    singleSearchInternal(
      ElasticQuery(
        baseQuery,
        collection.immutable.Seq(baseQuery.sources: _*),
        sql = Some(baseQuery.sql)
      ),
      baseQuery.fieldAliases,
      baseQuery.sqlAggregations,
      extractOutputFieldNames(baseQuery),
      baseQuery.nestedHitsMappings,
      retainDocumentId = true
    )
  }

  /** Create base query by removing window functions from SELECT
    */
  protected def createBaseQuery(
    request: SingleSearch
  ): SingleSearch = {

    // Remove window function fields from SELECT
    val baseFields = request.select.fields.filterNot(_.identifier.hasWindow)

    // Create modified request
    val baseRequest = request
      .copy(
        select = request.select.copy(fields = baseFields)
      )
      .update()

    baseRequest
  }

  /** Extract partition key from aggregation row
    */
  private def extractPartitionKey(
    row: ListMap[String, Any],
    request: SingleSearch
  ): PartitionKey = {

    // Get all partition fields from window functions
    val partitionFields = request.windowFunctions
      .flatMap(_.partitionBy)
      .map(_.aliasOrName)
      .distinct

    if (partitionFields.isEmpty) {
      return PartitionKey(ListMap("__global__" -> true))
    }

    val keyValues = partitionFields.flatMap { field =>
      row.get(field).map(field -> _)
    }

    PartitionKey(ListMap(keyValues: _*))
  }

  /** Extract window function values from aggregation row
    */
  private def extractWindowValues(
    row: ListMap[String, Any],
    aggregations: ListMap[String, ClientAggregation]
  ): WindowValues = {

    val values = extractAggregationValues(row, aggregations)

    WindowValues(values)
  }

  // ========================================================================
  // RESULT ENRICHMENT
  // ========================================================================

  /** Enrich response with window values
    */
  private def enrichResponseWithWindowValues(
    response: ElasticResponse,
    cache: WindowCache,
    request: SingleSearch
  )(implicit context: ConversionContext): ElasticResult[ElasticResponse] = {

    val baseRows = response.results
    val outputFields = extractOutputFieldNames(request)

    // Determine ONCE whether the document ID stays in the output — never per row
    val shouldKeepDocumentId = keepsDocumentId(outputFields)

    // Built once for the whole result set — never per row (#229)
    val normalizeOutputRow = rowNormalizer(outputFields)

    // Enrich each row with window values, then normalize field order. The base rows carry
    // their `_id` (see singleSearchInternal with retainDocumentId = true) for the ordinal
    // lookup — strip it on the way out unless the document-id column is enabled or `_id`
    // is selected. Only window-enriched rows ever pay this per-row strip.
    val enrichedRows = baseRows.map { row =>
      val enriched = enrichDocumentWithWindowValues(row, cache, request)
      val normalized = normalizeOutputRow(enriched)
      if (shouldKeepDocumentId) normalized
      else normalized - ElasticConversion.DocumentIdField
    }

    ElasticResult.success(response.copy(results = enrichedRows))
  }

  /** Enrich a single document with window values
    */
  protected def enrichDocumentWithWindowValues(
    doc: ListMap[String, Any],
    cache: WindowCache,
    request: SingleSearch
  ): ListMap[String, Any] = {

    if (request.windowFunctions.isEmpty) {
      return doc
    }

    // Build partition key from document
    val partitionKey = extractPartitionKey(doc, request)
    val rowId = doc.get("_id").map(_.toString).getOrElse("")

    val rankingAliases: Seq[String] =
      request.windowFields.flatMap { f =>
        f.identifier.windows.collect { case _: RankingWindow =>
          f.outputName
        }
      }

    // Lookup window values
    cache.get(partitionKey) match {
      case Some(windowValues) =>
        // Aggregation-style windows: merge the per-partition scalars.
        val withScalars = doc ++ windowValues.values

        // Ranking-style windows: look up the ordinal by row _id and inject
        // it under the SELECT-field alias. Rows that the top_hits sub-agg
        // didn't return (e.g. when the LIMIT push-down kept only top-N per
        // partition) receive null.
        if (rankingAliases.isEmpty) withScalars
        else {
          val rankEntries = rankingAliases.map { name =>
            val value = windowValues.rankings
              .get(name)
              .flatMap(_.get(rowId))
              .map(Long.box(_): Any)
              .orNull
            name -> value
          }
          withScalars ++ ListMap(rankEntries: _*)
        }

      case None =>
        logger.warn(s"⚠️ No window values found for partition: ${partitionKey.values}")

        // Add null values for missing window functions. Aggregation-style
        // windows key off their own alias/name; ranking windows have an empty
        // positional identifier, so their null must be injected under the
        // SELECT-field alias (mirrors the Some-branch).
        val aggNulls = request.windowFunctions.collect {
          case wf if !wf.isInstanceOf[RankingWindow] =>
            wf.identifier.aliasOrName -> (null: Any)
        }
        val rankingNulls = rankingAliases.map(_ -> (null: Any))

        doc ++ ListMap(aggNulls: _*) ++ ListMap(rankingNulls: _*)
    }
  }

  // ========================================================================
  // HELPER CASE CLASSES
  // ========================================================================

  /** Partition key for window function cache
    */
  protected case class PartitionKey(values: ListMap[String, Any]) {
    override def hashCode(): Int = values.hashCode()
    override def equals(obj: Any): Boolean = obj match {
      case other: PartitionKey => values == other.values
      case _                   => false
    }
  }

  /** Window function values for a partition.
    *
    * `values` carries the existing per-partition scalars (aggregation-style windows:
    * SUM/COUNT/MIN/MAX/AVG, plus FIRST_VALUE/LAST_VALUE/ARRAY_AGG).
    *
    * `rankings` carries the per-row ordinals computed Scala-side from each ranking window's
    * top_hits sub-aggregation: a map `windowFunction.aliasOrName → (rowId → rank)`. The base-row
    * enrichment step looks up the ordinal by `doc._id` for each ranking window.
    */
  protected case class WindowValues(
    values: ListMap[String, Any],
    rankings: Map[String, Map[String, Long]] = Map.empty
  )

  /** Cache of partition key -> window values
    */
  protected case class WindowCache(cache: ListMap[PartitionKey, WindowValues]) {
    def get(key: PartitionKey): Option[WindowValues] = cache.get(key)
    def size: Int = cache.size
  }

  // ========================================================================
  // ROW COMPLETENESS — SCROLL ROUTING (issues #209 / #224)
  // ========================================================================

  /** True when a row-shaped query must page through scroll instead of the one-shot search:
    *
    *   - no `LIMIT` at all (#209): a one-shot search cannot honor "no LIMIT means every row" — with
    *     no `size` Elasticsearch returns its default 10 hits;
    *   - an explicit `LIMIT` whose window (`offset + limit`) exceeds
    *     [[SearchApi.DefaultMaxResultWindow]] (#224): Elasticsearch rejects a one-shot search
    *     whenever `from + size > index.max_result_window` (default 10,000), so the SAME query would
    *     fail with `LIMIT 20000` yet succeed with no LIMIT. The window is per-index and not probed
    *     (a multi-index query has no single window anyway); on an index tuned HIGHER the query
    *     pages unnecessarily but stays correct — and a >10k one-shot response is better paged
    *     regardless.
    */
  /** Story 22.6 — true when at least one leg of a `UNION ALL` cannot be answered one-shot.
    *
    * `_msearch` sends every leg as its own one-shot request, so a row-shaped leg with no bounding
    * LIMIT silently comes back with Elasticsearch's default 10 hits. Aggregation-shaped legs and
    * legs bounded within `max_result_window` are unaffected and keep the single `_msearch`.
    *
    * ONE boolean per statement, decided on the AST: a bounded `UNION ALL` pays nothing.
    */
  private def unionAllNeedsPerLeg(multiple: MultiSearch): Boolean =
    this.isInstanceOf[ScrollApi] &&
    multiple.requests.exists(r => r.returnsRows && requiresScrollPaging(r.limit))

  /** Story 22.6 — sequential per-leg execution of a `UNION ALL` whose legs cannot all be one-shot.
    *
    * Rows are concatenated in LEG ORDER (the `_msearch` order). Output names come from the first
    * leg — the pre-existing simplification of the one-shot path, kept so the two routes agree. Each
    * leg's response is whatever `search(leg)` produces, so the schema attach, the temporal
    * literals, the scroll routing and the #224 error translation all apply per leg BY CONSTRUCTION
    * rather than by a second copy of any of them.
    *
    * SEQUENTIAL, not parallel, and that is a memory decision: N un-LIMITed legs in parallel would
    * open N scrolls and hold N partial results for a consumer that concatenates them anyway.
    *
    * ⚠️ Within a leg, row ORDER may differ from the one-shot order — an un-ordered extraction is
    * interleaved across sliced PIT readers (issue #238). Release-noted.
    */
  private def unionAllByLeg(multiple: MultiSearch, sql: String)(implicit
    context: ConversionContext
  ): ElasticResult[ElasticResponse] = {
    val zero: ElasticResult[Seq[ElasticResponse]] = ElasticResult.success(Seq.empty)
    multiple.requests
      .foldLeft(zero) {
        case (ElasticSuccess(acc), leg) =>
          // 🔴 [[searchResolved]], not `search` — the legs arrive ALREADY resolved from the seam
          // in `search`, and `resolveWithSchema` is not a pure check: a leg carrying a WHERE
          // subquery has its inner statement EXECUTED against Elasticsearch by
          // `SubqueryResolver`, uncached. Re-entering `search` here resolved every leg a second
          // time (issue #355); the licensed cap path was repaired the same way in story 22.6 and
          // this route still had it.
          searchResolved(leg, leg.sql) match {
            case ElasticSuccess(r)     => ElasticResult.success(acc :+ r)
            case ElasticFailure(error) => ElasticResult.failure(error)
          }
        case (failure, _) => failure
      }
      .map(mergeLegResponses(multiple, sql, _))
  }

  /** Execute a SingleSearch whose schema and temporal literals are ALREADY resolved.
    *
    * The body `search`'s `SingleSearch` arm used to inline, lifted so that a caller holding a
    * resolved statement can execute it without paying for — or re-running — the resolution (issue
    * #355). `search` reaches it through `resolveWithSchema`; `unionAllByLeg` reaches it with the
    * legs that seam already resolved.
    */
  private def searchResolved(single: SingleSearch, sql: String)(implicit
    context: ConversionContext
  ): ElasticResult[ElasticResponse] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    val elasticQuery = ElasticQuery(
      single,
      collection.immutable.Seq(single.sources: _*),
      sql = Some(sql),
      explodeNested = single.explodeNested
    )
    this match {
      case scrollApi: ScrollApi if single.returnsRows && requiresScrollPaging(single.limit) =>
        // A row query is data-bound, not time-bound: every page request below
        // carries its own timeout, so the stream always terminates.
        Await.result(
          scrollRows(scrollApi, single, elasticQuery),
          Duration.Inf
        )
      case _ =>
        if (single.windowRowQuery)
          searchWithWindowEnrichment(single)
        else
          singleSearch(
            elasticQuery,
            single.fieldAliases,
            single.sqlAggregations,
            extractOutputFieldNames(single),
            single.nestedHitsMappings,
            rowInvariantsOf(single)
          )
    }
  }

  /** The async twin of [[searchResolved]]. */
  private def searchResolvedAsync(single: SingleSearch)(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    implicit def timestamp: Long = System.currentTimeMillis()
    val elasticQuery = ElasticQuery(
      single,
      collection.immutable.Seq(single.sources: _*)
    )
    this match {
      case scrollApi: ScrollApi if single.returnsRows && requiresScrollPaging(single.limit) =>
        scrollRows(scrollApi, single, elasticQuery)
      case _ =>
        if (single.windowRowQuery)
          Future.successful(searchWithWindowEnrichment(single))
        else
          singleSearchAsync(
            elasticQuery,
            single.fieldAliases,
            single.sqlAggregations,
            extractOutputFieldNames(single),
            single.nestedHitsMappings,
            rowInvariantsOf(single)
          )
    }
  }

  /** The async twin. Sequential futures — same shape, same memory argument. */
  private def unionAllByLegAsync(multiple: MultiSearch, sql: String)(implicit
    ec: ExecutionContext,
    context: ConversionContext
  ): Future[ElasticResult[ElasticResponse]] = {
    val zero: Future[ElasticResult[Seq[ElasticResponse]]] =
      Future.successful(ElasticResult.success(Seq.empty))
    multiple.requests
      .foldLeft(zero) { (accF, leg) =>
        accF.flatMap {
          case ElasticSuccess(acc) =>
            // Already resolved by the seam — see `unionAllByLeg` (issue #355).
            searchResolvedAsync(leg).map {
              case ElasticSuccess(r)     => ElasticResult.success(acc :+ r)
              case ElasticFailure(error) => ElasticResult.failure(error)
            }
          case failure => Future.successful(failure)
        }
      }
      .map(_.map(mergeLegResponses(multiple, sql, _)))
  }

  /** THE row contract of a `UNION ALL`, in one place, for every route that concatenates legs.
    *
    * A `UNION ALL` has three execution routes — the one-shot `_msearch`, the per-leg route above,
    * and `CoreDqlExtension`'s licensed cap fold — and a caller must not be able to tell them apart
    * from the rows.
    *
    * 🔴 SQL-92 §7.10 matches set-operation branches BY ORDINAL POSITION, never by name: the
    * branches must agree on DEGREE and on the type of the i-th column, and the result takes the
    * FIRST branch's names. `CORRESPONDING` — the optional clause that asks for name-based matching
    * — is the proof, because nobody adds an opt-in for the behaviour they already have (almost
    * nobody implements it; DuckDB went the other way and added a non-standard `UNION BY NAME`).
    *
    * Every route used to look the first branch's names up IN THE LEG'S ROW, which is the path of
    * least resistance from a wire format that is a name → value map — and a wrong answer with HTTP
    * 200 (issue #354). MEASURED on real ES 8.18:
    *
    *   - `SELECT a, b FROM x UNION ALL SELECT a, a FROM y` — both parse-time guards admit it (same
    *     degree, same types) and column 2 of branch 2 came back NULL;
    *   - `SELECT id AS x FROM l UNION ALL SELECT id AS y FROM r` — `MultiSearch.fieldAliases`
    *     merges the branches' maps keyed by SOURCE field, so one of `x`/`y` survived and EVERY row,
    *     branch 1's own included, answered `{x -> null, y -> …}`.
    *
    * 🔴 The trap in the fix, and the reason this takes a NAME LIST rather than a row: "position"
    * means the index into the branch's DECLARED projection, never an index into whatever order the
    * row map happens to enumerate. A story-22.6 draft derived it from `row.keys` and put values
    * under the wrong column names — HTTP 200 again, and strictly harder to detect than the
    * raggedness it replaced, because the column names looked right.
    *
    * Two shapes are therefore left matched BY NAME on purpose, not by omission:
    *
    *   - a FIRST branch of `SELECT *` yields no names, so there is nothing to match against and
    *     every leg keeps its OWN projection (its own names, its own aliases — never the merged map,
    *     see [[unionAllLegProjections]]), which is what that leg would answer on its own;
    *   - an OPAQUE leg (`SELECT *` past the first branch) declares no projection of its own —
    *     `MultiSearch.declared` is `None` for it and the arity/type guards exempt it — so its rows
    *     arrive in `_source` order and a positional `zip` would put the id under the first branch's
    *     first column and DROP everything past its width. It keeps the first branch's names, looked
    *     up by name, which is the only matching an opaque projection admits.
    *
    * The returned functions hoist every stream-constant decision (the name arrays, the index, the
    * context) ONCE per statement; one is applied per row on the un-LIMITed extraction path, where
    * "per row" can mean millions (`feedback_no_per_row_hot_path_work`). Where the branches agree on
    * their column names — the overwhelmingly common case — every mapper is `rowNormalizer` itself,
    * fast path and all, so a `UNION ALL` of homogeneous branches pays exactly what it paid before.
    *
    * @return
    *   ONE mapper per leg, in leg order.
    */
  private[client] def unionAllRowMappers(
    multiple: MultiSearch
  )(implicit context: ConversionContext): Seq[ListMap[String, Any] => ListMap[String, Any]] = {
    val outputFields = unionAllOutputFieldNames(multiple)
    if (outputFields.isEmpty) multiple.requests.map(_ => identity[ListMap[String, Any]] _)
    else
      multiple.requests.map { leg =>
        rowProjector(unionAllLegFieldNames(leg, outputFields), outputFields)
      }
  }

  /** The names the result's columns take: the FIRST branch's, SQL-92 §7.10. */
  private def unionAllOutputFieldNames(multiple: MultiSearch): Seq[String] =
    multiple.requests.headOption.map(extractOutputFieldNames).getOrElse(Seq.empty)

  /** The projection a leg's rows are BUILT from — its own, or the result's names when the leg is an
    * opaque `SELECT *` that declares none (see [[unionAllRowMappers]]).
    */
  private def unionAllLegFieldNames(leg: SingleSearch, outputFields: Seq[String]): Seq[String] = {
    val own = extractOutputFieldNames(leg)
    if (own.isEmpty) outputFields else own
  }

  /** Everything each leg of a one-shot `_msearch` needs to build ITS OWN rows (issue #354).
    *
    * 🔴 Emitted even when the FIRST branch declares no projection, and that is not cosmetic: with
    * no projections `parseMultiSearchResponse` falls back to `multiple.fieldAliases`, which merges
    * the branches' maps keyed by the SOURCE field — so `SELECT * FROM t UNION ALL SELECT id AS x
    * FROM l UNION ALL SELECT id AS y FROM r` kept ONE of `x`/`y` and renamed BOTH later legs with
    * it. That is #354 case 2's exact mechanism, surviving behind an opaque first branch. There is
    * still nothing to RENAME to (the result has no declared names), so each leg simply keeps its
    * own — which is precisely what `search(leg)` gives the per-leg route, so the two agree.
    */
  private def unionAllLegProjections(multiple: MultiSearch): Seq[LegProjection] = {
    val outputFields = unionAllOutputFieldNames(multiple)
    multiple.requests.map { leg =>
      LegProjection(
        leg.fieldAliases,
        unionAllLegFieldNames(leg, outputFields),
        leg.nestedHitsMappings
      )
    }
  }

  /** ONE merge, two callers; the row contract is [[unionAllRowMappers]]'s. */
  private def mergeLegResponses(
    multiple: MultiSearch,
    sql: String,
    responses: Seq[ElasticResponse]
  )(implicit context: ConversionContext): ElasticResponse = {
    val mappers = unionAllRowMappers(multiple)
    ElasticResponse(
      Some(sql),
      responses.map(_.query).mkString("\n"),
      responses.zipWithIndex.flatMap { case (response, leg) =>
        val normalise = mappers.applyOrElse(leg, (_: Int) => identity[ListMap[String, Any]] _)
        response.results.map(normalise)
      },
      multiple.fieldAliases,
      toClientAggregations(multiple.sqlAggregations)
    )
  }

  private def requiresScrollPaging(limit: Option[Limit]): Boolean =
    limit match {
      case None => true
      case Some(l) =>
        l.limit.toLong + l.offset.map(_.offset.toLong).getOrElse(0L) >
          SearchApi.DefaultMaxResultWindow
    }

  /** Issue #224 — a one-shot search whose `from + size` exceeds `index.max_result_window` is
    * rejected by Elasticsearch with an `illegal_argument_exception` that names neither the SQL
    * `LIMIT` that produced the `size` nor the remedy — and downstream consumers often flatten it
    * further. An index tuned BELOW the routing threshold of [[SearchApi.DefaultMaxResultWindow]]
    * can still surface the rejection despite the scroll routing, so translate it into an actionable
    * message here; every other error passes through unchanged.
    */
  private[client] def enrichBoundError(error: ElasticError): ElasticError =
    enrichMaxTermsCountError(enrichMaxResultWindowError(error))

  /** Story 22.2 (AD-9) — the residual Elasticsearch-side bound, translated exactly as
    * `index.max_result_window` was by #224.
    *
    * An index tuned BELOW the default (`index.max_terms_count = 100`) rejects a larger `terms`
    * query with a shard-level `illegal_argument_exception` that names neither the subquery that
    * produced the list nor the remedy; and mode P's own overflow on ES >= 7.10 is a
    * `too_many_buckets_exception` against `search.max_buckets`. Both are the SAME bound from the
    * analyst's point of view, so both become the SAME message `SubqueryResolver.tooMany` emits —
    * one text whichever side hit the limit first. Every other error passes through unchanged.
    */
  private def enrichMaxTermsCountError(error: ElasticError): ElasticError = {
    def mentionsTerms(message: String): Boolean =
      message != null &&
      (message.contains("max_terms_count") || message.contains("too_many_buckets"))
    // The REST high-level clients (ES 6/7) surface the per-shard root cause as SUPPRESSED
    // exceptions on an "all shards failed" wrapper, so the scan walks both chains (bounded).
    def throwableMentionsTerms(t: Throwable, depth: Int = 10): Boolean =
      t != null && depth > 0 &&
      (mentionsTerms(t.getMessage) ||
      t.getSuppressed.exists(x => throwableMentionsTerms(x, depth - 1)) ||
      throwableMentionsTerms(t.getCause, depth - 1))
    if (mentionsTerms(error.message) || error.cause.exists(t => throwableMentionsTerms(t)))
      error.copy(
        // 🔴 The scan is TEXTUAL and this helper now sits on EVERY search path, so the wording has
        // to fit BOTH causes: a plain high-cardinality `GROUP BY` is the commonest way to exceed
        // `search.max_buckets`, and it must not be told it wrote a subquery it did not write.
        message = "This query exceeded an Elasticsearch cardinality bound: a `terms` query may " +
          "carry at most `index.max_terms_count` values (default 65536) and an aggregation at " +
          "most `search.max_buckets` buckets. Narrow the grouping, or the subquery that produced " +
          "the values, raise the setting on the index, or - for a subquery - rewrite the " +
          "statement as a JOIN (executed by the relational engine, " +
          s"softclient4es-arrow-extensions). Elasticsearch said: ${error.message}",
        statusCode = error.statusCode.orElse(Some(400))
      )
    else error
  }

  private def enrichMaxResultWindowError(error: ElasticError): ElasticError = {
    def mentionsWindow(message: String): Boolean =
      message != null &&
      (message.contains("max_result_window") || message.contains("Result window is too large"))
    // The REST high-level clients (ES 6/7) surface the per-shard root cause as SUPPRESSED
    // exceptions on an "all shards failed" wrapper, so the scan walks both chains (bounded —
    // exception graphs can be cyclic in theory).
    def throwableMentionsWindow(t: Throwable, depth: Int = 10): Boolean =
      t != null && depth > 0 &&
      (mentionsWindow(t.getMessage) ||
      t.getSuppressed.exists(s => throwableMentionsWindow(s, depth - 1)) ||
      throwableMentionsWindow(t.getCause, depth - 1))
    if (mentionsWindow(error.message) || error.cause.exists(t => throwableMentionsWindow(t)))
      error.copy(
        message =
          "LIMIT/OFFSET exceeds the index's `index.max_result_window` for a single search: " +
          "lower LIMIT/OFFSET so `offset + limit` fits within the window, raise " +
          "`index.max_result_window` on the index, or drop the LIMIT to page through every row. " +
          s"Elasticsearch said: ${error.message}",
        statusCode = error.statusCode.orElse(Some(400))
      )
    else error
  }

  /** Collect the rows of a row-shaped query through the scroll path.
    *
    * The scroll path pages completely (PIT / search_after) and already handles window enrichment
    * and script fields, so `search` / `searchAsync` route here every row query that
    * [[requiresScrollPaging]] flags: with no LIMIT the stream is unbounded (every matching row,
    * #209); with an explicit LIMIT above the one-shot window (#224) the stream is bounded by
    * `maxDocuments = offset + limit` (enforced by ScrollApi's `.take`) and the first `offset` rows
    * are dropped client-side — scroll contexts reject `from`, and per-page `size` is the scroll
    * batch size, so the statement's LIMIT is stripped before translation. Aggregation-shaped
    * queries must never be routed — their result is the aggregation itself, already bounded by an
    * explicit `terms` size.
    */
  private[client] def scrollRows(
    scrollApi: ScrollApi,
    single: SingleSearch,
    elasticQuery: ElasticQuery
  )(implicit context: ConversionContext): Future[ElasticResult[ElasticResponse]] = {
    implicit val system: ActorSystem = SearchApi.scrollRoutingSystem
    implicit val ec: ExecutionContext = system.dispatcher
    val sql = elasticQuery.sql.orElse(Option(single.sql))
    val offset = single.limit.flatMap(_.offset).map(_.offset.toLong).getOrElse(0L)
    val maxDocuments = single.limit.map(l => offset + l.limit.toLong)
    val statement = if (single.limit.isEmpty) single else single.copy(limit = None)
    logger.info(
      s"▶ Row query ${maxDocuments.fold("without LIMIT")(max => s"with LIMIT window $max above ${SearchApi.DefaultMaxResultWindow}")} — routing through scroll for row completeness:\n${sql
        .getOrElse(elasticQuery.query)}"
    )
    // #238 — an explicit LIMIT keeps the sequential PIT path (the `.drop(offset)` below needs a
    // deterministic `_doc` order); the statement's LIMIT was stripped above, so ScrollApi's own
    // clamp cannot see it and the clamp must be applied here. The no-LIMIT branch inherits the
    // configured ceiling (`maxSlices = None`).
    scrollApi
      .scroll(
        statement,
        scrollApi.defaultScrollConfig.copy(
          maxDocuments = maxDocuments,
          maxSlices = if (single.limit.isDefined) Some(1) else None
        )
      )
      .map(_._1)
      .drop(offset)
      .runWith(Sink.seq)
      .map { rows =>
        logger.info(s"✅ Scroll-routed search returned ${rows.size} rows")
        ElasticResult.success(
          ElasticResponse(
            sql,
            elasticQuery.query,
            rows,
            single.fieldAliases,
            ListMap.empty
          )
        )
      }
      .recover { case t =>
        logger.error(
          s"❌ Scroll-routed search failed for query \n${sql.getOrElse(elasticQuery.query)} -> ${t.getMessage}"
        )
        ElasticResult.failure(
          ElasticError(
            message = s"Scroll-routed search failed: ${t.getMessage}",
            cause = Some(t),
            index = Some(elasticQuery.indices.mkString(",")),
            operation = Some("searchAsync")
          )
        )
      }
  }

}

object SearchApi {

  /** Elasticsearch's default `index.max_result_window`: the ceiling on `from + size` for a one-shot
    * search, identical across ES 6/7/8/9. Row queries whose explicit LIMIT window exceeds it are
    * routed through scroll (#224) — see [[SearchApi.requiresScrollPaging]]. The actual per-index
    * setting is deliberately NOT probed; an index tuned higher just pages, an index tuned lower
    * keeps its (translated) one-shot rejection below this threshold.
    */
  val DefaultMaxResultWindow: Long = 10000L

  /** JVM-shared materializer for [[SearchApi.scrollRows]]. Daemonic so an un-terminated system can
    * never keep the JVM alive — clients don't own it, so no `close()` reaches it.
    */
  private[client] lazy val scrollRoutingSystem: ActorSystem =
    ActorSystem(
      "softclient4es-scroll-routing",
      ConfigFactory.parseString("akka.daemonic = on")
    )
}
