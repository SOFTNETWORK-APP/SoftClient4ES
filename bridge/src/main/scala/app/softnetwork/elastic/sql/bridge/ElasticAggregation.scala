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

package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import app.softnetwork.elastic.sql.`type`.SQLTemporal
import app.softnetwork.elastic.sql.query.{
  Asc,
  BucketIncludesExcludes,
  BucketNode,
  Criteria,
  Desc,
  Field,
  MetricSelectorScript,
  NestedElement,
  NestedElements,
  SQLAggregation,
  SortOrder
}
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.function.time.DateTrunc
import app.softnetwork.elastic.sql.time.TimeUnit
import com.sksamuel.elastic4s.ElasticApi.{
  avgAgg,
  bucketScriptAggregation,
  bucketSelectorAggregation,
  cardinalityAgg,
  extendedStatsAgg,
  maxAgg,
  minAgg,
  nestedAggregation,
  percentilesAgg,
  sumAgg,
  termsAgg,
  topHitsAgg,
  valueCountAgg
}
import com.sksamuel.elastic4s.requests.script.Script
import com.sksamuel.elastic4s.requests.searches.DateHistogramInterval
import com.sksamuel.elastic4s.requests.searches.aggs.{
  AbstractAggregation,
  Aggregation,
  CardinalityAggregation,
  DateHistogramAggregation,
  ExtendedStatsAggregation,
  FilterAggregation,
  HistogramOrder,
  NestedAggregation,
  StatsAggregation,
  TermsAggregation,
  TermsOrder
}
import com.sksamuel.elastic4s.requests.searches.sort.FieldSort

import scala.language.implicitConversions

case class ElasticAggregation(
  aggName: String,
  field: String,
  sourceField: String,
  sources: Seq[String] = Seq.empty,
  query: Option[String] = None,
  distinct: Boolean = false,
  nestedAgg: Option[NestedAggregation] = None,
  filteredAgg: Option[FilterAggregation] = None,
  aggType: AggregateFunction,
  agg: AbstractAggregation,
  direction: Option[SortOrder] = None,
  nestedElement: Option[NestedElement] = None,
  bucketPath: String = ""
) {
  val nested: Boolean = nestedElement.nonEmpty
  val filtered: Boolean = filteredAgg.nonEmpty

  // CHECK if it is a "global" metric (cardinality, etc.) or a bucket metric (avg, sum, etc.)
  val isGlobalMetric: Boolean = agg match {
    case _: CardinalityAggregation           => true
    case _: StatsAggregation                 => true
    case _: ExtendedStatsAggregation         => true
    case _: ScriptedExtendedStatsAggregation => true
    case _                                   => false
  }

  /** True when this aggregation is `STDDEV` / `VARIANCE` (any of the `extended_stats` family) over
    * a TRANSFORMED expression -- the shape a client module must either render with its script or
    * refuse (issue #222).
    *
    * Nothing in the emission path consults this: on both doors the decision is taken by the
    * injected [[SearchBodySerializer]], off the `SearchRequest` it is handed. This is the
    * per-aggregation view of the same predicate, for a caller holding an [[ElasticAggregation]] --
    * the client-module tests assert it on the `sqlQueryToAggregations` door.
    */
  def hasTransformExtendedStats: Boolean = ScriptedExtendedStatsAggregation.existsIn(Seq(agg))
}

/** The terms `order` a bucket asks for, looked up under BOTH spellings a sort can be keyed by: the
  * bucket's resolved column (`identifier.identifierName`) and its OUTPUT name (`Bucket.name`, i.e.
  * the SELECT alias when there is one, else the path).
  *
  * The metric path (`ElasticAggregation.apply`) has had this fallback all along; the BUCKET path
  * had only the first lookup, so a sort naming the SELECT alias of a grouped column silently
  * produced NO `order` at all: `SELECT country AS pays FROM t GROUP BY country ORDER BY pays` came
  * back in an arbitrary doc_count order with HTTP 200, and with a LIMIT that is a different SET of
  * groups.
  *
  * ⚠️ TWO spellings, not three. A third `.orElse` on `identifier.fieldAlias` would be unreachable:
  * `Bucket.name` IS `identifier.fieldAlias.getOrElse(path)`, so whenever an alias exists the second
  * lookup already used that key. (The metric path's three arms are NOT redundant -- they read two
  * different members -- so the shape must not be copied blindly from it.)
  */
private[bridge] object BucketOrder {
  def apply(
    bucketsDirection: Map[String, SortOrder],
    bucket: app.softnetwork.elastic.sql.query.Bucket
  ): Option[SortOrder] =
    bucketsDirection
      .get(bucket.identifier.identifierName)
      .orElse(bucketsDirection.get(bucket.name))
}

object ElasticAggregation {
  def apply(
    sqlAgg: Field,
    having: Option[Criteria],
    bucketsDirection: Map[String, SortOrder],
    allAggregations: Map[String, SQLAggregation]
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType
  ): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.path

    val direction =
      bucketsDirection
        .get(identifier.identifierName)
        .orElse(bucketsDirection.get(identifier.aliasOrName))
        .orElse(fieldAlias.flatMap(a => bucketsDirection.get(a.alias)))

    val field = fieldAlias match {
      case Some(alias) => alias.alias
      case _           => sourceField
    }

    val distinct = identifier.distinct

    var aggType = {
      if (isBucketScript) {
        BucketScriptAggregation(identifier)
      } else
        aggregateFunction.getOrElse(
          throw new IllegalArgumentException("Aggregation function is required")
        )
    }

    val aggName = {
      if (fieldAlias.isDefined)
        field
      else {
        aggType match {
          case COUNT if sourceField == "*" =>
            if (distinct) "count_distinct_all" else "count_all"
          case _ if distinct =>
            s"${aggType}_distinct_${sourceField.replace(".", "_")}"
          case th: WindowFunction =>
            s"${th.window.sql.toLowerCase}_${sourceField.replace(".", "_")}"
          case _ =>
            s"${aggType}_${sourceField.replace(".", "_")}"
        }
      }
    }

    var aggPath = Seq[String]()

    val (aggFuncs, transformFuncs) = FunctionUtils.aggregateAndTransformFunctions(identifier)

    if (!isBucketScript)
      require(aggFuncs.size == 1, s"Multiple aggregate functions not supported: $aggFuncs")

    def aggWithFieldOrScript(
      buildField: (String, String) => Aggregation,
      buildScript: (String, Script) => Aggregation
    ): Aggregation = {
      if (transformFuncs.nonEmpty) {
        val context = PainlessContext(context = contextType)
        val scriptSrc = identifier.painless(Some(context))
        val script = now(Script(s"$context$scriptSrc").lang("painless"))
        buildScript(aggName, script)
      } else {
        aggType match {
          case th: WindowFunction if th.shouldBeScripted =>
            val context = PainlessContext(context = contextType)
            val scriptSrc = th.identifier.painless(Some(context))
            val script = now(Script(s"$context$scriptSrc").lang("painless"))
            buildScript(aggName, script)
          case _ => buildField(aggName, sourceField)
        }
      }
    }

    val _agg =
      aggType match {
        case COUNT =>
          val field =
            sourceField match {
              case "*" | "_id" | "_index" | "_type" => "_index"
              case _                                => sourceField
            }
          if (distinct)
            cardinalityAgg(aggName, field)
          else {
            valueCountAgg(aggName, field)
          }
        case MIN => aggWithFieldOrScript(minAgg, (name, s) => minAgg(name, sourceField).script(s))
        case MAX => aggWithFieldOrScript(maxAgg, (name, s) => maxAgg(name, sourceField).script(s))
        case AVG => aggWithFieldOrScript(avgAgg, (name, s) => avgAgg(name, sourceField).script(s))
        case SUM => aggWithFieldOrScript(sumAgg, (name, s) => sumAgg(name, sourceField).script(s))
        case STDDEV | STDDEV_SAMP | STDDEV_POP | VARIANCE | VAR_SAMP | VAR_POP =>
          aggWithFieldOrScript(
            extendedStatsAgg,
            // Issue #222 -- a transform-bearing extended_stats is bound to the bridge's own marker:
            // elastic4s's ExtendedStatsAggregationBuilder drops `script`, so the library type
            // would serialise as the statistic of the raw field. See ScriptedExtendedStatsAggregation.
            (name, s) =>
              ScriptedExtendedStatsAggregation(extendedStatsAgg(name, sourceField).script(s))
          )
        case th: WindowFunction =>
          th.window match {
            case COUNT =>
              val field =
                sourceField match {
                  case "*" | "_id" | "_index" | "_type" => "_index"
                  case _                                => sourceField
                }
              if (distinct)
                cardinalityAgg(aggName, field)
              else {
                valueCountAgg(aggName, field)
              }
            case MIN =>
              aggWithFieldOrScript(minAgg, (name, s) => minAgg(name, sourceField).script(s))
            case MAX =>
              aggWithFieldOrScript(maxAgg, (name, s) => maxAgg(name, sourceField).script(s))
            case AVG =>
              aggWithFieldOrScript(avgAgg, (name, s) => avgAgg(name, sourceField).script(s))
            case SUM =>
              aggWithFieldOrScript(sumAgg, (name, s) => sumAgg(name, sourceField).script(s))
            case STDDEV | STDDEV_SAMP | STDDEV_POP | VARIANCE | VAR_SAMP | VAR_POP =>
              aggWithFieldOrScript(
                extendedStatsAgg,
                // Issue #222 -- a transform-bearing extended_stats is bound to the bridge's own marker:
                // elastic4s's ExtendedStatsAggregationBuilder drops `script`, so the library type
                // would serialise as the statistic of the raw field. See ScriptedExtendedStatsAggregation.
                (name, s) =>
                  ScriptedExtendedStatsAggregation(extendedStatsAgg(name, sourceField).script(s))
              )
            case PERCENTILE_CONT | PERCENTILE_DISC =>
              // Both map to ES `percentiles` (TDigest). One call → one percent;
              // the requested value column is `sourceField` (PercentileAgg.identifier).
              val pct: Seq[Double] = th match {
                case p: PercentileAgg => Seq(p.percent)
                case _                => Seq.empty
              }
              aggWithFieldOrScript(
                (name, field) => percentilesAgg(name, field).percents(pct),
                (name, s) => percentilesAgg(name, sourceField).percents(pct).script(s)
              )
            case _ =>
              val isRanking = th.isInstanceOf[RankingWindow]
              val limit = {
                th match {
                  case _: LastValue | _: FirstValue => Some(1)
                  // Ranking: top_hits.size driven by the AST's `limit`,
                  // populated by the inline `LIMIT N` inside OVER (the shipped
                  // top-N push-down syntax). When absent, default to ES
                  // `index.max_inner_result_window` (100); push the desired N
                  // via `LIMIT N` inside OVER for larger partitions. A
                  // non-positive LIMIT is meaningless for top-N, so it falls
                  // back to the default cap rather than emitting size:0.
                  case _: RankingWindow =>
                    Some(th.limit.map(_.limit).filter(_ > 0).getOrElse(100))
                  case _ => th.limit.map(_.limit)
                }
              }
              // Ranking emits fetchSource = only the ORDER BY columns (used
              // by the in-memory ordinal assigner to detect ties); `_id`
              // comes back automatically as hit metadata. The aggregation
              // window (LAST_VALUE / FIRST_VALUE / ARRAY_AGG) keeps the
              // existing identifier-name-based fetchSource.
              val fetchSourceCols: Array[String] =
                if (isRanking) {
                  th.orderBy.toSeq
                    .flatMap(_.sorts.map(_.field.name))
                    .distinct
                    .toArray
                } else {
                  (th.identifier.name +: th.fields
                    .filterNot(_.isScriptField)
                    .filterNot(_.sourceField == th.identifier.name)
                    .map(_.sourceField)
                    .distinct).toArray
                }
              val topHits =
                topHitsAgg(aggName)
                  .fetchSource(fetchSourceCols, Array.empty)
                  .copy(
                    scripts = th.fields
                      .filter(_.isScriptField)
                      .groupBy(_.sourceField)
                      .map(_._2.head)
                      .map(f => f.sourceField -> now(Script(f.painless(None)).lang("painless")))
                      .toMap,
                    size = limit,
                    sorts = th.orderBy
                      .map(
                        _.sorts.map(sort =>
                          sort.order match {
                            case Some(Desc) =>
                              th.window match {
                                case LAST_VALUE => FieldSort(sort.field.name).asc()
                                case _          => FieldSort(sort.field.name).desc()
                              }
                            case _ =>
                              th.window match {
                                case LAST_VALUE => FieldSort(sort.field.name).desc()
                                case _          => FieldSort(sort.field.name).asc()
                              }
                          }
                        )
                      )
                      .getOrElse(Seq.empty)
                  )
              topHits
          }
        case script: BucketScriptAggregation =>
          val params = allAggregations.get(aggName) match {
            case Some(sqlAgg) =>
              sqlAgg.aggType match {
                case bsa: BucketScriptAggregation =>
                  aggType = bsa
                  extractMetricsPathForBucketScript(bsa, allAggregations.values.toSeq)
                case _ => Map.empty
              }
            case None => Map.empty
          }
          val painless = script.identifier.painless(None)
          bucketScriptAggregation(
            aggName,
            now(Script(s"$painless").lang("painless")),
            params.toMap
          )
        case _ =>
          throw new IllegalArgumentException(s"Unsupported aggregation type: $aggType")
      }

    val nestedElement = sqlAgg.nestedElement

    val nestedElements: Seq[NestedElement] =
      nestedElement.map(n => NestedElements.buildNestedTrees(Seq(n))).getOrElse(Nil)

    val nestedAgg =
      nestedElements match {
        case Nil =>
          aggPath ++= Seq(aggName)
          None
        case nestedElements =>
          def buildNested(n: NestedElement): NestedAggregation = {
            aggPath ++= Seq(n.innerHitsName)
            val children = n.children
            if (children.nonEmpty) {
              val innerAggs = children.map(buildNested)
              val combinedAgg = if (innerAggs.size == 1) {
                innerAggs.head
              } else {
                innerAggs.reduceLeft { (agg1, agg2) =>
                  agg1.copy(subaggs = agg1.subaggs ++ Seq(agg2))
                }
              }
              nestedAggregation(
                n.innerHitsName,
                n.path
              ) subaggs Seq(combinedAgg)
            } else {
              nestedAggregation(
                n.innerHitsName,
                n.path
              )
            }
          }

          val root = nestedElements.head
          val nestedAgg = buildNested(root) subaggs Seq(_agg)
          having match {
            case Some(_) => aggPath ++= Seq("filtered_agg")
            case _       =>
          }
          aggPath ++= Seq(aggName)
          Some(nestedAgg)
      }

    val bucketPath =
      aggType.bucketPath match {
        case paths if paths.isEmpty => identifier.bucketPath
        case other                  => other
      }

    ElasticAggregation(
      aggPath.mkString("."),
      field,
      sourceField,
      distinct = distinct,
      nestedAgg = nestedAgg,
      aggType = aggType,
      agg = _agg,
      direction = direction,
      nestedElement = nestedElement,
      bucketPath = bucketPath
    )
  }

  def buildBuckets(
    buckets: Seq[Seq[BucketNode]],
    bucketsDirection: Map[String, SortOrder],
    aggs: Seq[ElasticAggregation],
    aggregationsDirection: Map[String, SortOrder],
    having: Option[Criteria],
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): Seq[Aggregation] = {
    for (tree <- buckets) yield {
      val treeNodes =
        tree.sortBy(_.level).reverse.foldLeft(Seq.empty[NodeAggregation]) { (current, node) =>
          val currentBucketPath = node.bucketPath

          val bucket = node.bucket

          val aggregations =
            aggs.filter(agg => agg.bucketPath == currentBucketPath).map(_.agg)

          // Determine the nested path of the current bucket
          val currentBucketNestedPath = bucket.identifier.path

          val aggScript =
            if (!bucket.isBucketScript && bucket.shouldBeScripted) {
              val context = PainlessContext(context = contextType)
              val painless = bucket.painless(Some(context))
              Some(now(Script(s"$context$painless").lang("painless")))
            } else {
              None
            }

          var agg: Aggregation = {
            bucket.out match {
              case _: SQLTemporal =>
                val functions = bucket.identifier.functions
                val interval: Option[DateHistogramInterval] =
                  if (functions.size == 1) {
                    functions.head match {
                      case trunc: DateTrunc =>
                        trunc.unit match {
                          case TimeUnit.YEARS    => Option(DateHistogramInterval.Year)
                          case TimeUnit.QUARTERS => Option(DateHistogramInterval.Quarter)
                          case TimeUnit.MONTHS   => Option(DateHistogramInterval.Month)
                          case TimeUnit.WEEKS    => Option(DateHistogramInterval.Week)
                          case TimeUnit.DAYS     => Option(DateHistogramInterval.Day)
                          case TimeUnit.HOURS    => Option(DateHistogramInterval.Hour)
                          case TimeUnit.MINUTES  => Option(DateHistogramInterval.Minute)
                          case TimeUnit.SECONDS  => Option(DateHistogramInterval.Second)
                          case _                 => None
                        }
                      case _ => None
                    }
                  } else {
                    None
                  }

                aggScript match {
                  case Some(script) =>
                    // Scripted date histogram
                    BucketOrder(bucketsDirection, bucket) match {
                      case Some(direction) =>
                        DateHistogramAggregation(bucket.name, calendarInterval = interval)
                          .script(script)
                          .minDocCount(1)
                          .order(direction match {
                            case Asc => HistogramOrder("_key", asc = true)
                            case _   => HistogramOrder("_key", asc = false)
                          })
                      case _ =>
                        DateHistogramAggregation(bucket.name, calendarInterval = interval)
                          .script(script)
                          .minDocCount(1)
                    }
                  case _ =>
                    // Standard date histogram
                    BucketOrder(bucketsDirection, bucket) match {
                      case Some(direction) =>
                        DateHistogramAggregation(bucket.name, calendarInterval = interval)
                          .field(currentBucketNestedPath)
                          .minDocCount(1)
                          .order(direction match {
                            case Asc => HistogramOrder("_key", asc = true)
                            case _   => HistogramOrder("_key", asc = false)
                          })
                      case _ =>
                        DateHistogramAggregation(bucket.name, calendarInterval = interval)
                          .field(currentBucketNestedPath)
                          .minDocCount(1)
                    }
                }

              case _ =>
                aggScript match {
                  case Some(script) =>
                    // Scripted terms aggregation
                    BucketOrder(bucketsDirection, bucket) match {
                      case Some(direction) =>
                        TermsAggregation(bucket.name)
                          .script(script)
                          .minDocCount(1)
                          .order(Seq(direction match {
                            case Asc => TermsOrder("_key", asc = true)
                            case _   => TermsOrder("_key", asc = false)
                          }))
                      case _ =>
                        TermsAggregation(bucket.name)
                          .script(script)
                          .minDocCount(1)
                    }
                  case _ =>
                    // Standard terms aggregation
                    BucketOrder(bucketsDirection, bucket) match {
                      case Some(direction) =>
                        termsAgg(bucket.name, currentBucketNestedPath)
                          .minDocCount(1)
                          .order(Seq(direction match {
                            case Asc => TermsOrder("_key", asc = true)
                            case _   => TermsOrder("_key", asc = false)
                          }))
                      case _ =>
                        termsAgg(bucket.name, currentBucketNestedPath)
                          .minDocCount(1)
                    }
                }
            }
          }
          agg match {
            case termsAgg: TermsAggregation =>
              // each step must chain on the previous one — restarting from `termsAgg`
              // would silently discard the earlier updates (issue #205)
              var terms = termsAgg
              bucket.size.foreach(s => terms = terms.size(s))
              having match {
                case Some(criteria) =>
                  criteria.includes(bucket, not = false, BucketIncludesExcludes()) match {
                    case BucketIncludesExcludes(_, Some(regex)) if regex.nonEmpty =>
                      terms = terms.includeRegex(regex)
                    case BucketIncludesExcludes(values, _) if values.nonEmpty =>
                      terms = terms.includeExactValues(values.toArray)
                    case _ =>
                  }
                  criteria.excludes(bucket, not = false, BucketIncludesExcludes()) match {
                    case BucketIncludesExcludes(_, Some(regex)) if regex.nonEmpty =>
                      terms = terms.excludeRegex(regex)
                    case BucketIncludesExcludes(values, _) if values.nonEmpty =>
                      terms = terms.excludeExactValues(values.toArray)
                    case _ =>
                  }
                case _ =>
              }
              agg = terms
            case _ =>
          }
          current match {
            case nodes if nodes.nonEmpty =>
              val childNodes =
                nodes.filter(_.node.parentBucketPath.getOrElse("") == node.bucketPath)
              agg match {
                case termsAgg: TermsAggregation =>
                  agg = termsAgg.subaggs(aggregations ++ childNodes.map(_.agg))
                case dateHistogramAgg: DateHistogramAggregation =>
                  agg = dateHistogramAgg.subaggs(aggregations ++ childNodes.map(_.agg))
                case _ =>
              }
              NodeAggregation(node, agg) +: nodes
            case Nil =>
              val subaggs =
                having match {
                  case Some(criteria) =>
                    val script = metricSelectorForBucket(
                      criteria,
                      nested,
                      allElasticAggregations
                    )

                    if (script.nonEmpty) {
                      val bucketSelector =
                        bucketSelectorAggregation(
                          "having_filter",
                          now(Script(script)),
                          extractMetricsPathForBucket(
                            criteria,
                            nested,
                            allElasticAggregations
                          )
                        )
                      aggregations :+ bucketSelector
                    } else {
                      aggregations
                    }
                  case None =>
                    aggregations
                }

              agg match {
                case termsAgg: TermsAggregation =>
                  val aggregationsWithOrder: Seq[TermsOrder] =
                    aggregationsDirection.toSeq.map { kv =>
                      kv._2 match {
                        case Asc => TermsOrder(kv._1, asc = true)
                        case _   => TermsOrder(kv._1, asc = false)
                      }
                    }
                  if (aggregationsWithOrder.nonEmpty)
                    agg = termsAgg.order(aggregationsWithOrder).copy(subaggs = subaggs)
                  else
                    agg = termsAgg.copy(subaggs = subaggs)
                case dateHistogramAggregation: DateHistogramAggregation =>
                  agg = dateHistogramAggregation.copy(subaggs = subaggs)
              }

              Seq(NodeAggregation(node, agg))
          }
        }

      treeNodes.headOption.map(_.agg)

    }
  }.flatten

  /** Generates the bucket_selector script for a given bucket
    */
  def metricSelectorForBucket(
    criteria: Criteria,
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  ): String = {

    val currentNestedPath = nested.map(_.nestedPath).getOrElse("")

    // No filtering
    val fullScript = MetricSelectorScript
      .metricSelector(criteria)
      .replaceAll("1 == 1 &&", "")
      .replaceAll("&& 1 == 1", "")
      .replaceAll("1 == 1", "")
      .trim

    //    println(s"[DEBUG] currentNestedPath = $currentNestedPath")
    //    println(s"[DEBUG] fullScript (complete) = $fullScript")

    if (fullScript.isEmpty) {
      return ""
    }

    // Parse the script to extract the conditions
    val conditions = parseConditions(fullScript)
    //    println(s"[DEBUG] conditions = $conditions")

    // Keep a condition only when every metric it reads is one extractMetricsPathForBucket publishes
    // for this very bucket -- both go through resolveBucketMetric, so the emitted script can never
    // read a `params.<x>` that buckets_path does not declare. A name no aggregation carries is an
    // opaque script parameter (`params.__now__`) at root level and unresolvable inside a nested one.
    val relevantConditions = conditions.filter { condition =>
      extractMetricNames(condition).forall { metricName =>
        resolveBucketMetric(metricName, currentNestedPath, allElasticAggregations) match {
          case Resolved(_) => true
          case Unknown     => currentNestedPath.isEmpty
          case OutOfScope  => false
        }
      }
    }

    //    println(s"[DEBUG] relevantConditions = $relevantConditions")

    if (relevantConditions.isEmpty) {
      ""
    } else {
      relevantConditions.mkString(" && ")
    }
  }

  /** HELPER: Parse the conditions of a script (separated by &&)
    */
  private def parseConditions(script: String): Seq[String] = {
    // Simple parsing : split by " && "
    // ⚠️ This simple implementation does not handle parentheses.
    script.split(" && ").map(_.trim).toSeq
  }

  /** HELPER: Extracts the metric names from a condition Example: "params.ingredient_count >= 3" =>
    * Seq("ingredient_count")
    */
  private def extractMetricNames(condition: String): Seq[String] = {
    // Pattern to extract "params.XXX"
    val pattern = "params\\.([a-zA-Z_][a-zA-Z0-9_]*)".r
    // `params.__now__` is the request clock the bridge binds itself, not a metric: left in, it
    // resolved Unknown and made every nested-level HAVING with `now - interval ...` drop its
    // condition silently (Unknown is only tolerated at root level).
    pattern.findAllMatchIn(condition).map(_.group(1)).filterNot(_ == "__now__").toSeq
  }

  // HELPER: Check if a path is a direct child
  private def isDirectChild(childPath: String, parentPath: String): Boolean = {
    if (parentPath.isEmpty) {
      childPath.nonEmpty && !childPath.contains(">")
    } else {
      childPath.startsWith(parentPath + ">") &&
      childPath.count(_ == '>') == parentPath.count(_ == '>') + 1
    }
  }

  /** How a metric a bucket pipeline reads (`params.<metricName>`) resolves against the aggregations
    * of the bucket being built. ONE resolver feeds both the `bucket_selector` script (which
    * conditions survive) and its `buckets_path` (which names are published), so the two cannot
    * drift.
    */
  private sealed trait MetricResolution

  /** Addressable from this bucket. `path` is the `buckets_path` value: the aggregation's LOCAL name
    * -- `agg.name`, the name the elastic4s aggregation was built with, never the `.`-joined
    * `aggName` a nested aggregation carries (issue #54) -- or `<child nested agg>><local name>` for
    * a global metric of a direct nested child.
    */
  private case class Resolved(path: String) extends MetricResolution

  /** Exists, but at a level this bucket cannot address (a bucket-level metric of a child, or
    * another branch entirely).
    */
  private case object OutOfScope extends MetricResolution

  /** No aggregation carries this name. */
  private case object Unknown extends MetricResolution

  private def resolveBucketMetric(
    metricName: String,
    currentNestedPath: String,
    allElasticAggregations: Seq[ElasticAggregation]
  ): MetricResolution =
    allElasticAggregations.find(agg => agg.aggName == metricName || agg.field == metricName) match {
      case Some(elasticAgg) =>
        val metricBucketPath = elasticAgg.nestedElement.map(_.nestedPath).getOrElse("")
        if (metricBucketPath == currentNestedPath)
          Resolved(elasticAgg.agg.name)
        else if (isDirectChild(metricBucketPath, currentNestedPath) && elasticAgg.isGlobalMetric) {
          val childNestedName = elasticAgg.nestedElement.map(_.innerHitsName).getOrElse("")
          Resolved(s"$childNestedName>${elasticAgg.agg.name}")
        } else
          OutOfScope
      case None => Unknown
    }

  def extractMetricsPathForBucketScript(
    bucketScriptAggregation: BucketScriptAggregation,
    allAggregations: Seq[SQLAggregation]
  ): Map[String, String] = {
    val currentBucketPath =
      bucketScriptAggregation.identifier.nestedElement.map(_.nestedPath).getOrElse("")
    // Extract ALL metrics paths
    val allMetricsPaths = bucketScriptAggregation.params
    val result =
      allMetricsPaths.flatMap { metric =>
        allAggregations.find(agg => agg.aggName == metric._2 || agg.field == metric._2) match {
          case Some(sqlAgg) =>
            val metricBucketPath = sqlAgg.nestedElement
              .map(_.nestedPath)
              .getOrElse("")
            if (metricBucketPath == currentBucketPath) {
              // Metric of the same level
              Some(metric._1 -> metric._2)
            } else if (isDirectChild(metricBucketPath, currentBucketPath)) {
              // Metric of a direct child
              // CHECK if it is a "global" metric (cardinality, etc.) or a bucket metric (avg, sum, etc.)
              val isGlobalMetric = sqlAgg.isGlobalMetric

              if (isGlobalMetric) {
                // Global metric: can be referenced from the parent
                val childNestedName = sqlAgg.nestedElement
                  .map(_.innerHitsName)
                  .getOrElse("")
                //              println(
                //                s"[DEBUG extractMetricsPath] Direct child (global metric): $metricName -> $childNestedName>$metricName"
                //              )
                Some(metric._1 -> s"$childNestedName>${metric._2}")
              } else {
                // Bucket metric: cannot be referenced from the parent
                //              println(
                //                s"[DEBUG extractMetricsPath] Direct child (bucket metric): $metricName -> SKIP (bucket-level metric)"
                //              )
                None
              }
            } else {
              None
            }
          case _ => None
        }
      }
    result.toMap
  }

  /** Extracts the buckets_path for a given bucket
    */
  def extractMetricsPathForBucket(
    criteria: Criteria,
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  ): Map[String, String] = {

    val currentBucketPath = nested.map(_.nestedPath).getOrElse("")

    // Extract ALL metrics paths
    val allMetricsPaths = criteria.extractAllMetricsPath

    //    println(s"[DEBUG extractMetricsPath] currentBucketPath = $currentBucketPath")
    //    println(s"[DEBUG extractMetricsPath] allMetricsPaths = $allMetricsPaths")

    // Publish, under the name the script reads, the path of the aggregation that carries it --
    // resolved by the very rule metricSelectorForBucket filtered the script with.
    allMetricsPaths.flatMap { case (metricName, _) =>
      resolveBucketMetric(metricName, currentBucketPath, allElasticAggregations) match {
        case Resolved(path) => Some(metricName -> path)
        case _              => None
      }
    }
  }
}

private case class NodeAggregation(node: BucketNode, agg: Aggregation)
