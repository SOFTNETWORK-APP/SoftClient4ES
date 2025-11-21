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

import app.softnetwork.elastic.sql.PainlessContext
import app.softnetwork.elastic.sql.query.{
  Asc,
  Bucket,
  BucketIncludesExcludes,
  Criteria,
  Desc,
  Field,
  MetricSelectorScript,
  NestedElement,
  NestedElements,
  SortOrder
}
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.aggregate._
import com.sksamuel.elastic4s.ElasticApi.{
  avgAgg,
  bucketSelectorAggregation,
  cardinalityAgg,
  maxAgg,
  minAgg,
  nestedAggregation,
  sumAgg,
  termsAgg,
  topHitsAgg,
  valueCountAgg
}
import com.sksamuel.elastic4s.requests.script.Script
import com.sksamuel.elastic4s.requests.searches.aggs.{
  Aggregation,
  CardinalityAggregation,
  ExtendedStatsAggregation,
  FilterAggregation,
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
  agg: Aggregation,
  direction: Option[SortOrder] = None,
  nestedElement: Option[NestedElement] = None
) {
  val nested: Boolean = nestedElement.nonEmpty
  val filtered: Boolean = filteredAgg.nonEmpty

  // CHECK if it is a "global" metric (cardinality, etc.) or a bucket metric (avg, sum, etc.)
  val isGlobalMetric: Boolean = agg match {
    case _: CardinalityAggregation   => true
    case _: StatsAggregation         => true
    case _: ExtendedStatsAggregation => true
    case _                           => false
  }
}

object ElasticAggregation {
  def apply(
    sqlAgg: Field,
    having: Option[Criteria],
    bucketsDirection: Map[String, SortOrder]
  ): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.path

    val direction = bucketsDirection.get(identifier.identifierName)

    val field = fieldAlias match {
      case Some(alias) => alias.alias
      case _           => sourceField
    }

    val distinct = identifier.distinct

    val aggType = aggregateFunction.getOrElse(
      throw new IllegalArgumentException("Aggregation function is required")
    )

    val aggName = {
      if (fieldAlias.isDefined)
        field
      else if (distinct)
        s"${aggType}_distinct_${sourceField.replace(".", "_")}"
      else {
        aggType match {
          case th: TopHitsAggregation =>
            s"${th.topHits.sql.toLowerCase}_${sourceField.replace(".", "_")}"
          case _ =>
            s"${aggType}_${sourceField.replace(".", "_")}"

        }
      }
    }

    var aggPath = Seq[String]()

    val (aggFuncs, transformFuncs) = FunctionUtils.aggregateAndTransformFunctions(identifier)

    require(aggFuncs.size == 1, s"Multiple aggregate functions not supported: $aggFuncs")

    def aggWithFieldOrScript(
      buildField: (String, String) => Aggregation,
      buildScript: (String, Script) => Aggregation
    ): Aggregation = {
      if (transformFuncs.nonEmpty) {
        val context = PainlessContext()
        val scriptSrc = identifier.painless(Some(context))
        val script = Script(s"$context$scriptSrc").lang("painless")
        buildScript(aggName, script)
      } else {
        buildField(aggName, sourceField)
      }
    }

    val _agg =
      aggType match {
        case COUNT =>
          if (distinct)
            cardinalityAgg(aggName, sourceField)
          else {
            valueCountAgg(aggName, sourceField)
          }
        case MIN => aggWithFieldOrScript(minAgg, (name, s) => minAgg(name, sourceField).script(s))
        case MAX => aggWithFieldOrScript(maxAgg, (name, s) => maxAgg(name, sourceField).script(s))
        case AVG => aggWithFieldOrScript(avgAgg, (name, s) => avgAgg(name, sourceField).script(s))
        case SUM => aggWithFieldOrScript(sumAgg, (name, s) => sumAgg(name, sourceField).script(s))
        case th: TopHitsAggregation =>
          val limit = {
            th match {
              case _: LastValue => 1
              //                case _: FirstValue => 1
              case _ => th.limit.map(_.limit).getOrElse(1)
            }
          }
          val topHits =
            topHitsAgg(aggName)
              .fetchSource(
                th.identifier.name +: th.fields
                  .filterNot(_.isScriptField)
                  .filterNot(_.sourceField == th.identifier.name)
                  .map(_.sourceField)
                  .distinct
                  .toArray,
                Array.empty
              )
              .copy(
                scripts = th.fields
                  .filter(_.isScriptField)
                  .groupBy(_.sourceField)
                  .map(_._2.head)
                  .map(f => f.sourceField -> Script(f.painless(None)).lang("painless"))
                  .toMap
              )
              .size(limit) sortBy th.orderBy.sorts.map(sort =>
              sort.order match {
                case Some(Desc) =>
                  th.topHits match {
                    case LAST_VALUE => FieldSort(sort.field).asc()
                    case _          => FieldSort(sort.field).desc()
                  }
                case _ =>
                  th.topHits match {
                    case LAST_VALUE => FieldSort(sort.field).desc()
                    case _          => FieldSort(sort.field).asc()
                  }
              }
            )
          /*th.fields.filter(_.isScriptField).foldLeft(topHits) { (agg, f) =>
            agg.script(f.sourceField, Script(f.painless, lang = Some("painless")))
          }*/
          topHits
      }

    val nestedElement = identifier.nestedElement

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

    ElasticAggregation(
      aggPath.mkString("."),
      field,
      sourceField,
      distinct = distinct,
      nestedAgg = nestedAgg,
      aggType = aggType,
      agg = _agg,
      direction = direction,
      nestedElement = nestedElement
    )
  }

  def buildBuckets(
    buckets: Seq[Bucket],
    bucketsDirection: Map[String, SortOrder],
    aggregations: Seq[Aggregation],
    aggregationsDirection: Map[String, SortOrder],
    having: Option[Criteria],
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  ): Option[TermsAggregation] = {
    buckets.reverse.foldLeft(Option.empty[TermsAggregation]) { (current, bucket) =>
      // Determine the bucketPath of the current bucket
      val currentBucketPath = bucket.identifier.path

      var agg = {
        bucketsDirection.get(bucket.identifier.identifierName) match {
          case Some(direction) =>
            termsAgg(bucket.name, currentBucketPath)
              .order(Seq(direction match {
                case Asc => TermsOrder("_key", asc = true)
                case _   => TermsOrder("_key", asc = false)
              }))
          case None =>
            termsAgg(bucket.name, currentBucketPath)
        }
      }
      bucket.size.foreach(s => agg = agg.size(s))
      having match {
        case Some(criteria) =>
          criteria.includes(bucket, not = false, BucketIncludesExcludes()) match {
            case BucketIncludesExcludes(_, Some(regex)) if regex.nonEmpty =>
              agg = agg.includeRegex(regex)
            case BucketIncludesExcludes(values, _) if values.nonEmpty =>
              agg = agg.includeExactValues(values.toArray)
            case _ =>
          }
          criteria.excludes(bucket, not = false, BucketIncludesExcludes()) match {
            case BucketIncludesExcludes(_, Some(regex)) if regex.nonEmpty =>
              agg = agg.excludeRegex(regex)
            case BucketIncludesExcludes(values, _) if values.nonEmpty =>
              agg = agg.excludeExactValues(values.toArray)
            case _ =>
          }
        case _ =>
      }
      current match {
        case Some(subAgg) => Some(agg.copy(subaggs = Seq(subAgg)))
        case None =>
          val aggregationsWithOrder: Seq[TermsOrder] = aggregationsDirection.toSeq.map { kv =>
            kv._2 match {
              case Asc => TermsOrder(kv._1, asc = true)
              case _   => TermsOrder(kv._1, asc = false)
            }
          }
          val withAggregationOrders =
            if (aggregationsWithOrder.nonEmpty)
              agg.order(aggregationsWithOrder)
            else
              agg
          val withHaving = having match {
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
                    Script(script),
                    extractMetricsPathForBucket(
                      criteria,
                      nested,
                      allElasticAggregations
                    )
                  )
                withAggregationOrders.copy(subaggs = aggregations :+ bucketSelector)
              } else {
                withAggregationOrders.copy(subaggs = aggregations)
              }
            case None => withAggregationOrders.copy(subaggs = aggregations)
          }
          Some(withHaving)
      }
    }
  }

  /** Generates the bucket_selector script for a given bucket
    */
  def metricSelectorForBucket(
    criteria: Criteria,
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  ): String = {

    val currentBucketPath = nested.map(_.bucketPath).getOrElse("")

    // No filtering
    val fullScript = MetricSelectorScript
      .metricSelector(criteria)
      .replaceAll("1 == 1 &&", "")
      .replaceAll("&& 1 == 1", "")
      .replaceAll("1 == 1", "")
      .trim

    //    println(s"[DEBUG] currentBucketPath = $currentBucketPath")
    //    println(s"[DEBUG] fullScript (complete) = $fullScript")

    if (fullScript.isEmpty) {
      return ""
    }

    // Parse the script to extract the conditions
    val conditions = parseConditions(fullScript)
    //    println(s"[DEBUG] conditions = $conditions")

    // Filter based on availability in buckets_path
    val relevantConditions = conditions.filter { condition =>
      val metricNames = extractMetricNames(condition)
      //      println(s"[DEBUG] condition = $condition, metricNames = $metricNames")

      metricNames.forall { metricName =>
        allElasticAggregations.find(agg =>
          agg.aggName == metricName || agg.field == metricName
        ) match {
          case Some(elasticAgg) =>
            val metricBucketPath = elasticAgg.nestedElement
              .map(_.bucketPath)
              .getOrElse("")

            //            println(
            //              s"[DEBUG] metricName = $metricName, metricBucketPath = $metricBucketPath, aggType = ${elasticAgg.agg.getClass.getSimpleName}"
            //            )

            val belongsToLevel = metricBucketPath == currentBucketPath

            val isDirectChildAndAccessible =
              if (isDirectChild(metricBucketPath, currentBucketPath)) {
                // Check if it's a "global" metric (cardinality, etc.)
                elasticAgg.isGlobalMetric
              } else {
                false
              }

            val result = belongsToLevel || isDirectChildAndAccessible

            //            println(
            //              s"[DEBUG] belongsToLevel = $belongsToLevel, isDirectChildAndAccessible = $isDirectChildAndAccessible, result = $result"
            //            )
            result

          case None =>
            //            println(s"[DEBUG] metricName = $metricName NOT FOUND")
            currentBucketPath.isEmpty
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
    pattern.findAllMatchIn(condition).map(_.group(1)).toSeq
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

  /** Extracts the buckets_path for a given bucket
    */
  def extractMetricsPathForBucket(
    criteria: Criteria,
    nested: Option[NestedElement],
    allElasticAggregations: Seq[ElasticAggregation]
  ): Map[String, String] = {

    val currentBucketPath = nested.map(_.bucketPath).getOrElse("")

    // Extract ALL metrics paths
    val allMetricsPaths = criteria.extractAllMetricsPath

    //    println(s"[DEBUG extractMetricsPath] currentBucketPath = $currentBucketPath")
    //    println(s"[DEBUG extractMetricsPath] allMetricsPaths = $allMetricsPaths")

    // Filter and adapt the paths for this bucket
    val result = allMetricsPaths.flatMap { case (metricName, metricPath) =>
      allElasticAggregations.find(agg =>
        agg.aggName == metricName || agg.field == metricName
      ) match {
        case Some(elasticAgg) =>
          val metricBucketPath = elasticAgg.nestedElement
            .map(_.bucketPath)
            .getOrElse("")

          //          println(
          //            s"[DEBUG extractMetricsPath] metricName = $metricName, metricBucketPath = $metricBucketPath, aggType = ${elasticAgg.agg.getClass.getSimpleName}"
          //          )

          if (metricBucketPath == currentBucketPath) {
            // Metric of the same level
            //            println(s"[DEBUG extractMetricsPath] Same level: $metricName -> $metricName")
            Some(metricName -> metricName)

          } else if (isDirectChild(metricBucketPath, currentBucketPath)) {
            // Metric of a direct child

            // CHECK if it is a "global" metric (cardinality, etc.) or a bucket metric (avg, sum, etc.)
            val isGlobalMetric = elasticAgg.isGlobalMetric

            if (isGlobalMetric) {
              // Global metric: can be referenced from the parent
              val childNestedName = elasticAgg.nestedElement
                .map(_.innerHitsName)
                .getOrElse("")
              //              println(
              //                s"[DEBUG extractMetricsPath] Direct child (global metric): $metricName -> $childNestedName>$metricName"
              //              )
              Some(metricName -> s"$childNestedName>$metricName")
            } else {
              // Bucket metric: cannot be referenced from the parent
              //              println(
              //                s"[DEBUG extractMetricsPath] Direct child (bucket metric): $metricName -> SKIP (bucket-level metric)"
              //              )
              None
            }

          } else {
            // A different level of metric
            //            println(s"[DEBUG extractMetricsPath] Other level: $metricName -> SKIP")
            None
          }

        case None =>
          //          println(s"[DEBUG extractMetricsPath] Not found: $metricName -> SKIP")
          None
      }
    }

    //    println(s"[DEBUG extractMetricsPath] result = $result")
    result
  }
}
