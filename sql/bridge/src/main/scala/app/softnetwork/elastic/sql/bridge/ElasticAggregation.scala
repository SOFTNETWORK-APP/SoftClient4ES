package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.PainlessContext
import app.softnetwork.elastic.sql.query.{
  Asc,
  Bucket,
  BucketIncludesExcludes,
  MetricSelectorScript,
  Field,
  Criteria,
  Desc,
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
  FilterAggregation,
  NestedAggregation,
  TermsAggregation,
  TermsOrder,
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
  nestedElement: Option[NestedElement] = None) {
  val nested: Boolean = nestedElement.nonEmpty
  val filtered: Boolean = filteredAgg.nonEmpty
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
                  .map(_.sourceField)
                  .toArray,
                Array.empty
              ).copy(
              scripts = th.fields.filter(_.isScriptField).map(f =>
                f.sourceField -> Script(f.painless(None)).lang("painless")
              ).toMap
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

    val filteredAggName = "filtered_agg"

    def filtered(): Unit =
      having match {
        case Some(_) =>
          aggPath ++= Seq(filteredAggName)
          aggPath ++= Seq(aggName)
        case _ =>
          aggPath ++= Seq(aggName)
      }

    val nestedElement = identifier.nestedElement

    val nestedElements: Seq[NestedElement] =
      nestedElement.map(n => NestedElements.buildNestedTrees(Seq(n))).getOrElse(Nil)

    val nestedAgg =
      nestedElements match {
        case Nil =>
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

          Some(buildNested(nestedElements.head))
      }

    filtered()

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
                    having: Option[Criteria]
  ): Option[TermsAggregation] = {
    buckets.reverse.foldLeft(Option.empty[TermsAggregation]) { (current, bucket) =>
      var agg = {
        bucketsDirection.get(bucket.identifier.identifierName) match {
          case Some(direction) =>
            termsAgg(bucket.name, s"${bucket.identifier.path}.keyword")
              .order(Seq(direction match {
                case Asc => TermsOrder("_key", asc = true)
                case _   => TermsOrder("_key", asc = false)
              }))
          case None =>
            termsAgg(bucket.name, s"${bucket.identifier.path}.keyword")
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
              val script = MetricSelectorScript.metricSelector(criteria)
              val bucketsPath = criteria.extractMetricsPath

              val bucketSelector =
                bucketSelectorAggregation(
                  "having_filter",
                  Script(script.replaceAll("1 == 1 &&", "").replaceAll("&& 1 == 1", "").trim),
                  bucketsPath
                )

              withAggregationOrders.copy(subaggs = aggregations :+ bucketSelector)

            case None => withAggregationOrders.copy(subaggs = aggregations)
          }
          Some(withHaving)
      }
    }
  }
}
