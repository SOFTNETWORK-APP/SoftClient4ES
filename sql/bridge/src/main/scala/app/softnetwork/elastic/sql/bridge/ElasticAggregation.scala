package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{
  AggregateFunction,
  Asc,
  Avg,
  BucketSelectorScript,
  Count,
  ElasticBoolQuery,
  Field,
  Max,
  Min,
  SQLBucket,
  SQLCriteria,
  SQLFunctionUtils,
  SortOrder,
  Sum
}
import com.sksamuel.elastic4s.ElasticApi.{
  avgAgg,
  bucketSelectorAggregation,
  cardinalityAgg,
  filterAgg,
  maxAgg,
  minAgg,
  nestedAggregation,
  sumAgg,
  termsAgg,
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
  direction: Option[SortOrder] = None) {
  val nested: Boolean = nestedAgg.nonEmpty
  val filtered: Boolean = filteredAgg.nonEmpty
}

object ElasticAggregation {
  def apply(
    sqlAgg: Field,
    having: Option[SQLCriteria],
    bucketsDirection: Map[String, SortOrder]
  ): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.name

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
      else
        s"${aggType}_${sourceField.replace(".", "_")}"
    }

    var aggPath = Seq[String]()

    val (aggFuncs, transformFuncs) = SQLFunctionUtils.aggregateAndTransformFunctions(identifier)

    require(aggFuncs.size == 1, s"Multiple aggregate functions not supported: $aggFuncs")

    def aggWithFieldOrScript(
      buildField: (String, String) => Aggregation,
      buildScript: (String, Script) => Aggregation
    ): Aggregation = {
      if (transformFuncs.nonEmpty) {
        val scriptSrc = SQLFunctionUtils.buildPainless(identifier)
        val script = Script(scriptSrc).lang("painless")
        buildScript(aggName, script)
      } else {
        buildField(aggName, sourceField)
      }
    }

    val _agg =
      aggType match {
        case Count =>
          if (distinct)
            cardinalityAgg(aggName, sourceField)
          else {
            valueCountAgg(aggName, sourceField)
          }
        case Min => aggWithFieldOrScript(minAgg, (name, s) => minAgg(name, sourceField).script(s))
        case Max => aggWithFieldOrScript(maxAgg, (name, s) => maxAgg(name, sourceField).script(s))
        case Avg => aggWithFieldOrScript(avgAgg, (name, s) => avgAgg(name, sourceField).script(s))
        case Sum => aggWithFieldOrScript(sumAgg, (name, s) => sumAgg(name, sourceField).script(s))
      }

    val filteredAggName = "filtered_agg"

    val filteredAgg: Option[FilterAggregation] =
      having match {
        case Some(f) =>
          val boolQuery = Option(ElasticBoolQuery(group = true))
          Some(
            filterAgg(
              filteredAggName,
              f.asFilter(boolQuery)
                .query(Set(identifier.innerHitsName).flatten, boolQuery)
            )
          )
        case _ =>
          None
      }

    def filtered(): Unit =
      filteredAgg match {
        case Some(_) =>
          aggPath ++= Seq(filteredAggName)
          aggPath ++= Seq(aggName)
        case _ =>
          aggPath ++= Seq(aggName)
      }

    val nestedAgg =
      if (identifier.nested) {
        val path = sourceField.split("\\.").head
        val nestedAgg = s"nested_${identifier.nestedType.getOrElse(aggName)}"
        aggPath ++= Seq(nestedAgg)
        filtered()
        Some(nestedAggregation(nestedAgg, path))
      } else {
        filtered()
        None
      }

    ElasticAggregation(
      aggPath.mkString("."),
      field,
      sourceField,
      distinct = distinct,
      nestedAgg = nestedAgg,
      filteredAgg = filteredAgg,
      aggType = aggType,
      agg = _agg,
      direction = direction
    )
  }

  def buildBuckets(
    buckets: Seq[SQLBucket],
    bucketsDirection: Map[String, SortOrder],
    aggregations: Seq[Aggregation],
    aggregationsDirection: Map[String, SortOrder],
    having: Option[SQLCriteria]
  ): Option[TermsAggregation] = {
    Console.println(bucketsDirection)
    buckets.reverse.foldLeft(Option.empty[TermsAggregation]) { (current, bucket) =>
      val agg = {
        bucketsDirection.get(bucket.identifier.identifierName) match {
          case Some(direction) =>
            termsAgg(bucket.name, s"${bucket.identifier.name}.keyword")
              .order(Seq(direction match {
                case Asc => TermsOrder(bucket.name, asc = true)
                case _   => TermsOrder(bucket.name, asc = false)
              }))
          case None =>
            termsAgg(bucket.name, s"${bucket.identifier.name}.keyword")
        }
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
              import BucketSelectorScript._
              val script = toPainless(criteria)
              val bucketsPath = extractBucketsPath(criteria)

              val bucketSelector =
                bucketSelectorAggregation("having_filter", Script(script), bucketsPath)

              withAggregationOrders.copy(subaggs = aggregations :+ bucketSelector)

            case None => withAggregationOrders.copy(subaggs = aggregations)
          }
          Some(withHaving)
      }
    }
  }
}
