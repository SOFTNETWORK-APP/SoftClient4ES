package app.softnetwork.elastic.sql.bridge

import app.softnetwork.elastic.sql.{
  AggregateFunction,
  Avg,
  Count,
  ElasticBoolQuery,
  Max,
  Min,
  SQLBucket,
  SQLCriteria,
  SQLField,
  Sum
}
import com.sksamuel.elastic4s.ElasticApi.{
  avgAgg,
  cardinalityAgg,
  filterAgg,
  matchAllQuery,
  maxAgg,
  minAgg,
  nestedAggregation,
  sumAgg,
  termsAgg,
  valueCountAgg
}
import com.sksamuel.elastic4s.requests.searches.aggs.{Aggregation, NestedAggregation, FilterAggregation}

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
  agg: Aggregation) {
  val nested: Boolean = nestedAgg.nonEmpty
  val filtered: Boolean = filteredAgg.nonEmpty
}

object ElasticAggregation {
  def apply(sqlAgg: SQLField, filter: Option[SQLCriteria]): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.name

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

    val _agg =
      aggType match {
        case Count =>
          if (distinct)
            cardinalityAgg(aggName, sourceField)
          else {
            valueCountAgg(aggName, sourceField)
          }
        case Min => minAgg(aggName, sourceField)
        case Max => maxAgg(aggName, sourceField)
        case Avg => avgAgg(aggName, sourceField)
        case Sum => sumAgg(aggName, sourceField)
      }

    val filteredAggName = "filtered_agg"

    val filteredAgg: Option[FilterAggregation] =
      filter match {
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
      agg = _agg
    )
  }

  def apply(buckets: Seq[SQLBucket], current: Option[Aggregation]): Option[Aggregation] = {
    buckets match {
      case Nil => current
      case bucket +: tail =>
        val agg = termsAgg(bucket.name, bucket.sourceBucket)
        current match {
          case Some(a) =>
            a.addSubagg(agg)
            apply(tail, Some(agg))
          case _ => apply(tail, Some(agg))
        }
    }
  }
}
