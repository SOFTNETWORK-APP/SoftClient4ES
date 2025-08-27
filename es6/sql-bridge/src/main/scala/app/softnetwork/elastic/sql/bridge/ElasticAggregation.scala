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
  maxAgg,
  minAgg,
  nestedAggregation,
  sumAgg,
  termsAgg,
  valueCountAgg
}
import com.sksamuel.elastic4s.searches.aggs.Aggregation

import scala.language.implicitConversions

case class ElasticAggregation(
  aggName: String,
  field: String,
  sourceField: String,
  sources: Seq[String] = Seq.empty,
  query: Option[String] = None,
  distinct: Boolean = false,
  nested: Boolean = false,
  filtered: Boolean = false,
  aggType: AggregateFunction,
  agg: Aggregation
)

object ElasticAggregation {
  def apply(sqlAgg: SQLField, filter: Option[SQLCriteria]): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.columnName

    val field = alias match {
      case Some(alias) => alias.alias
      case _           => sourceField
    }

    val distinct = identifier.distinct

    val agg =
      if (distinct)
        s"${function}_distinct_${sourceField.replace(".", "_")}"
      else
        s"${function}_${sourceField.replace(".", "_")}"

    var aggPath = Seq[String]()

    val aggType = aggregateFunction.getOrElse(
      throw new IllegalArgumentException("Aggregation function is required")
    )

    val _agg =
      aggType match {
        case Count =>
          if (distinct)
            cardinalityAgg(agg, sourceField)
          else {
            valueCountAgg(agg, sourceField)
          }
        case Min => minAgg(agg, sourceField)
        case Max => maxAgg(agg, sourceField)
        case Avg => avgAgg(agg, sourceField)
        case Sum => sumAgg(agg, sourceField)
      }

    def _filtered: Aggregation = filter match {
      case Some(f) =>
        val boolQuery = Option(ElasticBoolQuery(group = true))
        val filteredAgg = s"filtered_agg"
        aggPath ++= Seq(filteredAgg)
        filterAgg(
          filteredAgg,
          f.asFilter(boolQuery)
            .query(Set(identifier.innerHitsName).flatten, boolQuery)
        ) subaggs {
          aggPath ++= Seq(agg)
          _agg
        }
      case _ =>
        aggPath ++= Seq(agg)
        _agg
    }

    val aggregation =
      if (identifier.nested) {
        val path = sourceField.split("\\.").head
        val nestedAgg = s"nested_$agg"
        aggPath ++= Seq(nestedAgg)
        nestedAggregation(nestedAgg, path) subaggs {
          _filtered
        }
      } else {
        _filtered
      }

    ElasticAggregation(
      aggPath.mkString("."),
      field,
      sourceField,
      distinct = distinct,
      nested = identifier.nested,
      filtered = filter.nonEmpty,
      aggType = aggType, // TODO remove aggType by parsing it from agg
      agg = aggregation
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
