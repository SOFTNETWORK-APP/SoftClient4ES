package app.softnetwork.elastic.client.sql.bridge

import app.softnetwork.elastic.sql.{
  AggregateFunction,
  Avg,
  Count,
  ElasticBoolQuery,
  Max,
  Min,
  SQLAggregate,
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
  valueCountAgg
}
import com.sksamuel.elastic4s.requests.searches.aggs.Aggregation

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
  def apply(sqlAgg: SQLAggregate): ElasticAggregation = {
    import sqlAgg._
    val sourceField = identifier.columnName

    val field = alias match {
      case Some(alias) => alias.alias
      case _           => sourceField
    }

    val distinct = identifier.distinct.isDefined

    val agg =
      if (distinct)
        s"${function}_distinct_${sourceField.replace(".", "_")}"
      else
        s"${function}_${sourceField.replace(".", "_")}"

    var aggPath = Seq[String]()

    val _agg =
      function match {
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
          f.criteria
            .map(
              _.asFilter(boolQuery)
                .query(Set(identifier.innerHitsName).flatten, boolQuery)
            )
            .getOrElse(matchAllQuery())
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
      aggType = function,
      agg = aggregation
    )
  }
}
