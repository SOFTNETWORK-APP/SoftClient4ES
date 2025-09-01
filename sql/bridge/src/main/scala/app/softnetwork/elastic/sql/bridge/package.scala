package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.requests.searches.aggs.Aggregation
import com.sksamuel.elastic4s.requests.searches.queries.Query
import com.sksamuel.elastic4s.requests.searches.sort.FieldSort
import com.sksamuel.elastic4s.requests.searches.{
  MultiSearchRequest,
  SearchBodyBuilderFn,
  SearchRequest
}

import scala.language.implicitConversions

package object bridge {
  implicit def requestToElasticSearchRequest(request: SQLSearchRequest): ElasticSearchRequest =
    ElasticSearchRequest(
      request.select.fields,
      request.select.except,
      request.sources,
      request.where.flatMap(_.criteria),
      request.limit.map(_.limit),
      request,
      request.buckets,
      request.aggregates.map(ElasticAggregation(_, request.having.flatMap(_.criteria)))
    ).minScore(request.score)

  implicit def requestToSearchRequest(request: SQLSearchRequest): SearchRequest = {
    import request._
    val notNestedBuckets = buckets.filterNot(_.identifier.nested)
    val nestedBuckets = buckets.filter(_.identifier.nested).groupBy(_.nestedBucket.getOrElse(""))
    val aggregations = aggregates.map(ElasticAggregation(_, request.having.flatMap(_.criteria)))
    val notNestedAggregations = aggregations.filterNot(_.nested)
    val nestedAggregations =
      aggregations.filter(_.nested).groupBy(_.nestedAgg.map(_.name).getOrElse(""))
    var _search: SearchRequest = search("") query {
      where.flatMap(_.criteria.map(_.asQuery())).getOrElse(matchAllQuery())
    } sourceInclude fields

    _search = excludes match {
      case Nil      => _search
      case excludes => _search sourceExclude excludes
    }

    _search = if (nestedAggregations.nonEmpty) {
      _search aggregations {
        nestedAggregations.map { case (nested, aggs) =>
          val first = aggs.head
          val aggregations = aggs.map(_.agg)
          val buckets = ElasticAggregation.buildBuckets(
            nestedBuckets.getOrElse(nested, Seq.empty),
            aggregations
          ) match {
            case Some(b) => Seq(b)
            case _       => aggregations
          }
          val filtered: Option[Aggregation] =
            first.filteredAgg.map(filtered => filtered.subAggregations(buckets))
          first.nestedAgg.get.subAggregations(filtered.map(Seq(_)).getOrElse(buckets))
        }
      }
    } else {
      _search
    }

    _search = notNestedAggregations match {
      case Nil => _search
      case _   => _search aggregations {
        val first = notNestedAggregations.head
        val aggregations = notNestedAggregations.map(_.agg)
        val buckets = ElasticAggregation.buildBuckets(
          notNestedBuckets,
          aggregations
        ) match {
          case Some(b) => Seq(b)
          case _       => aggregations
        }
        val filtered: Option[Aggregation] =
          first.filteredAgg.map(filtered => filtered.subAggregations(buckets))
        filtered.map(Seq(_)).getOrElse(buckets)
      }
    }

    _search = orderBy match {
      case Some(o) =>
        _search sortBy o.sorts.map(sort =>
          sort.order match {
            case Some(Desc) => FieldSort(sort.field).desc()
            case _          => FieldSort(sort.field).asc()
          }
        )
      case _ => _search
    }

    if (aggregations.nonEmpty && fields.isEmpty) {
      _search size 0
    } else {
      limit match {
        case Some(l) => _search limit l.limit from 0
        case _       => _search
      }
    }
  }

  implicit def requestToMultiSearchRequest(
                                            request: SQLMultiSearchRequest
                                          ): MultiSearchRequest = {
    MultiSearchRequest(
      request.requests.map(implicitly[SearchRequest](_))
    )
  }

  implicit def expressionToQuery(expression: SQLExpression): Query = {
    import expression._
    value match {
      case n: SQLNumeric[Any] @unchecked =>
        operator match {
          case _: Ge.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lt n.sql
              case _ =>
                rangeQuery(identifier.name) gte n.sql
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lte n.sql
              case _ =>
                rangeQuery(identifier.name) gt n.sql
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gt n.sql
              case _ =>
                rangeQuery(identifier.name) lte n.sql
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gte n.sql
              case _ =>
                rangeQuery(identifier.name) lt n.sql
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, n.sql))
              case _ =>
                termQuery(identifier.name, n.sql)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, n.sql)
              case _ =>
                not(termQuery(identifier.name, n.sql))
            }
          case _ => matchAllQuery()
        }
      case l: SQLLiteral =>
        operator match {
          case _: Like.type =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.name, toRegex(l.value)))
              case _ =>
                regexQuery(identifier.name, toRegex(l.value))
            }
          case _: Ge.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lt l.value
              case _ =>
                rangeQuery(identifier.name) gte l.value
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lte l.value
              case _ =>
                rangeQuery(identifier.name) gt l.value
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gt l.value
              case _ =>
                rangeQuery(identifier.name) lte l.value
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gte l.value
              case _ =>
                rangeQuery(identifier.name) lt l.value
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, l.value))
              case _ =>
                termQuery(identifier.name, l.value)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, l.value)
              case _ =>
                not(termQuery(identifier.name, l.value))
            }
          case _ => matchAllQuery()
        }
      case b: SQLBoolean =>
        operator match {
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, b.value))
              case _ =>
                termQuery(identifier.name, b.value)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, b.value)
              case _ =>
                not(termQuery(identifier.name, b.value))
            }
          case _ => matchAllQuery()
        }
      case _ => matchAllQuery()
    }
  }

  implicit def isNullToQuery(
                              isNull: SQLIsNull
                            ): Query = {
    import isNull._
    not(existsQuery(identifier.name))
  }

  implicit def isNotNullToQuery(
                                 isNotNull: SQLIsNotNull
                               ): Query = {
    import isNotNull._
    existsQuery(identifier.name)
  }

  implicit def inToQuery[R, T <: SQLValue[R]](in: SQLIn[R, T]): Query = {
    import in._
    val _values: Seq[Any] = values.innerValues
    val t =
      _values.headOption match {
        case Some(_: Double) =>
          termsQuery(identifier.name, _values.asInstanceOf[Seq[Double]])
        case Some(_: Integer) =>
          termsQuery(identifier.name, _values.asInstanceOf[Seq[Integer]])
        case Some(_: Long) =>
          termsQuery(identifier.name, _values.asInstanceOf[Seq[Long]])
        case _ => termsQuery(identifier.name, _values.map(_.toString))
      }
    maybeNot match {
      case Some(_) => not(t)
      case _       => t
    }
  }

  implicit def betweenToQuery(
                               between: SQLBetween
                             ): Query = {
    import between._
    val r = rangeQuery(identifier.name) gte from.value lte to.value
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }

  implicit def geoDistanceToQuery(
                                   geoDistance: ElasticGeoDistance
                                 ): Query = {
    import geoDistance._
    geoDistanceQuery(identifier.name, lat.value, lon.value) distance distance.value
  }

  implicit def matchToQuery(
                             matchExpression: ElasticMatch
                           ): Query = {
    import matchExpression._
    matchQuery(identifier.name, value.value)
  }

  implicit def criteriaToElasticCriteria(
                                          criteria: SQLCriteria
                                        ): ElasticCriteria = {
    ElasticCriteria(
      criteria
    )
  }

  implicit def filterToQuery(
                              filter: ElasticFilter
                            ): ElasticQuery = {
    ElasticQuery(filter)
  }

  implicit def sqlQueryToAggregations(
                                       query: SQLQuery
                                     ): Seq[ElasticAggregation] = {
    import query._
    request
      .map {
        case Left(l) =>
          l.aggregates
            .map(ElasticAggregation(_, l.having.flatMap(_.criteria)))
            .map(aggregation => {
              val queryFiltered =
                l.where
                  .flatMap(_.criteria.map(ElasticCriteria(_).asQuery()))
                  .getOrElse(matchAllQuery())

              aggregation.copy(
                sources = l.sources,
                query = Some(
                  (aggregation.aggType match {
                    case Count if aggregation.sourceField.equalsIgnoreCase("_id") =>
                      SearchBodyBuilderFn(
                        ElasticApi.search("") query {
                          queryFiltered
                        }
                      )
                    case _ =>
                      SearchBodyBuilderFn(
                        ElasticApi.search("") query {
                          queryFiltered
                        }
                          aggregations {
                          val filtered =
                            aggregation.filteredAgg match {
                              case Some(filtered) => filtered.subAggregations(aggregation.agg)
                              case _              => aggregation.agg
                            }
                          aggregation.nestedAgg match {
                            case Some(nested) => nested.subAggregations(filtered)
                            case _            => filtered
                          }
                        }
                          size 0
                      )
                  }).string.replace("\"version\":true,", "") /*FIXME*/
                )
              )
            })

        case _ => Seq.empty

      }
      .getOrElse(Seq.empty)
  }
}
