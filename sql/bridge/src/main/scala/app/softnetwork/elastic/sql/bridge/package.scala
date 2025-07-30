package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.ElasticApi._
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
      request.aggregates.map(ElasticAggregation(_))
    ).minScore(request.score)

  implicit def requestToSearchRequest(request: SQLSearchRequest): SearchRequest = {
    import request._
    val aggregations = aggregates.map(ElasticAggregation(_))
    var _search: SearchRequest = search("") query {
      where.flatMap(_.criteria.map(_.asQuery())).getOrElse(matchAllQuery())
    } sourceInclude fields

    _search = excludes match {
      case Nil      => _search
      case excludes => _search sourceExclude excludes
    }

    _search = aggregations match {
      case Nil => _search
      case _   => _search aggregations { aggregations.map(_.agg) }
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
                rangeQuery(identifier.columnName) lt n.sql
              case _ =>
                rangeQuery(identifier.columnName) gte n.sql
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lte n.sql
              case _ =>
                rangeQuery(identifier.columnName) gt n.sql
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gt n.sql
              case _ =>
                rangeQuery(identifier.columnName) lte n.sql
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gte n.sql
              case _ =>
                rangeQuery(identifier.columnName) lt n.sql
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, n.sql))
              case _ =>
                termQuery(identifier.columnName, n.sql)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.columnName, n.sql)
              case _ =>
                not(termQuery(identifier.columnName, n.sql))
            }
          case _ => matchAllQuery()
        }
      case l: SQLLiteral =>
        operator match {
          case _: Like.type =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.columnName, toRegex(l.value)))
              case _ =>
                regexQuery(identifier.columnName, toRegex(l.value))
            }
          case _: Ge.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lt l.value
              case _ =>
                rangeQuery(identifier.columnName) gte l.value
            }
          case _: Gt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) lte l.value
              case _ =>
                rangeQuery(identifier.columnName) gt l.value
            }
          case _: Le.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gt l.value
              case _ =>
                rangeQuery(identifier.columnName) lte l.value
            }
          case _: Lt.type =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.columnName) gte l.value
              case _ =>
                rangeQuery(identifier.columnName) lt l.value
            }
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, l.value))
              case _ =>
                termQuery(identifier.columnName, l.value)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.columnName, l.value)
              case _ =>
                not(termQuery(identifier.columnName, l.value))
            }
          case _ => matchAllQuery()
        }
      case b: SQLBoolean =>
        operator match {
          case _: Eq.type =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.columnName, b.value))
              case _ =>
                termQuery(identifier.columnName, b.value)
            }
          case _: Ne.type =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.columnName, b.value)
              case _ =>
                not(termQuery(identifier.columnName, b.value))
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
    not(existsQuery(identifier.columnName))
  }

  implicit def isNotNullToQuery(
    isNotNull: SQLIsNotNull
  ): Query = {
    import isNotNull._
    existsQuery(identifier.columnName)
  }

  implicit def inToQuery[R, T <: SQLValue[R]](in: SQLIn[R, T]): Query = {
    import in._
    val _values: Seq[Any] = values.innerValues
    val t =
      _values.headOption match {
        case Some(_: Double) =>
          termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Double]])
        case Some(_: Integer) =>
          termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Integer]])
        case Some(_: Long) =>
          termsQuery(identifier.columnName, _values.asInstanceOf[Seq[Long]])
        case _ => termsQuery(identifier.columnName, _values.map(_.toString))
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
    val r = rangeQuery(identifier.columnName) gte from.value lte to.value
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }

  implicit def geoDistanceToQuery(
    geoDistance: ElasticGeoDistance
  ): Query = {
    import geoDistance._
    geoDistanceQuery(identifier.columnName, lat.value, lon.value) distance distance.value
  }

  implicit def matchToQuery(
    matchExpression: ElasticMatch
  ): Query = {
    import matchExpression._
    matchQuery(identifier.columnName, value.value)
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
            .map(ElasticAggregation(_))
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
                          aggregation.agg
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
