package app.softnetwork.elastic.sql

import com.sksamuel.elastic4s.ElasticApi
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.requests.script.Script
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
      request.aggregates.map(
        ElasticAggregation(_, request.having.flatMap(_.criteria), request.sorts)
      )
    ).minScore(request.score)

  implicit def requestToSearchRequest(request: SQLSearchRequest): SearchRequest = {
    import request._
    val notNestedBuckets = buckets.filterNot(_.identifier.nested)
    val nestedBuckets = buckets.filter(_.identifier.nested).groupBy(_.nestedBucket.getOrElse(""))
    val aggregations =
      aggregates.map(ElasticAggregation(_, request.having.flatMap(_.criteria), request.sorts))
    val notNestedAggregations = aggregations.filterNot(_.nested)
    val nestedAggregations =
      aggregations.filter(_.nested).groupBy(_.nestedAgg.map(_.name).getOrElse(""))
    var _search: SearchRequest = search("") query {
      where.flatMap(_.criteria.map(_.asQuery())).getOrElse(matchAllQuery())
    } sourceFiltering (fields, excludes)

    _search = if (nestedAggregations.nonEmpty) {
      _search aggregations {
        nestedAggregations.map { case (nested, aggs) =>
          val first = aggs.head
          val aggregations = aggs.map(_.agg)
          val aggregationDirections: Map[String, SortOrder] =
            aggs
              .filter(_.direction.isDefined)
              .map(agg => agg.agg.name -> agg.direction.getOrElse(Asc))
              .toMap
          val buckets =
            ElasticAggregation.buildBuckets(
              nestedBuckets.getOrElse(nested, Seq.empty),
              request.sorts -- aggregationDirections.keys,
              aggregations,
              aggregationDirections,
              request.having.flatMap(_.criteria)
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
        val aggregationDirections: Map[String, SortOrder] = notNestedAggregations
          .filter(_.direction.isDefined)
          .map(agg => agg.agg.name -> agg.direction.get)
          .toMap
        val aggregations = notNestedAggregations.map(_.agg)
        val buckets = ElasticAggregation.buildBuckets(
          notNestedBuckets,
          request.sorts -- aggregationDirections.keys,
          aggregations,
          aggregationDirections,
          request.having.flatMap(_.criteria)
        ) match {
          case Some(b) => Seq(b)
          case _       => aggregations
        }
        val filtered: Option[Aggregation] =
          first.filteredAgg.map(filtered => filtered.subAggregations(buckets))
        filtered.map(Seq(_)).getOrElse(buckets)
      }
    }

    _search = scriptFields match {
      case Nil => _search
      case _ =>
        _search scriptfields scriptFields.map { field =>
          scriptField(
            field.name,
            Script(script = field.painless).lang("painless").scriptType("source")
          )
        }
    }

    _search = orderBy match {
      case Some(o) if aggregates.isEmpty && buckets.isEmpty =>
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

  def applyNumericOp[A](n: SQLNumeric[_])(
    longOp: Long => A,
    doubleOp: Double => A
  ): A = n.toEither.fold(longOp, doubleOp)

  implicit def expressionToQuery(expression: SQLExpression): Query = {
    import expression._
    value match {
      case n: SQLNumeric[_] if !aggregation =>
        operator match {
          case Ge =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) lt l,
                  d => rangeQuery(identifier.name) lt d
                )
              case _ =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) gte l,
                  d => rangeQuery(identifier.name) gte d
                )
            }
          case Gt =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) lte l,
                  d => rangeQuery(identifier.name) lte d
                )
              case _ =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) gt l,
                  d => rangeQuery(identifier.name) gt d
                )
            }
          case Le =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) gt l,
                  d => rangeQuery(identifier.name) gt d
                )
              case _ =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) lte l,
                  d => rangeQuery(identifier.name) lte d
                )
            }
          case Lt =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) gte l,
                  d => rangeQuery(identifier.name) gte d
                )
              case _ =>
                applyNumericOp(n)(
                  l => rangeQuery(identifier.name) lt l,
                  d => rangeQuery(identifier.name) lt d
                )
            }
          case Eq =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => not(termQuery(identifier.name, l)),
                  d => not(termQuery(identifier.name, d))
                )
              case _ =>
                applyNumericOp(n)(
                  l => termQuery(identifier.name, l),
                  d => termQuery(identifier.name, d)
                )
            }
          case Ne | Diff =>
            maybeNot match {
              case Some(_) =>
                applyNumericOp(n)(
                  l => termQuery(identifier.name, l),
                  d => termQuery(identifier.name, d)
                )
              case _ =>
                applyNumericOp(n)(
                  l => not(termQuery(identifier.name, l)),
                  d => not(termQuery(identifier.name, d))
                )
            }
          case _ => matchAllQuery()
        }
      case l: SQLLiteral if !aggregation =>
        operator match {
          case Like =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.name, toRegex(l.value)))
              case _ =>
                regexQuery(identifier.name, toRegex(l.value))
            }
          case Ge =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lt l.value
              case _ =>
                rangeQuery(identifier.name) gte l.value
            }
          case Gt =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lte l.value
              case _ =>
                rangeQuery(identifier.name) gt l.value
            }
          case Le =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gt l.value
              case _ =>
                rangeQuery(identifier.name) lte l.value
            }
          case Lt =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gte l.value
              case _ =>
                rangeQuery(identifier.name) lt l.value
            }
          case Eq =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, l.value))
              case _ =>
                termQuery(identifier.name, l.value)
            }
          case Ne | Diff =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, l.value)
              case _ =>
                not(termQuery(identifier.name, l.value))
            }
          case _ => matchAllQuery()
        }
      case b: SQLBoolean if !aggregation =>
        operator match {
          case Eq =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, b.value))
              case _ =>
                termQuery(identifier.name, b.value)
            }
          case Ne | Diff =>
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

  implicit def dateMathToQuery(dateMath: SQLComparisonDateMath): Query = {
    import dateMath._
    if (aggregation)
      return matchAllQuery()
    dateTimeFunction match {
      case _: CurrentTimeFunction =>
        scriptQuery(Script(script = script).lang("painless").scriptType("source"))
      case _ =>
        val op = if (maybeNot.isDefined) operator.not else operator
        op match {
          case Gt => rangeQuery(identifier.name) gt script
          case Ge => rangeQuery(identifier.name) gte script
          case Lt => rangeQuery(identifier.name) lt script
          case Le => rangeQuery(identifier.name) lte script
          case Eq => rangeQuery(identifier.name) gte script lte script
          case Ne | Diff => not(rangeQuery(identifier.name) gte script lte script)
        }
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
                               between: SQLBetween[String]
                             ): Query = {
    import between._
    val r = rangeQuery(identifier.name) gte fromTo.from.value lte fromTo.to.value
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }

  implicit def betweenLongsToQuery(
                                   between: SQLBetween[Long]
                                 ): Query = {
    import between._
    val r = rangeQuery(identifier.name) gte fromTo.from.value lte fromTo.to.value
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }

  implicit def betweenDoublesToQuery(
                                      between: SQLBetween[Double]
                                    ): Query = {
    import between._
    val r = rangeQuery(identifier.name) gte fromTo.from.value lte fromTo.to.value
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
            .map(ElasticAggregation(_, l.having.flatMap(_.criteria), l.sorts))
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
