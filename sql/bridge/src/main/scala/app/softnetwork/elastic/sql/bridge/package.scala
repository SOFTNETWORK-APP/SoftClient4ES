package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`.{SQLBigInt, SQLDouble}
import app.softnetwork.elastic.sql.function.aggregate.COUNT
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.query._

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

    _search = scriptFields.filterNot(_.aggregation) match {
      case Nil => _search
      case _ =>
        _search scriptfields scriptFields.map { field =>
          scriptField(
            field.scriptName,
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

  def applyNumericOp[A](n: NumericValue[_])(
    longOp: Long => A,
    doubleOp: Double => A
  ): A = n.toEither.fold(longOp, doubleOp)

  implicit def expressionToQuery(expression: GenericExpression): Query = {
    import expression._
    if (aggregation)
      return matchAllQuery()
    if (identifier.functions.nonEmpty) {
      return scriptQuery(Script(script = painless).lang("painless").scriptType("source"))
    }
    value match {
      case n: NumericValue[_] =>
        operator match {
          case GE =>
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
          case GT =>
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
          case LE =>
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
          case LT =>
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
          case EQ =>
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
          case NE | DIFF =>
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
      case l: StringValue =>
        operator match {
          case LIKE =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.name, toRegex(l.value)))
              case _ =>
                regexQuery(identifier.name, toRegex(l.value))
            }
          case RLIKE =>
            maybeNot match {
              case Some(_) =>
                not(regexQuery(identifier.name, l.value))
              case _ =>
                regexQuery(identifier.name, l.value)
            }
          case GE =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lt l.value
              case _ =>
                rangeQuery(identifier.name) gte l.value
            }
          case GT =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) lte l.value
              case _ =>
                rangeQuery(identifier.name) gt l.value
            }
          case LE =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gt l.value
              case _ =>
                rangeQuery(identifier.name) lte l.value
            }
          case LT =>
            maybeNot match {
              case Some(_) =>
                rangeQuery(identifier.name) gte l.value
              case _ =>
                rangeQuery(identifier.name) lt l.value
            }
          case EQ =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, l.value))
              case _ =>
                termQuery(identifier.name, l.value)
            }
          case NE | DIFF =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, l.value)
              case _ =>
                not(termQuery(identifier.name, l.value))
            }
          case _ => matchAllQuery()
        }
      case b: BooleanValue =>
        operator match {
          case EQ =>
            maybeNot match {
              case Some(_) =>
                not(termQuery(identifier.name, b.value))
              case _ =>
                termQuery(identifier.name, b.value)
            }
          case NE | DIFF =>
            maybeNot match {
              case Some(_) =>
                termQuery(identifier.name, b.value)
              case _ =>
                not(termQuery(identifier.name, b.value))
            }
          case _ => matchAllQuery()
        }
      case i: Identifier =>
        operator match {
          case op: ComparisonOperator =>
            i.toScript match {
              case Some(script) =>
                val o = if (maybeNot.isDefined) op.not else op
                o match {
                  case GT        => rangeQuery(identifier.name) gt script
                  case GE        => rangeQuery(identifier.name) gte script
                  case LT        => rangeQuery(identifier.name) lt script
                  case LE        => rangeQuery(identifier.name) lte script
                  case EQ        => rangeQuery(identifier.name) gte script lte script
                  case NE | DIFF => not(rangeQuery(identifier.name) gte script lte script)
                }
              case _ =>
                scriptQuery(Script(script = painless).lang("painless").scriptType("source"))
            }
          case _ =>
            scriptQuery(Script(script = painless).lang("painless").scriptType("source"))
        }
      case _ => matchAllQuery()
    }
  }

  implicit def isNullToQuery(
                              isNull: IsNullExpr
                            ): Query = {
    import isNull._
    not(existsQuery(identifier.name))
  }

  implicit def isNotNullToQuery(
                                 isNotNull: IsNotNullExpr
                               ): Query = {
    import isNotNull._
    existsQuery(identifier.name)
  }

  implicit def isNullCriteriaToQuery(
    isNull: IsNullCriteria
  ): Query = {
    import isNull._
    not(existsQuery(identifier.name))
  }

  implicit def isNotNullCriteriaToQuery(
    isNotNull: IsNotNullCriteria
  ): Query = {
    import isNotNull._
    existsQuery(identifier.name)
  }

  implicit def inToQuery[R, T <: Value[R]](in: InExpr[R, T]): Query = {
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
    between: BetweenExpr[_]
  ): Query = {
    import between._
    val r =
      out match {
        case _: SQLDouble =>
          rangeQuery(identifier.name) gte fromTo.from.value.asInstanceOf[Double] lte fromTo.to.value.asInstanceOf[Double]
        case _: SQLBigInt =>
          rangeQuery(identifier.name) gte fromTo.from.value.asInstanceOf[Long] lte fromTo.to.value.asInstanceOf[Long]
        case _ =>
          rangeQuery(identifier.name) gte String.valueOf(fromTo.from.value) lte String.valueOf(fromTo.to.value)
      }
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
                                          criteria: Criteria
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
                    case COUNT if aggregation.sourceField.equalsIgnoreCase("_id") =>
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
