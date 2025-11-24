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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`.{
  SQLBigInt,
  SQLDouble,
  SQLNumeric,
  SQLTemporal,
  SQLVarchar
}
import app.softnetwork.elastic.sql.function.aggregate.COUNT
import app.softnetwork.elastic.sql.function.geo.{Distance, Meters}
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.query._
import com.sksamuel.elastic4s.{ElasticApi, FetchSourceContext}
import com.sksamuel.elastic4s.ElasticApi._
import com.sksamuel.elastic4s.http.ElasticDsl.BuildableTermsNoOp
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.script.Script
import com.sksamuel.elastic4s.script.ScriptType.Source
import com.sksamuel.elastic4s.searches.aggs.{
  Aggregation,
  FilterAggregation,
  NestedAggregation,
  TermsAggregation
}
import com.sksamuel.elastic4s.searches.queries.{BoolQuery, InnerHit, Query}
import com.sksamuel.elastic4s.searches.{MultiSearchRequest, SearchRequest}
import com.sksamuel.elastic4s.searches.sort.{FieldSort, ScriptSort, ScriptSortType}

import scala.language.implicitConversions

package object bridge {

  implicit def requestToNestedFilterAggregation(
    request: SQLSearchRequest,
    innerHitsName: String
  ): Option[FilterAggregation] = {
    val having: Option[Query] =
      request.having.flatMap(_.criteria) match {
        case Some(f) =>
          f.nestedCriteria(innerHitsName) match {
            case Nil => None
            case cs =>
              val boolQuery = ElasticBoolQuery(group = true)
              cs.map(c => boolQuery.filter(c.asFilter(Option(boolQuery))))
              boolQuery.query(
                request.aggregates.flatMap(_.identifier.innerHitsName).toSet,
                Option(boolQuery)
              ) match {
                case q: BoolQuery
                    if q.filters.forall(p =>
                      p == matchAllQuery()
                    ) && q.must.isEmpty && q.should.isEmpty && q.not.isEmpty =>
                  None
                case other => Some(other)
              }
          }
        case _ =>
          None
      }
    val where: Option[Query] =
      request.where.flatMap(_.criteria) match {
        case Some(f) =>
          f.nestedCriteria(innerHitsName) match {
            case Nil => None
            case cs =>
              val boolQuery = ElasticBoolQuery(group = true)
              cs.map(c => boolQuery.filter(c.asFilter(Option(boolQuery))))
              boolQuery.query(
                request.aggregates.flatMap(_.identifier.innerHitsName).toSet,
                Option(boolQuery)
              ) match {
                case q: BoolQuery
                    if q.filters.forall(p =>
                      p == matchAllQuery()
                    ) && q.must.isEmpty && q.should.isEmpty && q.not.isEmpty =>
                  None
                case other => Some(other)
              }
          }
        case _ =>
          None
      }
    (having, where) match {
      case (Some(h), Some(w)) =>
        Some(
          filterAgg(
            s"filtered_$innerHitsName",
            boolQuery().filter(h, w)
          )
        )
      case (Some(h), None) =>
        Some(
          filterAgg(
            s"filtered_$innerHitsName",
            h
          )
        )
      case (None, Some(w)) =>
        Some(
          filterAgg(
            s"filtered_$innerHitsName",
            w
          )
        )
      case _ =>
        None
    }
  }

  implicit def requestToFilterAggregation(
    request: SQLSearchRequest
  ): Option[FilterAggregation] =
    request.having.flatMap(_.criteria) match {
      case Some(f) =>
        val boolQuery = Option(ElasticBoolQuery(group = true))
        Some(
          filterAgg(
            "filtered_agg",
            f.asFilter(boolQuery)
              .query(request.aggregates.flatMap(_.identifier.innerHitsName).toSet, boolQuery)
          )
        )
      case _ =>
        None
    }

  implicit def requestToRootAggregations(
    request: SQLSearchRequest,
    aggregations: Seq[ElasticAggregation]
  ): Seq[Aggregation] = {
    val notNestedAggregations = aggregations.filterNot(_.nested)

    val notNestedBuckets = request.buckets.filterNot(_.nested)

    val rootAggregations = notNestedAggregations match {
      case Nil =>
        val buckets = ElasticAggregation.buildBuckets(
          notNestedBuckets,
          request.sorts,
          Seq.empty,
          Map.empty,
          request.having.flatMap(_.criteria),
          None,
          aggregations
        ) match {
          case Some(b) => Seq(b)
          case _       => Seq.empty
        }
        buckets
      case aggs =>
        val directions: Map[String, SortOrder] = aggs
          .filter(_.direction.isDefined)
          .map(agg => agg.agg.name -> agg.direction.get)
          .toMap

        val aggregations = aggs.map(_.agg)

        val buckets = ElasticAggregation.buildBuckets(
          notNestedBuckets,
          request.sorts -- directions.keys,
          aggregations,
          directions,
          request.having.flatMap(_.criteria),
          None,
          aggs
        ) match {
          case Some(b) => Seq(b)
          case _       => aggregations
        }
        buckets
    }
    rootAggregations
  }

  implicit def requestToScopedAggregations(
    request: SQLSearchRequest,
    aggregations: Seq[ElasticAggregation]
  ): Seq[NestedAggregation] = {
    // Group nested aggregations by their nested path
    val nestedAggregations: Map[String, Seq[ElasticAggregation]] = aggregations
      .filter(_.nested)
      .groupBy(
        _.nestedElement
          .map(_.path)
          .getOrElse(
            throw new IllegalArgumentException("Nested aggregation must have a nested element")
          )
      )

    // Group nested buckets by their nested path
    val nestedGroupedBuckets =
      request.buckets
        .filter(_.nested)
        .groupBy(
          _.nestedBucket.getOrElse(
            throw new IllegalArgumentException(
              "Nested bucket must have a nested element"
            )
          )
        )

    // Having criteria or none
    val havingCriteria = request.having.flatMap(_.criteria)

    // Build nested aggregations
    val scopedAggregations = NestedElements
      .buildNestedTrees(
        nestedAggregations.values.flatMap(_.flatMap(_.nestedElement)).toSeq.distinct
      )
      .map { tree => // For each nested tree, build the nested aggregation
        def buildNestedAgg(n: NestedElement): NestedAggregation = {
          // Get the aggregations for this nested path
          val elasticAggregations = nestedAggregations.getOrElse(n.path, Seq.empty)

          // Get the buckets for this nested element
          val nestedBuckets =
            nestedGroupedBuckets.getOrElse(n.innerHitsName, Seq.empty)

          val notRelatedAggregationsToBuckets = elasticAggregations
            .filterNot { ea =>
              nestedBuckets.exists(nb => nb.identifier.path == ea.sourceField)
            }
            .map(_.agg)

          val relatedAggregationsToBuckets = elasticAggregations
            .filter { ea =>
              nestedBuckets.exists(nb => nb.identifier.path == ea.sourceField)
            }
            .map(_.agg)

          // Get the directions for this nested aggregation
          val directions: Map[String, SortOrder] =
            elasticAggregations
              .filter(_.direction.isDefined)
              .map(elasticAggregation =>
                elasticAggregation.agg.name -> elasticAggregation.direction.getOrElse(Asc)
              )
              .toMap

          // Build filter aggregation for this nested aggregation
          val nestedFilteredAgg: Option[FilterAggregation] =
            requestToNestedFilterAggregation(request, n.innerHitsName)

          // Build buckets for this nested aggregation
          val buckets: Seq[Aggregation] =
            ElasticAggregation.buildBuckets(
              nestedBuckets,
              request.sorts -- directions.keys,
              notRelatedAggregationsToBuckets,
              directions,
              havingCriteria,
              Some(n),
              aggregations
            ) match {
              case Some(b) => Seq(b)
              case _       => notRelatedAggregationsToBuckets
            }

          val children = n.children
          if (children.nonEmpty) {
            val innerAggs = children.map(buildNestedAgg)
            val combinedAgg = if (innerAggs.size == 1) {
              innerAggs.head
            } else {
              innerAggs.reduceLeft { (agg1, agg2) =>
                agg1.copy(subaggs = agg1.subaggs ++ Seq(agg2))
              }
            }
            val combinedBuckets =
              buckets match {
                case Nil => Seq(combinedAgg)
                case b if b.size == 1 =>
                  addNestedAggregationsToTermsAggregation(b.head, Seq(combinedAgg)) match {
                    case Some(updated) => Seq(updated)
                    case _             => b ++ Seq(combinedAgg)
                  }
                case _ => buckets ++ Seq(combinedAgg)
              }
            nestedAggregation(
              n.innerHitsName,
              n.path
            ) subaggs (nestedFilteredAgg match {
              case Some(filteredAgg) =>
                Seq(filteredAgg subaggs relatedAggregationsToBuckets ++ combinedBuckets)
              case _ => relatedAggregationsToBuckets ++ combinedBuckets
            })
          } else {
            nestedAggregation(
              n.innerHitsName,
              n.path
            ) subaggs (nestedFilteredAgg match {
              case Some(filteredAgg) =>
                Seq(filteredAgg subaggs relatedAggregationsToBuckets ++ buckets)
              case _ => relatedAggregationsToBuckets ++ buckets
            })
          }
        }
        buildNestedAgg(tree)
      }
    scopedAggregations
  }

  implicit def requestToNestedWithoutCriteriaQuery(request: SQLSearchRequest): Option[Query] =
    NestedElements.buildNestedTrees(request.nestedElementsWithoutCriteria) match {
      case Nil => None
      case nestedTrees =>
        def nestedInner(n: NestedElement): InnerHit = {
          var inner = innerHits(n.innerHitsName)
          n.size match {
            case Some(s) =>
              inner = inner.from(0).size(s)
            case _ =>
          }
          if (n.sources.nonEmpty) {
            inner = inner.fetchSource(
              FetchSourceContext(
                fetchSource = true,
                includes = n.sources.toArray
              )
            )
          }
          inner
        }

        def buildNestedQuery(n: NestedElement): Query = {
          val children = n.children
          if (children.nonEmpty) {
            val innerQueries = children.map(child => buildNestedQuery(child))
            val combinedQuery = if (innerQueries.size == 1) {
              innerQueries.head
            } else {
              must(innerQueries)
            }
            nestedQuery(
              n.path,
              combinedQuery
            ) /*.scoreMode(ScoreMode.None)*/
              .inner(
                nestedInner(n)
              )
          } else {
            nestedQuery(
              n.path,
              matchAllQuery()
            ) /*.scoreMode(ScoreMode.None)*/
              .inner(
                nestedInner(n)
              )
          }
        }

        if (nestedTrees.size == 1) {
          Some(buildNestedQuery(nestedTrees.head))
        } else {
          val innerQueries = nestedTrees.map(nested => buildNestedQuery(nested))
          Some(boolQuery().filter(innerQueries))
        }
    }

  private def addNestedAggregationsToTermsAggregation(
    agg: Aggregation,
    nested: Seq[NestedAggregation]
  ): Option[TermsAggregation] = {
    agg match {
      case termsAgg: TermsAggregation =>
        termsAgg.subaggs.find(subAgg =>
          subAgg match {
            case _: TermsAggregation => true
            case _                   => false
          }
        ) match {
          case Some(t: TermsAggregation) =>
            val ret = addNestedAggregationsToTermsAggregation(t, nested)
            val updated =
              termsAgg.copy(subaggs = termsAgg.subaggs.filterNot(_ == t) ++ ret.toList)
            Some(updated)
          case _ => Some(termsAgg subaggs termsAgg.subaggs ++ nested)
        }
      case _ => None
    }
  }

  implicit def requestToElasticSearchRequest(request: SQLSearchRequest): ElasticSearchRequest =
    ElasticSearchRequest(
      request.select.fields,
      request.select.except,
      request.sources,
      request.where.flatMap(_.criteria),
      request.limit.map(_.limit),
      request.limit.flatMap(_.offset.map(_.offset)).orElse(Some(0)),
      request,
      request.buckets,
      request.aggregates.map(
        ElasticAggregation(_, request.having.flatMap(_.criteria), request.sorts)
      )
      // request.orderBy.map(_.sorts).getOrElse(Seq.empty)
    ).minScore(request.score)

  implicit def requestToSearchRequest(request: SQLSearchRequest): SearchRequest = {
    import request._

    val aggregations = request.aggregates.map(
      ElasticAggregation(_, request.having.flatMap(_.criteria), request.sorts)
    )

    val rootAggregations = requestToRootAggregations(request, aggregations)

    val scopedAggregations = requestToScopedAggregations(request, aggregations)

    val allAggregations = {
      rootAggregations match {
        case Nil => scopedAggregations
        case r if r.size == 1 =>
          addNestedAggregationsToTermsAggregation(r.head, scopedAggregations) match {
            case Some(agg) =>
              Seq(agg)
            case _ => r ++ scopedAggregations
          }
        case _ => rootAggregations ++ scopedAggregations
      }
    }

    val nestedWithoutCriteriaQuery: Option[Query] = requestToNestedWithoutCriteriaQuery(request)

    var _search: SearchRequest = search("") query {
      where.flatMap(_.criteria.map(_.asQuery())) match {
        case Some(c) =>
          val baseQuery = c
          nestedWithoutCriteriaQuery match {
            case Some(nc) => boolQuery().filter(baseQuery, nc)
            case _        => baseQuery
          }
        case _ =>
          nestedWithoutCriteriaQuery.getOrElse(matchAllQuery())
      }
    } sourceFiltering (fields, excludes)

    _search = if (allAggregations.nonEmpty) {
      _search aggregations {
        allAggregations
      }
    } else {
      _search
    }

    _search = scriptFields.filterNot(_.isAggregation) match {
      case Nil => _search
      case _ =>
        _search scriptfields scriptFields.map { field =>
          val context = PainlessContext()
          val script = field.painless(Some(context))
          scriptField(
            field.scriptName,
            Script(script = s"$context$script")
              .lang("painless")
              .scriptType("source")
              .params(field.identifier.functions.headOption match {
                case Some(f: PainlessParams) => f.params
                case _                       => Map.empty[String, Any]
              })
          )
        }
    }

    _search = orderBy match {
      case Some(o) if aggregates.isEmpty && buckets.isEmpty =>
        _search sortBy o.sorts.map { sort =>
          if (sort.isScriptSort) {
            val context = PainlessContext()
            val painless = sort.field.painless(Some(context))
            val painlessScript = s"$context$painless"
            val script =
              sort.out match {
                case _: SQLTemporal if !painless.endsWith("toEpochMilli()") =>
                  val parts = painlessScript.split(";").toSeq
                  if (parts.size > 1) {
                    val lastPart = parts.last.trim.stripPrefix("return ")
                    if (lastPart.split(" ").toSeq.size == 1) {
                      val newLastPart =
                        s"""($lastPart != null) ? $lastPart.toInstant().toEpochMilli() : null"""
                      s"${parts.dropRight(1).mkString(";")}; return $newLastPart"
                    } else {
                      painlessScript
                    }
                  } else {
                    s"$painlessScript.toInstant().toEpochMilli()"
                  }
                case _ => painlessScript
              }
            val scriptSort =
              ScriptSort(
                script = Script(script = script)
                  .lang("painless")
                  .scriptType(Source),
                scriptSortType = sort.field.out match {
                  case _: SQLTemporal | _: SQLNumeric => ScriptSortType.Number
                  case _                              => ScriptSortType.String
                }
              )
            sort.order match {
              case Some(Desc) => scriptSort.desc()
              case _          => scriptSort.asc()
            }
          } else {
            sort.order match {
              case Some(Desc) => FieldSort(sort.field.aliasOrName).desc()
              case _          => FieldSort(sort.field.aliasOrName).asc()
            }
          }
        }
      case _ => _search
    }

    if (allAggregations.nonEmpty || buckets.nonEmpty) {
      _search size 0 fetchSource false
    } else {
      limit match {
        case Some(l) => _search limit l.limit from l.offset.map(_.offset).getOrElse(0)
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
    if (isAggregation)
      return matchAllQuery()
    if (
      identifier.functions.nonEmpty && (identifier.functions.size > 1 || (identifier.functions.head match {
        case _: Distance => false
        case _           => true
      }))
    ) {
      val context = PainlessContext()
      val script = painless(Some(context))
      return scriptQuery(
        Script(script = s"$context$script").lang("painless").scriptType("source")
      )
    }
    // Geo distance special case
    identifier.functions.headOption match {
      case Some(d: Distance) =>
        operator match {
          case o: ComparisonOperator =>
            (value match {
              case l: LongValue =>
                Some(GeoDistance(l, Meters))
              case g: GeoDistance =>
                Some(g)
              case _ => None
            }) match {
              case Some(g) =>
                maybeNot match {
                  case Some(_) =>
                    return geoDistanceToQuery(
                      DistanceCriteria(
                        d,
                        o.not,
                        g
                      )
                    )
                  case _ =>
                    return geoDistanceToQuery(
                      DistanceCriteria(
                        d,
                        o,
                        g
                      )
                    )
                }
              case _ =>
            }
        }
      case _ =>
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
            i.script match {
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
                val context = PainlessContext()
                val script = painless(Some(context))
                scriptQuery(
                  Script(script = s"$context$script")
                    .lang("painless")
                    .scriptType("source")
                )
            }
          case _ =>
            val context = PainlessContext()
            val script = painless(Some(context))
            scriptQuery(
              Script(script = s"$context$script")
                .lang("painless")
                .scriptType("source")
            )
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
    between: BetweenExpr
  ): Query = {
    import between._
    // Geo distance special case
    identifier.functions.headOption match {
      case Some(d: Distance) =>
        fromTo match {
          case ft: GeoDistanceFromTo =>
            val fq =
              geoDistanceToQuery(
                DistanceCriteria(
                  d,
                  GE,
                  ft.from
                )
              )
            val tq =
              geoDistanceToQuery(
                DistanceCriteria(
                  d,
                  LE,
                  ft.to
                )
              )
            maybeNot match {
              case Some(_) => return not(fq, tq)
              case _       => return must(fq, tq)
            }
          case _ =>
        }
      case _ =>
    }
    val r =
      fromTo.out match {
        case _: SQLDouble =>
          rangeQuery(identifier.name) gte fromTo.from.value.asInstanceOf[Double] lte fromTo.to.value
            .asInstanceOf[Double]
        case _: SQLBigInt =>
          rangeQuery(identifier.name) gte fromTo.from.value.asInstanceOf[Long] lte fromTo.to.value
            .asInstanceOf[Long]
        case _: SQLVarchar =>
          rangeQuery(identifier.name) gte String.valueOf(fromTo.from.value) lte String.valueOf(
            fromTo.to.value
          )
        case _: SQLTemporal =>
          fromTo match {
            case ft: IdentifierFromTo =>
              (ft.from.script, ft.to.script) match {
                case (Some(from), Some(to)) =>
                  rangeQuery(identifier.name) gte from lte to
                case (Some(from), None) =>
                  val fq = rangeQuery(identifier.name) gte from
                  val tq = GenericExpression(identifier, LE, ft.to, None)
                  maybeNot match {
                    case Some(_) => return not(fq, tq)
                    case _       => return must(fq, tq)
                  }
                case (None, Some(to)) =>
                  val fq = GenericExpression(identifier, GE, ft.from, None)
                  val tq = rangeQuery(identifier.name) lte to
                  maybeNot match {
                    case Some(_) => return not(fq, tq)
                    case _       => return must(fq, tq)
                  }
                case _ =>
                  val fq = GenericExpression(identifier, GE, ft.from, None)
                  val tq = GenericExpression(identifier, LE, ft.to, None)
                  maybeNot match {
                    case Some(_) => return not(fq, tq)
                    case _       => return must(fq, tq)
                  }
              }
            case other =>
              throw new IllegalArgumentException(s"Unsupported type for range query: $other")
          }
        case _ =>
          throw new IllegalArgumentException(s"Unsupported out type for range query: ${fromTo.out}")
      }
    maybeNot match {
      case Some(_) => not(r)
      case _       => r
    }
  }

  implicit def geoDistanceToQuery(
    distanceCriteria: DistanceCriteria
  ): Query = {
    import distanceCriteria._
    operator match {
      case LE | LT if distance.oneIdentifier =>
        val identifier = distance.identifiers.head
        val point = distance.points.head
        geoDistanceQuery(
          identifier.name,
          point.lat.value,
          point.lon.value
        ) distance geoDistance.geoDistance
      case _ =>
        scriptQuery(
          Script(
            script = distanceCriteria.painless(None),
            lang = Some("painless"),
            scriptType = Source,
            params = distance.params
          )
        )
    }
  }

  implicit def matchToQuery(
    matchExpression: MatchCriteria
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
  ): ElasticBridge = {
    ElasticBridge(filter)
  }

  @deprecated
  implicit def sqlQueryToAggregations(
    query: SQLQuery
  ): Seq[ElasticAggregation] = {
    import query._
    request
      .map {
        case Left(l) =>
          val filteredAgg: Option[FilterAggregation] = requestToFilterAggregation(l)
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
                            filteredAgg match {
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
                  }).string().replace("\"version\":true,", "") /*FIXME*/
                )
              )
            })

        case _ => Seq.empty

      }
      .getOrElse(Seq.empty)
  }
}
