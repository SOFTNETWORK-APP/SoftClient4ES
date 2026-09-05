package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.Criteria
import com.fasterxml.jackson.databind.JsonNode
import com.sksamuel.elastic4s.ElasticApi.matchAllQuery
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** Created by smanciot on 13/04/17.
  */
class SQLCriteriaSpec extends AnyFlatSpec with Matchers {

  import parser.Queries._

  import scala.language.implicitConversions

  def asQuery(sql: String): String = {
    import SQLImplicits._
    implicit def timestamp: Long =
      ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli
    val criteria: Option[Criteria] = sql
    val result = SearchBodyBuilderFn(
      SearchRequest("*") query criteria.map(_.asQuery()).getOrElse(matchAllQuery())
    ).string
    println(result)
    result
  }

  "SQLCriteria" should "filter numerical eq" in {
    asQuery(numericalEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : 1.0
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ne" in {
    asQuery(numericalNe) shouldBe """{

        |"query":{
        |   "bool":{
        |       "filter":[{"bool":{"must_not":[
        |         {
        |           "term":{
        |             "identifier":{
        |               "value":1
        |             }
        |           }
        |         }
        |       ]
        |    }
        | }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical lt" in {
    asQuery(numericalLt) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "lt" : 1
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical le" in {
    asQuery(numericalLe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "lte" : 1
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical gt" in {
    asQuery(numericalGt) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gt" : 1
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter numerical ge" in {
    asQuery(numericalGe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gte" : 1
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal eq" in {
    asQuery(literalEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : "un"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal ne" in {
    asQuery(literalNe) shouldBe """{

        |"query":{
        |    "bool" : {
        |      "filter":[{"bool":{"must_not" : [
        |        {
        |          "term" : {
        |            "identifier" : {
        |              "value" : "un"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal like" in {
    asQuery(literalLike) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"regexp" : {
        |      "identifier" : {
        |        "value" : ".*u.n.*"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter literal not like" in {
    asQuery(literalNotLike) shouldBe """{
        |"query":{
        |    "bool": {
        |      "filter":[{"bool":{"must_not": [{
        |        "regexp": {
        |          "identifier": {
        |            "value": ".*un.*"
        |          }
        |        }
        |      }]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter between" in {
    asQuery(betweenExpression) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"range" : {
        |      "identifier" : {
        |        "gte" : "1",
        |        "lte" : "2"
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter and predicate" in {
    asQuery(andPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : 1
        |            }
        |          }
        |        },
        |        {
        |          "range" : {
        |            "identifier2" : {
        |              "gt" : 2
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter or predicate" in {
    asQuery(orPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : 1
        |            }
        |          }
        |        },
        |        {
        |          "range" : {
        |            "identifier2" : {
        |              "gt" : 2
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter left predicate with criteria" in {
    asQuery(leftPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier1" : {
        |                    "value" : 1
        |                  }
        |                }
        |              },
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : 2
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "term" : {
        |            "identifier3" : {
        |              "value" : 3
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter right predicate with criteria" in {
    asQuery(rightPredicate) shouldBe """{

        |"query":{
        |    "bool":{
        |      "filter" : [
        |        {
        |          "term" : {
        |            "identifier1" : {
        |              "value" : 1
        |            }
        |          }
        |        },
        |        {
        |          "bool" : {
        |            "should" : [
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : 2
        |                  }
        |                }
        |              },
        |              {
        |                "term" : {
        |                  "identifier3" : {
        |                    "value" : 3
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter multiple predicates" in {
    asQuery(predicates) shouldBe """{

        |"query":{
        |    "bool":{
        |      "should" : [
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier1" : {
        |                    "value" : 1
        |                  }
        |                }
        |              },
        |              {
        |                "range" : {
        |                  "identifier2" : {
        |                    "gt" : 2
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "bool" : {
        |            "filter" : [
        |              {
        |                "term" : {
        |                  "identifier3" : {
        |                    "value" : 3
        |                  }
        |                }
        |              },
        |              {
        |                "term" : {
        |                  "identifier4" : {
        |                    "value" : 4
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in literal expression" in {
    asQuery(inLiteralExpression) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        "val1",
        |        "val2",
        |        "val3"
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Int values" in {
    asQuery(inNumericalExpressionWithIntValues) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        1,
        |        2,
        |        3
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter in numerical expression with Double values" in {
    asQuery(inNumericalExpressionWithDoubleValues) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"terms" : {
        |      "identifier" : [
        |        1.0,
        |        2.1,
        |        3.4
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested predicate" in {
    asQuery(nestedPredicate) shouldBe
    """{
      |  "query": {
      |    "bool": {
      |      "filter": [
      |        {
      |          "term": {
      |            "identifier1": {
      |              "value": 1
      |            }
      |          }
      |        },
      |        {
      |          "nested": {
      |            "path": "nested",
      |            "query": {
      |              "bool": {
      |                "should": [
      |                  {
      |                    "range": {
      |                      "nested.identifier2": {
      |                        "gt": 2
      |                      }
      |                    }
      |                  },
      |                  {
      |                    "term": {
      |                      "nested.identifier3": {
      |                        "value": 3
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            "inner_hits": {
      |              "name": "nested"
      |            }
      |          }
      |        }
      |      ]
      |    }
      |  }
      |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested criteria" in {
    asQuery(nestedCriteria) shouldBe
    """{
      |  "query": {
      |    "bool": {
      |      "filter": [
      |        {
      |          "term": {
      |            "identifier1": {
      |              "value": 1
      |            }
      |          }
      |        },
      |        {
      |          "nested": {
      |            "path": "nested",
      |            "query": {
      |              "term": {
      |                "nested.identifier3": {
      |                  "value": 3
      |                }
      |              }
      |            },
      |            "inner_hits": {
      |              "name": "nested"
      |            }
      |          }
      |        }
      |      ]
      |    }
      |  }
      |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter child predicate" in {
    asQuery(childPredicate) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "identifier1": {
        |              "value": 1
        |            }
        |          }
        |        },
        |        {
        |          "has_child": {
        |            "type": "child",
        |            "score_mode": "none",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "should": [
        |                        {
        |                          "range": {
        |                            "child.identifier2": {
        |                              "gt": 2
        |                            }
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "child.identifier3": {
        |                              "value": 3
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  // Story 21.4 / #250 - N-ARY relation predicates, and the reason this assertion is on the
  // GENERATED QUERY rather than on the parse.
  //
  // `NESTED`/`CHILD`/`PARENT` used to take a strictly BINARY `predicate`, so three or more criteria
  // fell through to a form whose `start.?` swallowed the `(` while its `end.?` never fired. The
  // result was NOT a syntax error: it PARSED, with the relation scope collapsed to the FIRST
  // criterion and the rest escaping onto the PARENT document. Measured before the fix, the query
  // below was emitted with `child.identifier3` and `child.identifier4` as PARENT-level filters,
  // i.e. matched against the wrong documents entirely - the #205/#253 silent-wrong-answer family.
  //
  // What this pins: all THREE terms inside the single `has_child.query`, and NOTHING leaking into
  // the outer bool. A parse-only assertion could not have caught the defect.
  it should "filter child predicate with three criteria, all scoped inside the relation" in {
    asQuery(childPredicateN) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "has_child": {
        |            "type": "child",
        |            "score_mode": "none",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "filter": [
        |                        {
        |                          "bool": {
        |                            "filter": [
        |                              {
        |                                "term": {
        |                                  "child.identifier2": {
        |                                    "value": 2
        |                                  }
        |                                }
        |                              },
        |                              {
        |                                "term": {
        |                                  "child.identifier3": {
        |                                    "value": 3
        |                                  }
        |                                }
        |                              }
        |                            ]
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "child.identifier4": {
        |                              "value": 4
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  // The sharpest version of the defect: with OR, the operator itself used to span the relation
  // boundary (`CHILD(x = 1) OR y = 2 OR z = 3`), so a parent document matched if EITHER it had a
  // matching child OR its own field matched. Every `should` must now be inside the has_child.
  it should "filter child predicate with three OR criteria, all scoped inside the relation" in {
    asQuery(childPredicateNOr) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "has_child": {
        |            "type": "child",
        |            "score_mode": "none",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "should": [
        |                        {
        |                          "bool": {
        |                            "should": [
        |                              {
        |                                "term": {
        |                                  "child.identifier2": {
        |                                    "value": 2
        |                                  }
        |                                }
        |                              },
        |                              {
        |                                "term": {
        |                                  "child.identifier3": {
        |                                    "value": 3
        |                                  }
        |                                }
        |                              }
        |                            ]
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "child.identifier4": {
        |                              "value": 4
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  // The mixed case, which is what distinguishes "scoped correctly" from "everything moved inside":
  // `identifier1` is a genuine PARENT-level criterion and must stay in the outer bool, while all
  // three child criteria stay inside the has_child.
  it should "keep a parent-level criterion outside an N-ary child predicate" in {
    asQuery(childPredicateNWithParentCriterion) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "identifier1": {
        |              "value": 1
        |            }
        |          }
        |        },
        |        {
        |          "has_child": {
        |            "type": "child",
        |            "score_mode": "none",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "filter": [
        |                        {
        |                          "bool": {
        |                            "filter": [
        |                              {
        |                                "term": {
        |                                  "child.identifier2": {
        |                                    "value": 2
        |                                  }
        |                                }
        |                              },
        |                              {
        |                                "term": {
        |                                  "child.identifier3": {
        |                                    "value": 3
        |                                  }
        |                                }
        |                              }
        |                            ]
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "child.identifier4": {
        |                              "value": 4
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter parent predicate with three criteria, all scoped inside the relation" in {
    asQuery(parentPredicateN) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "has_parent": {
        |            "parent_type": "parent",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "filter": [
        |                        {
        |                          "bool": {
        |                            "filter": [
        |                              {
        |                                "term": {
        |                                  "parent.identifier2": {
        |                                    "value": 2
        |                                  }
        |                                }
        |                              },
        |                              {
        |                                "term": {
        |                                  "parent.identifier3": {
        |                                    "value": 3
        |                                  }
        |                                }
        |                              }
        |                            ]
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "parent.identifier4": {
        |                              "value": 4
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter child criteria" in {
    asQuery(childCriteria) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "identifier1": {
        |              "value": 1
        |            }
        |          }
        |        },
        |        {
        |          "has_child": {
        |            "type": "child",
        |            "score_mode": "none",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "term": {
        |                      "child.identifier3": {
        |                        "value": 3
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter parent predicate" in {
    asQuery(parentPredicate) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "identifier1": {
        |              "value": 1
        |            }
        |          }
        |        },
        |        {
        |          "has_parent": {
        |            "parent_type": "parent",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "bool": {
        |                      "should": [
        |                        {
        |                          "range": {
        |                            "parent.identifier2": {
        |                              "gt": 2
        |                            }
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "parent.identifier3": {
        |                              "value": 3
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter parent criteria" in {
    asQuery(parentCriteria) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "identifier1": {
        |              "value": 1
        |            }
        |          }
        |        },
        |        {
        |          "has_parent": {
        |            "parent_type": "parent",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "term": {
        |                      "parent.identifier3": {
        |                        "value": 3
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested with between" in {
    asQuery(nestedWithBetween) shouldBe
    """{
      |  "query": {
      |    "bool": {
      |      "filter": [
      |        {
      |          "nested": {
      |            "path": "ciblage",
      |            "query": {
      |              "bool": {
      |                "filter": [
      |                  {
      |                    "range": {
      |                      "ciblage.Archivage_CreationDate": {
      |                        "gte": "now-3M/M",
      |                        "lte": "now"
      |                      }
      |                    }
      |                  },
      |                  {
      |                    "term": {
      |                      "ciblage.statutComportement": {
      |                        "value": 1
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            "inner_hits": {
      |              "name": "ciblage"
      |            }
      |          }
      |        }
      |      ]
      |    }
      |  }
      |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean eq" in {
    asQuery(boolEq) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"term" : {
        |      "identifier" : {
        |        "value" : true
        |      }
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter boolean ne" in {
    asQuery(boolNe) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"bool" : {
        |      "must_not" : [
        |        {
        |          "term" : {
        |            "identifier" : {
        |              "value" : false
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is null" in {
    asQuery(isNull) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"bool" : {
        |      "must_not" : [
        |        {
        |          "exists" : {
        |            "field" : "identifier"
        |          }
        |        }
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter is not null" in {
    asQuery(isNotNull) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"exists" : {
        |      "field" : "identifier"
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter geo distance criteria" in {
    asQuery(geoDistanceCriteria) shouldBe
    """{

        |"query": {
        |    "bool":{"filter":[{"geo_distance": {
        |      "distance": "5km",
        |      "profile.location": [
        |        40.0,
        |        -70.0
        |      ]
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter match criteria" in {
    asQuery(matchCriteria) shouldBe
    """{
        | "query":{
        |   "bool":{
        |     "should":[
        |       {
        |         "match":{
        |           "identifier1":{
        |             "query":"value"
        |           }
        |         }
        |       },
        |       {
        |         "match":{
        |           "identifier2":{
        |             "query":"value"
        |           }
        |         }
        |       },
        |       {
        |         "match":{
        |           "identifier3":{
        |             "query":"value"
        |           }
        |         }
        |       }
        |     ]
        |   }
        | }
        | }""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter complex queries" in {
    val query =
      """select * from Table
        |where (identifier is not null and identifier = 1) or
        |(
        | (identifier is null or identifier2 > 2)
        | and identifier3 = 3
        |)""".stripMargin
    asQuery(query) shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "should": [
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "exists": {
        |                  "field": "identifier"
        |                }
        |              },
        |              {
        |                "term": {
        |                  "identifier": {
        |                    "value": 1
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "bool": {
        |                  "should": [
        |                    {
        |                      "bool": {
        |                        "must_not": [
        |                          {
        |                            "exists": {
        |                              "field": "identifier"
        |                            }
        |                          }
        |                        ]
        |                      }
        |                    },
        |                    {
        |                      "range": {
        |                        "identifier2": {
        |                          "gt": 2
        |                        }
        |                      }
        |                    }
        |                  ]
        |                }
        |              },
        |              {
        |                "term": {
        |                  "identifier3": {
        |                    "value": 3
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s", "")
  }

  /** #212 — a watcher search input flattens its FROM to bare index names, so an alias-qualified
    * WHERE must be resolved or the emitted query names a field that exists in no index. This is the
    * observable half of the ParserSpec assertions: what actually reaches Elasticsearch.
    */
  it should "emit the bare field name for an alias-qualified watcher search input" in {
    implicit def timestamp: Long =
      ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli
    val criteria: Option[Criteria] = parser
      .Parser("""CREATE OR REPLACE WATCHER my_watcher AS
                | EVERY 5 MINUTES
                | FROM orders o WHERE o.status = 'FAILED' WITHIN 2 MINUTES
                | ALWAYS DO
                | log_action AS LOG "x" AT INFO
                | END""".stripMargin)
      .toOption
      .collect { case c: query.CreateWatcher => c.input }
      .collect { case s: watcher.SearchWatcherInput => s.query }
      .flatten
    val result = SearchBodyBuilderFn(
      SearchRequest("*") query criteria.map(_.asQuery()).getOrElse(matchAllQuery())
    ).string
    println(result)
    result.replaceAll("\\s", "") shouldBe
    """{
        |"query":{
        |  "bool":{"filter":[{"term":{
        |    "status":{
        |      "value":"FAILED"
        |    }
        |  }
        |}
        |]}}}""".stripMargin.replaceAll("\\s", "")
  }

  /** #211 + #212 together, through the real criteria conversion: building a watcher that carries a
    * WHERE used to throw `ClassCastException` (value discard on the generic `ObjectNode.set`), and
    * the alias-qualified column used to reach Elasticsearch as `o.status`, a field in no index.
    */
  it should "build the watcher JSON for an alias-qualified search input" in {
    implicit def timestamp: Long =
      ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli
    implicit val toNode: Criteria => JsonNode = c => criteriaToNode(c)
    val watcher = parser
      .Parser("""CREATE OR REPLACE WATCHER my_watcher AS
                | EVERY 5 MINUTES
                | FROM orders o WHERE o.status = 'FAILED' WITHIN 2 MINUTES
                | ALWAYS DO
                | log_action AS LOG "x" AT INFO
                | END""".stripMargin)
      .toOption
      .collect { case c: query.CreateWatcher => c.watcher }
      .getOrElse(fail("expected a CreateWatcher"))
    val json = watcher.node.toString
    println(json)
    json should include("\"indices\":[\"orders\"]")
    json should include("\"term\":{\"status\":{\"value\":\"FAILED\"}}")
    json should not include "o.status"
  }

}
