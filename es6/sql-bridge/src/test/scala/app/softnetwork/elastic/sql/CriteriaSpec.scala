package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import com.sksamuel.elastic4s.ElasticApi.matchAllQuery
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.SearchRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class CriteriaSpec extends AnyFlatSpec with Matchers {

  import Queries._

  import scala.language.implicitConversions

  def asQuery(sql: String): String = {
    import SQLImplicits._
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
        |        "value" : ".*un.*"
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
    asQuery(nestedPredicate) shouldBe """{

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
        |          "nested" : {
        |            "path" : "nested",
        |            "query" : {
        |              "bool" : {
        |                "should" : [
        |                  {
        |                    "range" : {
        |                      "nested.identifier2" : {
        |                        "gt" : 2
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "term" : {
        |                      "nested.identifier3" : {
        |                        "value" : 3
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits":{"name":"nested","from":0,"size":3}
        |          }
        |        }
        |      ]
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s", "")
  }

  it should "filter nested criteria" in {
    asQuery(nestedCriteria) shouldBe """{

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
        |          "nested" : {
        |            "path" : "nested",
        |            "query" : {
        |              "term" : {
        |                "nested.identifier3" : {
        |                  "value" : 3
        |                }
        |              }
        |            },
        |            "inner_hits":{"name":"nested","from":0,"size":3}
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
    asQuery(nestedWithBetween) shouldBe """{

        |"query":{
        |    "bool":{"filter":[{"nested" : {
        |      "path" : "ciblage",
        |      "query" : {
        |        "bool" : {
        |          "filter" : [
        |            {
        |              "range" : {
        |                "ciblage.Archivage_CreationDate" : {
        |                  "gte" : "now-3M/M",
        |                  "lte" : "now"
        |                }
        |              }
        |            },
        |            {
        |              "term" : {
        |                "ciblage.statutComportement" : {
        |                  "value" : 1
        |                }
        |              }
        |            }
        |          ]
        |        }
        |      },
        |      "inner_hits":{"name":"ciblage","from":0,"size":3}
        |    }
        |  }
        |]}}}""".stripMargin.replaceAll("\\s", "")
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

}
