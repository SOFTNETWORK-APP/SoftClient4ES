package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.Queries._
import com.google.gson.{JsonArray, JsonObject, JsonParser, JsonPrimitive}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.JavaConverters._

/** Created by smanciot on 13/04/17.
  */
class SQLQuerySpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def sqlQueryToRequest(sqlQuery: SQLQuery): ElasticSearchRequest = {
    sqlQuery.request match {
      case Some(Left(value)) =>
        value.copy(score = sqlQuery.score)
      case None =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  "SQLQuery" should "perform native count" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery("select count(t.id) c2 from Table t where t.nom = \"Nom\"")
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe false
    result.aggName shouldBe "c2"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query.getOrElse("") shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "c2": {
        |      "value_count": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform count distinct" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery("select count(distinct t.id) as c2 from Table as t where nom = \"Nom\"")
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe true
    result.aggName shouldBe "c2"
    result.field shouldBe "c2"
    result.sources shouldBe Seq[String]("Table")
    result.query.getOrElse("") shouldBe
    """|{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "c2": {
        |      "cardinality": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(inner_emails.value) as email from index i, unnest(emails) as inner_emails where i.nom = \"Nom\""
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.email"
    result.field shouldBe "email"
    result.sources shouldBe Seq[String]("index")
    result.query.getOrElse("") shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "email": {
        |          "value_count": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with nested criteria" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query.getOrElse("") shouldBe
    """{
        |  "query": {
        |    "bool":{
        |      "filter": [
        |          {
        |            "term": {
        |              "nom": {
        |                "value": "Nom"
        |              }
        |            }
        |          },
        |          {
        |            "nested": {
        |              "path": "profiles",
        |              "query": {
        |                "terms": {
        |                  "profiles.postalCode": [
        |                    "75001",
        |                    "75002"
        |                  ]
        |                }
        |              },
        |              "inner_hits":{"name":"inner_profiles","from":0,"size":3}
        |            }
        |          }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_emails": {
        |          "value_count": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with filter" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\")) having inner_emails.context = \"profile\""
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_emails.filtered_agg.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query.getOrElse("") shouldBe
    """{
        |  "query": {
        |    "bool":{
        |      "filter": [
        |        {
        |          "term": {
        |            "nom": {
        |              "value": "Nom"
        |            }
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "profiles",
        |            "query": {
        |              "terms": {
        |                "profiles.postalCode": [
        |                  "75001",
        |                  "75002"
        |                ]
        |              }
        |            },
        |            "inner_hits":{"name":"inner_profiles","from":0,"size":3}
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "filtered_agg": {
        |          "filter": {
        |            "term": {
        |              "emails.context": {
        |                "value": "profile"
        |              }
        |            }
        |          },
        |          "aggs": {
        |            "count_emails": {
        |              "value_count": {
        |                "field": "emails.value"
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with \"and not\" operator" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(distinct inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where ((inner_profiles.postalCode = \"33600\") and (inner_profiles.postalCode <> \"75001\"))"
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_emails.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    result.query.getOrElse("") shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "nested": {
        |            "path": "profiles",
        |            "query": {
        |              "bool": {
        |                "filter": [
        |                  {
        |                    "term": {
        |                      "profiles.postalCode": {
        |                        "value": "33600"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "bool": {
        |                      "must_not": [
        |                        {
        |                          "term": {
        |                            "profiles.postalCode": {
        |                              "value": "75001"
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "inner_profiles",
        |              "from": 0,
        |              "size": 3
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "nested_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_emails": {
        |          "cardinality": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}
        |""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with date filtering" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(distinct inner_emails.value) as count_distinct_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where inner_profiles.postalCode = \"33600\" and inner_profiles.createdDate <= \"now-35M/M\""
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_emails.count_distinct_emails"
    result.field shouldBe "count_distinct_emails"
    result.sources shouldBe Seq[String]("index")
    result.query.getOrElse("") shouldBe
    """{
    "query": {
      |        "bool": {
      |            "filter": [
      |                {
      |                    "nested": {
      |                        "path": "profiles",
      |                        "query": {
      |                            "bool": {
      |                                "filter": [
      |                                    {
      |                                        "term": {
      |                                            "profiles.postalCode": {
      |                                                "value": "33600"
      |                                            }
      |                                        }
      |                                    },
      |                                    {
      |                                        "range": {
      |                                            "profiles.createdDate": {
      |                                                "lte": "now-35M/M"
      |                                            }
      |                                        }
      |                                    }
      |                                ]
      |                            }
      |                        },
      |                        "inner_hits": {
      |                            "name": "inner_profiles",
      |                            "from": 0,
      |                            "size": 3
      |                        }
      |                    }
      |                }
      |            ]
      |        }
      |    },
      |    "size": 0,
      |    "aggs": {
      |        "nested_emails": {
      |            "nested": {
      |                "path": "emails"
      |            },
      |            "aggs": {
      |                "count_distinct_emails": {
      |                    "cardinality": {
      |                        "field": "emails.value"
      |                    }
      |                }
      |            }
      |        }
      |    }
      |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested select" in {
    val select: ElasticSearchRequest =
      SQLQuery("""
        |SELECT
        |profileId,
        |profile_ccm.email as email,
        |profile_ccm.city as city,
        |profile_ccm.firstName as firstName,
        |profile_ccm.lastName as lastName,
        |profile_ccm.postalCode as postalCode,
        |profile_ccm.birthYear as birthYear
        |FROM index, unnest(profiles) as profile_ccm
        |WHERE
        |((profile_ccm.postalCode BETWEEN "10" AND "99999")
        |AND
        |(profile_ccm.birthYear <= 2000))
        |limit 100""".stripMargin)
    val query = select.query
    val queryWithoutSource = query.substring(0, query.indexOf("_source") - 2) + "}"
    queryWithoutSource shouldBe
    """{
      |    "query": {
      |        "bool": {
      |            "filter": [
      |                {
      |                    "nested": {
      |                        "path": "profiles",
      |                        "query": {
      |                            "bool": {
      |                                "filter": [
      |                                    {
      |                                        "range": {
      |                                            "profiles.postalCode": {
      |                                                "gte": "10",
      |                                                "lte": "99999"
      |                                            }
      |                                        }
      |                                    },
      |                                    {
      |                                        "range": {
      |                                            "profiles.birthYear": {
      |                                                "lte": "2000"
      |                                            }
      |                                        }
      |                                    }
      |                                ]
      |                            }
      |                        },
      |                        "inner_hits": {
      |                            "name": "profile_ccm",
      |                            "from": 0,
      |                            "size": 3
      |                        }
      |                    }
      |                }
      |            ]
      |        }
      |    },
      |    "from": 0,
      |    "size": 100
      |}""".stripMargin.replaceAll("\\s+", "")
    val includes = new JsonParser()
      .parse(query.substring(query.indexOf("_source") + 9, query.length - 1))
      .asInstanceOf[JsonObject]
      .get("includes")
      .asInstanceOf[JsonArray]
      .iterator()
      .asScala
    val sourceIncludes: Seq[String] = (
      for (i <- includes) yield i.asInstanceOf[JsonPrimitive].getAsString
    ).toSeq
    val expectedSourceIncludes = Seq(
      "profileId",
      "profile_ccm.email",
      "profile_ccm.city",
      "profile_ccm.firstName",
      "profile_ccm.lastName",
      "profile_ccm.postalCode",
      "profile_ccm.birthYear"
    )
    sourceIncludes should contain theSameElementsAs expectedSourceIncludes
  }

  it should "exclude fields from select" in {
    val select: ElasticSearchRequest =
      SQLQuery(
        except
      )
    select.query shouldBe
    """
        |{
        | "query":{
        |   "match_all":{}
        | },
        | "_source":{
        |   "excludes":["col1","col2"]
        | }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform complex query" in {
    val select: ElasticSearchRequest =
      SQLQuery(
        s"""SELECT
           |  inner_products.name,
           |  inner_products.category,
           |  inner_products.price,
           |  min(inner_products.price) as min_price,
           |  max(inner_products.price) as max_price
           |FROM
           |  stores store,
           |  UNNEST(store.products LIMIT 10) as inner_products
           |WHERE
           |  (
           |    firstName is not null AND
           |    lastName is not null AND
           |    description is not null AND
           |    preparationTime <= 120 AND
           |    store.deliveryPeriods.dayOfWeek=6 AND
           |    blockedCustomers not like "%uuid%" AND
           |    NOT receiptOfOrdersDisabled=true AND
           |    (
           |      distance(pickup.location,(0.0,0.0)) <= "7000m" OR
           |      distance(withdrawals.location,(0.0,0.0)) <= "7000m"
           |    ) AND
           |    (
           |      inner_products.deleted=false AND
           |      inner_products.upForSale=true AND
           |      inner_products.stock > 0
           |    )
           |  ) AND
           |  (
           |    match (products.name) against ("lasagnes") AND
           |    match (products.description, products.ingredients) against ("lasagnes")
           |  )
           |ORDER BY preparationTime ASC, nbOrders DESC
           |LIMIT 100""".stripMargin
      ).minScore(1.0)
    val query = select.query
    println(query)
    query shouldBe
    """
        |{
        |  "query": {
        |    "bool": {
        |      "must": [
        |        {
        |          "match": {
        |            "products.name": {
        |              "query": "lasagnes"
        |            }
        |          }
        |        },
        |        {
        |          "bool": {
        |            "should": [
        |              {
        |                "match": {
        |                  "products.description": {
        |                    "query": "lasagnes"
        |                  }
        |                }
        |              },
        |              {
        |                "match": {
        |                  "products.ingredients": {
        |                    "query": "lasagnes"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ],
        |      "filter": [
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "exists": {
        |                  "field": "firstName"
        |                }
        |              },
        |              {
        |                "exists": {
        |                  "field": "lastName"
        |                }
        |              },
        |              {
        |                "exists": {
        |                  "field": "description"
        |                }
        |              },
        |              {
        |                "range": {
        |                  "preparationTime": {
        |                    "lte": "120"
        |                  }
        |                }
        |              },
        |              {
        |                "term": {
        |                  "deliveryPeriods.dayOfWeek": {
        |                    "value": "6"
        |                  }
        |                }
        |              },
        |              {
        |                "bool": {
        |                  "must_not": [
        |                    {
        |                      "regexp": {
        |                        "blockedCustomers": {
        |                          "value": ".*?uuid.*?"
        |                        }
        |                      }
        |                    }
        |                  ]
        |                }
        |              },
        |              {
        |                "bool": {
        |                  "must_not": [
        |                    {
        |                      "term": {
        |                        "receiptOfOrdersDisabled": {
        |                          "value": true
        |                        }
        |                      }
        |                    }
        |                  ]
        |                }
        |              },
        |              {
        |                "bool": {
        |                  "should": [
        |                    {
        |                      "geo_distance": {
        |                        "distance": "7000m",
        |                        "pickup.location": [
        |                          0.0,
        |                          0.0
        |                        ]
        |                      }
        |                    },
        |                    {
        |                      "geo_distance": {
        |                        "distance": "7000m",
        |                        "withdrawals.location": [
        |                          0.0,
        |                          0.0
        |                        ]
        |                      }
        |                    }
        |                  ]
        |                }
        |              },
        |              {
        |                "nested": {
        |                  "path": "products",
        |                  "query": {
        |                    "bool": {
        |                      "filter": [
        |                        {
        |                          "term": {
        |                            "products.deleted": {
        |                              "value": false
        |                            }
        |                          }
        |                        },
        |                        {
        |                          "term": {
        |                            "products.upForSale": {
        |                              "value": true
        |                            }
        |                          }
        |                        },
        |                        {
        |                          "range": {
        |                            "products.stock": {
        |                              "gt": "0"
        |                            }
        |                          }
        |                        }
        |                      ]
        |                    }
        |                  },
        |                  "inner_hits": {
        |                    "name": "inner_products",
        |                    "from": 0,
        |                    "size": 10
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 100,
        |  "min_score": 1.0,
        |  "sort": [
        |    {
        |      "preparationTime": {
        |        "order": "asc"
        |      }
        |    },
        |    {
        |      "nbOrders": {
        |        "order": "desc"
        |      }
        |    }
        |  ],
        |  "_source": {
        |    "includes": [
        |      "inner_products.name",
        |      "inner_products.category",
        |      "inner_products.price"
        |    ]
        |  },
        |  "aggs": {
        |    "nested_products": {
        |      "nested": {
        |        "path": "products"
        |      },
        |      "aggs": {
        |        "min_price": {
        |          "min": {
        |            "field": "products.price"
        |          }
        |        },
        |        "max_price": {
        |          "max": {
        |            "field": "products.price"
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

}
