package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.Queries._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Created by smanciot on 13/04/17.
  */
class ElasticQuerySpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  "ElasticQuery" should "perform native count" in {
    val results =
      SQLQuery("select count(t.id) c2 from Table t where t.nom = \"Nom\"").aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe false
    result.aggName shouldBe "count_id"
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
        |    "count_id": {
        |      "value_count": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform count distinct" in {
    val results =
      SQLQuery("select count(distinct t.id) as c2 from Table as t where nom = \"Nom\"").aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe false
    result.distinct shouldBe true
    result.aggName shouldBe "count_distinct_id"
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
        |    "count_distinct_id": {
        |      "cardinality": {
        |        "field": "id"
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count" in {
    val results =
      SQLQuery(
        "select count(inner_emails.value) as email from index i, unnest(emails) as inner_emails where i.nom = \"Nom\""
      ).aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_count_emails_value.count_emails_value"
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
        |    "nested_count_emails_value": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_emails_value": {
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
    val results =
      SQLQuery(
        "select count(inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      ).aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_count_emails_value.count_emails_value"
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
        |    "nested_count_emails_value": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_emails_value": {
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
    val results =
      SQLQuery(
        "select count(inner_emails.value) as count_emails filter[inner_emails.context = \"profile\"] from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      ).aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "nested_count_emails_value.filtered_agg.count_emails_value"
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
        |    "nested_count_emails_value": {
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
        |            "count_emails_value": {
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
    val results =
      SQLQuery(
        "select count(distinct inner_emails.value) as count_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where ((inner_profiles.postalCode = \"33600\") and (inner_profiles.postalCode <> \"75001\"))"
      ).aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_count_distinct_emails_value.count_distinct_emails_value"
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
        |    "nested_count_distinct_emails_value": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_distinct_emails_value": {
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
    val results =
      SQLQuery(
        "select count(distinct inner_emails.value) as count_distinct_emails from index, unnest(emails) as inner_emails, unnest(profiles) as inner_profiles where inner_profiles.postalCode = \"33600\" and inner_profiles.createdDate <= \"now-35M/M\""
      ).aggregations
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "nested_count_distinct_emails_value.count_distinct_emails_value"
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
      |        "nested_count_distinct_emails_value": {
      |            "nested": {
      |                "path": "emails"
      |            },
      |            "aggs": {
      |                "count_distinct_emails_value": {
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
    val select =
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
        |limit 100""".stripMargin).search
    select.isDefined shouldBe true
    val result = select.get
    result.query shouldBe
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
      |    "size": 100,
      |    "_source": {
      |        "includes": [
      |            "profileId",
      |            "profile_ccm.email",
      |            "profile_ccm.city",
      |            "profile_ccm.firstName",
      |            "profile_ccm.lastName",
      |            "profile_ccm.postalCode",
      |            "profile_ccm.birthYear"
      |        ]
      |    }
      |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "exclude fields from select" in {
    val select =
      SQLQuery(
        except
      ).search
    select.isDefined shouldBe true
    val result = select.get
    result.query shouldBe
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
    val select =
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
           |    match(products.name, "lasagnes") AND
           |    (
           |      match(products.description, "lasagnes") OR
           |      match(products.ingredients, "lasagnes")
           |    )
           |  )
           |ORDER BY preparationTime ASC, nbOrders DESC
           |LIMIT 100""".stripMargin
      ).minScore(1.0).search
    select.isDefined shouldBe true
    val result = select.get
    println(result.query)
    result.query shouldBe
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
        |    "nested_min_products_price": {
        |      "nested": {
        |        "path": "products"
        |      },
        |      "aggs": {
        |        "min_products_price": {
        |          "min": {
        |            "field": "products.price"
        |          }
        |        }
        |      }
        |    },
        |    "nested_max_products_price": {
        |      "nested": {
        |        "path": "products"
        |      },
        |      "aggs": {
        |        "max_products_price": {
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
