package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.Queries._
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

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
      SQLQuery("select count(t.id) c2 from Table t where t.nom = 'Nom'")
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
      SQLQuery("select count(distinct t.id) as c2 from Table as t where nom = 'Nom'")
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
        "select count(inner_emails.value) as email from index i join unnest(i.emails) as inner_emails where i.nom = 'Nom'"
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "inner_emails.email"
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
        |    "inner_emails": {
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
        "select count(inner_emails.value) as count_emails from index join unnest(index.emails) as inner_emails join unnest(index.profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\"))"
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "inner_emails.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    val query = result.query.getOrElse("")
    println(query)
    query shouldBe
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
        |            "inner_hits": {
        |              "name": "inner_profiles"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "inner_emails": {
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
        "select count(inner_emails.value) as count_emails from index join unnest(index.emails) as inner_emails join unnest(index.profiles) as inner_profiles where nom = \"Nom\" and (inner_profiles.postalCode in (\"75001\",\"75002\")) having inner_emails.context = \"profile\""
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe false
    result.aggName shouldBe "inner_emails.filtered_agg.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    val query = result.query.getOrElse("")
    println(query)
    query shouldBe
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
        |            "inner_hits": {
        |              "name": "inner_profiles"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "inner_emails": {
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
        "select count(distinct inner_emails.value) as count_emails from index join unnest(index.emails) as inner_emails join unnest(index.profiles) as inner_profiles where ((inner_profiles.postalCode = \"33600\") and (inner_profiles.postalCode <> \"75001\"))"
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "inner_emails.count_emails"
    result.field shouldBe "count_emails"
    result.sources shouldBe Seq[String]("index")
    val query = result.query.getOrElse("")
    println(query)
    query shouldBe
    """{
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
        |              "name": "inner_profiles"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "inner_emails": {
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
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform nested count with date filtering" in {
    val results: Seq[ElasticAggregation] =
      SQLQuery(
        "select count(distinct inner_emails.value) as count_distinct_emails from index join unnest(index.emails) as inner_emails join unnest(index.profiles) as inner_profiles where inner_profiles.postalCode = \"33600\" and inner_profiles.createdDate <= \"now-35M/M\""
      )
    results.size shouldBe 1
    val result = results.head
    result.nested shouldBe true
    result.distinct shouldBe true
    result.aggName shouldBe "inner_emails.count_distinct_emails"
    result.field shouldBe "count_distinct_emails"
    result.sources shouldBe Seq[String]("index")
    val query = result.query.getOrElse("")
    println(query)
    query shouldBe
    """{
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
        |                    "range": {
        |                      "profiles.createdDate": {
        |                        "lte": "now-35M/M"
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "inner_profiles"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "aggs": {
        |    "inner_emails": {
        |      "nested": {
        |        "path": "emails"
        |      },
        |      "aggs": {
        |        "count_distinct_emails": {
        |          "cardinality": {
        |            "field": "emails.value"
        |          }
        |        }
        |      }
        |    }
        |  }
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
                 |FROM index join unnest(index.profiles) as profile_ccm
                 |WHERE
                 |((profile_ccm.postalCode BETWEEN "10" AND "99999")
                 |AND
                 |(profile_ccm.birthYear <= 2000))
                 |limit 100""".stripMargin)
    val query = select.query
    println(query)
    query shouldBe
    """{
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
        |                    "range": {
        |                      "profiles.postalCode": {
        |                        "gte": "10",
        |                        "lte": "99999"
        |                      }
        |                    }
        |                  },
        |                  {
        |                    "range": {
        |                      "profiles.birthYear": {
        |                        "lte": 2000
        |                      }
        |                    }
        |                  }
        |                ]
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "profile_ccm",
        |              "from": 0,
        |              "_source": {
        |                "includes": [
        |                  "profiles.email",
        |                  "profiles.postalCode",
        |                  "profiles.firstName",
        |                  "profiles.lastName",
        |                  "profiles.birthYear",
        |                  "profiles.city"
        |                ]
        |              },
        |              "size": 100
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 100,
        |  "_source": {
        |    "includes": [
        |      "profileId"
        |    ]
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
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
        |   "includes":["*"],
        |   "excludes":["col1","col2"]
        | }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "perform query with group by and having" in {
    val select: ElasticSearchRequest =
      SQLQuery(groupByWithHaving)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "Country": {
        |      "terms": {
        |        "field": "Country",
        |        "exclude": ["USA"],
        |        "min_doc_count": 1,
        |        "order": {
        |          "_key": "asc"
        |        }
        |      },
        |      "aggs": {
        |        "City": {
        |          "terms": {
        |            "field": "City",
        |            "exclude": ["Berlin"],
        |            "min_doc_count": 1,
        |            "order": {
        |              "cnt": "desc"
        |            }
        |          },
        |          "aggs": {
        |            "cnt": {
        |              "value_count": {
        |                "field": "CustomerID"
        |              }
        |            },
        |            "having_filter": {
        |              "bucket_selector": {
        |                "buckets_path": {
        |                  "cnt": "cnt"
        |                },
        |                "script": {
        |                  "source": "params.cnt > 1"
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("==", " == ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
  }

  it should "perform complex query" in {
    val select: ElasticSearchRequest =
      SQLQuery(
        s"""SELECT
           |  inner_products.category as cat,
           |  min(inner_products.price) as min_price,
           |  max(inner_products.price) as max_price
           |FROM
           |  stores store
           |  JOIN UNNEST(store.products) as inner_products
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
           |      distance(pickup.location, POINT(0.0, 0.0)) <= 7000 m OR
           |      distance(withdrawals.location, POINT(0.0, 0.0)) <= 7000 m
           |    )
           |  )
           |GROUP BY
           |  inner_products.category
           |HAVING inner_products.deleted=false AND
           |  inner_products.upForSale=true AND
           |  inner_products.stock > 0 AND
           |  match (
           |    inner_products.name,
           |    inner_products.description,
           |    inner_products.ingredients
           |  ) against ("lasagnes") AND
           |  min(inner_products.price) > 5.0 AND
           |  max(inner_products.price) < 50.0 AND
           |  inner_products.category <> "coffee"
           |  LIMIT 10""".stripMargin
      ).minScore(1.0)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
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
        |                    "lte": 120
        |                  }
        |                }
        |              },
        |              {
        |                "term": {
        |                  "deliveryPeriods.dayOfWeek": {
        |                    "value": 6
        |                  }
        |                }
        |              },
        |              {
        |                "bool": {
        |                  "must_not": [
        |                    {
        |                      "regexp": {
        |                        "blockedCustomers": {
        |                          "value": ".*uuid.*"
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
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "min_score": 1.0,
        |  "_source": false,
        |  "aggs": {
        |    "inner_products": {
        |      "nested": {
        |        "path": "products"
        |      },
        |      "aggs": {
        |        "filtered_inner_products": {
        |          "filter": {
        |            "bool": {
        |              "filter": [
        |                {
        |                  "bool": {
        |                    "must_not": [
        |                      {
        |                        "term": {
        |                          "products.category": {
        |                            "value": "coffee"
        |                          }
        |                        }
        |                      }
        |                    ]
        |                  }
        |                },
        |                {
        |                  "match_all": {}
        |                },
        |                {
        |                  "match_all": {}
        |                },
        |                {
        |                  "bool": {
        |                    "should": [
        |                      {
        |                        "match": {
        |                          "products.name": {
        |                            "query": "lasagnes"
        |                          }
        |                        }
        |                      },
        |                      {
        |                        "match": {
        |                          "products.description": {
        |                            "query": "lasagnes"
        |                          }
        |                        }
        |                      },
        |                      {
        |                        "match": {
        |                          "products.ingredients": {
        |                            "query": "lasagnes"
        |                          }
        |                        }
        |                      }
        |                    ]
        |                  }
        |                },
        |                {
        |                  "range": {
        |                    "products.stock": {
        |                      "gt": 0
        |                    }
        |                  }
        |                },
        |                {
        |                  "term": {
        |                    "products.upForSale": {
        |                      "value": true
        |                    }
        |                  }
        |                },
        |                {
        |                  "term": {
        |                    "products.deleted": {
        |                      "value": false
        |                    }
        |                  }
        |                }
        |              ]
        |            }
        |          },
        |          "aggs": {
        |            "cat": {
        |              "terms": {
        |                "field": "products.category",
        |                "size": 10,
        |                "min_doc_count": 1
        |              },
        |              "aggs": {
        |                "min_price": {
        |                  "min": {
        |                    "field": "products.price"
        |                  }
        |                },
        |                "max_price": {
        |                  "max": {
        |                    "field": "products.price"
        |                  }
        |                },
        |                "having_filter": {
        |                  "bucket_selector": {
        |                    "buckets_path": {
        |                      "min_price": "min_price",
        |                      "max_price": "max_price"
        |                    },
        |                    "script": {
        |                      "source": "params.min_price > 5.0 && params.max_price < 50.0"
        |                    }
        |                  }
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("==", " == ")
      .replaceAll("&&", " && ")
      .replaceAll("<(\\d)", " < $1")
      .replaceAll(">(\\d)", " > $1")

  }

  it should "add script fields" in {
    val select: ElasticSearchRequest =
      SQLQuery(fieldsWithInterval)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "ct": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.minus(35, ChronoUnit.MINUTES)); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("!=null", " != null")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("==", " == ")
  }

  it should "filter with date time and interval" in {
    val select: ElasticSearchRequest =
      SQLQuery(filterWithDateTimeAndInterval)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "range": {
        |            "createdAt": {
        |              "lt": "now"
        |            }
        |          }
        |        },
        |        {
        |          "range": {
        |            "createdAt": {
        |              "gte": "now-10d"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin.replaceAll("\\s", "").replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "filter with date and interval" in {
    val select: ElasticSearchRequest =
      SQLQuery(filterWithDateAndInterval)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "range": {
        |            "createdAt": {
        |              "lt": "now/d"
        |            }
        |          }
        |        },
        |        {
        |          "range": {
        |            "createdAt": {
        |              "gte": "now-10d/d"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin.replaceAll("\\s", "").replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "filter with time and interval" in {
    val select: ElasticSearchRequest =
      SQLQuery(filterWithTimeAndInterval)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "range": {
        |            "createdAt": {
        |              "lt": "now/s"
        |            }
        |          }
        |        },
        |        {
        |          "range": {
        |            "createdAt": {
        |              "gte": "now-10m/s"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll(">=", " >= ")
      .replaceAll("<", " < ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("null:", "null : ")
      .replaceAll("false:", "false : ")
      .replaceAll(":null", " : null ")
      .replaceAll("\\?", " ? ")
      .replaceAll("==", " == ")
      .replaceAll("\\);", "); ")
      .replaceAll("=\\(", " = (")
      .replaceAll("defl", "def l")
  }

  it should "handle having with date functions" in {
    val select: ElasticSearchRequest =
      SQLQuery("""SELECT userId, MAX(createdAt) as lastSeen
                 |FROM table
                 |GROUP BY userId
                 |HAVING MAX(createdAt) > now - interval 7 day""".stripMargin)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "userId": {
        |      "terms": {
        |        "field": "userId",
        |        "min_doc_count": 1
        |      },
        |      "aggs": {
        |        "lastSeen": {
        |          "max": {
        |            "field": "createdAt"
        |          }
        |        },
        |        "having_filter": {
        |          "bucket_selector": {
        |            "buckets_path": {
        |              "lastSeen": "lastSeen"
        |            },
        |            "script": {
        |              "source": "params.lastSeen > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS).toInstant().toEpochMilli()"
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
  }

  it should "handle group by with having and date time functions" in {
    val select: ElasticSearchRequest =
      SQLQuery(groupByWithHavingAndDateTimeFunctions)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "Country": {
        |      "terms": {
        |        "field": "Country",
        |        "exclude": ["USA"],
        |        "min_doc_count": 1,
        |        "order": {
        |          "_key": "asc"
        |        }
        |      },
        |      "aggs": {
        |        "City": {
        |          "terms": {
        |            "field": "City",
        |            "exclude": ["Berlin"],
        |            "min_doc_count": 1
        |          },
        |          "aggs": {
        |            "cnt": {
        |              "value_count": {
        |                "field": "CustomerID"
        |              }
        |            },
        |            "lastSeen": {
        |              "max": {
        |                "field": "createdAt"
        |              }
        |            },
        |            "having_filter": {
        |              "bucket_selector": {
        |                "buckets_path": {
        |                  "cnt": "cnt",
        |                  "lastSeen": "lastSeen"
        |                },
        |                "script": {
        |                  "source": "params.cnt > 1 && params.lastSeen > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS).toInstant().toEpochMilli()"
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
  }

  it should "handle group by index" in {
    val select: ElasticSearchRequest =
      SQLQuery(
        groupByWithHavingAndDateTimeFunctions.replace("GROUP BY Country, City", "GROUP BY 3, 2")
      )
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "Country": {
        |      "terms": {
        |        "field": "Country",
        |        "exclude": ["USA"],
        |        "min_doc_count": 1,
        |        "order": {
        |          "_key": "asc"
        |        }
        |      },
        |      "aggs": {
        |        "City": {
        |          "terms": {
        |            "field": "City",
        |            "exclude": ["Berlin"],
        |            "min_doc_count": 1
        |          },
        |          "aggs": {
        |            "cnt": {
        |              "value_count": {
        |                "field": "CustomerID"
        |              }
        |            },
        |            "lastSeen": {
        |              "max": {
        |                "field": "createdAt"
        |              }
        |            },
        |            "having_filter": {
        |              "bucket_selector": {
        |                "buckets_path": {
        |                  "cnt": "cnt",
        |                  "lastSeen": "lastSeen"
        |                },
        |                "script": {
        |                  "source": "params.cnt > 1 && params.lastSeen > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS).toInstant().toEpochMilli()"
        |                }
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
  }

  it should "handle date_parse function" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateParse)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "identifier": {
        |      "terms": {
        |        "field": "identifier",
        |        "min_doc_count": 1,
        |        "order": {
        |          "ct": "desc"
        |        }
        |      },
        |      "aggs": {
        |        "ct": {
        |          "value_count": {
        |            "field": "identifier2"
        |          }
        |        },
        |        "lastSeen": {
        |          "max": {
        |            "field": "createdAt",
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); (param1 == null) ? null : LocalDate.parse(param1, DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))"
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll(",ChronoUnit", ", ChronoUnit")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll(",DateTimeFormatter", ", DateTimeFormatter")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(">", " > ")
      .replaceAll(",LocalDate", ", LocalDate")
  }

  it should "handle date_format function" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateFormat)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "y": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.withDayOfYear(1).truncatedTo(ChronoUnit.DAYS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "q": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value); def param2 = param1 != null ? param1.withMonth((((param1.getMonthValue() - 1) / 3) * 3) + 1).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS) : null; def param3 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param3.format(param2)"
        |      }
        |    },
        |    "m": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "w": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.with(DayOfWeek.SUNDAY).truncatedTo(ChronoUnit.DAYS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "d": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.truncatedTo(ChronoUnit.DAYS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "h": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.truncatedTo(ChronoUnit.HOURS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "m2": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.truncatedTo(ChronoUnit.MINUTES)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    },
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.truncatedTo(ChronoUnit.SECONDS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll(",ChronoUnit", ", ChronoUnit")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll("==", " == ")
      .replaceAll("-(\\d)", " - $1")
      .replaceAll("\\+", " + ")
      .replaceAll("/", " / ")
      .replaceAll("\\*", " * ")
      .replaceAll("=p", " = p")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(">", " > ")
      .replaceAll(",LocalDate", ", LocalDate")
  }

  it should "handle datetime_parse function" in { // #25
    val select: ElasticSearchRequest =
      SQLQuery(dateTimeParse)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "identifier": {
        |      "terms": {
        |        "field": "identifier",
        |        "min_doc_count": 1,
        |        "order": {
        |          "ct": "desc"
        |        }
        |      },
        |      "aggs": {
        |        "ct": {
        |          "value_count": {
        |            "field": "identifier2"
        |          }
        |        },
        |        "lastSeen": {
        |          "max": {
        |            "field": "createdAt",
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); (param1 == null) ? null : ZonedDateTime.parse(param1, DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSS XXX\")).truncatedTo(ChronoUnit.MINUTES).get(ChronoField.YEAR)"
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(">", " > ")
      .replaceAll(",ZonedDateTime", ", ZonedDateTime")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll(",DateTimeFormatter", ", DateTimeFormatter")
      .replaceAll("SSSXXX", "SSS XXX")
      .replaceAll("ddHH", "dd HH")
  }

  it should "handle datetime_format function" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateTimeFormat)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); def param2 = DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss XXX\"); (param1 == null) ? null : param2.format(param1)"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(">", " > ")
      .replaceAll(",ZonedDateTime", ", ZonedDateTime")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll("SSSXXX", "SSS XXX")
      .replaceAll("ddHH", "dd HH")
      .replaceAll("XXX", " XXX")
  }

  it should "handle date_diff function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateDiff)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "diff": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['updatedAt'].size() == 0 ? null : doc['updatedAt'].value); def param2 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); (param1 == null || param2 == null) ? null : ChronoUnit.DAYS.between(param1, param2)"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(",p", ", p")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
  }

  it should "handle aggregation with date_diff function" in {
    val select: ElasticSearchRequest =
      SQLQuery(aggregationWithDateDiff)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "identifier": {
        |      "terms": {
        |        "field": "identifier",
        |        "min_doc_count": 1
        |      },
        |      "aggs": {
        |        "max_diff": {
        |          "max": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['updatedAt'].size() == 0 ? null : doc['updatedAt'].value); def param2 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); def param3 = (param2 == null) ? null : ZonedDateTime.parse(param2, DateTimeFormatter.ofPattern(\"yyyy-MM-dd HH:mm:ss.SSS XXX\")); (param1 == null || param2 == null) ? null : ChronoUnit.DAYS.between(param1, param3)"
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(",p", ", p")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll(",DateTimeFormatter", ", DateTimeFormatter")
      .replaceAll("SSSXXX", "SSS XXX")
      .replaceAll("ddHH", "dd HH")
  }

  it should "handle date_add function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateAdd)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.plus(10, ChronoUnit.DAYS)); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "handle date_sub function as script field" in { // 30
    val select: ElasticSearchRequest =
      SQLQuery(dateSub)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.minus(10, ChronoUnit.DAYS)); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "handle datetime_add function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateTimeAdd)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.plus(10, ChronoUnit.DAYS)); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "handle datetime_sub function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(dateTimeSub)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier2"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "lastSeen": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.minus(10, ChronoUnit.DAYS)); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
  }

  it should "handle is_null function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(isnull)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "flag": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); param1 == null"
        |      }
        |    }
        |  },
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":false", " : false")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
  }

  it should "handle is_notnull function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(isnotnull)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "flag": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); param1 != null"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defs", "def s")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":true", " : true")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
  }

  it should "handle is_null criteria as must_not exists" in {
    val select: ElasticSearchRequest =
      SQLQuery(isNullCriteria)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "bool": {
        |            "must_not": [
        |              {
        |                "exists": {
        |                  "field": "identifier"
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "handle is_notnull criteria as exists" in {
    val select: ElasticSearchRequest =
      SQLQuery(isNotNullCriteria)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "exists": {
        |            "field": "identifier"
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "handle coalesce function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(coalesce)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "c": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.minus(35, ChronoUnit.MINUTES)); def param2 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); param1 != null ? param1 : param2"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll(";defp", "; defp")
      .replaceAll("defp", "def p")
      .replaceAll("defv", " def v")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll(";}", "; }")
      .replaceAll(";e", "; e")
      .replaceAll(";v", "; v")
      .replaceAll(":p", " : p")
      .replaceAll(";p", "; p")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll(":ZonedDateTime", " : ZonedDateTime")
  }

  it should "handle nullif function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(nullif)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "c": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.toLocalDate()); def param2 = LocalDate.parse(\"2025-09-11\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).minus(2, ChronoUnit.DAYS); def param3 = param1 == null || param1.isEqual(param2) ? null : param1; def param4 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); param3 != null ? param3 : param4"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defv", " def v")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";def", "; def")
      .replaceAll(";return", "; return")
      .replaceAll(";v", "; v")
      .replaceAll("(\\d)=p", "$1 = p")
      .replaceAll(";p", "; p")
      .replaceAll(":p", " : p")
      .replaceAll("returnv", " return v")
      .replaceAll("returne", " return e")
      .replaceAll(";}", "; }")
      .replaceAll(";\\(", "; (")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll(",LocalDate", ", LocalDate")
      .replaceAll("=LocalDate", " = LocalDate")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll(",DateTimeFormatter", ", DateTimeFormatter")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll(":ZonedDateTime", " : ZonedDateTime")
  }

  it should "handle cast function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(conversion)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "c": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.toLocalDate()); def param2 = LocalDate.parse(\"2025-09-11\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); def param3 = param1 == null || param1.isEqual(param2) ? null : param1; def param4 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().minus(2, ChronoUnit.HOURS); try { param3 != null ? param3 : param4 } catch (Exception e) { return null; }"
        |      }
        |    },
        |    "c2": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = ZonedDateTime.now(ZoneId.of('Z')); param1.toInstant().toEpochMilli()"
        |      }
        |    },
        |    "c3": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = ZonedDateTime.now(ZoneId.of('Z')); param1.toLocalDate()"
        |      }
        |    },
        |    "c4": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "Long.parseLong(\"125\").longValue()"
        |      }
        |    },
        |    "c5": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = LocalDate.parse(\"2025-09-11\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); param1"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll(";defp", "; defp")
      .replaceAll("defp", "def p")
      .replaceAll("defv", " def v")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll(",LocalDate", ", LocalDate")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll("try\\{", "try {")
      .replaceAll("}catch", "} catch ")
      .replaceAll("Exceptione\\)", "Exception e) ")
      .replaceAll(",DateTimeFormatter", ", DateTimeFormatter")
      .replaceAll("(\\d)=p", "$1 = p")
      .replaceAll(";p", "; p")
      .replaceAll(":p", " : p")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll("=LocalDate", " = LocalDate")
      .replaceAll(":ZonedDateTime", " : ZonedDateTime")
      .replaceAll("try \\{", "try { ")
      .replaceAll("} catch", " } catch")
  }

  it should "handle case function as script field" in { // 40
    val select: ElasticSearchRequest =
      SQLQuery(caseWhen)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "c": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value); def param2 = ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS); def param3 = param1 == null ? false : (param1.isAfter(param2)); def param4 = (doc['lastSeen'].size() == 0 ? null : doc['lastSeen'].value.plus(2, ChronoUnit.DAYS)); def param5 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); param3 ? param1 : param4 != null ? param4 : param5"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defd", " def d")
      .replaceAll("defe", " def e")
      .replaceAll("defl", " def l")
      .replaceAll("if\\(", "if (")
      .replaceAll("\\{if", "{ if")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("false:", "false : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll(";if", "; if")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll(">", " > ")
      .replaceAll("if \\(\\s*def", "if (def")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll("=p", " = p")
      .replaceAll(":p", " : p")
  }

  it should "handle case with expression function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(caseWhenExpr)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "c": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().minus(7, ChronoUnit.DAYS); def param2 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.toLocalDate().minus(3, ChronoUnit.DAYS)); def param3 = (doc['lastSeen'].size() == 0 ? null : doc['lastSeen'].value.toLocalDate().plus(2, ChronoUnit.DAYS)); def param4 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.toLocalDate()); param1 != null && param1.isEqual(param2) ? param2 : param1 != null && param1.isEqual(param3) ? param3 : param4"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defd", " def d")
      .replaceAll("defe", " def e")
      .replaceAll("defl", " def l")
      .replaceAll("if\\(", "if (")
      .replaceAll("\\{if", "{ if")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("false:", "false : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll(";if", "; if")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll(">", " > ")
      .replaceAll("if \\(\\s*def", "if (def")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll("=e", " = e")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll("=p", " = p")
      .replaceAll(":p", " : p")
  }

  it should "handle extract function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(extract)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "dom": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.DAY_OF_MONTH)); param1"
        |      }
        |    },
        |    "dow": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.DAY_OF_WEEK)); param1"
        |      }
        |    },
        |    "doy": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.DAY_OF_YEAR)); param1"
        |      }
        |    },
        |    "m": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MONTH_OF_YEAR)); param1"
        |      }
        |    },
        |    "y": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.YEAR)); param1"
        |      }
        |    },
        |    "h": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.HOUR_OF_DAY)); param1"
        |      }
        |    },
        |    "minutes": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MINUTE_OF_HOUR)); param1"
        |      }
        |    },
        |    "s": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.SECOND_OF_MINUTE)); param1"
        |      }
        |    },
        |    "nano": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.NANO_OF_SECOND)); param1"
        |      }
        |    },
        |    "micro": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MICRO_OF_SECOND)); param1"
        |      }
        |    },
        |    "milli": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MILLI_OF_SECOND)); param1"
        |      }
        |    },
        |    "epoch": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.EPOCH_DAY)); param1"
        |      }
        |    },
        |    "off": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.OFFSET_SECONDS)); param1"
        |      }
        |    },
        |    "w": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)); param1"
        |      }
        |    },
        |    "q": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(java.time.temporal.IsoFields.QUARTER_OF_YEAR)); param1"
        |      }
        |    }
        |  },
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll(";if", "; if")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll(">", " > ")
      .replaceAll("if \\(\\s*def", "if (def")
  }

  it should "handle arithmetic function as script field and condition" in {
    val select: ElasticSearchRequest =
      SQLQuery(arithmetic.replace("as group1", ""))
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); def param2 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().get(ChronoField.YEAR); (param1 == null) ? null : (param1 * (param2 - 10)) > 10000"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "add": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 + 1)"
        |      }
        |    },
        |    "sub": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 - 1)"
        |      }
        |    },
        |    "mul": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 * 2)"
        |      }
        |    },
        |    "div": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 / 2)"
        |      }
        |    },
        |    "mod": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 % 2)"
        |      }
        |    },
        |    "identifier_mul_identifier2_minus_10": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); def param2 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); def lv0 = ((param1 == null || param2 == null) ? null : (param1 * param2)); (lv0 == null) ? null : (lv0 - 10)"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defp", "def p")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("defr", "def r")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      // .replaceAll("(\\d)=", "$1 =")
      .replaceAll("=ZonedDateTime", " = ZonedDateTime")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll(">", " > ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll("%", " % ")
      .replaceAll("\\+", " + ")
      .replaceAll("-", " - ")
      .replaceAll("==", " == ")
      .replaceAll("\\|\\|", " || ")
  }

  it should "handle mathematic function as script field and condition" in {
    val select: ElasticSearchRequest =
      SQLQuery(mathematical)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.sqrt(param1) > 100.0"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "abs_identifier_plus_1_0_mul_2": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); ((param1 == null) ? null : Math.abs(param1) + 1.0) * ((double) 2)"
        |      }
        |    },
        |    "ceil_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.ceil(param1)"
        |      }
        |    },
        |    "floor_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.floor(param1)"
        |      }
        |    },
        |    "sqrt_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.sqrt(param1)"
        |      }
        |    },
        |    "exp_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.exp(param1)"
        |      }
        |    },
        |    "log_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.log(param1)"
        |      }
        |    },
        |    "log10_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.log10(param1)"
        |      }
        |    },
        |    "pow_identifier_3": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.pow(param1, 3)"
        |      }
        |    },
        |    "round_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); def param2 = Math.pow(10, 0); (param1 == null || param2 == null) ? null : Math.round((param1 * param2) / param2)"
        |      }
        |    },
        |    "round_identifier_2": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); def param2 = Math.pow(10, 2); (param1 == null || param2 == null) ? null : Math.round((param1 * param2) / param2)"
        |      }
        |    },
        |    "sign_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : (param1 > 0 ? 1 : (param1 < 0 ? -1 : 0))"
        |      }
        |    },
        |    "cos_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.cos(param1)"
        |      }
        |    },
        |    "acos_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.acos(param1)"
        |      }
        |    },
        |    "sin_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.sin(param1)"
        |      }
        |    },
        |    "asin_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.asin(param1)"
        |      }
        |    },
        |    "tan_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.tan(param1)"
        |      }
        |    },
        |    "atan_identifier": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.atan(param1)"
        |      }
        |    },
        |    "atan2_identifier_3_0": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier'].size() == 0 ? null : doc['identifier'].value); (param1 == null) ? null : Math.atan2(param1, 3.0)"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defp", "def p")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(":0", " : 0")
      .replaceAll("=Math", " = Math")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("between\\(s,", "between(s, ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll(">", " > ")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll(",LocalDate", ", LocalDate")
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll("\\(double\\)(\\d)", "(double) $1")
  }

  it should "handle string function as script field and condition" in { // 45
    val select: ElasticSearchRequest =
      SQLQuery(string)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.trim().length() > 10"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "len": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.length()"
        |      }
        |    },
        |    "low": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.toLowerCase()"
        |      }
        |    },
        |    "upp": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.toUpperCase()"
        |      }
        |    },
        |    "sub": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.substring(0, Math.min(3, param1.length()))"
        |      }
        |    },
        |    "tr": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.trim()"
        |      }
        |    },
        |    "ltr": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.replaceAll(\"^\\\\s+\",\"\")"
        |      }
        |    },
        |    "rtr": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.replaceAll(\"\\\\s+$\",\"\")"
        |      }
        |    },
        |    "con": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : String.valueOf(param1) + \"_test\" + String.valueOf(1)"
        |      }
        |    },
        |    "l": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.substring(0, Math.min(5, param1.length()))"
        |      }
        |    },
        |    "r": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.substring(param1.length() - Math.min(3, param1.length()))"
        |      }
        |    },
        |    "rep": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.replace(\"el\", \"le\")"
        |      }
        |    },
        |    "rev": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : new StringBuilder(param1).reverse().toString()"
        |      }
        |    },
        |    "pos": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : param1.indexOf(\"soft\", 0) + 1"
        |      }
        |    },
        |    "reg": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['identifier2'].size() == 0 ? null : doc['identifier2'].value); (param1 == null) ? null : java.util.regex.Pattern.compile(\"soft\", java.util.regex.Pattern.CASE_INSENSITIVE | java.util.regex.Pattern.MULTILINE).matcher(param1).find()"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(":0", " : 0")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+(\\d)", " + $1")
      .replaceAll("\\)\\+", ") + ")
      .replaceAll("\\+String", " + String")
      .replaceAll("-", " - ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll(">", " > ")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(";\\s\\s", "; ")
      .replaceAll("false:", "false : ")
      .replaceAll("(\\d),", "$1, ")
      .replaceAll(":(\\d)", " : $1")
      .replaceAll("new", "new ")
      .replaceAll(""",\\"le""", """, \\"le""")
      .replaceAll(":arg", " : arg")
      .replaceAll(",java", ", java")
      .replaceAll("\\|java", " | java")
  }

  it should "handle top hits aggregation" in {
    val select: ElasticSearchRequest =
      SQLQuery(topHits)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "script_fields": {
        |    "hire_date": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['hire_date'].size() == 0 ? null : doc['hire_date'].value.toLocalDate()); param1"
        |      }
        |    }
        |  },
        |  "_source": false,
        |  "aggs": {
        |    "dept": {
        |      "terms": {
        |        "field": "department",
        |        "min_doc_count": 1
        |      },
        |      "aggs": {
        |        "cnt": {
        |          "cardinality": {
        |            "field": "salary"
        |          }
        |        },
        |        "first_salary": {
        |          "top_hits": {
        |            "size": 1,
        |            "sort": [
        |              {
        |                "hire_date": {
        |                  "order": "asc"
        |                }
        |              }
        |            ],
        |            "_source": {
        |              "includes": [
        |                "salary",
        |                "firstName"
        |              ]
        |            }
        |          }
        |        },
        |        "last_salary": {
        |          "top_hits": {
        |            "size": 1,
        |            "sort": [
        |              {
        |                "hire_date": {
        |                  "order": "desc"
        |                }
        |              }
        |            ],
        |            "_source": {
        |              "includes": [
        |                "salary",
        |                "firstName"
        |              ]
        |            }
        |          }
        |        },
        |        "employees": {
        |          "top_hits": {
        |            "size": 1000,
        |            "sort": [
        |              {
        |                "hire_date": {
        |                  "order": "asc"
        |                }
        |              },
        |              {
        |                "salary": {
        |                  "order": "desc"
        |                }
        |              }
        |            ],
        |            "_source": {
        |              "includes": [
        |                "name"
        |              ]
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll("-", " - ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll(">", " > ")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
  }

  it should "handle last day function" in {
    val select: ElasticSearchRequest =
      SQLQuery(lastDay)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "def param1 = ZonedDateTime.now(ZoneId.of('Z')); param1.toLocalDate().withDayOfMonth(param1.toLocalDate().lengthOfMonth()).get(ChronoField.DAY_OF_MONTH) > 28"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "ld": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.toLocalDate()); (param1 == null) ? null : param1.withDayOfMonth(param1.lengthOfMonth())"
        |      }
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "identifier"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll("-", " - ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll(">", " > ")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
  }

  it should "handle all extractors" in {
    val select: ElasticSearchRequest =
      SQLQuery(extractors)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "script_fields": {
        |    "y": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.YEAR)); param1"
        |      }
        |    },
        |    "m": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MONTH_OF_YEAR)); param1"
        |      }
        |    },
        |    "wd": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value); (param1 == null) ? null : (param1.get(ChronoField.DAY_OF_WEEK) + 6) % 7"
        |      }
        |    },
        |    "yd": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.DAY_OF_YEAR)); param1"
        |      }
        |    },
        |    "d": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.DAY_OF_MONTH)); param1"
        |      }
        |    },
        |    "h": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.HOUR_OF_DAY)); param1"
        |      }
        |    },
        |    "minutes": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MINUTE_OF_HOUR)); param1"
        |      }
        |    },
        |    "s": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.SECOND_OF_MINUTE)); param1"
        |      }
        |    },
        |    "nano": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.NANO_OF_SECOND)); param1"
        |      }
        |    },
        |    "micro": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MICRO_OF_SECOND)); param1"
        |      }
        |    },
        |    "milli": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.MILLI_OF_SECOND)); param1"
        |      }
        |    },
        |    "epoch": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.EPOCH_DAY)); param1"
        |      }
        |    },
        |    "off": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(ChronoField.OFFSET_SECONDS)); param1"
        |      }
        |    },
        |    "w": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(java.time.temporal.IsoFields.WEEK_OF_WEEK_BASED_YEAR)); param1"
        |      }
        |    },
        |    "q": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "def param1 = (doc['createdAt'].size() == 0 ? null : doc['createdAt'].value.get(java.time.temporal.IsoFields.QUARTER_OF_YEAR)); param1"
        |      }
        |    }
        |  },
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll("-", " - ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll("%", " % ")
      .replaceAll(">", " > ")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
  }

  it should "handle geo distance as script fields and criteria" in {
    val select: ElasticSearchRequest =
      SQLQuery(geoDistance)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "bool": {
        |            "must": [
        |              {
        |                "script": {
        |                  "script": {
        |                    "lang": "painless",
        |                    "source": "(def arg0 = (doc['toLocation'].size() == 0 ? null : doc['toLocation']); (arg0 == null) ? null : arg0.arcDistance(params.lat, params.lon)) >= 4000000.0",
        |                    "params": {
        |                      "lat": -70.0,
        |                      "lon": 40.0
        |                    }
        |                  }
        |                }
        |              },
        |              {
        |                "geo_distance": {
        |                  "distance": "5000km",
        |                  "toLocation": [
        |                    40.0,
        |                    -70.0
        |                  ]
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "(def arg0 = (doc['fromLocation'].size() == 0 ? null : doc['fromLocation']); def arg1 = (doc['toLocation'].size() == 0 ? null : doc['toLocation']); (arg0 == null || arg1 == null) ? null : arg0.arcDistance(arg1.lat, arg1.lon)) < 2000000.0"
        |            }
        |          }
        |        },
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "0.0 < 1000000.0"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "script_fields": {
        |    "d1": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "(def arg0 = (doc['toLocation'].size() == 0 ? null : doc['toLocation']); (arg0 == null) ? null : arg0.arcDistance(params.lat, params.lon))",
        |        "params": {
        |          "lat": -70.0,
        |          "lon": 40.0
        |        }
        |      }
        |    },
        |    "d2": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "(def arg0 = (doc['fromLocation'].size() == 0 ? null : doc['fromLocation']); (arg0 == null) ? null : arg0.arcDistance(params.lat, params.lon))",
        |        "params": {
        |          "lat": -70.0,
        |          "lon": 40.0
        |        }
        |      }
        |    },
        |    "d3": {
        |      "script": {
        |        "lang": "painless",
        |        "source": "8318612.0"
        |      }
        |    }
        |  },
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defv", " def v")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll("\\*", " * ")
      .replaceAll("/", " / ")
      .replaceAll(">(\\d)", " > $1")
      .replaceAll("=(\\d)", "= $1")
      .replaceAll(">=", " >=")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
      .replaceAll(",params", ", params")
      .replaceAll("GeoPoint", " GeoPoint")
      .replaceAll("lat,arg", "lat, arg")
  }

  it should "handle between with temporal" in { // 50
    val select: ElasticSearchRequest =
      SQLQuery(betweenTemporal)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "range": {
        |            "createdAt": {
        |              "gte": "now-1M/d",
        |              "lte": "now/d"
        |            }
        |          }
        |        },
        |        {
        |          "bool": {
        |            "must": [
        |              {
        |                "script": {
        |                  "script": {
        |                    "lang": "painless",
        |                    "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.toLocalDate()); def param2 = LocalDate.parse(\"2025-09-11\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); param1 == null ? false : (param1.isBefore(param2.withDayOfMonth(param2.lengthOfMonth())) == false)"
        |                  }
        |                }
        |              },
        |              {
        |                "range": {
        |                  "lastUpdated": {
        |                    "lte": "now/d"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "_source": {
        |    "includes": [
        |      "*"
        |    ]
        |  }
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll(">=", " >= ")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll(">(\\d)", " > $1")
      .replaceAll("=(\\d)", "= $1")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
      .replaceAll(",params", ", params")
      .replaceAll("GeoPoint", " GeoPoint")
      .replaceAll("lat,arg", "lat, arg")
      .replaceAll("false:", "false : ")
      .replaceAll("DateTimeFormatter", " DateTimeFormatter")
  }

  it should "handle nested of nested" in {
    val select: ElasticSearchRequest =
      SQLQuery(nestedOfNested)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "nested": {
        |            "path": "comments",
        |            "query": {
        |              "nested": {
        |                "path": "comments.replies",
        |                "query": {
        |                  "bool": {
        |                    "filter": [
        |                      {
        |                        "match": {
        |                          "comments.content": {
        |                            "query": "Nice"
        |                          }
        |                        }
        |                      },
        |                      {
        |                        "script": {
        |                          "script": {
        |                            "lang": "painless",
        |                            "source": "def param1 = (doc['comments.replies.lastUpdated'].size() == 0 ? null : doc['comments.replies.lastUpdated'].value.toLocalDate()); def param2 = LocalDate.parse(\"2025-09-10\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); param1 == null ? false : (param1.isBefore(param2.withDayOfMonth(param2.lengthOfMonth())))"
        |                          }
        |                        }
        |                      }
        |                    ]
        |                  }
        |                },
        |                "inner_hits": {
        |                  "name": "matched_replies",
        |                  "from": 0,
        |                  "_source": {
        |                    "includes": [
        |                      "comments.replies.reply_author",
        |                      "comments.replies.reply_text"
        |                    ]
        |                  },
        |                  "size": 5
        |                }
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "matched_comments",
        |              "from": 0,
        |              "_source": {
        |                "includes": [
        |                  "comments.author",
        |                  "comments.comments"
        |                ]
        |              },
        |              "size": 5
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 5,
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll(">=", " >= ")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll(">(\\d)", " > $1")
      .replaceAll("=(\\d)", "= $1")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
      .replaceAll(",params", ", params")
      .replaceAll("GeoPoint", " GeoPoint")
      .replaceAll("lat,arg", "lat, arg")
      .replaceAll("false:", "false : ")
      .replaceAll("DateTimeFormatter", " DateTimeFormatter")
  }

  it should "handle predicate with distinct nested" in {
    val select: ElasticSearchRequest =
      SQLQuery(predicateWithDistinctNested)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "bool": {
        |            "must_not": [
        |              {
        |                "nested": {
        |                  "path": "replies",
        |                  "query": {
        |                    "script": {
        |                      "script": {
        |                        "lang": "painless",
        |                        "source": "def param1 = (doc['replies.lastUpdated'].size() == 0 ? null : doc['replies.lastUpdated'].value.toLocalDate()); def param2 = LocalDate.parse(\"2025-09-10\", DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); param1 == null ? false : (param1.isBefore(param2.withDayOfMonth(param2.lengthOfMonth())))"
        |                      }
        |                    }
        |                  },
        |                  "inner_hits": {
        |                    "name": "matched_replies",
        |                    "from": 0,
        |                    "_source": {
        |                      "includes": [
        |                        "replies.reply_author",
        |                        "replies.reply_text"
        |                      ]
        |                    },
        |                    "size": 5
        |                  }
        |                }
        |              }
        |            ],
        |            "filter": [
        |              {
        |                "nested": {
        |                  "path": "comments",
        |                  "query": {
        |                    "match": {
        |                      "comments.content": {
        |                        "query": "Nice"
        |                      }
        |                    }
        |                  },
        |                  "inner_hits": {
        |                    "name": "matched_comments",
        |                    "from": 0,
        |                    "_source": {
        |                      "includes": [
        |                        "comments.author",
        |                        "comments.comments"
        |                      ]
        |                    },
        |                    "size": 5
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
        |  "size": 5,
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll(">=", " >= ")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll(">(\\d)", " > $1")
      .replaceAll("=(\\d)", "= $1")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
      .replaceAll(",params", ", params")
      .replaceAll("GeoPoint", " GeoPoint")
      .replaceAll("lat,arg", "lat, arg")
      .replaceAll("false:", "false : ")
      .replaceAll("DateTimeFormatter", " DateTimeFormatter")
  }

  it should "handle nested without criteria" in {
    val select: ElasticSearchRequest =
      SQLQuery(nestedWithoutCriteria)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "bool": {
        |            "filter": [
        |              {
        |                "script": {
        |                  "script": {
        |                    "lang": "painless",
        |                    "source": "def param1 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.toLocalDate()); def param2 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); param1 == null ? false : (param1.isBefore(param2))"
        |                  }
        |                }
        |              }
        |            ]
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "comments",
        |            "query": {
        |              "nested": {
        |                "path": "comments.replies",
        |                "query": {
        |                  "match_all": {}
        |                },
        |                "inner_hits": {
        |                  "name": "matched_replies",
        |                  "from": 0,
        |                  "_source": {
        |                    "includes": [
        |                      "reply_author",
        |                      "reply_text"
        |                    ]
        |                  },
        |                  "size": 5
        |                }
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "matched_comments",
        |              "from": 0,
        |              "_source": {
        |                "includes": [
        |                  "author",
        |                  "comments"
        |                ]
        |              },
        |              "size": 5
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "from": 0,
        |  "size": 5,
        |  "_source": true
        |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("\\s+", "")
      .replaceAll("defp", "def p")
      .replaceAll("defa", "def a")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("def_", "def _")
      .replaceAll("=_", " = _")
      .replaceAll(",_", ", _")
      .replaceAll(",\\(", ", (")
      .replaceAll("if\\(", "if (")
      .replaceAll(">=", " >= ")
      .replaceAll("=\\(", " = (")
      .replaceAll(":\\(", " : (")
      .replaceAll(",(\\d)", ", $1")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll("; if", ";if")
      .replaceAll("==", " == ")
      .replaceAll("\\+", " + ")
      .replaceAll(">(\\d)", " > $1")
      .replaceAll("=(\\d)", "= $1")
      .replaceAll("<", " < ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("(\\d)=", "$1 = ")
      .replaceAll(",params", ", params")
      .replaceAll("GeoPoint", " GeoPoint")
      .replaceAll("lat,arg", "lat, arg")
      .replaceAll("false:", "false : ")
      .replaceAll("DateTimeFormatter", " DateTimeFormatter")
  }

  it should "determine the aggregation context" in {
    val select: ElasticSearchRequest =
      SQLQuery(determinationOfTheAggregationContext)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |    "query": {
        |        "match_all": {}
        |    },
        |    "size": 0,
        |    "_source": false,
        |    "aggs": {
        |        "avg_popularity": {
        |            "avg": {
        |                "field": "popularity"
        |            }
        |        },
        |        "comments": {
        |            "nested": {
        |                "path": "comments"
        |            },
        |            "aggs": {
        |                "avg_comment_likes": {
        |                    "avg": {
        |                        "field": "comments.likes"
        |                    }
        |                }
        |            }
        |        }
        |    }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "handle aggregation with nested of nested context" in {
    val select: ElasticSearchRequest =
      SQLQuery(aggregationWithNestedOfNestedContext)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "match_all": {}
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "comments": {
        |      "nested": {
        |        "path": "comments"
        |      },
        |      "aggs": {
        |        "replies": {
        |          "nested": {
        |            "path": "comments.replies"
        |          },
        |          "aggs": {
        |            "avg_reply_likes": {
        |              "avg": {
        |                "field": "comments.replies.likes"
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

  it should "handle where filters according to scope" in {
    val select: ElasticSearchRequest =
      SQLQuery(whereFiltersAccordingToScope)
    val query = select.query
    println(query)
    query shouldBe
    """{
        |  "query": {
        |    "bool": {
        |      "filter": [
        |        {
        |          "term": {
        |            "status": {
        |              "value": "active"
        |            }
        |          }
        |        },
        |        {
        |          "nested": {
        |            "path": "comments",
        |            "query": {
        |              "term": {
        |                "comments.sentiment": {
        |                  "value": "positive"
        |                }
        |              }
        |            },
        |            "inner_hits": {
        |              "name": "comments"
        |            }
        |          }
        |        }
        |      ]
        |    }
        |  },
        |  "size": 0,
        |  "_source": false,
        |  "aggs": {
        |    "comments": {
        |      "nested": {
        |        "path": "comments"
        |      },
        |      "aggs": {
        |        "filtered_comments": {
        |          "filter": {
        |            "bool": {
        |              "filter": [
        |                {
        |                  "term": {
        |                    "comments.sentiment": {
        |                      "value": "positive"
        |                    }
        |                  }
        |                }
        |              ]
        |            }
        |          },
        |          "aggs": {
        |            "nb_comments": {
        |              "value_count": {
        |                "field": "comments.id"
        |              }
        |            }
        |          }
        |        }
        |      }
        |    }
        |  }
        |}""".stripMargin.replaceAll("\\s+", "")
  }

}
