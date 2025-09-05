package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.Queries._
import com.google.gson.{JsonArray, JsonObject, JsonParser, JsonPrimitive}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

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
      |                                                "lte": 2000
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
      |  "_source": true,
      |  "aggs": {
      |    "filtered_agg": {
      |      "filter": {
      |        "bool": {
      |          "filter": [
      |            {
      |              "bool": {
      |                "must_not": [
      |                  {
      |                    "term": {
      |                      "Country": {
      |                        "value": "USA"
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            {
      |              "bool": {
      |                "must_not": [
      |                  {
      |                    "term": {
      |                      "City": {
      |                        "value": "Berlin"
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            {
      |              "match_all": {}
      |            }
      |          ]
      |        }
      |      },
      |      "aggs": {
      |        "Country": {
      |          "terms": {
      |            "field": "Country.keyword",
      |            "order": {
      |              "Country": "asc"
      |            }
      |          },
      |          "aggs": {
      |            "City": {
      |              "terms": {
      |                "field": "City.keyword",
      |                "order": {
      |                  "cnt": "desc"
      |                }
      |              },
      |              "aggs": {
      |                "cnt": {
      |                  "value_count": {
      |                    "field": "CustomerID"
      |                  }
      |                },
      |                "having_filter": {
      |                  "bucket_selector": {
      |                    "buckets_path": {
      |                      "cnt": "cnt"
      |                    },
      |                    "script": {
      |                      "source": "1 == 1 && 1 == 1 && params.cnt > 1"
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
           |    )
           |  )
           |GROUP BY
           |  inner_products.category
           |HAVING inner_products.deleted=false AND
           |  inner_products.upForSale=true AND
           |  inner_products.stock > 0 AND
           |  match (inner_products.name) against ("lasagnes") AND
           |  match (inner_products.description, inner_products.ingredients) against ("lasagnes") AND
           |  min(inner_products.price) > 5.0 AND
           |  max(inner_products.price) < 50.0 AND
           |  inner_products.category <> "coffee"""".stripMargin
      ).minScore(1.0)
    val query = select.query
    println(query)
    query shouldBe
    """
        |{
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
        |  "_source": true,
        |  "aggs": {
        |    "nested_products": {
        |      "nested": {
        |        "path": "products"
        |      },
        |      "aggs": {
        |        "filtered_agg": {
        |          "filter": {
        |            "bool": {
        |              "filter": [
        |                {
        |                  "term": {
        |                    "products.deleted": {
        |                      "value": false
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
        |                  "range": {
        |                    "products.stock": {
        |                      "gt": 0
        |                    }
        |                  }
        |                },
        |                {
        |                  "match": {
        |                    "products.name": {
        |                      "query": "lasagnes"
        |                    }
        |                  }
        |                },
        |                {
        |                  "bool": {
        |                    "should": [
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
        |                  "match_all": {}
        |                },
        |                {
        |                  "match_all": {}
        |                },
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
        |                }
        |              ]
        |            }
        |          },
        |          "aggs": {
        |            "cat": {
        |              "terms": {
        |                "field": "products.category.keyword"
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
        |                      "source": "1 == 1 && 1 == 1 && 1 == 1 && 1 == 1 && 1 == 1 && params.min_price > 5.0 && params.max_price < 50.0 && 1 == 1"
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
      .replaceAll("<", " < ")
      .replaceAll(">", " > ")

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
      |        "source": "doc['createdAt'].value.minus(35, ChronoUnit.MINUTES)"
      |      }
      |    }
      |  },
      |  "_source": {
      |    "includes": ["identifier"]
      |  }
      |}""".stripMargin.replaceAll("\\s", "").replaceAll("ChronoUnit", " ChronoUnit")
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
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "return doc['createdAt'].value.toLocalTime() < LocalTime.now();"
        |            }
        |          }
        |        },
        |        {
        |          "script": {
        |            "script": {
        |              "lang": "painless",
        |              "source": "return doc['createdAt'].value.toLocalTime() >= LocalTime.now().minus(10, ChronoUnit.MINUTES);"
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
      .replaceAll("return", "return ")
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
      |  "_source": true,
      |  "aggs": {
      |    "filtered_agg": {
      |      "filter": {
      |        "match_all": {}
      |      },
      |      "aggs": {
      |        "userId": {
      |          "terms": {
      |            "field": "userId.keyword"
      |          },
      |          "aggs": {
      |            "lastSeen": {
      |              "max": {
      |                "field": "createdAt"
      |              }
      |            },
      |            "having_filter": {
      |              "bucket_selector": {
      |                "buckets_path": {
      |                  "lastSeen": "lastSeen"
      |                },
      |                "script": {
      |                  "source": "(params.lastSeen != null) && (params.lastSeen > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS).toInstant().toEpochMilli())"
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
      |  "_source": true,
      |  "aggs": {
      |    "filtered_agg": {
      |      "filter": {
      |        "bool": {
      |          "filter": [
      |            {
      |              "bool": {
      |                "must_not": [
      |                  {
      |                    "term": {
      |                      "Country": {
      |                        "value": "USA"
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            {
      |              "bool": {
      |                "must_not": [
      |                  {
      |                    "term": {
      |                      "City": {
      |                        "value": "Berlin"
      |                      }
      |                    }
      |                  }
      |                ]
      |              }
      |            },
      |            {
      |              "match_all": {}
      |            },
      |            {
      |              "range": {
      |                "lastSeen": {
      |                  "gt": "now-7d"
      |                }
      |              }
      |            }
      |          ]
      |        }
      |      },
      |      "aggs": {
      |        "Country": {
      |          "terms": {
      |            "field": "Country.keyword",
      |            "order": {
      |              "Country": "asc"
      |            }
      |          },
      |          "aggs": {
      |            "City": {
      |              "terms": {
      |                "field": "City.keyword"
      |              },
      |              "aggs": {
      |                "cnt": {
      |                  "value_count": {
      |                    "field": "CustomerID"
      |                  }
      |                },
      |                "lastSeen": {
      |                  "max": {
      |                    "field": "createdAt"
      |                  }
      |                },
      |                "having_filter": {
      |                  "bucket_selector": {
      |                    "buckets_path": {
      |                      "cnt": "cnt"
      |                    },
      |                    "script": {
      |                      "source": "1 == 1 && 1 == 1 && params.cnt > 1 && 1 == 1"
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
      .replaceAll("\\s", "")
      .replaceAll("ChronoUnit", " ChronoUnit")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
  }

  it should "handle parse_date function" in {
    val select: ElasticSearchRequest =
      SQLQuery(parseDate)
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
      |  "_source": true,
      |  "aggs": {
      |    "identifier": {
      |      "terms": {
      |        "field": "identifier.keyword",
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
      |              "source": "DateTimeFormatter.ofPattern('yyyy-MM-dd').parse(doc['createdAt'].value, LocalDate::from)"
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll(",ChronoUnit", ", ChronoUnit")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
      .replaceAll(",LocalDate", ", LocalDate")
  }

  it should "handle parse_datetime function" in {
    val select: ElasticSearchRequest =
      SQLQuery(parseDateTime)
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
      |  "_source": true,
      |  "aggs": {
      |    "identifier": {
      |      "terms": {
      |        "field": "identifier.keyword",
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
      |              "source": "DateTimeFormatter.ofPattern('yyyy-MM-ddTHH:mm:ssZ').parse(doc['createdAt'].value, LocalDateTime::from).truncatedTo(ChronoUnit.MINUTES)"
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll(">", " > ")
      .replaceAll(",LocalDate", ", LocalDate")
  }
}
