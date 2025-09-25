package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.Queries._
import app.softnetwork.elastic.sql.query.SQLQuery
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
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.minus(35, ChronoUnit.MINUTES) : null)"
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
      .replaceAll("defv", "def v")
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
      |              "source": "def left = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); left == null ? false : left < ZonedDateTime.now(ZoneId.of('Z')).toLocalTime()"
      |            }
      |          }
      |        },
      |        {
      |          "script": {
      |            "script": {
      |              "lang": "painless",
      |              "source": "def left = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); left == null ? false : left >= ZonedDateTime.now(ZoneId.of('Z')).toLocalTime().minus(10, ChronoUnit.MINUTES)"
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
      |                  "source": "params.lastSeen > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS)"
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
      SQLQuery(groupByWithHavingAndDateTimeFunctions.replace("GROUP BY 3, 2", "GROUP BY 3, 2"))
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
      |              "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? DateTimeFormatter.ofPattern('yyyy-MM-dd').parse(e0, LocalDate::from) : null)"
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defv", "def v")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(";", "; ")
      .replaceAll(",ChronoUnit", ", ChronoUnit")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll(">", " > ")
      .replaceAll(",LocalDate", ", LocalDate")
  }

  it should "handle parse_datetime function" in {
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
      |              "source": "(def e2 = (def e1 = (def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? DateTimeFormatter.ofPattern('yyyy-MM-ddTHH:mm:ssZ').parse(e0, ZonedDateTime::from) : null); e1 != null ? e1.truncatedTo(ChronoUnit.MINUTES) : null); e2 != null ? e2.get(ChronoField.YEAR) : null)"
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defv", "def v")
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
      |        "source": "(def arg0 = (!doc.containsKey('updatedAt') || doc['updatedAt'].empty ? null : doc['updatedAt'].value); def arg1 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); (arg0 == null || arg1 == null) ? null : ChronoUnit.DAYS.between(arg0, arg1))"
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
      .replaceAll("defv", "def v")
      .replaceAll("defe", "def e")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(",a", ", a")
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
      |  "_source": true,
      |  "aggs": {
      |    "identifier": {
      |      "terms": {
      |        "field": "identifier.keyword"
      |      },
      |      "aggs": {
      |        "max_diff": {
      |          "max": {
      |            "script": {
      |              "lang": "painless",
      |              "source": "(def arg0 = (!doc.containsKey('updatedAt') || doc['updatedAt'].empty ? null : doc['updatedAt'].value); def arg1 = (def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? DateTimeFormatter.ofPattern('yyyy-MM-ddTHH:mm:ssZ').parse(e0, ZonedDateTime::from) : null); (arg0 == null || arg1 == null) ? null : ChronoUnit.DAYS.between(arg0, arg1))"
      |            }
      |          }
      |        }
      |      }
      |    }
      |  }
      |}""".stripMargin
      .replaceAll("\\s", "")
      .replaceAll("defv", "def v")
      .replaceAll("defe", "def e")
      .replaceAll("defa", "def a")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll(",a", ", a")
      .replaceAll(";", "; ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ZonedDateTime", " ZonedDateTime")
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
      |        "source": "(def e0 = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); e0 != null ? e0.plus(10, ChronoUnit.DAYS) : null)"
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
      .replaceAll("defv", "def v")
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

  it should "handle date_sub function as script field" in {
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
      |        "source": "(def e0 = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); e0 != null ? e0.minus(10, ChronoUnit.DAYS) : null)"
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
      .replaceAll("defv", "def v")
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
      |        "source": "(def e0 = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); e0 != null ? e0.plus(10, ChronoUnit.DAYS) : null)"
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
      .replaceAll("defv", "def v")
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
      |        "source": "(def e0 = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); e0 != null ? e0.minus(10, ChronoUnit.DAYS) : null)"
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
      .replaceAll("defv", "def v")
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
      |        "source": "(def e0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); e0 == null)"
      |      }
      |    }
      |  },
      |  "_source": true
      |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defv", "def v")
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
      |        "source": "(def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null)"
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
      .replaceAll("defv", "def v")
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
      |        "source": "{ def v0 = (def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.minus(35, ChronoUnit.MINUTES) : null);if (v0 != null) return v0; return ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().atStartOfDay(ZoneId.of('Z')); }"
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
      .replaceAll("defv", " def v")
      .replaceAll("defe", "def e")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
      .replaceAll("\\?", " ? ")
      .replaceAll(":null", " : null")
      .replaceAll(";}", "; }")
      .replaceAll(";e", "; e")
      .replaceAll(":null", " : null")
      .replaceAll("null:", "null : ")
      .replaceAll("return", " return ")
      .replaceAll("==", " == ")
      .replaceAll("!=", " != ")
      .replaceAll("&&", " && ")
      .replaceAll("\\|\\|", " || ")
      .replaceAll("ChronoUnit", " ChronoUnit")
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
      |        "source": "{ def v0 = ((def arg0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); (arg0 == null) ? null : arg0 == DateTimeFormatter.ofPattern('yyyy-MM-dd').parse(\"2025-09-11\", LocalDate::from).minus(2, ChronoUnit.DAYS) ? null : arg0));if (v0 != null) return v0; return ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); }"
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
      .replaceAll("=DateTimeFormatter", " = DateTimeFormatter")
      .replaceAll("ZonedDateTime", " ZonedDateTime")
  }

  it should "handle cast function as script field" in {
    val select: ElasticSearchRequest =
      SQLQuery(cast)
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
      |        "source": "{ def v0 = ((def arg0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); (arg0 == null) ? null : arg0 == DateTimeFormatter.ofPattern('yyyy-MM-dd').parse(\"2025-09-11\", LocalDate::from) ? null : arg0));if (v0 != null) return v0; return ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().atStartOfDay(ZoneId.of('Z')).minus(2, ChronoUnit.HOURS); }.toInstant().toEpochMilli()"
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
  }

  it should "handle case function as script field" in {
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
      |        "source": "{ if (def left = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); left == null ? false : left > ZonedDateTime.now(ZoneId.of('Z')).minus(7, ChronoUnit.DAYS)) return left; if (def left = (!doc.containsKey('lastSeen') || doc['lastSeen'].empty ? null : doc['lastSeen'].value); left != null) return left.plus(2, ChronoUnit.DAYS); def dval = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); return dval; }"
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
      .replaceAll("defv", " def v")
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
      |        "source": "{ def expr = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().minus(7, ChronoUnit.DAYS); def e0 = (!doc.containsKey('lastUpdated') || doc['lastUpdated'].empty ? null : doc['lastUpdated'].value); def val0 = e0 != null ? e0.minus(3, ChronoUnit.DAYS).atStartOfDay(ZoneId.of('Z')) : null; if (expr == val0) return e0; def val1 = (!doc.containsKey('lastSeen') || doc['lastSeen'].empty ? null : doc['lastSeen'].value); if (expr == val1) return val1.plus(2, ChronoUnit.DAYS); def dval = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); return dval; }"
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
      .replaceAll("defv", " def v")
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
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.DAY_OF_MONTH) : null)"
      |      }
      |    },
      |    "dow": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.DAY_OF_WEEK) : null)"
      |      }
      |    },
      |    "doy": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.DAY_OF_YEAR) : null)"
      |      }
      |    },
      |    "m": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.MONTH_OF_YEAR) : null)"
      |      }
      |    },
      |    "y": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.YEAR) : null)"
      |      }
      |    },
      |    "h": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.HOUR_OF_DAY) : null)"
      |      }
      |    },
      |    "minutes": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.MINUTE_OF_HOUR) : null)"
      |      }
      |    },
      |    "s": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e0 != null ? e0.get(ChronoField.SECOND_OF_MINUTE) : null)"
      |      }
      |    }
      |  },
      |  "_source": true
      |}""".stripMargin
      .replaceAll("\\s+", "")
      .replaceAll("defe", "def e")
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
      |              "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 * (ZonedDateTime.now(ZoneId.of('Z')).toLocalDate().get(ChronoField.YEAR) - 10)) > 10000"
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
      |        "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 + 1)"
      |      }
      |    },
      |    "sub": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 - 1)"
      |      }
      |    },
      |    "mul": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 * 2)"
      |      }
      |    },
      |    "div": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 / 2)"
      |      }
      |    },
      |    "mod": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "def lv0 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); ( lv0 == null ) ? null : (lv0 % 2)"
      |      }
      |    },
      |    "identifier_mul_identifier2_minus_10": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "def lv0 = ((def lv1 = ((!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value)); def rv1 = ((!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value)); ( lv1 == null || rv1 == null ) ? null : (lv1 * rv1))); ( lv0 == null ) ? null : (lv0 - 10)"
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
      .replaceAll("defv", "def v")
      .replaceAll("defe", "def e")
      .replaceAll("defl", "def l")
      .replaceAll("defr", "def r")
      .replaceAll("if\\(", "if (")
      .replaceAll("=\\(", " = (")
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
      |              "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.sqrt(arg0)) > 100.0"
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
      |        "source": "((def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.abs(arg0)) + 1.0) * ((double) 2)"
      |      }
      |    },
      |    "ceil_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.ceil(arg0))"
      |      }
      |    },
      |    "floor_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.floor(arg0))"
      |      }
      |    },
      |    "sqrt_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.sqrt(arg0))"
      |      }
      |    },
      |    "exp_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.exp(arg0))"
      |      }
      |    },
      |    "log_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.log(arg0))"
      |      }
      |    },
      |    "log10_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.log10(arg0))"
      |      }
      |    },
      |    "pow_identifier_3": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.pow(arg0, 3))"
      |      }
      |    },
      |    "round_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : (def p = Math.pow(10, 0); Math.round((arg0 * p) / p)))"
      |      }
      |    },
      |    "round_identifier_2": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : (def p = Math.pow(10, 2); Math.round((arg0 * p) / p)))"
      |      }
      |    },
      |    "sign_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); arg0 != null ? (arg0 > 0 ? 1 : (arg0 < 0 ? -1 : 0)) : null)"
      |      }
      |    },
      |    "cos_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.cos(arg0))"
      |      }
      |    },
      |    "acos_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.acos(arg0))"
      |      }
      |    },
      |    "sin_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.sin(arg0))"
      |      }
      |    },
      |    "asin_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.asin(arg0))"
      |      }
      |    },
      |    "tan_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.tan(arg0))"
      |      }
      |    },
      |    "atan_identifier": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.atan(arg0))"
      |      }
      |    },
      |    "atan2_identifier_3_0": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier') || doc['identifier'].empty ? null : doc['identifier'].value); (arg0 == null) ? null : Math.atan2(arg0, 3.0))"
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
      .replaceAll("defv", " def v")
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

  it should "handle string function as script field and condition" in {
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
      |              "source": "def left = (def e1 = (def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null ? e0.trim() : null); e1 != null ? e1.length() : null); left == null ? false : left > 10"
      |            }
      |          }
      |        }
      |      ]
      |    }
      |  },
      |  "script_fields": {
      |    "l": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null ? e0.length() : null)"
      |      }
      |    },
      |    "low": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null ? e0.lower() : null)"
      |      }
      |    },
      |    "upp": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null ? e0.upper() : null)"
      |      }
      |    },
      |    "sub": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); (arg0 == null) ? null : ((1 - 1) < 0 || (1 - 1 + 3) > arg0.length()) ? null : arg0.substring((1 - 1), (1 - 1 + 3)))"
      |      }
      |    },
      |    "tr": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def e0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); e0 != null ? e0.trim() : null)"
      |      }
      |    },
      |    "con": {
      |      "script": {
      |        "lang": "painless",
      |        "source": "(def arg0 = (!doc.containsKey('identifier2') || doc['identifier2'].empty ? null : doc['identifier2'].value); (arg0 == null) ? null : String.valueOf(arg0) + \"_test\" + String.valueOf(1))"
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
      .replaceAll(":0", " : 0")
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
      .replaceAll(";\\s\\s", "; ")
      .replaceAll("false:", "false : ")
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
      |        "source": "(!doc.containsKey('hire_date') || doc['hire_date'].empty ? null : doc['hire_date'].value)"
      |      }
      |    }
      |  },
      |  "_source": true,
      |  "aggs": {
      |    "dept": {
      |      "terms": {
      |        "field": "department.keyword"
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
      |        }
      |      }
      |    }
      |  }
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
        |              "source": "(def e1 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); e1.withDayOfMonth(e1.lengthOfMonth())).get(ChronoField.DAY_OF_MONTH) > 28"
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
        |        "source": "(def e1 = (!doc.containsKey('createdAt') || doc['createdAt'].empty ? null : doc['createdAt'].value); e1 != null ? e1.withDayOfMonth(e1.lengthOfMonth()) : null)"
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

}
