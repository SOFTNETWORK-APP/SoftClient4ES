package app.softnetwork.elastic.client

import org.json4s.ext.{JavaTimeSerializers, JavaTypesSerializers, JodaTimeSerializers}
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.util.{Failure, Success}

class ElasticConversionSpec extends AnyFlatSpec with Matchers with ElasticConversion {

  implicit val formats: Formats =
    Serialization.formats(NoTypeHints) ++
    JodaTimeSerializers.all ++
    JavaTypesSerializers.all ++
    JavaTimeSerializers.all

  "elastic conversion" should "parse simple hits" in {
    val results =
      """{
        |  "took": 5,
        |  "timed_out": false,
        |  "hits": {
        |    "total": { "value": 2, "relation": "eq" },
        |    "max_score": 1.0,
        |    "hits": [
        |      {
        |        "_index": "products",
        |        "_id": "1",
        |        "_score": 1.0,
        |        "_source": {
        |          "name": "Laptop",
        |          "price": 999.99,
        |          "category": "Electronics",
        |          "tags": ["computer", "portable"]
        |        }
        |      },
        |      {
        |        "_index": "products",
        |        "_id": "2",
        |        "_score": 0.8,
        |        "_source": {
        |          "name": "Mouse",
        |          "price": 29.99,
        |          "category": "Electronics"
        |        }
        |      }
        |    ]
        |  }
        |}""".stripMargin

    parseResponse(results, Map.empty, Map.empty) match {
      case Success(rows) =>
        rows.foreach(println)
      // Map(name -> Laptop, price -> 999.99, category -> Electronics, tags -> List(computer, portable), _id -> 1, _index -> products, _score -> 1.0)
      // Map(name -> Mouse, price -> 29.99, category -> Electronics, _id -> 2, _index -> products, _score -> 0.8)
      case Failure(error) =>
        throw error
    }
  }
  it should "parse hits with field object" in {
    val results =
      """{
        |  "took": 8,
        |  "hits": {
        |    "total": { "value": 1, "relation": "eq" },
        |    "max_score": 1.0,
        |    "hits": [
        |      {
        |        "_index": "users",
        |        "_id": "u1",
        |        "_score": 1.0,
        |        "_source": {
        |          "id": "u1",
        |          "name": "Alice",
        |          "address": {
        |            "street": "123 Main St",
        |            "city": "Wonderland",
        |            "country": "Fictionland"
        |          }
        |        }
        |      }
        |    ]
        |  }
        |}""".stripMargin
    parseResponse(
      results,
      Map.empty,
      Map.empty
    ) match {
      case Success(rows) =>
        rows.foreach(println)
        // Map(name -> Alice, address -> Map(street -> 123 Main St, city -> Wonderland, country -> Fictionland), _id -> u1, _index -> users, _score -> 1.0)
        val users = rows.map(row => convertTo[User](row))
        users.foreach(println)
        // User(u1,Alice,Address(123 Main St,Wonderland,Fictionland))
        users.size shouldBe 1
        users.head.id shouldBe "u1"
        users.head.name shouldBe "Alice"
        users.head.address.street shouldBe "123 Main St"
        users.head.address.city shouldBe "Wonderland"
        users.head.address.country shouldBe "Fictionland"
      case Failure(error) =>
        throw error
    }
  }
  it should "parse aggregations with top hits" in {
    val results = """{
                    |  "took": 10,
                    |  "hits": { "total": { "value": 100 }, "hits": [] },
                    |  "aggregations": {
                    |    "category": {
                    |      "doc_count_error_upper_bound": 0,
                    |      "sum_other_doc_count": 0,
                    |      "buckets": [
                    |        {
                    |          "key": "Electronics",
                    |          "doc_count": 50,
                    |          "avg_price": {
                    |            "value": 450.5
                    |          },
                    |          "max_price": {
                    |            "value": 999.99
                    |          },
                    |          "top_products": {
                    |            "hits": {
                    |              "total": { "value": 50 },
                    |              "max_score": 1.0,
                    |              "hits": [
                    |                {
                    |                  "_id": "1",
                    |                  "_score": 1.0,
                    |                  "_source": {
                    |                    "name": "Laptop",
                    |                    "price": 999.99,
                    |                    "stock": 15
                    |                  }
                    |                },
                    |                {
                    |                  "_id": "2",
                    |                  "_score": 0.95,
                    |                  "_source": {
                    |                    "name": "Phone",
                    |                    "price": 699.99,
                    |                    "stock": 25
                    |                  }
                    |                }
                    |              ]
                    |            }
                    |          }
                    |        },
                    |        {
                    |          "key": "Books",
                    |          "doc_count": 30,
                    |          "avg_price": {
                    |            "value": 25.0
                    |          },
                    |          "max_price": {
                    |            "value": 45.0
                    |          },
                    |          "top_products": {
                    |            "hits": {
                    |              "hits": [
                    |                {
                    |                  "_id": "3",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "name": "Programming Book",
                    |                    "price": 45.0,
                    |                    "stock": 50
                    |                  }
                    |                }
                    |              ]
                    |            }
                    |          }
                    |        }
                    |      ]
                    |    }
                    |  }
                    |}""".stripMargin

    parseResponse(
      results,
      Map.empty,
      Map(
        "top_products" -> ClientAggregation(
          "top_products",
          aggType = AggregationType.ArrayAgg,
          distinct = false,
          "name",
          windowing = true,
          ""
        )
      )
    ) match {
      case Success(rows) =>
        rows.foreach(println)
        //HashMap(top_products -> List(HashMap(_score -> 1.0, stock -> 15, name -> Laptop, _id -> 1, price -> 999.99), HashMap(_score -> 0.95, stock -> 25, name -> Phone, _id -> 2, price -> 699.99)), max_price -> 999.99, category -> Electronics, avg_price -> 450.5, category_doc_count -> 50)
        //HashMap(top_products -> List(HashMap(_score -> 0.0, stock -> 50, name -> Programming Book, _id -> 3, price -> 45.0)), max_price -> 45.0, category -> Books, avg_price -> 25.0, category_doc_count -> 30)
        val products = rows.map(row => convertTo[Products](row))
        products.foreach(println)
        // Products(Electronics,List(Product(Laptop,999.99,15,None), Product(Phone,699.99,25,None)),450.5)
        // Products(Books,List(Product(Programming Book,45.0,50,None)),25.0)
        products.size shouldBe 2
        products.count(_.category == "Electronics") shouldBe 1
        products.count(_.category == "Books") shouldBe 1
        products.minBy(_.avg_price).avg_price shouldBe 25.0
        products.maxBy(_.avg_price).avg_price shouldBe 450.5
        products.flatMap(
          _.top_products.map(_.name)
        ) should contain allOf ("Laptop", "Phone", "Programming Book")
      case Failure(error) =>
        throw error
    }
  }

  it should "parse nested aggregations" in {
    val results = """{
                    |  "aggregations": {
                    |    "country": {
                    |      "buckets": [
                    |        {
                    |          "key": "France",
                    |          "doc_count": 100,
                    |          "city": {
                    |            "buckets": [
                    |              {
                    |                "key": "Paris",
                    |                "doc_count": 60,
                    |                "product": {
                    |                  "buckets": [
                    |                    {
                    |                      "key": "Laptop",
                    |                      "doc_count": 30,
                    |                      "total_sales": { "value": 29997.0 },
                    |                      "avg_price": { "value": 999.9 }
                    |                    },
                    |                    {
                    |                      "key": "Phone",
                    |                      "doc_count": 30,
                    |                      "total_sales": { "value": 20997.0 },
                    |                      "avg_price": { "value": 699.9 }
                    |                    }
                    |                  ]
                    |                }
                    |              },
                    |              {
                    |                "key": "Lyon",
                    |                "doc_count": 40,
                    |                "product": {
                    |                  "buckets": [
                    |                    {
                    |                      "key": "Tablet",
                    |                      "doc_count": 40,
                    |                      "total_sales": { "value": 15996.0 },
                    |                      "avg_price": { "value": 399.9 }
                    |                    }
                    |                  ]
                    |                }
                    |              }
                    |            ]
                    |          }
                    |        },
                    |        {
                    |          "key": "Germany",
                    |          "doc_count": 80,
                    |          "city": {
                    |            "buckets": [
                    |              {
                    |                "key": "Berlin",
                    |                "doc_count": 80,
                    |                "product": {
                    |                  "buckets": [
                    |                    {
                    |                      "key": "Mouse",
                    |                      "doc_count": 80,
                    |                      "total_sales": { "value": 2399.2 },
                    |                      "avg_price": { "value": 29.99 }
                    |                    }
                    |                  ]
                    |                }
                    |              }
                    |            ]
                    |          }
                    |        }
                    |      ]
                    |    }
                    |  }
                    |}""".stripMargin
    parseResponse(results, Map.empty, Map.empty) match {
      case Success(rows) =>
        rows.foreach(println)
        // Map(country -> France, country_doc_count -> 100, city -> Paris, city_doc_count -> 60, product -> Laptop, product_doc_count -> 30, total_sales -> 29997.0, avg_price -> 999.9)
        // Map(country -> France, country_doc_count -> 100, city -> Paris, city_doc_count -> 60, product -> Phone, product_doc_count -> 30, total_sales -> 20997.0, avg_price -> 699.9)
        // Map(country -> France, country_doc_count -> 100, city -> Lyon, city_doc_count -> 40, product -> Tablet, product_doc_count -> 40, total_sales -> 15996.0, avg_price -> 399.9)
        // Map(country -> Germany, country_doc_count -> 80, city -> Berlin, city_doc_count -> 80, product -> Mouse, product_doc_count -> 80, total_sales -> 2399.2, avg_price -> 29.99)
        val sales = rows.map(row => convertTo[Sales](row))
        sales.foreach(println)
        // Sales(France,Paris,Laptop,29997.0,999.9)
        // Sales(France,Paris,Phone,20997.0,699.9)
        // Sales(France,Lyon,Tablet,15996.0,399.9)
        // Sales(Germany,Berlin,Mouse,2399.2,29.99)
        sales.size shouldBe 4
        sales.count(_.country == "France") shouldBe 3
        sales.count(_.country == "Germany") shouldBe 1
        sales.map(_.city) should contain allOf ("Paris", "Lyon", "Berlin")
        sales
          .filter(_.country == "France")
          .map(_.product) should contain allOf ("Laptop", "Phone", "Tablet")
        sales.filter(_.country == "Germany").map(_.product) should contain only "Mouse"
      case Failure(error) =>
        throw error
    }
  }

  it should "parse date histogram aggregations" in {
    val results = """{
                    |  "aggregations": {
                    |    "sales_over_time": {
                    |      "buckets": [
                    |        {
                    |          "key_as_string": "2024-01-01T00:00:00.000Z",
                    |          "key": 1704067200000,
                    |          "doc_count": 100,
                    |          "total_revenue": { "value": 50000.0 }
                    |        },
                    |        {
                    |          "key_as_string": "2024-02-01T00:00:00.000Z",
                    |          "key": 1706745600000,
                    |          "doc_count": 150,
                    |          "total_revenue": { "value": 75000.0 }
                    |        }
                    |      ]
                    |    }
                    |  }
                    |}""".stripMargin
    parseResponse(results, Map.empty, Map.empty) match {
      case Success(rows) =>
        rows.foreach(println)
        // Map(date -> 2024-01-01T00:00:00.000Z, doc_count -> 100, total_sales -> 50000.0)
        // Map(date -> 2024-02-01T00:00:00.000Z, doc_count -> 150, total_sales -> 75000.0)
        val history = rows.map(row => convertTo[SalesHistory](row))
        history.foreach(println)
        // SalesHistory(2024-01-01T00:00,50000.0)
        // SalesHistory(2024-02-01T00:00,75000.0)
        history.size shouldBe 2
        history.map(_.sales_over_time.getMonthValue) should contain allOf (1, 2)
        history.find(_.sales_over_time.getMonthValue == 1).get.total_revenue shouldBe 50000.0
        history.find(_.sales_over_time.getMonthValue == 2).get.total_revenue shouldBe 75000.0
      case Failure(error) =>
        throw error
    }
  }

  it should "parse aggregations with FIRST, LAST and ARRAY_AGG" in {
    val results = """{
                    |  "took": 45,
                    |  "timed_out": false,
                    |  "_shards": {
                    |    "total": 5,
                    |    "successful": 5,
                    |    "skipped": 0,
                    |    "failed": 0
                    |  },
                    |  "hits": {
                    |    "total": {
                    |      "value": 150,
                    |      "relation": "eq"
                    |    },
                    |    "max_score": null,
                    |    "hits": []
                    |  },
                    |  "aggregations": {
                    |    "dept": {
                    |      "doc_count_error_upper_bound": 0,
                    |      "sum_other_doc_count": 0,
                    |      "buckets": [
                    |        {
                    |          "key": "Engineering",
                    |          "doc_count": 45,
                    |          "cnt": {
                    |            "value": 38
                    |          },
                    |          "first_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 45,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "1",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 55000,
                    |                    "firstName": "John"
                    |                  },
                    |                  "sort": [
                    |                    1420070400000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "last_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 45,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "45",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 95000,
                    |                    "firstName": "Sarah"
                    |                  },
                    |                  "sort": [
                    |                    1672531200000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "employees": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 45,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "1",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "name": "John Doe"
                    |                  },
                    |                  "sort": [
                    |                    1420070400000,
                    |                    95000
                    |                  ]
                    |                },
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "2",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "name": "Jane Smith"
                    |                  },
                    |                  "sort": [
                    |                    1425254400000,
                    |                    88000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          }
                    |        },
                    |        {
                    |          "key": "Sales",
                    |          "doc_count": 32,
                    |          "cnt": {
                    |            "value": 28
                    |          },
                    |          "first_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 32,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "50",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 48000,
                    |                    "firstName": "Michael"
                    |                  },
                    |                  "sort": [
                    |                    1388534400000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "last_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 32,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "82",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 72000,
                    |                    "firstName": "Emily"
                    |                  },
                    |                  "sort": [
                    |                    1667260800000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "employees": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 32,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "50",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "name": "Michael Brown"
                    |                  },
                    |                  "sort": [
                    |                    1388534400000,
                    |                    72000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          }
                    |        },
                    |        {
                    |          "key": "Marketing",
                    |          "doc_count": 28,
                    |          "cnt": {
                    |            "value": 25
                    |          },
                    |          "first_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 28,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "100",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 52000,
                    |                    "firstName": "David"
                    |                  },
                    |                  "sort": [
                    |                    1404172800000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "last_salary": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 28,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "128",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "salary": 78000,
                    |                    "firstName": "Lisa"
                    |                  },
                    |                  "sort": [
                    |                    1672531200000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          },
                    |          "employees": {
                    |            "hits": {
                    |              "total": {
                    |                "value": 28,
                    |                "relation": "eq"
                    |              },
                    |              "max_score": null,
                    |              "hits": [
                    |                {
                    |                  "_index": "employees",
                    |                  "_type": "_doc",
                    |                  "_id": "100",
                    |                  "_score": null,
                    |                  "_source": {
                    |                    "name": "David Wilson"
                    |                  },
                    |                  "sort": [
                    |                    1404172800000,
                    |                    78000
                    |                  ]
                    |                }
                    |              ]
                    |            }
                    |          }
                    |        }
                    |      ]
                    |    }
                    |  },
                    |  "fields": {
                    |    "hire_date": []
                    |  }
                    |}""".stripMargin

    parseResponse(
      results,
      Map.empty,
      Map(
        "employees" -> ClientAggregation(
          aggName = "employees",
          aggType = AggregationType.ArrayAgg,
          distinct = false,
          "name",
          windowing = true,
          ""
        )
      )
    ) match {
      case Success(rows) =>
        rows.foreach(println)
      //HashMap(dept_doc_count -> 45, last_salary -> HashMap(_score -> 0.0, salary -> 95000, firstName -> Sarah, _id -> 45, _index -> employees), first_salary -> HashMap(_score -> 0.0, salary -> 55000, firstName -> John, _id -> 1, _index -> employees), cnt -> 38, employees -> List(Map(name -> John Doe, _id -> 1, _index -> employees, _score -> 0.0), Map(name -> Jane Smith, _id -> 2, _index -> employees, _score -> 0.0)), dept -> Engineering)
      //HashMap(dept_doc_count -> 32, last_salary -> HashMap(_score -> 0.0, salary -> 72000, firstName -> Emily, _id -> 82, _index -> employees), first_salary -> HashMap(_score -> 0.0, salary -> 48000, firstName -> Michael, _id -> 50, _index -> employees), cnt -> 28, employees -> Map(name -> Michael Brown, _id -> 50, _index -> employees, _score -> 0.0), dept -> Sales)
      //HashMap(dept_doc_count -> 28, last_salary -> HashMap(_score -> 0.0, salary -> 78000, firstName -> Lisa, _id -> 128, _index -> employees), first_salary -> HashMap(_score -> 0.0, salary -> 52000, firstName -> David, _id -> 100, _index -> employees), cnt -> 25, employees -> Map(name -> David Wilson, _id -> 100, _index -> employees, _score -> 0.0), dept -> Marketing)
      case Failure(error) =>
        throw error
    }
  }
}

case class Products(category: String, top_products: List[Product], avg_price: Double)

case class Product(
  name: String,
  price: Double,
  stock: Int,
  tags: Option[List[String]] = None
)

case class Sales(
  country: String,
  city: String,
  product: String,
  total_sales: Double,
  avg_price: Double
)

case class SalesHistory(
  sales_over_time: ZonedDateTime,
  total_revenue: Double
)

case class Address(
  street: String,
  city: String,
  country: String
)

case class User(
  id: String,
  name: String,
  address: Address
)
