package app.softnetwork.elastic.client

import app.softnetwork.serialization.commonFormats
import org.json4s.Formats
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.util.{Failure, Success}

class ElasticConversionSpec extends AnyFlatSpec with Matchers with ElasticConversion {

  implicit val formats: Formats = commonFormats

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

    parseResponse(results) match {
      case Success(rows) =>
        rows.foreach(println)
      // Map(name -> Laptop, price -> 999.99, category -> Electronics, tags -> List(computer, portable), _id -> 1, _index -> products, _score -> 1.0)
      // Map(name -> Mouse, price -> 29.99, category -> Electronics, _id -> 2, _index -> products, _score -> 0.8)
      case Failure(error) =>
        throw error
    }
  }
  it should "parse aggregations with top_hits" in {
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

    parseResponse(results) match {
      case Success(rows) =>
        rows.foreach(println)
        // HashMap(_score -> 1.0, name -> Laptop, _id -> 1, avg_price -> 450.5, by_category_doc_count -> 50, stock -> 15, category -> Electronics, max_price -> 999.99, price -> 999.99)
        // HashMap(_score -> 0.95, name -> Phone, _id -> 2, avg_price -> 450.5, by_category_doc_count -> 50, stock -> 25, category -> Electronics, max_price -> 999.99, price -> 699.99)
        // HashMap(name -> Programming Book, _id -> 3, avg_price -> 25.0, by_category_doc_count -> 30, stock -> 50, category -> Books, max_price -> 45.0, price -> 45.0)
        val products = rows.map(row => convertTo[Product](row))
        products.foreach(println)
        // Product(Laptop,450.5,Electronics,15,None)
        // Product(Phone,450.5,Electronics,25,None)
        // Product(Programming Book,25.0,Books,50,None)
        products.size shouldBe 3
        products.count(_.category == "Electronics") shouldBe 2
        products.count(_.category == "Books") shouldBe 1
        products.minBy(_.avg_price).avg_price shouldBe 25.0
        products.maxBy(_.avg_price).avg_price shouldBe 450.5
        products.map(_.name) should contain allOf ("Laptop", "Phone", "Programming Book")
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
    parseResponse(results) match {
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
    parseResponse(results) match {
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
}

case class Product(
  name: String,
  avg_price: Double,
  category: String,
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
