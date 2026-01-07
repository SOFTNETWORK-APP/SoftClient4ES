# ![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)


![Build Status](https://github.com/SOFTNETWORK-APP/SoftClient4ES/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES/graph/badge.svg?token=XYCWBGVHAC)](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/1c13d6eb7d6c4a1495cd47e457c132dc)](https://app.codacy.com/gh/SOFTNETWORK-APP/elastic/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![License](https://img.shields.io/github/license/SOFTNETWORK-APP/elastic)](https://github.com/SOFTNETWORK-APP/elastic/blob/main/LICENSE)

**SoftClient4ES** is a modular, version-resilient, and production-ready Scala client for Elasticsearch. Built on top of official Elasticsearch clients, it provides a unified, stable API that simplifies migration across Elasticsearch versions, accelerates development, and offers advanced features for search, indexing, mapping management, and data manipulation.

---

## **Why SoftClient4ES?**

- ✅ **Version-agnostic**: Write once, run on Elasticsearch 6, 7, 8, or 9
- ✅ **Type-safe**: Leverages Scala's type system for compile-time safety
- ✅ **Production-ready**: Built-in error handling, validation, and rollback mechanisms
- ✅ **Stream-powered**: Akka Streams integration for high-performance bulk operations
- ✅ **SQL-compatible**: Translate SQL queries to Elasticsearch DSL
- ✅ **Zero-downtime migrations**: Automatic mapping migration with rollback support
- ✅ **Event-driven**: Seamless Akka Persistence integration

---

## **Key Features**

### **1. Unified Elasticsearch API**

SoftClient4ES provides a trait-based interface (`ElasticClientApi`) that aggregates all core Elasticsearch functionalities through composable APIs. This design abstracts the underlying client implementation and ensures compatibility across different Elasticsearch versions.

#### **Core APIs**

| API                                                    | Description                                                                         | Documentation                                   |
|--------------------------------------------------------|-------------------------------------------------------------------------------------|-------------------------------------------------|
| **[RefreshApi](documentation/client/refresh.md)**      | Control index refresh for real-time search                                          | [📖 Docs](documentation/client/refresh.md)      |
| **[IndicesApi](documentation/client/indices.md)**      | Create, update, and manage indices with settings and mappings                       | [📖 Docs](documentation/client/indices.md)      |
| **[SettingsApi](documentation/client/settings.md)**    | Dynamic index settings management                                                   | [📖 Docs](documentation/client/settings.md)     |
| **[AliasApi](documentation/client/aliases.md)**        | Manage index aliases for zero-downtime deployments                                  | [📖 Docs](documentation/client/aliases.md)      |
| **[MappingApi](documentation/client/mappings.md)**     | Smart mapping management with automatic migration and rollback                      | [📖 Docs](documentation/client/mappings.md)     |
| **[IndexApi](documentation/client/index.md)**          | Index documents                                                                     | [📖 Docs](documentation/client/index.md)        |
| **[UpdateApi](documentation/client/update.md)**        | Partial document updates with script support                                        | [📖 Docs](documentation/client/update.md)       |
| **[DeleteApi](documentation/client/delete.md)**        | Delete documents by ID or query                                                     | [📖 Docs](documentation/client/delete.md)       |
| **[BulkApi](documentation/client/bulk.md)**            | High-performance bulk operations with Akka Streams                                  | [📖 Docs](documentation/client/bulk.md)         |
| **[GetApi](documentation/client/get.md)**              | Get documents by ID                                                                 | [📖 Docs](documentation/client/get.md)          |
| **[SearchApi](documentation/client/search.md)**        | Advanced search with SQL and aggregations support                                   | [📖 Docs](documentation/client/search.md)       |
| **[ScrollApi](documentation/client/scroll.md)**        | Stream large datasets with automatic strategy detection (PIT, search_after, scroll) | [📖 Docs](documentation/client/scroll.md)       |
| **[AggregationApi](documentation/client/aggregations.md)** | Type-safe way to execute aggregations using SQL queries                             | [📖 Docs](documentation/client/aggregations.md) |
| **[TemplateApi](documentation/client/templates.md)**   | Templates management                                                                | [📖 Docs](documentation/client/templates.md)    |
| **[GatewayApi](documentation/client/gateway.md)**     | Unified SQL interface for DQL, DML, and DDL statements                | [📖 Docs](documentation/client/gateway.md)     |

#### **Client Implementations**

- **`JavaClientApi`**: For Elasticsearch 8 and 9 using the official Java client
- **`RestHighLevelClientApi`**: For Elasticsearch 6 and 7 using the official high-level REST client
- **`JestClientApi`**: For Elasticsearch 6 using the open-source [Jest client](https://github.com/searchbox-io/Jest)

**Example:**

```scala
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.spi._

// Create a unified client using Service Provider Interface (SPI)
val client: ElasticClientApi = ElasticClientFactory.create()

// Client with metrics support
val clientWithMetrics: ElasticClientApi = ElasticClientFactory.createWithMetrics()

//=== Elasticsearch Metrics ===
//  Total Operations: 111
//  Success Rate: 98.1981981981982%
//  Failure Rate: 1.8018018018018012%
//  Average Duration: 37.333333333333336ms
//  Min Duration: 1ms
//  Max Duration: 212ms
//=============================

// Client with monitoring support
val clientWithMonitoring: ElasticClientApi = ElasticClientFactory.createWithMonitoring()

// Automatic periodic reports every 30s:
// === Elasticsearch Metrics ===
// Total Operations: 150
// Success Rate: 98.5%
// Average Duration: 45ms
// =============================

// Automatic alerts when thresholds exceeded:
// ⚠️  HIGH FAILURE RATE: 15.0%
// ⚠️  HIGH LATENCY: 1200ms

// Client usage examples :

// Upsert a document
val result = client.update("users", "user-1", """{"name":"Alice","age":30}""", upsert = true)

// Search using plain SQL query
val searchResult = client.search(SQLQuery("SELECT * FROM users WHERE age > 25"))

// Domain model
case class Product(id: String, name: String, price: Double, category: String, obsolete: Boolean)

// Scroll through large datasets
val obsoleteProducts: Source[Product, NotUsed] = client.scrollAsUnchecked[Product](
  """
    |SELECT uuid AS id, name, price, category, outdated AS obsolete FROM products WHERE outdated = true
    |""".stripMargin
)

// Bulk operations with Akka Streams
implicit val bulkOptions: BulkOptions = BulkOptions(
  defaultIndex =  "products",
  maxBulkSize = 1000,
  balance = 4
)
val idsToDelete: Source[String, NotUsed] = obsoleteProducts.map(_._1.id)

client.bulkSource(
  items = idsToDelete,
  toDocument = id => s"""{"id": "$id"}""",
  idKey = Some("id"),
  delete = Some(true)
).runWith(Sink.foreach {
  case Right(success) =>
    println(s"✅ Success: ${success.id} in ${success.index}")

  case Left(failed) =>
    println(s"❌ Failed: ${failed.id} - ${failed.error}")
})
```

---

### **2. Zero-Downtime Mapping Migration**

The **MappingApi** provides intelligent mapping management with automatic migration, validation, and rollback capabilities.

#### **Features**

✅ **Automatic Change Detection**: Compares existing mappings with new ones  
✅ **Safe Migration Strategy**: Creates temporary indices, reindexes, and renames atomically  
✅ **Automatic Rollback**: Reverts to original state if migration fails  
✅ **Backup & Restore**: Preserves original mappings and settings  
✅ **Progress Tracking**: Detailed logging of migration steps  
✅ **Validation**: Strict JSON validation with error reporting

#### **Migration Workflow**

1. Backup current mapping and settings
2. Create temporary index with new mapping
3. Reindex data from original to temporary
4. Delete original index
5. Recreate original index with new mapping
6. Reindex data from temporary to original
7. Delete temporary index
8. Rollback if any step fails

**Example:**

```scala
val newMapping = """{
  "properties": {
    "name": {"type": "text"},
    "email": {"type": "keyword"},
    "age": {"type": "integer"},
    "created": {"type": "date"}
  }
}"""

// Automatically detects changes and migrates
val result = mappingApi.updateMapping("users", newMapping)

result match {
  case ElasticSuccess(true) => 
    println("✅ Migration completed successfully")
  case ElasticFailure(error) => 
    println(s"❌ Migration failed: ${error.message}")
    // Original mapping automatically restored
}
```

---

### **3. SQL compatible**

### **3.1 SQL Gateway — Unified SQL Interface for Elasticsearch**

SoftClient4ES includes a high‑level SQL Gateway that allows you to execute **DQL, DML, DDL, and Pipeline statements** directly against Elasticsearch using standard SQL syntax.

The Gateway exposes a single entry point:

```scala
gateway.run(sql: String): Future[ElasticResult[QueryResult]]
```

It automatically:

- normalizes SQL (removes comments, trims whitespace)
- parses SQL into AST nodes
- routes statements to the appropriate executor
- returns a typed `QueryResult` (`QueryRows`, `TableResult`, `PipelineResult`, `DmlResult`, `DdlResult`, `SQLResult`)

#### **Supported SQL Categories**

| Category | Examples |
|---------|----------|
| **DQL** | `SELECT`, `JOIN UNNEST`, `GROUP BY`, `HAVING`, window functions |
| **DML** | `INSERT`, `UPDATE`, `DELETE`, `COPY INTO` |
| **DDL (Tables)** | `CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, `DESCRIBE TABLE`, `SHOW TABLE`, `SHOW CREATE TABLE` |
| **DDL (Pipelines)** | `CREATE PIPELINE`, `ALTER PIPELINE`, `DROP PIPELINE`, `DESCRIBE PIPELINE`, `SHOW PIPELINE`, `SHOW CREATE PIPELINE` |

#### **Example**

```scala
gateway.run("""
  CREATE TABLE users (
    id INT,
    name TEXT,
    age INT,
    PRIMARY KEY (id)
  );
  INSERT INTO users VALUES (1, 'Alice', 30);
  SELECT * FROM users;
""")
```

#### **Documentation**

- 📘 **Gateway API** — `documentation/client/gateway.md`
- 📘 **DQL** — `documentation/sql/dql_statements.md`
- 📘 **DML** — `documentation/sql/dml_statements.md`
- 📘 **DDL** — `documentation/sql/ddl_statements.md`

---

### **3.2 SQL to Elasticsearch Query DSL**

SoftClient4ES includes a powerful SQL parser that translates standard SQL `SELECT` queries into native Elasticsearch queries.

#### **Supported SQL Features**

- ✅ `SELECT` with field selection and aliases
- ✅ Nested field access (e.g., `user.address.city`)
- ✅ `UNION ALL` for multi-query results
- ✅ `FROM` clause for index specification
- ✅ `JOIN` with UNNEST support
- ✅ `WHERE` clauses with complex conditions
- ✅ `HAVING` clauses for post-aggregation filtering
- ✅ `ORDER BY` with multiple fields
- ✅ `LIMIT` and `OFFSET` (pagination)
- ✅ `GROUP BY` and aggregations
- ✅ Logical operators (`AND`, `OR`, `NOT`)
- ✅ Comparison operators (`=`, `!=`, `<>`, `>`, `<`, `>=`, `<=`)
- ✅ Conditional operators (`CASE`, `IN`, `NOT IN`, `BETWEEN`, `IS NULL`, `IS NOT NULL`, `LIKE`, `NOT LIKE`, `RLIKE`, `NOT RLIKE`)
- ✅ Arithmetic operators (`+`, `-`, `*`, `/`, `%`)
- ✅ Cast operator (`::`)
- ✅ Cast functions (`CAST`, `CONVERT`, `TRY_CAST`, `SAFE_CAST`)
- ✅ Numeric functions (`ABS`, `SIGN`, `CEIL`, `FLOOR`, `ROUND`, `SQRT`, `POWER`, `LOG`, `LOG10`, `EXP`)
- ✅ Trigonometric functions (`SIN`, `COS`, `TAN`, `ASIN`, `ACOS`, `ATAN`, `ATAN2`, `DEGREES`, `RADIANS`)
- ✅ Conditional functions (`COALESCE`, `NULLIF`, `ISNULL`, `ISNOTNULL`, `MATCH ... AGAINST`)
- ✅ String functions (`CONCAT`, `SUBSTRING`, `LOWER`, `UPPER`, `TRIM`, `LTRIM`, `RTRIM`,`LENGTH`, `REPLACE`, `LEFT`, `RIGHT`, `REVERSE`, `POSITION`, `REGEXP_LIKE`)
- ✅ Date / Time functions (`YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`, `NANOSECOND`, `EPOCHDAY`, `OFFSET_SECONDS`, `LAST_DAY`, `WEEKDAY`, `YEARDAY`, `INTERVAL`, `CURRENT_DATE`, `CURDATE`, `TODAY`, `NOW`, `CURRENT_TIME`, `CURTIME`, `CURRENT_DATETIME`, `CURRENT_TIMESTAMP`, `DATE_ADD`, `DATEADD`, `DATE_SUB`, `DATESUB`, `DATETIME_ADD`, `DATETIMEADD`, `DATETIME_SUB`, `DATETIMESUB`, `DATE_DIFF`, `DATEDIFF`, `DATE_FORMAT`, `DATE_PARSE`, `DATETIME_FORMAT`, `DATETIME_PARSE`, `DATE_TRUNC`, `EXTRACT`)
- ✅ Geospatial functions (`POINT`, `ST_DISTANCE`)
- ✅ Aggregate functions (`COUNT`, `SUM`, `AVG`, `MIN`, `MAX`, `DISTINCT`, `FIRST_VALUE`, `LAST_VALUE`, `ARRAY_AGG`)
- ✅ [Window functions](#32-window-functions-support) with `OVER` clause
- ✅ [DML Support](#34-dml-support) (`INSERT`, `UPDATE`, `DELETE`)
- ✅ [DDL Support](#35-ddl-support) (`CREATE TABLE`, `ALTER TABLE`, `DROP TABLE`, `TRUNCATE TABLE`, `CREATE PIPELINE`, `ALTER PIPELINE`, `DROP PIPELINE`)

**Example:**

```scala
val sqlQuery = """
  SELECT
		restaurant_name,
		restaurant_city,
		menu.category AS menu_category,
		dish.name AS dish_name,
		ingredient.name AS ingredient_name,
		COUNT(distinct ingredient.name) AS ingredient_count,
		AVG(ingredient.cost) AS avg_ingredient_cost,
		SUM(ingredient.calories) AS total_calories,
		AVG(dish.price) AS avg_dish_price
	FROM restaurants 
		JOIN UNNEST(restaurants.menus) AS menu 
		JOIN UNNEST (menu.dishes) AS dish 
		JOIN UNNEST(dish.ingredients) AS ingredient
	WHERE
		restaurant_status = 'open'
	GROUP BY restaurant_name, restaurant_city, menu.category, dish.name, ingredient.name
	HAVING
		menu.is_available = true
		AND COUNT(distinct ingredient.name) >= 3
		AND AVG(ingredient.cost) <= 5
		AND SUM(ingredient.calories) <= 800
		AND AVG(dish.price) >= 10
	LIMIT 1000
"""

val results = client.search(SQLQuery(sqlQuery))
```

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "term": {
            "restaurant_status": {
              "value": "open"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "_source": false,
  "aggs": {
    "restaurant_name": {
      "terms": {
        "field": "restaurant_name",
        "size": 1000,
        "min_doc_count": 1
      },
      "aggs": {
        "restaurant_city": {
          "terms": {
            "field": "restaurant_city",
            "size": 1000,
            "min_doc_count": 1
          },
          "aggs": {
            "menu": {
              "nested": {
                "path": "menus"
              },
              "aggs": {
                "filtered_menu": {
                  "filter": {
                    "bool": {
                      "filter": [
                        {
                          "term": {
                            "menus.is_available": {
                              "value": true
                            }
                          }
                        }
                      ]
                    }
                  },
                  "aggs": {
                    "menu_category": {
                      "terms": {
                        "field": "menus.category",
                        "size": 1000,
                        "min_doc_count": 1
                      },
                      "aggs": {
                        "dish": {
                          "nested": {
                            "path": "menus.dishes"
                          },
                          "aggs": {
                            "dish_name": {
                              "terms": {
                                "field": "menus.dishes.name",
                                "size": 1000,
                                "min_doc_count": 1
                              },
                              "aggs": {
                                "having_filter": {
                                  "bucket_selector": {
                                    "buckets_path": {
                                      "ingredient_count": "ingredient>ingredient_count",
                                      "avg_dish_price": "avg_dish_price"
                                    },
                                    "script": {
                                      "source": "params.ingredient_count >= 3 && params.avg_dish_price >= 10"
                                    }
                                  }
                                },
                                "ingredient": {
                                  "nested": {
                                    "path": "menus.dishes.ingredients"
                                  },
                                  "aggs": {
                                    "ingredient_count": {
                                      "cardinality": {
                                        "field": "menus.dishes.ingredients.name"
                                      }
                                    },
                                    "ingredient_name": {
                                      "terms": {
                                        "field": "menus.dishes.ingredients.name",
                                        "size": 1000,
                                        "min_doc_count": 1
                                      },
                                      "aggs": {
                                        "avg_ingredient_cost": {
                                          "avg": {
                                            "field": "menus.dishes.ingredients.cost"
                                          }
                                        },
                                        "total_calories": {
                                          "sum": {
                                            "field": "menus.dishes.ingredients.calories"
                                          }
                                        },
                                        "having_filter": {
                                          "bucket_selector": {
                                            "buckets_path": {
                                              "ingredient_count": "ingredient_count",
                                              "avg_ingredient_cost": "avg_ingredient_cost",
                                              "total_calories": "total_calories"
                                            },
                                            "script": {
                                              "source": "params.ingredient_count >= 3 && params.avg_ingredient_cost <= 5 && params.total_calories <= 800"
                                            }
                                          }
                                        }
                                      }
                                    }
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```
---

### **3.2 Window Functions Support**

SoftClient4ES supports **SQL window functions** that are automatically translated into Elasticsearch aggregations. Window functions allow you to perform calculations across sets of rows that are related to the current row, without collapsing the result set.

#### **Supported Window Functions**

| Window Function    | SQL Syntax                                                        | Description                                     | Use Case                             |
|--------------------|-------------------------------------------------------------------|-------------------------------------------------|--------------------------------------|
| **FIRST_VALUE**    | `FIRST_VALUE(field) OVER (PARTITION BY ... ORDER BY ...)`         | Returns the first value in an ordered partition | Get first sale amount per product    |
| **LAST_VALUE**     | `LAST_VALUE(field) OVER (PARTITION BY ... ORDER BY ...)`          | Returns the last value in an ordered partition  | Get most recent price per product    |
| **ARRAY_AGG**      | `ARRAY_AGG(field) OVER (PARTITION BY ... ORDER BY ... LIMIT ...)` | Aggregates values into an array                 | Collect all sale amounts per product |
| **COUNT**          | `COUNT(field) OVER (PARTITION BY ...)`                            | Counts values in each partition                 | Count sales per product              |
| **SUM**            | `SUM(field) OVER (PARTITION BY ...)`                              | Sums values in each partition                   | Calculate total sales per product    |
| **AVG**            | `AVG(field) OVER (PARTITION BY ...)`                              | Averages values in each partition               | Calculate average price per category |
| **MIN**            | `MIN(field) OVER (PARTITION BY ...)`                              | Finds minimum value in each partition           | Find lowest price per product        |
| **MAX**            | `MAX(field) OVER (PARTITION BY ...)`                              | Finds maximum value in each partition           | Find highest sale per product        |
| **COUNT DISTINCT** | `COUNT(DISTINCT field) OVER (PARTITION BY ...)`                   | Counts unique values in each partition          | Count unique customers per product   |

#### **Key Features**

✅ **PARTITION BY**: Group rows into partitions for separate calculations  
✅ **ORDER BY**: Define ordering within partitions (required for `FIRST_VALUE`, `LAST_VALUE`, `ARRAY_AGG`)  
✅ **Multiple Partitions**: Support for different partition schemes in the same query  
✅ **Mixed Aggregations**: Combine window functions with standard `GROUP BY` aggregations  
✅ **In-Memory Join**: Window function results are joined with main query results via partition keys  
✅ **Type Safety**: Full compile-time validation for window function queries

#### **How Window Functions Work**

When a SQL query mixes window functions with non-aggregated fields, SoftClient4ES executes **two separate queries**:

1. **Window Functions Query**: Computes window function results using aggregations
2. **Main Query**: Retrieves non-aggregated fields using standard Elasticsearch search

The results are then **joined in memory** using the partition keys (`PARTITION BY` fields), ensuring that each row from the main query is enriched with its corresponding window function values.

**Query Execution Flow:**

```
SQL Query with Window Functions
         ↓
    ┌────────────────────────────────────┐
    │  Query Analysis & Decomposition    │
    └────────────────────────────────────┘
         ↓                    ↓
    ┌─────────────┐    ┌──────────────────┐
    │ Main Query  │    │ Window Functions │
    │ (Fields)    │    │ Query (Aggs)     │
    └─────────────┘    └──────────────────┘
         ↓                    ↓
    ┌─────────────┐    ┌──────────────────┐
    │ ES Search   │    │ ES Aggregations  │
    └─────────────┘    └──────────────────┘
         ↓                    ↓
    ┌─────────────────────────────────────┐
    │   In-Memory Join (Partition Keys)   │
    └─────────────────────────────────────┘
         ↓
    ┌─────────────────────────────────────┐
    │      Enriched Result Set            │
    └─────────────────────────────────────┘
```

#### **Example 1: Product Sales Analysis with Window Functions**

```scala
case class ProductSalesAnalysis(
  productId: String,
  productName: String,
  saleMonth: String,
  monthlySales: Double,
  firstSaleAmount: Double,      // FIRST_VALUE
  lastSaleAmount: Double,        // LAST_VALUE
  allSaleAmounts: List[Double],  // ARRAY_AGG
  totalSales: Double,            // SUM OVER
  avgSaleAmount: Double,         // AVG OVER
  minSaleAmount: Double,         // MIN OVER
  maxSaleAmount: Double,         // MAX OVER
  saleCount: Long,               // COUNT OVER
  uniqueCustomers: Long          // COUNT DISTINCT OVER
)

// Type-safe execution with compile-time validation
val results: Source[ProductSalesAnalysis, NotUsed] = 
  client.scrollAs[ProductSalesAnalysis]("""
  SELECT
    product_id AS productId,
    product_name AS productName,
    DATE_TRUNC('month', sale_date) AS saleMonth,
    SUM(amount) AS monthlySales,
    
    -- Window functions with different partitions
    FIRST_VALUE(amount) OVER (
      PARTITION BY product_id, DATE_TRUNC('month', sale_date) 
      ORDER BY sale_date ASC
    ) AS firstSaleAmount,
    
    LAST_VALUE(amount) OVER (
      PARTITION BY product_id, DATE_TRUNC('month', sale_date) 
      ORDER BY sale_date ASC
    ) AS lastSaleAmount,
    
    ARRAY_AGG(amount) OVER (
      PARTITION BY product_id 
      ORDER BY sale_date ASC
      LIMIT 100
    ) AS allSaleAmounts,
    
    SUM(amount) OVER (PARTITION BY product_id) AS totalSales,
    AVG(amount) OVER (PARTITION BY product_id) AS avgSaleAmount,
    MIN(amount) OVER (PARTITION BY product_id) AS minSaleAmount,
    MAX(amount) OVER (PARTITION BY product_id) AS maxSaleAmount,
    COUNT(amount) OVER (PARTITION BY product_id) AS saleCount,
    COUNT(DISTINCT customer_id) OVER (PARTITION BY product_id) AS uniqueCustomers
    
  FROM sales
  WHERE sale_date >= '2024-01-01'
  GROUP BY product_id, product_name, DATE_TRUNC('month', sale_date)
  ORDER BY product_id, saleMonth
""")

results.runWith(Sink.foreach { analysis =>
  println(s"""
    Product: ${analysis.productName} (${analysis.productId})
    Month: ${analysis.saleMonth}
    Monthly Sales: ${analysis.monthlySales}
    First Sale: ${analysis.firstSaleAmount}
    Last Sale: ${analysis.lastSaleAmount}
    All Sales: ${analysis.allSaleAmounts.mkString("[", ", ", "]")}
    Total Sales (All Time): ${analysis.totalSales}
    Average Sale: ${analysis.avgSaleAmount}
    Price Range: ${analysis.minSaleAmount} - ${analysis.maxSaleAmount}
    Sale Count: ${analysis.saleCount}
    Unique Customers: ${analysis.uniqueCustomers}
  """)
})
```

#### **Example 1: Translation to Elasticsearch DSL**

Since we use `GROUP BY` there is none non-aggregated fields and the SQL query above is decomposed into only one Elasticsearch query:

**Query: Window Functions Aggregations**

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "sale_date": {
              "gte": "2024-01-01"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "_source": false,
  "aggs": {
    "productId": {
      "terms": {
        "field": "product_id",
        "min_doc_count": 1,
        "order": {
          "_key": "asc"
        }
      },
      "aggs": {
        "allSaleAmounts": {
          "top_hits": {
            "size": 100,
            "sort": [
              {
                "sale_date": {
                  "order": "asc"
                }
              }
            ],
            "_source": {
              "includes": [
                "amount"
              ]
            }
          }
        },
        "totalSales": {
          "sum": {
            "field": "amount"
          }
        },
        "avgSaleAmount": {
          "avg": {
            "field": "amount"
          }
        },
        "minSaleAmount": {
          "min": {
            "field": "amount"
          }
        },
        "maxSaleAmount": {
          "max": {
            "field": "amount"
          }
        },
        "saleCount": {
          "value_count": {
            "field": "amount"
          }
        },
        "uniqueCustomers": {
          "cardinality": {
            "field": "customer_id"
          }
        },
        "saleMonth": {
          "date_histogram": {
            "interval": "1M",
            "min_doc_count": 1,
            "field": "sale_date"
          },
          "aggs": {
            "firstSaleAmount": {
              "top_hits": {
                "size": 1,
                "sort": [
                  {
                    "sale_date": {
                      "order": "asc"
                    }
                  }
                ],
                "_source": {
                  "includes": [
                    "amount"
                  ]
                }
              }
            },
            "lastSaleAmount": {
              "top_hits": {
                "size": 1,
                "sort": [
                  {
                    "sale_date": {
                      "order": "desc"
                    }
                  }
                ],
                "_source": {
                  "includes": [
                    "amount"
                  ]
                }
              }
            }
          }
        },
        "productName": {
          "terms": {
            "field": "product_name",
            "min_doc_count": 1
          },
          "aggs": {
            "saleMonth": {
              "date_histogram": {
                "interval": "1M",
                "min_doc_count": 1,
                "field": "sale_date"
              },
              "aggs": {
                "monthlySales": {
                  "sum": {
                    "field": "amount"
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
```

#### **Example 2: Customer Purchase Patterns**

```scala
case class CustomerPurchasePattern(
  customerId: String,
  customerName: String,
  purchaseDate: String,
  amount: Double,
  firstPurchaseAmount: Double,
  lastPurchaseAmount: Double,
  allPurchaseAmounts: List[Double],
  totalSpent: Double,
  avgPurchaseAmount: Double,
  purchaseCount: Long
)

val sqlQuery = """
  SELECT
    customer_id AS customerId,
    customer_name AS customerName,
    purchase_date AS purchaseDate,
    amount,
    
    FIRST_VALUE(amount) OVER (
      PARTITION BY customer_id 
      ORDER BY purchase_date ASC
    ) AS firstPurchaseAmount,
    
    LAST_VALUE(amount) OVER (
      PARTITION BY customer_id 
      ORDER BY purchase_date ASC
    ) AS lastPurchaseAmount,
    
    ARRAY_AGG(amount) OVER (
      PARTITION BY customer_id 
      ORDER BY purchase_date ASC
      LIMIT 100
    ) AS allPurchaseAmounts,
    
    SUM(amount) OVER (PARTITION BY customer_id) AS totalSpent,
    AVG(amount) OVER (PARTITION BY customer_id) AS avgPurchaseAmount,
    COUNT(*) OVER (PARTITION BY customer_id) AS purchaseCount
    
  FROM purchases
  WHERE purchase_date >= '2024-01-01'
  ORDER BY customer_id, purchase_date
"""

val patterns: Source[CustomerPurchasePattern, NotUsed] = 
  client.scrollAs[CustomerPurchasePattern](sqlQuery)
```

**Execution Strategy for Mixed Queries:**

Since this query includes non-aggregated fields (`customerName`, `purchaseDate`, `amount`), the execution involves:

1. **Window Functions Query**: Computes all `OVER` clause results grouped by `customer_id`
2. **Main Query**: Retrieves individual purchase records with fields
3. **In-Memory Join**: Each purchase record is enriched with window function values matching its `customer_id`

#### **Example 2: Translation to Elasticsearch DSL**

The SQL query above is decomposed into two Elasticsearch queries:

**Query 1: Window Functions Aggregations**

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "purchase_date": {
              "gte": "2024-01-01"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "_source": false,
  "aggs": {
    "customerId": {
      "terms": {
        "field": "customer_id",
        "min_doc_count": 1
      },
      "aggs": {
        "firstPurchaseAmount": {
          "top_hits": {
            "size": 1,
            "sort": [
              {
                "purchase_date": {
                  "order": "asc"
                }
              }
            ],
            "_source": {
              "includes": [
                "amount"
              ]
            }
          }
        },
        "lastPurchaseAmount": {
          "top_hits": {
            "size": 1,
            "sort": [
              {
                "purchase_date": {
                  "order": "desc"
                }
              }
            ],
            "_source": {
              "includes": [
                "amount"
              ]
            }
          }
        },
        "allPurchaseAmounts": {
          "top_hits": {
            "size": 100,
            "sort": [
              {
                "purchase_date": {
                  "order": "asc"
                }
              }
            ],
            "_source": {
              "includes": [
                "amount"
              ]
            }
          }
        },
        "totalSpent": {
          "sum": {
            "field": "amount"
          }
        },
        "avgPurchaseAmount": {
          "avg": {
            "field": "amount"
          }
        },
        "purchaseCount": {
          "value_count": {
            "field": "_index"
          }
        }
      }
    }
  }
}
```

**Query 2: Main Query (Non-Aggregated Fields)**

Since this query includes non-aggregated fields, a separate search query would be executed:

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "purchase_date": {
              "gte": "2024-01-01"
            }
          }
        }
      ]
    }
  },
  "sort": [
    {
      "customerId": {
        "order": "asc"
      }
    },
    {
      "purchaseDate": {
        "order": "asc"
      }
    }
  ],
  "_source": {
    "includes": [
      "customer_id",
      "customer_name",
      "purchase_date",
      "amount"
    ]
  }
}
```

#### **Example 3: Time-Series Analysis with Window Functions**

```scala
val sqlQuery = """
  SELECT
    sensor_id AS sensorId,
    timestamp,
    temperature,
    
    -- Rolling statistics using window functions
    AVG(temperature) OVER (
      PARTITION BY sensor_id 
      ORDER BY timestamp 
    ) AS movingAvg,
    
    MIN(temperature) OVER (PARTITION BY sensor_id) AS minTemp,
    MAX(temperature) OVER (PARTITION BY sensor_id) AS maxTemp,
    
    FIRST_VALUE(temperature) OVER (
      PARTITION BY sensor_id 
      ORDER BY timestamp ASC
    ) AS firstReading,
    
    LAST_VALUE(temperature) OVER (
      PARTITION BY sensor_id 
      ORDER BY timestamp ASC
    ) AS currentReading
    
  FROM sensor_data
  WHERE timestamp >= NOW() - INTERVAL 1 HOUR
  ORDER BY sensor_id, timestamp
"""
```

#### **Example 3: Translation to Elasticsearch DSL**

The SQL query above is decomposed into two Elasticsearch queries:

**Query 1: Window Functions Aggregations**

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "timestamp": {
              "gte": "now-1H"
            }
          }
        }
      ]
    }
  },
  "size": 0,
  "_source": false,
  "aggs": {
    "sensorId": {
      "terms": {
        "field": "sensor_id",
        "min_doc_count": 1
      },
      "aggs": {
        "movingAvg": {
          "avg": {
            "field": "temperature"
          }
        },
        "minTemp": {
          "min": {
            "field": "temperature"
          }
        },
        "maxTemp": {
          "max": {
            "field": "temperature"
          }
        },
        "firstReading": {
          "top_hits": {
            "size": 1,
            "sort": [
              {
                "timestamp": {
                  "order": "asc"
                }
              }
            ],
            "_source": {
              "includes": [
                "temperature"
              ]
            }
          }
        },
        "currentReading": {
          "top_hits": {
            "size": 1,
            "sort": [
              {
                "timestamp": {
                  "order": "desc"
                }
              }
            ],
            "_source": {
              "includes": [
                "temperature"
              ]
            }
          }
        }
      }
    }
  }
}
```
**Query 2: Main Query (Non-Aggregated Fields)**

Since this query includes non-aggregated fields, a separate search query would be executed:

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "range": {
            "timestamp": {
              "gte": "now-1H"
            }
          }
        }
      ]
    }
  },
  "sort": [
    {
      "sensorId": {
        "order": "asc"
      }
    },
    {
      "timestamp": {
        "order": "asc"
      }
    }
  ],
  "_source": {
    "includes": [
      "sensor_id",
      "timestamp",
      "temperature"
    ]
  }
}
```

#### **Performance Considerations**

| Consideration           | Recommendation                                           | Impact            |
|-------------------------|----------------------------------------------------------|-------------------|
| **ARRAY_AGG Size**      | Use `LIMIT` in `OVER` clause to control array size       | Memory usage      |
| **Multiple Partitions** | Different partition keys require separate aggregations   | Query complexity  |
| **ORDER BY in OVER**    | Adds sorting overhead; use only when necessary           | Performance       |
| **Large Result Sets**   | Use `scrollAs[T]` instead of `searchAs[T]` for streaming | Memory efficiency |
| **In-Memory Join**      | Partition keys should have reasonable cardinality        | Memory & CPU      |
| **Mixed Queries**       | Non-aggregated fields require additional search query    | Network & latency |

#### **Best Practices**

✅ **Minimize Partition Keys**: Use the smallest set of partition keys necessary  
✅ **Reuse Partitions**: Group window functions with the same `PARTITION BY` clause  
✅ **Limit Array Sizes**: Control `ARRAY_AGG` result size to prevent memory issues  
✅ **Use Streaming**: Prefer `scrollAs[T]` for large datasets  
✅ **Index Partition Fields**: Ensure partition key fields are indexed in Elasticsearch  
✅ **Monitor Memory**: Track memory usage when joining large result sets

#### **Limitations**

⚠️ **ROWS/RANGE Frames**: Not yet supported  
⚠️ **RANK/ROW_NUMBER**: Not yet supported  
⚠️ **LEAD/LAG**: Not yet supported  
⚠️ **NTILE**: Not yet supported  
⚠️ **Partition Cardinality**: Very high cardinality partitions may impact performance

📖 **[Full Window Functions Documentation](documentation/sql/functions_aggregate.md)**

---

### **3.3. Compile-Time SQL Query Validation**

SoftClient4ES provides **compile-time validation** for SQL queries used with type-safe methods like `searchAs[T]` and `scrollAs[T]`. This ensures that your queries are compatible with your Scala case classes **before your code even runs**, preventing runtime deserialization errors.

#### **Why Compile-Time Validation?**

- ✅ **Catch Errors Early**: Detect missing fields, typos, and type mismatches at compile-time
- ✅ **Type Safety**: Ensure SQL queries match your domain models
- ✅ **Better Developer Experience**: Get helpful error messages with suggestions
- ✅ **Prevent Runtime Failures**: No more Jackson deserialization exceptions in production

#### **Validated Operations**

| Validation             | Description                                            | Level      |
|------------------------|--------------------------------------------------------|------------|
| **SELECT * Rejection** | Prohibits `SELECT *` to ensure compile-time validation | ❌ ERROR    |
| **Required Fields**    | Verifies that all required fields are selected         | ❌ ERROR    |
| **Unknown Fields**     | Detects fields that don't exist in the case class      | ⚠️ WARNING |
| **Nested Objects**     | Validates the structure of nested objects              | ❌ ERROR    |
| **Nested Collections** | Validates the use of UNNEST for collections            | ❌ ERROR    |
| **Type Compatibility** | Checks compatibility between SQL and Scala types       | ❌ ERROR    |

#### **Example 1: Missing Required Field with Nested Object**

```scala
case class Address(
  street: String,
  city: String,
  country: String
)

case class User(
  id: String,
  name: String,
  address: Address  // ❌ Required nested object
)

// ❌ COMPILE ERROR: Missing required field 'address'
client.searchAs[User]("SELECT id, name FROM users")
```

**Compile Error:**

```
❌ SQL query does not select the required field: address

Example query:
SELECT id, name, address FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

**✅ Solution:**

```scala
// Option 1: Select the entire nested object (recommended)
client.searchAs[User]("SELECT id, name, address FROM users")

// Option 2: Make the field optional
case class User(
  id: String,
  name: String,
  address: Option[Address] = None
)
client.searchAs[User]("SELECT id, name FROM users")
```

#### **Example 2: Typo Detection with Smart Suggestions**

```scala
case class Product(
  id: String,
  name: String,
  price: Double,
  stock: Int
)

// ❌ COMPILE ERROR: Typo in 'name' -> 'nam'
client.searchAs[Product]("SELECT id, nam, price, stock FROM products")
```

**Compile Error:**
```
❌ SQL query does not select the required field: name
You have selected unknown field "nam", did you mean "name"?

Example query:
SELECT id, price, stock, name FROM ...

To fix this, either:
  1. Add it to the SELECT clause
  2. Make it Option[T] in the case class
  3. Provide a default value in the case class definition
```

**✅ Solution:**
```scala
// Fix the typo
client.searchAs[Product]("SELECT id, name, price, stock FROM products")
```

#### **Dynamic Queries (Skip Validation)**

For dynamic SQL queries where validation isn't possible, use the `*Unchecked` variants:

```scala
val dynamicQuery = buildQueryAtRuntime()

// ✅ Skip compile-time validation for dynamic queries
client.searchAsUnchecked[Product](SQLQuery(dynamicQuery))
client.scrollAsUnchecked[Product](dynamicQuery)
```

---

📖 **[Full SQL Validation Documentation](documentation/sql/validation.md)**

📖 **[Full SQL Documentation](documentation/sql/README.md)**

---

### **4. High-Performance Bulk API with Akka Streams**

The **BulkApi** leverages Akka Streams for efficient, backpressure-aware bulk operations.

#### **Features**

✅ **Streaming Architecture**: Process millions of documents without memory issues  
✅ **Multiple data sources**: In-memory, File-based (JSON, JSON Array, Parquet, Delta Lake)  
✅ **Backpressure Handling**: Automatic flow control based on Elasticsearch capacity  
✅ **Error Recovery**: Configurable retry strategies  
✅ **Batching**: Automatic batching for optimal performance  
✅ **Parallel Processing**: Concurrent bulk requests with configurable parallelism

#### Data Sources

| Source Type      | Format        | Description                           |
|------------------|---------------|---------------------------------------|
| **In-Memory**    | Scala objects | Direct streaming from collections     |
| **JSON**         | Text          | Newline-delimited JSON (NDJSON)       |
| **JSON Array**   | Text          | JSON array with nested structures     |
| **Parquet**      | Binary        | Columnar storage format               |
| **Delta Lake**   | Directory     | ACID transactional data lake          |

#### Operation Types

| Operation  | Action         | Behavior                                  |
|------------|----------------|-------------------------------------------|
| **INDEX**  | Insert/Replace | Creates or replaces entire document       |
| **UPDATE** | Upsert         | Updates existing or creates new (partial) |
| **DELETE** | Remove         | Deletes document by ID                    |

**Examples:**

```scala
// High-performance file indexing
implicit val options: BulkOptions = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 10000,
  balance = 16,
  disableRefresh = true
)

implicit val hadoopConf: Configuration = new Configuration()

// Load from Parquet
client.bulkFromFile(
  filePath = "/data/products.parquet",
  format = Parquet,
  idKey = Some("id")
).foreach { result =>
  result.indices.foreach(client.refresh)
  println(s"Indexed ${result.successCount} docs at ${result.metrics.throughput} docs/sec")
}

// Load from Delta Lake
client.bulkFromFile(
  filePath = "/data/delta-products",
  format = Delta,
  idKey = Some("id"),
  update = Some(true)
).foreach { result =>
  println(s"Updated ${result.successCount} products from Delta Lake")
}

// Load JSON Array with nested objects
client.bulkFromFile(
  filePath = "/data/persons.json",
  format = JsonArray,
  idKey = Some("uuid")
).foreach { result =>
  println(s"Indexed ${result.successCount} persons with nested structures")
}
```

---

### **5. Smart Scroll API with Automatic Strategy Detection**

The **ScrollApi** provides efficient retrieval of large datasets with automatic selection of the optimal scrolling strategy.

#### **Supported Strategies**

| Strategy                               | Elasticsearch Version  | Use Case                    |
|----------------------------------------|------------------------|-----------------------------|
| **Point-in-Time (PIT) + search_after** | 7.10+                  | Best performance, stateless |
| **search_after**                       | 6.5+                   | Good for deep pagination    |
| **Classic Scroll**                     | All versions           | Legacy support              |

**Example:**

```scala
// Simple SQL query
val query = SQLQuery(
  query = """
    SELECT id, name, price, category
    FROM products
    WHERE price > 100
    ORDER BY price DESC
  """
)

// Automatically selects the best strategy
client.scroll(query).runWith(Sink.foreach { case (doc, metrics) =>
  processDocument(doc)
  println(s"Progress: ${metrics.totalDocuments} docs, ${metrics.documentsPerSecond} docs/sec")
})
```

---

### **6. Akka Persistence Integration**

Seamlessly integrate Elasticsearch with [event-sourced systems](https://github.com/SOFTNETWORK-APP/generic-persistence-api) using Akka Persistence.

**Example:**

```scala
/** The in-memory state of the entity actor
 */
trait State {
  def uuid: String
}

trait Timestamped extends State {
  def createdDate: Instant
  def lastUpdated: Instant
}

case class Person(
  uuid: String,
  name: String,
  birthDate: String,
  createdDate: Instant,
  lastUpdated: Instant
) extends Timestamped

sealed trait Event

/** Crud events * */
trait CrudEvent extends Event

trait Created[T <: Timestamped] extends CrudEvent {
  def document: T
}

trait Updated[T <: Timestamped] extends CrudEvent {
  def document: T
  def upsert: Boolean = true
}

trait Deleted extends CrudEvent {
  def uuid: String
}

sealed trait PersonEvent extends CrudEvent
case class PersonCreatedEvent(document: Person) extends Created[Person] with PersonEvent
case class PersonUpdatedEvent(document: Person) extends Updated[Person] with PersonEvent
case class PersonDeletedEvent(uuid: String) extends Deleted with PersonEvent

import app.softnetwork.elastic.persistence.query._
import app.softnetwork.persistence.query._

trait PersonToElasticProcessorStream
  extends State2ElasticProcessorStream[Person, PersonEvent] {
  _: JournalProvider with OffsetProvider =>
}

/** Command objects * */
trait Command

sealed trait PersonCommand extends Command
case class AddPerson(name: String, birthDate: String) extends PersonCommand
case class UpdatePerson(name: String, birthDate: String) extends PersonCommand
case object DeletePerson extends PersonCommand

/** Command result * */
trait CommandResult

sealed trait PersonCommandResult extends CommandResult
case class PersonAdded(uuid: String) extends PersonCommandResult
case object PersonUpdated extends PersonCommandResult
case object PersonDeleted extends PersonCommandResult

import app.softnetwork.persistence.typed._

trait PersonBehavior
  extends TimeStampedBehavior[PersonCommand, Person, PersonEvent, PersonCommandResult] {

  override protected val manifestWrapper: ManifestW = ManifestW()

  /** associate a set of tags to an event before the latter will be appended to the event log
   *
   * This allows events to be easily filtered and queried based on their tags, improving the
   * efficiency of read-side projections
   *
   * @param entityId
   *   - entity id
   * @param event
   *   - the event to tag
   * @return
   *   set of tags to associate to this event
   */
  override protected def tagEvent(entityId: String, event: PersonEvent): Set[String] = {
    Set(s"${persistenceId.toLowerCase}-to-external", persistenceId)
  }

  /** @param entityId
   *   - entity identity
   * @param state
   *   - current state
   * @param command
   *   - command to handle
   * @param replyTo
   *   - optional actor to reply to
   * @param timers
   *   - scheduled messages associated with this entity behavior
   * @return
   *   effect
   */
  override def handleCommand(
    entityId: String,
    state: Option[Person],
    command: PersonCommand,
    replyTo: Option[ActorRef[PersonCommandResult]],
    timers: TimerScheduler[PersonCommand]
  )(implicit context: ActorContext[PersonCommand]): Effect[PersonEvent, Option[Person]] = {
    command match {
      case cmd: AddPerson =>
        import cmd._
        Effect
          .persist(PersonCreatedEvent(Person(entityId, name, birthDate)))
          .thenRun(_ => PersonAdded(entityId) ~> replyTo)
      case cmd: UpdatePerson =>
        state match {
          case Some(person) =>
            import cmd._
            Effect
              .persist(
                PersonUpdatedEvent(
                  person.copy(name = name, birthDate = birthDate, lastUpdated = persistence.now())
                )
              )
              .thenRun(_ => PersonUpdated ~> replyTo)
          case _ =>
            Effect.none.thenRun(_ => PersonNotFound ~> replyTo)
        }
      case DeletePerson =>
        state match {
          case Some(_) =>
            Effect
              .persist(PersonDeletedEvent(entityId))
              .thenRun(_ => PersonDeleted ~> replyTo)
          case _ =>
            Effect.none.thenRun(_ => PersonNotFound ~> replyTo)
        }
      case _ => super.handleCommand(entityId, state, command, replyTo, timers)
    }
  }
}
```

---

## **Advanced Features**

### **Robust Error Handling**

All operations return a type-safe `ElasticResult[T]` that encapsulates success or failure:

```scala
sealed trait ElasticResult[+T] {
  def isSuccess: Boolean
  def isFailure: Boolean
  def get: T
  def error: Option[ElasticError]
}

case class ElasticSuccess[T](value: T) extends ElasticResult[T]
case class ElasticFailure[T](error: ElasticError) extends ElasticResult[T]

case class ElasticError(
  message: String,
  cause: Option[Throwable] = None,
  statusCode: Option[Int] = None,
  index: Option[String] = None,
  operation: Option[String] = None
)
```

### **Comprehensive Validation**

- ✅ Index name validation (lowercase, no special characters)
- ✅ JSON syntax validation
- ✅ Mapping compatibility checks
- ✅ Settings validation

### **Production-Ready Logging**

Structured logging with different levels for debugging and monitoring:

```scala
logger.debug("Checking if index 'users' exists")
logger.info("✅ Index 'users' created successfully")
logger.warn("Rolling back migration for 'users'")
logger.error("❌ Migration failed: Reindex error")
```

---

## **Getting Started**

### **Installation**

Add to your `build.sbt`:

```scala
ThisBuild / resolvers ++= Seq(
  "Softnetwork Server" at "https://softnetwork.jfrog.io/artifactory/releases/",
  //...
)

// For Elasticsearch 6
// Using Jest client
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es6-jest-client" % 0.15.0
// Or using Rest High Level client
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es6-rest-client" % 0.15.0

// For Elasticsearch 7
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es7-rest-client" % 0.15.0

// For Elasticsearch 8
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es8-java-client" % 0.15.0

// For Elasticsearch 9
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es9-java-client" % 0.15.0
```

### **Quick Example**

```scala
import app.softnetwork.elastic.client._
import scala.concurrent.ExecutionContext.Implicits.global

// Initialize client
val client = ElasticClientFactory.create()

// Create index with mapping
val mapping = """{
  "properties": {
    "name": {"type": "text"},
    "email": {"type": "keyword"}
  }
}"""

client.createIndex("users", mapping) match {
  case ElasticSuccess(true) => 
    // Index documents
    client.index("users", "user-1", """{"name":"Alice","email":"alice@example.com"}""", wait = true)
    
    // Search
    val results = client.singleSearch(ElasticQuery(query = """{"query":{"match":{"name":"Alice"}}}""", indices = Seq("users")))
    
  case ElasticFailure(error) => 
    println(s"Failed to create index: ${error.message}")
}
```

---

## **Roadmap**

### **Short-term**

- [ ] Full **JDBC connector for Elasticsearch**

### **Long-term**

- [ ] Advanced monitoring and metrics

---

## **Documentation**

- 📖 **[Complete API Documentation](documentation/client/README.md)**
- 📖 **[SQL Query Guide](documentation/sql/README.md)**

[//]: # (- 📖 **[Best Practices]&#40;documentation/best-practices.md&#41;**)

---

## **Contributing**

Contributions are welcome! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## **License**

This project is open source and licensed under the **Apache License 2.0**.

---

## **Support**

- 🐛 **Issues** : [GitHub Issues](https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues)
- 💬 **Discussions**: [GitHub Discussions](https://github.com/SOFTNETWORK-APP/SoftClient4ES/discussions)
- 📧 **Email**: admin@softnetwork.fr

---

**Built with ❤️ by the SoftNetwork team**