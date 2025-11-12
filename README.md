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

| API                                                        | Description                                                                         | Documentation                                   |
|------------------------------------------------------------|-------------------------------------------------------------------------------------|-------------------------------------------------|
| **[RefreshApi](documentation/client/refresh.md)**          | Control index refresh for real-time search                                          | [📖 Docs](documentation/client/refresh.md)      |
| **[IndicesApi](documentation/client/indices.md)**          | Create, update, and manage indices with settings and mappings                       | [📖 Docs](documentation/client/indices.md)      |
| **[SettingsApi](documentation/client/settings.md)**        | Dynamic index settings management                                                   | [📖 Docs](documentation/client/settings.md)     |
| **[AliasApi](documentation/client/aliases.md)**            | Manage index aliases for zero-downtime deployments                                  | [📖 Docs](documentation/client/aliases.md)      |
| **[MappingApi](documentation/client/mappings.md)**         | Smart mapping management with automatic migration and rollback                      | [📖 Docs](documentation/client/mappings.md)     |
| **[IndexApi](documentation/client/index.md)**              | Index documents                                                                     | [📖 Docs](documentation/client/index.md)        |
| **[UpdateApi](documentation/client/update.md)**            | Partial document updates with script support                                        | [📖 Docs](documentation/client/update.md)       |
| **[DeleteApi](documentation/client/delete.md)**            | Delete documents by ID or query                                                     | [📖 Docs](documentation/client/delete.md)       |
| **[BulkApi](documentation/client/bulk.md)**                | High-performance bulk operations with Akka Streams                                  | [📖 Docs](documentation/client/bulk.md)         |
| **[GetApi](documentation/client/get.md)**                  | Get documents by ID                                                                 | [📖 Docs](documentation/client/get.md)          |
| **[SearchApi](documentation/client/search.md)**            | Advanced search with SQL and aggregations support                                   | [📖 Docs](documentation/client/search.md)       |
| **[ScrollApi](documentation/client/scroll.md)**            | Stream large datasets with automatic strategy detection (PIT, search_after, scroll) | [📖 Docs](documentation/client/scroll.md)       |
| **[AggregationApi](documentation/client/aggregations.md)** | Type-safe way to execute aggregations using SQL queries                             | [📖 Docs](documentation/client/aggregations.md) |

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

### **3. SQL compatible **

### **3.1 SQL to Elasticsearch Query DSL**

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

**Example:**

```scala
val sqlQuery = """
  SELECT
    min(inner_products.price) as min_price,
    max(inner_products.price) as max_price
  FROM
    stores store
    JOIN UNNEST(store.products) as inner_products
  WHERE
    (
      firstName is not null AND
      lastName is not null AND
      description is not null AND
      preparationTime <= 120 AND
      store.deliveryPeriods.dayOfWeek=6 AND
      blockedCustomers not like "%uuid%" AND
      NOT receiptOfOrdersDisabled=true AND
      (
        distance(pickup.location, POINT(0.0, 0.0)) <= 7000 m OR
        distance(withdrawals.location, POINT(0.0, 0.0)) <= 7000 m
      )
    )
  GROUP BY
    inner_products.category
  HAVING inner_products.deleted=false AND
    inner_products.upForSale=true AND
    inner_products.stock > 0 AND
    match (
      inner_products.name,
      inner_products.description,
      inner_products.ingredients
    ) against ("lasagnes") AND
    min(inner_products.price) > 5.0 AND
    max(inner_products.price) < 50.0 AND
    inner_products.category <> "coffee"
  LIMIT 10 OFFSET 0
"""

val results = client.search(SQLQuery(sqlQuery))
```

```json
{
  "query": {
    "bool": {
      "filter": [
        {
          "bool": {
            "filter": [
              {
                "exists": {
                  "field": "firstName"
                }
              },
              {
                "exists": {
                  "field": "lastName"
                }
              },
              {
                "exists": {
                  "field": "description"
                }
              },
              {
                "range": {
                  "preparationTime": {
                    "lte": 120
                  }
                }
              },
              {
                "term": {
                  "deliveryPeriods.dayOfWeek": {
                    "value": 6
                  }
                }
              },
              {
                "bool": {
                  "must_not": [
                    {
                      "regexp": {
                        "blockedCustomers": {
                          "value": ".*uuid.*"
                        }
                      }
                    }
                  ]
                }
              },
              {
                "bool": {
                  "must_not": [
                    {
                      "term": {
                        "receiptOfOrdersDisabled": {
                          "value": true
                        }
                      }
                    }
                  ]
                }
              },
              {
                "bool": {
                  "should": [
                    {
                      "geo_distance": {
                        "distance": "7000m",
                        "pickup.location": [
                          0.0,
                          0.0
                        ]
                      }
                    },
                    {
                      "geo_distance": {
                        "distance": "7000m",
                        "withdrawals.location": [
                          0.0,
                          0.0
                        ]
                      }
                    }
                  ]
                }
              }
            ]
          }
        }
      ]
    }
  },
  "size": 0,
  "min_score": 1.0,
  "_source": true,
  "aggs": {
    "inner_products": {
      "nested": {
        "path": "products"
      },
      "aggs": {
        "filtered_inner_products": {
          "filter": {
            "bool": {
              "filter": [
                {
                  "bool": {
                    "must_not": [
                      {
                        "term": {
                          "products.category": {
                            "value": "coffee"
                          }
                        }
                      }
                    ]
                  }
                },
                {
                  "match_all": {}
                },
                {
                  "match_all": {}
                },
                {
                  "bool": {
                    "should": [
                      {
                        "match": {
                          "products.name": {
                            "query": "lasagnes"
                          }
                        }
                      },
                      {
                        "match": {
                          "products.description": {
                            "query": "lasagnes"
                          }
                        }
                      },
                      {
                        "match": {
                          "products.ingredients": {
                            "query": "lasagnes"
                          }
                        }
                      }
                    ]
                  }
                },
                {
                  "range": {
                    "products.stock": {
                      "gt": 0
                    }
                  }
                },
                {
                  "term": {
                    "products.upForSale": {
                      "value": true
                    }
                  }
                },
                {
                  "term": {
                    "products.deleted": {
                      "value": false
                    }
                  }
                }
              ]
            }
          },
          "aggs": {
            "cat": {
              "terms": {
                "field": "products.category.keyword"
              },
              "aggs": {
                "min_price": {
                  "min": {
                    "field": "products.price"
                  }
                },
                "max_price": {
                  "max": {
                    "field": "products.price"
                  }
                },
                "having_filter": {
                  "bucket_selector": {
                    "buckets_path": {
                      "min_price": "inner_products>min_price",
                      "max_price": "inner_products>max_price"
                    },
                    "script": {
                      "source": "params.min_price > 5.0 && params.max_price < 50.0"
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

### **3.2. Compile-Time SQL Query Validation**

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

📖 **[Full SQL Validation Documentation](documentation/sql/validation.md)**

📖 **[Full SQL Documentation](documentation/sql/README.md)**

---

### **4. High-Performance Bulk API with Akka Streams**

The **BulkApi** leverages Akka Streams for efficient, backpressure-aware bulk operations.

#### **Features**

✅ **Streaming Architecture**: Process millions of documents without memory issues  
✅ **Backpressure Handling**: Automatic flow control based on Elasticsearch capacity  
✅ **Error Recovery**: Configurable retry strategies  
✅ **Batching**: Automatic batching for optimal performance  
✅ **Parallel Processing**: Concurrent bulk requests with configurable parallelism

**Example:**

```scala
import akka.stream.scaladsl._

// Stream-based bulk indexing
val documents: List[String] = List(
  """{"id":"user-1","name":"Alice","age":30}""",
  """{"id":"user-2","name":"Bob","age":25}""",
  """{"id":"user-3","name":"Charlie","age":35}"""
)

implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "users")
client.bulkSource(Source.fromIterator(() => documents), identity, indexKey=Some("id"))
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
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es6-jest-client" % 0.11.0
// Or using Rest High Level client
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es6-rest-client" % 0.11.0

// For Elasticsearch 7
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es7-rest-client" % 0.11.0

// For Elasticsearch 8
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es8-java-client" % 0.11.0

// For Elasticsearch 9
libraryDependencies += "app.softnetwork.elastic" %% s"softclient4es9-java-client" % 0.11.0
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

- [ ] Support for `INSERT`, `UPDATE`, `DELETE` SQL operations
- [ ] Support for `CREATE TABLE`, `ALTER TABLE` SQL operations

### **Long-term**

- [ ] Full **JDBC connector for Elasticsearch**
- [ ] GraphQL query support
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