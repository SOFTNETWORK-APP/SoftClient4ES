[Back to index](README.md)

# SEARCH API

## Table of Contents

- [Core Concepts](#core-concepts)
- [Synchronous Search](#synchronous-search)
  - [search](#search)
  - [singleSearch](#singlesearch)
  - [multiSearch](#multisearch)
- [Asynchronous Search](#asynchronous-search)
  - [searchAsync](#searchasync)
  - [singleSearchAsync](#singlesearchasync)
  - [multiSearchAsync](#multisearchasync)
- [Search with Type Conversion](#search-with-type-conversion)
  - [searchAsUnchecked](#searchasunchecked)
  - [searchAs](#searchas)
  - [singleSearchAs](#singlesearchas)
  - [multisearchAs](#multisearchas)
- [Asynchronous Search with Type Conversion](#asynchronous-search-with-type-conversion)
  - [searchAsyncAsUnchecked](#searchasyncasunchecked)
  - [searchAsyncAs](#searchasyncas)
  - [singleSearchAsyncAs](#singlesearchasyncas)
  - [multiSearchAsyncAs](#multisearchasyncas)
- [Implementation Requirements](#implementation-requirements)
  - [executeSingleSearch](#executesinglesearch)
  - [executeMultiSearch](#executemultisearch)
  - [executeSingleSearchAsync](#executesinglesearchasync)
  - [executeMultiSearchAsync](#executemultisearchasync)
- [Common Patterns](#common-patterns)
- [Performance Optimization](#performance-optimization)
- [Error Handling](#error-handling)
- [Testing Scenarios](#testing-scenarios)
- [Best Practices](#best-practices)
- [SQL Query Search](#sql-query-search)
- [SQL Query Patterns](#sql-query-patterns)
- [SQL Query Best Practices](#best-practices-for-sql-queries)
- [Summary](#summary)

## Overview

The **SearchApi** trait provides comprehensive search functionality for Elasticsearch with support for - [SQL Queries](../sql/README.md), native Elasticsearch queries, aggregations, and automatic type conversion. It offers both synchronous and asynchronous operations with unified error handling via `ElasticResult`.

**Features:**
- **SQL query support** with automatic conversion to Elasticsearch DSL
- **Native Elasticsearch query execution**
- **Single and multi-search operations**
- **Automatic type conversion** to Scala case classes
- **Field aliasing** for query result mapping
- **Aggregation support** (SQL and Elasticsearch)
- **Synchronous and asynchronous operations**
- **Comprehensive error handling and validation**
- **Query validation** before execution

**Dependencies:**
- Requires `ElasticConversion` for type conversion
- Requires `ElasticClientHelpers` for validation and utilities

**Large Result Sets - Scroll API**

For searching large datasets that don't fit in a single response, use the **dedicated Scroll API** instead of regular search methods.

See [Scroll API](scroll.md) documentation for complete implementation

**When to use Scroll API:**
- Retrieving more than 10,000 documents
- Exporting large datasets
- Processing all documents in an index
- Batch processing operations

---

## Core Concepts

### Query Types

**1. SQL Query**
```scala
case class SQLQuery(
  query: String,                              // SQL query string
  score: Option[Double] = None                // Optional minimum score
)

// Example
val sqlQuery = SQLQuery(
  query = "SELECT * FROM products WHERE price > 100",
  score = Some(1.0)
)
```

**2. Elasticsearch Query**
```scala
case class ElasticQuery(
  query: String,                              // JSON query
  indices: Seq[String]                        // Target indices
)

// Example
val elasticQuery = ElasticQuery(
  query = """{"query": {"match": {"name": "laptop"}}}""",
  indices = Seq("products")
)
```

**3. Multi-Search**
```scala
case class ElasticQueries(
  queries: List[ElasticQuery]                 // Multiple queries
)
```

### Response Types

```scala

object AggregationType extends Enumeration {
  type AggregationType = Value
  val Count, Min, Max, Avg, Sum, FirstValue, LastValue, ArrayAgg = Value
}

case class ClientAggregation(
  aggName: String,                          // aggregation name 
  aggType: AggregationType.AggregationType, // aggregation type
  distinct: Boolean                         // distinct values for multivalued aggregations
) {
  def multivalued: Boolean = aggType == AggregationType.ArrayAgg
  def singleValued: Boolean = !multivalued
}

case class ElasticResponse(
  query: String,                               // Original query
  response: String,                            // Raw JSON response
  fieldAliases: Map[String, String],           // Field name mappings
  aggregations: Map[String, ClientAggregation] // Aggregation definitions
)
```

---

## Synchronous Search

### search

Executes a search using an SQL query.

**Signature:**

```scala
def search(sql: SQLQuery): ElasticResult[ElasticResponse]
```

**Parameters:**
- `sql` - SQL query containing the search request

**Returns:**
- `ElasticSuccess[ElasticResponse]` with search results
- `ElasticFailure` with error details (400 for invalid query)

**Behavior:**
- Validates SQL query structure
- Converts SQL to Elasticsearch query
- Executes single or multi-search based on query type
- Returns raw Elasticsearch response

**Examples:**

```scala
// Basic SQL search
val sqlQuery = SQLQuery(
  query = "SELECT * FROM products WHERE category = 'electronics'"
)

client.search(sqlQuery) match {
  case ElasticSuccess(response) =>
    println(s"✅ Found results: ${response.response}")
    // Process raw JSON response
    
  case ElasticFailure(error) =>
    println(s"❌ Search failed: ${error.message}")
}

// SQL with aggregations
val aggregationQuery = SQLQuery(
  query = "SELECT category, AVG(price) FROM products GROUP BY category"
)

client.search(aggregationQuery) match {
  case ElasticSuccess(response) =>
    // Access aggregations
    response.aggregations.foreach { case (name, agg) =>
      println(s"Aggregation: $name")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// Multi-search with SQL
val multiQuery = SQLQuery(
  query = """
        SELECT * FROM products WHERE category = 'electronics'
        UNION ALL
        SELECT * FROM products WHERE category = 'books'
        UNION ALL
        SELECT * FROM products WHERE category = 'clothing'
      """
)

client.search(multiQuery) match {
  case ElasticSuccess(response) =>
    println(s"✅ Multi-search completed")
    // Response contains combined results
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}

// Error handling for invalid query
val invalidQuery = SQLQuery(
  query = "INVALID SQL SYNTAX"
)

client.search(invalidQuery) match {
  case ElasticFailure(error) =>
    assert(error.message.contains("valid search request"))
    assert(error.operation.contains("search"))
}

// Monadic composition
val result = for {
  response1 <- client.search(query1)
  response2 <- client.search(query2)
  combined = combineResponses(response1, response2)
} yield combined
```

---

### singleSearch

Executes a single Elasticsearch query.

**Signature:**

```scala
def singleSearch(
  elasticQuery: ElasticQuery,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
): ElasticResult[ElasticResponse]
```

**Parameters:**
- `elasticQuery` - Elasticsearch query with target indices
- `fieldAliases` - Field name mappings for result conversion
- `aggregations` - Aggregation definitions for multivalued aggregations conversion

**Returns:**
- `ElasticSuccess[ElasticResponse]` with search results
- `ElasticFailure` with error details (400 for invalid JSON)

**Validation:**
- JSON syntax validation before execution

**Examples:**

```scala
// Basic search
val query = ElasticQuery(
  query = """
  {
    "query": {
      "match": {
        "name": "laptop"
      }
    }
  }
  """,
  indices = Seq("products")
)

client.singleSearch(query, Map.empty, Map.empty) match {
  case ElasticSuccess(response) =>
    println(s"✅ Search completed in indices: ${query.indices.mkString(",")}")
    
  case ElasticFailure(error) =>
    println(s"❌ Search failed: ${error.message}")
}

// Search with field aliases
val fieldAliases = Map(
  "product_name" -> "name",
  "product_price" -> "price"
)

val queryWithAliases = ElasticQuery(
  query = """{"query": {"match_all": {}}}""",
  indices = Seq("products")
)

client.singleSearch(queryWithAliases, fieldAliases, Map.empty) match {
  case ElasticSuccess(response) =>
    // Field aliases applied to results
    println(s"✅ Results with aliases: ${response.fieldAliases}")
}

// Multi-index search
val multiIndexQuery = ElasticQuery(
  query = """{"query": {"term": {"status": "active"}}}""",
  indices = Seq("products", "inventory", "catalog")
)

client.singleSearch(multiIndexQuery, Map.empty, Map.empty)

// Complex query with filters
val complexQuery = ElasticQuery(
  query = """
  {
    "query": {
      "bool": {
        "must": [
          {"range": {"price": {"gte": 100, "lte": 1000}}},
          {"term": {"category": "electronics"}}
        ],
        "filter": [
          {"term": {"in_stock": true}}
        ]
      }
    },
    "sort": [{"price": "asc"}],
    "size": 100
  }
  """,
  indices = Seq("products")
)

client.singleSearch(complexQuery, Map.empty, Map.empty)

// Invalid JSON handling
val invalidQuery = ElasticQuery(
  query = """{"query": INVALID_JSON}""",
  indices = Seq("products")
)

client.singleSearch(invalidQuery, Map.empty, Map.empty) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid query"))
}
```

---

### multiSearch

Executes multiple Elasticsearch queries in a single request.

**Signature:**

```scala
def multiSearch(
  elasticQueries: ElasticQueries,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
): ElasticResult[ElasticResponse]
```

**Parameters:**
- `elasticQueries` - Multiple Elasticsearch queries
- `fieldAliases` - Field name mappings for result conversion
- `aggregations` - Aggregation definitions for multivalued aggregations conversion

**Returns:**
- `ElasticSuccess[ElasticResponse]` with combined results
- `ElasticFailure` with error details (400 for invalid queries)

**Validation:**
- Validates all queries before execution
- Returns errors for any invalid query

**Examples:**

```scala
// Basic multi-search
val queries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """{"query": {"match": {"category": "electronics"}}}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": {"match": {"category": "books"}}}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": {"match": {"status": "completed"}}}""",
      indices = Seq("orders")
    )
  )
)

client.multiSearch(queries, Map.empty, Map.empty) match {
  case ElasticSuccess(response) =>
    println(s"✅ Multi-search completed with ${queries.queries.size} queries")
    // Response contains combined results from all queries
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}

// Multi-search across different indices
val crossIndexQueries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """{"query": {"term": {"user_id": "user-123"}}}""",
      indices = Seq("orders")
    ),
    ElasticQuery(
      query = """{"query": {"term": {"user_id": "user-123"}}}""",
      indices = Seq("reviews")
    ),
    ElasticQuery(
      query = """{"query": {"term": {"user_id": "user-123"}}}""",
      indices = Seq("wishlist")
    )
  )
)

client.multiSearch(crossIndexQueries, Map.empty, Map.empty) match {
  case ElasticSuccess(response) =>
    println("✅ Retrieved user data from multiple indices")
}

// Multi-search with aggregations
val aggregationQueries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """
      {
        "query": {"match_all": {}},
        "aggs": {"avg_price": {"avg": {"field": "price"}}}
      }
      """,
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """
      {
        "query": {"match_all": {}},
        "aggs": {"total_orders": {"value_count": {"field": "order_id"}}}
      }
      """,
      indices = Seq("orders")
    )
  )
)

client.multiSearch(aggregationQueries, Map.empty, Map.empty)

// Error handling for invalid queries
val mixedQueries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """{"query": {"match": {"name": "valid"}}}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": INVALID}""",
      indices = Seq("products")
    )
  )
)

client.multiSearch(mixedQueries, Map.empty, Map.empty) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid queries"))
    // Contains information about which query failed
}
```

---

## Asynchronous Search

### searchAsync

Asynchronously executes a search using an SQL query.

**Signature:**

```scala
def searchAsync(
  sqlQuery: SQLQuery
)(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]]
```

**Parameters:**
- `sqlQuery` - SQL query containing the search request
- `ec` - Implicit ExecutionContext

**Returns:**
- `Future[ElasticResult[ElasticResponse]]` that completes with search results

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// Basic async search
val sqlQuery = SQLQuery(
  query = "SELECT * FROM products WHERE price > 100"
)

client.searchAsync(sqlQuery).onComplete {
  case Success(ElasticSuccess(response)) =>
    println(s"✅ Async search completed")
    
  case Success(ElasticFailure(error)) =>
    println(s"❌ Search failed: ${error.message}")
    
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Chained async operations
val result: Future[ElasticResult[CombinedData]] = for {
  response1 <- client.searchAsync(query1)
  response2 <- client.searchAsync(query2)
  combined = combineResults(response1, response2)
} yield combined

// Parallel async searches
val searches = List(query1, query2, query3)

val futures = searches.map(query => client.searchAsync(query))

Future.sequence(futures).map { results =>
  results.foreach {
    case ElasticSuccess(response) => println(s"✅ Success")
    case ElasticFailure(error) => println(s"❌ Failed: ${error.message}")
  }
}
```

---

### singleSearchAsync

Asynchronously executes a single Elasticsearch query.

**Signature:**

```scala
def singleSearchAsync(
  elasticQuery: ElasticQuery,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]]
```

**Examples:**

```scala
val query = ElasticQuery(
  query = """{"query": {"match": {"name": "laptop"}}}""",
  indices = Seq("products")
)

client.singleSearchAsync(query, Map.empty, Map.empty).foreach {
  case ElasticSuccess(response) =>
    println("✅ Async search completed")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### multiSearchAsync

Asynchronously executes multiple Elasticsearch queries.

**Signature:**

```scala
def multiSearchAsync(
  elasticQueries: ElasticQueries,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit ec: ExecutionContext): Future[ElasticResult[ElasticResponse]]
```

**Examples:**

```scala
val queries = ElasticQueries(
  queries = List(query1, query2, query3)
)

client.multiSearchAsync(queries, Map.empty, Map.empty).foreach {
  case ElasticSuccess(response) =>
    println(s"✅ Multi-search completed with ${queries.queries.size} queries")
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}
```

---

## Search with Type Conversion

### searchAsUnchecked

Searches and automatically converts results to typed entities using an SQL query.

**Signature:**

```scala
def searchAs[U](
  sqlQuery: SQLQuery
)(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]]
```

**Parameters:**
- `sqlQuery` - SQL query
- `m` - Implicit Manifest for type information
- `formats` - Implicit JSON serialization formats

**Returns:**
- `ElasticSuccess[Seq[U]]` with typed entities
- `ElasticFailure` with conversion or search errors

**Examples:**

```scala
import org.json4s.DefaultFormats

implicit val formats: Formats = DefaultFormats

// Domain model
case class Product(
  id: String,
  name: String,
  price: Double,
  category: String
)

// Search and convert to typed entities
val sqlQuery = SQLQuery(
  query = "SELECT * FROM products WHERE category = 'electronics'"
)

client.searchAs[Product](sqlQuery) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} products")
    products.foreach { product =>
      println(s"Product: ${product.name}, Price: ${product.price}")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Search failed: ${error.message}")
}

// Search with complex types
case class Order(
  id: String,
  userId: String,
  items: List[OrderItem],
  total: Double,
  status: String
)

case class OrderItem(productId: String, quantity: Int, price: Double)

val orderQuery = SQLQuery(
  query = "SELECT * FROM orders WHERE status = 'completed'"
)

client.searchAs[Order](orderQuery) match {
  case ElasticSuccess(orders) =>
    val totalRevenue = orders.map(_.total).sum
    println(s"✅ Total revenue: $totalRevenue")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// Error handling for conversion failures
client.searchAs[Product](sqlQuery) match {
  case ElasticFailure(error) if error.operation.contains("convertToEntities") =>
    println(s"❌ Type conversion failed: ${error.message}")
    error.cause.foreach(ex => println(s"Cause: ${ex.getMessage}"))
    
  case ElasticFailure(error) =>
    println(s"❌ Search failed: ${error.message}")
}

// Monadic composition with type conversion
val result: ElasticResult[List[EnrichedProduct]] = for {
  products <- client.searchAs[Product](productQuery)
  enriched = products.map(enrichProduct)
} yield enriched.toList
```

---

### searchAs

Searches and automatically converts results to typed entities using an SQL query [validated at compile-time](../sql/validation.md).

**Signature:**

```scala
def searchAs[U](
  query: String
)(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]]
```

**Parameters:**
- `query` - SQL query
- `m` - Implicit Manifest for type information
- `formats` - Implicit JSON serialization formats

**Returns:**
- `ElasticSuccess[Seq[U]]` with typed entities
- `ElasticFailure` with conversion or search errors

---

### singleSearchAs

Searches and converts results to typed entities using an Elasticsearch query.

**Signature:**

```scala
def singleSearchAs[U](
  elasticQuery: ElasticQuery,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]]
```

**Examples:**

```scala
case class Product(id: String, name: String, price: Double)

implicit val formats: Formats = DefaultFormats

val query = ElasticQuery(
  query = """
  {
    "query": {
      "range": {
        "price": {
          "gte": 100,
          "lte": 1000
        }
      }
    }
  }
  """,
  indices = Seq("products")
)

client.singleSearchAs[Product](query, Map.empty, Map.empty) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} products in price range")
    products.foreach(p => println(s"${p.name}: ${p.price}"))
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// With field aliases for mapping
val fieldAliases = Map(
  "product_name" -> "name",
  "product_price" -> "price",
  "product_id" -> "id"
)

client.singleSearchAs[Product](query, fieldAliases, Map.empty) match {
  case ElasticSuccess(products) =>
    println(s"✅ Converted with field aliases")
}
```

---

### multisearchAs

Multi-search with automatic type conversion.

**Signature:**

```scala
def multisearchAs[U](
  elasticQueries: ElasticQueries,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit m: Manifest[U], formats: Formats): ElasticResult[Seq[U]]
```

**Examples:**

```scala
case class Product(id: String, name: String, price: Double, category: String)

implicit val formats: Formats = DefaultFormats

val queries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """{"query": {"term": {"category": "electronics"}}}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": {"term": {"category": "books"}}}""",
      indices = Seq("products")
    )
  )
)

client.multisearchAs[Product](queries, Map.empty, Map.empty) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} products across categories")
    val byCategory = products.groupBy(_.category)
    byCategory.foreach { case (category, items) =>
      println(s"$category: ${items.size} items")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}
```

---

## Asynchronous Search with Type Conversion

### searchAsyncAsUnchecked

Asynchronously searches and converts results to typed entities.

**Signature:**

```scala
def searchAsyncAs[U](
  sqlQuery: SQLQuery
)(implicit
  m: Manifest[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Seq[U]]]
```

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global

case class Product(id: String, name: String, price: Double)

implicit val formats: Formats = DefaultFormats

val sqlQuery = SQLQuery(
  query = "SELECT * FROM products WHERE price > 100"
)

client.searchAsyncAs[Product](sqlQuery).onComplete {
  case Success(ElasticSuccess(products)) =>
    println(s"✅ Found ${products.size} products")
    products.foreach(p => println(s"${p.name}: ${p.price}"))
    
  case Success(ElasticFailure(error)) =>
    println(s"❌ Search failed: ${error.message}")
    
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Chained async operations with type conversion
val result: Future[ElasticResult[Summary]] = for {
  products <- client.searchAsyncAs[Product](productQuery)
  orders <- client.searchAsyncAs[Order](orderQuery)
  summary = createSummary(products, orders)
} yield summary

// Parallel async searches with conversion
val futures = List(
  client.searchAsyncAs[Product](query1),
  client.searchAsyncAs[Order](query2),
  client.searchAsyncAs[User](query3)
)

Future.sequence(futures).map { results =>
  results.foreach {
    case ElasticSuccess(items) => println(s"✅ Found ${items.size} items")
    case ElasticFailure(error) => println(s"❌ Failed: ${error.message}")
  }
}
```
---

### searchAsyncAs

Asynchronously searches and converts results to typed entities using an SQL query [validated at compile-time](../sql/validation.md).

**Signature:**

```scala
def searchAsyncAs[U](
  query: String
)(implicit
  m: Manifest[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Seq[U]]]
```

---

### singleSearchAsyncAs

Asynchronously searches and converts using an Elasticsearch query.

**Signature:**

```scala
def singleSearchAsyncAs[U](
  elasticQuery: ElasticQuery,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit
  m: Manifest[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Seq[U]]]
```

**Examples:**

```scala
val query = ElasticQuery(
  query = """{"query": {"match": {"category": "electronics"}}}""",
  indices = Seq("products")
)

client.singleSearchAsyncAs[Product](query, Map.empty, Map.empty).foreach {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} electronics")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### multiSearchAsyncAs

Asynchronously executes multi-search with type conversion.

**Signature:**

```scala
def multiSearchAsyncAs[U](
  elasticQueries: ElasticQueries,
  fieldAliases: Map[String, String],
  aggregations: Map[String, ClientAggregation]
)(implicit
  m: Manifest[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Seq[U]]]
```

**Examples:**

```scala
val queries = ElasticQueries(
  queries = List(query1, query2, query3)
)

client.multiSearchAsyncAs[Product](queries, Map.empty, Map.empty).foreach {
  case ElasticSuccess(products) =>
    println(s"✅ Multi-search returned ${products.size} products")
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}
```

---

## Implementation Requirements

### executeSingleSearch

```scala
private[client] def executeSingleSearch(
  elasticQuery: ElasticQuery
): ElasticResult[Option[String]]
```

**Implementation Example:**

```scala
private[client] def executeSingleSearch(
  elasticQuery: ElasticQuery
): ElasticResult[Option[String]] = {
  executeRestAction[SearchResponse, Option[String]](
    operation = "search",
    index = Some(elasticQuery.indices.mkString(","))
  )(
    action = {
      val request = new SearchRequest(elasticQuery.indices: _*)
      request.source(new SearchSourceBuilder().query(
        QueryBuilders.wrapperQuery(elasticQuery.query)
      ))
      client.search(request, RequestOptions.DEFAULT)
    }
  )(
    transformer = resp => Some(resp.toString)
  )
}
```

---

### executeMultiSearch

```scala
private[client] def executeMultiSearch(
  elasticQueries: ElasticQueries
): ElasticResult[Option[String]]
```

**Implementation Example:**

```scala
private[client] def executeMultiSearch(
  elasticQueries: ElasticQueries
): ElasticResult[Option[String]] = {
  executeRestAction[MultiSearchResponse, Option[String]](
    operation = "multiSearch",
    index = None
  )(
    action = {
      val request = new MultiSearchRequest()
      
      elasticQueries.queries.foreach { query =>
        val searchRequest = new SearchRequest(query.indices: _*)
        searchRequest.source(new SearchSourceBuilder().query(
          QueryBuilders.wrapperQuery(query.query)
        ))
        request.add(searchRequest)
      }
      
      client.msearch(request, RequestOptions.DEFAULT)
    }
  )(
    transformer = resp => Some(resp.toString)
  )
}
```

---

### executeSingleSearchAsync

```scala
private[client] def executeSingleSearchAsync(
  elasticQuery: ElasticQuery
)(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]]
```

**Implementation Example:**

```scala
private[client] def executeSingleSearchAsync(
  elasticQuery: ElasticQuery
)(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] = {
  val promise = Promise[ElasticResult[Option[String]]]()
  
  val request = new SearchRequest(elasticQuery.indices: _*)
  request.source(new SearchSourceBuilder().query(
    QueryBuilders.wrapperQuery(elasticQuery.query)
  ))
  
  client.searchAsync(
    request,
    RequestOptions.DEFAULT,
    new ActionListener[SearchResponse] {
      override def onResponse(response: SearchResponse): Unit = {
        promise.success(ElasticSuccess(Some(response.toString)))
      }
      
      override def onFailure(e: Exception): Unit = {
        promise.success(ElasticFailure(ElasticError(
          message = s"Async search failed: ${e.getMessage}",
          operation = Some("searchAsync"),
          index = Some(elasticQuery.indices.mkString(",")),
          cause = Some(e)
        )))
      }
    }
  )
  
  promise.future
}
```

---

### executeMultiSearchAsync

```scala
private[client] def executeMultiSearchAsync(
  elasticQueries: ElasticQueries
)(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]]
```

**Implementation Example:**

```scala
private[client] def executeMultiSearchAsync(
  elasticQueries: ElasticQueries
)(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] = {
  val promise = Promise[ElasticResult[Option[String]]]()
  
  val request = new MultiSearchRequest()
  elasticQueries.queries.foreach { query =>
    val searchRequest = new SearchRequest(query.indices: _*)
    searchRequest.source(new SearchSourceBuilder().query(
      QueryBuilders.wrapperQuery(query.query)
    ))
    request.add(searchRequest)
  }
  
  client.msearchAsync(
    request,
    RequestOptions.DEFAULT,
    new ActionListener[MultiSearchResponse] {
      override def onResponse(response: MultiSearchResponse): Unit = {
        promise.success(ElasticSuccess(Some(response.toString)))
      }
      
      override def onFailure(e: Exception): Unit = {
        promise.success(ElasticFailure(ElasticError(
          message = s"Async multi-search failed: ${e.getMessage}",
          operation = Some("multiSearchAsync"),
          cause = Some(e)
        )))
      }
    }
  )
  
  promise.future
}
```

---

### sqlSearchRequestToJsonQuery

```scala
private[client] implicit def sqlSearchRequestToJsonQuery(
  sqlSearch: SQLSearchRequest
): String
```

**Implementation Example:**

```scala
private[client] implicit def sqlSearchRequestToJsonQuery(
  sqlSearch: SQLSearchRequest
): String = {
  import org.json4s._
  import org.json4s.jackson.JsonMethods._
  
  implicit val formats: Formats = DefaultFormats
  
  // Convert SQL search request to Elasticsearch JSON query
  val queryJson = ("query" -> sqlSearch.query) ~
                  ("size" -> sqlSearch.size) ~
                  ("from" -> sqlSearch.from)
  
  compact(render(queryJson))
}
```

---

## Common Patterns

### Repository Pattern with Search

```scala
trait SearchRepository[T] extends ElasticClientDelegator {
  implicit val formats: Formats = DefaultFormats

  def findAll(implicit
    m: Manifest[T]
  ): ElasticResult[Seq[T]] = {
    val indexName = m.runtimeClass.getSimpleName.toLowerCase
    searchAs[T](SQLQuery(s"SELECT * FROM $indexName")).map(_.toSeq)
  }

  def findById(id: String)(implicit
                           m: Manifest[T]
  ): ElasticResult[Option[T]] = {
    val indexName = m.runtimeClass.getSimpleName.toLowerCase
    val query = ElasticQuery(
      query = s"""{"query": {"term": {"_id": "$id"}}}""",
      indices = Seq(indexName)
    )
    singleSearchAs[T](query, Map.empty, Map.empty).map(_.headOption)
  }

  def search(query: SQLQuery)(implicit
                              m: Manifest[T]
  ): ElasticResult[Seq[T]] = {
    val indexName = m.runtimeClass.getSimpleName.toLowerCase
    searchAs[T](query)
  }
}

// Usage
case class Product(id: String, name: String, price: Double, category: String)

object ProductRepository extends SearchRepository[Product] {
  lazy val delegate: ElasticClientApi = ElasticClientFactory.create()

  def findByCategory(category: String): ElasticResult[Seq[Product]] = {
    search(SQLQuery(s"SELECT * FROM product WHERE category = '$category'"))
  }

  def findByPriceRange(min: Double, max: Double): ElasticResult[Seq[Product]] = {
    search(SQLQuery(s"SELECT * FROM product WHERE price BETWEEN $min AND $max"))
  }
}
// Using the repository
ProductRepository.findByCategory("electronics") match {
  case ElasticSuccess(products) =>
    println(s"Found ${products.size} electronics")
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Pagination Pattern

```scala
case class Page[T](
  items: Seq[T],
  total: Long,
  page: Int,
  pageSize: Int
) {
  def totalPages: Int = Math.ceil(total.toDouble / pageSize).toInt
  def hasNext: Boolean = page < totalPages
  def hasPrevious: Boolean = page > 1
}

def searchWithPagination[T](
  query: String,
  page: Int = 1,
  pageSize: Int = 20
)(implicit 
  client: ElasticClient,
  m: Manifest[T],
  formats: Formats
): ElasticResult[Page[T]] = {
  
  val from = (page - 1) * pageSize
  val indexName = m.runtimeClass.getSimpleName.toLowerCase
  
  val elasticQuery = ElasticQuery(
    query = s"""
    {
      "query": $query,
      "from": $from,
      "size": $pageSize
    }
    """,
    indices = Seq(indexName)
  )
  
  for {
    response <- client.singleSearch(elasticQuery, Map.empty, Map.empty)
    entities <- client.convertToEntities[T](response)
    total = extractTotal(response.response)
  } yield Page(entities, total, page, pageSize)
}

// Usage
searchWithPagination[Product](
  query = """{"match": {"category": "electronics"}}""",
  page = 1,
  pageSize = 20
) match {
  case ElasticSuccess(page) =>
    println(s"Page ${page.page} of ${page.totalPages}")
    println(s"Items: ${page.items.size} / ${page.total}")
    page.items.foreach(println)
    
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Full-Text Search Pattern

```scala
def fullTextSearch[T](
  searchText: String,
  fields: Seq[String],
  fuzzy: Boolean = false,
  boost: Map[String, Double] = Map.empty
)(implicit 
  client: ElasticClient,
  m: Manifest[T],
  formats: Formats
): ElasticResult[Seq[T]] = {
  
  val indexName = m.runtimeClass.getSimpleName.toLowerCase
  
  val fieldQueries = fields.map { field =>
    val boostValue = boost.getOrElse(field, 1.0)
    val matchType = if (fuzzy) "match" else "match_phrase"
    s"""
    {
      "$matchType": {
        "$field": {
          "query": "$searchText",
          "boost": $boostValue
        }
      }
    }
    """
  }.mkString(",")
  
  val query = ElasticQuery(
    query = s"""
    {
      "query": {
        "bool": {
          "should": [$fieldQueries],
          "minimum_should_match": 1
        }
      }
    }
    """,
    indices = Seq(indexName)
  )
  
  client.singleSearchAs[T](query, Map.empty, Map.empty)
}

// Usage
fullTextSearch[Product](
  searchText = "wireless bluetooth headphones",
  fields = Seq("name", "description", "tags"),
  fuzzy = true,
  boost = Map(
    "name" -> 3.0,
    "description" -> 1.0,
    "tags" -> 2.0
  )
) match {
  case ElasticSuccess(products) =>
    println(s"Found ${products.size} matching products")
    products.foreach(p => println(s"${p.name} - ${p.price}"))
    
  case ElasticFailure(error) =>
    println(s"Search failed: ${error.message}")
}
```

---

### Filter and Sort Pattern

```scala
case class SearchCriteria(
  filters: Map[String, Any] = Map.empty,
  rangeFilters: Map[String, (Option[Double], Option[Double])] = Map.empty,
  sortBy: Option[String] = None,
  sortOrder: String = "asc",
  size: Int = 100
)

def advancedSearch[T](
  criteria: SearchCriteria
)(implicit 
  client: ElasticClient,
  m: Manifest[T],
  formats: Formats
): ElasticResult[Seq[T]] = {
  
  val indexName = m.runtimeClass.getSimpleName.toLowerCase
  
  // Build filter clauses
  val termFilters = criteria.filters.map { case (field, value) =>
    s"""{"term": {"$field": "$value"}}"""
  }.mkString(",")
  
  val rangeFilters = criteria.rangeFilters.map { case (field, (min, max)) =>
    val minClause = min.map(v => s""""gte": $v""").getOrElse("")
    val maxClause = max.map(v => s""""lte": $v""").getOrElse("")
    val clauses = Seq(minClause, maxClause).filter(_.nonEmpty).mkString(",")
    s"""{"range": {"$field": {$clauses}}}"""
  }.mkString(",")
  
  val allFilters = Seq(termFilters, rangeFilters)
    .filter(_.nonEmpty)
    .mkString(",")
  
  // Build sort clause
  val sortClause = criteria.sortBy.map { field =>
    s""""sort": [{"$field": "${criteria.sortOrder}"}]"""
  }.getOrElse("")
  
  val query = ElasticQuery(
    query = s"""
    {
      "query": {
        "bool": {
          "filter": [$allFilters]
        }
      },
      $sortClause,
      "size": ${criteria.size}
    }
    """,
    indices = Seq(indexName)
  )
  
  client.singleSearchAs[T](query, Map.empty, Map.empty)
}

// Usage
val criteria = SearchCriteria(
  filters = Map(
    "category" -> "electronics",
    "brand" -> "Sony"
  ),
  rangeFilters = Map(
    "price" -> (Some(100.0), Some(500.0)),
    "rating" -> (Some(4.0), None)
  ),
  sortBy = Some("price"),
  sortOrder = "asc",
  size = 50
)

advancedSearch[Product](criteria) match {
  case ElasticSuccess(products) =>
    println(s"Found ${products.size} products matching criteria")
    products.foreach(p => println(s"${p.name}: ${p.price}"))
    
  case ElasticFailure(error) =>
    println(s"Search failed: ${error.message}")
}
```

---

## Performance Optimization

### Query Caching

```scala
import scala.collection.concurrent.TrieMap

class CachedSearchApi(client: ElasticClient) {
  private val cache = TrieMap[String, (ElasticResponse, Long)]()
  private val cacheTTL = 5 * 60 * 1000 // 5 minutes
  
  def searchWithCache(
    query: ElasticQuery,
    fieldAliases: Map[String, String] = Map.empty,
    aggregations: Map[String, ClientAggregation] = Map.empty
  ): ElasticResult[ElasticResponse] = {
    
    val cacheKey = s"${query.indices.mkString(",")}:${query.query}"
    val now = System.currentTimeMillis()
    
    cache.get(cacheKey) match {
      case Some((response, timestamp)) if (now - timestamp) < cacheTTL =>
        logger.debug(s"✅ Cache hit for query: $cacheKey")
        ElasticResult.success(response)
        
      case _ =>
        logger.debug(s"❌ Cache miss for query: $cacheKey")
        client.singleSearch(query, fieldAliases, aggregations) match {
          case success @ ElasticSuccess(response) =>
            cache.put(cacheKey, (response, now))
            success
          case failure => failure
        }
    }
  }
  
  def clearCache(): Unit = cache.clear()
  
  def removeCacheEntry(query: ElasticQuery): Unit = {
    val cacheKey = s"${query.indices.mkString(",")}:${query.query}"
    cache.remove(cacheKey)
  }
}

// Usage
val cachedSearch = new CachedSearchApi(client)

// First call - hits Elasticsearch
cachedSearch.searchWithCache(query)

// Second call - returns cached result
cachedSearch.searchWithCache(query)
```

---

## Error Handling

### Query Validation Errors

```scala
// Invalid JSON query
val invalidQuery = ElasticQuery(
  query = """{"query": INVALID_JSON}""",
  indices = Seq("products")
)

client.singleSearch(invalidQuery, Map.empty, Map.empty) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid query"))
    assert(error.operation.contains("search"))
    assert(error.index.contains("products"))
}
```

---

### Type Conversion Errors

```scala
case class Product(id: String, name: String, price: Double)

val query = ElasticQuery(
  query = """{"query": {"match_all": {}}}""",
  indices = Seq("products")
)

client.singleSearchAs[Product](query, Map.empty, Map.empty) match {
  case ElasticFailure(error) if error.operation.contains("convertToEntities") =>
    println(s"❌ Type conversion failed: ${error.message}")
    println(s"Target type: Product")
    error.cause.foreach { ex =>
      println(s"Cause: ${ex.getMessage}")
      println(s"Stack trace: ${ex.getStackTrace.mkString("\n")}")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Search failed: ${error.message}")
}
```

---

### Multi-Search Partial Failures

```scala
val queries = ElasticQueries(
  queries = List(
    ElasticQuery(
      query = """{"query": {"match": {"name": "valid"}}}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": INVALID}""",
      indices = Seq("products")
    ),
    ElasticQuery(
      query = """{"query": {"match": {"name": "also-valid"}}}""",
      indices = Seq("products")
    )
  )
)

client.multiSearch(queries, Map.empty, Map.empty) match {
  case ElasticFailure(error) =>
    // Error message contains information about all invalid queries
    assert(error.message.contains("Invalid queries"))
    assert(error.statusCode.contains(400))
    println(s"Failed queries: ${error.message}")
}
```

---

## Testing Scenarios

### Test Basic Search

```scala
def testBasicSearch()(implicit client: ElasticClient): Unit = {
  val testIndex = "test-search"
  
  // Setup: Index test documents
  client.createIndex(testIndex)
  client.index(testIndex, "1", """{"name": "Product 1", "price": 100}""")
  client.index(testIndex, "2", """{"name": "Product 2", "price": 200}""")
  client.refresh(testIndex)
  
  // Test search
  val query = ElasticQuery(
    query = """{"query": {"match_all": {}}}""",
    indices = Seq(testIndex)
  )
  
  client.singleSearch(query, Map.empty, Map.empty) match {
    case ElasticSuccess(response) =>
      assert(response.query.contains("match_all"))
      println("✅ Basic search test passed")
      
    case ElasticFailure(error) =>
      throw new AssertionError(s"Search failed: ${error.message}")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Search with Type Conversion

```scala
def testSearchWithConversion()(implicit client: ElasticClient): Unit = {
  case class TestProduct(name: String, price: Double)
  
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-conversion"
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, "1", """{"name": "Laptop", "price": 999.99}""")
  client.index(testIndex, "2", """{"name": "Mouse", "price": 29.99}""")
  client.refresh(testIndex)
  
  // Test
  val query = ElasticQuery(
    query = """{"query": {"match_all": {}}}""",
    indices = Seq(testIndex)
  )
  
  client.singleSearchAs[TestProduct](query, Map.empty, Map.empty) match {
    case ElasticSuccess(products) =>
      assert(products.size == 2, s"Expected 2 products, got ${products.size}")
      assert(products.exists(_.name == "Laptop"))
      assert(products.exists(_.name == "Mouse"))
      println("✅ Search with conversion test passed")
      
    case ElasticFailure(error) =>
      throw new AssertionError(s"Search failed: ${error.message}")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Multi-Search

```scala
def testMultiSearch()(implicit client: ElasticClient): Unit = {
  val testIndex = "test-multi-search"
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, "1", """{"category": "electronics", "name": "Laptop"}""")
  client.index(testIndex, "2", """{"category": "books", "name": "Novel"}""")
  client.refresh(testIndex)
  
  // Test
  val queries = ElasticQueries(
    queries = List(
      ElasticQuery(
        query = """{"query": {"term": {"category": "electronics"}}}""",
        indices = Seq(testIndex)
      ),
      ElasticQuery(
        query = """{"query": {"term": {"category": "books"}}}""",
        indices = Seq(testIndex)
      )
    )
  )
  
  client.multiSearch(queries, Map.empty, Map.empty) match {
    case ElasticSuccess(response) =>
      println("✅ Multi-search test passed")
      
    case ElasticFailure(error) =>
      throw new AssertionError(s"Multi-search failed: ${error.message}")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Async Search

```scala
def testAsyncSearch()(implicit 
  client: ElasticClient,
  ec: ExecutionContext
): Future[Unit] = {
  val testIndex = "test-async-search"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- client.indexAsync(testIndex, "1", """{"name": "Test Product"}""")
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = ElasticQuery(
      query = """{"query": {"match_all": {}}}""",
      indices = Seq(testIndex)
    )
    result <- client.singleSearchAsync(query, Map.empty, Map.empty)
    
    _ = result match {
      case ElasticSuccess(response) =>
        println("✅ Async search test passed")
      case ElasticFailure(error) =>
        throw new AssertionError(s"Async search failed: ${error.message}")
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield ()
}
```

---

## Best Practices

**1. Use Type-Safe Search Methods**

```scala
// ✅ Good - type-safe with automatic conversion
case class Product(id: String, name: String, price: Double)

client.singleSearchAs[Product](query, Map.empty, Map.empty) match {
  case ElasticSuccess(products) => products.foreach(println)
  case ElasticFailure(error) => println(error.message)
}

// ❌ Avoid - manual JSON parsing
client.singleSearch(query, Map.empty, Map.empty) match {
  case ElasticSuccess(response) =>
    // Manual JSON parsing required
    val json = parse(response.response)
    // Error-prone extraction
}
```

---

**2. Validate Queries Before Execution**

```scala
// ✅ Good - validation happens automatically
client.singleSearch(query, Map.empty, Map.empty)

// ✅ Good - additional custom validation
def validateAndSearch(query: ElasticQuery): ElasticResult[ElasticResponse] = {
  if (query.indices.isEmpty) {
    return ElasticResult.failure(ElasticError(
      message = "No indices specified",
      statusCode = Some(400)
    ))
  }
  
  client.singleSearch(query, Map.empty, Map.empty)
}
```

---

**3. Use Async for Multiple Searches**

```scala
// ✅ Good - parallel async searches
val futures = List(query1, query2, query3).map { query =>
  client.singleSearchAsync(query, Map.empty, Map.empty)
}

Future.sequence(futures).map { results =>
  results.foreach {
    case ElasticSuccess(response) => println("Success")
    case ElasticFailure(error) => println(s"Failed: ${error.message}")
  }
}

// ❌ Avoid - sequential blocking searches
val result1 = client.singleSearch(query1, Map.empty, Map.empty)
val result2 = client.singleSearch(query2, Map.empty, Map.empty)
val result3 = client.singleSearch(query3, Map.empty, Map.empty)
```

---

**4. Handle Field Aliases Properly**

```scala
// ✅ Good - use field aliases for mapping
val fieldAliases = Map(
  "product_name" -> "name",
  "product_price" -> "price",
  "product_id" -> "id"
)

client.singleSearchAs[Product](query, fieldAliases, Map.empty)

// ❌ Avoid - expecting exact field names
client.singleSearchAs[Product](query, Map.empty, Map.empty)
// May fail if ES field names don't match case class fields
```

---

**5. Use Multi-Search for Related Queries**

```scala
// ✅ Good - single multi-search request
val queries = ElasticQueries(
  queries = List(query1, query2, query3)
)
client.multiSearch(queries, Map.empty, Map.empty)

// ❌ Avoid - multiple single searches
client.singleSearch(query1, Map.empty, Map.empty)
client.singleSearch(query2, Map.empty, Map.empty)
client.singleSearch(query3, Map.empty, Map.empty)
```

---

## SQL Query Search

### Overview

The **SQL Query Search** is a key feature of this API, allowing you to query Elasticsearch using familiar SQL syntax instead of complex JSON DSL. The API automatically converts SQL queries to Elasticsearch queries.

**Benefits:**
- **Familiar syntax** for developers with SQL background
- **Simpler queries** compared to Elasticsearch JSON DSL
- **Automatic conversion** to optimized Elasticsearch queries
- **Full support** for WHERE, ORDER BY, LIMIT, GROUP BY, HAVING, UNNEST, and aggregations
- **Type-safe results** with automatic conversion to Scala case classes

### SQL Query Examples

**Basic SELECT**

```scala
case class Product(id: String, name: String, price: Double, category: String)

implicit val formats: Formats = DefaultFormats

// Simple SELECT with WHERE clause
val query1 = SQLQuery(
  query = "SELECT * FROM products WHERE category = 'electronics'"
)

client.searchAs[Product](query1) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} electronics")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// Multiple conditions
val query2 = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics' 
    AND price > 100 
    AND price < 1000
  """
)

client.searchAs[Product](query2)

// OR conditions
val query3 = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics' OR category = 'computers'
  """
)

client.searchAs[Product](query3)

// IN clause
val query4 = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category IN ('electronics', 'computers', 'phones')
  """
)

client.searchAs[Product](query4)

// LIKE for pattern matching
val query5 = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE name LIKE '%laptop%'
  """
)

client.searchAs[Product](query5)
```

---

**ORDER BY and LIMIT**

```scala
// Sort by price ascending
val sortAsc = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics'
    ORDER BY price ASC
  """
)

client.searchAs[Product](sortAsc) match {
  case ElasticSuccess(products) =>
    println("Products sorted by price (low to high):")
    products.foreach(p => println(s"${p.name}: ${p.price}"))
}

// Sort by price descending with limit
val sortDesc = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics'
    ORDER BY price DESC
    LIMIT 10
  """
)

client.searchAs[Product](sortDesc) match {
  case ElasticSuccess(products) =>
    println("Top 10 most expensive electronics:")
    products.foreach(p => println(s"${p.name}: ${p.price}"))
}

// Multiple sort fields
val multiSort = SQLQuery(
  query = """
    SELECT * FROM products 
    ORDER BY category ASC, price DESC
    LIMIT 20
  """
)

client.searchAs[Product](multiSort)

// Pagination with LIMIT and OFFSET
val paginated = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics'
    ORDER BY price ASC
    LIMIT 20 OFFSET 40
  """
)

client.searchAs[Product](paginated) // Returns page 3 (items 41-60)
```

---

**GROUP BY and Aggregations**

For a full list of supported [aggregation functions](../sql/functions_aggregate.md), refer to the SQL documentation.

```scala
// Count by category
val countByCategory = SQLQuery(
  query = """
    SELECT category, COUNT(*) as total
    FROM products
    GROUP BY category
  """
)

client.search(countByCategory)

// Average price by category
val avgPriceByCategory = SQLQuery(
  query = """
    SELECT category, AVG(price) as avg_price
    FROM products
    GROUP BY category
  """
)

client.search(avgPriceByCategory)

// Multiple aggregations
val multiAgg = SQLQuery(
  query = """
    SELECT 
      category,
      COUNT(*) as total_products,
      AVG(price) as avg_price,
      MIN(price) as min_price,
      MAX(price) as max_price,
      SUM(price) as total_value
    FROM products
    GROUP BY category
  """
)

client.search(multiAgg)

// Aggregation with filter
val filteredAgg = SQLQuery(
  query = """
    SELECT category, AVG(price) as avg_price
    FROM products
    WHERE price > 100
    GROUP BY category
  """
)

client.search(filteredAgg)
```

---

**JOIN-like Queries (Multi-Search)**

```scala
// Search across multiple indices
val multiIndexQuery = SQLQuery(
  query = """
    SELECT * FROM products WHERE user_id = 'user-123'
    UNION ALL
    SELECT * FROM orders WHERE user_id = 'user-123'
    UNION ALL
    SELECT * FROM reviews WHERE user_id = 'user-123'
  """
)

client.search(multiIndexQuery) match {
  case ElasticSuccess(response) =>
    println("✅ Retrieved user data from multiple indices")
}

// Related data queries
val relatedData = SQLQuery(
  query = """
    SELECT * FROM products WHERE category = 'electronics'
    UNION ALL
    SELECT * FROM products WHERE category = 'accessories'
  """
)

client.searchAs[Product](relatedData) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} related products")
}
```

---

**Date Range Queries**

For date functions, refer to the [SQL Date / Time / Datetime / Timestamp / Interval Functions](../sql/functions_date_time.md) documentation.

```scala
case class Order(
  id: String,
  userId: String,
  total: Double,
  status: String,
  createdAt: String
)

// Orders from last 30 days
val recentOrders = SQLQuery(
  query = """
    SELECT * FROM orders 
    WHERE createdAt >= CURRENT_DATE - INTERVAL 30 DAY
    AND status = 'completed'
    ORDER BY createdAt DESC
  """
)

client.searchAs[Order](recentOrders)

// Orders in date range
val dateRange = SQLQuery(
  query = """
    SELECT * FROM orders 
    WHERE createdAt BETWEEN '2024-01-01' AND '2024-01-31'
  """
)

client.searchAs[Order](dateRange)

// Orders from specific year
val yearQuery = SQLQuery(
  query = """
    SELECT * FROM orders 
    WHERE EXTRACT(YEAR FROM createdAt) = 2024
  """
)

client.searchAs[Order](yearQuery)
```

---

**Async SQL Queries**

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// Async SQL search
val asyncQuery = SQLQuery(
  query = "SELECT * FROM products WHERE price > 500"
)

client.searchAsyncAs[Product](asyncQuery).foreach {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} expensive products")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// Parallel SQL queries
val queries = List(
  SQLQuery("SELECT * FROM products WHERE category = 'electronics'"),
  SQLQuery("SELECT * FROM products WHERE category = 'books'"),
  SQLQuery("SELECT * FROM products WHERE category = 'clothing'")
)

val futures = queries.map(query => client.searchAsyncAs[Product](query))

Future.sequence(futures).map { results =>
  results.foreach {
    case ElasticSuccess(products) =>
      println(s"✅ Category: ${products.size} products")
    case ElasticFailure(error) =>
      println(s"❌ Failed: ${error.message}")
  }
}

// Chained async SQL queries
val result = for {
  products <- client.searchAsyncAs[Product](productQuery)
  topProducts = products.getOrElse(Seq.empty).sortBy(-_.price).take(10)
  orders <- client.searchAsyncAs[Order](orderQuery)
} yield (topProducts, orders)
```

---

## SQL Query Patterns

### Full-Text Search with SQL

For more details, refer to the [SQL Full-Text Search Function](../sql/functions_string.md#full-text-search-function) documentation.

```scala
// Text search
val textSearch = SQLQuery(
  query = """
    SELECT * FROM books
    WHERE MATCH(title, abstract, preface, keywords) AGAINST ('machine learning')
  """
)

client.searchAs[Book](textSearch)

```

---

### Complex Filtering with SQL

```scala
// Multiple conditions with precedence
val complexFilter = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE (category = 'electronics' OR category = 'computers')
    AND price BETWEEN 100 AND 1000
    AND stock > 0
    ORDER BY price ASC
  """
)

client.searchAs[Product](complexFilter)

// NOT conditions
val notQuery = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics'
    AND NOT (brand = 'BrandX' OR brand = 'BrandY')
  """
)

client.searchAs[Product](notQuery)

// NULL checks
val nullCheck = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE discount IS NOT NULL
    AND discount > 0
  """
)

client.searchAs[Product](nullCheck)
```

---

## Best Practices for SQL Queries

**1. Use SQL for Readable Queries**

```scala
// ✅ Good - SQL is clear and readable
val sqlQuery = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics' 
    AND price BETWEEN 100 AND 1000
    ORDER BY price DESC
    LIMIT 20
  """
)

client.searchAs[Product](sqlQuery)

// ❌ Avoid - Complex JSON DSL
val jsonQuery = ElasticQuery(
  query = """
  {
    "query": {
      "bool": {
        "must": [
          {"term": {"category": "electronics"}},
          {"range": {"price": {"gte": 100, "lte": 1000}}}
        ]
      }
    },
    "sort": [{"price": "desc"}],
    "size": 20
  }
  """,
  indices = Seq("products")
)
```

---

**2. Use Type-Safe Conversion**

```scala
// ✅ Good - Type-safe with case class
case class Product(id: String, name: String, price: Double)

client.searchAs[Product](sqlQuery) match {
  case ElasticSuccess(products) => products.foreach(println)
  case ElasticFailure(error) => println(error.message)
}

// ❌ Avoid - Raw response parsing
client.search(sqlQuery) match {
  case ElasticSuccess(response) =>
    // Manual JSON parsing required
}
```

---

**3. Use Parameterized Queries**

```scala
// ✅ Good - Parameterized query builder
def findProductsByCategory(category: String, minPrice: Double): SQLQuery = {
  SQLQuery(
    query = s"""
      SELECT * FROM products 
      WHERE category = '$category' 
      AND price >= $minPrice
    """
  )
}

client.searchAs[Product](findProductsByCategory("electronics", 100))

// ✅ Good - Query builder class
case class ProductQueryBuilder(
  category: Option[String] = None,
  minPrice: Option[Double] = None,
  maxPrice: Option[Double] = None,
  orderBy: Option[String] = None,
  limit: Int = 100
) {
  def build: SQLQuery = {
    val conditions = Seq(
      category.map(c => s"category = '$c'"),
      minPrice.map(p => s"price >= $p"),
      maxPrice.map(p => s"price <= $p")
    ).flatten
    
    val whereClause = if (conditions.nonEmpty) {
      s"WHERE ${conditions.mkString(" AND ")}"
    } else ""
    
    val orderClause = orderBy.map(field => s"ORDER BY $field").getOrElse("")
    
    SQLQuery(
      query = s"SELECT * FROM products $whereClause $orderClause LIMIT $limit"
    )
  }
}

// Usage
val query = ProductQueryBuilder(
  category = Some("electronics"),
  minPrice = Some(100),
  maxPrice = Some(1000),
  orderBy = Some("price DESC"),
  limit = 20
).build

client.searchAs[Product](query)
```

---

**4. Use Async for Multiple Queries**

```scala
// ✅ Good - Parallel async SQL queries
import scala.concurrent.ExecutionContext.Implicits.global

val queries = List(
  SQLQuery("SELECT * FROM products WHERE category = 'electronics'"),
  SQLQuery("SELECT * FROM products WHERE category = 'books'"),
  SQLQuery("SELECT * FROM products WHERE category = 'clothing'")
)

val futures = queries.map(query => client.searchAsyncAs[Product](query))

Future.sequence(futures).map { results =>
  results.foreach {
    case ElasticSuccess(products) =>
      println(s"✅ Found ${products.size} products")
    case ElasticFailure(error) =>
      println(s"❌ Failed: ${error.message}")
  }
}

// ❌ Avoid - Sequential blocking queries
val result1 = client.searchAs[Product](query1)
val result2 = client.searchAs[Product](query2)
val result3 = client.searchAs[Product](query3)
```

---

**5. Use UNION ALL for Multi-Index Queries**

```scala
// ✅ Good - Use UNION ALL for combining results from multiple queries
val unionQuery = SQLQuery(
  query = """
    SELECT * FROM products WHERE category = 'electronics'
    UNION ALL
    SELECT * FROM products WHERE category = 'computers'
    UNION ALL
    SELECT * FROM products WHERE category = 'phones'
  """
)

client.searchAs[Product](unionQuery) match {
  case ElasticSuccess(products) =>
    println(s"✅ Found ${products.size} products across categories")
    val byCategory = products.groupBy(_.category)
    byCategory.foreach { case (category, items) =>
      println(s"$category: ${items.size} items")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Multi-search failed: ${error.message}")
}

// ❌ Avoid - Multiple separate queries
val electronics = client.searchAs[Product](SQLQuery("SELECT * FROM products WHERE category = 'electronics'", ...))
val computers = client.searchAs[Product](SQLQuery("SELECT * FROM products WHERE category = 'computers'", ...))
val phones = client.searchAs[Product](SQLQuery("SELECT * FROM products WHERE category = 'phones'", ...))
```

---

**6. Use Scroll API for Large Result Sets**

```scala
// ✅ Good - Use Scroll API for large datasets
// See Scroll API documentation for complete implementation

val scrollQuery = SQLQuery(
  query = "SELECT * FROM large_index WHERE category = 'electronics'"
)

// Use dedicated Scroll API methods:
// - ScrollApi.scroll() for iteration
// - ScrollApi.scrollAs[T]() for type-safe iteration
// See Scroll API documentation

// ❌ Avoid - Regular search for large datasets
// Don't use searchAs() for more than 10,000 documents
client.searchAs[Product](sqlQuery) // Limited to max 10,000 results
```

---

## Summary

### Key Takeaways

1. **Use SQL queries** as the primary search method (key feature)
2. **Use UNION ALL** for combining multiple queries efficiently
3. **Use type-safe conversion** with `searchAs[T]()` methods
4. **Use async methods** for better performance
5. **Use Scroll API** for large result sets (>10,000 documents)
6. **Parameterize queries** for reusability and safety
7. **Handle errors** with pattern matching on `ElasticResult`
8. **Test thoroughly** with different query patterns

### Quick Reference - SQL Search

```scala
import org.json4s.DefaultFormats

implicit val formats: Formats = DefaultFormats

// ============================================================
// BASIC SQL SEARCH
// ============================================================
case class Product(id: String, name: String, price: Double, category: String)

val query = SQLQuery(
  query = "SELECT * FROM products WHERE category = 'electronics' AND price > 100"
)

client.searchAs[Product](query) match {
  case ElasticSuccess(products) => products.foreach(println)
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}

// ============================================================
// SQL WITH ORDER BY AND LIMIT
// ============================================================
val sortedQuery = SQLQuery(
  query = """
    SELECT * FROM products 
    WHERE category = 'electronics'
    ORDER BY price DESC
    LIMIT 20
  """
)

client.searchAs[Product](sortedQuery)

// ============================================================
// SQL WITH AGGREGATIONS
// ============================================================
val aggQuery = SQLQuery(
  query = """
    SELECT category, AVG(price) as avg_price, COUNT(*) as total
    FROM products
    GROUP BY category
  """
)

client.search(aggQuery)

// ============================================================
// UNION ALL FOR MULTI-SEARCH
// ============================================================
val unionQuery = SQLQuery(
  query = """
    SELECT * FROM products WHERE category = 'electronics'
    UNION ALL
    SELECT * FROM products WHERE category = 'books'
    UNION ALL
    SELECT * FROM products WHERE category = 'clothing'
  """
)

client.searchAs[Product](unionQuery)

// ============================================================
// ASYNC SQL SEARCH
// ============================================================
import scala.concurrent.ExecutionContext.Implicits.global

client.searchAsyncAs[Product](query).foreach {
  case ElasticSuccess(products) => println(s"Found ${products.size}")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}

// ============================================================
// LARGE DATASETS - USE SCROLL API
// ============================================================
// For >10,000 documents, use dedicated Scroll API
// See Scroll API documentation for complete examples
```

---

[Back to Index](README.md) | [Next: Scroll API](scroll.md)