[Back to index](README.md)

# GET API

## Overview

The **Get API** provides methods to retrieve documents from Elasticsearch by their document ID. It supports both synchronous and asynchronous operations, with automatic type conversion and comprehensive error handling.

**Key Features:**
- **Document retrieval by ID** (synchronous and asynchronous)
- **Type-safe deserialization** with automatic conversion
- **Document existence checking** without retrieving full content
- **Automatic index name inference** from entity types
- **Comprehensive error handling** with detailed error messages
- **Input validation** for index names and document IDs
- **Logging integration** for debugging and monitoring

**Dependencies:**
- Requires `SerializationApi` for JSON conversion
- Requires `ElasticClientHelpers` for validation utilities

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Basic Usage](#basic-usage)
3. [Typed Retrieval](#typed-retrieval)
4. [Asynchronous Operations](#asynchronous-operations)
5. [Document Existence Check](#document-existence-check)
6. [Error Handling](#error-handling)
7. [Advanced Patterns](#advanced-patterns)
8. [Testing](#testing)
9. [Best Practices](#best-practices)

---

## Core Concepts

### API Methods Overview

The Get API provides four main methods:

```scala
trait GetApi {
  // Check if document exists
  def exists(id: String, index: String): ElasticResult[Boolean]

  // Get document as JSON string
  def get(id: String, index: String): ElasticResult[Option[String]]

  // Get document as typed entity
  def getAs[U <: AnyRef](
                          id: String,
                          index: Option[String] = None,
                          maybeType: Option[String] = None
                        )(implicit m: Manifest[U], formats: Formats): ElasticResult[Option[U]]

  // Async: Get document as JSON string
  def getAsync(
                id: String,
                index: String
              )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]]

  // Async: Get document as typed entity
  def getAsyncAs[U <: AnyRef](
                               id: String,
                               index: Option[String] = None,
                               maybeType: Option[String] = None
                             )(implicit
                               m: Manifest[U],
                               ec: ExecutionContext,
                               formats: Formats
                             ): Future[ElasticResult[Option[U]]]
}
```

---

### Method Comparison

| Method       | Return Type                             | Async  | Type-Safe  | Use Case                 |
|--------------|-----------------------------------------|--------|------------|--------------------------|
| `exists`     | `ElasticResult[Boolean]`                | ❌      | N/A        | Check document existence |
| `get`        | `ElasticResult[Option[String]]`         | ❌      | ❌          | Get raw JSON             |
| `getAs`      | `ElasticResult[Option[U]]`              | ❌      | ✅          | Get typed entity         |
| `getAsync`   | `Future[ElasticResult[Option[String]]]` | ✅      | ❌          | Get raw JSON (async)     |
| `getAsyncAs` | `Future[ElasticResult[Option[U]]]`      | ✅      | ✅          | Get typed entity (async) |

---

### Return Types

All methods return `ElasticResult` or `Future[ElasticResult]`:

```scala
// Synchronous result
sealed trait ElasticResult[+T] {
  def map[U](f: T => U): ElasticResult[U]
  def flatMap[U](f: T => ElasticResult[U]): ElasticResult[U]
}

case class ElasticSuccess[T](value: T) extends ElasticResult[T]
case class ElasticFailure(error: ElasticError) extends ElasticResult[Nothing]

// Error details
case class ElasticError(
  message: String,
  statusCode: Option[Int] = None,
  index: Option[String] = None,
  operation: Option[String] = None,
  cause: Option[Throwable] = None
)
```

---

## Basic Usage

### Simple Document Retrieval

```scala
// Get document by ID
val result: ElasticResult[Option[String]] = client.get(
  id = "product-123",
  index = "products"
)

result match {
  case ElasticSuccess(Some(json)) =>
    println(s"✅ Document found: $json")
    
  case ElasticSuccess(None) =>
    println("⚠️ Document not found")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### Processing Retrieved Document

```scala
import org.json4s._
import org.json4s.jackson.JsonMethods._

val result = client.get("product-123", "products")

result match {
  case ElasticSuccess(Some(jsonString)) =>
    val json = parse(jsonString)
    val name = (json \ "name").extract[String]
    val price = (json \ "price").extract[Double]
    
    println(s"Product: $name")
    println(f"Price: $$${price}%.2f")
    
  case ElasticSuccess(None) =>
    println("Product not found")
    
  case ElasticFailure(error) =>
    println(s"Failed to retrieve product: ${error.message}")
}
```

---

### Using flatMap for Chaining

```scala
val result = client.get("product-123", "products").flatMap { jsonOpt =>
  jsonOpt match {
    case Some(json) =>
      // Process the JSON
      val processed = processJson(json)
      ElasticSuccess(processed)
      
    case None =>
      ElasticFailure(ElasticError("Document not found"))
  }
}

def processJson(json: String): String = {
  // Processing logic
  json.toUpperCase
}
```

---

## Typed Retrieval

### Basic Typed Retrieval

```scala
import org.json4s.DefaultFormats

case class Product(
  id: String,
  name: String,
  price: Double,
  category: String,
  stock: Int
)

implicit val formats: Formats = DefaultFormats

// Get document as typed entity
val result: ElasticResult[Option[Product]] = client.getAs[Product](
  id = "product-123",
  index = Some("products")
)

result match {
  case ElasticSuccess(Some(product)) =>
    println(s"✅ Product: ${product.name}")
    println(f"   Price: $$${product.price}%.2f")
    println(s"   Stock: ${product.stock} units")
    
  case ElasticSuccess(None) =>
    println("⚠️ Product not found")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### Automatic Index Inference

```scala
case class Product(id: String, name: String, price: Double)

// Index name automatically inferred from class name (lowercase)
// Will use "product" as index name
val result = client.getAs[Product](id = "123")

result match {
  case ElasticSuccess(Some(product)) =>
    println(s"Found product: ${product.name}")
    
  case ElasticSuccess(None) =>
    println("Product not found")
    
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Custom Index and Type Names

```scala
case class Product(id: String, name: String, price: Double)

// Specify custom index name
val result1 = client.getAs[Product](
  id = "123",
  index = Some("my-products-index")
)

// Specify custom type name (affects index inference)
val result2 = client.getAs[Product](
  id = "123",
  maybeType = Some("catalog-item")
)

// Both custom index and type
val result3 = client.getAs[Product](
  id = "123",
  index = Some("my-products-index"),
  maybeType = Some("catalog-item")
)
```

---

### Nested Objects

```scala
case class Address(
  street: String,
  city: String,
  country: String,
  zipCode: String
)

case class Customer(
  id: String,
  name: String,
  email: String,
  address: Address,
  orders: Seq[String]
)

implicit val formats: Formats = DefaultFormats

val result = client.getAs[Customer](
  id = "customer-456",
  index = Some("customers")
)

result match {
  case ElasticSuccess(Some(customer)) =>
    println(s"Customer: ${customer.name}")
    println(s"Email: ${customer.email}")
    println(s"City: ${customer.address.city}")
    println(s"Orders: ${customer.orders.size}")
    
  case ElasticSuccess(None) =>
    println("Customer not found")
    
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Custom Formats

```scala
import org.json4s._
import org.json4s.ext.JavaTimeSerializers
import java.time.LocalDateTime

case class Order(
  id: String,
  customerId: String,
  total: Double,
  createdAt: LocalDateTime,
  status: String
)

// Custom formats with date serializers
implicit val formats: Formats = DefaultFormats ++ JavaTimeSerializers.all

val result = client.getAs[Order](
  id = "order-789",
  index = Some("orders")
)

result match {
  case ElasticSuccess(Some(order)) =>
    println(s"Order ID: ${order.id}")
    println(s"Created: ${order.createdAt}")
    println(f"Total: $$${order.total}%.2f")
    
  case ElasticSuccess(None) =>
    println("Order not found")
    
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

## Asynchronous Operations

### Basic Async Retrieval

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// Async get as JSON
val futureResult: Future[ElasticResult[Option[String]]] = 
  client.getAsync("product-123", "products")

futureResult.foreach {
  case ElasticSuccess(Some(json)) =>
    println(s"✅ Document retrieved: $json")
    
  case ElasticSuccess(None) =>
    println("⚠️ Document not found")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### Async Typed Retrieval

```scala
case class Product(id: String, name: String, price: Double)

implicit val formats: Formats = DefaultFormats
implicit val ec: ExecutionContext = ExecutionContext.global

// Async get as typed entity
val futureResult: Future[ElasticResult[Option[Product]]] = 
  client.getAsyncAs[Product](
    id = "product-123",
    index = Some("products")
  )

futureResult.foreach {
  case ElasticSuccess(Some(product)) =>
    println(s"✅ Product: ${product.name}")
    println(f"   Price: $$${product.price}%.2f")
    
  case ElasticSuccess(None) =>
    println("⚠️ Product not found")
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### Chaining Async Operations

```scala
// Chain multiple async operations
val result: Future[ElasticResult[String]] = for {
  productResult <- client.getAsyncAs[Product]("product-123", Some("products"))
  customerResult <- productResult match {
    case ElasticSuccess(Some(product)) =>
      // Get related customer
      client.getAsyncAs[Customer](product.customerId, Some("customers"))
    case _ =>
      Future.successful(ElasticFailure(ElasticError("Product not found")))
  }
} yield {
  customerResult match {
    case ElasticSuccess(Some(customer)) =>
      ElasticSuccess(s"Customer: ${customer.name}")
    case ElasticSuccess(None) =>
      ElasticFailure(ElasticError("Customer not found"))
    case failure @ ElasticFailure(_) =>
      failure
  }
}

result.foreach {
  case ElasticSuccess(message) => println(message)
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}
```

---

### Parallel Async Operations

```scala
// Retrieve multiple documents in parallel
val ids = Seq("product-1", "product-2", "product-3")

val futures: Seq[Future[ElasticResult[Option[Product]]]] = ids.map { id =>
  client.getAsyncAs[Product](id, Some("products"))
}

// Wait for all to complete
Future.sequence(futures).foreach { results =>
  val products = results.flatMap {
    case ElasticSuccess(Some(product)) => Some(product)
    case _ => None
  }
  
  println(s"Retrieved ${products.size} products:")
  products.foreach { p =>
    println(s"  - ${p.name}: $$${p.price}")
  }
}
```

---

### Error Recovery

```scala
val result = client.getAsyncAs[Product]("product-123", Some("products"))
  .recover {
    case ex: Exception =>
      println(s"Recovered from exception: ${ex.getMessage}")
      ElasticFailure(ElasticError(
        message = "Failed to retrieve product",
        cause = Some(ex)
      ))
  }

result.foreach {
  case ElasticSuccess(Some(product)) =>
    println(s"Product: ${product.name}")
  case ElasticSuccess(None) =>
    println("Product not found")
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

## Document Existence Check

### Basic Existence Check

```scala
// Check if document exists
val exists: ElasticResult[Boolean] = client.exists(
  id = "product-123",
  index = "products"
)

exists match {
  case ElasticSuccess(true) =>
    println("✅ Document exists")
    
  case ElasticSuccess(false) =>
    println("⚠️ Document does not exist")
    
  case ElasticFailure(error) =>
    println(s"❌ Error checking existence: ${error.message}")
}
```

---

### Conditional Retrieval

```scala
def getProductIfExists(id: String): ElasticResult[Option[Product]] = {
  client.exists(id, "products") match {
    case ElasticSuccess(true) =>
      // Document exists, retrieve it
      client.getAs[Product](id, Some("products"))
      
    case ElasticSuccess(false) =>
      // Document doesn't exist
      ElasticSuccess(None)
      
    case failure @ ElasticFailure(_) =>
      // Error checking existence
      failure.asInstanceOf[ElasticResult[Option[Product]]]
  }
}

// Usage
getProductIfExists("product-123") match {
  case ElasticSuccess(Some(product)) =>
    println(s"Product found: ${product.name}")
  case ElasticSuccess(None) =>
    println("Product not found")
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Batch Existence Check

```scala
def checkMultipleExists(ids: Seq[String], index: String): Map[String, Boolean] = {
  ids.map { id =>
    val exists = client.exists(id, index) match {
      case ElasticSuccess(value) => value
      case ElasticFailure(_) => false
    }
    id -> exists
  }.toMap
}

// Usage
val ids = Seq("product-1", "product-2", "product-3", "product-4")
val existenceMap = checkMultipleExists(ids, "products")

existenceMap.foreach { case (id, exists) =>
  if (exists) {
    println(s"✅ $id exists")
  } else {
    println(s"❌ $id does not exist")
  }
}
```

---

### Async Existence Check

```scala
def existsAsync(id: String, index: String)
  (implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = {
  client.getAsync(id, index).map {
    case ElasticSuccess(Some(_)) => ElasticSuccess(true)
    case ElasticSuccess(None) => ElasticSuccess(false)
    case failure @ ElasticFailure(_) => failure
  }
}

// Usage
existsAsync("product-123", "products").foreach {
  case ElasticSuccess(true) => println("Document exists")
  case ElasticSuccess(false) => println("Document does not exist")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}
```

---

## Error Handling

### Common Error Scenarios

```scala
val result = client.get("product-123", "products")

result match {
  // Success - document found
  case ElasticSuccess(Some(json)) =>
    println(s"✅ Document: $json")
    
  // Success - document not found (404)
  case ElasticSuccess(None) =>
    println("⚠️ Document not found")
    
  // Failure - various error types
  case ElasticFailure(error) =>
    error.statusCode match {
      case Some(400) =>
        println(s"❌ Bad request: ${error.message}")
        
      case Some(404) =>
        println(s"❌ Not found: ${error.message}")
        
      case Some(500) =>
        println(s"❌ Server error: ${error.message}")
        
      case _ =>
        println(s"❌ Error: ${error.message}")
    }
}
```

---

### Error Details

```scala
val result = client.get("product-123", "invalid-index!")

result match {
  case ElasticFailure(error) =>
    println(s"Message: ${error.message}")
    println(s"Status Code: ${error.statusCode.getOrElse("N/A")}")
    println(s"Index: ${error.index.getOrElse("N/A")}")
    println(s"Operation: ${error.operation.getOrElse("N/A")}")
    
    error.cause.foreach { throwable =>
      println(s"Cause: ${throwable.getMessage}")
      throwable.printStackTrace()
    }
    
  case _ =>
    println("No error")
}
```

---

### Validation Errors

```scala
// Invalid index name
val result1 = client.get("doc-123", "INVALID_INDEX!")

result1 match {
  case ElasticFailure(error) =>
    println(s"Validation error: ${error.message}")
    // Output: "Invalid index: index name must be lowercase..."
    
  case _ => ()
}

// Empty document ID
val result2 = client.get("", "products")

result2 match {
  case ElasticFailure(error) =>
    println(s"Validation error: ${error.message}")
    
  case _ => ()
}
```

---

### Deserialization Errors

```scala
case class Product(id: String, name: String, price: Double)

implicit val formats: Formats = DefaultFormats

// Document has incompatible structure
val result = client.getAs[Product]("invalid-doc", "products")

result match {
  case ElasticSuccess(Some(product)) =>
    println(s"Product: ${product.name}")
    
  case ElasticSuccess(None) =>
    println("Product not found")
    
  case ElasticFailure(error) =>
    // Deserialization error
    println(s"❌ Failed to deserialize: ${error.message}")
    error.cause.foreach { ex =>
      println(s"   Cause: ${ex.getMessage}")
    }
}
```

---

### Comprehensive Error Handler

```scala
def handleGetResult[T](result: ElasticResult[Option[T]]): Option[T] = {
  result match {
    case ElasticSuccess(Some(value)) =>
      logger.info("✅ Document retrieved successfully")
      Some(value)
      
    case ElasticSuccess(None) =>
      logger.warn("⚠️ Document not found")
      None
      
    case ElasticFailure(error) =>
      error.statusCode match {
        case Some(400) =>
          logger.error(s"❌ Bad request: ${error.message}")
        case Some(404) =>
          logger.warn(s"⚠️ Not found: ${error.message}")
        case Some(500) =>
          logger.error(s"❌ Server error: ${error.message}")
        case Some(503) =>
          logger.error(s"❌ Service unavailable: ${error.message}")
        case _ =>
          logger.error(s"❌ Unexpected error: ${error.message}")
      }
      
      error.cause.foreach { throwable =>
        logger.error("Exception details:", throwable)
      }
      
      None
  }
}

// Usage
val product = handleGetResult(
  client.getAs[Product]("product-123", Some("products"))
)

product match {
  case Some(p) => println(s"Product: ${p.name}")
  case None => println("Failed to retrieve product")
}
```

---

## Advanced Patterns

### Caching Pattern

```scala
import scala.collection.concurrent.TrieMap

class CachedProductRepository {
  private val cache = TrieMap.empty[String, Product]
  
  def getProduct(id: String): ElasticResult[Option[Product]] = {
    // Check cache first
    cache.get(id) match {
      case Some(product) =>
        logger.debug(s"Cache hit for product $id")
        ElasticSuccess(Some(product))
        
      case None =>
        // Cache miss, fetch from Elasticsearch
        logger.debug(s"Cache miss for product $id")
        client.getAs[Product](id, Some("products")) match {
          case success @ ElasticSuccess(Some(product)) =>
            // Store in cache
            cache.put(id, product)
            success
            
          case other => other
        }
    }
  }
  
  def invalidate(id: String): Unit = {
    cache.remove(id)
  }
  
  def clear(): Unit = {
    cache.clear()
  }
}

// Usage
val repo = new CachedProductRepository()

val product1 = repo.getProduct("product-123") // Cache miss, fetches from ES
val product2 = repo.getProduct("product-123") // Cache hit, no ES call
```

---

### Fallback Pattern

```scala
def getProductWithFallback(id: String): ElasticResult[Option[Product]] = {
  // Try primary index
  client.getAs[Product](id, Some("products")) match {
    case success @ ElasticSuccess(Some(_)) =>
      success
      
    case ElasticSuccess(None) =>
      // Try fallback index
      logger.warn(s"Product $id not found in primary index, trying fallback")
      client.getAs[Product](id, Some("products-archive"))
      
    case failure @ ElasticFailure(_) =>
      failure
  }
}

// Usage
getProductWithFallback("product-123") match {
  case ElasticSuccess(Some(product)) =>
    println(s"Product found: ${product.name}")
  case ElasticSuccess(None) =>
    println("Product not found in any index")
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

### Batch Retrieval

```scala
def getBatch[T <: AnyRef](
  ids: Seq[String],
  index: String
)(implicit
  m: Manifest[T],
  formats: Formats
): Map[String, T] = {
  ids.flatMap { id =>
    client.getAs[T](id, Some(index)) match {
      case ElasticSuccess(Some(entity)) =>
        Some(id -> entity)
      case _ =>
        logger.warn(s"Failed to retrieve document $id")
        None
    }
  }.toMap
}

// Usage
val productIds = Seq("product-1", "product-2", "product-3")
val products: Map[String, Product] = getBatch[Product](productIds, "products")

products.foreach { case (id, product) =>
  println(s"$id: ${product.name}")
}
```

---

### Async Batch Retrieval

```scala
def getBatchAsync[T <: AnyRef](
  ids: Seq[String],
  index: String
)(implicit
  m: Manifest[T],
  ec: ExecutionContext,
  formats: Formats
): Future[Map[String, T]] = {
  val futures = ids.map { id =>
    client.getAsyncAs[T](id, Some(index)).map { result =>
      result match {
        case ElasticSuccess(Some(entity)) => Some(id -> entity)
        case _ => None
      }
    }
  }
  
  Future.sequence(futures).map(_.flatten.toMap)
}

// Usage
getBatchAsync[Product](
  Seq("product-1", "product-2", "product-3"),
  "products"
).foreach { products =>
  println(s"Retrieved ${products.size} products")
  products.foreach { case (id, product) =>
    println(s"  $id: ${product.name}")
  }
}
```

---

### Retry Pattern

```scala
import scala.annotation.tailrec
import scala.concurrent.duration._

@tailrec
def getWithRetry[T <: AnyRef](
  id: String,
  index: String,
  maxRetries: Int = 3,
  retryDelay: Duration = 1.second
)(implicit
  m: Manifest[T],
  formats: Formats
): ElasticResult[Option[T]] = {
  client.getAs[T](id, Some(index)) match {
    case success @ ElasticSuccess(_) =>
      success
      
    case failure @ ElasticFailure(error) if maxRetries > 0 =>
      error.statusCode match {
        case Some(500) | Some(503) =>
          // Retry on server errors
          logger.warn(s"Retrying after ${retryDelay.toMillis}ms (${maxRetries} retries left)")
          Thread.sleep(retryDelay.toMillis)
          getWithRetry(id, index, maxRetries - 1, retryDelay)
          
        case _ =>
          // Don't retry on client errors
          failure
      }
      
    case failure =>
      failure
  }
}

// Usage
val product = getWithRetry[Product]("product-123", "products")
```

---

### Validation and Transformation

```scala
def getAndValidate[T <: AnyRef](
  id: String,
  index: String,
  validate: T => Either[String, T]
)(implicit
  m: Manifest[T],
  formats: Formats
): ElasticResult[Option[T]] = {
  client.getAs[T](id, Some(index)).flatMap {
    case Some(entity) =>
      validate(entity) match {
        case Right(validEntity) =>
          ElasticSuccess(Some(validEntity))
        case Left(error) =>
          ElasticFailure(ElasticError(
            message = s"Validation failed: $error",
            index = Some(index),
            operation = Some("getAndValidate")
          ))
      }
    case None =>
      ElasticSuccess(None)
  }
}

// Usage
def validateProduct(product: Product): Either[String, Product] = {
  if (product.price < 0) {
    Left("Price cannot be negative")
  } else if (product.name.isEmpty) {
    Left("Name cannot be empty")
  } else {
    Right(product)
  }
}

getAndValidate[Product]("product-123", "products", validateProduct) match {
  case ElasticSuccess(Some(product)) =>
    println(s"Valid product: ${product.name}")
  case ElasticSuccess(None) =>
    println("Product not found")
  case ElasticFailure(error) =>
    println(s"Error: ${error.message}")
}
```

---

## Testing

### Test Basic Get

```scala
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class GetApiSpec extends AnyFlatSpec with Matchers {
  
  "GetApi" should "retrieve existing document" in {
    val testIndex = "test-get"
    val docId = "test-doc-1"
    val docContent = """{"id":"test-doc-1","name":"Test Product","price":99.99}"""
    
    // Setup
    client.createIndex(testIndex)
    client.index(testIndex, docId, docContent)
    client.refresh(testIndex)
    
    // Test
    val result = client.get(docId, testIndex)
    
    // Assertions
    result match {
      case ElasticSuccess(Some(json)) =>
        json should include("Test Product")
        json should include("99.99")
        
      case other =>
        fail(s"Expected success, got: $other")
    }
    
    // Cleanup
    client.deleteIndex(testIndex)
  }
  
  it should "return None for non-existent document" in {
    val testIndex = "test-get"
    
    // Setup
    client.createIndex(testIndex)
    
    // Test
    val result = client.get("non-existent-id", testIndex)
    
    // Assertions
    result match {
      case ElasticSuccess(None) =>
        // Expected
        succeed
        
      case other =>
        fail(s"Expected None, got: $other")
    }
    
    // Cleanup
    client.deleteIndex(testIndex)
  }
}
```

---

## Test Typed Get

```scala
"GetApi" should "retrieve and deserialize document" in {
  case class TestProduct(id: String, name: String, price: Double)
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-typed-get"
  val docId = "product-1"
  val docContent = """{"id":"product-1","name":"Laptop","price":999.99}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test
  val result = client.getAs[TestProduct](docId, Some(testIndex))
  
  // Assertions
  result match {
    case ElasticSuccess(Some(product)) =>
      product.id shouldBe "product-1"
      product.name shouldBe "Laptop"
      product.price shouldBe 999.99
      
    case other =>
      fail(s"Expected success, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Existence Check

```scala
"GetApi" should "check document existence correctly" in {
  val testIndex = "test-exists"
  val docId = "test-doc-1"
  val docContent = """{"id":"test-doc-1","value":"test"}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test - document exists
  val existsResult = client.exists(docId, testIndex)
  existsResult match {
    case ElasticSuccess(true) => succeed
    case other => fail(s"Expected true, got: $other")
  }
  
  // Test - document doesn't exist
  val notExistsResult = client.exists("non-existent-id", testIndex)
  notExistsResult match {
    case ElasticSuccess(false) => succeed
    case other => fail(s"Expected false, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Async Operations

```scala
"GetApi" should "retrieve document asynchronously" in {
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  val testIndex = "test-async-get"
  val docId = "async-doc-1"
  val docContent = """{"id":"async-doc-1","name":"Async Product","price":149.99}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test
  val futureResult = client.getAsync(docId, testIndex)
  
  futureResult.map {
    case ElasticSuccess(Some(json)) =>
      json should include("Async Product")
      json should include("149.99")
      
    case other =>
      fail(s"Expected success, got: $other")
  }.flatMap { _ =>
    // Cleanup
    Future.successful(client.deleteIndex(testIndex))
  }
}
```

---

### Test Async Typed Get

```scala
"GetApi" should "retrieve and deserialize document asynchronously" in {
  case class AsyncProduct(id: String, name: String, price: Double)
  implicit val formats: Formats = DefaultFormats
  implicit val ec: ExecutionContext = ExecutionContext.global
  
  val testIndex = "test-async-typed"
  val docId = "product-1"
  val docContent = """{"id":"product-1","name":"Wireless Mouse","price":29.99}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test
  val futureResult = client.getAsyncAs[AsyncProduct](docId, Some(testIndex))
  
  futureResult.map {
    case ElasticSuccess(Some(product)) =>
      product.id shouldBe "product-1"
      product.name shouldBe "Wireless Mouse"
      product.price shouldBe 29.99
      
    case other =>
      fail(s"Expected success, got: $other")
  }.flatMap { _ =>
    // Cleanup
    Future.successful(client.deleteIndex(testIndex))
  }
}
```

---

### Test Error Handling

```scala
"GetApi" should "handle invalid index name" in {
  val result = client.get("doc-1", "INVALID_INDEX!")
  
  result match {
    case ElasticFailure(error) =>
      error.statusCode shouldBe Some(400)
      error.message should include("Invalid index")
      error.operation shouldBe Some("get")
      
    case other =>
      fail(s"Expected failure, got: $other")
  }
}

it should "handle deserialization errors" in {
  case class StrictProduct(id: String, name: String, price: Double, requiredField: String)
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-deser-error"
  val docId = "incomplete-doc"
  // Missing requiredField
  val docContent = """{"id":"incomplete-doc","name":"Product","price":99.99}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test
  val result = client.getAs[StrictProduct](docId, Some(testIndex))
  
  result match {
    case ElasticFailure(error) =>
      error.message should include("Failed to retrieve")
      
    case other =>
      fail(s"Expected failure, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Nested Objects

```scala
"GetApi" should "handle nested objects" in {
  case class Address(street: String, city: String, zipCode: String)
  case class Customer(id: String, name: String, email: String, address: Address)
  
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-nested"
  val docId = "customer-1"
  val docContent = """{
    "id": "customer-1",
    "name": "John Doe",
    "email": "john@example.com",
    "address": {
      "street": "123 Main St",
      "city": "New York",
      "zipCode": "10001"
    }
  }"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test
  val result = client.getAs[Customer](docId, Some(testIndex))
  
  result match {
    case ElasticSuccess(Some(customer)) =>
      customer.id shouldBe "customer-1"
      customer.name shouldBe "John Doe"
      customer.email shouldBe "john@example.com"
      customer.address.street shouldBe "123 Main St"
      customer.address.city shouldBe "New York"
      customer.address.zipCode shouldBe "10001"
      
    case other =>
      fail(s"Expected success, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Batch Operations

```scala
"GetApi" should "retrieve multiple documents" in {
  case class TestDoc(id: String, value: Int)
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-batch"
  
  // Setup
  client.createIndex(testIndex)
  (1 to 5).foreach { i =>
    client.index(testIndex, s"doc-$i", s"""{"id":"doc-$i","value":$i}""")
  }
  client.refresh(testIndex)
  
  // Test - retrieve all documents
  val results = (1 to 5).map { i =>
    client.getAs[TestDoc](s"doc-$i", Some(testIndex))
  }
  
  // Assertions
  results.foreach {
    case ElasticSuccess(Some(doc)) =>
      doc.value should be > 0
      doc.value should be <= 5
      
    case other =>
      fail(s"Expected success, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

### Test Index Inference

```scala
"GetApi" should "infer index name from type" in {
  case class Product(id: String, name: String)
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "product" // Lowercase class name
  val docId = "product-1"
  val docContent = """{"id":"product-1","name":"Test Product"}"""
  
  // Setup
  client.createIndex(testIndex)
  client.index(testIndex, docId, docContent)
  client.refresh(testIndex)
  
  // Test - without explicit index (should infer "product")
  val result = client.getAs[Product](docId)
  
  result match {
    case ElasticSuccess(Some(product)) =>
      product.id shouldBe "product-1"
      product.name shouldBe "Test Product"
      
    case other =>
      fail(s"Expected success, got: $other")
  }
  
  // Cleanup
  client.deleteIndex(testIndex)
}
```

---

## Best Practices

### 1. Always Handle All Result Cases

```scala
// ❌ BAD: Only handling success case
val result = client.get("doc-1", "products")
val json = result match {
  case ElasticSuccess(Some(json)) => json
}
// Throws MatchError if document not found or error occurs

// ✅ GOOD: Handle all cases
val result = client.get("doc-1", "products")
val json = result match {
  case ElasticSuccess(Some(json)) => 
    Some(json)
  case ElasticSuccess(None) => 
    logger.warn("Document not found")
    None
  case ElasticFailure(error) => 
    logger.error(s"Error: ${error.message}")
    None
}
```

---

### 2. Use Typed Retrieval When Possible

```scala
case class Product(id: String, name: String, price: Double)
implicit val formats: Formats = DefaultFormats

// ❌ BAD: Manual JSON parsing
val result = client.get("product-1", "products")
val product = result match {
  case ElasticSuccess(Some(json)) =>
    val parsed = parse(json)
    Product(
      (parsed \ "id").extract[String],
      (parsed \ "name").extract[String],
      (parsed \ "price").extract[Double]
    )
  case _ => null
}

// ✅ GOOD: Automatic deserialization
val result = client.getAs[Product]("product-1", Some("products"))
val product = result match {
  case ElasticSuccess(Some(p)) => Some(p)
  case _ => None
}
```

---

### 3. Use Async for Multiple Retrievals

```scala
implicit val ec: ExecutionContext = ExecutionContext.global

// ❌ BAD: Sequential synchronous calls
val product1 = client.getAs[Product]("product-1", Some("products"))
val product2 = client.getAs[Product]("product-2", Some("products"))
val product3 = client.getAs[Product]("product-3", Some("products"))
// Total time: T1 + T2 + T3

// ✅ GOOD: Parallel async calls
val futures = Seq("product-1", "product-2", "product-3").map { id =>
  client.getAsyncAs[Product](id, Some("products"))
}
Future.sequence(futures)
// Total time: max(T1, T2, T3)
```

---

### 4. Validate Index Names

```scala
// ✅ GOOD: Index validation is automatic
val result = client.get("doc-1", "INVALID_INDEX!")
result match {
  case ElasticFailure(error) =>
    // Error is caught with statusCode 400
    println(s"Validation error: ${error.message}")
  case _ => ()
}
```

---

### 5. Use exists() for Existence Checks

```scala
// ❌ BAD: Using get() just to check existence
val exists = client.get("doc-1", "products") match {
  case ElasticSuccess(Some(_)) => true
  case _ => false
}

// ✅ GOOD: Use dedicated exists() method
val exists = client.exists("doc-1", "products") match {
  case ElasticSuccess(value) => value
  case ElasticFailure(_) => false
}
```

---

### 6. Handle Deserialization Errors

```scala
case class Product(id: String, name: String, price: Double)
implicit val formats: Formats = DefaultFormats

// ✅ GOOD: Handle deserialization errors
val result = client.getAs[Product]("product-1", Some("products"))
result match {
  case ElasticSuccess(Some(product)) =>
    println(s"Product: ${product.name}")
    
  case ElasticSuccess(None) =>
    println("Product not found")
    
  case ElasticFailure(error) =>
    // Could be network error OR deserialization error
    error.cause match {
      case Some(ex: org.json4s.MappingException) =>
        println(s"Deserialization error: ${ex.getMessage}")
      case _ =>
        println(s"Error: ${error.message}")
    }
}
```

---

### 7. Use Custom Formats for Complex Types

```scala
import org.json4s.ext.JavaTimeSerializers
import java.time.LocalDateTime

case class Order(
  id: String,
  total: Double,
  createdAt: LocalDateTime
)

// ✅ GOOD: Include necessary serializers
implicit val formats: Formats = DefaultFormats ++ JavaTimeSerializers.all

val result = client.getAs[Order]("order-1", Some("orders"))
```

---

### 8. Log Operations for Debugging

```scala
// ✅ GOOD: Logging is built-in
val result = client.get("product-1", "products")
// Automatically logs:
// DEBUG: Getting document with id 'product-1' from index 'products'
// INFO: ✅ Successfully retrieved document with id 'product-1' from index 'products'
// or
// ERROR: ❌ Failed to retrieve document with id 'product-1' from index 'products': <error>
```

---

### 9. Use Index Inference Wisely

```scala
case class Product(id: String, name: String)
implicit val formats: Formats = DefaultFormats

// ✅ GOOD: Use inference for simple cases
val result1 = client.getAs[Product]("product-1")
// Uses "product" as index name

// ✅ GOOD: Be explicit for complex cases
val result2 = client.getAs[Product](
  "product-1",
  index = Some("products-v2") // Explicit index
)
```

---

### 10. Implement Retry Logic for Transient Errors

```scala
// ✅ GOOD: Retry on server errors
def getWithRetry[T <: AnyRef](
  id: String,
  index: String,
  maxRetries: Int = 3
)(implicit
  m: Manifest[T],
  formats: Formats
): ElasticResult[Option[T]] = {
  
  @tailrec
  def retry(attemptsLeft: Int): ElasticResult[Option[T]] = {
    client.getAs[T](id, Some(index)) match {
      case success @ ElasticSuccess(_) =>
        success
        
      case failure @ ElasticFailure(error) =>
        error.statusCode match {
          case Some(500 | 503) if attemptsLeft > 0 =>
            logger.warn(s"Retrying... ($attemptsLeft attempts left)")
            Thread.sleep(1000)
            retry(attemptsLeft - 1)
            
          case _ =>
            failure
        }
    }
  }
  
  retry(maxRetries)
}
```

---

### 11. Use Pattern Matching for Clean Code

```scala
// ✅ GOOD: Clean pattern matching
def processProduct(id: String): Unit = {
  client.getAs[Product](id, Some("products")) match {
    case ElasticSuccess(Some(product)) =>
      println(s"Processing: ${product.name}")
      // Process product
      
    case ElasticSuccess(None) =>
      println(s"Product $id not found")
      
    case ElasticFailure(error) =>
      error.statusCode match {
        case Some(404) => println("Not found")
        case Some(500) => println("Server error")
        case _ => println(s"Error: ${error.message}")
      }
  }
}
```

---

### 12. Test Edge Cases

```scala
// ✅ GOOD: Test various scenarios
class GetApiEdgeCasesSpec extends AnyFlatSpec with Matchers {
  
  "GetApi" should "handle empty document ID" in {
    val result = client.get("", "products")
    result shouldBe a[ElasticFailure]
  }
  
  it should "handle very long document ID" in {
    val longId = "a" * 1000
    val result = client.get(longId, "products")
    // Should handle gracefully
  }
  
  it should "handle special characters in ID" in {
    val specialId = "product-123!@#$%"
    val result = client.get(specialId, "products")
    // Should handle gracefully
  }
  
  it should "handle non-existent index" in {
    val result = client.get("doc-1", "non-existent-index")
    result match {
      case ElasticFailure(error) =>
        error.statusCode should (be(Some(404)) or be(Some(400)))
      case _ => ()
    }
  }
}
```

---

## Common Patterns

### Pattern 1: Get or Create Default

```scala
def getOrDefault[T <: AnyRef](
  id: String,
  index: String,
  default: => T
)(implicit
  m: Manifest[T],
  formats: Formats
): T = {
  client.getAs[T](id, Some(index)) match {
    case ElasticSuccess(Some(entity)) => entity
    case _ => default
  }
}

// Usage
val product = getOrDefault[Product](
  "product-123",
  "products",
  Product("product-123", "Default Product", 0.0)
)
```

---

### Pattern 2: Get with Transformation

```scala
def getAndTransform[T <: AnyRef, U](
  id: String,
  index: String,
  transform: T => U
)(implicit
  m: Manifest[T],
  formats: Formats
): ElasticResult[Option[U]] = {
  client.getAs[T](id, Some(index)).map {
    case Some(entity) => Some(transform(entity))
    case None => None
  }
}

// Usage
val productSummary = getAndTransform[Product, String](
  "product-123",
  "products",
  p => s"${p.name}: $${p.price}"
)
```

---

### Pattern 3: Get with Validation

```scala
def getValidated[T <: AnyRef](
  id: String,
  index: String,
  validate: T => Boolean
)(implicit
  m: Manifest[T],
  formats: Formats
): ElasticResult[Option[T]] = {
  client.getAs[T](id, Some(index)).flatMap {
    case Some(entity) if validate(entity) =>
      ElasticSuccess(Some(entity))
      
    case Some(_) =>
      ElasticFailure(ElasticError(
        message = s"Validation failed for document $id",
        index = Some(index)
      ))
      
    case None =>
      ElasticSuccess(None)
  }
}

// Usage
val validProduct = getValidated[Product](
  "product-123",
  "products",
  p => p.price > 0 && p.name.nonEmpty
)
```

---

### Pattern 4: Repository Pattern

```scala
class ProductRepository(client: ElasticClient) {
  private val indexName = "products"
  
  implicit val formats: Formats = DefaultFormats
  
  def findById(id: String): Option[Product] = {
    client.getAs[Product](id, Some(indexName)) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error) =>
        logger.error(s"Failed to find product $id: ${error.message}")
        None
    }
  }
  
  def exists(id: String): Boolean = {
    client.exists(id, indexName) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(_) => false
    }
  }
  
  def findByIdAsync(id: String)
    (implicit ec: ExecutionContext): Future[Option[Product]] = {
    client.getAsyncAs[Product](id, Some(indexName)).map {
      case ElasticSuccess(result) => result
      case ElasticFailure(error) =>
        logger.error(s"Failed to find product $id: ${error.message}")
        None
    }
  }
}

// Usage
val repo = new ProductRepository(client)
val product = repo.findById("product-123")
```

---

### Pattern 5: Caching with TTL

```scala
import scala.concurrent.duration._

case class CacheEntry[T](value: T, expiresAt: Long)

class CachedRepository[T <: AnyRef](
  client: ElasticClient,
  index: String,
  ttl: Duration = 5.minutes
)(implicit m: Manifest[T], formats: Formats) {
  
  private val cache = TrieMap.empty[String, CacheEntry[T]]
  
  def get(id: String): Option[T] = {
    val now = System.currentTimeMillis()
    
    cache.get(id) match {
      case Some(entry) if entry.expiresAt > now =>
        // Cache hit
        Some(entry.value)
        
      case _ =>
        // Cache miss or expired
        client.getAs[T](id, Some(index)) match {
          case ElasticSuccess(Some(entity)) =>
            val expiresAt = now + ttl.toMillis
            cache.put(id, CacheEntry(entity, expiresAt))
            Some(entity)
            
          case _ =>
            cache.remove(id)
            None
        }
    }
  }
  
  def invalidate(id: String): Unit = {
    cache.remove(id)
  }
}

// Usage
val repo = new CachedRepository[Product](client, "products", 10.minutes)
val product = repo.get("product-123")
```

---

## Summary

The **Get API** provides:

✅ **Simple document retrieval** by ID  
✅ **Type-safe deserialization** with automatic conversion  
✅ **Existence checking** without full retrieval  
✅ **Async operations** for better performance  
✅ **Comprehensive error handling** with detailed errors  
✅ **Automatic validation** of index names  
✅ **Index name inference** from entity types

**Method Selection Guide:**

| Use Case                 | Recommended Method                   |
|--------------------------|--------------------------------------|
| Check if document exists | `exists()`                           |
| Get raw JSON             | `get()`                              |
| Get typed entity         | `getAs[T]()`                         |
| Get multiple documents   | `getAsync()` + `Future.sequence()`   |
| Get with custom index    | `getAs[T](id, Some("custom-index"))` |
| Get with type inference  | `getAs[T](id)`                       |

**Best Practices:**

1. ✅ Always handle all result cases (Success/None/Failure)
2. ✅ Use typed retrieval (`getAs`) when possible
3. ✅ Use async methods for multiple retrievals
4. ✅ Use `exists()` for existence checks only
5. ✅ Handle deserialization errors explicitly
6. ✅ Use custom formats for complex types
7. ✅ Implement retry logic for transient errors
8. ✅ Use repository pattern for cleaner code
9. ✅ Test edge cases (empty IDs, invalid indexes)
10. ✅ Add caching for frequently accessed documents

**Error Handling:**

- **400**: Invalid index name or parameters
- **404**: Document or index not found
- **500**: Server error (consider retry)
- **503**: Service unavailable (consider retry)

---

[Back to index](README.md) | [Next: Search Documents](search.md)