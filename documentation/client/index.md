[Back to index](README.md)

# INDEX API

## Overview

The **IndexApi** trait provides functionality to index documents into Elasticsearch, supporting both synchronous and asynchronous operations with automatic serialization.

**Features:**
- Synchronous and asynchronous indexing
- Automatic JSON serialization from Scala objects
- Type-safe indexing with implicit serialization
- Wait for a refresh to happen after indexing if required
- Comprehensive validation and error handling
- Support for custom index names and document IDs

**Dependencies:**
- Requires `RefreshApi` for automatic refresh after indexing
- Requires `SerializationApi` for JSON serialization

---

## Public Methods

### indexAs

Indexes a Scala object into Elasticsearch with automatic JSON serialization.

**Signature:**

```scala
def indexAs[U <: AnyRef](
  entity: U,
  id: String,
  index: Option[String] = None,
  maybeType: Option[String] = None,
  wait: Boolean = false
)(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean]
```

**Parameters:**
- `entity` - The Scala object to index
- `id` - The document ID
- `index` - Optional index name (defaults to entity type name in lowercase)
- `maybeType` - Optional type name (defaults to entity class name in lowercase)
- `wait` - If `true`, waits for a refresh to happen after indexing (default is `false`)
- `u` - Implicit ClassTag for type information
- `formats` - Implicit JSON serialization formats

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if indexed successfully
- `ElasticFailure` with error details

**Behavior:**
- Automatically serializes entity to JSON
- Defaults index name to entity class name if not provided
- Waits for a refresh to happen after successful indexing (disabled by default)

**Examples:**

```scala
// Domain models
case class Product(name: String, price: Double, category: String)
case class User(username: String, email: String, age: Int)

implicit val formats: Formats = DefaultFormats

// Basic indexing with auto-generated index name
val product = Product("Laptop", 999.99, "Electronics")
client.indexAs(product, id = "prod-001") match {
  case ElasticSuccess(true) => println("Product indexed")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}
// Indexes to "product" index (class name lowercased)

// Explicit index name
val user = User("john_doe", "john@example.com", 30)
client.indexAs(
  entity = user,
  id = "user-123",
  index = Some("users-v2")
)
// Indexes to "users-v2" index

// Custom type name
client.indexAs(
  entity = product,
  id = "prod-002",
  index = Some("catalog"),
  maybeType = Some("electronics")
)

// Index multiple documents
val products = List(
  Product("Phone", 699.99, "Electronics"),
  Product("Tablet", 499.99, "Electronics"),
  Product("Headphones", 199.99, "Audio")
)

products.zipWithIndex.foreach { case (product, idx) =>
  client.indexAs(product, id = s"prod-${idx + 1}", index = Some("products"))
}

// With custom serialization formats
import org.json4s.ext.JavaTimeSerializers

case class Event(name: String, timestamp: java.time.Instant)

implicit val customFormats: Formats = DefaultFormats ++ JavaTimeSerializers.all

val event = Event("UserLogin", java.time.Instant.now())
client.indexAs(event, id = "evt-001", index = Some("events"))

// Error handling with pattern matching
client.indexAs(product, id = "prod-001") match {
  case ElasticSuccess(true) =>
    println("✅ Document indexed and searchable")
  case ElasticSuccess(false) =>
    println("⚠️ Document not indexed")
  case ElasticFailure(error) =>
    println(s"❌ Indexing failed: ${error.message}")
    error.cause.foreach(ex => println(s"Cause: ${ex.getMessage}"))
}

// Monadic composition
def indexWithValidation[T <: AnyRef](
  entity: T,
  id: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    validated <- validateEntity(entity)
    indexed <- client.indexAs(validated, id)
  } yield indexed
}
```

---

### index

Indexes a document into Elasticsearch using a raw JSON string.

**Signature:**

```scala
def index(index: String, id: String, source: String, wait: Boolean = false): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name
- `id` - The document ID
- `source` - The document as a JSON string
- `wait` - If `true`, waits for a refresh to happen after indexing (default is `false`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if indexed successfully
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation

**Behavior:**
- Creates or updates document (upsert behavior)
- Waits for a refresh to happen after successful indexing (disabled by default)

**Examples:**

```scala
// Basic indexing with JSON string
val json = """
{
  "name": "Laptop",
  "price": 999.99,
  "category": "Electronics"
}
"""
client.index("products", "prod-001", json)

// Compact JSON
val compactJson = """{"title":"Elasticsearch Guide","author":"John Doe"}"""
client.index("books", "book-001", compactJson)

// Dynamic JSON generation
def createUserJson(username: String, email: String): String = {
  s"""
  {
    "username": "$username",
    "email": "$email",
    "created_at": "${java.time.Instant.now()}"
  }
  """
}

client.index("users", "user-123", createUserJson("john_doe", "john@example.com"))

// Index with JSON library
import org.json4s.jackson.JsonMethods._

val data = Map(
  "name" -> "Product Name",
  "price" -> 99.99,
  "tags" -> List("electronics", "sale")
)
val jsonString = compact(render(decompose(data)))
client.index("products", "prod-002", jsonString)

// Batch indexing with JSON
val documents = List(
  ("doc-1", """{"field": "value1"}"""),
  ("doc-2", """{"field": "value2"}"""),
  ("doc-3", """{"field": "value3"}""")
)

documents.foreach { case (id, json) =>
  client.index("my-index", id, json)
}

// Update existing document
val updatedJson = """
{
  "name": "Updated Product",
  "price": 899.99,
  "category": "Electronics",
  "updated_at": "2024-01-15T10:30:00Z"
}
"""
client.index("products", "prod-001", updatedJson) // Overwrites existing

// Index with validation
def safeIndex(index: String, id: String, json: String): ElasticResult[Boolean] = {
  // Validate JSON before indexing
  ElasticResult.attempt(parse(json)).flatMap { _ =>
    client.index(index, id, json)
  }
}

// Error handling
client.index("products", "prod-001", json) match {
  case ElasticSuccess(true) =>
    println("✅ Document indexed and refreshed")
  case ElasticFailure(error) if error.statusCode.contains(400) =>
    println(s"❌ Validation error: ${error.message}")
  case ElasticFailure(error) =>
    println(s"❌ Indexing error: ${error.message}")
}
```

---

### indexAsyncAs

Asynchronously indexes a Scala object with automatic JSON serialization.

**Signature:**

```scala
def indexAsyncAs[U <: AnyRef](
  entity: U,
  id: String,
  index: Option[String] = None,
  maybeType: Option[String] = None,
  wait: Boolean = false
)(implicit
  u: ClassTag[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Boolean]]
```

**Parameters:**
- `entity` - The Scala object to index
- `id` - The document ID
- `index` - Optional index name
- `maybeType` - Optional type name
- `wait` - If `true`, waits for a refresh to happen after indexing (default is `false`)
- `u` - Implicit ClassTag
- `ec` - Implicit ExecutionContext for async execution
- `formats` - Implicit JSON serialization formats

**Returns:**
- `Future[ElasticResult[Boolean]]` that completes when indexing finishes

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

case class Product(name: String, price: Double)
implicit val formats: Formats = DefaultFormats

// Basic async indexing
val product = Product("Laptop", 999.99)
val futureResult: Future[ElasticResult[Boolean]] = 
  client.indexAsyncAs(product, id = "prod-001")

futureResult.onComplete {
  case Success(ElasticSuccess(true)) =>
    println("✅ Product indexed asynchronously")
  case Success(ElasticFailure(error)) =>
    println(s"❌ Indexing failed: ${error.message}")
  case Failure(exception) =>
    println(s"❌ Future failed: ${exception.getMessage}")
}

// Await result (for testing)
import scala.concurrent.Await
import scala.concurrent.duration._

val result = Await.result(
  client.indexAsyncAs(product, id = "prod-001"),
  5.seconds
)

// Batch async indexing
val products = List(
  Product("Phone", 699.99),
  Product("Tablet", 499.99),
  Product("Laptop", 999.99)
)

val futures: List[Future[ElasticResult[Boolean]]] = 
  products.zipWithIndex.map { case (product, idx) =>
    client.indexAsyncAs(product, id = s"prod-${idx + 1}")
  }

Future.sequence(futures).onComplete {
  case Success(results) =>
    val successful = results.count {
      case ElasticSuccess(true) => true
      case _ => false
    }
    println(s"✅ Indexed $successful/${results.length} documents")
  case Failure(exception) =>
    println(s"❌ Batch indexing failed: ${exception.getMessage}")
}

// Parallel indexing with rate limiting
def indexWithRateLimit[T <: AnyRef](
  entities: List[(T, String)],
  maxConcurrent: Int = 10
)(implicit ct: ClassTag[T], ec: ExecutionContext, formats: Formats): Future[List[ElasticResult[Boolean]]] = {
  
  entities.grouped(maxConcurrent).foldLeft(Future.successful(List.empty[ElasticResult[Boolean]])) {
    case (accFuture, batch) =>
      accFuture.flatMap { acc =>
        val batchFutures = batch.map { case (entity, id) =>
          client.indexAsyncAs(entity, id)
        }
        Future.sequence(batchFutures).map(acc ++ _)
      }
  }
}

// Non-blocking pipeline
def processAndIndex(data: List[RawData]): Future[Int] = {
  val processedFuture = Future {
    data.map(transform)
  }
  
  processedFuture.flatMap { processed =>
    val indexFutures = processed.map { entity =>
      client.indexAsyncAs(entity, id = entity.id)
    }
    
    Future.sequence(indexFutures).map { results =>
      results.count {
        case ElasticSuccess(true) => true
        case _ => false
      }
    }
  }
}

// Error recovery
def indexWithRetry[T <: AnyRef](
  entity: T,
  id: String,
  maxRetries: Int = 3
)(implicit ct: ClassTag[T], ec: ExecutionContext, formats: Formats): Future[ElasticResult[Boolean]] = {
  
  def attempt(remaining: Int): Future[ElasticResult[Boolean]] = {
    client.indexAsyncAs(entity, id).flatMap {
      case success @ ElasticSuccess(true) => Future.successful(success)
      case failure if remaining > 0 =>
        Thread.sleep(1000) // Backoff
        attempt(remaining - 1)
      case failure => Future.successful(failure)
    }
  }
  
  attempt(maxRetries)
}
```

---

### indexAsync

Asynchronously indexes a document using a raw JSON string.

**Signature:**

```scala
def indexAsync(
  index: String,
  id: String,
  source: String,
  wait: Boolean = false
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Parameters:**
- `index` - The index name
- `id` - The document ID
- `source` - The document as a JSON string
- `wait` - If `true`, waits for a refresh to happen after indexing (default is `false`)
- `ec` - Implicit ExecutionContext

**Returns:**
- `Future[ElasticResult[Boolean]]` that completes when indexing finishes

**Validation:**
- Index name format validation (performed synchronously before async execution)

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// Basic async indexing
val json = """{"name": "Product", "price": 99.99}"""
val future = client.indexAsync("products", "prod-001", json)

future.onComplete {
  case Success(ElasticSuccess(true)) =>
    println("✅ Document indexed")
  case Success(ElasticFailure(error)) =>
    println(s"❌ Error: ${error.message}")
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Batch async indexing
val documents = List(
  ("doc-1", """{"field": "value1"}"""),
  ("doc-2", """{"field": "value2"}"""),
  ("doc-3", """{"field": "value3"}""")
)

val futures = documents.map { case (id, json) =>
  client.indexAsync("my-index", id, json)
}

Future.sequence(futures).map { results =>
  val successCount = results.count {
    case ElasticSuccess(true) => true
    case _ => false
  }
  println(s"Indexed $successCount/${results.length} documents")
}

// Streaming indexing
import akka.stream.scaladsl._

def streamIndex(
  index: String,
  source: Source[(String, String), _]
): Future[Int] = {
  source
    .mapAsync(parallelism = 10) { case (id, json) =>
      client.indexAsync(index, id, json)
    }
    .runFold(0) { (count, result) =>
      result match {
        case ElasticSuccess(true) => count + 1
        case _ => count
      }
    }
}

// Dynamic content generation
def generateAndIndex(
  index: String,
  ids: List[String]
): Future[List[ElasticResult[Boolean]]] = {
  val futures = ids.map { id =>
    val json = generateDynamicContent(id)
    client.indexAsync(index, id, json)
  }
  Future.sequence(futures)
}

// Chained async operations
def fetchAndIndex(
  externalId: String
): Future[ElasticResult[Boolean]] = {
  for {
    data <- fetchFromExternalAPI(externalId)
    json = convertToJson(data)
    result <- client.indexAsync("my-index", externalId, json)
  } yield result
}

// Error handling with recovery
client.indexAsync("products", "prod-001", json)
  .recover {
    case ex: Exception =>
      ElasticFailure(ElasticError(
        message = s"Indexing failed: ${ex.getMessage}",
        cause = Some(ex)
      ))
  }
  .foreach {
    case ElasticSuccess(true) => println("Success")
    case ElasticFailure(e) => println(s"Failed: ${e.message}")
  }
```

---

## Implementation Requirements

### executeIndex

```scala
private[client] def executeIndex(
  index: String,
  id: String,
  source: String,
  wait: Boolean
): ElasticResult[Boolean]
```

**Implementation Example:**

```scala
private[client] def executeIndex(
  index: String,
  id: String,
  source: String,
  wait: Boolean
): ElasticResult[Boolean] = {
  executeRestAction[IndexRequest, IndexResponse, Boolean](
    operation = "index",
    index = Some(index),
    retryable = false
  )(request =
    new IndexRequest(index)
      .id(id)
      .source(source, XContentType.JSON)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = req => apply().index(req, RequestOptions.DEFAULT)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.CREATED |
             DocWriteResponse.Result.UPDATED | DocWriteResponse.Result.NOOP =>
          true
        case _ => false
      }
  )
}
```

---

### executeIndexAsync

```scala
private[client] def executeIndexAsync(
  index: String,
  id: String,
  source: String
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Implementation Example:**

```scala
private[client] def executeIndexAsync(
  index: String,
  id: String,
  source: String,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = {
  executeAsyncRestAction[IndexRequest, IndexResponse, Boolean](
    operation = "indexAsync",
    index = Some(index),
    retryable = false
  )(
    request = new IndexRequest(index)
      .id(id)
      .source(source, XContentType.JSON)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = (req, listener) => apply().indexAsync(req, RequestOptions.DEFAULT, listener)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.CREATED |
             DocWriteResponse.Result.UPDATED | DocWriteResponse.Result.NOOP =>
          true
        case _ => false
      }
  )
}
```

---

## Common Patterns

### Repository Pattern

```scala
trait Repository[T <: AnyRef] {
  def save(entity: T, id: String)(implicit 
    ct: ClassTag[T], 
    formats: Formats,
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    client.indexAs(entity, id)
  }
  
  def saveAsync(entity: T, id: String)(implicit 
    ct: ClassTag[T], 
    formats: Formats,
    ec: ExecutionContext,
    client: ElasticClient
  ): Future[ElasticResult[Boolean]] = {
    client.indexAsyncAs(entity, id)
  }
}

case class Product(name: String, price: Double)

object ProductRepository extends Repository[Product] {
  implicit val formats: Formats = DefaultFormats
  
  def saveProduct(product: Product, id: String)(implicit 
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    save(product, id)
  }
}
```

### Bulk Indexing with Individual Operations

```scala
def indexAllIndividually[T <: AnyRef](
  entities: List[(T, String)],
  indexName: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[List[Boolean]] = {
  ElasticResult.sequence(
    entities.map { case (entity, id) =>
      client.indexAs(entity, id, index = Some(indexName))
    }
  )
}
```

### Upsert Pattern

```scala
def upsertDocument(
  index: String,
  id: String,
  document: Map[String, Any]
): ElasticResult[Boolean] = {
  val json = compact(render(decompose(document)))
  client.index(index, id, json)
}
```

### Versioned Documents

```scala
case class VersionedDocument[T](
  data: T,
  version: Int,
  updatedAt: java.time.Instant
)

def indexVersioned[T <: AnyRef](
  entity: T,
  id: String,
  version: Int
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  val versioned = VersionedDocument(entity, version, java.time.Instant.now())
  client.indexAs(versioned, id)
}
```

---

## Performance Optimization

### Disable Refresh for Bulk Operations

```scala
def bulkIndexOptimized[T <: AnyRef](
  entities: List[(T, String)],
  indexName: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Unit] = {
  for {
    // Disable refresh
    _ <- client.toggleRefresh(indexName, enable = false)
    
    // Index all documents
    _ <- entities.foldLeft(ElasticResult.success(())) { case (acc, (entity, id)) =>
      acc.flatMap(_ => client.indexAs(entity, id, Some(indexName)).map(_ => ()))
    }
    
    // Re-enable refresh
    _ <- client.toggleRefresh(indexName, enable = true)
    
    // Manual refresh
    _ <- client.refresh(indexName)
  } yield ()
}
```

### Parallel Async Indexing

```scala
def parallelIndex[T <: AnyRef](
  entities: List[(T, String)],
  parallelism: Int = 10
)(implicit 
  ct: ClassTag[T], 
  ec: ExecutionContext, 
  formats: Formats
): Future[List[ElasticResult[Boolean]]] = {
  
  entities
    .grouped(parallelism)
    .foldLeft(Future.successful(List.empty[ElasticResult[Boolean]])) {
      case (accFuture, batch) =>
        accFuture.flatMap { acc =>
          val batchFutures = batch.map { case (entity, id) =>
            client.indexAsyncAs(entity, id)
          }
          Future.sequence(batchFutures).map(acc ++ _)
        }
    }
}
```

---

## Error Handling

**Invalid Index Name:**

```scala
client.index("INVALID INDEX", "doc-1", json) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid index"))
}
```

**Serialization Failure:**

```scala
case class InvalidEntity(data: java.io.InputStream) // Not serializable

client.indexAs(InvalidEntity(null), "doc-1") match {
  case ElasticFailure(error) =>
    println(s"Serialization failed: ${error.message}")
}
```

**Async Failure Handling:**

```scala
client.indexAsync("products", "prod-001", invalidJson)
  .recover {
    case ex: Exception =>
      ElasticFailure(ElasticError(
        message = s"Async operation failed: ${ex.getMessage}",
        cause = Some(ex)
      ))
  }
```

---

## Best Practices

**1. Use Type-Safe indexAs for Domain Objects**

```scala
// ✅ Good - type-safe
case class User(name: String, email: String)
client.indexAs(user, "user-123")

// ❌ Avoid - error-prone
val json = s"""{"name":"${user.name}","email":"${user.email}"}"""
client.index("user", "user-123", json)
```

**2. Handle Refresh Appropriately**

```scala
// For single documents - automatic refresh is fine
client.indexAs(product, "prod-001")

// For bulk operations - disable refresh
for {
  _ <- client.toggleRefresh("products", enable = false)
  _ <- indexMany(products)
  _ <- client.toggleRefresh("products", enable = true)
  _ <- client.refresh("products")
} yield ()
```

**3. Use Async for High-Throughput Scenarios**

```scala
// ✅ Good - non-blocking
val futures = documents.map { case (id, doc) =>
  client.indexAsyncAs(doc, id)
}
Future.sequence(futures)

// ❌ Avoid - blocks thread pool
documents.foreach { case (id, doc) =>
  Await.result(client.indexAsyncAs(doc, id), 10.seconds)
}
```

**4. Implement Proper Error Handling**

```scala
def safeIndex[T <: AnyRef](entity: T, id: String)(implicit 
  ct: ClassTag[T], 
  formats: Formats
): ElasticResult[Boolean] = {
  client.indexAs(entity, id) match {
    case success @ ElasticSuccess(true) => success
    case failure @ ElasticFailure(error) =>
      logger.error(s"Failed to index $id: ${error.message}")
      // Implement retry logic or fallback
      failure
  }
}
```

---

[Back to index](README.md) | [Next: Update Documents](update.md)