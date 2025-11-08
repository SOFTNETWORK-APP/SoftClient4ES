[Back to index](README.md)

# UPDATE API

## Overview

The **UpdateApi** trait provides functionality to update documents in Elasticsearch, supporting both synchronous and asynchronous operations with automatic serialization and upsert capabilities.

**Features:**
- Synchronous and asynchronous updates
- Automatic JSON serialization from Scala objects
- Upsert support (insert if document doesn't exist)
- Type-safe updates with implicit serialization
- Wait for a refresh to happen after update if required
- Comprehensive validation and error handling
- Partial document updates

**Dependencies:**
- Requires `RefreshApi` for automatic refresh after updates
- Requires `SerializationApi` for JSON serialization

---

## Understanding Updates

**Update vs Index:**
- **Update:** Modifies existing document fields (partial update)
- **Index :** Replaces entire document (full replacement)

**Upsert Behavior:**
- `upsert = true`: Creates document if it doesn't exist
- `upsert = false`: Fails if document doesn't exist

**Update Process:**
1. Retrieves current document
2. Applies changes
3. Re-indexes modified document
4. Optionally wait for a refresh to happen

---

## Public Methods

### update

Updates a document in Elasticsearch using a raw JSON string.

**Signature:**

```scala
def update(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean = false
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name
- `id` - The document ID to update
- `source` - The update data as JSON (partial or full document)
- `wait` - If `true`, waits for a refresh to happen after update (default is `false`)
- `upsert` - Whether to create document if it doesn't exist

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if updated successfully
- `ElasticFailure` with error details (400 for validation errors, 404 if document not found and upsert=false)

**Validation:**
- Index name format validation
- JSON syntax validation

**Behavior:**
- Automatically refreshes index after successful update
- Returns failure if document doesn't exist and upsert=false

**Examples:**

```scala
// Basic update with upsert
val updateJson = """
{
  "price": 899.99,
  "updated_at": "2024-01-15T10:30:00Z"
}
"""
client.update("products", "prod-001", updateJson, upsert = true) match {
  case ElasticSuccess(true) => println("Document updated")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Update without upsert (fails if document doesn't exist)
val partialUpdate = """
{
  "status": "shipped",
  "tracking_number": "TRK123456"
}
"""
client.update("orders", "order-001", partialUpdate, upsert = false)

// Full document replacement via update
val fullDoc = """
{
  "name": "Updated Product",
  "price": 999.99,
  "category": "Electronics",
  "tags": ["new", "featured"],
  "updated_at": "2024-01-15T10:30:00Z"
}
"""
client.update("products", "prod-001", fullDoc, upsert = true)

// Increment counter
val incrementJson = """
{
  "views": 150,
  "last_viewed": "2024-01-15T10:30:00Z"
}
"""
client.update("analytics", "page-001", incrementJson, upsert = true)

// Update nested fields
val nestedUpdate = """
{
  "user": {
    "email": "newemail@example.com",
    "verified": true
  }
}
"""
client.update("users", "user-123", nestedUpdate, upsert = false)

// Conditional update with validation
def updateIfValid(
  index: String,
  id: String,
  json: String
): ElasticResult[Boolean] = {
  for {
    _ <- validateUpdateData(json)
    result <- client.update(index, id, json, upsert = false)
  } yield result
}

// Update with retry on failure
def updateWithRetry(
  index: String,
  id: String,
  json: String,
  maxRetries: Int = 3
): ElasticResult[Boolean] = {
  def attempt(remaining: Int): ElasticResult[Boolean] = {
    client.update(index, id, json, upsert = true) match {
      case success @ ElasticSuccess(true) => success
      case failure if remaining > 0 =>
        Thread.sleep(1000)
        attempt(remaining - 1)
      case failure => failure
    }
  }
  attempt(maxRetries)
}

// Batch updates
val updates = List(
  ("prod-001", """{"price": 899.99}"""),
  ("prod-002", """{"price": 699.99}"""),
  ("prod-003", """{"price": 499.99}""")
)

updates.foreach { case (id, json) =>
  client.update("products", id, json, upsert = true)
}

// Error handling
client.update("products", "prod-999", updateJson, upsert = false) match {
  case ElasticSuccess(true) =>
    println("✅ Document updated")
  case ElasticFailure(error) if error.message.contains("not updated") =>
    println("❌ Document not found")
  case ElasticFailure(error) =>
    println(s"❌ Update failed: ${error.message}")
}
```

---

### updateAs

Updates a Scala object in Elasticsearch with automatic JSON serialization.

**Signature:**

```scala
def updateAs[U <: AnyRef](
  entity: U,
  id: String,
  index: Option[String] = None,
  maybeType: Option[String] = None,
  upsert: Boolean = true,
  wait: Boolean = false
)(implicit u: ClassTag[U], formats: Formats): ElasticResult[Boolean]
```

**Parameters:**
- `entity` - The Scala object containing update data
- `id` - The document ID to update
- `index` - Optional index name (defaults to entity type name)
- `maybeType` - Optional type name (defaults to class name in lowercase)
- `upsert` - Whether to create document if it doesn't exist (default: true)
- `wait` - If `true`, waits for a refresh to happen after update (default is `false`)
- `u` - Implicit ClassTag for type information
- `formats` - Implicit JSON serialization formats

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if updated successfully
- `ElasticFailure` with error details

**Examples:**

```scala
// Domain models
case class Product(name: String, price: Double, category: String)
case class ProductUpdate(price: Double, updatedAt: String)
case class User(username: String, email: String, verified: Boolean)

implicit val formats: Formats = DefaultFormats

// Basic update with upsert
val productUpdate = ProductUpdate(899.99, "2024-01-15T10:30:00Z")
client.updateAs(productUpdate, id = "prod-001") match {
  case ElasticSuccess(true) => println("Product updated")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Update with explicit index
val user = User("john_doe", "newemail@example.com", verified = true)
client.updateAs(
  entity = user,
  id = "user-123",
  index = Some("users-v2"),
  upsert = true
)

// Update without upsert (strict update)
client.updateAs(
  entity = productUpdate,
  id = "prod-001",
  index = Some("products"),
  upsert = false
) match {
  case ElasticSuccess(true) => println("✅ Existing document updated")
  case ElasticFailure(e) => println(s"❌ Document not found: ${e.message}")
}

// Partial update model
case class StatusUpdate(status: String, updatedBy: String, timestamp: Long)

val statusUpdate = StatusUpdate(
  status = "completed",
  updatedBy = "admin",
  timestamp = System.currentTimeMillis()
)
client.updateAs(statusUpdate, id = "task-001", index = Some("tasks"))

// Update multiple documents
val updates = List(
  ("prod-001", ProductUpdate(899.99, "2024-01-15")),
  ("prod-002", ProductUpdate(699.99, "2024-01-15")),
  ("prod-003", ProductUpdate(499.99, "2024-01-15"))
)

updates.foreach { case (id, update) =>
  client.updateAs(update, id, index = Some("products"))
}

// Conditional update based on current state
def updateIfChanged[T <: AnyRef](
  entity: T,
  id: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    current <- client.get(id, indexNameFor[T])
    needsUpdate = hasChanged(current, entity)
    result <- if (needsUpdate) {
      client.updateAs(entity, id, upsert = false)
    } else {
      ElasticResult.success(false)
    }
  } yield result
}

// Type-safe update with validation
def validateAndUpdate[T <: AnyRef](
  entity: T,
  id: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    validated <- validate(entity)
    updated <- client.updateAs(validated, id, upsert = true)
  } yield updated
}

// Monadic composition
def updateWithAudit[T <: AnyRef](
  entity: T,
  id: String,
  userId: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    updated <- client.updateAs(entity, id)
    _ <- logAuditTrail(id, userId, "update")
  } yield updated
}

// Error handling
client.updateAs(productUpdate, "prod-001", upsert = false) match {
  case ElasticSuccess(true) =>
    println("✅ Document updated successfully")
  case ElasticFailure(error) if error.message.contains("not updated") =>
    println("❌ Document does not exist")
  case ElasticFailure(error) =>
    println(s"❌ Update failed: ${error.message}")
    error.cause.foreach(ex => println(s"Cause: ${ex.getMessage}"))
}
```

---

### updateAsync

Asynchronously updates a document using a raw JSON string.

**Signature:**

```scala
def updateAsync(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean = false
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Parameters:**
- `index` - The index name
- `id` - The document ID to update
- `source` - The update data as JSON
- `upsert` - Whether to create document if it doesn't exist
- `wait` - If `true`, waits for a refresh to happen after update (default is `false`)
- `ec` - Implicit ExecutionContext

**Returns:**
- `Future[ElasticResult[Boolean]]` that completes when update finishes

**Validation:**
- Index name and JSON validation performed synchronously before async execution

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// Basic async update
val updateJson = """{"price": 899.99, "updated_at": "2024-01-15"}"""
val future = client.updateAsync("products", "prod-001", updateJson, upsert = true)

future.onComplete {
  case Success(ElasticSuccess(true)) =>
    println("✅ Document updated")
  case Success(ElasticFailure(error)) =>
    println(s"❌ Error: ${error.message}")
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Batch async updates
val updates = List(
  ("prod-001", """{"price": 899.99}"""),
  ("prod-002", """{"price": 699.99}"""),
  ("prod-003", """{"price": 499.99}""")
)

val futures = updates.map { case (id, json) =>
  client.updateAsync("products", id, json, upsert = true)
}

Future.sequence(futures).map { results =>
  val successCount = results.count {
    case ElasticSuccess(true) => true
    case _ => false
  }
  println(s"Updated $successCount/${results.length} documents")
}

// Chained async operations
def fetchUpdateAndSave(id: String): Future[ElasticResult[Boolean]] = {
  for {
    data <- fetchFromExternalAPI(id)
    json = transformToJson(data)
    result <- client.updateAsync("products", id, json, upsert = true)
  } yield result
}

// Parallel updates with rate limiting
def updateWithRateLimit(
  updates: List[(String, String)],
  maxConcurrent: Int = 10
): Future[List[ElasticResult[Boolean]]] = {
  updates.grouped(maxConcurrent).foldLeft(Future.successful(List.empty[ElasticResult[Boolean]])) {
    case (accFuture, batch) =>
      accFuture.flatMap { acc =>
        val batchFutures = batch.map { case (id, json) =>
          client.updateAsync("products", id, json, upsert = true)
        }
        Future.sequence(batchFutures).map(acc ++ _)
      }
  }
}

// Error recovery
client.updateAsync("products", "prod-001", updateJson, upsert = true)
  .recover {
    case ex: Exception =>
      ElasticFailure(ElasticError(
        message = s"Update failed: ${ex.getMessage}",
        cause = Some(ex)
      ))
  }
  .foreach {
    case ElasticSuccess(true) => println("Success")
    case ElasticFailure(e) => println(s"Failed: ${e.message}")
  }

// Conditional async update
def updateIfExists(
  index: String,
  id: String,
  json: String
): Future[ElasticResult[Boolean]] = {
  client.existsAsync(index, id).flatMap {
    case ElasticSuccess(true) =>
      client.updateAsync(index, id, json, upsert = false)
    case ElasticSuccess(false) =>
      Future.successful(ElasticFailure(ElasticError(
        message = s"Document $id does not exist"
      )))
    case failure @ ElasticFailure(_) =>
      Future.successful(failure)
  }
}

// Streaming updates
import akka.stream.scaladsl._

def streamUpdates(
  index: String,
  source: Source[(String, String), _]
): Future[Int] = {
  source
    .mapAsync(parallelism = 10) { case (id, json) =>
      client.updateAsync(index, id, json, upsert = true)
    }
    .runFold(0) { (count, result) =>
      result match {
        case ElasticSuccess(true) => count + 1
        case _ => count
      }
    }
}
```

---

### updateAsyncAs

Asynchronously updates a Scala object with automatic JSON serialization.

**Signature:**

```scala
def updateAsyncAs[U <: AnyRef](
  entity: U,
  id: String,
  index: Option[String] = None,
  maybeType: Option[String] = None,
  upsert: Boolean = true,
  wait: Boolean = false
)(implicit
  u: ClassTag[U],
  ec: ExecutionContext,
  formats: Formats
): Future[ElasticResult[Boolean]]
```

**Parameters:**
- `entity` - The Scala object containing update data
- `id` - The document ID to update
- `index` - Optional index name
- `maybeType` - Optional type name
- `upsert` - Whether to create document if it doesn't exist (default: true)
- `wait` - If `true`, waits for a refresh to happen after update (default is `false`)
- `u` - Implicit ClassTag
- `ec` - Implicit ExecutionContext
- `formats` - Implicit JSON serialization formats

**Returns:**
- `Future[ElasticResult[Boolean]]` that completes when update finishes

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global

case class ProductUpdate(price: Double, updatedAt: String)
implicit val formats: Formats = DefaultFormats

// Basic async update
val update = ProductUpdate(899.99, "2024-01-15")
val future = client.updateAsyncAs(update, id = "prod-001")

future.onComplete {
  case Success(ElasticSuccess(true)) =>
    println("✅ Product updated")
  case Success(ElasticFailure(error)) =>
    println(s"❌ Error: ${error.message}")
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Batch async updates
val updates = List(
  ("prod-001", ProductUpdate(899.99, "2024-01-15")),
  ("prod-002", ProductUpdate(699.99, "2024-01-15")),
  ("prod-003", ProductUpdate(499.99, "2024-01-15"))
)

val futures = updates.map { case (id, update) =>
  client.updateAsyncAs(update, id, index = Some("products"))
}

Future.sequence(futures).map { results =>
  val successful = results.count {
    case ElasticSuccess(true) => true
    case _ => false
  }
  println(s"✅ Updated $successful/${results.length} documents")
}

// Non-blocking pipeline
def processAndUpdate[T <: AnyRef](
  data: List[(String, RawData)]
)(implicit ct: ClassTag[T], ec: ExecutionContext, formats: Formats): Future[Int] = {
  val processedFuture = Future {
    data.map { case (id, raw) => (id, transform(raw)) }
  }
  
  processedFuture.flatMap { processed =>
    val updateFutures = processed.map { case (id, entity) =>
      client.updateAsyncAs(entity, id, upsert = true)
    }
    
    Future.sequence(updateFutures).map { results =>
      results.count {
        case ElasticSuccess(true) => true
        case _ => false
      }
    }
  }
}

// Error recovery with retry
def updateWithRetry[T <: AnyRef](
  entity: T,
  id: String,
  maxRetries: Int = 3
)(implicit 
  ct: ClassTag[T], 
  ec: ExecutionContext, 
  formats: Formats
): Future[ElasticResult[Boolean]] = {
  
  def attempt(remaining: Int): Future[ElasticResult[Boolean]] = {
    client.updateAsyncAs(entity, id, upsert = true).flatMap {
      case success @ ElasticSuccess(true) => Future.successful(success)
      case failure if remaining > 0 =>
        Future {
          Thread.sleep(1000)
        }.flatMap(_ => attempt(remaining - 1))
      case failure => Future.successful(failure)
    }
  }
  
  attempt(maxRetries)
}

// Parallel updates with error collection
def updateAllWithErrors[T <: AnyRef](
  updates: List[(String, T)]
)(implicit 
  ct: ClassTag[T], 
  ec: ExecutionContext, 
  formats: Formats
): Future[(List[String], List[(String, ElasticError)])] = {
  
  val futures = updates.map { case (id, entity) =>
    client.updateAsyncAs(entity, id, upsert = true).map(result => (id, result))
  }
  
  Future.sequence(futures).map { results =>
    val (successes, failures) = results.partition {
      case (_, ElasticSuccess(true)) => true
      case _ => false
    }
    
    val successIds = successes.map(_._1)
    val failureDetails = failures.collect {
      case (id, ElasticFailure(error)) => (id, error)
    }
    
    (successIds, failureDetails)
  }
}

// Await result (for testing)
import scala.concurrent.Await
import scala.concurrent.duration._

val result = Await.result(
  client.updateAsyncAs(update, id = "prod-001"),
  5.seconds
)
```

---

## Implementation Requirements

### executeUpdate

```scala
private[client] def executeUpdate(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean
): ElasticResult[Boolean]
```

**Implementation Example:**

```scala
private[client] def executeUpdate(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean
): ElasticResult[Boolean] = {
  executeRestAction[UpdateRequest, UpdateResponse, Boolean](
    operation = "update",
    index = Some(index),
    retryable = false
  )(
    request = new UpdateRequest(index, id)
      .doc(source, XContentType.JSON)
      .docAsUpsert(upsert)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = req => apply().update(req, RequestOptions.DEFAULT)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
             DocWriteResponse.Result.NOOP =>
          true
        case DocWriteResponse.Result.NOT_FOUND =>
          throw new IOException(s"Document ($index#$id) not found") // if upsert is false
        case _ => false
      }
  )
}
```

---

### executeUpdateAsync

```scala
private[client] def executeUpdateAsync(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Implementation Example:**

```scala
private[client] def executeUpdateAsync(
  index: String,
  id: String,
  source: String,
  upsert: Boolean,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = {
  executeAsyncRestAction[UpdateRequest, UpdateResponse, Boolean](
    operation = "updateAsync",
    index = Some(index),
    retryable = false
  )(
    request = new UpdateRequest(index, id)
      .doc(source, XContentType.JSON)
      .docAsUpsert(upsert)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = (req, listener) => apply().updateAsync(req, RequestOptions.DEFAULT, listener)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
             DocWriteResponse.Result.NOOP =>
          true
        case DocWriteResponse.Result.NOT_FOUND =>
          throw new IOException(s"Document ($index#$id) not found") // if upsert is false
        case _ => false
      }
  )
}
```

---

## Common Patterns

### Repository Pattern with Updates

```scala
trait Repository[T <: AnyRef] {
  def update(entity: T, id: String, createIfMissing: Boolean = true)(implicit 
    ct: ClassTag[T], 
    formats: Formats,
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    client.updateAs(entity, id, upsert = createIfMissing)
  }
  
  def updateAsync(entity: T, id: String, createIfMissing: Boolean = true)(implicit 
    ct: ClassTag[T], 
    formats: Formats,
    ec: ExecutionContext,
    client: ElasticClient
  ): Future[ElasticResult[Boolean]] = {
    client.updateAsyncAs(entity, id, upsert = createIfMissing)
  }
}

case class Product(name: String, price: Double, stock: Int)

object ProductRepository extends Repository[Product] {
  implicit val formats: Formats = DefaultFormats
  
  def updatePrice(id: String, newPrice: Double)(implicit 
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    val json = s"""{"price": $newPrice}"""
    client.update("products", id, json, upsert = false)
  }
  
  def updateStock(id: String, quantity: Int)(implicit 
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    val json = s"""{"stock": $quantity}"""
    client.update("products", id, json, upsert = false)
  }
}
```

### Partial Update Pattern

```scala
// Separate update models from domain models
case class Product(name: String, price: Double, category: String, stock: Int)

case class PriceUpdate(price: Double, updatedAt: String)
case class StockUpdate(stock: Int, updatedBy: String)
case class StatusUpdate(status: String, timestamp: Long)

def updatePrice(id: String, newPrice: Double): ElasticResult[Boolean] = {
  val update = PriceUpdate(newPrice, java.time.Instant.now().toString)
  client.updateAs(update, id, index = Some("products"), upsert = false)
}

def updateStock(id: String, quantity: Int, user: String): ElasticResult[Boolean] = {
  val update = StockUpdate(quantity, user)
  client.updateAs(update, id, index = Some("products"), upsert = false)
}
```

### Optimistic Locking

```scala
case class VersionedDocument[T](data: T, version: Long)

def updateWithVersion[T <: AnyRef](
  entity: T,
  id: String,
  expectedVersion: Long
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    current <- client.get(id, indexNameFor[T])
    currentVersion = extractVersion(current)
    _ <- if (currentVersion == expectedVersion) {
      ElasticResult.success(())
    } else {
      ElasticResult.failure(s"Version mismatch: expected $expectedVersion, got $currentVersion")
    }
    updated <- client.updateAs(entity, id, upsert = false)
  } yield updated
}
```

### Conditional Update

```scala
def updateIfCondition[T <: AnyRef](
  entity: T,
  id: String,
  condition: T => Boolean
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Boolean] = {
  for {
    current <- client.getAs[T](id)
    shouldUpdate = condition(current)
    result <- if (shouldUpdate) {
      client.updateAs(entity, id, upsert = false)
    } else {
      ElasticResult.success(false)
    }
  } yield result
}

// Usage
updateIfCondition(productUpdate, "prod-001", (p: Product) => p.stock > 0)
```

---

## Performance Optimization

### Batch Updates with Disabled Refresh

```scala
def batchUpdateOptimized[T <: AnyRef](
  updates: List[(String, T)],
  indexName: String
)(implicit ct: ClassTag[T], formats: Formats): ElasticResult[Unit] = {
  for {
    // Disable refresh
    _ <- client.toggleRefresh(indexName, enable = false)
    
    // Update all documents
    _ <- updates.foldLeft(ElasticResult.success(())) { case (acc, (id, entity)) =>
      acc.flatMap(_ => client.updateAs(entity, id, Some(indexName), upsert = true).map(_ => ()))
    }
    
    // Re-enable refresh
    _ <- client.toggleRefresh(indexName, enable = true)
    
    // Manual refresh
    _ <- client.refresh(indexName)
  } yield ()
}
```

### Parallel Async Updates

```scala
def parallelUpdate[T <: AnyRef](
  updates: List[(String, T)],
  parallelism: Int = 10
)(implicit 
  ct: ClassTag[T], 
  ec: ExecutionContext, 
  formats: Formats
): Future[List[ElasticResult[Boolean]]] = {
  
  updates
    .grouped(parallelism)
    .foldLeft(Future.successful(List.empty[ElasticResult[Boolean]])) {
      case (accFuture, batch) =>
        accFuture.flatMap { acc =>
          val batchFutures = batch.map { case (id, entity) =>
            client.updateAsyncAs(entity, id, upsert = true)
          }
          Future.sequence(batchFutures).map(acc ++ _)
        }
    }
}
```

---

## Error Handling

**Document Not Found (upsert=false):**

```scala
client.update("products", "non-existent", updateJson, upsert = false) match {
  case ElasticFailure(error) if error.message.contains("not updated") =>
    println("❌ Document does not exist")
  case ElasticFailure(error) =>
    println(s"❌ Other error: ${error.message}")
}
```

**Invalid JSON:**

```scala
client.update("products", "prod-001", "{ invalid }", upsert = true) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid JSON"))
}
```

**Serialization Failure:**

```scala
case class InvalidEntity(data: java.io.InputStream) // Not serializable

client.updateAs(InvalidEntity(null), "doc-1", upsert = true) match {
  case ElasticFailure(error) =>
    println(s"Serialization failed: ${error.message}")
}
```

---

## Best Practices

**1. Use Partial Update Models**

```scala
// ✅ Good - explicit update model
case class PriceUpdate(price: Double)
client.updateAs(PriceUpdate(899.99), "prod-001")

// ❌ Avoid - full domain model for partial updates
case class Product(name: String, price: Double, category: String, stock: Int)
client.updateAs(Product("", 899.99, "", 0), "prod-001") // Overwrites other fields
```

**2. Choose Appropriate Upsert Behavior**

```scala
// ✅ Use upsert=true for idempotent operations
client.updateAs(entity, id, upsert = true)

// ✅ Use upsert=false when document must exist
client.updateAs(entity, id, upsert = false)
```

**3. Handle Document Not Found Gracefully**

```scala
def safeUpdate[T <: AnyRef](entity: T, id: String)(implicit 
  ct: ClassTag[T], 
  formats: Formats
): ElasticResult[Boolean] = {
  client.updateAs(entity, id, upsert = false).recoverWith {
    case error if error.message.contains("not updated") =>
    // ...
  }
}
```

---

[Back to index](README.md) | [Next: Delete Documents](delete.md)