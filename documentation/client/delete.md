[Back to index](README.md)

# DELETE API

## Overview

The **DeleteApi** trait provides functionality to delete documents from Elasticsearch indices, supporting both synchronous and asynchronous operations with comprehensive validation and error handling.

**Features:**
- Synchronous and asynchronous document deletion
- Wait for a refresh to happen after deletion to happen if required
- Index name validation
- Comprehensive error handling and logging
- Safe deletion with existence checking

**Dependencies:**
- Requires `RefreshApi` for automatic refresh after deletion

---

## Understanding Delete Operations

**Delete Behavior:**
- Deletes a document by its ID from a specific index
- Returns `true` if document was deleted
- Returns `false` if document doesn't exist (not an error)
- Wait for a refresh to happen after deletion if required

**Idempotency:**
- Delete operations are idempotent
- Deleting a non-existent document returns success (false) but doesn't fail
- Safe to retry delete operations

---

## Public Methods

### delete

Deletes a document from an Elasticsearch index by ID.

**Signature:**

```scala
def delete(id: String, index: String, wait: Boolean): ElasticResult[Boolean]
```

**Parameters:**
- `id` - The document ID to delete
- `index` - The index name containing the document
- `wait` - If `true`, waits for a refresh to happen after deletion (default is `false`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if document was deleted
- `ElasticSuccess[Boolean]` with `false` if document doesn't exist
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation

**Behavior:**
- Logs success/failure with appropriate emoji indicators
- Waits for a refresh to happen after successful deletion (disabled by default)
- Returns success even if document doesn't exist (idempotent)

**Examples:**

```scala
// Basic document deletion
client.delete("prod-001", "products") match {
  case ElasticSuccess(true) => println("✅ Document deleted")
  case ElasticSuccess(false) => println("⚠️ Document not found")
  case ElasticFailure(e) => println(s"❌ Error: ${e.message}")
}

// Delete with existence check
def deleteIfExists(id: String, index: String): ElasticResult[Boolean] = {
  for {
    exists <- client.exists(id, index)
    result <- if (exists) {
      client.delete(id, index)
    } else {
      ElasticResult.success(false)
    }
  } yield result
}

// Delete multiple documents
val idsToDelete = List("prod-001", "prod-002", "prod-003")

idsToDelete.foreach { id =>
  client.delete(id, "products") match {
    case ElasticSuccess(true) => println(s"✅ Deleted: $id")
    case ElasticSuccess(false) => println(s"⚠️ Not found: $id")
    case ElasticFailure(e) => println(s"❌ Failed: $id - ${e.message}")
  }
}

// Batch deletion with result tracking
def deleteMany(ids: List[String], index: String): (Int, Int, Int) = {
  val results = ids.map(id => client.delete(id, index))
  
  val deleted = results.count {
    case ElasticSuccess(true) => true
    case _ => false
  }
  
  val notFound = results.count {
    case ElasticSuccess(false) => true
    case _ => false
  }
  
  val failed = results.count {
    case ElasticFailure(_) => true
    case _ => false
  }
  
  (deleted, notFound, failed)
}

val (deleted, notFound, failed) = deleteMany(idsToDelete, "products")
println(s"Deleted: $deleted, Not found: $notFound, Failed: $failed")

// Conditional deletion
def deleteOldDocuments(index: String, cutoffDate: String): ElasticResult[List[Boolean]] = {
  for {
    oldDocs <- client.searchByDateRange(index, "created_at", None, Some(cutoffDate))
    results <- ElasticResult.sequence(
      oldDocs.map(doc => client.delete(doc.id, index))
    )
  } yield results
}

// Delete with retry logic
def deleteWithRetry(
  id: String,
  index: String,
  maxRetries: Int = 3
): ElasticResult[Boolean] = {
  def attempt(remaining: Int): ElasticResult[Boolean] = {
    client.delete(id, index) match {
      case success @ ElasticSuccess(_) => success
      case failure if remaining > 0 =>
        Thread.sleep(1000)
        attempt(remaining - 1)
      case failure => failure
    }
  }
  attempt(maxRetries)
}

// Safe deletion with validation
def safeDelete(id: String, index: String): ElasticResult[Boolean] = {
  for {
    exists <- client.exists(id, index)
    _ <- if (!exists) {
      ElasticResult.failure(s"Document $id does not exist in index $index")
    } else {
      ElasticResult.success(())
    }
    deleted <- client.delete(id, index)
  } yield deleted
}

// Delete with audit trail
def deleteWithAudit(
  id: String,
  index: String,
  userId: String
): ElasticResult[Boolean] = {
  for {
    deleted <- client.delete(id, index)
    _ <- if (deleted) {
      logAuditEvent(s"User $userId deleted document $id from $index")
    } else {
      ElasticResult.success(())
    }
  } yield deleted
}

// Error handling
client.delete("prod-001", "INVALID INDEX") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid index"))
}

// Monadic composition
def archiveAndDelete(id: String, index: String): ElasticResult[Boolean] = {
  for {
    document <- client.get(id, index)
    _ <- client.index("archive", id, document)
    deleted <- client.delete(id, index)
  } yield deleted
}
```

---

### deleteAsync

Asynchronously deletes a document from an Elasticsearch index.

**Signature:**

```scala
def deleteAsync(
  id: String,
  index: String,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Parameters:**
- `id` - The document ID to delete
- `index` - The index name containing the document
- `wait` - If `true`, waits for a refresh to happen after deletion (default is `false`)
- `ec` - Implicit ExecutionContext for async execution

**Returns:**
- `Future[ElasticResult[Boolean]]` that completes when deletion finishes
- `true` if document was deleted, `false` if not found

**Validation:**
- Index name validation performed synchronously before async execution

**Examples:**

```scala
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

// Basic async deletion
client.deleteAsync("prod-001", "products").onComplete {
  case Success(ElasticSuccess(true)) =>
    println("✅ Document deleted")
  case Success(ElasticSuccess(false)) =>
    println("⚠️ Document not found")
  case Success(ElasticFailure(error)) =>
    println(s"❌ Error: ${error.message}")
  case Failure(ex) =>
    println(s"❌ Future failed: ${ex.getMessage}")
}

// Batch async deletion
val idsToDelete = List("prod-001", "prod-002", "prod-003")

val deleteFutures = idsToDelete.map { id =>
  client.deleteAsync(id, "products")
}

Future.sequence(deleteFutures).map { results =>
  val deletedCount = results.count {
    case ElasticSuccess(true) => true
    case _ => false
  }
  val notFoundCount = results.count {
    case ElasticSuccess(false) => true
    case _ => false
  }
  println(s"Deleted: $deletedCount, Not found: $notFoundCount")
}

// Chained async operations
def fetchAndDelete(id: String, index: String): Future[ElasticResult[Boolean]] = {
  for {
    exists <- client.existsAsync(id, index)
    result <- if (exists) {
      client.deleteAsync(id, index)
    } else {
      Future.successful(ElasticResult.success(false))
    }
  } yield result
}

// Parallel deletion with rate limiting
def deleteWithRateLimit(
  ids: List[String],
  index: String,
  maxConcurrent: Int = 10
): Future[List[ElasticResult[Boolean]]] = {
  ids.grouped(maxConcurrent).foldLeft(Future.successful(List.empty[ElasticResult[Boolean]])) {
    case (accFuture, batch) =>
      accFuture.flatMap { acc =>
        val batchFutures = batch.map { id =>
          client.deleteAsync(id, index)
        }
        Future.sequence(batchFutures).map(acc ++ _)
      }
  }
}

// Archive before delete
def archiveAndDeleteAsync(
  id: String,
  sourceIndex: String,
  archiveIndex: String
): Future[ElasticResult[Boolean]] = {
  for {
    doc <- client.getAsync(id, sourceIndex)
    _ <- client.indexAsync(archiveIndex, id, doc)
    deleted <- client.deleteAsync(id, sourceIndex)
  } yield deleted
}

// Error recovery
client.deleteAsync("prod-001", "products")
  .recover {
    case ex: Exception =>
      ElasticFailure(ElasticError(
        message = s"Delete failed: ${ex.getMessage}",
        cause = Some(ex)
      ))
  }
  .foreach {
    case ElasticSuccess(true) => println("Success")
    case ElasticSuccess(false) => println("Not found")
    case ElasticFailure(e) => println(s"Failed: ${e.message}")
  }

// Conditional async deletion
def deleteIfConditionAsync(
  id: String,
  index: String,
  condition: String => Future[Boolean]
): Future[ElasticResult[Boolean]] = {
  condition(id).flatMap { shouldDelete =>
    if (shouldDelete) {
      client.deleteAsync(id, index)
    } else {
      Future.successful(ElasticResult.success(false))
    }
  }
}

// Streaming deletion
import akka.stream.scaladsl._

def streamDelete(
  index: String,
  source: Source[String, _]
): Future[Int] = {
  source
    .mapAsync(parallelism = 10) { id =>
      client.deleteAsync(id, index)
    }
    .runFold(0) { (count, result) =>
      result match {
        case ElasticSuccess(true) => count + 1
        case _ => count
      }
    }
}

// Retry with exponential backoff
def deleteWithRetryAsync(
  id: String,
  index: String,
  maxRetries: Int = 3
): Future[ElasticResult[Boolean]] = {
  
  def attempt(remaining: Int, delay: Long = 1000): Future[ElasticResult[Boolean]] = {
    client.deleteAsync(id, index).flatMap {
      case success @ ElasticSuccess(_) => Future.successful(success)
      case failure if remaining > 0 =>
        Future {
          Thread.sleep(delay)
        }.flatMap(_ => attempt(remaining - 1, delay * 2))
      case failure => Future.successful(failure)
    }
  }
  
  attempt(maxRetries)
}

// Collect deletion results with errors
def deleteAllWithErrors(
  ids: List[String],
  index: String
): Future[(List[String], List[(String, ElasticError)])] = {
  
  val futures = ids.map { id =>
    client.deleteAsync(id, index).map(result => (id, result))
  }
  
  Future.sequence(futures).map { results =>
    val (successes, failures) = results.partition {
      case (_, ElasticSuccess(true)) => true
      case _ => false
    }
    
    val deletedIds = successes.map(_._1)
    val failureDetails = failures.collect {
      case (id, ElasticFailure(error)) => (id, error)
    }
    
    (deletedIds, failureDetails)
  }
}

// Await result (for testing)
import scala.concurrent.Await
import scala.concurrent.duration._

val result = Await.result(
  client.deleteAsync("prod-001", "products"),
  5.seconds
)
```

---

## Implementation Requirements

### executeDelete

```scala
private[client] def executeDelete(
  index: String,
  id: String,
  wait: boolean
): ElasticResult[Boolean]
```

**Implementation Example:**

```scala
private[client] def executeDelete(
  index: String,
  id: String,
  wait: Boolean
): ElasticResult[Boolean] = {
  executeRestAction[DeleteRequest, DeleteResponse, Boolean](
    operation = "delete",
    index = Some(index),
    retryable = false
  )(
    request = new DeleteRequest(index, id)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = req => apply().delete(req, RequestOptions.DEFAULT)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.DELETED | DocWriteResponse.Result.NOOP => true
        case _                                                              => false
      }
  )
}
```

---

### executeDeleteAsync

```scala
private[client] def executeDeleteAsync(
  index: String,
  id: String,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]]
```

**Implementation Example:**

```scala
private[client] def executeDeleteAsync(
  index: String,
  id: String,
  wait: Boolean
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = {
  executeAsyncRestAction[DeleteRequest, DeleteResponse, Boolean](
    operation = "deleteAsync",
    index = Some(index),
    retryable = false
  )(
    request = new DeleteRequest(index, id)
      .setRefreshPolicy(
        if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
      )
  )(
    executor = (req, listener) => apply().deleteAsync(req, RequestOptions.DEFAULT, listener)
  )(
    transformer = resp =>
      resp.getResult match {
        case DocWriteResponse.Result.DELETED | DocWriteResponse.Result.NOOP => true
        case _                                                              => false
      }
  )
}
```

---

## Common Patterns

### Repository Pattern with Delete

```scala
trait Repository[T <: AnyRef] {
  def delete(id: String)(implicit 
    ct: ClassTag[T],
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    val indexName = ct.runtimeClass.getSimpleName.toLowerCase
    client.delete(id, indexName)
  }
  
  def deleteAsync(id: String)(implicit 
    ct: ClassTag[T],
    ec: ExecutionContext,
    client: ElasticClient
  ): Future[ElasticResult[Boolean]] = {
    val indexName = ct.runtimeClass.getSimpleName.toLowerCase
    client.deleteAsync(id, indexName)
  }
}

case class Product(name: String, price: Double)

object ProductRepository extends Repository[Product] {
  def deleteProduct(id: String)(implicit 
    client: ElasticClient
  ): ElasticResult[Boolean] = {
    delete(id)
  }
}
```

### Soft Delete Pattern

```scala
case class SoftDeletable(deleted: Boolean, deletedAt: Option[String])

def softDelete(id: String, index: String): ElasticResult[Boolean] = {
  val update = SoftDeletable(
    deleted = true,
    deletedAt = Some(java.time.Instant.now().toString)
  )
  client.updateAs(update, id, Some(index), upsert = false)
}

def hardDelete(id: String, index: String): ElasticResult[Boolean] = {
  client.delete(id, index)
}

// Query only non-deleted documents
def searchActive(index: String, query: String): ElasticResult[List[Document]] = {
  val searchQuery = s"""
  {
    "query": {
      "bool": {
        "must": [
          {"match": {"_all": "$query"}},
          {"term": {"deleted": false}}
        ]
      }
    }
  }
  """
  client.search(index, searchQuery)
}
```

### Archive Before Delete

```scala
def archiveAndDelete(
  id: String,
  sourceIndex: String,
  archiveIndex: String
): ElasticResult[Boolean] = {
  for {
    // Get document
    document <- client.get(id, sourceIndex)
    
    // Archive it
    _ <- client.index(archiveIndex, id, document)
    
    // Delete from source
    deleted <- client.delete(id, sourceIndex)
  } yield deleted
}

// Async version
def archiveAndDeleteAsync(
  id: String,
  sourceIndex: String,
  archiveIndex: String
)(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = {
  for {
    document <- client.getAsync(id, sourceIndex)
    _ <- client.indexAsync(archiveIndex, id, document)
    deleted <- client.deleteAsync(id, sourceIndex)
  } yield deleted
}
```

### Bulk Delete by Query

```scala
def deleteByQuery(
  index: String,
  query: String
): ElasticResult[Int] = {
  for {
    // Search for matching documents
    results <- client.search(index, query)
    ids = results.map(_.id)
    
    // Delete each document
    deleteResults <- ElasticResult.sequence(
      ids.map(id => client.delete(id, index))
    )
    
    // Count successful deletions
    deletedCount = deleteResults.count(_ == true)
  } yield deletedCount
}

// Example: Delete all products with price > 1000
val expensiveProductsQuery = """
{
  "query": {
    "range": {
      "price": {
        "gt": 1000
      }
    }
  }
}
"""
deleteByQuery("products", expensiveProductsQuery)
```

### Cascading Delete

```scala
def cascadeDelete(
  parentId: String,
  parentIndex: String,
  childIndex: String,
  parentField: String
): ElasticResult[Int] = {
  for {
    // Find all child documents
    childQuery = s"""
    {
      "query": {
        "term": {
          "$parentField": "$parentId"
        }
      }
    }
    """
    children <- client.search(childIndex, childQuery)
    
    // Delete all children
    childResults <- ElasticResult.sequence(
      children.map(child => client.delete(child.id, childIndex))
    )
    
    // Delete parent
    _ <- client.delete(parentId, parentIndex)
    
    // Count total deletions
    totalDeleted = childResults.count(_ == true) + 1
  } yield totalDeleted
}

// Example: Delete order and all its items
cascadeDelete("order-123", "orders", "order-items", "order_id")
```

---

## Performance Optimization

### Batch Delete with Disabled Refresh

```scala
def batchDeleteOptimized(
  ids: List[String],
  index: String
): ElasticResult[Int] = {
  for {
    // Disable refresh
    _ <- client.toggleRefresh(index, enable = false)
    
    // Delete all documents
    results <- ElasticResult.sequence(
      ids.map(id => client.delete(id, index))
    )
    
    // Re-enable refresh
    _ <- client.toggleRefresh(index, enable = true)
    
    // Manual refresh
    _ <- client.refresh(index)
    
    // Count deletions
    deletedCount = results.count(_ == true)
  } yield deletedCount
}
```

### Parallel Async Delete

```scala
def parallelDelete(
  ids: List[String],
  index: String,
  parallelism: Int = 10
)(implicit ec: ExecutionContext): Future[Int] = {
  
  ids
    .grouped(parallelism)
    .foldLeft(Future.successful(0)) { case (accFuture, batch) =>
      accFuture.flatMap { acc =>
        val batchFutures = batch.map { id =>
          client.deleteAsync(id, index)
        }
        
        Future.sequence(batchFutures).map { results =>
          val batchCount = results.count {
            case ElasticSuccess(true) => true
            case _ => false
          }
          acc + batchCount
        }
      }
    }
}
```

---

## Error Handling

**Invalid Index Name:**

```scala
client.delete("doc-001", "INVALID INDEX") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid index"))
    assert(error.operation.contains("delete"))
}
```

**Document Not Found (Not an Error):**

```scala
client.delete("non-existent-id", "products") match {
  case ElasticSuccess(false) =>
    println("⚠️ Document not found, but operation succeeded")
  case ElasticSuccess(true) =>
    println("✅ Document deleted")
  case ElasticFailure(error) =>
    println(s"❌ Actual error: ${error.message}")
}
```

**Async Failure Handling:**

```scala
client.deleteAsync("doc-001", "products")
  .recover {
    case ex: Exception =>
      ElasticFailure(ElasticError(
        message = s"Async delete failed: ${ex.getMessage}",
        operation = Some("deleteAsync"),
        cause = Some(ex)
      ))
  }
  .foreach {
    case ElasticSuccess(true) => println("Deleted")
    case ElasticSuccess(false) => println("Not found")
    case ElasticFailure(e) => println(s"Error: ${e.message}")
  }
```

---

## Best Practices

**1. Check Existence Before Delete (Optional)**

```scala
// ✅ Direct delete (idempotent)
client.delete("prod-001", "products")

// ✅ Check existence first (if you need to know)
for {
  exists <- client.exists("prod-001", "products")
  deleted <- if (exists) {
    client.delete("prod-001", "products")
  } else {
    ElasticResult.success(false)
  }
} yield deleted
```

**2. Use Async for Batch Operations**

```scala
// ✅ Good - non-blocking batch delete
val futures = ids.map(id => client.deleteAsync(id, "products"))
Future.sequence(futures)

// ❌ Avoid - blocking batch delete
ids.foreach(id => client.delete(id, "products"))
```

**3. Archive Important Data Before Deletion**

```scala
// ✅ Good - archive before delete
def safeDelete(id: String, index: String): ElasticResult[Boolean] = {
  for {
    doc <- client.get(id, index)
    _ <- client.index("archive", id, doc)
    deleted <- client.delete(id, index)
  } yield deleted
}

// ❌ Risky - direct delete without backup
client.delete(id, index)
```

**4. Handle Deletion Results Appropriately**

```scala
// ✅ Good - distinguish between deleted and not found
client.delete("prod-001", "products") match {
  case ElasticSuccess(true) => 
    println("Document was deleted")
  case ElasticSuccess(false) => 
    println("Document didn't exist")
  case ElasticFailure(e) => 
    println(s"Error occurred: ${e.message}")
}

// ❌ Avoid - treating not found as error
client.delete("prod-001", "products") match {
  case ElasticSuccess(true) => println("Success")
  case _ => println("Failed") // Too broad
}
```

**5. Use Soft Delete for Recoverable Data**

```scala
// ✅ Good - soft delete for user data
def softDeleteUser(id: String): ElasticResult[Boolean] = {
  val update = """{"deleted": true, "deleted_at": "2024-01-15"}"""
  client.update("users", id, update, upsert = false)
}

// ✅ Good - hard delete for temporary data
def hardDeleteSession(id: String): ElasticResult[Boolean] = {
  client.delete(id, "sessions")
}
```

---

## Comparison with Related Operations

### Delete vs Update (Soft Delete)

| Operation       | Data Retained   | Recoverable  | Performance  | Use Case             |
|-----------------|-----------------|--------------|--------------|----------------------|
| **Hard Delete** | No              | No           | Fast         | Temporary data, logs |
| **Soft Delete** | Yes             | Yes          | Slower       | User data, orders    |

```scala
// Hard delete
client.delete("doc-001", "products")

// Soft delete
client.updateAs(SoftDelete(deleted = true), "doc-001", Some("products"))
```

---

## Testing Scenarios

### Test Delete Functionality

```scala
def testDelete(): Unit = {
  val testIndex = "test-delete-index"
  val testId = "test-doc-001"
  
  for {
    // Create document
    _ <- client.createIndex(testIndex)
    _ <- client.index(testIndex, testId, """{"name": "test"}""")
    
    // Verify exists
    exists1 <- client.exists(testId, testIndex)
    _ = assert(exists1, "Document should exist before delete")
    
    // Delete
    deleted <- client.delete(testId, testIndex)
    _ = assert(deleted, "Delete should return true")
    
    // Verify deleted
    exists2 <- client.exists(testId, testIndex)
    _ = assert(!exists2, "Document should not exist after delete")
    
    // Delete again (idempotent)
    deleted2 <- client.delete(testId, testIndex)
    _ = assert(!deleted2, "Second delete should return false")
    
    // Cleanup
    _ <- client.deleteIndex(testIndex)
  } yield ()
}
```

### Test Async Delete

```scala
def testDeleteAsync()(implicit ec: ExecutionContext): Future[Unit] = {
  val testIndex = "test-async-delete"
  val testId = "test-doc-001"
  
  for {
    _ <- client.createIndexAsync(testIndex)
    _ <- client.indexAsync(testIndex, testId, """{"name": "test"}""")
    
    result <- client.deleteAsync(testId, testIndex)
    _ = result match {
      case ElasticSuccess(true) => println("✅ Async delete successful")
      case _ => throw new Exception("Async delete failed")
    }
    
    _ <- client.deleteIndexAsync(testIndex)
  } yield ()
}
```

---

[Back to index](README.md) | [Next: Bulk Operations](bulk.md)