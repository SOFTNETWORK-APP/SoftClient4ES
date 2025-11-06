[Back to index](README.md)

# FLUSH API

## Overview

The **FlushApi** trait provides functionality to flush Elasticsearch indices, ensuring all in-memory operations are written to disk and creating a new Lucene commit point.

**Features:**
- Force flush operations to disk
- Wait for flush completion
- Index name validation
- Comprehensive error handling and logging

**Use Cases:**
- Ensure data durability before backup/snapshot
- Prepare for cluster maintenance
- Guarantee data persistence after critical operations
- Testing and development scenarios

---

## Understanding Flush

**What is Flush?**
- Writes all in-memory index data (translog) to disk
- Creates a new Lucene commit point
- Clears the translog after successful write
- Makes data recoverable after unexpected shutdown

**Flush vs Refresh:**
- **Refresh:** Makes recent changes searchable (in-memory operation)
- **Flush:** Persists data to disk (I/O operation)

**When to Use Flush:**
- Before taking snapshots
- Before cluster shutdown/restart
- After critical bulk operations
- When maximum durability is required

---

## Public Methods

### flush

Flushes an index to ensure all operations are written to disk.

**Signature:**

```scala
def flush(
  index: String,
  force: Boolean = true,
  wait: Boolean = true
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to flush
- `force` - Whether to force the flush even if not necessary (default: `true`)
- `wait` - Whether to wait for flush completion (default: `true`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if flushed successfully
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation

**Behavior:**
- **force = true:** Always performs flush regardless of whether changes exist
- **force = false:** Only flushes if there are uncommitted changes
- **wait = true:** Blocks until flush completes
- **wait = false:** Returns immediately (asynchronous flush)

**Examples:**

```scala
// Basic flush
client.flush("products") match {
  case ElasticSuccess(true) => println("Index flushed")
  case ElasticSuccess(false) => println("Flush not needed")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Force flush with wait
client.flush("critical-data", force = true, wait = true)

// Opportunistic flush (only if needed)
client.flush("logs", force = false, wait = true)

// Asynchronous flush
client.flush("temp-index", force = true, wait = false)

// Flush after bulk operations
for {
  _ <- client.bulkIndex(largeDataset)
  _ <- client.refresh("products")
  _ <- client.flush("products")
} yield "Data persisted to disk"

// Pre-backup flush
def prepareForBackup(index: String): ElasticResult[Unit] = {
  for {
    _ <- client.flush(index, force = true, wait = true)
    _ = println(s"✅ Index $index ready for backup")
  } yield ()
}

// Flush multiple indices
def flushAll(indices: List[String]): ElasticResult[List[Boolean]] = {
  ElasticResult.sequence(
    indices.map(index => client.flush(index))
  )
}

val indices = List("users", "products", "orders")
flushAll(indices) match {
  case ElasticSuccess(results) =>
    results.zip(indices).foreach { case (success, index) =>
      if (success) println(s"✅ $index flushed")
      else println(s"⚠️ $index not flushed")
    }
  case ElasticFailure(e) =>
    println(s"Flush failed: ${e.message}")
}

// Conditional flush
def flushIfNeeded(index: String, threshold: Long): ElasticResult[Boolean] = {
  // Check translog size (implementation depends on stats API)
  getTranslogSize(index).flatMap { size =>
    if (size > threshold) {
      client.flush(index, force = true, wait = true)
    } else {
      ElasticResult.success(false)
    }
  }
}

// Error handling
client.flush("invalid index name") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid index"))
}
```

---

## Implementation Requirements

### executeFlush

```scala
private[client] def executeFlush(
  index: String,
  force: Boolean,
  wait: Boolean
): ElasticResult[Boolean]
```

**Implementation Example:**

```scala
private[client] def executeFlush(
  index: String,
  force: Boolean,
  wait: Boolean
): ElasticResult[Boolean] = {
  executeRestAction[FlushResponse, Boolean](
    operation = "flush",
    index = Some(index)
  )(
    action = {
      val request = new FlushRequest(index)
        .force(force)
        .waitIfOngoing(wait)
      client.indices().flush(request, RequestOptions.DEFAULT)
    }
  )(
    transformer = resp => {
      resp.getFailedShards == 0
    }
  )
}
```

---

## Common Workflows

### Pre-Backup Flush

```scala
def prepareIndicesForBackup(indices: List[String]): ElasticResult[Unit] = {
  for {
    // Flush all indices
    _ <- indices.foldLeft(ElasticResult.success(())) { (acc, index) =>
      acc.flatMap(_ => client.flush(index, force = true, wait = true).map(_ => ()))
    }
    _ = println("✅ All indices flushed and ready for backup")
  } yield ()
}

// Usage
prepareIndicesForBackup(List("users", "products", "orders"))
```

### Bulk Operation with Flush

```scala
def bulkIndexWithPersistence[T](
  index: String,
  documents: Seq[T]
): ElasticResult[Unit] = {
  for {
    // Disable refresh for performance
    _ <- client.toggleRefresh(index, enable = false)
    
    // Bulk index
    _ <- client.bulkIndex(documents)
    
    // Re-enable refresh
    _ <- client.toggleRefresh(index, enable = true)
    
    // Make searchable
    _ <- client.refresh(index)
    
    // Persist to disk
    _ <- client.flush(index, force = true, wait = true)
  } yield ()
}
```

### Scheduled Maintenance Flush

```scala
import java.util.concurrent.{Executors, TimeUnit}

def schedulePeriodicFlush(
  index: String,
  intervalMinutes: Int
): Unit = {
  val scheduler = Executors.newScheduledThreadPool(1)
  
  scheduler.scheduleAtFixedRate(
    () => {
      client.flush(index, force = false, wait = true) match {
        case ElasticSuccess(true) =>
          println(s"✅ Periodic flush completed for $index")
        case ElasticSuccess(false) =>
          println(s"⚠️ No flush needed for $index")
        case ElasticFailure(e) =>
          println(s"❌ Periodic flush failed for $index: ${e.message}")
      }
    },
    intervalMinutes,
    intervalMinutes,
    TimeUnit.MINUTES
  )
}

// Flush every 30 minutes
schedulePeriodicFlush("logs", 30)
```

### Critical Operation Pattern

```scala
def performCriticalOperation[T](
  index: String,
  operation: => ElasticResult[T]
): ElasticResult[T] = {
  for {
    // Perform operation
    result <- operation
    
    // Ensure persistence
    _ <- client.flush(index, force = true, wait = true)
    
    // Verify
    _ = println(s"✅ Critical operation completed and persisted for $index")
  } yield result
}

// Usage
performCriticalOperation("financial-transactions", {
  client.index("financial-transactions", criticalTransaction)
})
```

### Cluster Shutdown Preparation

```scala
def prepareForShutdown(indices: List[String]): ElasticResult[Unit] = {
  for {
    // Flush all indices
    flushResults <- ElasticResult.sequence(
      indices.map(index => client.flush(index, force = true, wait = true))
    )
    
    // Verify all succeeded
    _ <- if (flushResults.forall(_ == true)) {
      println("✅ All indices flushed successfully")
      ElasticResult.success(())
    } else {
      ElasticResult.failure("Some indices failed to flush")
    }
  } yield ()
}
```

---

## Performance Considerations

### Flush Impact

**I/O Impact:**
- Flush is an I/O-intensive operation
- Can temporarily impact cluster performance
- Avoid frequent flushes in high-throughput scenarios

**Best Practices:**

```scala
// ❌ Bad - too frequent
documents.foreach { doc =>
  client.index("my-index", doc)
  client.flush("my-index") // Very expensive!
}

// ✅ Good - batch and flush once
documents.foreach { doc =>
  client.index("my-index", doc)
}
client.flush("my-index")

// ✅ Better - use bulk operations
client.bulkIndex(documents)
client.flush("my-index")
```

### Translog Configuration

```scala
// Configure translog durability in index settings
val durableSettings = """
{
  "index": {
    "translog.durability": "request",
    "translog.sync_interval": "5s"
  }
}
"""

// Less durable but faster
val asyncSettings = """
{
  "index": {
    "translog.durability": "async",
    "translog.sync_interval": "30s"
  }
}
"""
```

### Selective Flushing

```scala
def flushCriticalIndicesOnly(
  allIndices: List[String],
  criticalIndices: Set[String]
): ElasticResult[Unit] = {
  val indicesToFlush = allIndices.filter(criticalIndices.contains)
  
  for {
    _ <- indicesToFlush.foldLeft(ElasticResult.success(())) { (acc, index) =>
      acc.flatMap(_ => client.flush(index, force = true, wait = true).map(_ => ()))
    }
  } yield ()
}

val critical = Set("financial-transactions", "user-accounts")
flushCriticalIndicesOnly(allIndices, critical)
```

---

## Error Handling

**Invalid Index Name:**

```scala
client.flush("INVALID INDEX") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.operation.contains("refresh")) // Note: logs as "refresh"
}
```

**Index Not Found:**

```scala
client.flush("non-existent-index") match {
  case ElasticFailure(error) =>
    println(s"Flush failed: ${error.message}")
}
```

**Partial Failure:**

```scala
client.flush("my-index") match {
  case ElasticSuccess(false) =>
    println("⚠️ Flush completed but some shards failed")
  case ElasticSuccess(true) =>
    println("✅ All shards flushed successfully")
  case ElasticFailure(error) =>
    println(s"❌ Flush failed: ${error.message}")
}
```

---

## Comparison with Related Operations

### Flush vs Refresh vs Fsync

| Operation | Purpose | Scope | Cost |
|-----------|---------|-------|------|
| **Refresh** | Make changes searchable | In-memory | Low |
| **Flush** | Persist to disk | Disk I/O | Medium |
| **Fsync** | OS-level sync | Disk I/O | High |

### When to Use Each

```scala
// After bulk indexing - make searchable
client.bulkIndex(documents)
client.refresh("my-index")

// Before backup - ensure durability
client.flush("my-index", force = true, wait = true)

// Critical transaction - maximum durability
client.index("financial-transactions", transaction)
client.flush("financial-transactions", force = true, wait = true)
```

---

## Best Practices

**1. Flush After Bulk Operations**

```scala
def safeBulkIndex[T](index: String, docs: Seq[T]): ElasticResult[Unit] = {
  for {
    _ <- client.bulkIndex(docs)
    _ <- client.refresh(index)
    _ <- client.flush(index)
  } yield ()
}
```

**2. Use force = false for Regular Maintenance**

```scala
// Only flush if there are uncommitted changes
def maintenanceFlush(index: String): ElasticResult[Boolean] = {
  client.flush(index, force = false, wait = true)
}
```

**3. Always Wait for Critical Operations**

```scala
// For critical data, always wait
def persistCriticalData[T](index: String, data: T): ElasticResult[Unit] = {
  for {
    _ <- client.index(index, data)
    _ <- client.flush(index, force = true, wait = true)
  } yield ()
}
```

**4. Batch Flushes for Multiple Indices**

```scala
def batchFlush(indices: List[String]): ElasticResult[Unit] = {
  indices.foldLeft(ElasticResult.success(())) { (acc, index) =>
    acc.flatMap(_ => 
      client.flush(index, force = false, wait = true).map(_ => ())
    )
  }
}
```

**5. Monitor Flush Performance**

```scala
def timedFlush(index: String): ElasticResult[(Boolean, Long)] = {
  val start = System.currentTimeMillis()
  client.flush(index).map { result =>
    val duration = System.currentTimeMillis() - start
    println(s"Flush took ${duration}ms")
    (result, duration)
  }
}
```

---

## Testing Scenarios

### Test Flush Functionality

```scala
def testFlush(): Unit = {
  val testIndex = "test-flush-index"
  
  for {
    // Create and populate
    _ <- client.createIndex(testIndex)
    _ <- client.index(testIndex, testDocument)
    
    // Flush
    flushed <- client.flush(testIndex, force = true, wait = true)
    _ = assert(flushed, "Flush should succeed")
    
    // Cleanup
    _ <- client.deleteIndex(testIndex)
  } yield ()
}
```

### Test Durability

```scala
def testDurability(): Unit = {
  val testIndex = "test-durability"
  
  for {
    _ <- client.createIndex(testIndex)
    _ <- client.index(testIndex, testDoc)
    _ <- client.flush(testIndex, force = true, wait = true)
    
    // Simulate restart by closing and reopening
    _ <- client.closeIndex(testIndex)
    _ <- client.openIndex(testIndex)
    
    // Verify data still exists
    result <- client.search(testIndex, matchAllQuery)
    _ = assert(result.nonEmpty, "Data should persist after flush")
    
    _ <- client.deleteIndex(testIndex)
  } yield ()
}
```

---

[Back to index](README.md) | [Next: Refresh Index](refresh.md)