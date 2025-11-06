[Back to index](README.md)

# BULK API

## Overview

The **BulkApi** trait provides high-performance bulk operations for Elasticsearch using Akka Streams, supporting indexing, updating, and deleting large volumes of documents with advanced features like automatic retry, progress tracking, and detailed result reporting.

**Features:**
- **High-performance streaming** with Akka Streams
- **Automatic retry** with exponential backoff
- **Parallel processing** with configurable balance
- **Real-time progress tracking** and metrics
- **Detailed success/failure reporting**
- **Automatic index refresh management**
- **Date-based index suffixing**
- **Upsert and delete operations**
- **Configurable batch sizes**
- **Event callbacks** for monitoring

**Dependencies:**
- Requires `RefreshApi` for index refresh operations
- Requires `SettingsApi` for index settings management
- Requires `IndexApi` for individual document operations (retry)
- Requires Akka Streams for reactive processing

---

## Core Concepts

### Bulk Operations Flow

```scala
// Data flow pipeline
Iterator[D]
  -> Transform to JSON
  -> Create BulkItem
  -> Apply settings (refresh, replicas)
  -> Group into batches
  -> Execute bulk requests (parallel)
  -> Extract results
  -> Retry failures (automatic)
  -> Return Either[Failed, Success]
```

### Operation Types

| Operation  | Action         | Behavior                                  |
|------------|----------------|-------------------------------------------|
| **INDEX**  | Insert/Replace | Creates or replaces entire document       |
| **UPDATE** | Upsert         | Updates existing or creates new (partial) |
| **DELETE** | Remove         | Deletes document by ID                    |

### Result Types

```scala
// Success result
Right(SuccessfulDocument(id = "doc-001", index = "products"))

// Failure result
Left(FailedDocument(
  id = "doc-001",
  index = "products",
  document = """{"name": "Product"}""",
  error = "version_conflict_engine_exception",
  retryable = true
))
```

---

## Configuration

### BulkOptions

```scala
case class BulkOptions(
  defaultIndex: String,               // Base index name
  maxBulkSize: Int = 1000,            // Documents per batch
  balance: Int = 4,                   // Parallel workers
  disableRefresh: Boolean = false,    // Disable auto-refresh
  retryOnFailure: Boolean = true,     // Enable auto-retry
  maxRetries: Int = 3,                // Max retry attempts
  retryDelay: FiniteDuration = 1.second,     // Initial retry delay
  retryBackoffMultiplier: Double = 2.0,      // Backoff multiplier
  logEvery: Int = 10                  // Log progress every N batches
)

// Usage
implicit val bulkOptions = BulkOptions(
  defaultIndex =  "products",
  maxBulkSize = 5000,
  balance = 8,
  retryOnFailure = true
)
```

### BulkCallbacks

```scala
case class BulkCallbacks(
  onSuccess: (String, String) => Unit = (_, _) => (),
  onFailure: FailedDocument => Unit = _ => (),
  onComplete: BulkResult => Unit = _ => (),
  onBatchComplete: (Int, BulkMetrics) => Unit = (_, _) => {}
)

// Custom callbacks
val callbacks = BulkCallbacks(
  onSuccess = (id, index) => println(s"✅ Indexed: $id in $index"),
  onFailure = failed => println(s"❌ Failed: ${failed.id} - ${failed.error}"),
  onComplete = result =>
    println(
      s"📊 Bulk completed: ${result.successCount} successes, ${result.failedCount} failures " +
        s"in ${result.metrics.durationMs}ms (${result.metrics.throughput} docs/sec)"
    ),
  onBatchComplete = (batchSize, metrics) =>
    println(s"📊 Batch completed: $batchSize docs (${metrics.throughput} docs/sec)")
)
```

---

## Public Methods

### bulkWithResult

Executes bulk operations with detailed success/failure reporting and metrics.

**Signature:**

```scala
def bulkWithResult[D](
  items: Iterator[D],
  toDocument: D => String,
  indexKey: Option[String] = None,
  idKey: Option[String] = None,
  suffixDateKey: Option[String] = None,
  suffixDatePattern: Option[String] = None,
  update: Option[Boolean] = None,
  delete: Option[Boolean] = None,
  parentIdKey: Option[String] = None,
  callbacks: BulkCallbacks = BulkCallbacks.default
)(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult]
```

**Parameters:**
- `items` - Iterator of documents to process
- `toDocument` - Function to convert document to JSON string
- `indexKey` - Optional field name containing index name
- `idKey` - Optional field name containing document ID
- `suffixDateKey` - Optional date field for index suffix (e.g., "2024-01-15")
- `suffixDatePattern` - Date pattern for suffix formatting
- `update` - If true, performs upsert operation
- `delete` - If true, performs delete operation
- `parentIdKey` - Optional parent document ID field
- `callbacks` - Event callbacks for monitoring
- `bulkOptions` - Implicit bulk configuration
- `system` - Implicit ActorSystem for Akka Streams

**Returns:**
- `Future[BulkResult]` with detailed success/failure information

**BulkResult Structure:**

```scala
case class BulkResult(
  successCount: Int,                    // Number of successful operations
  successIds: Set[String],              // IDs of successful documents
  failedCount: Int,                     // Number of failed operations
  failedDocuments: Seq[FailedDocument], // Failed documents with errors
  indices: Set[String],                 // All indices affected
  metrics: BulkMetrics                  // Performance metrics
)

case class BulkMetrics(
  startTime: Long = System.currentTimeMillis(),
  endTime: Option[Long] = None,
  totalBatches: Int = 0,
  totalDocuments: Int = 0,
  failuresByStatus: Map[Int, Int] = Map.empty,
  failuresByType: Map[String, Int] = Map.empty
) {
  def durationMs: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime

  def throughput: Double = // Documents per second
    if (durationMs > 0) totalDocuments * 1000.0 / durationMs
    else 0.0

  def complete: BulkMetrics = copy(endTime = Some(System.currentTimeMillis()))

  def addFailure(error: BulkError): BulkMetrics = copy(
    failuresByStatus =
      failuresByStatus + (error.status -> (failuresByStatus.getOrElse(error.status, 0) + 1)),
    failuresByType =
      failuresByType + (error.`type` -> (failuresByType.getOrElse(error.`type`, 0) + 1))
  )
}
```

**Examples:**

```scala
import akka.actor.ActorSystem
import scala.concurrent.ExecutionContext.Implicits.global

implicit val system: ActorSystem = ActorSystem("bulk-system")
implicit val bulkOptions: BulkOptions = BulkOptions(
  defaultIndex =  "products",
  maxBulkSize = 1000,
  balance = 4
)

// Domain model
case class Product(id: String, name: String, price: Double, category: String)

// Basic bulk indexing
val products: Iterator[Product] = getProducts() // Large dataset

val toJson: Product => String = product => s"""
{
  "id": "${product.id}",
  "name": "${product.name}",
  "price": ${product.price},
  "category": "${product.category}"
}
"""

val resultFuture: Future[BulkResult] = client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id")
)

resultFuture.foreach { result =>
  println(s"✅ Success: ${result.successCount}")
  println(s"❌ Failed: ${result.failedCount}")
  println(s"📊 Throughput: ${result.metrics.throughput} docs/sec")
  
  // Handle failures
  result.failedDocuments.foreach { failed =>
    println(s"Failed ID: ${failed.id}, Error: ${failed.error}")
  }
}

// With callbacks for real-time monitoring
val callbacks = BulkCallbacks(
  onSuccess = (id, index) => 
    logger.info(s"✅ Indexed document $id in $index"),
  
  onFailure = failed => 
    logger.error(s"❌ Failed to index ${failed.id}: ${failed.error}"),
  
  onComplete = result => {
    logger.info(s"""
      |Bulk operation completed:
      |  - Success: ${result.successCount}
      |  - Failed: ${result.failedCount}
      |  - Duration: ${result.metrics.durationMs}ms
      |  - Throughput: ${result.metrics.throughput} docs/sec
    """.stripMargin)
  }
)

client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id"),
  callbacks = callbacks
)

// Bulk update (upsert)
client.bulkWithResult(
  items = productUpdates,
  toDocument = toJson,
  idKey = Some("id"),
  update = Some(true)  // Upsert mode
).foreach { result =>
  println(s"Updated ${result.successCount} products")
}

// Bulk delete
val idsToDelete: Iterator[String] = getObsoleteProductIds()

client.bulkWithResult(
  items = idsToDelete.map(id => Map("id" -> id)),
  toDocument = doc => s"""{"id": "${doc("id")}"}""",
  idKey = Some("id"),
  delete = Some(true)
)

// Date-based index suffixing
case class LogEntry(id: String, message: String, timestamp: String)

val logs: Iterator[LogEntry] = getLogEntries()

client.bulkWithResult(
  items = logs,
  toDocument = log => s"""
  {
    "id": "${log.id}",
    "message": "${log.message}",
    "timestamp": "${log.timestamp}"
  }
  """,
  idKey = Some("id"),
  suffixDateKey = Some("timestamp"),      // Field containing date
  suffixDatePattern = Some("yyyy-MM-dd")  // Pattern for suffix
)(
  bulkOptions.copy(defaultIndex = "logs"),       // Base index: "logs-2024-01-15"
  system
)

// Error handling and retry analysis
resultFuture.foreach { result =>
  if (result.failedCount > 0) {
    // Group errors by type
    val errorsByType = result.failedDocuments
      .groupBy(_.error)
      .mapValues(_.size)
    
    errorsByType.foreach { case (errorType, count) =>
      println(s"Error: $errorType - Count: $count")
    }
    
    // Identify retryable failures
    val retryable = result.failedDocuments.filter(_.retryable)
    println(s"Retryable failures: ${retryable.size}")
  }
}

// Performance tuning example
implicit val highThroughputOptions: BulkOptions = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 5000,        // Larger batches
  balance = 8,               // More parallel workers
  disableRefresh = true,     // Disable auto-refresh for speed
  retryOnFailure = true,
  maxRetries = 5
)

client.bulkWithResult(
  items = largeDataset,
  toDocument = toJson,
  idKey = Some("id")
).foreach { result =>
  // Manual refresh after bulk
  result.indices.foreach(client.refresh)
  println(s"Bulk completed: ${result.metrics.throughput} docs/sec")
}
```

---

### bulkSource

Returns an Akka Streams Source that emits real-time results for each document.

**Signature:**

```scala
def bulkSource[D](
  items: Iterator[D],
  toDocument: D => String,
  idKey: Option[String] = None,
  suffixDateKey: Option[String] = None,
  suffixDatePattern: Option[String] = None,
  update: Option[Boolean] = None,
  delete: Option[Boolean] = None,
  parentIdKey: Option[String] = None
)(implicit
  bulkOptions: BulkOptions,
  system: ActorSystem
): Source[Either[FailedDocument, SuccessfulDocument], NotUsed]
```

**Parameters:**
- Same as `bulkWithResult` (except callbacks)

**Returns:**
- `Source[Either[FailedDocument, SuccessfulDocument], NotUsed]`
- Emits `Right(SuccessfulDocument)` for success
- Emits `Left(FailedDocument)` for failure

**Use Cases:**
- Custom stream processing
- Real-time monitoring
- Integration with other Akka Streams
- Custom error handling logic

**Examples:**

```scala
import akka.stream.scaladsl._
import akka.NotUsed

// Basic streaming with real-time results
val source: Source[Either[FailedDocument, SuccessfulDocument], NotUsed] = 
  client.bulkSource(
    items = products,
    toDocument = toJson,
    idKey = Some("id")
  )

// Process results in real-time
source.runWith(Sink.foreach {
  case Right(success) =>
    println(s"✅ Success: ${success.id} in ${success.index}")
  
  case Left(failed) =>
    println(s"❌ Failed: ${failed.id} - ${failed.error}")
})

// Count successes and failures
source
  .runWith(Sink.fold((0, 0)) {
    case ((successCount, failCount), Right(_)) => 
      (successCount + 1, failCount)
    case ((successCount, failCount), Left(_)) => 
      (successCount, failCount + 1)
  })
  .foreach { case (success, failed) =>
    println(s"Results: $success success, $failed failed")
  }

// Filter only failures
source
  .collect { case Left(failed) => failed }
  .runWith(Sink.foreach { failed =>
    logger.error(s"Failed document: ${failed.id}")
  })

// Custom retry logic
source
  .mapAsync(1) {
    case Right(success) => Future.successful(Right(success))
    
    case Left(failed) if failed.retryable =>
      // Custom retry logic
      retryDocument(failed).map {
        case true => Right(SuccessfulDocument(failed.id, failed.index))
        case false => Left(failed)
      }
    
    case Left(failed) => Future.successful(Left(failed))
  }
  .runWith(Sink.ignore)

// Integration with other streams
val csvSource: Source[String, NotUsed] = 
  FileIO.fromPath(Paths.get("products.csv"))
    .via(Framing.delimiter(ByteString("\n"), 1024))
    .map(_.utf8String)

csvSource
  .map(parseCsvLine)
  .grouped(1000)
  .flatMapConcat { batch =>
    client.bulkSource(
      items = batch.iterator,
      toDocument = toJson,
      idKey = Some("id")
    )
  }
  .runWith(Sink.foreach {
    case Right(success) => println(s"✅ ${success.id}")
    case Left(failed) => println(s"❌ ${failed.id}")
  })

// Progress tracking
var processed = 0
source
  .map { result =>
    processed += 1
    if (processed % 1000 == 0) {
      println(s"Processed: $processed documents")
    }
    result
  }
  .runWith(Sink.ignore)

// Write failures to file
source
  .collect { case Left(failed) => failed }
  .map(failed => s"${failed.id},${failed.error}\n")
  .map(ByteString(_))
  .runWith(FileIO.toPath(Paths.get("failures.csv")))

// Broadcast to multiple sinks
source
  .alsoTo(Sink.foreach {
    case Right(success) => metricsCollector.recordSuccess()
    case Left(failed) => metricsCollector.recordFailure()
  })
  .runWith(Sink.ignore)
```

---

### bulk (Deprecated)

Legacy synchronous bulk method. **Use `bulkWithResult` instead.**

**Signature:**

```scala
@deprecated("Use bulkWithResult for better error handling")
def bulk[D](
  items: Iterator[D],
  toDocument: D => String,
  idKey: Option[String] = None,
  suffixDateKey: Option[String] = None,
  suffixDatePattern: Option[String] = None,
  update: Option[Boolean] = None,
  delete: Option[Boolean] = None,
  parentIdKey: Option[String] = None
)(implicit bulkOptions: BulkOptions, system: ActorSystem): ElasticResult[BulkResult]
```

**Note:** This method blocks the current thread. Use `bulkWithResult` for non-blocking operations.

---

## Implementation Requirements

### toBulkElasticAction

```scala
implicit private[client] def toBulkElasticAction(
  a: BulkActionType
): BulkElasticAction
```

Converts internal `BulkActionType` to Elasticsearch-specific bulk action.

---

### bulkFlow

```scala
private[client] def bulkFlow(implicit
  bulkOptions: BulkOptions,
  system: ActorSystem
): Flow[Seq[BulkActionType], BulkResultType, NotUsed]
```

**Implementation Example:**

```scala
private[client] def bulkFlow(implicit
  bulkOptions: BulkOptions,
  system: ActorSystem
): Flow[Seq[BulkActionType], BulkResultType, NotUsed] = {
  
  implicit val ec: ExecutionContext = system.dispatcher
  
  Flow[Seq[BulkActionType]]
    .mapAsync(1) { actions =>
      val bulkRequest = new BulkRequest()
      
      actions.foreach { action =>
        val elasticAction = toBulkElasticAction(action)
        bulkRequest.add(elasticAction)
      }
      
      Future {
        client.bulk(bulkRequest, RequestOptions.DEFAULT)
      }
    }
}
```

---

### extractBulkResults

```scala
private[client] def extractBulkResults(
  result: BulkResultType,
  originalBatch: Seq[BulkItem]
): Seq[Either[FailedDocument, SuccessfulDocument]]
```

**Implementation Example:**

```scala
private[client] def extractBulkResults(
  result: BulkResponse,
  originalBatch: Seq[BulkItem]
): Seq[Either[FailedDocument, SuccessfulDocument]] = {
  
  result.getItems.zip(originalBatch).map { case (item, original) =>
    if (item.isFailed) {
      Left(FailedDocument(
        id = original.id.getOrElse("unknown"),
        index = original.index,
        document = original.document,
        error = item.getFailureMessage,
        retryable = isRetryable(item.getFailure)
      ))
    } else {
      Right(SuccessfulDocument(
        id = item.getId,
        index = item.getIndex
      ))
    }
  }
}

private def isRetryable(failure: BulkItemResponse.Failure): Boolean = {
  val retryableErrors = Set(
    "version_conflict_engine_exception",
    "es_rejected_execution_exception",
    "timeout_exception"
  )
  retryableErrors.exists(failure.getMessage.contains)
}
```

---

### toBulkAction & actionToBulkItem

```scala
private[client] def toBulkAction(bulkItem: BulkItem): BulkActionType

private[client] def actionToBulkItem(action: BulkActionType): BulkItem
```

Bidirectional conversion between internal `BulkItem` and Elasticsearch-specific `BulkActionType`.

---

## Common Patterns

### High-Throughput Indexing

```scala
// Optimize for maximum throughput
implicit val highPerformanceOptions: BulkOptions = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 10000,       // Large batches
  balance = 16,              // Many parallel workers
  disableRefresh = true,     // No refresh during bulk
  retryOnFailure = false,    // Skip retry for speed
  logEvery = 50              // Less frequent logging
)

val result = client.bulkWithResult(
  items = massiveDataset,
  toDocument = toJson,
  idKey = Some("id")
)

result.foreach { r =>
  // Manual refresh once at the end
  r.indices.foreach(client.refresh)
  println(s"Indexed ${r.successCount} documents at ${r.metrics.throughput} docs/sec")
}
```

### Reliable Indexing with Retry

```scala
// Optimize for reliability
implicit val reliableOptions: BulkOptions = BulkOptions(
  defaultIndex = "critical-data",
  maxBulkSize = 500,         // Smaller batches
  balance = 2,               // Conservative parallelism
  disableRefresh = false,    // Auto-refresh
  retryOnFailure = true,     // Enable retry
  maxRetries = 5,            // More retry attempts
  retryDelay = 2.seconds,    // Longer initial delay
  retryBackoffMultiplier = 3.0
)

val result = client.bulkWithResult(
  items = criticalData,
  toDocument = toJson,
  idKey = Some("id")
)

result.foreach { r =>
  if (r.failedCount > 0) {
    // Log all failures for investigation
    r.failedDocuments.foreach { failed =>
      logger.error(s"Critical failure: ${failed.id} - ${failed.error}")
      alerting.sendAlert(s"Failed to index critical document: ${failed.id}")
    }
  }
}
```

### Time-Series Data with Date Suffixes

```scala
case class LogEntry(
  id: String,
  timestamp: String,  // ISO format: "2024-01-15T10:30:00Z"
  level: String,
  message: String
)

val logs: Iterator[LogEntry] = streamLogs()

implicit val logOptions: BulkOptions = BulkOptions(
  defaultIndex = "logs",            // Base index
  maxBulkSize = 2000,
  balance = 4
)

client.bulkWithResult(
  items = logs,
  toDocument = log => s"""
  {
    "id": "${log.id}",
    "timestamp": "${log.timestamp}",
    "level": "${log.level}",
    "message": "${log.message}"
  }
  """,
  idKey = Some("id"),
  suffixDateKey = Some("timestamp"),
  suffixDatePattern = Some("yyyy-MM-dd")
)
// Creates indices: logs-2024-01-15, logs-2024-01-16, etc.
```

### Incremental Updates

```scala
case class ProductUpdate(id: String, price: Double, stock: Int)

val updates: Iterator[ProductUpdate] = getProductUpdates()

client.bulkWithResult(
  items = updates,
  toDocument = update => s"""
  {
    "id": "${update.id}",
    "price": ${update.price},
    "stock": ${update.stock}
  }
  """,
  idKey = Some("id"),
  update = Some(true)  // Upsert mode
).foreach { result =>
  println(s"Updated ${result.successCount} products")
}
```

### Batch Deletion

```scala
val obsoleteIds: Iterator[String] = findObsoleteDocuments()

client.bulkWithResult(
  items = obsoleteIds.map(id => Map("id" -> id)),
  toDocument = doc => s"""{"id": "${doc("id")}"}""",
  idKey = Some("id"),
  delete = Some(true)
).foreach { result =>
  println(s"Deleted ${result.successCount} documents")
}
```

---

## Performance Optimization

### Tuning Parameters

| Parameter        | Low Throughput  | Balanced  | High Throughput  |
|------------------|-----------------|-----------|------------------|
| `maxBulkSize`    | 500             | 1000      | 5000-10000       |
| `balance`        | 1-2             | 4         | 8-16             |
| `disableRefresh` | false           | false     | true             |
| `retryOnFailure` | true            | true      | false            |

### Memory Considerations

```scala
// For large documents, use smaller batches
implicit val largeDocOptions: BulkOptions = BulkOptions(
  defaultIndex = "documents",
  maxBulkSize = 100,  // Fewer large documents per batch
  balance = 2
)

// For small documents, use larger batches
implicit val smallDocOptions: BulkOptions = BulkOptions(
  defaultIndex = "events",
  maxBulkSize = 10000,  // Many small documents per batch
  balance = 8
)
```

### Backpressure Handling

```scala
// Akka Streams automatically handles backpressure
val source = client.bulkSource(
  items = infiniteStream,
  toDocument = toJson,
  idKey = Some("id")
)

// Add throttling if needed
source
  .throttle(1000, 1.second)  // Max 1000 docs/sec
  .runWith(Sink.foreach {
    case Right(success) => println(s"✅ ${success.id}")
    case Left(failed) => println(s"❌ ${failed.id}")
  })
end_scalar
```

---

## Error Handling

### Retryable vs Non-Retryable Errors

```scala
// Retryable errors (automatic retry)
val retryableErrors = Set(
  "version_conflict_engine_exception",  // Concurrent modification
  "es_rejected_execution_exception",    // Queue full
  "timeout_exception",                  // Temporary timeout
  "connect_exception"                   // Network issue
)

// Non-retryable errors (fail immediately)
val nonRetryableErrors = Set(
  "mapper_parsing_exception",           // Invalid document structure
  "illegal_argument_exception",         // Invalid field value
  "index_not_found_exception"           // Missing index
)
```

### Handling Failures

```scala
val result = client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id")
)

result.foreach { r =>
  if (r.failedCount > 0) {
    // Group by error type
    val errorGroups = r.failedDocuments.groupBy(_.error)
    
    errorGroups.foreach { case (errorType, failures) =>
      println(s"Error: $errorType")
      println(s"Count: ${failures.size}")
      
      // Handle specific error types
      errorType match {
        case e if e.contains("mapper_parsing") =>
          // Log invalid documents for review
          failures.foreach { f =>
            logger.error(s"Invalid document: ${f.document}")
          }
        
        case e if e.contains("version_conflict") =>
          // Retry with latest version
          failures.foreach { f =>
            retryWithFreshVersion(f.id)
          }
        
        case _ =>
          logger.error(s"Unhandled error: $errorType")
      }
    }
  }
}
```

---

## Monitoring and Metrics

### Real-Time Progress Tracking

```scala
val callbacks = BulkCallbacks(
  onSuccess = (id, index) => {
    metricsCollector.incrementSuccess()
  },
  
  onFailure = failed => {
    metricsCollector.incrementFailure(failed.error)
  },
  
  onComplete = result => {
    val metrics = result.metrics
    logger.info(s"""
      |Bulk Operation Summary:
      |  Duration: ${metrics.durationMs}ms
      |  Total Documents: ${metrics.totalDocuments}
      |  Success: ${result.successCount}
      |  Failed: ${result.failedCount}
      |  Throughput: ${metrics.throughput} docs/sec
      |  Batches: ${metrics.totalBatches}
      |  Indices: ${result.indices.mkString(", ")}
    """.stripMargin)
    
    // Error breakdown
    metrics.errorsByType.foreach { case (errorType, count) =>
      logger.info(s"  $errorType: $count")
    }
  }
)

client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id"),
  callbacks = callbacks
)
```

### Custom Metrics Collection

```scala
var successCount = 0
var failureCount = 0
val startTime = System.currentTimeMillis()

client.bulkSource(
  items = products,
  toDocument = toJson,
  idKey = Some("id")
).runWith(Sink.foreach {
  case Right(_) =>
    successCount += 1
    if (successCount % 1000 == 0) {
      val elapsed = System.currentTimeMillis() - startTime
      val throughput = (successCount * 1000.0) / elapsed
      println(s"Progress: $successCount docs, $throughput docs/sec")
    }
  
  case Left(_) =>
    failureCount += 1
})
```

---

## Best Practices

**1. Choose Appropriate Batch Sizes**

```scala
// ✅ Good - balanced batch size
implicit val options = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 1000  // Good for most use cases
)

// ❌ Too small - overhead
implicit val tooSmall = BulkOptions(maxBulkSize = 10)

// ❌ Too large - memory issues
implicit val tooLarge = BulkOptions(maxBulkSize = 100000)
```

**2. Disable Refresh for Large Bulks**

```scala
// ✅ Good - disable refresh during bulk
implicit val options = BulkOptions(
  defaultIndex = "products",
  disableRefresh = true
)

val result = client.bulkWithResult(items, toJson, Some("id"))
result.foreach { r =>
  // Manual refresh once at the end
  r.indices.foreach(client.refresh)
}
```

**3. Handle Failures Appropriately**

```scala
// ✅ Good - detailed failure handling
result.foreach { r =>
  r.failedDocuments.foreach { failed =>
    if (failed.retryable) {
      retryQueue.add(failed)
    } else {
      deadLetterQueue.add(failed)
    }
  }
}

// ❌ Avoid - ignoring failures
result.foreach { r =>
  println(s"Success: ${r.successCount}")
  // Failures ignored!
}
```

**4. Use Callbacks for Monitoring**

```scala
// ✅ Good - real-time monitoring
val callbacks = BulkCallbacks(
  onSuccess = (id, index) => recordSuccess(id, index),
  onFailure = failed => recordFailure(failed.error),
  onComplete = result => sendCompletionNotification(result)
)

client.bulkWithResult(items, toJson, Some("id"), callbacks = callbacks)
```

**5. Tune Parallelism Based on Cluster Size**

```scala
// Small cluster (1-3 nodes)
implicit val smallCluster = BulkOptions(balance = 2)

// Medium cluster (4-10 nodes)
implicit val mediumCluster = BulkOptions(balance = 4)

// Large cluster (10+ nodes)
implicit val largeCluster = BulkOptions(balance = 8)
```

---

## Testing Scenarios

### Test Basic Bulk Indexing

```scala
def testBulkIndexing()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(
    defaultIndex = "test-bulk",
    maxBulkSize = 100
  )
  
  val testData = (1 to 1000).map { i =>
    Map("id" -> s"doc-$i", "name" -> s"Product $i", "price" -> (i * 10.0))
  }
  
  val toJson: Map[String, Any] => String = doc => s"""
  {
    "id": "${doc("id")}",
    "name": "${doc("name")}",
    "price": ${doc("price")}
  }
  """
  
  client.bulkWithResult(
    items = testData.iterator,
    toDocument = toJson,
    idKey = Some("id")
  ).map { result =>
    assert(result.successCount == 1000, "All documents should be indexed")
    assert(result.failedCount == 0, "No failures expected")
    assert(result.indices.contains("test-bulk"), "Index should be created")
    
    println(s"✅ Bulk test passed: ${result.successCount} documents indexed")
  }
}
```

### Test Bulk Update

```scala
def testBulkUpdate()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "test-bulk")
  
  for {
    // First, index documents
    _ <- client.bulkWithResult(
      items = testData.iterator,
      toDocument = toJson,
      idKey = Some("id")
    )
    
    // Then, update them
    updates = testData.map(doc => doc.updated("price", 999.99))
    updateResult <- client.bulkWithResult(
      items = updates.iterator,
      toDocument = toJson,
      idKey = Some("id"),
      update = Some(true)
    )
    
    _ = assert(updateResult.successCount == testData.size, "All updates should succeed")
    
    // Verify updates
    doc <- client.get("doc-1", "test-bulk")
    _ = assert(doc.contains("999.99"), "Price should be updated")
  } yield {
    println("✅ Bulk update test passed")
  }
}
```

### Test Bulk Delete

```scala
def testBulkDelete()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "test-bulk")
  
  for {
    // Index documents
    _ <- client.bulkWithResult(
      items = testData.iterator,
      toDocument = toJson,
      idKey = Some("id")
    )
    
    // Delete them
    deleteResult <- client.bulkWithResult(
      items = testData.iterator,
      toDocument = toJson,
      idKey = Some("id"),
      delete = Some(true)
    )
    
    _ = assert(deleteResult.successCount == testData.size, "All deletes should succeed")
    
    // Verify deletion
    exists <- client.exists("doc-1", "test-bulk")
    _ = assert(!exists, "Document should be deleted")
  } yield {
    println("✅ Bulk delete test passed")
  }
}
```

### Test Error Handling

```scala
def testBulkErrorHandling()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(
    defaultIndex = "test-bulk",
    retryOnFailure = false  // Disable retry for testing
  )
  
  val mixedData = Seq(
    """{"id": "valid-1", "name": "Valid Product"}""",
    """{"id": "invalid", "name": INVALID_JSON}""",  // Invalid JSON
    """{"id": "valid-2", "name": "Another Valid"}"""
  )
  
  client.bulkWithResult(
    items = mixedData.iterator,
    toDocument = identity,
    idKey = Some("id")
  ).map { result =>
    assert(result.successCount == 2, "Two valid documents should succeed")
    assert(result.failedCount == 1, "One invalid document should fail")
    
    val failed = result.failedDocuments.head
    assert(failed.id == "invalid", "Failed document ID should match")
    assert(failed.error.contains("parse"), "Error should mention parsing")
    
    println("✅ Error handling test passed")
  }
}
```

### Test Date-Based Index Suffixing

```scala
def testDateSuffixing()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "logs")
  
  val logs = Seq(
    """{"id": "log-1", "timestamp": "2024-01-15T10:00:00Z", "message": "Log 1"}""",
    """{"id": "log-2", "timestamp": "2024-01-16T10:00:00Z", "message": "Log 2"}""",
    """{"id": "log-3", "timestamp": "2024-01-17T10:00:00Z", "message": "Log 3"}"""
  )
  
  client.bulkWithResult(
    items = logs.iterator,
    toDocument = identity,
    idKey = Some("id"),
    suffixDateKey = Some("timestamp"),
    suffixDatePattern = Some("yyyy-MM-dd")
  ).map { result =>
    assert(result.successCount == 3, "All logs should be indexed")
    assert(result.indices.contains("logs-2024-01-15"), "Index with date suffix should exist")
    assert(result.indices.contains("logs-2024-01-16"), "Index with date suffix should exist")
    assert(result.indices.contains("logs-2024-01-17"), "Index with date suffix should exist")
    assert(result.indices.size == 3, "Three different indices should be created")
    
    println("✅ Date suffixing test passed")
  }
}
```

### Test Retry Mechanism

```scala
def testRetryMechanism()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(
    defaultIndex = "test-bulk",
    retryOnFailure = true,
    maxRetries = 3,
    retryDelay = 100.millis
  )
  
  var attemptCount = 0
  
  // Simulate transient failure
  val mockData = Seq("""{"id": "doc-1", "name": "Test"}""")
  
  client.bulkWithResult(
    items = mockData.iterator,
    toDocument = { doc =>
      attemptCount += 1
      if (attemptCount < 3) {
        // Simulate transient error
        throw new Exception("Simulated transient error")
      }
      doc
    },
    idKey = Some("id")
  ).map { result =>
    assert(result.successCount == 1, "Document should succeed after retry")
    assert(attemptCount >= 2, "Should have retried at least once")
    
    println(s"✅ Retry test passed (attempts: $attemptCount)")
  }
}
```

### Test Performance Metrics

```scala
def testPerformanceMetrics()(implicit system: ActorSystem): Future[Unit] = {
  implicit val bulkOptions: BulkOptions = BulkOptions(
    defaultIndex = "test-bulk",
    maxBulkSize = 1000,
    logEvery = 10
  )
  
  val largeDataset = (1 to 10000).map { i =>
    s"""{"id": "doc-$i", "name": "Product $i"}"""
  }
  
  client.bulkWithResult(
    items = largeDataset.iterator,
    toDocument = identity,
    idKey = Some("id")
  ).map { result =>
    val metrics = result.metrics
    
    assert(metrics.totalDocuments == 10000, "Total documents should match")
    assert(metrics.totalBatches == 10, "Should have 10 batches (1000 each)")
    assert(metrics.throughput > 0, "Throughput should be calculated")
    assert(metrics.duration > 0, "Duration should be recorded")
    
    println(s"""
      |✅ Performance test passed:
      |  Documents: ${metrics.totalDocuments}
      |  Batches: ${metrics.totalBatches}
      |  Duration: ${metrics.duration}ms
      |  Throughput: ${metrics.throughput} docs/sec
    """.stripMargin)
  }
}
```

---

## Advanced Use Cases

### Multi-Index Bulk Operations

```scala
case class Document(id: String, index: String, data: String)

val multiIndexDocs: Iterator[Document] = getDocuments()

// Custom transformation to handle multiple indices
client.bulkWithResult(
  items = multiIndexDocs,
  toDocument = doc => s"""
  {
    "id": "${doc.id}",
    "index": "${doc.index}",
    "data": "${doc.data}"
  }
  """,
  indexKey = Some("index"),  // Dynamic index per document
  idKey = Some("id")
)(
  bulkOptions.copy(defaultIndex = "default"),  // Fallback index
  system
).foreach { result =>
  println(s"Indexed across ${result.indices.size} indices")
  result.indices.foreach(idx => println(s"  - $idx"))
}
```

### Conditional Bulk Operations

```scala
def bulkWithCondition[D](
  items: Iterator[D],
  toDocument: D => String,
  condition: D => Boolean
)(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {
  
  val filteredItems = items.filter(condition)
  
  client.bulkWithResult(
    items = filteredItems,
    toDocument = toDocument,
    idKey = Some("id")
  )
}

// Usage: Only index products with price > 0
bulkWithCondition(
  items = products,
  toDocument = toJson,
  condition = (p: Product) => p.price > 0
)
```

### Bulk with Transformation Pipeline

```scala
def bulkWithTransformation[D, T](
  items: Iterator[D],
  transform: D => T,
  toDocument: T => String
)(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {
  
  val transformedItems = items.map(transform)
  
  client.bulkWithResult(
    items = transformedItems,
    toDocument = toDocument,
    idKey = Some("id")
  )
}

// Usage: Enrich products before indexing
case class EnrichedProduct(
  id: String,
  name: String,
  price: Double,
  category: String,
  enrichedAt: String
)

def enrichProduct(product: Product): EnrichedProduct = {
  EnrichedProduct(
    id = product.id,
    name = product.name,
    price = product.price,
    category = categorize(product),
    enrichedAt = java.time.Instant.now().toString
  )
}

bulkWithTransformation(
  items = products,
  transform = enrichProduct,
  toDocument = toJson
)
```

### Bulk with External API Integration

```scala
def bulkWithExternalEnrichment[D](
  items: Iterator[D],
  enrichmentApi: D => Future[D],
  toDocument: D => String
)(implicit 
  bulkOptions: BulkOptions, 
  system: ActorSystem,
  ec: ExecutionContext
): Future[BulkResult] = {
  
  // Enrich in batches to avoid overwhelming external API
  val enrichedFuture = Future.sequence(
    items.grouped(100).map { batch =>
      Future.sequence(batch.map(enrichmentApi))
    }
  ).map(_.flatten)
  
  enrichedFuture.flatMap { enrichedItems =>
    client.bulkWithResult(
      items = enrichedItems.iterator,
      toDocument = toDocument,
      idKey = Some("id")
    )
  }
}
```

### Bulk with Deduplication

```scala
def bulkWithDeduplication[D](
  items: Iterator[D],
  getId: D => String,
  toDocument: D => String
)(implicit bulkOptions: BulkOptions, system: ActorSystem): Future[BulkResult] = {
  
  val seen = scala.collection.mutable.Set[String]()
  val dedupedItems = items.filter { item =>
    val id = getId(item)
    if (seen.contains(id)) {
      false
    } else {
      seen.add(id)
      true
    }
  }
  
  client.bulkWithResult(
    items = dedupedItems,
    toDocument = toDocument,
    idKey = Some("id")
  )
}
```

---

## Troubleshooting

### Common Issues and Solutions

**1. Out of Memory Errors**

```scala
// Problem: Large batches causing OOM
implicit val problematic = BulkOptions(maxBulkSize = 100000)

// Solution: Reduce batch size
implicit val fixed = BulkOptions(maxBulkSize = 1000)
```

**2. Slow Performance**

```scala
// Problem: Sequential processing
implicit val slow = BulkOptions(balance = 1)

// Solution: Increase parallelism
implicit val fast = BulkOptions(
  balance = 8,
  maxBulkSize = 5000,
  disableRefresh = true
)
```

**3. Too Many Retries**

```scala
// Problem: Retrying non-retryable errors
implicit val wasteful = BulkOptions(
  retryOnFailure = true,
  maxRetries = 10
)

// Solution: Identify and skip non-retryable errors
result.foreach { r =>
  r.failedDocuments.foreach { failed =>
    if (!failed.retryable) {
      deadLetterQueue.add(failed)  // Don't retry
    }
  }
}
```

**4. Index Refresh Issues**

```scala
// Problem: Slow indexing due to frequent refresh
implicit val slow = BulkOptions(disableRefresh = false)

// Solution: Disable refresh during bulk, refresh once at end
implicit val fast = BulkOptions(disableRefresh = true)

client.bulkWithResult(items, toJson, Some("id")).foreach { result =>
  result.indices.foreach(client.refresh)  // Manual refresh
}
```

---

## Comparison with Other Operations

### Bulk vs Individual Operations

| Aspect             | Individual       | Bulk                  |
|--------------------|------------------|-----------------------|
| **Performance**    | Slow (1 req/doc) | Fast (1000s docs/req) |
| **Network**        | High overhead    | Minimal overhead      |
| **Memory**         | Low              | Higher                |
| **Error Handling** | Immediate        | Batched               |
| **Use Case**       | Single documents | Large datasets        |

```scala
// Individual indexing (slow)
products.foreach { product =>
  client.index("products", product.id, toJson(product))
}

// Bulk indexing (fast)
client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id")
)
```

---

## Summary

### Key Takeaways

1. **Use bulk operations for large datasets** (> 100 documents)
2. **Tune batch size** based on document size and memory
3. **Disable refresh** during bulk, refresh once at end
4. **Enable retry** for production reliability
5. **Monitor metrics** for performance optimization
6. **Handle failures** appropriately (retry vs dead letter queue)
7. **Use callbacks** for real-time monitoring
8. **Adjust parallelism** based on cluster size

### Quick Reference

```scala
// High-performance bulk indexing
implicit val options = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 5000,
  balance = 8,
  disableRefresh = true,
  retryOnFailure = true,
  maxRetries = 3
)

client.bulkWithResult(
  items = products,
  toDocument = toJson,
  idKey = Some("id"),
  callbacks = BulkCallbacks.logging(logger)
).foreach { result =>
  result.indices.foreach(client.refresh)
  println(s"Indexed ${result.successCount} docs at ${result.metrics.throughput} docs/sec")
}
```

---

[Back to index](README.md) | [Next: Get Documents](get.md)