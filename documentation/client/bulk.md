[Back to index](README.md)

# BULK API

## Overview

The **BulkApi** trait provides high-performance bulk operations for Elasticsearch using Akka Streams, supporting indexing, updating, and deleting large volumes of documents with advanced features like automatic retry, progress tracking, and detailed result reporting.

**Features:**
- **High-performance streaming** with Akka Streams
- **Multiple data sources**: In-memory, File-based (JSON, JSON Array, Parquet, Delta Lake)
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
- Requires Hadoop for file operations (Parquet, Delta Lake)

---

## Core Concepts

### Bulk Operations Flow

```scala
// Data flow pipeline
Source[D, NotUsed]
  -> Transform to JSON
  -> Create BulkItem
  -> Apply settings (refresh, replicas)
  -> Group into batches
  -> Execute bulk requests in parallel
  -> Extract results
  -> Retry failures (automatic)
  -> Return Either[FailedDocument, SuccessfulDocument]
```

### Data Sources

| Source Type      | Format        | Description                           |
|------------------|---------------|---------------------------------------|
| **In-Memory**    | Scala objects | Direct streaming from collections     |
| **JSON**         | Text          | Newline-delimited JSON (NDJSON)       |
| **JSON Array**   | Text          | JSON array with nested structures     |
| **Parquet**      | Binary        | Columnar storage format               |
| **Delta Lake**   | Directory     | ACID transactional data lake          |

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
implicit val bulkOptions: BulkOptions = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 5000,
  balance = 8
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
  items: Source[D, NotUsed],
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
- `items` - Source of documents to process
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

---

### bulkFromFile

**NEW**: Loads and indexes documents directly from files with support for multiple formats.

**Signature:**

```scala
def bulkFromFile(
  filePath: String,
  format: FileFormat = Json,
  indexKey: Option[String] = None,
  idKey: Option[String] = None,
  suffixDateKey: Option[String] = None,
  suffixDatePattern: Option[String] = None,
  update: Option[Boolean] = None,
  delete: Option[Boolean] = None,
  callbacks: BulkCallbacks = BulkCallbacks.default
)(implicit 
  bulkOptions: BulkOptions, 
  system: ActorSystem,
  conf: Configuration = new Configuration()
): Future[BulkResult]
```

**Parameters:**
- `filePath` - Path to the file (local or HDFS)
- `format` - File format: `Json`, `JsonArray`, `Parquet`, or `Delta`
- `indexKey` - Optional field name containing index name
- `idKey` - Optional field name containing document ID
- `suffixDateKey` - Optional date field for index suffix
- `suffixDatePattern` - Date pattern for suffix formatting
- `update` - If true, performs upsert operation
- `delete` - If true, performs delete operation
- `callbacks` - Event callbacks for monitoring
- `bulkOptions` - Implicit bulk configuration
- `system` - Implicit ActorSystem
- `conf` - Implicit Hadoop Configuration (for Parquet/Delta)

**Supported File Formats:**

```scala
sealed trait FileFormat
case object Json extends FileFormat       // Newline-delimited JSON
case object JsonArray extends FileFormat  // JSON array with nested objects
case object Parquet extends FileFormat    // Apache Parquet columnar format
case object Delta extends FileFormat      // Delta Lake tables
```

**Returns:**
- `Future[BulkResult]` with detailed success/failure information

**Examples:**

#### 1. Load from JSON File (NDJSON)

```scala
// NDJSON file (one JSON object per line):
// {"id": "1", "name": "Product 1", "price": 10.0}
// {"id": "2", "name": "Product 2", "price": 20.0}

val result = client.bulkFromFile(
  filePath = "/data/products.jsonl",
  format = Json,
  idKey = Some("id")
).futureValue

println(s"Indexed ${result.successCount} products from JSON")
```

#### 2. Load from JSON Array with Nested Objects

```scala
// JSON Array file:
// [
//   { "uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0 },
//   { "uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09",
//     "children": [
//       { "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09" },
//       { "parentId": "A16", "name": "Josh Gumble", "birthDate": "2002-05-09" }
//     ],
//     "childrenCount": 2
//   }
// ]

val result = client.bulkFromFile(
  filePath = "/data/persons.json",
  format = JsonArray,
  idKey = Some("uuid")
).futureValue

println(s"Indexed ${result.successCount} persons with nested structures")
```

#### 3. Load from Parquet File

```scala
// Parquet file with schema: id, name, price, category
val result = client.bulkFromFile(
  filePath = "/data/products.parquet",
  format = Parquet,
  idKey = Some("id")
).futureValue

println(s"Indexed ${result.successCount} products from Parquet")
println(s"Throughput: ${result.metrics.throughput} docs/sec")
```

#### 4. Load from Delta Lake Table

```scala
// Delta Lake table directory structure:
// /data/delta-products/
//   ├── _delta_log/
//   │   ├── 00000000000000000000.json
//   │   └── 00000000000000000001.json
//   ├── part-00000.parquet
//   └── part-00001.parquet

val result = client.bulkFromFile(
  filePath = "/data/delta-products",
  format = Delta,
  idKey = Some("id")
).futureValue

println(s"Indexed ${result.successCount} products from Delta Lake")
```

#### 5. Load with Date-Based Index Suffixing

```scala
// Load logs with automatic date-based index partitioning
val result = client.bulkFromFile(
  filePath = "/data/logs.jsonl",
  format = Json,
  idKey = Some("id"),
  suffixDateKey = Some("timestamp"),
  suffixDatePattern = Some("yyyy-MM-dd")
)(
  bulkOptions.copy(defaultIndex = "logs"),
  system,
  hadoopConf
).futureValue

// Creates indices: logs-2024-01-15, logs-2024-01-16, etc.
println(s"Indexed ${result.successCount} logs across ${result.indices.size} indices")
result.indices.foreach(idx => println(s"  - $idx"))
```

#### 6. Load with Update (Upsert)

```scala
// Update existing documents or insert new ones
val result = client.bulkFromFile(
  filePath = "/data/product-updates.json",
  format = JsonArray,
  idKey = Some("id"),
  update = Some(true)  // Upsert mode
).futureValue

println(s"Updated/Inserted ${result.successCount} products")
```

#### 7. Load with Callbacks for Monitoring

```scala
val callbacks = BulkCallbacks(
  onSuccess = (id, index) => 
    logger.info(s"✅ Indexed document $id in $index"),
  
  onFailure = failed => 
    logger.error(s"❌ Failed to index ${failed.id}: ${failed.error}"),
  
  onComplete = result => {
    logger.info(s"""
      |📊 File bulk operation completed:
      |  - File: /data/products.parquet
      |  - Success: ${result.successCount}
      |  - Failed: ${result.failedCount}
      |  - Duration: ${result.metrics.durationMs}ms
      |  - Throughput: ${result.metrics.throughput} docs/sec
    """.stripMargin)
  },
  
  onBatchComplete = (batchSize, metrics) =>
    logger.info(s"📦 Processed batch: $batchSize docs (${metrics.throughput} docs/sec)")
)

val result = client.bulkFromFile(
  filePath = "/data/large-dataset.parquet",
  format = Parquet,
  idKey = Some("id"),
  callbacks = callbacks
).futureValue
```

#### 8. Load from HDFS

```scala
// Configure Hadoop for HDFS access
implicit val hadoopConf: Configuration = new Configuration()
hadoopConf.set("fs.defaultFS", "hdfs://namenode:8020")
hadoopConf.set("dfs.client.use.datanode.hostname", "true")

// Load from HDFS
val result = client.bulkFromFile(
  filePath = "hdfs://namenode:8020/data/products.parquet",
  format = Parquet,
  idKey = Some("id")
).futureValue

println(s"Indexed ${result.successCount} products from HDFS")
```

#### 9. Error Handling

```scala
val result = client.bulkFromFile(
  filePath = "/data/products.json",
  format = JsonArray,
  idKey = Some("id")
).futureValue

// Handle failures
if (result.failedCount > 0) {
  println(s"⚠️  ${result.failedCount} documents failed to index")
  
  // Group errors by type
  val errorsByType = result.failedDocuments
    .groupBy(_.error)
    .view.mapValues(_.size)
  
  errorsByType.foreach { case (errorType, count) =>
    println(s"  - $errorType: $count failures")
  }
  
  // Write failures to file for investigation
  val failedIds = result.failedDocuments.map(_.id)
  java.nio.file.Files.write(
    java.nio.file.Paths.get("/data/failed-ids.txt"),
    failedIds.mkString("\n").getBytes
  )
}
```

#### 10. Performance Tuning for Large Files

```scala
// Optimize for large file processing
implicit val highPerformanceOptions: BulkOptions = BulkOptions(
  defaultIndex = "products",
  maxBulkSize = 5000,        // Larger batches
  balance = 8,               // More parallel workers
  disableRefresh = true,     // Disable auto-refresh
  retryOnFailure = true,
  maxRetries = 3,
  logEvery = 20              // Log less frequently
)

val result = client.bulkFromFile(
  filePath = "/data/large-products.parquet",
  format = Parquet,
  idKey = Some("id")
).futureValue

// Manual refresh after bulk
result.indices.foreach(client.refresh)

println(s"""
  |✅ Large file processing completed:
  |  - Documents: ${result.successCount}
  |  - Duration: ${result.metrics.durationMs}ms
  |  - Throughput: ${result.metrics.throughput} docs/sec
""".stripMargin)
```

#### 11. Delta Lake with Specific Version

```scala
// Load specific version of Delta table
val deltaPath = "/data/delta-products"

// Get Delta table info
val tableInfo = DeltaFileSource.getTableInfo(deltaPath)
println(s"Delta table version: ${tableInfo.version}")
println(s"Number of files: ${tableInfo.numFiles}")
println(s"Size: ${tableInfo.sizeInBytes} bytes")

// Load from specific version
val result = client.bulkFromFile(
  filePath = deltaPath,
  format = Delta,
  idKey = Some("id")
).futureValue

println(s"Indexed ${result.successCount} products from Delta Lake v${tableInfo.version}")
```

---

### File Format Comparison

| Format       | Speed      | Size    | Schema | Nested | Use Case                        |
|--------------|------------|---------|--------|--------|---------------------------------|
| **JSON**     | Medium     | Large   | No     | Yes    | Semi-structured data            |
| **JsonArray**| Medium     | Large   | No     | Yes    | Complex nested structures       |
| **Parquet**  | Very Fast  | Small   | Yes    | Yes    | Big data, analytics             |
| **Delta**    | Very Fast  | Small   | Yes    | Yes    | ACID transactions, time travel  |

**Recommendations :**
- **JSON/JsonArray**: APIs, logs, semi-structured data
- **Parquet**: Large datasets, columnar analytics
- **Delta Lake**: Data lakes, versioning, ACID compliance

---

### bulkSource

Returns an Akka Streams Source that emits real-time results for each document.

**Signature:**

```scala
def bulkSource[D](
  items: Source[D, NotUsed],
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

// Integration with file streaming
val csvSource: Source[String, NotUsed] = 
  FileIO.fromPath(Paths.get("products.csv"))
    .via(Framing.delimiter(ByteString("\n"), 1024))
    .map(_.utf8String)

csvSource
  .map(parseCsvLine)
  .grouped(1000)
  .flatMapConcat { batch =>
    client.bulkSource(
      items = Source(batch),
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
  items: Source[D, NotUsed],
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

## File-Based Bulk Operations

### File Validation

All file-based operations automatically validate:
- ✅ File/directory existence
- ✅ Read permissions
- ✅ File format compatibility
- ✅ Non-empty content (with warnings)

```scala
// Automatic validation
try {
  val result = client.bulkFromFile(
    filePath = "/data/products.parquet",
    format = Parquet,
    idKey = Some("id")
  ).futureValue
} catch {
  case e: IllegalArgumentException if e.getMessage.contains("does not exist") =>
    println("File not found")
  case e: IllegalArgumentException if e.getMessage.contains("not a file") =>
    println("Path is not a file")
  case e: IllegalArgumentException if e.getMessage.contains("not a directory") =>
    println("Path is not a directory (required for Delta)")
}
```

### File Metadata

Get information about files before processing:

```scala
// Parquet metadata (available)
val parquetMeta = ParquetFileSource.getFileMetadata("/data/products.parquet")
println(s"""
           |Parquet file:
           |  Rows: ${parquetMeta.numRows}
           |  Row groups: ${parquetMeta.numRowGroups}
           |  Columns: ${parquetMeta.schema.getFieldCount}
""".stripMargin)

// Delta table info (available)
val deltaMeta = DeltaFileSource.getTableInfo("/data/delta-products")
println(s"""
           |Delta table:
           |  Version: ${deltaMeta.version}
           |  Files: ${deltaMeta.numFiles}
           |  Size: ${deltaMeta.sizeInBytes} bytes
           |  Partitions: ${deltaMeta.partitionColumns.mkString(", ")}
""".stripMargin)

// JSON Array metadata (available)
val jsonMeta = JsonArrayFileSource.getMetadata("/data/persons.json")
println(s"""
           |JSON array:
           |  Elements: ${jsonMeta.elementCount}
           |  Has nested arrays: ${jsonMeta.hasNestedArrays}
           |  Has nested objects: ${jsonMeta.hasNestedObjects}
           |  Max depth: ${jsonMeta.maxDepth}
""".stripMargin)
```

### Format-Specific Metadata Methods

| Format       | Metadata Method               | Available Info                                    |
|--------------|-------------------------------|---------------------------------------------------|
| **Parquet**  | `getFileMetadata(path)`       | ✅ Rows, row groups, schema, compression         |
| **Delta**    | `getTableInfo(path)`          | ✅ Version, files, size, partitions              |
| **JsonArray**| `getMetadata(path)`           | ✅ Element count, nesting info, max depth        |

---

## Common Patterns

### High-Throughput File Indexing

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

val result = client.bulkFromFile(
  filePath = "/data/massive-dataset.parquet",
  format = Parquet,
  idKey = Some("id")
).futureValue

// Manual refresh once at the end
result.indices.foreach(client.refresh)
println(s"Indexed ${result.successCount} documents at ${result.metrics.throughput} docs/sec")
```

### Reliable File Indexing with Retry

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

val result = client.bulkFromFile(
  filePath = "/data/critical-data.json",
  format = JsonArray,
  idKey = Some("id")
).futureValue

if (result.failedCount > 0) {
  // Log all failures for investigation
  result.failedDocuments.foreach { failed =>
    logger.error(s"Critical failure: ${failed.id} - ${failed.error}")
    alerting.sendAlert(s"Failed to index critical document: ${failed.id}")
  }
}
```

### Time-Series Data from Files

```scala
// Load logs with automatic date-based partitioning
implicit val logOptions: BulkOptions = BulkOptions(
  defaultIndex = "logs",
  maxBulkSize = 2000,
  balance = 4
)

val result = client.bulkFromFile(
  filePath = "/data/application-logs.jsonl",
  format = Json,
  idKey = Some("id"),
  suffixDateKey = Some("timestamp"),
  suffixDatePattern = Some("yyyy-MM-dd")
).futureValue

// Creates indices: logs-2024-01-15, logs-2024-01-16, etc.
println(s"Indexed ${result.successCount} logs across ${result.indices.size} daily indices")
```

### Batch Processing Multiple Files

```scala
val files = Seq(
  "/data/products-2024-01.parquet",
  "/data/products-2024-02.parquet",
  "/data/products-2024-03.parquet"
)

val results = Future.sequence(
  files.map { file =>
    client.bulkFromFile(
      filePath = file,
      format = Parquet,
      idKey = Some("id")
    )
  }
)

results.foreach { resultList =>
  val totalSuccess = resultList.map(_.successCount).sum
  val totalFailed = resultList.map(_.failedCount).sum
  
  println(s"""
    |📊 Batch processing completed:
    |  - Files processed: ${files.size}
    |  - Total success: $totalSuccess
    |  - Total failed: $totalFailed
  """.stripMargin)
}
```

### Incremental Delta Lake Updates

```scala
// Track last processed version
var lastVersion: Long = 0

// Get current Delta table version
val tableInfo = DeltaFileSource.getTableInfo("/data/delta-products")

if (tableInfo.version > lastVersion) {
  println(s"Processing Delta updates from v$lastVersion to v${tableInfo.version}")
  
  val result = client.bulkFromFile(
    filePath = "/data/delta-products",
    format = Delta,
    idKey = Some("id"),
    update = Some(true)  // Upsert mode
  ).futureValue
  
  lastVersion = tableInfo.version
  
  println(s"Updated ${result.successCount} products from Delta Lake")
} else {
  println("No new Delta versions to process")
}
```

---

## Performance Optimization

### Tuning Parameters for File Operations

| Parameter        | Small Files (<1GB) | Medium Files (1-10GB) | Large Files (>10GB) |
|------------------|--------------------|-----------------------|---------------------|
| `maxBulkSize`    | 1000               | 5000                  | 10000               |
| `balance`        | 4                  | 8                     | 16                  |
| `disableRefresh` | false              | true                  | true                |
| `retryOnFailure` | true               | true                  | false               |

### Format-Specific Optimization

```scala
// JSON - Text parsing overhead
implicit val textOptions: BulkOptions = BulkOptions(
  maxBulkSize = 2000,
  balance = 4
)

// Parquet/Delta - Binary format, faster
implicit val binaryOptions: BulkOptions = BulkOptions(
  maxBulkSize = 10000,
  balance = 16
)
```

### Memory Considerations

```scala
// For large Parquet files with wide schemas
implicit val wideSchemaOptions: BulkOptions = BulkOptions(
  maxBulkSize = 500,   // Smaller batches
  balance = 2          // Less parallelism
)

// For narrow schemas (few columns)
implicit val narrowSchemaOptions: BulkOptions = BulkOptions(
  maxBulkSize = 10000,
  balance = 8
)
```

---

## Error Handling

### File-Specific Errors

```scala
val result = client.bulkFromFile(
  filePath = "/data/products.json",
  format = JsonArray,
  idKey = Some("id")
)

result.recover {
  case e: IllegalArgumentException if e.getMessage.contains("does not exist") =>
    logger.error(s"File not found: ${e.getMessage}")
    BulkResult.empty
  
  case e: IllegalArgumentException if e.getMessage.contains("not a JSON array") =>
    logger.error(s"Invalid JSON format: ${e.getMessage}")
    BulkResult.empty
  
  case e: java.io.IOException =>
    logger.error(s"IO error reading file: ${e.getMessage}")
    BulkResult.empty
  
  case e: Exception =>
    logger.error(s"Unexpected error: ${e.getMessage}", e)
    throw e
}
```

### Handling Partial Failures

```scala
val result = client.bulkFromFile(
  filePath = "/data/products.parquet",
  format = Parquet,
  idKey = Some("id")
).futureValue

if (result.failedCount > 0) {
  val failureRate = result.failedCount.toDouble / (result.successCount + result.failedCount)
  
  if (failureRate > 0.1) {
    // More than 10% failures - investigate
    logger.error(s"High failure rate: ${failureRate * 100}%")
    
    // Write failed documents for reprocessing
    val failedJson = result.failedDocuments.map(_.document).mkString("\n")
    java.nio.file.Files.write(
      java.nio.file.Paths.get("/data/failed-documents.jsonl"),
      failedJson.getBytes
    )
  }
}
```

---

## Testing File-Based Bulk Operations

### Test JSON Array with Nested Objects

```scala
"bulkFromFile" should "handle JSON array with nested objects" in {
  implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "test-json")
  
  val tempFile = java.io.File.createTempFile("test", ".json")
  tempFile.deleteOnExit()
  
  val writer = new java.io.PrintWriter(tempFile)
  writer.println("""[
    |  {"uuid": "A16", "name": "Person", "children": [
    |    {"name": "Child 1", "age": 10},
    |    {"name": "Child 2", "age": 12}
    |  ]}
    |]""".stripMargin)
  writer.close()
  
  val result = client.bulkFromFile(
    filePath = tempFile.getAbsolutePath,
    format = JsonArray,
    idKey = Some("uuid")
  ).futureValue
  
  result.successCount shouldBe 1
  result.failedCount shouldBe 0
  
  // Verify nested structure was preserved
  val doc = client.get("A16", "test-json").futureValue
  doc should include("children")
  doc should include("Child 1")
}
```

### Test Parquet File Loading

```scala
"bulkFromFile" should "load and index Parquet file" in {
  implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = "test-parquet")
  
  // Assume Parquet file exists
  val result = client.bulkFromFile(
    filePath = "/test-data/products.parquet",
    format = Parquet,
    idKey = Some("id")
  ).futureValue
  
  result.successCount should be > 0
  result.failedCount shouldBe 0
  result.metrics.throughput should be > 0.0
}
```

---

## Best Practices for File-Based Operations

**1. Choose the Right Format**

```scala
// ✅ Good - Use Parquet for large datasets
client.bulkFromFile("/data/big-dataset.parquet", Parquet, idKey = Some("id"))

// ❌ Avoid - Json for large datasets (slow parsing)
client.bulkFromFile("/data/big-dataset.json", Json, idKey = Some("id"))
```

**2. Validate Files Before Processing**

```scala
// ✅ Good - Check file metadata first
val parquetMeta = ParquetFileSource.getFileMetadata("/data/products.parquet")
if (parquetMeta.numRows > 1000000) {
  // Use optimized settings for large files
  implicit val options: BulkOptions = BulkOptions(maxBulkSize = 10000, balance = 16)
}
```

**3. Handle Large Files Efficiently**

```scala
// ✅ Good - Disable refresh for large files
implicit val options: BulkOptions = BulkOptions(
  disableRefresh = true,
  maxBulkSize = 10000
)

val result = client.bulkFromFile("/data/huge-file.parquet", Parquet, Some("id")).futureValue
result.indices.foreach(client.refresh)  // Manual refresh once
```

**4. Monitor File Processing**

```scala
// ✅ Good - Use callbacks for monitoring
val callbacks = BulkCallbacks(
  onBatchComplete = (size, metrics) =>
    println(s"Processed batch: $size docs at ${metrics.throughput} docs/sec"),
  onComplete = result =>
    println(s"File processing: ${result.successCount} success, ${result.failedCount} failed")
)

client.bulkFromFile("/data/file.parquet", Parquet, Some("id"), callbacks = callbacks)
```

**5. Use Delta Lake for Incremental Updates**

```scala
// ✅ Good - Track Delta versions
val currentVersion = DeltaFileSource.getTableInfo("/data/delta").version
// Process only new data...
```

**Example: Smart File Processing with Metadata**

```scala
def smartBulkFromFile(
  filePath: String,
  format: FileFormat,
  idKey: Option[String]
)(implicit system: ActorSystem, hadoopConf: Configuration): Future[BulkResult] = {
  
  // Auto-tune based on available metadata
  implicit val bulkOptions: BulkOptions = format match {
    case Parquet =>
      val meta = ParquetFileSource.getFileMetadata(filePath)
      println(s"📊 Parquet file: ${meta.numRows} rows")
      
      if (meta.numRows > 10000000) {
        // Very large file
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 10000,
          balance = 16,
          disableRefresh = true,
          logEvery = 100
        )
      } else if (meta.numRows > 1000000) {
        // Large file
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 5000,
          balance = 8,
          disableRefresh = true
        )
      } else {
        // Small file
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 1000,
          balance = 4
        )
      }
    
    case Delta =>
      val info = DeltaFileSource.getTableInfo(filePath)
      println(s"📊 Delta table: version ${info.version}, ${info.numFiles} files, ${info.sizeInBytes / 1024 / 1024}MB")
      
      if (info.sizeInBytes > 1024 * 1024 * 1024) {
        // >1GB
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 10000,
          balance = 16,
          disableRefresh = true
        )
      } else {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 5000,
          balance = 8
        )
      }
    
    case JsonArray =>
      val meta = JsonArrayFileSource.getMetadata(filePath)
      println(s"📊 JSON array: ${meta.elementCount} elements, nested=${meta.hasNestedArrays}")
      
      if (meta.elementCount > 100000) {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 5000,
          balance = 8,
          disableRefresh = true
        )
      } else {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 1000,
          balance = 4
        )
      }
    
    case _ =>
      val path = new Path(filePath)
      val fs = FileSystem.get(conf)
      val status = fs.getFileStatus(path)
      val sizeMB = status.getLen() / 1024 / 1024
      println(s"📊 $format file: ${sizeMB}MB")
      
      if (sizeMB > 100) {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 10000,
          balance = 16,
          disableRefresh = true
        )
      } else if (sizeMB > 10) {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 5000,
          balance = 8
        )
      } else {
        BulkOptions(
          defaultIndex = "data",
          maxBulkSize = 1000,
          balance = 4
        )
      }
  }
  
  client.bulkFromFile(filePath, format, idKey)
}

// Usage
smartBulkFromFile("/data/products.parquet", Parquet, Some("id"))
  .foreach { result =>
    println(s"✅ Indexed ${result.successCount} documents at ${result.metrics.throughput} docs/sec")
  }
```

---

## Summary

### Key Takeaways

1. **File-based bulk operations** support JSON, JSON Array, Parquet, and Delta Lake
2. **Parquet and Delta** offer best performance for large datasets
3. **JSON Array** handles complex nested structures correctly
4. **Automatic validation** ensures file integrity before processing
5. **Same configuration** applies to both in-memory and file-based operations
6. **Streaming architecture** enables processing of files larger than memory
7. **Delta Lake** provides versioning and ACID compliance

### Quick Reference

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

[Back to index](README.md) | [Next: Get Documents](get.md)