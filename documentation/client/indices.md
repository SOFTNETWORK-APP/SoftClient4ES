[Back to index](README.md)

# INDICES API

## Overview

The **IndicesApi** trait provides comprehensive index management functionality for Elasticsearch, including creation, deletion, lifecycle operations (open/close), reindexing, and existence checks.

**Features:**
- Robust error handling with `ElasticResult`
- Detailed logging for debugging
- Parameter validation (index names, JSON settings)
- Automatic refresh after reindexing
- Pre-configured default settings with n-gram analysis

**Dependencies:**
- Extends `ElasticClientHelpers` for validation and logging
- Requires `RefreshApi` for post-reindex refresh operations

---

## Configuration

### defaultSettings

Pre-configured index settings with n-gram tokenizer and analyzer for partial matching capabilities.

**Configuration Details:**

```scala
val defaultSettings: String = """
{
  "index": {
    "max_ngram_diff": "20",
    "mapping": {
      "total_fields": {
        "limit": "2000"
      }
    },
    "analysis": {
      "analyzer": {
        "ngram_analyzer": {
          "tokenizer": "ngram_tokenizer",
          "filter": ["lowercase", "asciifolding"]
        },
        "search_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "asciifolding"]
        }
      },
      "tokenizer": {
        "ngram_tokenizer": {
          "type": "ngram",
          "min_gram": 1,
          "max_gram": 20,
          "token_chars": ["letter", "digit"]
        }
      }
    }
  }
}
"""
```

**Features:**
- **N-gram tokenizer:** Supports partial matching (1-20 characters)
- **ASCII folding:** Normalizes accented characters
- **Field limit:** Allows up to 2000 fields per index
- **Case insensitive:** Lowercase filter applied

---

## 🔧 Index State Detection and Automatic State Restoration

SoftClient4ES ensures that operations requiring an **open index** (such as `deleteByQuery` and `truncateIndex`) behave safely and consistently, even when the target index is **closed**.

Elasticsearch does **not** allow `_delete_by_query` on a closed index (including ES 8.x and ES 9.x).  
To guarantee correct behavior, SoftClient4ES automatically:

1. Detects whether the index is open or closed
2. Opens the index if needed
3. Executes the operation
4. Restores the original state (re‑closes the index if it was closed)

This mechanism is fully transparent to the user.

---

## Public Methods

### createIndex

Creates a new index with specified settings.

**Signature:**

```scala
def createIndex(
  index: String,
  settings: String = defaultSettings,
  mappings: Option[String] = None,
  aliases: Seq[TableAlias] = Seq.empty
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index to create
- `settings` - JSON settings for the index (defaults to `defaultSettings`)
- `mappings` - Optional JSON mappings for the index
- `aliases` - Optional list of aliases to assign to the index

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if created, `false` otherwise
- `ElasticFailure` with error details (400 for validation, other codes from ES)

**Validation:**
- Index name format validation
- JSON settings syntax validation
- JSON mappings syntax validation if provided
- Alias name format validation

**Examples:**

```scala
// Create with default settings
client.createIndex("products") match {
  case ElasticSuccess(true) => println("Index created")
  case ElasticSuccess(false) => println("Index already exists")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Create with custom settings
val customSettings = """
{
  "index": {
    "number_of_shards": 3,
    "number_of_replicas": 2,
    "refresh_interval": "30s"
  }
}
"""
client.createIndex("high-volume-index", customSettings)

// Create with mappings
val settingsWithMappings = """
{
  "settings": {
    "number_of_shards": 1
  },
  "mappings": {
    "properties": {
      "title": { "type": "text" },
      "price": { "type": "double" },
      "created_at": { "type": "date" }
    }
  }
}
"""
client.createIndex("catalog", settingsWithMappings)

// Monadic creation with error handling
for {
  created <- client.createIndex("users")
  _ <- if (created) ElasticResult.success(())
       else ElasticResult.failure("Index not created")
  indexed <- client.index("users", userData)
} yield indexed
```

---

### getIndex

Gets an existing index.

**Signature:**

```scala
def getIndex(index: String): ElasticResult[Option[Index]]
```

**Parameters:**
- `index` - Name of the index to get

**Returns:**
- `ElasticSuccess[Option[Index]]` with index configuration if index found, None otherwise
- `ElasticFailure` with error details

---

### deleteIndex

Deletes an existing index.

**Signature:**

```scala
def deleteIndex(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index to delete

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if deleted, `false` otherwise
- `ElasticFailure` with error details

**Examples:**

```scala
// Simple deletion
client.deleteIndex("old-index")

// Safe deletion with existence check
for {
  exists <- client.indexExists("temp-index")
  deleted <- if (exists) client.deleteIndex("temp-index")
             else ElasticResult.success(false)
} yield deleted

// Cleanup multiple indices
val oldIndices = List("logs-2023-01", "logs-2023-02", "logs-2023-03")
oldIndices.foreach { index =>
  client.deleteIndex(index) match {
    case ElasticSuccess(_) => println(s"Deleted $index")
    case ElasticFailure(e) => println(s"Failed to delete $index: ${e.message}")
  }
}
```

⚠️ **Warning:** Deletion is irreversible. All data in the index will be permanently lost.

---

### closeIndex

Closes an index, blocking read/write operations while preserving data on disk.

**Signature:**

```scala
def closeIndex(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index to close

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if closed, `false` otherwise
- `ElasticFailure` with error details

**Use Cases:**
- Reduce memory/CPU usage for inactive indices
- Perform maintenance operations
- Prepare for backup or snapshot

**Examples:**

```scala
// Close inactive index
client.closeIndex("archive-2023")

// Close multiple seasonal indices
val winterIndices = List("sales-dec", "sales-jan", "sales-feb")
winterIndices.foreach(client.closeIndex)

// Close and verify
for {
  closed <- client.closeIndex("old-data")
  exists <- client.indexExists("old-data")
} yield (closed, exists) // (true, true) - closed but still exists
```

---

### openIndex

Opens a previously closed index, making it available for read/write operations.

**Signature:**

```scala
def openIndex(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index to open

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if opened, `false` otherwise
- `ElasticFailure` with error details

**Examples:**

```scala
// Reactivate closed index
client.openIndex("archive-2023")

// Open and search
for {
  opened <- client.openIndex("historical-data")
  results <- client.search("historical-data", searchQuery)
} yield results

// Conditional opening
def ensureIndexOpen(index: String): ElasticResult[Boolean] = {
  client.indexExists(index).flatMap {
    case true => client.openIndex(index)
    case false => ElasticResult.failure(s"Index $index does not exist")
  }
}
```

---

### reindex

Copies documents from a source index to a target index with optional refresh.

**Signature:**

```scala
def reindex(
  sourceIndex: String,
  targetIndex: String,
  refresh: Boolean = true
): ElasticResult[(Boolean, Option[Long])]
```

**Parameters:**
- `sourceIndex` - Name of the source index
- `targetIndex` - Name of the target index (must already exist)
- `refresh` - Whether to refresh target index after reindexing (default: `true`)

**Returns:**
- `ElasticSuccess[(Boolean, Option[Long])]` with success flag and document count
- `ElasticFailure` with error details

**Validation:**
- Both indices must have valid names
- Source and target must be different
- Both indices must exist (404 if not found)

**Examples:**

```scala
// Basic reindex
client.reindex("products-v1", "products-v2") match {
  case ElasticSuccess((true, Some(count))) => 
    println(s"Reindexed $count documents")
  case ElasticSuccess((true, None)) => 
    println("Reindex succeeded (count unavailable)")
  case ElasticSuccess((false, _)) => 
    println("Reindex failed")
  case ElasticFailure(e) => 
    println(s"Error: ${e.message}")
}

// Reindex without immediate refresh (better performance)
client.reindex("logs-old", "logs-new", refresh = false)

// Complete migration workflow
def migrateIndex(oldIndex: String, newIndex: String): ElasticResult[Unit] = {
  for {
    // Create new index with updated settings
    _ <- client.createIndex(newIndex, improvedSettings)
    
    // Copy all documents
    (success, count) <- client.reindex(oldIndex, newIndex)
    
    // Verify count matches
    _ <- if (success) ElasticResult.success(())
         else ElasticResult.failure("Reindex failed")
    
    // Delete old index
    _ <- client.deleteIndex(oldIndex)
  } yield ()
}

// Reindex with error recovery
client.reindex("source", "target") match {
  case ElasticSuccess((true, Some(count))) =>
    println(s"✅ Successfully reindexed $count documents")
  case ElasticSuccess((true, None)) =>
    println("⚠️ Reindex succeeded but document count unavailable")
  case ElasticSuccess((false, _)) =>
    println("❌ Reindex operation failed")
    // Attempt cleanup
    client.deleteIndex("target")
  case ElasticFailure(error) if error.statusCode.contains(404) =>
    println(s"❌ Index not found: ${error.message}")
  case ElasticFailure(error) =>
    println(s"❌ Reindex error: ${error.message}")
}

// Batch reindexing with progress tracking
val migrations = List(
  ("users-v1", "users-v2"),
  ("orders-v1", "orders-v2"),
  ("products-v1", "products-v2")
)

migrations.foreach { case (source, target) =>
  client.reindex(source, target) match {
    case ElasticSuccess((true, Some(count))) =>
      println(s"✅ $source -> $target: $count docs")
    case ElasticFailure(e) =>
      println(s"❌ $source -> $target: ${e.message}")
  }
}
```

**Notes:**
- Target index must be created before reindexing
- Reindexing does not copy index settings or mappings
- For large indices, consider using `refresh = false` and manually refresh later
- The operation is synchronous and may take time for large datasets

---

### indexExists

Checks whether an index exists in the cluster.

**Signature:**

```scala
def indexExists(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index to check

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if exists, `false` otherwise
- `ElasticFailure` with error details

**Examples:**

```scala
// Simple existence check
client.indexExists("products") match {
  case ElasticSuccess(true) => println("Index exists")
  case ElasticSuccess(false) => println("Index does not exist")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Conditional creation
def createIfNotExists(index: String, settings: String): ElasticResult[Boolean] = {
  client.indexExists(index).flatMap {
    case false => client.createIndex(index, settings)
    case true => ElasticResult.success(false) // Already exists
  }
}

// Safe deletion
def deleteIfExists(index: String): ElasticResult[Boolean] = {
  for {
    exists <- client.indexExists(index)
    deleted <- if (exists) client.deleteIndex(index)
               else ElasticResult.success(false)
  } yield deleted
}

// Validate multiple indices
val requiredIndices = List("users", "products", "orders")
val existenceChecks = requiredIndices.map { index =>
  index -> client.indexExists(index)
}

existenceChecks.foreach {
  case (index, ElasticSuccess(true)) => println(s"✅ $index exists")
  case (index, ElasticSuccess(false)) => println(s"❌ $index missing")
  case (index, ElasticFailure(e)) => println(s"⚠️ $index check failed: ${e.message}")
}
```

---

### isIndexClosed

Checks whether an index is currently **closed**.

**Signature:**

```scala
def isIndexClosed(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` – Name of the index to inspect

**Returns:**
- `ElasticSuccess(true)` if the index is closed
- `ElasticSuccess(false)` if the index is open
- `ElasticFailure` if the index does not exist or the request fails

**Behavior:**
- Uses the Elasticsearch `_cat/indices` API internally
- Supported across Jest (ES6), REST HL (ES6/7), Java API Client (ES8/ES9)

**Examples:**

```scala
client.isIndexClosed("archive-2023") match {
  case ElasticSuccess(true)  => println("Index is closed")
  case ElasticSuccess(false) => println("Index is open")
  case ElasticFailure(err)   => println(s"Error: ${err.message}")
}
```

---

### Automatic Index Opening and State Restoration

Some operations require the index to be **open**.  
SoftClient4ES automatically handles this through an internal helper:

- Detect initial state (`open` or `closed`)
- Open the index if needed
- Execute the operation
- Restore the original state

This ensures:

- **Safety** (no accidental state changes)
- **Idempotence** (index ends in the same state it started)
- **Compatibility** with all Elasticsearch versions

This logic is used internally by:

- `deleteByQuery`
- `truncateIndex`

---

### deleteByQuery

Deletes documents from an index using either a JSON query or a SQL `DELETE`/`SELECT` expression.

If the index is **closed**, SoftClient4ES will:

1. Detect that the index is closed
2. Open it temporarily
3. Execute the delete‑by‑query
4. Re‑close it afterward

**Signature:**

```scala
def deleteByQuery(
  index: String,
  query: String,
  refresh: Boolean = true
): ElasticResult[Long]
```

**Parameters:**
- `index` – Name of the index
- `query` – JSON or SQL delete expression
- `refresh` – Whether to refresh the index after deletion

**Returns:**
- `ElasticSuccess[Long]` – number of deleted documents
- `ElasticFailure` – error details

**Behavior:**
- Validates index name
- Parses SQL into JSON when needed
- Ensures index exists
- Automatically opens closed indices
- Restores original state after execution
- Uses `_delete_by_query` internally

**Examples:**

```scala
// JSON delete
client.deleteByQuery(
  "users",
  """{"query": {"term": {"active": false}}}"""
)

// SQL delete
client.deleteByQuery(
  "orders",
  "DELETE FROM orders WHERE status = 'cancelled'"
)

// SQL select (equivalent to delete)
client.deleteByQuery(
  "sessions",
  "SELECT * FROM sessions WHERE expired = true"
)
```

---

### truncateIndex

Deletes **all documents** from an index while preserving its mappings, settings, and aliases.

This is implemented as:

```scala
deleteByQuery(index, """{"query": {"match_all": {}}}""")
```

If the index is closed, SoftClient4ES will automatically:

- Open it
- Execute the truncate
- Restore the closed state

**Signature:**

```scala
def truncateIndex(index: String): ElasticResult[Long]
```

**Parameters:**
- `index` – Name of the index to truncate

**Returns:**
- `ElasticSuccess[Long]` – number of deleted documents
- `ElasticFailure` – error details

**Examples:**

```scala
// Remove all documents
client.truncateIndex("logs-2024")

// Safe truncate with existence check
for {
  exists  <- client.indexExists("cache")
  deleted <- if (exists) client.truncateIndex("cache")
             else ElasticResult.success(0L)
} yield deleted
```

**Notes:**
- Index structure is preserved
- Operation is irreversible
- Works even if the index is initially closed

---

## Implementation Requirements

The following methods must be implemented by each client-specific trait:

### executeCreateIndex

```scala
private[client] def executeCreateIndex(
  index: String,
  settings: String
): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeCreateIndex(
  index: String,
  settings: String
): ElasticResult[Boolean] = {
  executeRestAction[CreateIndexResponse, Boolean](
    operation = "createIndex",
    index = Some(index)
  )(
    action = client.indices().create(
      new CreateIndexRequest(index).source(settings, XContentType.JSON),
      RequestOptions.DEFAULT
    )
  )(
    transformer = _.isAcknowledged
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeCreateIndex(
  index: String,
  settings: String
): ElasticResult[Boolean] = {
  executeJavaAction[CreateIndexResponse, Boolean](
    operation = "createIndex",
    index = Some(index)
  )(
    action = {
      val request = CreateIndexRequest.of(b => 
        b.index(index).withJson(new StringReader(settings))
      )
      client.indices().create(request)
    }
  )(
    transformer = _.acknowledged()
  )
}
```

---

### executeDeleteIndex

```scala
private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
  executeRestAction[AcknowledgedResponse, Boolean](
    operation = "deleteIndex",
    index = Some(index)
  )(
    action = client.indices().delete(
      new DeleteIndexRequest(index),
      RequestOptions.DEFAULT
    )
  )(
    transformer = _.isAcknowledged
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
  executeJavaAction[DeleteIndexResponse, Boolean](
    operation = "deleteIndex",
    index = Some(index)
  )(
    action = client.indices().delete(
      DeleteIndexRequest.of(b => b.index(index))
    )
  )(
    transformer = _.acknowledged()
  )
}
```

---

### executeCloseIndex

```scala
private[client] def executeCloseIndex(index: String): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
  executeRestAction[CloseIndexResponse, Boolean](
    operation = "closeIndex",
    index = Some(index)
  )(
    action = client.indices().close(
      new CloseIndexRequest(index),
      RequestOptions.DEFAULT
    )
  )(
    transformer = _.isAcknowledged
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
  executeJavaAction[CloseIndexResponse, Boolean](
    operation = "closeIndex",
    index = Some(index)
  )(
    action = client.indices().close(
      CloseIndexRequest.of(b => b.index(index))
    )
  )(
    transformer = _.acknowledged()
  )
}
```

---

### executeOpenIndex

```scala
private[client] def executeOpenIndex(index: String): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
  executeRestAction[OpenIndexResponse, Boolean](
    operation = "openIndex",
    index = Some(index)
  )(
    action = client.indices().open(
      new OpenIndexRequest(index),
      RequestOptions.DEFAULT
    )
  )(
    transformer = _.isAcknowledged
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
  executeJavaAction[OpenIndexResponse, Boolean](
    operation = "openIndex",
    index = Some(index)
  )(
    action = client.indices().open(
      OpenIndexRequest.of(b => b.index(index))
    )
  )(
    transformer = _.acknowledged()
  )
}
```

---

### executeReindex

```scala
private[client] def executeReindex(
  sourceIndex: String,
  targetIndex: String,
  refresh: Boolean
): ElasticResult[(Boolean, Option[Long])]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeReindex(
  sourceIndex: String,
  targetIndex: String,
  refresh: Boolean
): ElasticResult[(Boolean, Option[Long])] = {
  executeRestAction[BulkByScrollResponse, (Boolean, Option[Long])](
    operation = "reindex",
    index = Some(targetIndex)
  )(
    action = {
      val request = new ReindexRequest()
        .setSourceIndices(sourceIndex)
        .setDestIndex(targetIndex)
        .setRefresh(refresh)
      client.reindex(request, RequestOptions.DEFAULT)
    }
  )(
    transformer = resp => {
      val success = resp.getBulkFailures.isEmpty
      val count = Some(resp.getTotal)
      (success, count)
    }
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeReindex(
  sourceIndex: String,
  targetIndex: String,
  refresh: Boolean
): ElasticResult[(Boolean, Option[Long])] = {
  executeJavaAction[ReindexResponse, (Boolean, Option[Long])](
    operation = "reindex",
    index = Some(targetIndex)
  )(
    action = {
      val request = ReindexRequest.of(b => b
        .source(s => s.index(sourceIndex))
        .dest(d => d.index(targetIndex))
        .refresh(refresh)
      )
      client.reindex(request)
    }
  )(
    transformer = resp => {
      val success = resp.failures().isEmpty
      val count = Some(resp.total())
      (success, count)
    }
  )
}
```

---

### executeIndexExists

```scala
private[client] def executeIndexExists(index: String): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
  executeRestAction[java.lang.Boolean, Boolean](
    operation = "indexExists",
    index = Some(index)
  )(
    action = client.indices().exists(
      new GetIndexRequest(index),
      RequestOptions.DEFAULT
    )
  )(
    transformer = exists => exists.booleanValue()
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
  executeJavaAction[BooleanResponse, Boolean](
    operation = "indexExists",
    index = Some(index)
  )(
    action = client.indices().exists(
      ExistsRequest.of(b => b.index(index))
    )
  )(
    transformer = _.value()
  )
}
```

---

## Error Handling

**Invalid Index Name (400):**

```scala
client.createIndex("INVALID INDEX") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.operation.contains("createIndex"))
}
```

**Invalid JSON Settings (400):**

```scala
client.createIndex("test", "{ invalid json }") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("Invalid settings"))
    assert(error.statusCode.contains(400))
}
```

**Index Not Found (404):**

```scala
client.reindex("missing-source", "target") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(404))
    assert(error.message.contains("does not exist"))
}
```

**Same Source and Target:**

```scala
client.reindex("products", "products") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("cannot be the same"))
    assert(error.statusCode.contains(400))
}
```

---

## Best Practices

**Index Lifecycle Management:**

```scala
// Create index with appropriate settings
val settings = if (isProduction) productionSettings else defaultSettings
client.createIndex("app-index", settings)

// Regular maintenance
def archiveOldData(activeIndex: String, archiveIndex: String): Unit = {
  for {
    _ <- client.createIndex(archiveIndex)
    (success, count) <- client.reindex(activeIndex, archiveIndex)
    _ <- if (success) client.closeIndex(archiveIndex)
         else ElasticResult.failure("Archival failed")
  } yield count
}
```

**Safe Index Operations:**

```scala
// Always check existence before operations
def safeCreateIndex(index: String): ElasticResult[Boolean] = {
  client.indexExists(index).flatMap {
    case true => 
      println(s"Index $index already exists")
      ElasticResult.success(false)
    case false => 
      client.createIndex(index)
  }
}
```

**Performance Optimization:**

```scala
// For large reindex operations, disable refresh
client.reindex("large-source", "large-target", refresh = false)
  .flatMap { case (success, count) =>
    if (success) {
      // Manual refresh after completion
      Thread.sleep(5000) // Allow time for indexing
      client.refresh("large-target")
    } else {
      ElasticResult.failure("Reindex failed")
    }
  }
```

---

[Back to index](README.md) | [Next: Settings Management](settings.md)