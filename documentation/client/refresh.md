[Back to index](README.md)

# REFRESH API

## Overview

The **RefreshApi** trait provides functionality to refresh Elasticsearch indices, making all recently indexed documents immediately searchable. This is useful for testing, real-time search requirements, or after bulk indexing operations.

**Dependencies:** Extends `ElasticClientHelpers` for validation and logging utilities.

---

## Public Methods

### refresh

Refreshes an index to ensure all documents are indexed and immediately searchable.

**Signature:**

```scala
def refresh(index: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The name of the index to refresh

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if refresh succeeded, `false` otherwise
- `ElasticFailure` with error details if operation fails

**Behavior:**
- Validates index name before execution (returns 400 error if invalid)
- Logs debug message before refresh attempt
- Logs success with ✅ or failure with ❌
- Enriches validation errors with operation context (index name, status code 400, operation "refresh")

**Examples:**

```scala
// Basic refresh
val result = client.refresh("my-index")
result match {
  case ElasticSuccess(true) => println("Index refreshed")
  case ElasticSuccess(false) => println("Refresh not performed")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Monadic chaining
for {
  _ <- client.index("users", user)
  refreshed <- client.refresh("users")
  result <- client.search(query)
} yield result

// Multiple indices refresh
val indices = List("index1", "index2", "index3")
val results = indices.map(client.refresh)
results.foreach {
  case ElasticSuccess(_) => println("OK")
  case ElasticFailure(e) => println(s"Failed: ${e.message}")
}

// Conditional refresh for testing
def refreshIfTest(index: String): ElasticResult[Boolean] = {
  if (sys.env.get("ENV").contains("test")) {
    client.refresh(index)
  } else {
    ElasticResult.success(false) // Skip in production
  }
}
```

**Common Use Cases:**

- **Testing:** Ensure documents are searchable immediately after indexing
- **Bulk Operations:** Refresh after large batch imports
- **Real-time Search:** Force visibility of recent changes
- **Data Validation:** Verify indexing before downstream operations

**Performance Considerations:**

⚠️ Refreshing is expensive and should be used sparingly in production. Elasticsearch automatically refreshes indices every second by default.

```scala
// ❌ Bad - refresh after each document
documents.foreach { doc =>
  client.index("products", id, doc)
  client.refresh("products") // Too frequent!
}

// ✅ Good - refresh once after bulk operation
client.bulk(documents)
client.refresh("products")
```

---

## Implementation Requirements

### executeRefresh

Must be implemented by each client-specific trait.

**Signature:**

```scala
private[client] def executeRefresh(index: String): ElasticResult[Boolean]
```

**Implementation Examples:**

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
  executeRestAction[RefreshResponse, Boolean](
    operation = "refresh",
    index = Some(index)
  )(
  action = client.indices().refresh(
    new RefreshRequest(index),
    RequestOptions.DEFAULT
    )
  )(
    transformer = resp => resp.getStatus == RestStatus.OK
  )
}
```

**Java Client (ES 8-9):**

```scala
private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
  executeJavaAction[RefreshResponse, Boolean](
    operation = "refresh",
    index = Some(index)
  )(
    action = client.indices().refresh(
    new RefreshRequest.Builder().index(index).build()
  )
  )(
    transformer = resp => !resp.shards().failures().isEmpty
  )
}
```

**Jest Client (ES 5-6):**

```scala
private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
  executeJestAction[JestResult, Boolean](
    operation = "refresh",
    index = Some(index)
  )(
    action = new Refresh.Builder().addIndex(index).build()
  )(
    transformer = _.isSucceeded
  )
}
```

---

## Error Handling

**Invalid Index Name:**

```scala
client.refresh("") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.operation.contains("refresh"))
    assert(error.index.isDefined)
}
```

**Index Not Found:**

```scala
client.refresh("non-existent-index") match {
  case ElasticFailure(error) =>
    // Typically 404 error from Elasticsearch
    println(s"Index not found: ${error.message}")
}
```

---

[Back to index](README.md) | [Next: Indices Management](indices.md)