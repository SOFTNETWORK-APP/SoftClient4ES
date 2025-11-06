[Back to index](README.md)

# SETTINGS API

## Overview

The **SettingsApi** trait provides functionality to manage Elasticsearch index settings dynamically, including refresh intervals, replica counts, and custom settings updates.

**Features:**
- Dynamic settings updates with automatic index close/open
- Refresh interval toggling for bulk operations optimization
- Replica management for availability control
- Settings inspection and retrieval
- Comprehensive validation and error handling

**Dependencies:**
- Requires `IndicesApi` for index lifecycle operations (close/open)

---

## Public Methods

### toggleRefresh

Enables or disables the automatic refresh interval for an index. Disabling refresh improves bulk indexing performance.

**Signature:**

```scala
def toggleRefresh(index: String, enable: Boolean): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index
- `enable` - `true` to enable refresh (1s interval), `false` to disable (-1)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if settings updated successfully
- `ElasticFailure` with error details

**Behavior:**
- When enabled: Sets refresh interval to "1s" (default behavior)
- When disabled: Sets refresh interval to "-1" (no automatic refresh)
- Automatically closes and reopens the index

**Examples:**

```scala
// Disable refresh before bulk indexing
client.toggleRefresh("products", enable = false) match {
  case ElasticSuccess(true) => 
    println("Refresh disabled for bulk operation")
  case ElasticFailure(e) => 
    println(s"Failed to disable refresh: ${e.message}")
}

// Bulk indexing workflow
for {
  _ <- client.toggleRefresh("products", enable = false)
  _ <- client.bulkIndex(largeDataset)
  _ <- client.toggleRefresh("products", enable = true)
  _ <- client.refresh("products")
} yield "Bulk indexing complete"

// Performance optimization pattern
def bulkIndexWithOptimization[T](
  index: String,
  documents: Seq[T]
): ElasticResult[Unit] = {
  for {
    _ <- client.toggleRefresh(index, enable = false)
    _ <- client.bulkIndex(documents)
    _ <- client.toggleRefresh(index, enable = true)
    _ <- client.refresh(index)
  } yield ()
}
```

**Performance Impact:**
- **Disabled:** 2-5x faster bulk indexing, documents not immediately searchable
- **Enabled:** Normal indexing speed, documents searchable within 1 second

---

### setReplicas

Updates the number of replica shards for an index.

**Signature:**

```scala
def setReplicas(index: String, replicas: Int): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index
- `replicas` - Number of replica shards (0 or more)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if updated successfully
- `ElasticFailure` with error details

**Behavior:**
- Automatically closes and reopens the index
- Changes take effect immediately after reopening

**Examples:**

```scala
// Increase replicas for high availability
client.setReplicas("critical-data", 2)

// Remove replicas for single-node cluster
client.setReplicas("dev-index", 0)

// Dynamic replica management based on cluster size
def adjustReplicas(index: String, nodeCount: Int): ElasticResult[Boolean] = {
  val optimalReplicas = Math.max(0, nodeCount - 1)
  client.setReplicas(index, optimalReplicas)
}

// Disaster recovery: increase replicas
for {
  _ <- client.setReplicas("users", 3)
  _ <- client.setReplicas("orders", 3)
  _ <- client.setReplicas("products", 3)
} yield "Replicas increased for all indices"

// Temporary replica reduction for maintenance
def maintenanceMode(index: String): ElasticResult[Unit] = {
  for {
    currentSettings <- client.loadSettings(index)
    _ <- client.setReplicas(index, 0)
    _ <- performMaintenance(index)
    _ <- client.setReplicas(index, 2) // Restore
  } yield ()
}
```

**Replica Guidelines:**
- **0 replicas:** Development, single-node clusters (no redundancy)
- **1 replica:** Production minimum (survives 1 node failure)
- **2+ replicas:** High availability (survives multiple node failures)

---

### updateSettings

Updates arbitrary index settings with custom JSON configuration.

**Signature:**

```scala
def updateSettings(
  index: String,
  settings: String = defaultSettings
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - Name of the index
- `settings` - JSON string with settings to update (defaults to `defaultSettings`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if updated successfully
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation
- JSON syntax validation

**Behavior:**
- Closes the index before applying settings
- Opens the index after successful update
- If closing fails, settings are not applied
- If update fails, index remains closed

**Examples:**

```scala
// Update multiple settings at once
val settings = """
{
  "index": {
    "refresh_interval": "30s",
    "number_of_replicas": 2,
    "max_result_window": 20000
  }
}
"""
client.updateSettings("products", settings)

// Update analysis settings
val analysisSettings = """
{
  "index": {
    "analysis": {
      "analyzer": {
        "custom_analyzer": {
          "type": "custom",
          "tokenizer": "standard",
          "filter": ["lowercase", "stop"]
        }
      }
    }
  }
}
"""
client.updateSettings("search-index", analysisSettings)

// Adjust performance settings
val performanceSettings = """
{
  "index": {
    "refresh_interval": "60s",
    "translog.durability": "async",
    "translog.sync_interval": "30s"
  }
}
"""
client.updateSettings("logs", performanceSettings)

// Error handling
client.updateSettings("my-index", invalidJson) match {
  case ElasticSuccess(true) => 
    println("Settings updated")
  case ElasticSuccess(false) => 
    println("Update failed (index may be closed)")
  case ElasticFailure(error) if error.statusCode.contains(400) =>
    println(s"Validation error: ${error.message}")
  case ElasticFailure(error) =>
    println(s"Update error: ${error.message}")
}

// Monadic settings update
for {
  exists <- client.indexExists("my-index")
  updated <- if (exists) client.updateSettings("my-index", newSettings)
             else ElasticResult.failure("Index does not exist")
} yield updated
```

**Common Settings:**

```scala
// Refresh interval
"""{"index": {"refresh_interval": "30s"}}"""

// Replicas
"""{"index": {"number_of_replicas": 2}}"""

// Max result window
"""{"index": {"max_result_window": 50000}}"""

// Translog settings
"""{"index": {"translog.durability": "async"}}"""

// Merge settings
"""{"index": {"merge.scheduler.max_thread_count": 1}}"""
```

⚠️ **Warning:** Some settings require the index to be closed. The method automatically handles this but the index will be temporarily unavailable.

---

### loadSettings

Retrieves the current settings of an index as a JSON string.

**Signature:**

```scala
def loadSettings(index: String): ElasticResult[String]
```

**Parameters:**
- `index` - Name of the index

**Returns:**
- `ElasticSuccess[String]` containing index settings as JSON
- `ElasticFailure` with error details (400 for invalid index name, 404 if not found)

**Behavior:**
- Retrieves full settings from Elasticsearch
- Extracts only the `index` settings object
- Validates JSON response structure

**Examples:**

```scala
// Load and inspect settings
client.loadSettings("products") match {
  case ElasticSuccess(json) => 
    println(s"Current settings: $json")
  case ElasticFailure(e) => 
    println(s"Failed to load: ${e.message}")
}

// Parse settings for specific values
client.loadSettings("my-index").map { json =>
  val settings = parse(json)
  val replicas = (settings \ "number_of_replicas").extract[Int]
  val refreshInterval = (settings \ "refresh_interval").extract[String]
  (replicas, refreshInterval)
}

// Backup settings before modification
def updateWithBackup(
  index: String,
  newSettings: String
): ElasticResult[Boolean] = {
  for {
    backup <- client.loadSettings(index)
    _ = saveToFile(s"$index-settings-backup.json", backup)
    updated <- client.updateSettings(index, newSettings)
  } yield updated
}

// Compare settings across indices
val indices = List("index1", "index2", "index3")
val allSettings = indices.map { index =>
  index -> client.loadSettings(index)
}

allSettings.foreach {
  case (index, ElasticSuccess(settings)) =>
    println(s"$index: $settings")
  case (index, ElasticFailure(e)) =>
    println(s"$index: Error - ${e.message}")
}

// Extract specific setting
def getRefreshInterval(index: String): ElasticResult[String] = {
  client.loadSettings(index).flatMap { json =>
    ElasticResult.attempt {
      val settings = parse(json)
      (settings \ "refresh_interval").extract[String]
    }
  }
}

// Validate settings match expected configuration
def validateSettings(
  index: String,
  expectedReplicas: Int
): ElasticResult[Boolean] = {
  client.loadSettings(index).map { json =>
    val settings = parse(json)
    val actualReplicas = (settings \ "number_of_replicas").extract[Int]
    actualReplicas == expectedReplicas
  }
}
```

**Response Format:**

The returned JSON contains index-level settings only:

```scala
{
  "number_of_shards": "1",
  "number_of_replicas": "1",
  "refresh_interval": "1s",
  "max_result_window": "10000",
  "provided_name": "my-index",
  "creation_date": "1699564800000",
  "uuid": "abc123...",
  "version": {
    "created": "8110399"
  }
}
```

---

## Implementation Requirements

### executeUpdateSettings

```scala
private[client] def executeUpdateSettings(
  index: String,
  settings: String
): ElasticResult[Boolean]
```

**Implementation Example:**

```scala
private[client] def executeUpdateSettings(
  index: String,
  settings: String
): ElasticResult[Boolean] = {
  executeRestAction[AcknowledgedResponse, Boolean](
    operation = "updateSettings",
    index = Some(index)
  )(
    action = client.indices().putSettings(
      new UpdateSettingsRequest(index).settings(settings, XContentType.JSON),
      RequestOptions.DEFAULT
    )
  )(
    transformer = _.isAcknowledged
  )
}
```

---

### executeLoadSettings

```scala
private[client] def executeLoadSettings(index: String): ElasticResult[String]
```

**Implementation Example:**

```scala
private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
  executeRestAction[GetSettingsResponse, String](
    operation = "loadSettings",
    index = Some(index)
  )(
    action = client.indices().getSettings(
      new GetSettingsRequest().indices(index),
      RequestOptions.DEFAULT
    )
  )(
    transformer = resp => {
      val settings = resp.getIndexToSettings
      settings.toString // Returns full JSON response
    }
  )
}
```

---

## Error Handling

**Invalid Index Name:**

```scala
client.updateSettings("", newSettings) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.operation.contains("updateSettings"))
}
```

**Invalid JSON Settings:**

```scala
client.updateSettings("my-index", "{ invalid }") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("Invalid settings"))
}
```

**Index Not Found:**

```scala
client.loadSettings("non-existent") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("not found"))
}
```

**Failed to Close Index:**

```scala
// If index cannot be closed, settings update is aborted
client.updateSettings("locked-index", settings) match {
  case ElasticFailure(error) =>
    println(s"Cannot update: ${error.message}")
    // Index remains in original state
}
```

---

## Best Practices

**Bulk Indexing Optimization:**

```scala
def optimizedBulkIndex[T](
  index: String,
  documents: Seq[T]
): ElasticResult[Unit] = {
  for {
    // Disable refresh
    _ <- client.toggleRefresh(index, enable = false)
    
    // Reduce replicas temporarily
    _ <- client.setReplicas(index, 0)
    
    // Perform bulk indexing
    _ <- client.bulkIndex(documents)
    
    // Restore replicas
    _ <- client.setReplicas(index, 1)
    
    // Re-enable refresh
    _ <- client.toggleRefresh(index, enable = true)
    
    // Force refresh
    _ <- client.refresh(index)
  } yield ()
}
```

**Settings Backup and Restore:**

```scala
case class SettingsBackup(index: String, settings: String, timestamp: Long)

def backupSettings(index: String): ElasticResult[SettingsBackup] = {
  client.loadSettings(index).map { settings =>
    SettingsBackup(index, settings, System.currentTimeMillis())
  }
}

def restoreSettings(backup: SettingsBackup): ElasticResult[Boolean] = {
  client.updateSettings(backup.index, backup.settings)
}
```

**Gradual Settings Changes:**

```scala
// Gradually increase replicas across cluster
def scaleReplicas(indices: List[String], targetReplicas: Int): Unit = {
  indices.foreach { index =>
    client.setReplicas(index, targetReplicas)
    Thread.sleep(5000) // Allow cluster to rebalance
  }
}
```

---

## Workflow Examples

**Complete Index Reconfiguration:**

```scala
def reconfigureIndex(index: String): ElasticResult[Unit] = {
  for {
    // Backup current settings
    backup <- client.loadSettings(index)
    _ = println(s"Backed up settings: $backup")
    
    // Apply new settings
    newSettings = """
    {
      "index": {
        "refresh_interval": "30s",
        "number_of_replicas": 2,
        "max_result_window": 20000
      }
    }
    """
    _ <- client.updateSettings(index, newSettings)
    
    // Verify changes
    updated <- client.loadSettings(index)
    _ = println(s"New settings: $updated")
  } yield ()
}
```

**Performance Tuning:**

```scala
def tuneForWrites(index: String): ElasticResult[Unit] = {
  val writeOptimized = """
  {
    "index": {
      "refresh_interval": "30s",
      "number_of_replicas": 0,
      "translog.durability": "async"
    }
  }
  """
  client.updateSettings(index, writeOptimized).map(_ => ())
}

def tuneForReads(index: String): ElasticResult[Unit] = {
  val readOptimized = """
  {
    "index": {
      "refresh_interval": "1s",
      "number_of_replicas": 2
    }
  }
  """
  client.updateSettings(index, readOptimized).map(_ => ())
}
```

---

[Back to index](README.md) | [Next: Alias Management](aliases.md)