[Back to index](README.md)

# MAPPING API

## Overview

The **MappingApi** trait provides comprehensive mapping management functionality for Elasticsearch indices, including creation, retrieval, comparison, and safe migration with automatic rollback capabilities.

**Features:**
- Set and retrieve index mappings
- Compare mappings to detect changes
- Automatic mapping migration with rollback on failure
- Zero-downtime mapping updates
- Comprehensive validation and error handling
- Backup and restore mechanisms

**Dependencies:**
- Requires `SettingsApi` for settings management
- Requires `IndicesApi` for index operations
- Requires `RefreshApi` for post-migration refresh

---

## Public Methods

### setMapping

Sets or updates the mapping for an index.

**Signature:**

```scala
def setMapping(index: String, mapping: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to set the mapping for
- `mapping` - JSON string containing the mapping definition

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if mapping set successfully
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation
- JSON syntax validation

**Limitations:**
- In Elasticsearch, most mapping changes are additive only
- Cannot change existing field types (requires reindexing)
- Cannot delete fields (they remain in the mapping but can be ignored)

**Examples:**

```scala
// Basic mapping
val mapping = """
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "standard"
    },
    "price": {
      "type": "double"
    },
    "created_at": {
      "type": "date"
    }
  }
}
"""
client.setMapping("products", mapping)

// Add new fields to existing mapping
val additionalFields = """
{
  "properties": {
    "description": {
      "type": "text"
    },
    "tags": {
      "type": "keyword"
    }
  }
}
"""
client.setMapping("products", additionalFields)

// Complex mapping with nested objects
val complexMapping = """
{
  "properties": {
    "user": {
      "type": "object",
      "properties": {
        "name": { "type": "text" },
        "email": { "type": "keyword" },
        "age": { "type": "integer" }
      }
    },
    "address": {
      "type": "nested",
      "properties": {
        "street": { "type": "text" },
        "city": { "type": "keyword" },
        "zipcode": { "type": "keyword" }
      }
    }
  }
}
"""
client.setMapping("users", complexMapping)

// Mapping with custom analyzers
val searchMapping = """
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "ngram_analyzer",
      "search_analyzer": "search_analyzer"
    },
    "category": {
      "type": "keyword"
    }
  }
}
"""
client.setMapping("search-index", searchMapping)

// Error handling
client.setMapping("my-index", "{ invalid json }") match {
  case ElasticSuccess(true) => println("Mapping set")
  case ElasticSuccess(false) => println("Mapping not set")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}
```

---

### getMapping

Retrieves the current mapping of an index as a JSON string.

**Signature:**

```scala
def getMapping(index: String): ElasticResult[String]
```

**Parameters:**
- `index` - The index name to retrieve the mapping from

**Returns:**
- `ElasticSuccess[String]` containing the mapping as JSON
- `ElasticFailure` with error details (400 for validation, 404 if index not found)

**Examples:**

```scala
// Retrieve mapping
client.getMapping("products") match {
  case ElasticSuccess(json) => 
    println(s"Mapping: $json")
  case ElasticFailure(e) => 
    println(s"Error: ${e.message}")
}

// Parse and inspect mapping
client.getMapping("my-index").map { json =>
  val mapping = parse(json)
  val properties = (mapping \ "properties").extract[Map[String, Any]]
  properties.keys.foreach(field => println(s"Field: $field"))
}

// Compare mappings across indices
def compareMappings(index1: String, index2: String): ElasticResult[Boolean] = {
  for {
    mapping1 <- client.getMapping(index1)
    mapping2 <- client.getMapping(index2)
  } yield mapping1 == mapping2
}

// Backup mapping before changes
def backupMapping(index: String): ElasticResult[Unit] = {
  client.getMapping(index).map { json =>
    saveToFile(s"$index-mapping-backup.json", json)
  }
}

// Extract specific field mapping
def getFieldMapping(index: String, field: String): ElasticResult[String] = {
  client.getMapping(index).flatMap { json =>
    ElasticResult.attempt {
      val mapping = parse(json)
      val fieldMapping = (mapping \ "properties" \ field)
      compact(render(fieldMapping))
    }
  }
}
```

---

### getMappingProperties

Retrieves the mapping properties of an index (alias for `getMapping`).

**Signature:**

```scala
def getMappingProperties(index: String): ElasticResult[String]
```

**Parameters:**
- `index` - The index name

**Returns:**
- `ElasticSuccess[String]` containing the mapping properties as JSON
- `ElasticFailure` with error details

**Note:** This method is functionally identical to `getMapping` and exists for semantic clarity.

---

### shouldUpdateMapping

Determines if an index's mapping differs from a provided mapping definition.

**Signature:**

```scala
def shouldUpdateMapping(
  index: String,
  mapping: String
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to check
- `mapping` - The target mapping to compare against

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if mappings differ, `false` if identical
- `ElasticFailure` with error details

**Behavior:**
- Uses `MappingComparator.isMappingDifferent` for comparison
- Compares structural differences in field definitions

**Examples:**

```scala
// Check if update needed
val newMapping = """
{
  "properties": {
    "title": { "type": "text" },
    "price": { "type": "double" },
    "new_field": { "type": "keyword" }
  }
}
"""

client.shouldUpdateMapping("products", newMapping) match {
  case ElasticSuccess(true) => 
    println("Mapping needs update")
  case ElasticSuccess(false) => 
    println("Mapping is current")
  case ElasticFailure(e) => 
    println(s"Error: ${e.message}")
}

// Conditional update
def updateIfNeeded(index: String, mapping: String): ElasticResult[Boolean] = {
  for {
    needsUpdate <- client.shouldUpdateMapping(index, mapping)
    result <- if (needsUpdate) {
      client.updateMapping(index, mapping)
    } else {
      ElasticResult.success(false)
    }
  } yield result
}

// Audit mapping status
def auditMappings(
  indices: List[String],
  expectedMapping: String
): Map[String, Boolean] = {
  indices.flatMap { index =>
    client.shouldUpdateMapping(index, expectedMapping) match {
      case ElasticSuccess(needsUpdate) => Some(index -> needsUpdate)
      case ElasticFailure(_) => None
    }
  }.toMap
}
```

---

### updateMapping

Intelligently updates an index's mapping, handling three scenarios automatically:
1. Index doesn't exist → Create with mapping
2. Mapping is outdated → Migrate with rollback protection
3. Mapping is current → Do nothing

**Signature:**

```scala
def updateMapping(
  index: String,
  mapping: String,
  settings: String = defaultSettings
): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to update
- `mapping` - The new mapping definition
- `settings` - Index settings (defaults to `defaultSettings`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if mapping created/updated successfully
- `ElasticFailure` with error details

**Migration Process:**
When mapping update requires reindexing:
1. Backup original mapping, settings and aliases
2. Create temporary index with new mapping
3. Reindex data to temporary index
4. Delete original index
5. Recreate original index with new mapping
6. Reindex data back from temporary
7. Open the index
8. Delete temporary index
9. **On failure:** Automatic rollback, which never deletes the last copy of the documents (see [rollbackMigration](#rollbackmigration))

The aliases of the index are not carried over by a successful migration: re-add them afterwards.

**Examples:**

```scala
// Simple update
val mapping = """
{
  "properties": {
    "title": { "type": "text" },
    "price": { "type": "double" }
  }
}
"""
client.updateMapping("products", mapping)

// Update with custom settings
val customSettings = """
{
  "index": {
    "number_of_shards": 3,
    "number_of_replicas": 2
  }
}
"""
client.updateMapping("products", mapping, customSettings)

// Safe production update
def safeProductionUpdate(
  index: String,
  newMapping: String
): ElasticResult[Boolean] = {
  for {
    // Backup current state
    currentMapping <- client.getMapping(index)
    _ = saveBackup(index, currentMapping)
    
    // Check if update needed
    needsUpdate <- client.shouldUpdateMapping(index, newMapping)
    
    // Perform update if needed
    result <- if (needsUpdate) {
      println(s"Updating mapping for $index...")
      client.updateMapping(index, newMapping)
    } else {
      println(s"Mapping for $index is already current")
      ElasticResult.success(true)
    }
  } yield result
}

// Batch update multiple indices
def updateAllIndices(
  indices: List[String],
  mapping: String
): List[(String, ElasticResult[Boolean])] = {
  indices.map { index =>
    index -> client.updateMapping(index, mapping)
  }
}

// Update with verification
def updateAndVerify(
  index: String,
  mapping: String
): ElasticResult[Boolean] = {
  for {
    updated <- client.updateMapping(index, mapping)
    _ <- if (updated) {
      client.shouldUpdateMapping(index, mapping).flatMap {
        case false => ElasticResult.success(())
        case true => ElasticResult.failure("Mapping verification failed")
      }
    } else {
      ElasticResult.success(())
    }
  } yield updated
}

// Scheduled mapping updates
def scheduledMappingUpdate(
  index: String,
  mapping: String
): ElasticResult[Boolean] = {
  // Disable refresh for better performance
  for {
    _ <- client.toggleRefresh(index, enable = false)
    updated <- client.updateMapping(index, mapping)
    _ <- client.toggleRefresh(index, enable = true)
    _ <- client.refresh(index)
  } yield updated
}
```

**Rollback Protection:**

```scala
// Automatic rollback on failure
client.updateMapping("critical-index", newMapping) match {
  case ElasticSuccess(true) =>
    println("✅ Migration successful")
  case ElasticSuccess(false) =>
    println("⚠️ No update needed")
  case ElasticFailure(error) =>
    // the cause, then where the documents are:
    // "... Index 'critical-index' is unchanged." / "... was restored with its documents."
    // / "... could not be restored: ...; its documents are kept in index 'critical-index_tmp_…'."
    println(s"❌ Migration failed: ${error.message}")
}
```

---

## Private Helper Methods

### createIndexWithMapping

Creates a new index with the specified mapping and settings.

**Process:**
1. Create index with settings
2. Set mapping on the index

```scala
private def createIndexWithMapping(
  index: String,
  mapping: String,
  settings: String
): ElasticResult[Boolean]
```

---

### migrateMappingWithRollback

Backs up the original mapping, settings and aliases, then runs the migration through `migrateIndex`.

```scala
private def migrateMappingWithRollback(
  index: String,
  newMapping: String,
  settings: String
): ElasticResult[Boolean]
```

---

### migrateIndex

The one entry point of a reindex migration: `updateMapping` and an `ALTER TABLE` that needs a reindex both run it. It runs the steps, rolls back a failure, and builds the error.

**Process:**
1. Create temporary index with new mapping
2. Reindex data from original to temporary
3. Delete original index
4. Recreate original with new mapping
5. Reindex data back from temporary
6. Open the index
7. Delete temporary index

A step that throws is a failure of that step. A failed deletion of the temporary index (step 7) is not a failure: the index already holds every document under the new mapping, so the migration succeeds and a warning names the temporary index, to delete once the index is checked.

```scala
private[client] def migrateIndex(
  index: String,
  tempIndex: String,
  mapping: String,
  settings: String,
  aliases: Seq[TableAlias],
  original: IndexBackup
): ElasticResult[Boolean]
```

**Temporary Index Naming:**
- Format: `{index}_tmp_{uuid}` (`updateMapping`), `{index}_tmp_{timestamp}` (`ALTER TABLE`)
- Example: `products_tmp_a1b2c3d4`

---

### rollbackMigration

Rolls back a failed migration from the step that failed. It never deletes the last copy of the documents.

**Rules:**
- **The original was never deleted** (steps 1 and 2): the temporary index is removed if it exists; the original index is not touched.
- **The deletion of the original failed** (step 3): a deletion that answered an error may still take effect, so one existence check decides. Still there: nothing is deleted, and the temporary index is kept and named. Gone: the index is restored. Unknown: nothing is deleted.
- **The original was deleted** (steps 4 to 6): the index is restored, if the temporary index exists. The restore deletes the index left by the failed step, re-creates it with the original mapping and settings (without the settings Elasticsearch generates: `uuid`, `creation_date`, `provided_name`, `version`) and with its ingest pipelines (`default_pipeline`, `final_pipeline`) set to `_none`, copies the documents back through no pipeline, attaches the original pipelines back (close, update the settings, open; an index with no pipeline is only opened), re-adds each original alias (filters included), and deletes the temporary index last. The documents come back as the copy to the temporary index (step 2) left them: no pipeline runs on them a second time.
- Any rollback step that fails stops the rollback. After a failed deletion of one copy, the other copy is never deleted.
- A temporary index kept while the index holds every document loses the aliases it was created with, so no alias reads both copies.

**What the error says:** the cause, then one sentence saying where the documents are:

| State | Message ends with | Status |
|-------|-------------------|--------|
| Unchanged | `Index '<index>' is unchanged.` | the cause's |
| Restored | `Index '<index>' was restored with its documents.` | the cause's |
| Kept | `Index '<index>' could not be restored: <reason>; its documents are kept in index '<tmp>'.` | 500 |
| No temporary copy | `Index '<index>' was left as it is: no temporary index '<tmp>' exists.` | 500 |

Unchanged and Restored get `; the temporary index '<tmp>' was kept: <reason>.` when the temporary index could not be removed. When the documents are kept in the temporary index, copy them back with a reindex (`client.reindex(tmp, index)`) once the cause is fixed.

```scala
private[client] def rollbackMigration(
  index: String,
  tempIndex: String,
  original: IndexBackup,
  failedAt: MigrationStep,
  tempAliases: Seq[TableAlias] = Nil
): RollbackOutcome
```

A migration copies the documents as they are when step 2 runs: pause the writes to the index while it migrates.

---

## Implementation Requirements

### executeSetMapping

```scala
private[client] def executeSetMapping(
  index: String,
  mapping: String
): ElasticResult[Boolean]
```

---

### executeGetMapping

```scala
private[client] def executeGetMapping(index: String): ElasticResult[String]
```

---

## Common Mapping Patterns

### Basic Field Types

```scala
val basicMapping = """
{
  "properties": {
    "text_field": { "type": "text" },
    "keyword_field": { "type": "keyword" },
    "integer_field": { "type": "integer" },
    "long_field": { "type": "long" },
    "double_field": { "type": "double" },
    "boolean_field": { "type": "boolean" },
    "date_field": { "type": "date" },
    "geo_point_field": { "type": "geo_point" }
  }
}
"""
```

### Text Analysis

```scala
val textAnalysisMapping = """
{
  "properties": {
    "title": {
      "type": "text",
      "analyzer": "standard",
      "fields": {
        "keyword": {
          "type": "keyword",
          "ignore_above": 256
        },
        "ngram": {
          "type": "text",
          "analyzer": "ngram_analyzer"
        }
      }
    }
  }
}
"""
```

### Nested Objects

```scala
val nestedMapping = """
{
  "properties": {
    "user": {
      "type": "nested",
      "properties": {
        "name": { "type": "text" },
        "email": { "type": "keyword" }
      }
    }
  }
}
"""
```

### Dynamic Mapping Control

```scala
val strictMapping = """
{
  "dynamic": "strict",
  "properties": {
    "allowed_field": { "type": "text" }
  }
}
"""
```

---

## Migration Workflows

### Zero-Downtime Mapping Update

```scala
def zeroDowntimeMappingUpdate(
  index: String,
  newMapping: String
): ElasticResult[Unit] = {
  for {
    // Check if migration needed
    needsUpdate <- client.shouldUpdateMapping(index, newMapping)
    
    // Perform update with automatic rollback
    _ <- if (needsUpdate) {
      client.updateMapping(index, newMapping).map { success =>
        if (success) {
          println(s"✅ Mapping updated for $index")
        } else {
          println(s"⚠️ Mapping update failed for $index")
        }
      }
    } else {
      println(s"✅ Mapping already current for $index")
      ElasticResult.success(())
    }
  } yield ()
}
```

### Version-Based Migration

```scala
def versionedMappingUpdate(
  baseIndex: String,
  version: Int,
  mapping: String
): ElasticResult[String] = {
  val newIndex = s"$baseIndex-v$version"
  val alias = baseIndex
  
  for {
    // Create new versioned index
    _ <- client.createIndex(newIndex)
    _ <- client.setMapping(newIndex, mapping)
    
    // Find current version
    currentIndices <- findIndicesWithAlias(alias)
    
    // Reindex if previous version exists
    _ <- currentIndices.headOption match {
      case Some(oldIndex) => 
        client.reindex(oldIndex, newIndex)
      case None => 
        ElasticResult.success((true, None))
    }
    
    // Swap alias
    _ <- currentIndices.headOption match {
      case Some(oldIndex) => 
        client.swapAlias(oldIndex, newIndex, alias)
      case None => 
        client.addAlias(newIndex, alias)
    }
  } yield newIndex
}
```

### Incremental Mapping Evolution

```scala
def evolveMappingIncrementally(
  index: String,
  changes: List[String]
): ElasticResult[Boolean] = {
  changes.foldLeft(ElasticResult.success(true)) { (acc, change) =>
    acc.flatMap { _ =>
      client.setMapping(index, change)
    }
  }
}

// Example usage
val changes = List(
  """{"properties": {"new_field_1": {"type": "text"}}}""",
  """{"properties": {"new_field_2": {"type": "keyword"}}}""",
  """{"properties": {"new_field_3": {"type": "date"}}}"""
)

evolveMappingIncrementally("my-index", changes)
```

---

## Error Handling

**Invalid Mapping JSON:**

```scala
client.setMapping("my-index", "{ invalid }") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid mapping"))
}
```

**Index Not Found:**

```scala
client.getMapping("non-existent") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(404))
}
```

**Migration Failure with Rollback:**

```scala
client.updateMapping("my-index", incompatibleMapping) match {
  case ElasticFailure(error) =>
    // e.g. "Reindex failed for index 'my-index_tmp_1a2b3c4d'. Index 'my-index' is unchanged."
    println(s"Migration failed: ${error.message}")
}
```

---

## Best Practices

**1. Always Backup Before Migration**

```scala
def safeMigration(index: String, mapping: String): ElasticResult[Boolean] = {
  for {
    backup <- client.getMapping(index)
    _ = saveToFile(s"$index-backup.json", backup)
    updated <- client.updateMapping(index, mapping)
  } yield updated
}
```

**2. Use Multi-Field Mappings for Flexibility**

```scala
val flexibleMapping = """
{
  "properties": {
    "title": {
      "type": "text",
      "fields": {
        "keyword": { "type": "keyword" },
        "ngram": { "type": "text", "analyzer": "ngram_analyzer" }
      }
    }
  }
}
"""
```

**3. Plan for Schema Evolution**

```scala
// Use dynamic templates for future fields
val evolutionMapping = """
{
  "dynamic_templates": [
    {
      "strings_as_keywords": {
        "match_mapping_type": "string",
        "mapping": {
          "type": "keyword"
        }
      }
    }
  ],
  "properties": {
    "known_field": { "type": "text" }
  }
}
"""
```

**4. Test Mappings in Development**

```scala
def testMapping(mapping: String): ElasticResult[Boolean] = {
  val testIndex = s"test-${UUID.randomUUID().toString}"
  for {
    _ <- client.createIndex(testIndex)
    _ <- client.setMapping(testIndex, mapping)
    _ <- client.deleteIndex(testIndex)
  } yield true
}
```

---

[Back to index](README.md) | [Next: Index Documents](index.md)