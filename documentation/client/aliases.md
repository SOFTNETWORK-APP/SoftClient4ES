[Back to index](README.md)

# ALIAS API

## Overview

The **AliasApi** trait provides comprehensive alias management functionality for Elasticsearch indices, enabling flexible index naming strategies, zero-downtime deployments, and index versioning patterns.

**Features:**
- Add/remove aliases to indices
- Atomic alias swapping for zero-downtime deployments
- Alias existence checking
- Retrieve all aliases for an index
- Full parameter validation
- Support for multi-index aliases

**Dependencies:**
- Requires `IndicesApi` for index existence validation

---

## Alias Naming Rules

Aliases follow the same naming conventions as indices:

- **Lowercase only**
- **No special characters:** `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, space, comma, `#`
- **Cannot start with:** `-`, `_`, `+`
- **Cannot be:** `.` or `..`
- **Maximum length:** 255 characters

---

## Public Methods

### addAlias

Adds an alias to an existing index.

**Signature:**

```scala
def addAlias(index: String, alias: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to add the alias to
- `alias` - The alias name to create

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if alias added successfully
- `ElasticFailure` with error details (400 for validation, 404 if index not found)

**Validation:**
- Index name format validation
- Alias name format validation
- Index and alias cannot have the same name
- Index must exist before adding alias

**Behavior:**
- An alias can point to multiple indices (useful for searches across versions)
- An index can have multiple aliases
- Adding an existing alias is idempotent (no error)

**Examples:**

```scala
// Basic alias creation
client.addAlias("products-2024-01", "products-current") match {
  case ElasticSuccess(_) => println("Alias added")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}

// Version management pattern
client.createIndex("users-v2")
client.addAlias("users-v2", "users-latest")
client.addAlias("users-v2", "users")

// Multi-index alias for searching across versions
for {
  _ <- client.addAlias("logs-2024-01", "logs-all")
  _ <- client.addAlias("logs-2024-02", "logs-all")
  _ <- client.addAlias("logs-2024-03", "logs-all")
} yield "Multi-index alias created"

// Environment-specific aliases
def setupEnvironmentAliases(env: String): ElasticResult[Unit] = {
  val indexName = s"products-$env"
  for {
    _ <- client.createIndex(indexName)
    _ <- client.addAlias(indexName, "products-active")
    _ <- client.addAlias(indexName, s"products-$env-current")
  } yield ()
}

// Error handling
client.addAlias("products", "products") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("same name"))
    assert(error.statusCode.contains(400))
}

client.addAlias("non-existent", "my-alias") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("does not exist"))
    assert(error.statusCode.contains(404))
}
```

---

### removeAlias

Removes an alias from an index.

**Signature:**

```scala
def removeAlias(index: String, alias: String): ElasticResult[Boolean]
```

**Parameters:**
- `index` - The index name to remove the alias from
- `alias` - The alias name to remove

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if alias removed successfully
- `ElasticFailure` with error details (400 for validation, 404 if alias not found)

**Validation:**
- Index name format validation
- Alias name format validation

**Behavior:**
- If the alias does not exist, Elasticsearch returns a 404 error
- Removing an alias does not affect the underlying index
- If alias points to multiple indices, only removes from specified index

**Examples:**

```scala
// Simple removal
client.removeAlias("products-v1", "products-current") match {
  case ElasticSuccess(_) => println("Alias removed")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}

// Cleanup old aliases
val oldAliases = List("temp-alias", "test-alias", "staging-alias")
oldAliases.foreach { alias =>
  client.removeAlias("my-index", alias)
}

// Safe removal with existence check
def safeRemoveAlias(index: String, alias: String): ElasticResult[Boolean] = {
  client.aliasExists(alias).flatMap {
    case true => client.removeAlias(index, alias)
    case false => ElasticResult.success(false)
  }
}

// Remove all aliases from an index
def removeAllAliases(index: String): ElasticResult[Unit] = {
  for {
    aliases <- client.getAliases(index)
    _ <- aliases.foldLeft(ElasticResult.success(())) { (acc, alias) =>
      acc.flatMap(_ => client.removeAlias(index, alias).map(_ => ()))
    }
  } yield ()
}

// Deployment cleanup
def cleanupOldVersion(oldIndex: String, currentAlias: String): ElasticResult[Unit] = {
  for {
    _ <- client.removeAlias(oldIndex, currentAlias)
    _ <- client.closeIndex(oldIndex)
  } yield ()
}
```

---

### aliasExists

Checks whether an alias exists in the cluster.

**Signature:**

```scala
def aliasExists(alias: String): ElasticResult[Boolean]
```

**Parameters:**
- `alias` - The alias name to check

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if alias exists, `false` otherwise
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Alias name format validation

**Examples:**

```scala
// Simple existence check
client.aliasExists("products-current") match {
  case ElasticSuccess(true) => println("Alias exists")
  case ElasticSuccess(false) => println("Alias does not exist")
  case ElasticFailure(error) => println(s"Error: ${error.message}")
}

// Conditional alias creation
def ensureAliasExists(index: String, alias: String): ElasticResult[Boolean] = {
  client.aliasExists(alias).flatMap {
    case false => client.addAlias(index, alias)
    case true => ElasticResult.success(true)
  }
}

// Pre-deployment validation
def validateDeploymentReady(newIndex: String, alias: String): ElasticResult[Boolean] = {
  for {
    indexExists <- client.indexExists(newIndex)
    aliasExists <- client.aliasExists(alias)
  } yield indexExists && aliasExists
}

// Check multiple aliases
val requiredAliases = List("products", "products-current", "products-active")
val checks = requiredAliases.map { alias =>
  alias -> client.aliasExists(alias)
}

checks.foreach {
  case (alias, ElasticSuccess(true)) => println(s"✅ $alias exists")
  case (alias, ElasticSuccess(false)) => println(s"❌ $alias missing")
  case (alias, ElasticFailure(e)) => println(s"⚠️ Error checking $alias: ${e.message}")
}

// Wait for alias creation
def waitForAlias(alias: String, maxAttempts: Int = 10): ElasticResult[Boolean] = {
  def attempt(remaining: Int): ElasticResult[Boolean] = {
    client.aliasExists(alias).flatMap {
      case true => ElasticResult.success(true)
      case false if remaining > 0 =>
        Thread.sleep(1000)
        attempt(remaining - 1)
      case false => 
        ElasticResult.failure(s"Alias $alias not found after $maxAttempts attempts")
    }
  }
  attempt(maxAttempts)
}
```

---

### getAliases

Retrieves all aliases associated with an index.

**Signature:**

```scala
def getAliases(index: String): ElasticResult[Set[String]]
```

**Parameters:**
- `index` - The index name to retrieve aliases for

**Returns:**
- `ElasticSuccess[Set[String]]` containing all alias names (empty set if no aliases)
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Index name format validation

**Behavior:**
- Returns empty set if index has no aliases
- Returns empty set if index does not exist (with warning log)

**Examples:**

```scala
// Retrieve aliases
client.getAliases("products-v2") match {
  case ElasticSuccess(aliases) => 
    println(s"Aliases: ${aliases.mkString(", ")}")
  case ElasticFailure(error) => 
    println(s"Error: ${error.message}")
}

// Check if index has specific alias
def hasAlias(index: String, alias: String): ElasticResult[Boolean] = {
  client.getAliases(index).map(_.contains(alias))
}

// List all aliases for multiple indices
val indices = List("products-v1", "products-v2", "products-v3")
indices.foreach { index =>
  client.getAliases(index) match {
    case ElasticSuccess(aliases) if aliases.nonEmpty =>
      println(s"$index: ${aliases.mkString(", ")}")
    case ElasticSuccess(_) =>
      println(s"$index: no aliases")
    case ElasticFailure(e) =>
      println(s"$index: error - ${e.message}")
  }
}

// Find all indices with a specific alias
def findIndicesForAlias(
  indices: List[String],
  targetAlias: String
): ElasticResult[List[String]] = {
  val results = indices.map { index =>
    client.getAliases(index).map(aliases => (index, aliases))
  }
  
  ElasticResult.sequence(results).map { indexAliases =>
    indexAliases.filter(_._2.contains(targetAlias)).map(_._1)
  }
}

// Audit alias configuration
def auditAliases(indices: List[String]): Map[String, Set[String]] = {
  indices.flatMap { index =>
    client.getAliases(index) match {
      case ElasticSuccess(aliases) => Some(index -> aliases)
      case ElasticFailure(_) => None
    }
  }.toMap
}

// Verify expected aliases
def verifyAliases(
  index: String,
  expectedAliases: Set[String]
): ElasticResult[Boolean] = {
  client.getAliases(index).map { actual =>
    actual == expectedAliases
  }
}
```

---

### swapAlias

Atomically swaps an alias from one index to another in a single operation.

**Signature:**

```scala
def swapAlias(
  oldIndex: String,
  newIndex: String,
  alias: String
): ElasticResult[Boolean]
```

**Parameters:**
- `oldIndex` - The current index pointed to by the alias
- `newIndex` - The new index that should receive the alias
- `alias` - The alias name to swap

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if swap succeeded
- `ElasticFailure` with error details (400 for validation errors)

**Validation:**
- Old index name format validation
- New index name format validation
- Alias name format validation
- Old and new indices must be different

**Behavior:**
- **Atomic operation:** Alias is removed from old index and added to new index in a single request
- No downtime period where alias doesn't exist
- Recommended for zero-downtime deployments
- If alias doesn't exist on old index, it's still added to new index

**Examples:**

```scala
// Zero-downtime deployment
client.swapAlias(
  oldIndex = "products-v1",
  newIndex = "products-v2",
  alias = "products"
) match {
  case ElasticSuccess(_) => 
    println("✅ Alias swapped, new version deployed")
  case ElasticFailure(error) => 
    println(s"❌ Error: ${error.message}")
}

// Complete deployment workflow
def deployNewVersion(
  currentVersion: String,
  newVersion: String,
  productionAlias: String
): ElasticResult[Unit] = {
  for {
    // Create new index
    _ <- client.createIndex(newVersion)
    
    // Index data into new version
    _ <- populateIndex(newVersion)
    
    // Verify data
    _ <- verifyIndexData(newVersion)
    
    // Atomic swap
    _ <- client.swapAlias(currentVersion, newVersion, productionAlias)
    
    // Cleanup old version
    _ <- client.closeIndex(currentVersion)
  } yield ()
}

// Blue-green deployment
def blueGreenDeploy(): ElasticResult[Unit] = {
  val blue = "products-blue"
  val green = "products-green"
  val alias = "products"
  
  for {
    // Determine current active index
    aliases <- client.getAliases(blue)
    (oldIndex, newIndex) = if (aliases.contains(alias)) {
      (blue, green)
    } else {
      (green, blue)
    }
    
    // Deploy to inactive index
    _ <- client.createIndex(newIndex)
    _ <- populateIndex(newIndex)
    
    // Swap alias
    _ <- client.swapAlias(oldIndex, newIndex, alias)
  } yield ()
}

// Rollback pattern
def rollback(
  currentIndex: String,
  previousIndex: String,
  alias: String
): ElasticResult[Unit] = {
  for {
    _ <- client.swapAlias(currentIndex, previousIndex, alias)
    _ = println(s"✅ Rolled back to $previousIndex")
  } yield ()
}

// Multi-alias swap for multiple environments
def swapMultipleAliases(
  oldIndex: String,
  newIndex: String,
  aliases: List[String]
): ElasticResult[List[Boolean]] = {
  ElasticResult.sequence(
    aliases.map(alias => client.swapAlias(oldIndex, newIndex, alias))
  )
}

// Safe swap with validation
def safeSwap(
  oldIndex: String,
  newIndex: String,
  alias: String
): ElasticResult[Boolean] = {
  for {
    // Verify new index exists
    newExists <- client.indexExists(newIndex)
    _ <- if (!newExists) {
      ElasticResult.failure(s"New index $newIndex does not exist")
    } else {
      ElasticResult.success(())
    }
    
    // Verify new index has data
    // (implementation depends on your count method)
    
    // Perform swap
    swapped <- client.swapAlias(oldIndex, newIndex, alias)
    
    // Verify alias now points to new index
    aliases <- client.getAliases(newIndex)
    _ <- if (aliases.contains(alias)) {
      ElasticResult.success(())
    } else {
      ElasticResult.failure("Alias swap verification failed")
    }
  } yield swapped
}

// Error handling
client.swapAlias("products-v1", "products-v1", "products") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("cannot be the same"))
    assert(error.statusCode.contains(400))
}
```

---

## Implementation Requirements

### executeAddAlias

```scala
private[client] def executeAddAlias(
  index: String,
  alias: String
): ElasticResult[Boolean]
```

---

### executeRemoveAlias

```scala
private[client] def executeRemoveAlias(
  index: String,
  alias: String
): ElasticResult[Boolean]
```

---

### executeAliasExists

```scala
private[client] def executeAliasExists(alias: String): ElasticResult[Boolean]
```

---

### executeGetAliases

```scala
private[client] def executeGetAliases(index: String): ElasticResult[String]
```

**Expected JSON Response Format:**

```scala
{
  "my-index": {
    "aliases": {
      "alias1": {},
      "alias2": {},
      "alias3": {}
    }
  }
}
```

---

### executeSwapAlias

```scala
private[client] def executeSwapAlias(
  oldIndex: String,
  newIndex: String,
  alias: String
): ElasticResult[Boolean]
```

**Implementation Note:** This should use Elasticsearch's bulk alias API to perform both remove and add operations atomically in a single request.

---

## Error Handling

**Invalid Alias Name:**

```scala
client.addAlias("my-index", "INVALID ALIAS") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.message.contains("Invalid alias"))
}
```

**Same Name for Index and Alias:**

```scala
client.addAlias("products", "products") match {
  case ElasticFailure(error) =>
    assert(error.message.contains("same name"))
}
```

**Index Not Found:**

```scala
client.addAlias("non-existent-index", "my-alias") match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(404))
    assert(error.message.contains("does not exist"))
}
```

**Alias Not Found:**

```scala
client.removeAlias("my-index", "non-existent-alias") match {
  case ElasticFailure(error) =>
    // Elasticsearch returns 404 when alias doesn't exist
    assert(error.statusCode.contains(404))
}
```

---

## Common Patterns

### Zero-Downtime Deployment

```scala
def zeroDowntimeDeployment(
  currentVersion: String,
  newVersion: String,
  productionAlias: String,
  settings: String
): ElasticResult[Unit] = {
  for {
    // 1. Create new index
    _ <- client.createIndex(newVersion, settings)
    
    // 2. Populate new index
    _ <- reindexData(currentVersion, newVersion)
    
    // 3. Verify new index
    verified <- verifyNewIndex(newVersion)
    _ <- if (verified) ElasticResult.success(())
         else ElasticResult.failure("New index verification failed")
    
    // 4. Atomic swap
    _ <- client.swapAlias(currentVersion, newVersion, productionAlias)
    
    // 5. Monitor for issues
    _ = monitorHealth(newVersion)
    
    // 6. Cleanup old index after grace period
    _ = scheduleCleanup(currentVersion)
  } yield ()
}
```

### Blue-Green Deployment

```scala
object BlueGreenDeployment {
  val BLUE = "products-blue"
  val GREEN = "products-green"
  val ALIAS = "products"
  
  def deploy(data: Seq[Document]): ElasticResult[Unit] = {
    for {
      // Find current active
      blueAliases <- client.getAliases(BLUE)
      (active, inactive) = if (blueAliases.contains(ALIAS)) {
        (BLUE, GREEN)
      } else {
        (GREEN, BLUE)
      }
      
      // Deploy to inactive
      _ <- client.deleteIndex(inactive).recover(_ => false)
      _ <- client.createIndex(inactive)
      _ <- bulkIndex(inactive, data)
      
      // Swap
      _ <- client.swapAlias(active, inactive, ALIAS)
    } yield ()
  }
  
  def rollback(): ElasticResult[Unit] = {
    for {
      greenAliases <- client.getAliases(GREEN)
      (current, previous) = if (greenAliases.contains(ALIAS)) {
        (GREEN, BLUE)
      } else {
        (BLUE, GREEN)
      }
      _ <- client.swapAlias(current, previous, ALIAS)
    } yield ()
  }
}
```

### Time-Based Index Management

```scala
def setupMonthlyIndex(year: Int, month: Int): ElasticResult[Unit] = {
  val indexName = f"logs-$year-$month%02d"
  val currentAlias = "logs-current"
  val allLogsAlias = "logs-all"
  
  for {
    // Create new month's index
    _ <- client.createIndex(indexName)
    
    // Add to "all logs" alias (multi-index)
    _ <- client.addAlias(indexName, allLogsAlias)
    
    // Find previous current index
    previousIndices <- findIndicesWithAlias(currentAlias)
    
    // Swap current alias to new month
    _ <- previousIndices.headOption match {
      case Some(prevIndex) => 
        client.swapAlias(prevIndex, indexName, currentAlias)
      case None => 
        client.addAlias(indexName, currentAlias)
    }
  } yield ()
}
```

### Alias-Based Read/Write Splitting

```scala
object ReadWriteSplit {
  val WRITE_ALIAS = "products-write"
  val READ_ALIAS = "products-read"
  
  def setupSplit(activeIndex: String, replicaIndices: List[String]): ElasticResult[Unit] = {
    for {
      // Write alias points to single active index
      _ <- client.addAlias(activeIndex, WRITE_ALIAS)
      
      // Read alias points to all indices
      _ <- client.addAlias(activeIndex, READ_ALIAS)
      _ <- replicaIndices.foldLeft(ElasticResult.success(())) { (acc, index) =>
        acc.flatMap(_ => client.addAlias(index, READ_ALIAS).map(_ => ()))
      }
    } yield ()
  }
}
```

---

## Best Practices

**1. Always Use Aliases in Application Code**

```scala
// ❌ Bad - hardcoded index names
client.search("products-v2", query)

// ✅ Good - use aliases
client.search("products", query)
```

**2. Atomic Swaps for Deployments**

```scala
// ❌ Bad - non-atomic, causes downtime
client.removeAlias(oldIndex, alias)
Thread.sleep(100) // ⚠️ Alias doesn't exist here!
client.addAlias(newIndex, alias)

// ✅ Good - atomic operation
client.swapAlias(oldIndex, newIndex, alias)
```

**3. Verify Before Swapping**

```scala
def verifiedSwap(
  oldIndex: String,
  newIndex: String,
  alias: String
): ElasticResult[Boolean] = {
  for {
    // Verify new index is ready
    exists <- client.indexExists(newIndex)
    _ <- if (!exists) ElasticResult.failure("New index missing")
         else ElasticResult.success(())
    
    // Perform swap
    swapped <- client.swapAlias(oldIndex, newIndex, alias)
  } yield swapped
}
```

**4. Use Descriptive Alias Names**

```scala
// ✅ Good alias naming
"products-current"    // Active version
"products-read"       // Read operations
"products-write"      // Write operations
"products-all"        // All versions
"products-staging"    // Staging environment
```

---

[Back to index](README.md) | [Next: Mappings API](mappings.md)