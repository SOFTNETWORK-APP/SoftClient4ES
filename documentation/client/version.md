[Back to index](README.md)

# VERSION API

## Overview

The **VersionApi** trait provides functionality to retrieve the Elasticsearch cluster version with automatic caching to minimize network overhead.

**Dependencies:** N/A.

---

## Public Methods

### version

Retrieves the Elasticsearch cluster version. Results are cached after the first successful call.

**Signature:**

```scala
def version: ElasticResult[String]
```

**Returns:**
- `ElasticSuccess[String]` containing the version string (e.g., "7.17.0", "8.11.3")
- `ElasticFailure` with error details if retrieval fails

**Behavior:**
- Returns cached version immediately if available
- On first call, executes `executeVersion()` and caches successful results
- Failures are NOT cached (allows retry on next call)
- Logs success with ✅ and failures with ❌

**Examples:**

```scala
// Basic usage
val result = client.version
result match {
  case ElasticSuccess(v) => println(s"Running ES $v")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Monadic operations
val majorVersion = client.version.map(_.split("\\.").head.toInt)

// Version-based logic
client.version.foreach { v =>
  if (v.startsWith("8.")) {
  // Use ES 8.x specific features
  }
}

// Compatibility check
def requiresMinVersion(min: String): ElasticResult[Unit] = {
  client.version.flatMap { current =>
    if (current >= min) ElasticResult.success(())
    else ElasticResult.failure(s"Requires ES $min+, found $current")
  }
}
```

---

## Implementation Requirements

### executeVersion

Must be implemented by each client-specific trait.

**Signature:**

```scala
private[client] def executeVersion(): ElasticResult[String]
```

**Implementation Examples:**

**Java Client (ES 8-9):**

```scala
private[client] def executeVersion(): ElasticResult[String] = {
  executeJavaAction[InfoResponse, String](
    operation = "version"
  )(
    action = client.info()
  )(
    transformer = _.version().number()
  )
}
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeVersion(): ElasticResult[String] = {
  executeRestLowLevelAction[String](
    operation = "version"
  )(
    request = new Request("GET", "/")
  )(
    transformer = { resp =>
      val json = parse(EntityUtils.toString(resp.getEntity))
      (json \\ "version" \\ "number").extract[String]
    }
  )
}
```

**Jest Client (ES 5-6):**

```scala
private[client] def executeVersion(): ElasticResult[String] = {
  executeJestAction[JestResult, String](
    operation = "version"
  )(
    action = new Info.Builder().build()
  )(
    transformer = { result =>
      val json = parse(result.getJsonString)
      (json \\ "version").extract[String]
    }
  )
}
```

---

[Back to index](README.md) | [Next: Flush Index](flush.md)