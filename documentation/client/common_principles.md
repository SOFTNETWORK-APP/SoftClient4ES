[Back to index](README.md)

# COMMON PRINCIPLES

## Table of Contents

- [Architecture Overview](common_principles.md#architecture-overview)
- [Service Provider Interface (SPI)](common_principles.md#service-provider-interface-spi)
- [Client Factory](common_principles.md#client-factory)
- [Decorator Pattern (Metrics & Monitoring)](common_principles.md#decorator-pattern-metrics--monitoring)
- [Configuration Management](common_principles.md#configuration-management)
- [Result Handling](common_principles.md#result-handling)
- [Error Model](common_principles.md#error-model)
- [Validation Helpers](common_principles.md#validation-helpers)
- [Execution Patterns](common_principles.md#execution-patterns)
- [Logging Conventions](common_principles.md#logging-conventions)

---

## Architecture Overview

### Trait Composition (Cake Pattern)

The Elasticsearch client API is built using **trait composition**, allowing modular and extensible design:

```scala
trait ElasticClientApi
  extends IndicesApi
  with SettingsApi
  with AliasApi
  with MappingApi
  with CountApi
  with SearchApi
  // ... other APIs
  with Closeable
```

**Benefits** :

- ✅ Separation of concerns (each API is independent)
- ✅ Easy to test (mock individual traits)
- ✅ Flexible composition (choose which APIs to include)

### Self-Type Annotations

APIs use **self-type annotations** to declare dependencies:

```scala
trait ScrollApi extends ElasticClientHelpers {
  _: VersionApi with SearchApi =>
  // This trait requires VersionApi and SearchApi to be mixed in
  // ...
}
```

**Purpose** :

- Ensures compile-time dependency validation
- Documents required traits explicitly
- Enables modular composition

### Template Method Pattern

Each API defines:

1. **Public methods** - High-level interface for users
2. **Private implementation methods** - Client-specific logic (prefixed with `execute*`)

```scala
  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  // Cache ES version (avoids calling it every time)
  @volatile private var cachedVersion: Option[String] = None

  /** Get Elasticsearch version.
    * @return
    *   the Elasticsearch version
    */
  def version: ElasticResult[String] = {
    //...
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================
  
  private[client] def executeVersion(): ElasticResult[String]

```

**Implementations** :

- `JestClientVersion` - Jest Client (ES 5-6)
- `RestHighLevelClientVersion` - REST High Level Client (ES 6-7)
- `JavaClientVersion` - Java Client (ES 8-9)

---

## Service Provider Interface (SPI)

### ElasticClientSpi

The library uses **Java's ServiceLoader mechanism** for pluggable client implementations:

```scala
trait ElasticClientSpi {
  def client(conf: Config): ElasticClientApi
}
```

**Benefits** :

- ✅ Pluggable architecture - Add new clients without modifying core code
- ✅ Loose coupling - Factory doesn't depend on concrete implementations
- ✅ Runtime discovery - Clients loaded automatically via classpath

### Implementation Example

```scala
class RestHighLevelClientSpi extends ElasticClientSpi {
  override def client(config: Config): ElasticClientApi = {
    new RestHighLevelClientApi(config)
  }
}
```

**Registration** (in META-INF/services/app.softnetwork.elastic.client.spi.ElasticClientSpi):

- app.softnetwork.elastic.client.spi.JestClientSpi (`softclient4es6-jest-client`)
- app.softnetwork.elastic.client.spi.RestHighLevelClientSpi (`softclient4es6-rest-client`, `softclient4es7-rest-client`)
- app.softnetwork.elastic.client.spi.JavaClientSpi (`softclient4es8-java-client`, `softclient4es9-java-client`)

---

## Client Factory

### ElasticClientFactory

#### Central factory for creating and caching Elasticsearch clients:

```scala
object ElasticClientFactory {
  def create(config: Config = ConfigFactory.load()): ElasticClientApi
  def createWithMetrics(config: Config = ConfigFactory.load()): MetricsElasticClient
  def createWithMonitoring(config: Config = ConfigFactory.load()): MonitoredElasticClient
}
```

#### Exemple :

```scala
val config = ConfigFactory.load()

// Client created according to configuration
val client = ElasticClientFactory.create(config)

// Normal usage
client.createIndex("products")
client.index("products", "123", """{"name": "Product"}""")

// Access metrics if enabled
client match {
  case metricsClient: MetricsElasticClient =>
    val metrics = metricsClient.getMetrics
    println(s"Operations: ${metrics.totalOperations}")
  case _ => println("Metrics not enabled")
}
```

### Client Creation Modes

1. Base Client (No Metrics)

```scala
val client = ElasticClientFactory.create(config)
```

2. Client with Metrics

```scala
val client = ElasticClientFactory.createWithMetrics(config)

// Access metrics
val metrics = client.getMetrics
println(s"Total operations: ${metrics.totalOperations}")
println(s"Success rate: ${metrics.successRate}%")
```

3. Client with Monitoring

```scala
val client = ElasticClientFactory.createWithMonitoring(config)

// Automatic periodic reports every 30s:
// === Elasticsearch Metrics ===
// Total Operations: 150
// Success Rate: 98.5%
// Average Duration: 45ms
// =============================

// Automatic alerts when thresholds exceeded:
// ⚠️  HIGH FAILURE RATE: 15.0%
// ⚠️  HIGH LATENCY: 1200ms

client.logMetrics()
```

4. Custom Metrics Collector

```scala
val sharedCollector = new MetricsCollector()

// Multiple clients sharing same collector
val client1 = ElasticClientFactory.createWithCustomMetrics(config, sharedCollector)
val client2 = ElasticClientFactory.createWithCustomMetrics(config, sharedCollector)

// Aggregated metrics across all clients
val metrics = sharedCollector.getMetrics
```

```text
=== Elasticsearch Metrics ===
Total Operations: 106
Success Rate: 98.11320754716981%
Failure Rate: 1.8867924528301927%
Average Duration: 35.735849056603776ms
Min Duration: 2ms
Max Duration: 223ms
=============================
```

### Lifecycle Management

#### Automatic Shutdown Hook

```scala
sys.addShutdownHook {
  logger.info("JVM shutdown detected, closing all Elasticsearch clients")
  ElasticClientFactory.shutdown()
}
```

#### Manual Shutdown

```scala
// Shutdown all cached clients
ElasticClientFactory.shutdown()

// Clear cache without shutdown (testing)
ElasticClientFactory.clearCache()

// Cache statistics
val stats = ElasticClientFactory.getCacheStats
// Map("baseClients" -> 2, "metricsClients" -> 1, "monitoredClients" -> 1)
```

---

## Decorator Pattern (Metrics & Monitoring)

### Architecture

The library uses the `Decorator Pattern` to add metrics and monitoring capabilities without modifying the base client:

`ElasticClientApi` (base interface)
      ↑
      |
`ElasticClientDelegator` (delegation helper)
      ↑
      |
`MetricsElasticClient` (adds metrics)
      ↑
      |
`MonitoredElasticClient` (adds monitoring + alerts)

### MetricsElasticClient

Decorates any `ElasticClientApi` with metrics collection:

```scala
class MetricsElasticClient(
  val delegate: ElasticClientApi,
  val metricsCollector: MetricsCollector
) extends ElasticClientDelegator with MetricsApi
```

#### Features:

- ✅ Records operation duration
- ✅ Tracks success/failure rates
- ✅ Aggregates metrics by operation
- ✅ Aggregates metrics by index
- ✅ Thread-safe using AtomicLong

#### Measurement Pattern:

```scala
private def measureResult[T](operation: String, index: Option[String])(
  block: => ElasticResult[T]
): ElasticResult[T] = {
  val startTime = System.currentTimeMillis()
  val result = block
  val duration = System.currentTimeMillis() - startTime
  metricsCollector.recordOperation(operation, duration, result.isSuccess, index)
  result
}
```

### MetricsCollector

Thread-safe metrics accumulator using atomic operations:

```scala
class MetricsCollector {
  private class MetricAccumulator {
    val totalOps = new AtomicLong(0)
    val successOps = new AtomicLong(0)
    val failureOps = new AtomicLong(0)
    val totalDuration = new AtomicLong(0)
    val minDuration = new AtomicLong(Long.MaxValue)
    val maxDuration = new AtomicLong(Long.MinValue)
    val lastExecution = new AtomicLong(0)
  }
}
```

**Atomic updates** ensure thread-safety without locks:

```scala
totalOps.incrementAndGet()
totalDuration.addAndGet(duration)
minDuration.updateAndGet(current => Math.min(current, duration))
```

### MetricsApi

#### Global Metrics

```scala
val metrics = client.getMetrics

println(s"Total operations: ${metrics.totalOperations}")
println(s"Success rate: ${metrics.successRate}%")
println(s"Average duration: ${metrics.averageDuration}ms")
println(s"Min/Max: ${metrics.minDuration}ms / ${metrics.maxDuration}ms")
```

#### Metrics by Operation

```scala
client.getMetricsByOperation("search").foreach { metrics =>
  println(s"Search operations: ${metrics.totalOperations}")
  println(s"Search avg latency: ${metrics.averageDuration}ms")
  
  // Performance grading
  val grade = metrics.averageDuration match {
    case d if d < 100 => "Excellent"
    case d if d < 500 => "Good"
    case d if d < 1000 => "Average"
    case _ => "Needs optimization"
  }
  println(s"Performance: $grade")
}
```

#### Metrics by Index

```scala
client.getMetricsByIndex("products").foreach { metrics =>
  println(s"Products index operations: ${metrics.totalOperations}")
  println(s"Products index avg duration: ${metrics.averageDuration}ms")
}

// Compare index performance
val productsPerf = client.getMetricsByIndex("products")
  .map(_.averageDuration).getOrElse(0.0)
val ordersPerf = client.getMetricsByIndex("orders")
  .map(_.averageDuration).getOrElse(0.0)

if (productsPerf > ordersPerf * 2) {
  println("⚠️ Products index is significantly slower")
}
```

#### Aggregated Metrics

```scala
val aggregated = client.getAggregatedMetrics

println(s"=== Global ===")
println(s"Total: ${aggregated.totalOperations} ops")
println(s"Success rate: ${aggregated.successRate}%")

println(s"=== By Operation ===")
aggregated.operationMetrics.foreach { case (op, m) =>
  println(s"$op: ${m.totalOperations} ops, ${m.averageDuration}ms avg")
}

println(s"=== By Index ===")
aggregated.indexMetrics.foreach { case (idx, m) =>
  println(s"$idx: ${m.totalOperations} ops, ${m.averageDuration}ms avg")
}
```

#### Reset Metrics

```scala
// Useful for warmup phases or testing
client.resetMetrics()
```

### MonitoredElasticClient

Extends `MetricsElasticClient` with automatic monitoring and alerting:

```scala
class MonitoredElasticClient(
  delegate: ElasticClientApi,
  metricsCollector: MetricsCollector,
  monitoringConfig: MonitoringConfig
)(implicit system: ActorSystem)
extends MetricsElasticClient(delegate, metricsCollector)
```

#### Features

- ✅ Periodic reports - Logs metrics at configured intervals
- ✅ Automatic alerts - Warns when thresholds exceeded
- ✅ Graceful shutdown - Logs final metrics before closing
- ✅ Akka Scheduler - Non-blocking periodic execution

#### Monitoring Loop:

```scala
system.scheduler.scheduleAtFixedRate(interval, interval) { () =>
  logMetrics()    // Log current metrics
  checkAlerts()   // Check thresholds and alert
}
```

#### Alert Conditions:

// High failure rate
if (metrics.failureRate > failureRateThreshold) {
logger.warn(s"⚠️  HIGH FAILURE RATE: ${metrics.failureRate}%")
}

// High latency
if (metrics.averageDuration > latencyThreshold) {
logger.warn(s"⚠️  HIGH LATENCY: ${metrics.averageDuration}ms")
}

---

## Configuration Management

### Typesafe Config

All configuration uses **Typesafe Config** (`HOCON` format):

```hocon
elastic {
  # Connection settings
  host = "localhost"
  host = ${?ELASTIC_HOST}
  port = 9200
  port = ${?ELASTIC_PORT}
  
  # Authentication
  credentials {
    url      = "http://"${elastic.host}":"${elastic.port}
    username = ""
    password = ""
    url      = ${?ELASTIC_CREDENTIALS_URL}
    username = ${?ELASTIC_CREDENTIALS_USERNAME}
    password = ${?ELASTIC_CREDENTIALS_PASSWORD}
  }
  
  # Performance
  multithreaded      = true
  connection-timeout = 5s
  socket-timeout     = 30s
  
  # Cluster discovery
  discovery {
    enabled   = false
    frequency = 5m
  }
  
  # Metrics and Monitoring
  metrics {
    enabled = true
    monitoring {
      enabled                = true
      interval               = 30s
      failure-rate-threshold = 10.0   # Alert if > 10% failures
      latency-threshold      = 1000.0 # Alert if > 1000ms
    }
  }
}
```

### Environment Variable Overrides

Configuration supports environment variable substitution :

```shell
# Override host
export ELASTIC_HOST="prod-es-cluster.example.com"

# Override credentials
export ELASTIC_CREDENTIALS_USERNAME="admin"
export ELASTIC_CREDENTIALS_PASSWORD="secret"

# Override port
export ELASTIC_PORT=9243
```

### Loading Configuration

```scala
// Default (loads application.conf)
val config = ConfigFactory.load()
val elasticConfig = ElasticConfig(config)

// Custom configuration file
val config = ConfigFactory.parseFile(new File("custom.conf"))

// Programmatic configuration
val config = ConfigFactory.parseString("""
  elastic {
    host = "localhost"
    port = 9200
    metrics.enabled = false
  }
""")
```

---

## Result Handling

### ElasticResult[T] - Monadic ADT

`ElasticResult[T]` is a functional wrapper for Elasticsearch operations, similar to `Try[T]` or `Either[E, T]`:

```scala
sealed trait ElasticResult[+T]
case class ElasticSuccess[T](value: T) extends ElasticResult[T]
case class ElasticFailure(elasticError: ElasticError) extends ElasticResult[Nothing]
```

#### Monadic Operations

```scala
val result: ElasticResult[String] = version

// Transform success value
result.map(v => s"ES version: $v")

// Chain operations
result.flatMap(v => anotherElasticOperation(v))

// Filter results
result.filter(v => v.startsWith("7."), "Unsupported version")
```

#### Extracting Values

```scala
// Safe extraction
result.getOrElse("default-version")

// Pattern matching
result.fold(
  onFailure = error => s"Error: ${error.message}",
  onSuccess = version => s"Version: $version"
)

// Conversions
result.toOption    // Option[T]
result.toEither    // Either[ElasticError, T]
```

#### Side Effects

```scala
// Execute on success
result.foreach(v => println(s"Version: $v"))

// Check status
if (result.isSuccess) { /* ... */ }
if (result.isFailure) { /* ... */ }
```

### Utility Methods

#### Creating results

```scala
ElasticResult.success("7.17.0")
ElasticResult.failure("Connection failed")
ElasticResult.failure("Timeout", new TimeoutException())
```

#### From Other Types

```scala
// From Try
ElasticResult.fromTry(Try { /* operation */ })

// From Option
ElasticResult.fromOption(Some("value"), "Not found")

// From Either
ElasticResult.fromEither(Right("value"))

// From Future
ElasticResult.fromFuture(futureOperation)
```

#### Collection Operations

```scala
// Sequence: List[ElasticResult[T]] => ElasticResult[List[T]]
val results: List[ElasticResult[String]] = List(...)
ElasticResult.sequence(results)

// Traverse: Apply function to list
ElasticResult.traverse(indices)(index => getMapping(index))
```

### Implicit Extensions

#### Boolean Results

```scala
val exists: ElasticResult[Boolean] = indexExists("my-index")

exists.isTrue      // true if ElasticSuccess(true)
exists.isFalse     // true if ElasticSuccess(false)
exists.succeeded   // true if successful (ignores value)
```

#### Logging

```scala
result
  .logSuccess(logger, v => s"Version retrieved: $v")
  .logError(logger)
```

---

## Error Model

### ElasticError Structure

```scala
case class ElasticError(
  message: String,                  // Human-readable error message
  cause: Option[Throwable] = None,  // Root exception
  statusCode: Option[Int] = None,   // HTTP status code
  index: Option[String] = None,     // Related index
  operation: Option[String] = None  // ES operation name
)
```

### Full Context Message

```scala
val error = ElasticError(
  message = "Index not found",
  statusCode = Some(404),
  index = Some("my-index"),
  operation = Some("getMapping")
)

error.fullMessage
// Output: "[getMapping] index=my-index status=404 Index not found"
```

### Logging Errors

```scala
// Automatic logging with context
error.log(logger)

// Logs with exception stacktrace if cause is present
```

### Common Status Codes

| **Code**  | **Meaning**       | **Example**                            |
|-----------|-------------------|----------------------------------------|
| 200-299   | Success           | Index created, document indexed        |
| 400       | Bad Request       | Invalid index name, malformed JSON     |
| 404       | Not Found         | Index/document doesn't exist           |
| 409       | Conflict          | Version conflict, index already exists |
| 429       | Too Many Requests | Rate limiting, circuit breaker         |
| 500-599   | Server Error      | ES cluster issue, node failure         |

---

## Validation Helpers

### ElasticClientHelpers

Provides common validation methods used across all APIs:

#### Index Name Validation

```scala
protected def validateIndexName(index: String): Option[ElasticError]
```

**Elasticsearch Rules** :

- ✅ Not empty
- ✅ Lowercase only
- ✅ No characters: `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, space, comma, `#`
- ✅ Does not start with `-`, `_`, `+`
- ✅ Is not `.` or `..`
- ✅ Maximum 255 characters

**Usage** :

```scala
validateIndexName("my-index") match {
  case Some(error) => ElasticFailure(error)
  case None => // proceed with operation
}
```

#### JSON Validation

```scala
protected def validateJson(
  operation: String,
  jsonString: String
): Option[ElasticError]

protected def validateJsonSettings(settings: String): Option[ElasticError]
```

**Validates** :

- ✅ Non-empty
- ✅ No comments
- ✅ Valid JSON syntax (using json4s parser)

#### Alias Name Validation

```scala
protected def validateAliasName(alias: String): Option[ElasticError]
```

Aliases follow the same rules as index names.

#### Error Logging

```scala
protected def logError(
  operation: String,
  indexStr: String,
  error: ElasticError
): Unit
```

**Logging Levels by Status Code** :

- 404 → `DEBUG` (not always an error, e.g., indexExists)
- 400-499 → `WARN` (client error)
- 500-599 → `ERROR` (server error)
- Other → `ERROR`

---

## Execution Patterns

### REST High Level Client Helpers

The `RestHighLevelClientHelpers` trait provides generic execution methods for all REST High Level Client operations.

#### Generic Execution

```scala
private[client] def executeRestAction[Req, Resp, T](
  operation: String,
  index: Option[String] = None,
  retryable: Boolean = true
)(
  request: => Req
)(
  executor: Req => Resp
)(
  transformer: Resp => T
): ElasticResult[T]
```

**Flow** :

1. **Validation** (if needed, before calling)
2. **Execution** - Wrapped in `Try` to catch exceptions
3. **Error Handling** - Converts exceptions to `ElasticError`
4. **Transformation** - Converts response to desired type `T`

**Exemple** :

```scala
executeRestAction[CreateIndexRequest, CreateIndexResponse, Boolean](
  operation = "createIndex",
  index = Some("my-index")
)(
  request = new CreateIndexRequest("my-index")
)(
  executor = req => client.indices().create(req, RequestOptions.DEFAULT)
)(
  transformer = _.isAcknowledged
)
```

#### Boolean Operations (Acknowledged)

```scala
private[client] def executeRestBooleanAction[Req, Resp <: AcknowledgedResponse](
  operation: String,
  index: Option[String] = None,
  retryable: Boolean = true
)(
  request: => Req
)(
  executor: Req => Resp
): ElasticResult[Boolean]
```

Simplified variant for operations returning `AcknowledgedResponse`.

#### Low-Level REST Client

```scala
private[client] def executeRestLowLevelAction[T](
  operation: String,
  index: Option[String] = None,
  retryable: Boolean = true
)(
  request: => Request
)(
  transformer: Response => T
): ElasticResult[T]
```

Used for operations not available in the high-level client (e.g., `/_cat`, custom endpoints).

**Example** :

```scala
executeRestLowLevelAction[String](
  operation = "version"
)(
  request = new Request("GET", "/")
)(
  transformer = resp => {
    val json = parse(EntityUtils.toString(resp.getEntity))
    (json \\ "version" \\ "number").extract[String]
  }
)
```

#### Asynchronous Execution

```scala
private[client] def executeAsyncRestAction[Req, Resp, T](
  operation: String,
  index: Option[String] = None,
  retryable: Boolean = true
)(
  request: => Req
)(
  executor: (Req, ActionListener[Resp]) => Unit
)(
  transformer: Resp => T
)(implicit ec: ExecutionContext): Future[ElasticResult[T]]
```

Returns `Future[ElasticResult[T]]` for non-blocking operations.

### Java Client Helpers

The `JavaClientHelpers` trait provides similar generic execution methods for all Java operations.

#### Generic Execution

```scala
private[client] def executeJavaAction[Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    transformer: Resp => T
  ): ElasticResult[T]
```

#### Boolean Operations

```scala
private[client] def executeJavaBooleanAction[Resp](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    acknowledgedExtractor: Resp => Boolean
  ): ElasticResult[Boolean]
```

#### Asynchronous Execution

```scala
private[client] def executeAsyncJavaAction[Resp, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Resp
  )(
    transformer: Resp => T
  )(implicit ec: ExecutionContext): Future[ElasticResult[T]]
```

### Jest Client Helpers

The `JestClientHelpers` trait provides similar generic execution methods for all Jest operations.

#### Generic Execution

```scala
private[client] def executeJestAction[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Action[R]
  )(
    transformer: R => T
  ): ElasticResult[T]
```

#### Boolean Operations

```scala
private[client] def executeJestBooleanAction[R <: JestResult: ClassTag](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Action[R]
  ): ElasticResult[Boolean]
```

#### Asynchronous Execution

```scala
private[client] def executeAsyncJestAction[R <: JestResult: ClassTag, T](
    operation: String,
    index: Option[String] = None,
    retryable: Boolean = true
  )(
    action: => Action[R]
  )(
    transformer: R => T
  )(implicit ec: ExecutionContext): Future[ElasticResult[T]]
```

### Exception Handling

#### ElasticsearchException

```scala
case Failure(ex: ElasticsearchException) =>
  val statusCode = Option(ex.status()).map(_.getStatus)
  ElasticFailure(ElasticError(
    message = ex.getDetailedMessage,
    cause = Some(ex),
    statusCode = statusCode
  ))
```

#### ResponseException (Low-Level)

```scala
case Failure(ex: ResponseException) =>
  val statusCode = Some(ex.getResponse.getStatusLine.getStatusCode)
  ElasticFailure(ElasticError(
    message = ex.getMessage,
    cause = Some(ex),
    statusCode = statusCode
  ))
```

#### Generic Exceptions

```scala
case Failure(ex) =>
  ElasticFailure(ElasticError(
    message = ex.getMessage,
    cause = Some(ex)
  ))
```

---

## Logging Conventions

### Log Levels

| **Level**  | **Usage**                          | **Example**                                       |
|------------|------------------------------------|---------------------------------------------------|
| DEBUG      | Operation start/end, 404 responses | "Executing operation 'version'"                   |
| INFO       | Successful operations              | "✅ Elasticsearch version: 7.17.0"                 |
| WARN       | Client errors (4xx)                | "Client error during 'createIndex': Invalid name" |
| ERROR      | Server errors (5xx), exceptions    | "❌ Failed to get version: Connection timeout"     |

### Log Format

```scala
// Operation start
logger.debug(s"Executing operation '$operation'$indexStr")

// Success
logger.info(s"✅ Operation '$operation'$indexStr succeeded")

// Failure
logger.error(s"❌ Operation '$operation'$indexStr failed: ${error.message}")
```

### Emojis for Readability

- ✅ Success
- ❌ Failure
- ⚠️ Warning/Alert

---

[Back to index](README.md)