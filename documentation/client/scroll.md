[Back to index](README.md)

# SCROLL API

## Overview

The **Scroll API** provides efficient streaming access to large result sets from Elasticsearch using **Akka Streams**. It automatically selects the optimal scrolling strategy based on your Elasticsearch version, and whether aggregations are present.

**Key Features:**
- **Automatic strategy selection** (PIT + search_after, search_after, or classic scroll)
- **Akka Streams integration** for reactive data processing
- **Type-safe result conversion** with automatic deserialization
- **Built-in metrics tracking** (throughput, batches, duration)
- **Automatic error handling** with retry logic
- **Memory-efficient streaming** for large datasets
- **Configurable batch sizes** and limits

**Dependencies:**
- Requires `SearchApi` for query execution
- Requires Akka Streams for reactive streaming
- Requires `ElasticConversion` for result parsing

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [Scroll Strategies](#scroll-strategies)
3. [Configuration](#configuration)
4. [Basic Usage](#basic-usage)
5. [Typed Scrolling](#typed-scrolling)
6. [Metrics and Monitoring](#metrics-and-monitoring)
7. [Error Handling](#error-handling)
8. [Performance Tuning](#performance-tuning)
9. [Advanced Patterns](#advanced-patterns)
10. [Testing](#testing)
11. [Best Practices](#best-practices)

---

## Core Concepts

### Scrolling Strategies

The API automatically selects the best strategy based on your query and the Elasticsearch version:

**Strategy Selection Matrix:**

| ES Version      | Aggregations  | Strategy                                                                 |
|-----------------|---------------|--------------------------------------------------------------------------|
| 7.15+           | No            | PIT + search_after, **sliced** when no `ORDER BY` / no `LIMIT`: one reader per primary shard (0.21.0+) |
| 7.12+           | No            | PIT + search_after (recommended)                                         |
| 7.12+           | Yes           | Classic scroll                                                           |
| < 7.12          | No            | search_after                                                             |
| < 7.12          | Yes           | Classic scroll                                                           |

PIT paging is gated at 7.12 (not 7.10, where the PIT API first appeared): the `_shard_doc`
tiebreaker Elasticsearch appends under a PIT only exists from 7.12, and without it a paged
extraction silently drops rows across shards. PIT *slicing* (`slice` + `pit` in one request)
exists from 7.15.

---

### Scroll Strategy Types

```scala
sealed trait ScrollStrategy

// Point In Time + search_after (ES 7.12+, best performance; sliced from 7.15)
case object UsePIT extends ScrollStrategy

// search_after only (efficient, no server state)
case object UseSearchAfter extends ScrollStrategy

// Classic scroll (supports aggregations)
case object UseScroll extends ScrollStrategy
```

**Strategy Comparison:**

| Strategy               | Server State  | Aggregations  | Deep Pagination  | Timeout Issues  | Performance  |
|------------------------|---------------|---------------|------------------|-----------------|--------------|
| **PIT + search_after** | Minimal       | ❌ No          | ✅ Excellent      | ❌ None          | ⭐⭐⭐⭐⭐ |
| **search_after**       | None          | ❌ No          | ✅ Good           | ❌ None          | ⭐⭐⭐⭐   |
| **Classic scroll**     | Yes           | ✅ Yes         | ⚠️ Limited       | ⚠️ Possible     | ⭐⭐⭐       |

---

### Source Types

```scala
// Basic scroll source (returns Map with metrics)
def scroll(
  sql: SQLQuery,
  config: ScrollConfig = ScrollConfig()
)(implicit system: ActorSystem): Source[(Map[String, Any], ScrollMetrics), NotUsed]

// Typed scroll source (automatic deserialization)
def scrollAsUnchecked[T](
  sql: SQLQuery,
  config: ScrollConfig = ScrollConfig()
)(implicit
  system: ActorSystem,
  m: Manifest[T],
  formats: Formats
): Source[(T, ScrollMetrics), NotUsed]
```

---

## Scroll Strategies

### Point In Time (PIT) + search_after

**Best for:** ES 7.12+, large result sets, no aggregations

**Advantages:**
- ✅ Consistent snapshot across pagination
- ✅ No scroll timeout issues
- ✅ Better resource usage
- ✅ Automatic cleanup
- ✅ Suitable for deep pagination

**Limitations:**
- ❌ Not supported with aggregations
- ❌ Requires ES 7.12+

```scala
// Automatically used for ES 7.12+ without aggregations
val query = SQLQuery(
  query = """
    SELECT id, name, price
    FROM products
    WHERE category = 'electronics'
    ORDER BY price DESC
  """
)

// Will use PIT + search_after strategy
client.scroll(query).runWith(Sink.seq)
```

---

### Sliced PIT paging (ES 7.15+)

**Default since 0.21.0.** A no-`ORDER BY`, no-`LIMIT` extraction from an N-shard index opens **one**
PIT and reads `min(N, max-slices)` slices of it concurrently — one reader per primary shard — merged
page by page into the single stream you already consume. The sequential pipeline was latency-bound
(one round-trip per page, every page fanning out to every shard); slicing is what makes the wall
clock improve when shards and nodes are added.

**How the slice count is derived (once per stream, in core):**

| Condition | Slices |
|-----------|--------|
| Strategy is not PIT + search_after (aggregations, classic scroll opt-out, ES < 7.12) | 1 |
| The statement has an `ORDER BY` (AST or JSON `sort`) | 1 |
| The statement has an explicit `LIMIT` (incl. the `LIMIT > max_result_window` route) | 1 |
| Effective ceiling `maxSlices.getOrElse(elastic.scroll.max-slices)` is `<= 1` | 1 |
| ES < 7.15 (PIT without slicing) | 1 |
| Otherwise | `max(1, min(Σ number_of_shards of the resolved indices, ceiling))` |

The shard count comes from `GET <indices>/_settings` (the indices a wildcard, alias or data stream
resolves to are summed and deduplicated) and is **cached per index set for 5 minutes** — the same
TTL as the schema cache (`shardCountCacheTtlMs`, overridable in a subclass) — so a workload of many
small un-LIMITed queries pays one round-trip per table per TTL, not one per query (concurrent cold
extractions of the same set share one lookup). What is remembered: a positive count, and a
**privilege** failure (HTTP 401/403 — the credentials lack `view_index_metadata`), for which the
extraction logs **one WARN per TTL** naming the privilege (DEBUG while the cached failure is
replayed). A transient failure (timeout, 503, index not found, unparseable payload) is **not**
cached — every extraction probes again and logs its WARN — and neither is an expression that
matches no index, so a table created right after is seen at once. The lookup never fails the
extraction: it degrades to sequential paging. A stale count is a performance matter only (it changes
how the PIT is split, never which rows come back); every schema-cache write or invalidation on the
same client — `createIndex`, `updateSchema`, `invalidateSchema` (REPL `refresh [table]`, `DROP
TABLE`) — drops the cached counts **naming that index**, and `invalidateAllSchemas()` drops all of
them. Cache keys are the statement's `FROM` expressions, so the match is made per expression and by
case-insensitive **prefix**: invalidating `orders` drops the entries for `orders`, `orders_2026`,
`orders*` and any multi-index set containing them, but not an unrelated wildcard (`logs-*`) or an
alias that the index merely happens to match — those, like DDL issued through another client, are
seen after the TTL. `ScrollMetrics.slices` reports the resolved count on every row.

**Ceiling and opt-out:**

```scala
// Per call — Some(n) is explicit, n <= 1 pages sequentially
client.scroll(query, ScrollConfig(maxSlices = Some(2)))

// Everywhere — HOCON or environment; 1 disables slicing on every path, INCLUDING explicit
// ScrollConfig(...) values that leave maxSlices = None (the default)
//   elastic.scroll.max-slices = 1          ELASTIC_SCROLL_MAX_SLICES=1
```

`ScrollConfig.maxSlices` is an `Option` on purpose: `None` inherits the client's configured ceiling,
so the HOCON / environment switch is a real kill switch even for callers that build their own
`ScrollConfig`. The default ceiling is `ScrollConfig.DefaultMaxSlices = 8`: one slice per primary
shard up to 8, under the REST client's per-route pool — which the es7 / es8 / es9 clients now size
from `max-slices` (`max(10, max-slices + 2)` per route). Slices are never configured above the shard
count (an explicit ceiling below it is still a whole-shard split on the Elasticsearch side).

**What changes for consumers:**

- Row order of an un-ordered extraction **interleaves across slices** (it was incidentally
  `_doc`-ordered before). Add an `ORDER BY` when order matters — ordered statements stay sequential.
- A licence-quota-capped result (`maxDocuments`) is now an **arbitrary subset** of the matching
  rows, not a stable prefix.
- `SELECT *` column metadata derived from the first row (JDBC, Arrow) may vary between runs on
  heterogeneous multi-shard indices.
- In-flight memory is bounded by about `2 × slices × scrollSize` rows plus `slices` raw page
  responses (each slice keeps one page in flight while the previous one drains).
- The PIT is opened lazily (at materialization) and closed exactly once — on completion, failure
  or cancellation — whatever the slice count; a failing slice fails the whole stream (never a
  silently truncated result). A cancel that lands while the PIT-open round-trip is in flight is
  also handled (the PIT is closed as soon as its id arrives); the residual window is microseconds.
- Above the ceiling the load is uneven: 12 shards with `max-slices = 8` gives four slices that
  read two shards each and four that read one — the wall clock follows the two-shard slices.
  "One reader per primary shard" holds up to the ceiling.
- The REST pool is per client and sized to the ceiling for ONE extraction; concurrent sliced
  extractions on the same client share it and queue (they still complete — measured k = 4 at 34 s
  vs 48 s sequential — but do not scale linearly).

---

### search_after

**Best for:** ES < 7.12, large result sets, no aggregations

**Advantages:**
- ✅ No server-side state
- ✅ Efficient pagination
- ✅ No timeout issues
- ✅ Good for deep pagination

**Limitations:**
- ❌ Not supported with aggregations
- ❌ Requires sort fields
- ⚠️ No consistent snapshot (data can change between pages)

```scala
// Automatically used for ES < 7.12 without aggregations
val query = SQLQuery(
  query = """
    SELECT id, name, price
    FROM products
    WHERE category = 'electronics'
    ORDER BY created_at DESC, id ASC
  """
)

// Will use search_after strategy
client.scroll(query).runWith(Sink.seq)
```

---

### Classic Scroll

**Best for:** Queries with aggregations, consistent snapshots required

**Advantages:**
- ✅ Supports aggregations
- ✅ Consistent snapshot
- ✅ Works on all ES versions
- ✅ Automatic cleanup

**Limitations:**
- ⚠️ Server-side state (scroll context)
- ⚠️ Subject to scroll timeout
- ⚠️ Higher resource usage
- ⚠️ Limited deep pagination

```scala
// Automatically used when aggregations are present
val query = SQLQuery(
  query = """
    SELECT 
      category,
      COUNT(*) as total,
      AVG(price) as avg_price
    FROM products
    GROUP BY category
  """
)

// Will use classic scroll strategy
client.scroll(query).runWith(Sink.seq)
```

---

## Configuration

### ScrollConfig

```scala
case class ScrollConfig(
  // Batch size (documents per request)
  scrollSize: Int = 1000,
  
  // Keep-alive time for scroll context
  keepAlive: String = "1m",
  
  // Maximum documents to retrieve (None = unlimited)
  maxDocuments: Option[Long] = None,
  
  // Prefer search_after over classic scroll
  preferSearchAfter: Boolean = true,
  
  // Log progress every N batches
  logEvery: Int = 10,
  
  // Initial metrics
  metrics: ScrollMetrics = ScrollMetrics(),

  // Retry configuration
  retryConfig: RetryConfig = RetryConfig(),

  // Ceiling on concurrent PIT slices for a no-ORDER-BY extraction (ES 7.15+, 0.21.0+).
  // None = inherit the client's `elastic.scroll.max-slices`; Some(n) explicit, n <= 1 sequential
  maxSlices: Option[Int] = None,

  // Internal (set by ScrollApi, not by callers): the slice count resolved for this stream
  slices: Int = 1
)
```

When no configuration is passed, `client.scroll(query)` uses `client.defaultScrollConfig`: the page
size comes from `elastic.scroll.size` and the slice ceiling from `elastic.scroll.max-slices`
(see [Sliced PIT paging](#sliced-pit-paging-es-715)). An explicit `ScrollConfig(...)` keeps its
explicit fields but still inherits the configured ceiling through `maxSlices = None`.

**Configuration Options:**

| Parameter           | Type            | Default           | Description                                  |
|---------------------|-----------------|-------------------|----------------------------------------------|
| `scrollSize`        | `Int`           | `1000`            | Number of documents per batch (`elastic.scroll.size` when omitted) |
| `keepAlive`         | `String`        | `"1m"`            | Scroll / PIT context keep-alive              |
| `maxDocuments`      | `Option[Long]`  | `None`            | Maximum documents to retrieve (binds on the merged total when sliced) |
| `preferSearchAfter` | `Boolean`       | `true`            | Prefer search_after when available           |
| `logEvery`          | `Int`           | `10`              | Log progress every N batches                 |
| `metrics`           | `ScrollMetrics` | `ScrollMetrics()` | Initial metrics state                        |
| `maxSlices`         | `Option[Int]`   | `None`            | Ceiling on concurrent PIT slices; `None` inherits `elastic.scroll.max-slices` (default 8), `Some(1)` pages sequentially |
| `slices`            | `Int`           | `1`               | Internal — the resolved slice count (read it on `ScrollMetrics.slices`) |

---

### ScrollMetrics

```scala
case class ScrollMetrics(
  totalDocuments: Long = 0,
  totalBatches: Int = 0,
  startTime: Long = System.currentTimeMillis(),
  endTime: Option[Long] = None,
  slices: Int = 1 // PIT slices merged into this stream (1 = sequential)
) {
  // Calculate duration in milliseconds
  def duration: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime
  
  // Calculate throughput (documents per second)
  def documentsPerSecond: Double = {
    val durationSec = duration / 1000.0
    if (durationSec > 0) totalDocuments / durationSec else 0.0
  }
  
  // Mark as complete
  def complete: ScrollMetrics = copy(endTime = Some(System.currentTimeMillis()))
}
```

**Metrics Fields:**

| Field                | Type           | Description                    |
|----------------------|----------------|--------------------------------|
| `totalDocuments`     | `Long`         | Total documents retrieved      |
| `totalBatches`       | `Int`          | Total batches processed        |
| `startTime`          | `Long`         | Start timestamp (milliseconds) |
| `endTime`            | `Option[Long]` | End timestamp (milliseconds)   |
| `slices`             | `Int`          | PIT slices merged into this stream (1 = sequential) |
| `duration`           | `Long`         | Total duration (milliseconds)  |
| `documentsPerSecond` | `Double`       | Throughput rate                |

---

## Basic Usage

### Simple Scrolling

```scala
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import scala.concurrent.ExecutionContext.Implicits.global

implicit val system: ActorSystem = ActorSystem("scroll-example")

// Simple SQL query
val query = SQLQuery(
  query = """
    SELECT id, name, price, category
    FROM products
    WHERE price > 100
    ORDER BY price DESC
  """
)

// Scroll through results
client.scroll(query).runWith(Sink.foreach { case (doc, metrics) =>
  println(s"Document: $doc")
  println(s"Progress: ${metrics.totalDocuments} docs, ${metrics.documentsPerSecond} docs/sec")
})
```

---

### Collecting All Results

```scala
// Collect all documents into a sequence
val allDocs: Future[Seq[(Map[String, Any], ScrollMetrics)]] = 
  client.scroll(query).runWith(Sink.seq)

allDocs.foreach { results =>
  println(s"Retrieved ${results.size} documents")
  
  // Get final metrics
  val finalMetrics = results.lastOption.map(_._2)
  finalMetrics.foreach { m =>
    println(s"Total time: ${m.duration}ms")
    println(s"Throughput: ${m.documentsPerSecond} docs/sec")
  }
  
  // Process documents
  results.foreach { case (doc, _) =>
    println(s"ID: ${doc.get("id")}, Name: ${doc.get("name")}")
  }
}
```

---

### Limited Scrolling

```scala
// Limit to first 5000 documents
val config = ScrollConfig(
  scrollSize = 500,
  maxDocuments = Some(5000)
)

client.scroll(query, config).runWith(Sink.foreach { case (doc, metrics) =>
  println(s"Document ${metrics.totalDocuments}: ${doc.get("name")}")
})
```

---

### Custom Batch Size

```scala
// Process in batches of 2000
val config = ScrollConfig(scrollSize = 2000)

client.scroll(query, config)
  .grouped(2000)
  .runWith(Sink.foreach { batch =>
    println(s"Processing batch of ${batch.size} documents")
    // Batch processing logic
    processBatch(batch.map(_._1))
  })
```

---

## Typed Scrolling

### Basic Typed Scrolling

```scala
case class Product(
  id: String,
  name: String,
  price: Double,
  category: String,
  stock: Int
)

implicit val formats: Formats = DefaultFormats

val query = SQLQuery(
  query = """
    SELECT id, name, price, category, stock
    FROM products
    WHERE category = 'electronics'
  """
)

// Scroll with automatic type conversion
client.scrollAsUnchecked[Product](query).runWith(Sink.foreach { case (product, metrics) =>
  println(s"Product: ${product.name} - $${product.price}")
  println(s"Progress: ${metrics.totalDocuments} products")
})
```

---

### Collecting Typed Results

```scala
// Collect all products
val allProducts: Future[Seq[Product]] = 
  client.scrollAsUnchecked[Product](query)
    .map(_._1) // Extract product, discard metrics
    .runWith(Sink.seq)

allProducts.foreach { products =>
  println(s"Retrieved ${products.size} products")
  
  val totalValue = products.map(_.price).sum
  println(f"Total inventory value: $$${totalValue}%,.2f")
}
```

---

### Filtering Typed Results

```scala
// Filter expensive products during streaming
client.scrollAsUnchecked[Product](query)
  .filter { case (product, _) => product.price > 500 }
  .map(_._1) // Extract product
  .runWith(Sink.seq)
  .foreach { expensiveProducts =>
    println(s"Found ${expensiveProducts.size} expensive products")
    expensiveProducts.foreach { p =>
      println(s"  ${p.name}: $${p.price}")
    }
  }
```

---

### Transforming Typed Results

```scala
case class ProductSummary(name: String, value: Double)

client.scrollAsUnchecked[Product](query)
  .map { case (product, _) =>
    ProductSummary(
      name = product.name,
      value = product.price * product.stock
    )
  }
  .runWith(Sink.seq)
  .foreach { summaries =>
    val totalValue = summaries.map(_.value).sum
    println(f"Total inventory value: $$${totalValue}%,.2f")
  }
```

### Validating Query at compile-time

```scala
val query = 
  """
    SELECT id, name, price, category, stock
    FROM products
    WHERE category = 'electronics'
  """

// scrollAs is a macro: the config must be passed explicitly (macro applications take no default
// arguments) — `client.defaultScrollConfig` carries the `elastic.scroll` settings
client.scrollAs[Product](query, client.defaultScrollConfig)
  .map { case (product, _) =>
    ProductSummary(
      name = product.name,
      value = product.price * product.stock
    )
  }
  .runWith(Sink.seq)
  .foreach { summaries =>
    val totalValue = summaries.map(_.value).sum
    println(f"Total inventory value: $$${totalValue}%,.2f")
  }
)

```

📖 **[Full SQL Validation Documentation](../sql/validation.md)**

---

## Metrics and Monitoring

### Tracking Progress

```scala
val config = ScrollConfig(
  scrollSize = 1000,
  logEvery = 5 // Log every 5 batches
)

client.scroll(query, config).runWith(Sink.foreach { case (doc, metrics) =>
  // Metrics are automatically updated
  if (metrics.totalBatches % 5 == 0) {
    println(s"Progress Report:")
    println(s"  Documents: ${metrics.totalDocuments}")
    println(s"  Batches: ${metrics.totalBatches}")
    println(s"  Duration: ${metrics.duration}ms")
    println(s"  Throughput: ${metrics.documentsPerSecond} docs/sec")
  }
})

// Output:
// Progress Report:
//   Documents: 5000
//   Batches: 5
//   Duration: 2345ms
//   Throughput: 2132.2 docs/sec
```

---

### Final Metrics

```scala
client.scroll(query)
  .runWith(Sink.last)
  .foreach { case (_, finalMetrics) =>
    val completed = finalMetrics.complete
    
    println("Scroll Completed!")
    println(s"  Total Documents: ${completed.totalDocuments}")
    println(s"  Total Batches: ${completed.totalBatches}")
    println(s"  Total Duration: ${completed.duration}ms")
    println(s"  Average Throughput: ${completed.documentsPerSecond} docs/sec")
  }
```

---

### Custom Metrics Tracking

```scala
case class CustomMetrics(
  scrollMetrics: ScrollMetrics,
  processedCount: Long = 0,
  errorCount: Long = 0,
  skippedCount: Long = 0
)

client.scroll(query)
  .scan(CustomMetrics(ScrollMetrics())) { case (custom, (doc, scrollMetrics)) =>
    // Update custom metrics
    val processed = if (processDocument(doc)) {
      custom.copy(
        scrollMetrics = scrollMetrics,
        processedCount = custom.processedCount + 1
      )
    } else {
      custom.copy(
        scrollMetrics = scrollMetrics,
        skippedCount = custom.skippedCount + 1
      )
    }
    processed
  }
  .runWith(Sink.last)
  .foreach { finalCustom =>
    println(s"Processed: ${finalCustom.processedCount}")
    println(s"Skipped: ${finalCustom.skippedCount}")
    println(s"Errors: ${finalCustom.errorCount}")
  }

def processDocument(doc: Map[String, Any]): Boolean = {
  // Processing logic
  true
}
```

---

## Error Handling

### Built-in Error Handling

The API automatically handles:

- ✅ Network timeouts (with retry)
- ✅ Expired scroll contexts
- ✅ Elasticsearch errors
- ✅ Connection issues

### A page that cannot be read fails the stream

**Since 0.23.0.** Every paging path — classic scroll, `search_after` and PIT — decides
end-of-stream on the **raw** page, never on the converted rows, and a page whose body cannot be
converted raises a non-retriable `IllegalStateException` that fails the stream. Earlier versions
logged the conversion failure and returned an empty page, which the paging loop read as "no more
data": the consumer received a **silently truncated** result set presented as a success. Extractions
that used to "succeed" with fewer rows now fail loudly, so a `recover`/`recoverWith` on the stream
is the right place to decide what a partial read means for your application.

The failure is deliberately **not** retried: a scroll cursor that has already advanced cannot be
re-polled without skipping rows.

```scala
// Automatic error handling is built-in
client.scroll(query).runWith(Sink.seq).recover {
  case ex: Exception =>
    logger.error("Scroll failed", ex)
    Seq.empty
}
```

---

### Custom Error Recovery

```scala
import akka.stream.Supervision

// Define custom recovery strategy
implicit val decider: Supervision.Decider = {
  case _: java.net.SocketTimeoutException =>
    logger.warn("Timeout, resuming...")
    Supervision.Resume
    
  case _: org.elasticsearch.ElasticsearchException =>
    logger.error("ES error, stopping...")
    Supervision.Stop
    
  case ex =>
    logger.error(s"Unexpected error: ${ex.getMessage}")
    Supervision.Stop
}

// Apply supervision strategy
client.scroll(query)
  .withAttributes(ActorAttributes.supervisionStrategy(decider))
  .runWith(Sink.seq)
```

---

### Retry Logic

```scala
import akka.stream.scaladsl.RetryFlow
import scala.concurrent.duration._

// Add retry logic for failed batches
client.scroll(query)
  .via(RetryFlow.withBackoff(
    minBackoff = 1.second,
    maxBackoff = 10.seconds,
    randomFactor = 0.2,
    maxRetries = 3
  ) { case (doc, _) =>
    Future {
      processDocument(doc)
    }
  })
  .runWith(Sink.seq)
```

---

### Error Logging

```scala
client.scroll(query)
  .recover {
    case ex: java.net.SocketTimeoutException =>
      logger.error("Network timeout", ex)
      throw ex
      
    case ex: org.elasticsearch.ElasticsearchException =>
      logger.error(s"Elasticsearch error: ${ex.getMessage}", ex)
      throw ex
      
    case ex: Exception =>
      logger.error("Unexpected error during scroll", ex)
      throw ex
  }
  .runWith(Sink.seq)
  .recover {
    case ex =>
      logger.error("Failed to complete scroll", ex)
      Seq.empty
  }
```

---

### Graceful Degradation

```scala
def scrollWithFallback(query: SQLQuery): Future[Seq[Map[String, Any]]] = {
  client.scroll(query)
    .map(_._1) // Extract documents
    .runWith(Sink.seq)
    .recoverWith {
      case ex: Exception =>
        logger.warn(s"Scroll failed, trying regular search: ${ex.getMessage}")
        
        // Fallback to regular search
        client.search(query).map {
          case ElasticSuccess(results) => results
          case ElasticFailure(error) =>
            logger.error(s"Fallback also failed: ${error.message}")
            Seq.empty
        }
    }
}
```

---

## Performance Tuning

### Optimal Batch Size

```scala
// Small documents (< 1KB each)
val smallDocConfig = ScrollConfig(scrollSize = 5000)

// Medium documents (1-10KB each)
val mediumDocConfig = ScrollConfig(scrollSize = 1000)

// Large documents (> 10KB each)
val largeDocConfig = ScrollConfig(scrollSize = 100)

// Choose based on document size
val config = if (avgDocSize < 1024) smallDocConfig
            else if (avgDocSize < 10240) mediumDocConfig
            else largeDocConfig

client.scroll(query, config).runWith(Sink.seq)
```

---

### Parallel Processing

```scala
// Process batches in parallel
client.scroll(query)
  .grouped(1000)
  .mapAsync(parallelism = 4) { batch =>
    Future {
      // Parallel batch processing
      processBatchInParallel(batch.map(_._1))
    }
  }
  .runWith(Sink.ignore)
```

---

### Memory Management

```scala
// Stream to file to avoid memory issues
import java.io.PrintWriter

val writer = new PrintWriter("results.json")

client.scroll(query)
  .map { case (doc, _) => 
    // Convert to JSON string
    compact(render(Extraction.decompose(doc)))
  }
  .runWith(Sink.foreach { json =>
    writer.println(json)
  })
  .onComplete { _ =>
    writer.close()
    println("Results written to file")
  }
```

---

### Backpressure Handling

```scala
// Add buffer to handle backpressure
client.scroll(query)
  .buffer(100, OverflowStrategy.backpressure)
  .mapAsync(parallelism = 2) { case (doc, _) =>
    // Slow processing
    processDocumentAsync(doc)
  }
  .runWith(Sink.seq)
```

---

### Throttling

```scala
import scala.concurrent.duration._

// Throttle to 100 documents per second
client.scroll(query)
  .throttle(100, 1.second)
  .runWith(Sink.foreach { case (doc, metrics) =>
    println(s"Processing: ${doc.get("id")}")
  })
```

---

## Advanced Patterns

### Batch Processing with Commit

```scala
// Process in batches with commit points
client.scroll(query)
  .grouped(1000)
  .mapAsync(1) { batch =>
    for {
      _ <- processBatch(batch.map(_._1))
      _ <- commitBatch(batch.size)
    } yield batch.size
  }
  .runWith(Sink.fold(0)(_ + _))
  .foreach { total =>
    println(s"Processed and committed $total documents")
  }

def processBatch(docs: Seq[Map[String, Any]]): Future[Unit] = {
  // Batch processing logic
  Future.successful(())
}

def commitBatch(size: Int): Future[Unit] = {
  // Commit logic (e.g., database transaction)
  Future.successful(())
}
```

---

### Data Transformation Pipeline

```scala
case class RawProduct(id: String, name: String, price: Double)
case class EnrichedProduct(id: String, name: String, price: Double, category: String, tags: Seq[String])

client.scrollAsUnchecked[RawProduct](query)
  .mapAsync(parallelism = 4) { case (raw, _) =>
    // Enrich each product
    enrichProduct(raw)
  }
  .filter(_.tags.nonEmpty) // Filter enriched products
  .grouped(100)
  .mapAsync(1) { batch =>
    // Bulk index enriched products
    bulkIndexProducts(batch)
  }
  .runWith(Sink.ignore)

def enrichProduct(raw: RawProduct): Future[EnrichedProduct] = {
  // Enrichment logic (e.g., external API call)
  Future.successful(
    EnrichedProduct(
      raw.id,
      raw.name,
      raw.price,
      "electronics",
      Seq("popular", "sale")
    )
  )
}

def bulkIndexProducts(products: Seq[EnrichedProduct]): Future[Unit] = {
  // Bulk indexing logic
  Future.successful(())
}
```

---

### Fan-Out Processing

```scala
import akka.stream.scaladsl.Broadcast

// Fan-out to multiple sinks
val graph = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>
  import GraphDSL.Implicits._
  
  val source = client.scroll(query).map(_._1)
  val broadcast = builder.add(Broadcast[Map[String, Any]](3))
  
  // Sink 1: Write to file
  val fileSink = Sink.foreach[Map[String, Any]] { doc =>
    writeToFile(doc)
  }
  
  // Sink 2: Index to another ES
  val indexSink = Sink.foreach[Map[String, Any]] { doc =>
    indexToElasticsearch(doc)
  }
  
  // Sink 3: Send to Kafka
  val kafkaSink = Sink.foreach[Map[String, Any]] { doc =>
    sendToKafka(doc)
  }
  
  source ~> broadcast
  broadcast ~> fileSink
  broadcast ~> indexSink
  broadcast ~> kafkaSink
  
  ClosedShape
})

graph.run()

def writeToFile(doc: Map[String, Any]): Unit = { /* ... */ }
def indexToElasticsearch(doc: Map[String, Any]): Unit = { /* ... */ }
def sendToKafka(doc: Map[String, Any]): Unit = { /* ... */ }
```

---

### Aggregating During Scroll

```scala
case class Statistics(
  count: Long = 0,
  sum: Double = 0.0,
  min: Double = Double.MaxValue,
  max: Double = Double.MinValue
) {
  def avg: Double = if (count > 0) sum / count else 0.0
  
  def update(value: Double): Statistics = Statistics(
    count = count + 1,
    sum = sum + value,
    min = math.min(min, value),
    max = math.max(max, value)
  )
}

client.scrollAsUnchecked[Product](query)
  .map(_._1.price) // Extract prices
  .fold(Statistics())(_ update _)
  .runWith(Sink.head)
  .foreach { stats =>
    println(s"Price Statistics:")
    println(f"  Count: ${stats.count}")
    println(f"  Average: $$${stats.avg}%.2f")
    println(f"  Min: $$${stats.min}%.2f")
    println(f"  Max: $$${stats.max}%.2f")
  }
```

---

### Conditional Processing

```scala
client.scrollAsUnchecked[Product](query)
  .mapAsync(parallelism = 4) { case (product, _) =>
    product.category match {
      case "electronics" => processElectronics(product)
      case "clothing" => processClothing(product)
      case "books" => processBooks(product)
      case _ => processGeneric(product)
    }
  }
  .runWith(Sink.ignore)

def processElectronics(p: Product): Future[Unit] = Future.successful(())
def processClothing(p: Product): Future[Unit] = Future.successful(())
def processBooks(p: Product): Future[Unit] = Future.successful(())
def processGeneric(p: Product): Future[Unit] = Future.successful(())
```

---

## Testing

### Test Basic Scrolling

```scala
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers
import akka.stream.scaladsl.Sink

class ScrollApiSpec extends AsyncFlatSpec with Matchers {
  
  implicit val system: ActorSystem = ActorSystem("test")
  
  "ScrollApi" should "scroll through all documents" in {
    val testIndex = "test-scroll"
    
    for {
      // Setup
      _ <- client.createIndexAsync(testIndex)
      _ <- Future.sequence((1 to 100).map { i =>
        client.indexAsync(testIndex, i.toString, s"""{"id": $i, "value": ${i * 10}}""")
      })
      _ <- client.refreshAsync(testIndex)
      
      // Test
      query = SQLQuery(query = s"SELECT * FROM $testIndex")
      results <- client.scroll(query).map(_._1).runWith(Sink.seq)
      
      // Assertions
      _ = {
        results should have size 100
        results.map(_("id").toString.toInt).sorted shouldBe (1 to 100)
      }
      
      // Cleanup
      _ <- client.deleteIndexAsync(testIndex)
    } yield succeed
  }
}
```

---

### Test Typed Scrolling

```scala
"ScrollApi" should "scroll with type conversion" in {
  case class TestDoc(id: Int, value: Int)
  implicit val formats: Formats = DefaultFormats
  
  val testIndex = "test-typed-scroll"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 50).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"id": $i, "value": ${i * 10}}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(query = s"SELECT id, value FROM $testIndex")
    results <- client.scrollAsUnchecked[TestDoc](query).map(_._1).runWith(Sink.seq)
    
    // Assertions
    _ = {
      results should have size 50
      results.map(_.id).sorted shouldBe (1 to 50)
      results.foreach { doc =>
        doc.value shouldBe doc.id * 10
      }
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

## Test Metrics Tracking

```scala
"ScrollApi" should "track metrics correctly" in {
  val testIndex = "test-metrics"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 1000).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"id": $i}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(query = s"SELECT * FROM $testIndex")
    config = ScrollConfig(scrollSize = 100)
    
    lastMetrics <- client.scroll(query, config)
      .map(_._2)
      .runWith(Sink.last)
    
    // Assertions
    _ = {
      val finalMetrics = lastMetrics.complete
      finalMetrics.totalDocuments shouldBe 1000
      finalMetrics.totalBatches shouldBe 10
      finalMetrics.duration should be > 0L
      finalMetrics.documentsPerSecond should be > 0.0
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Max Documents Limit

```scala
"ScrollApi" should "respect maxDocuments limit" in {
  val testIndex = "test-limit"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 1000).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"id": $i}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test with limit
    query = SQLQuery(query = s"SELECT * FROM $testIndex")
    config = ScrollConfig(
      scrollSize = 100,
      maxDocuments = Some(250)
    )
    
    results <- client.scroll(query, config).map(_._1).runWith(Sink.seq)
    
    // Assertions
    _ = {
      results.size shouldBe 250
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Error Recovery

```scala
"ScrollApi" should "handle errors gracefully" in {
  val testIndex = "test-error-recovery"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 100).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"id": $i}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test with error handling
    query = SQLQuery(query = s"SELECT * FROM $testIndex")
    
    result <- client.scroll(query)
      .map(_._1)
      .runWith(Sink.seq)
      .recover {
        case ex: Exception =>
          // Should recover from errors
          Seq.empty
      }
    
    // Assertions
    _ = {
      result should not be empty
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Empty Index

```scala
"ScrollApi" should "handle empty index" in {
  val testIndex = "test-empty"
  
  for {
    // Setup empty index
    _ <- client.createIndexAsync(testIndex)
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(query = s"SELECT * FROM $testIndex")
    results <- client.scroll(query).map(_._1).runWith(Sink.seq)
    
    // Assertions
    _ = {
      results shouldBe empty
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Batch Processing

```scala
"ScrollApi" should "process documents in batches" in {
  val testIndex = "test-batches"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 500).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"id": $i}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(query = s"SELECT * FROM $testIndex")
    config = ScrollConfig(scrollSize = 100)
    
    batches <- client.scroll(query, config)
      .map(_._1)
      .grouped(100)
      .runWith(Sink.seq)
    
    // Assertions
    _ = {
      batches should have size 5
      batches.foreach { batch =>
        batch should have size 100
      }
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

## Best Practices

### 1. Choose Appropriate Batch Size

```scala
// ❌ BAD: Too small batch size (too many requests)
val badConfig = ScrollConfig(scrollSize = 10)

// ❌ BAD: Too large batch size (memory issues)
val tooBigConfig = ScrollConfig(scrollSize = 50000)

// ✅ GOOD: Reasonable batch size based on document size
val goodConfig = ScrollConfig(
  scrollSize = if (avgDocumentSize < 1024) 5000
              else if (avgDocumentSize < 10240) 1000
              else 100
)

client.scroll(query, goodConfig).runWith(Sink.seq)
```

---

### 2. Always Set maxDocuments for Safety

```scala
// ❌ BAD: No limit (could consume all memory)
client.scroll(query).runWith(Sink.seq)

// ✅ GOOD: Set reasonable limit
val config = ScrollConfig(
  scrollSize = 1000,
  maxDocuments = Some(100000) // Safety limit
)

client.scroll(query, config).runWith(Sink.seq)
```

---

### 3. Use Typed Scrolling When Possible

```scala
case class Product(id: String, name: String, price: Double)

// ❌ BAD: Manual type conversion
client.scroll(query).map { case (doc, _) =>
  Product(
    doc("id").toString,
    doc("name").toString,
    doc("price").toString.toDouble
  )
}.runWith(Sink.seq)

// ✅ GOOD: Automatic type conversion
implicit val formats: Formats = DefaultFormats
client.scrollAsUnchecked[Product](query)
  .map(_._1)
  .runWith(Sink.seq)
```

---

### 4. Handle Backpressure

```scala
// ✅ GOOD: Add buffer for backpressure handling
client.scroll(query)
  .buffer(100, OverflowStrategy.backpressure)
  .mapAsync(parallelism = 4) { case (doc, _) =>
    // Slow async processing
    processDocumentAsync(doc)
  }
  .runWith(Sink.ignore)
```

---

### 5. Monitor Progress with Metrics

```scala
// ✅ GOOD: Log progress regularly
val config = ScrollConfig(
  scrollSize = 1000,
  logEvery = 10 // Log every 10 batches
)

client.scroll(query, config)
  .runWith(Sink.foreach { case (doc, metrics) =>
    // Metrics are automatically logged
    // Custom processing here
    processDocument(doc)
  })
```

---

### 6. Clean Up Resources

```scala
// ✅ GOOD: Ensure proper cleanup
val scrollFuture = client.scroll(query).runWith(Sink.seq)

scrollFuture.onComplete {
  case Success(results) =>
    logger.info(s"Scroll completed: ${results.size} documents")
    // Cleanup is automatic
    
  case Failure(ex) =>
    logger.error("Scroll failed", ex)
    // Cleanup is automatic even on failure
}
```

---

### 7. Use Appropriate Strategy

```scala
// ✅ GOOD: Let the API choose the best strategy
val query = SQLQuery(
  query = """
    SELECT id, name, price
    FROM products
    WHERE category = 'electronics'
    ORDER BY price DESC
  """
)

// Automatically uses:
// - PIT + search_after for ES 7.12+ (best performance; sliced from 7.15)
// - search_after for ES < 7.12
// - Classic scroll for aggregations

client.scroll(query).runWith(Sink.seq)
```

---

### 8. Handle Large Result Sets Efficiently

```scala
// ✅ GOOD: Stream to file instead of collecting in memory
import java.io.{BufferedWriter, FileWriter}

val writer = new BufferedWriter(new FileWriter("results.jsonl"))

client.scroll(query)
  .map { case (doc, _) => 
    compact(render(Extraction.decompose(doc)))
  }
  .runWith(Sink.foreach { json =>
    writer.write(json)
    writer.newLine()
  })
  .onComplete { _ =>
    writer.close()
    logger.info("Results written to file")
  }
```

---

### 9. Implement Proper Error Handling

```scala
// ✅ GOOD: Comprehensive error handling
implicit val decider: Supervision.Decider = {
  case _: java.net.SocketTimeoutException =>
    logger.warn("Network timeout, resuming...")
    Supervision.Resume
    
  case ex: org.elasticsearch.ElasticsearchException =>
    logger.error(s"ES error: ${ex.getMessage}")
    Supervision.Stop
    
  case ex =>
    logger.error(s"Unexpected error: ${ex.getMessage}", ex)
    Supervision.Stop
}

client.scroll(query)
  .withAttributes(ActorAttributes.supervisionStrategy(decider))
  .runWith(Sink.seq)
  .recover {
    case ex: Exception =>
      logger.error("Failed to complete scroll", ex)
      Seq.empty
  }
```

---

### 10. Optimize Queries

```scala
// ❌ BAD: Select all fields (wastes bandwidth)
val badQuery = SQLQuery(query = "SELECT * FROM products")

// ✅ GOOD: Select only needed fields
val goodQuery = SQLQuery(
  query = """
    SELECT id, name, price
    FROM products
    WHERE category = 'electronics'
    AND price > 100
    ORDER BY price DESC
  """
)

client.scroll(goodQuery).runWith(Sink.seq)
```

---

### 11. Use Parallel Processing Wisely

```scala
// ✅ GOOD: Balance parallelism with resources
val parallelism = Runtime.getRuntime.availableProcessors()

client.scroll(query)
  .mapAsync(parallelism) { case (doc, _) =>
    // Process documents in parallel
    processDocumentAsync(doc)
  }
  .runWith(Sink.ignore)
```

---

### 12. Test Scroll Behavior

```scala
// ✅ GOOD: Test with different scenarios
class ScrollBehaviorSpec extends AsyncFlatSpec with Matchers {
  
  "Scroll" should "work with small datasets" in {
    testScroll(documentCount = 100)
  }
  
  it should "work with large datasets" in {
    testScroll(documentCount = 10000)
  }
  
  it should "work with empty results" in {
    testScroll(documentCount = 0)
  }
  
  it should "respect maxDocuments limit" in {
    testScrollWithLimit(
      documentCount = 1000,
      maxDocuments = 500
    )
  }
  
  def testScroll(documentCount: Int): Future[Assertion] = {
    // Test implementation
    Future.successful(succeed)
  }
  
  def testScrollWithLimit(
    documentCount: Int,
    maxDocuments: Int
  ): Future[Assertion] = {
    // Test implementation
    Future.successful(succeed)
  }
}
```

---

## Common Patterns

### Pattern 1: Export to File

```scala
def exportToFile(
  query: SQLQuery,
  outputPath: String
): Future[Long] = {
  val writer = new PrintWriter(new FileWriter(outputPath))
  
  client.scroll(query)
    .map { case (doc, _) => 
      compact(render(Extraction.decompose(doc)))
    }
    .runWith(Sink.fold(0L) { (count, json) =>
      writer.println(json)
      count + 1
    })
    .andThen {
      case _ => writer.close()
    }
}

// Usage
exportToFile(
  SQLQuery(query = "SELECT * FROM products"),
  "products.jsonl"
).foreach { count =>
  println(s"Exported $count documents")
}
```

---

### Pattern 2: Bulk Reindex

```scala
def bulkReindex(
  sourceQuery: SQLQuery,
  targetIndex: String,
  batchSize: Int = 1000
): Future[Long] = {
  client.scroll(sourceQuery)
    .map(_._1) // Extract documents
    .grouped(batchSize)
    .mapAsync(1) { batch =>
      // Bulk index to target
      val bulkRequest = batch.map { doc =>
        s"""{"index":{"_index":"$targetIndex"}}
           |${compact(render(Extraction.decompose(doc)))}
           |""".stripMargin
      }.mkString
      
      client.bulkAsync(bulkRequest).map(_ => batch.size)
    }
    .runWith(Sink.fold(0L)(_ + _))
}

// Usage
bulkReindex(
  SQLQuery(query = "SELECT * FROM old_products"),
  "new_products"
).foreach { count =>
  println(s"Reindexed $count documents")
}
```

---

### Pattern 3: Data Validation

```scala
case class ValidationResult(
  valid: Long,
  invalid: Long,
  errors: Seq[String]
)

def validateData(query: SQLQuery): Future[ValidationResult] = {
  client.scrollAsUnchecked[Product](query)
    .map(_._1)
    .runWith(Sink.fold(ValidationResult(0, 0, Seq.empty)) { (result, product) =>
      if (isValid(product)) {
        result.copy(valid = result.valid + 1)
      } else {
        result.copy(
          invalid = result.invalid + 1,
          errors = result.errors :+ s"Invalid product: ${product.id}"
        )
      }
    })
}

def isValid(product: Product): Boolean = {
  product.price > 0 && product.name.nonEmpty
}

// Usage
validateData(SQLQuery(query = "SELECT * FROM products")).foreach { result =>
  println(s"Valid: ${result.valid}")
  println(s"Invalid: ${result.invalid}")
  if (result.errors.nonEmpty) {
    println("Errors:")
    result.errors.take(10).foreach(println)
  }
}
```

---

### Pattern 4: Data Aggregation

```scala
case class CategoryStats(
  category: String,
  count: Long,
  totalValue: Double,
  avgPrice: Double
)

def aggregateByCategory(query: SQLQuery): Future[Map[String, CategoryStats]] = {
  client.scrollAsUnchecked[Product](query)
    .map(_._1)
    .runWith(Sink.fold(Map.empty[String, CategoryStats]) { (stats, product) =>
      val current = stats.getOrElse(
        product.category,
        CategoryStats(product.category, 0, 0.0, 0.0)
      )
      
      val updated = CategoryStats(
        category = product.category,
        count = current.count + 1,
        totalValue = current.totalValue + product.price,
        avgPrice = (current.totalValue + product.price) / (current.count + 1)
      )
      
      stats + (product.category -> updated)
    })
}

// Usage
aggregateByCategory(SQLQuery(query = "SELECT * FROM products")).foreach { stats =>
  println("Category Statistics:")
  stats.values.foreach { s =>
    println(s"  ${s.category}:")
    println(f"    Count: ${s.count}")
    println(f"    Total Value: $$${s.totalValue}%,.2f")
    println(f"    Avg Price: $$${s.avgPrice}%.2f")
  }
}
```

---

### Pattern 5: Data Transformation Pipeline

```scala
case class RawOrder(id: String, customerId: String, total: Double, items: Seq[String])
case class EnrichedOrder(
  id: String,
  customerId: String,
  customerName: String,
  total: Double,
  itemCount: Int,
  category: String
)

def transformOrders(query: SQLQuery): Future[Seq[EnrichedOrder]] = {
  client.scrollAsUnchecked[RawOrder](query)
    .map(_._1)
    .mapAsync(parallelism = 4) { order =>
      // Enrich with customer data
      fetchCustomerName(order.customerId).map { customerName =>
        EnrichedOrder(
          id = order.id,
          customerId = order.customerId,
          customerName = customerName,
          total = order.total,
          itemCount = order.items.size,
          category = categorizeOrder(order)
        )
      }
    }
    .filter(_.itemCount > 0) // Filter empty orders
    .runWith(Sink.seq)
}

def fetchCustomerName(customerId: String): Future[String] = {
  // Fetch from database or cache
  Future.successful(s"Customer $customerId")
}

def categorizeOrder(order: RawOrder): String = {
  if (order.total > 1000) "premium"
  else if (order.total > 100) "standard"
  else "basic"
}

// Usage
transformOrders(SQLQuery(query = "SELECT * FROM orders")).foreach { enriched =>
  println(s"Transformed ${enriched.size} orders")
}
```

---

## Summary

The **Scroll API** provides:

✅ **Automatic strategy selection** for optimal performance  
✅ **Akka Streams integration** for reactive processing  
✅ **Type-safe scrolling** with automatic deserialization  
✅ **Built-in metrics tracking** for monitoring  
✅ **Automatic error handling** with retry logic  
✅ **Memory-efficient streaming** for large datasets  
✅ **Flexible configuration** for different use cases

**Key Features by Strategy:**

| Feature | PIT + search_after | search_after | Classic Scroll |
|---------|-------------------|--------------|----------------|
| **ES Version** | 7.12+ (sliced 7.15+) | All | All |
| **Aggregations** | ❌ | ❌ | ✅ |
| **Consistent Snapshot** | ✅ | ❌ | ✅ |
| **Deep Pagination** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Performance** | ⭐⭐⭐⭐⭐ | ⭐⭐⭐⭐ | ⭐⭐⭐ |
| **Resource Usage** | Low | Low | Medium |
| **Timeout Issues** | ❌ | ❌ | ⚠️ |

**When to Use:**

- **PIT + search_after**: ES 7.12+, large datasets, no aggregations (recommended; sliced one-reader-per-shard from 7.15)
- **search_after**: ES < 7.12, large datasets, no aggregations
- **Classic scroll**: Any version with aggregations, or when consistent snapshot is required

**Best Practices:**

1. ✅ Choose appropriate batch size based on document size
2. ✅ Always set `maxDocuments` for safety
3. ✅ Use typed scrolling when possible
4. ✅ Handle backpressure with buffers
5. ✅ Monitor progress with metrics
6. ✅ Implement proper error handling
7. ✅ Stream to file for large result sets
8. ✅ Use parallel processing wisely
9. ✅ Optimize queries (select only needed fields)
10. ✅ Test with different scenarios

**Performance Tips:**

- 📊 Small documents (< 1KB): batch size 5000
- 📊 Medium documents (1-10KB): batch size 1000
- 📊 Large documents (> 10KB): batch size 100
- 🚀 Use parallelism = number of CPU cores
- 💾 Stream to file for > 100K documents
- ⏱️ Add throttling for rate-limited operations

---

[Back to index](README.md) | [Next: Aggregations API](aggregations.md)
