[Back to index](README.md)

# AGGREGATE API

## Overview

The **Aggregate API** provides a powerful and type-safe way to execute aggregations on Elasticsearch data using SQL queries. It automatically extracts and converts aggregation results into strongly-typed Scala values with comprehensive error handling.

**Key Features:**
- **SQL-based aggregations** (AVG, SUM, COUNT, MIN, MAX, etc.)
- **Type-safe result extraction** with safe casting methods
- **Multiple value types** (Boolean, Numeric, String, Temporal, Object, Arrays)
- **Error handling** for each aggregation
- **Pattern matching support** with `fold` method
- **Pretty printing** for debugging
- **Automatic type conversion** from Elasticsearch responses

**Dependencies:**
- Requires `SearchApi` for query execution
- Requires `ElasticConversion` for response parsing

**Roadmap** :
- Support for multi-value aggregations (currently single-value only)

---

## Table of Contents

1. [Core Concepts](#core-concepts)
2. [SingleValueAggregateResult](#singlevalueaggregateresult)
3. [Basic Usage](#basic-usage)
4. [Safe Value Extraction](#safe-value-extraction)
5. [Pattern Matching with fold](#pattern-matching-with-fold)
6. [Error Handling](#error-handling)
7. [Pretty Printing](#pretty-printing)
8. [Testing](#testing)
9. [Best Practices](#best-practices)

---

## Core Concepts

### Aggregation Result Types

The API uses a sealed trait structure to represent different types of aggregation results:

```scala
// Base trait for all aggregation results
sealed trait AggregateResult {
  def field: String
  def error: Option[String]
}

// Metric aggregations (single values)
sealed trait MetricAggregateResult extends AggregateResult {
  def aggType: AggregationType.AggregationType
}

// Single value result (AVG, SUM, MIN, MAX, COUNT, etc.)
case class SingleValueAggregateResult(
  field: String,
  aggType: AggregationType.AggregationType,
  value: AggregateValue,
  error: Option[String] = None
) extends MetricAggregateResult
```

---

### Value Types

The API supports multiple value types through the `AggregateValue` sealed trait:

```scala
sealed trait AggregateValue

// ============================================================
// SCALAR VALUES
// ============================================================

// Boolean value
case class BooleanValue(value: Boolean) extends AggregateValue

// Numeric value (supports all numeric types)
case class NumericValue(value: Number) extends AggregateValue

// String value
case class StringValue(value: String) extends AggregateValue

// Temporal value (dates, timestamps, etc.)
case class TemporalValue(value: Temporal) extends AggregateValue

// Object/Map value
case class ObjectValue(value: Map[String, Any]) extends AggregateValue

// ============================================================
// ARRAY VALUES
// ============================================================

sealed trait ArrayAggregateValue[T] extends AggregateValue {
  def value: Seq[T]
}

case class ArrayOfBooleanValue(value: Seq[Boolean]) 
  extends ArrayAggregateValue[Boolean]

case class ArrayOfNumericValue(value: Seq[Number]) 
  extends ArrayAggregateValue[Number]

case class ArrayOfStringValue(value: Seq[String]) 
  extends ArrayAggregateValue[String]

case class ArrayOfTemporalValue(value: Seq[Temporal]) 
  extends ArrayAggregateValue[Temporal]

case class ArrayOfObjectValue(value: Seq[Map[String, Any]]) 
  extends ArrayAggregateValue[Map[String, Any]]

// ============================================================
// EMPTY VALUE
// ============================================================

case object EmptyValue extends AggregateValue
```

**Value Type Matrix:**

| Type            | Class                  | Example                | Use Case                    |
|-----------------|------------------------|------------------------|-----------------------------|
| Boolean         | `BooleanValue`         | `true`, `false`        | Boolean aggregations        |
| Numeric         | `NumericValue`         | `42`, `3.14`, `100L`   | AVG, SUM, MIN, MAX, COUNT   |
| String          | `StringValue`          | `"electronics"`        | Category names, labels      |
| Temporal        | `TemporalValue`        | `2024-01-15T10:30:00Z` | Date aggregations           |
| Object          | `ObjectValue`          | `Map("count" -> 10)`   | Complex nested results      |
| Array[Boolean]  | `ArrayOfBooleanValue`  | `Seq(true, false)`     | Multi-valued boolean fields |
| Array[Numeric]  | `ArrayOfNumericValue`  | `Seq(1, 2, 3)`         | Multi-valued numeric fields |
| Array[String]   | `ArrayOfStringValue`   | `Seq("a", "b")`        | Multi-valued string fields  |
| Array[Temporal] | `ArrayOfTemporalValue` | `Seq(date1, date2)`    | Multi-valued date fields    |
| Array[Object]   | `ArrayOfObjectValue`   | `Seq(map1, map2)`      | Multi-valued object fields  |
| Empty           | `EmptyValue`           | `null`                 | No data available           |

---

## SingleValueAggregateResult

### Structure

The `SingleValueAggregateResult` represents a single aggregated value from an Elasticsearch query.

```scala
case class SingleValueAggregateResult(
  field: String,                              // Field name being aggregated
  aggType: AggregationType.AggregationType,   // Type of aggregation (AVG, SUM, etc.)
  value: AggregateValue,                      // The aggregated value
  error: Option[String] = None                // Optional error message
) extends MetricAggregateResult
```

**Fields:**

| Field     | Type              | Description                                                     |
|-----------|-------------------|-----------------------------------------------------------------|
| `field`   | `String`          | Name of the aggregated field (e.g., "avg_price", "total_count") |
| `aggType` | `AggregationType` | Type of aggregation performed                                   |
| `value`   | `AggregateValue`  | The actual aggregated value (typed)                             |
| `error`   | `Option[String]`  | Error message if aggregation failed                             |

---

### Properties and Methods

```scala
val result: SingleValueAggregateResult = // ... from aggregation

// ============================================================
// CHECK METHODS
// ============================================================

// Check if the result is empty
val isEmpty: Boolean = result.isEmpty

// Check if the result has an error
val hasError: Boolean = result.hasError

// ============================================================
// SAFE EXTRACTION METHODS
// ============================================================

// Extract as Boolean
val boolResult: Try[Boolean] = result.asBooleanSafe

// Extract as Number
val numResult: Try[Number] = result.asNumericSafe

// Extract as Double
val doubleResult: Try[Double] = result.asDoubleSafe

// Extract as Int
val intResult: Try[Int] = result.asIntSafe

// Extract as Long
val longResult: Try[Long] = result.asLongSafe

// Extract as Byte
val byteResult: Try[Byte] = result.asByteSafe

// Extract as Short
val shortResult: Try[Short] = result.asShortSafe

// Extract as String
val strResult: Try[String] = result.asStringSafe

// Extract as Temporal
val tempResult: Try[Temporal] = result.asTemporalSafe

// Extract as Map
val mapResult: Try[Map[String, Any]] = result.asMapSafe

// Extract as Sequence
val seqResult: Try[Seq[Any]] = result.asSeqSafe

// ============================================================
// UTILITY METHODS
// ============================================================

// Get value with default
val value: Double = result.getOrElse(0.0) {
  case NumericValue(n) => Some(n.doubleValue())
  case _ => None
}

// Pattern matching with fold
val formatted: String = result.fold(
  onBoolean = b => s"Boolean: $b",
  onNumeric = n => s"Number: $n",
  onString = s => s"String: $s",
  onTemporal = t => s"Temporal: $t",
  onObject = o => s"Object: $o",
  onMulti = m => s"Multi: $m",
  onEmpty = "Empty"
)

// Pretty print for debugging
val prettyString: String = result.prettyPrint
```

---

## Basic Usage

### Simple Aggregation

```scala
import scala.concurrent.ExecutionContext.Implicits.global

// SQL query with aggregation
val avgPriceQuery = SQLQuery(
  query = """
    SELECT AVG(price) as avg_price
    FROM products
    WHERE category = 'electronics'
  """
)

// Execute aggregation
client.aggregate(avgPriceQuery).foreach {
  case ElasticSuccess(results) =>
    results.foreach { result =>
      println(s"Field: ${result.field}")
      println(s"Type: ${result.aggType}")
      
      // Safe extraction
      result.asDoubleSafe match {
        case Success(avgPrice) =>
          println(f"Average price: $$${avgPrice}%.2f")
        case Failure(ex) =>
          println(s"Failed to extract: ${ex.getMessage}")
      }
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Aggregation failed: ${error.message}")
}
```

---

### Multiple Aggregations

```scala
// SQL query with multiple aggregations
val statsQuery = SQLQuery(
  query = """
    SELECT 
      AVG(price) as avg_price,
      MIN(price) as min_price,
      MAX(price) as max_price,
      SUM(price) as total_value,
      COUNT(*) as product_count
    FROM products
    WHERE category = 'electronics'
  """
)

client.aggregate(statsQuery).foreach {
  case ElasticSuccess(results) =>
    println("Product Statistics:")
    
    results.foreach { result =>
      result.field match {
        case "avg_price" =>
          result.asDoubleSafe.foreach(v => println(f"  Average: $$${v}%.2f"))
          
        case "min_price" =>
          result.asDoubleSafe.foreach(v => println(f"  Minimum: $$${v}%.2f"))
          
        case "max_price" =>
          result.asDoubleSafe.foreach(v => println(f"  Maximum: $$${v}%.2f"))
          
        case "total_value" =>
          result.asDoubleSafe.foreach(v => println(f"  Total Value: $$${v}%,.2f"))
          
        case "product_count" =>
          result.asLongSafe.foreach(v => println(s"  Count: $v products"))
          
        case _ =>
          println(s"  ${result.field}: ${result.prettyPrint}")
      }
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

## Safe Value Extraction

### Type-Safe Methods

All extraction methods return `Try[T]` for safe error handling:

```scala
val result: SingleValueAggregateResult = // ... from aggregation

// ============================================================
// BOOLEAN EXTRACTION
// ============================================================

result.asBooleanSafe match {
  case Success(bool) => println(s"Boolean: $bool")
  case Failure(ex) => println(s"Not a boolean: ${ex.getMessage}")
}

// ============================================================
// NUMERIC EXTRACTIONS
// ============================================================

// As generic Number
result.asNumericSafe match {
  case Success(num) => println(s"Number: $num")
  case Failure(ex) => println(s"Not a number: ${ex.getMessage}")
}

// As Double
result.asDoubleSafe match {
  case Success(d) => println(f"Double: $d%.2f")
  case Failure(ex) => println(s"Not a double: ${ex.getMessage}")
}

// As Int
result.asIntSafe match {
  case Success(i) => println(s"Int: $i")
  case Failure(ex) => println(s"Not an int: ${ex.getMessage}")
}

// As Long
result.asLongSafe match {
  case Success(l) => println(s"Long: $l")
  case Failure(ex) => println(s"Not a long: ${ex.getMessage}")
}

// As Byte
result.asByteSafe match {
  case Success(b) => println(s"Byte: $b")
  case Failure(ex) => println(s"Not a byte: ${ex.getMessage}")
}

// As Short
result.asShortSafe match {
  case Success(s) => println(s"Short: $s")
  case Failure(ex) => println(s"Not a short: ${ex.getMessage}")
}

// ============================================================
// STRING EXTRACTION
// ============================================================

result.asStringSafe match {
  case Success(str) => println(s"String: $str")
  case Failure(ex) => println(s"Not a string: ${ex.getMessage}")
}

// ============================================================
// TEMPORAL EXTRACTION
// ============================================================

result.asTemporalSafe match {
  case Success(temporal) => println(s"Temporal: $temporal")
  case Failure(ex) => println(s"Not a temporal: ${ex.getMessage}")
}

// ============================================================
// MAP EXTRACTION
// ============================================================

result.asMapSafe match {
  case Success(map) => 
    println("Map:")
    map.foreach { case (k, v) => println(s"  $k: $v") }
  case Failure(ex) => println(s"Not a map: ${ex.getMessage}")
}

// ============================================================
// SEQUENCE EXTRACTION
// ============================================================

result.asSeqSafe match {
  case Success(seq) => 
    println(s"Sequence with ${seq.size} elements:")
    seq.foreach(println)
  case Failure(ex) => println(s"Not a sequence: ${ex.getMessage}")
}
```

---

### getOrElse Method

Extract values with default fallback:

```scala
val result: SingleValueAggregateResult = // ... from aggregation

// Extract Double with default
val avgPrice: Double = result.getOrElse(0.0) {
  case NumericValue(n) => Some(n.doubleValue())
  case _ => None
}

// Extract Int with default
val productCount: Int = result.getOrElse(0) {
  case NumericValue(n) => Some(n.intValue())
  case _ => None
}

// Extract String with default
val categoryName: String = result.getOrElse("Unknown") {
  case StringValue(s) => Some(s)
  case _ => None
}

// Extract Boolean with default
val isActive: Boolean = result.getOrElse(false) {
  case BooleanValue(b) => Some(b)
  case _ => None
}

// Extract Sequence with default
val prices: Seq[Double] = result.getOrElse(Seq.empty[Double]) {
  case ArrayOfNumericValue(nums) => Some(nums.map(_.doubleValue()))
  case _ => None
}

// Extract Map with default
val metadata: Map[String, Any] = result.getOrElse(Map.empty[String, Any]) {
  case ObjectValue(m) => Some(m)
  case _ => None
}
```

---

## Pattern Matching with fold

### Basic fold Usage

The `fold` method provides exhaustive pattern matching for all value types:

```scala
val result: SingleValueAggregateResult = // ... from aggregation

val output: String = result.fold(
  onBoolean = bool => s"Boolean value: $bool",
  onNumeric = num => f"Numeric value: ${num.doubleValue()}%.2f",
  onString = str => s"String value: '$str'",
  onTemporal = temp => s"Temporal value: $temp",
  onObject = obj => s"Object with ${obj.size} fields",
  onMulti = seq => s"Array with ${seq.size} elements",
  onEmpty = "No value"
)

println(output)
```

---

### Advanced fold Patterns

```scala
// ============================================================
// CALCULATE TOTAL
// ============================================================

def calculateTotal(result: SingleValueAggregateResult): Double = {
  result.fold(
    onBoolean = _ => 0.0,
    onNumeric = num => num.doubleValue(),
    onString = _ => 0.0,
    onTemporal = _ => 0.0,
    onObject = _ => 0.0,
    onMulti = seq => seq.collect { case n: Number => n.doubleValue() }.sum,
    onEmpty = 0.0
  )
}

// ============================================================
// FORMAT VALUE FOR DISPLAY
// ============================================================

def formatValue(result: SingleValueAggregateResult): String = {
  result.fold(
    onBoolean = bool => if (bool) "✓" else "✗",
    onNumeric = num => f"${num.doubleValue()}%,.2f",
    onString = str => s"\"$str\"",
    onTemporal = temp => temp.toString,
    onObject = obj => obj.map { case (k, v) => s"$k=$v" }.mkString("{", ", ", "}"),
    onMulti = seq => seq.mkString("[", ", ", "]"),
    onEmpty = "N/A"
  )
}

// ============================================================
// CONVERT TO JSON
// ============================================================

def toJson(result: SingleValueAggregateResult): String = {
  val valueJson = result.fold(
    onBoolean = bool => bool.toString,
    onNumeric = num => num.toString,
    onString = str => s"\"$str\"",
    onTemporal = temp => s"\"$temp\"",
    onObject = obj => obj.map { case (k, v) => s"\"$k\":\"$v\"" }.mkString("{", ",", "}"),
    onMulti = seq => seq.map {
      case s: String => s"\"$s\""
      case other => other.toString
    }.mkString("[", ",", "]"),
    onEmpty = "null"
  )
  
  s"""{"field":"${result.field}","type":"${result.aggType}","value":$valueJson}"""
}

// ============================================================
// VALIDATE VALUE
// ============================================================

def validateValue(result: SingleValueAggregateResult): Either[String, Any] = {
  result.fold(
    onBoolean = bool => Right(bool),
    onNumeric = num => {
      val d = num.doubleValue()
      if (d.isNaN || d.isInfinite) Left("Invalid numeric value")
      else Right(d)
    },
    onString = str => {
      if (str.trim.isEmpty) Left("Empty string")
      else Right(str)
    },
    onTemporal = temp => Right(temp),
    onObject = obj => {
      if (obj.isEmpty) Left("Empty object")
      else Right(obj)
    },
    onMulti = seq => {
      if (seq.isEmpty) Left("Empty array")
      else Right(seq)
    },
    onEmpty = Left("No value present")
  )
}

// ============================================================
// TYPE CONVERSION
// ============================================================

def convertToString(result: SingleValueAggregateResult): String = {
  result.fold(
    onBoolean = _.toString,
    onNumeric = _.toString,
    onString = identity,
    onTemporal = _.toString,
    onObject = _.toString,
    onMulti = _.mkString(", "),
    onEmpty = ""
  )
}
```

---

## Error Handling

### Checking for Errors

```scala
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    results.foreach { result =>
      if (result.hasError) {
        println(s"⚠️ Error in ${result.field}:")
        println(s"   ${result.error.getOrElse("Unknown error")}")
      } else if (result.isEmpty) {
        println(s"ℹ️ ${result.field}: No data")
      } else {
        println(s"✅ ${result.field}: ${result.prettyPrint}")
      }
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Aggregation failed: ${error.message}")
}
```

---

### Handling Individual Result Errors

```scala
def processAggregationResult(result: SingleValueAggregateResult): Unit = {
  result.error match {
    case Some(errorMsg) =>
      println(s"❌ Aggregation '${result.field}' failed:")
      println(s"   Error: $errorMsg")
      println(s"   Type: ${result.aggType}")
      
    case None if result.isEmpty =>
      println(s"ℹ️ Aggregation '${result.field}' returned no data")
      
    case None =>
      println(s"✅ ${result.prettyPrint}")
      
      // Process the value
      result.asDoubleSafe match {
        case Success(value) =>
          println(f"   Processed value: $value%.2f")
        case Failure(ex) =>
          println(s"   ⚠️ Type conversion failed: ${ex.getMessage}")
      }
  }
}

// Usage
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    results.foreach(processAggregationResult)
    
  case ElasticFailure(error) =>
    println(s"❌ Query execution failed: ${error.message}")
}
```

---

### Comprehensive Error Handler

```scala
def handleAggregationResults(
  results: Seq[SingleValueAggregateResult]
): Map[String, Either[String, Any]] = {
  results.map { result =>
    val value = if (result.hasError) {
      Left(result.error.getOrElse("Unknown error"))
    } else if (result.isEmpty) {
      Left("No data available")
    } else {
      result.fold(
        onBoolean = b => Right(b),
        onNumeric = n => Right(n.doubleValue()),
        onString = s => Right(s),
        onTemporal = t => Right(t),
        onObject = o => Right(o),
        onMulti = m => Right(m),
        onEmpty = Left("Empty value")
      )
    }
    
    result.field -> value
  }.toMap
}

// Usage
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    val processed = handleAggregationResults(results)
    
    processed.foreach {
      case (field, Right(value)) =>
        println(s"✅ $field: $value")
      case (field, Left(error)) =>
        println(s"❌ $field: $error")
    }
    
  case ElasticFailure(error) =>
    println(s"❌ Query failed: ${error.message}")
}
```

---

## Pretty Printing

### Using prettyPrint

```scala
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    println("Aggregation Results:")
    println("=" * 50)
    
    results.foreach { result =>
      println(result.prettyPrint)
    }
    
    println("=" * 50)
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}

// Example output:
// Aggregation Results:
// ==================================================
// AVG(price) = 599.99
// MIN(price) = 29.99
// MAX(price) = 1999.99
// COUNT(*) = 150
// SUM(price) = 89998.50
// ==================================================
```

---

### Custom Formatting

```scala
def formatAggregationReport(results: Seq[SingleValueAggregateResult]): String = {
  val header = "AGGREGATION REPORT"
  val separator = "=" * 60
  
  val lines = results.map { result =>
    val status = if (result.hasError) "❌" else if (result.isEmpty) "⚠️" else "✅"
    val value = if (result.hasError) {
      result.error.getOrElse("Unknown error")
    } else {
      result.fold(
        onBoolean = b => b.toString,
        onNumeric = n => f"${n.doubleValue()}%,.2f",
        onString = s => s""""$s"""",
        onTemporal = t => t.toString,
        onObject = o => s"Object(${o.size} fields)",
        onMulti = m => s"Array(${m.size} elements)",
        onEmpty = "No data"
      )
    }
    
    f"$status ${result.aggType}%-10s ${result.field}%-20s = $value"
  }
  
  Seq(separator, header, separator) ++ lines ++ Seq(separator) mkString "\n"
}

// Usage
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    println(formatAggregationReport(results))
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

### Tabular Format

```scala
def formatAsTable(results: Seq[SingleValueAggregateResult]): String = {
  val headers = Seq("Status", "Type", "Field", "Value", "Error")
  val colWidths = Seq(8, 12, 25, 20, 30)
  
  def formatRow(cells: Seq[String]): String = {
    cells.zip(colWidths).map { case (cell, width) =>
      cell.padTo(width, ' ').take(width)
    }.mkString("| ", " | ", " |")
  }
  
  val separator = colWidths.map("-" * _).mkString("+-", "-+-", "-+")
  
  val headerRow = formatRow(headers)
  
  val dataRows = results.map { result =>
    val status = if (result.hasError) "❌ Error" 
                 else if (result.isEmpty) "⚠️ Empty" 
                 else "✅ OK"
    
    val value = result.fold(
      onBoolean = b => b.toString,
      onNumeric = n => f"${n.doubleValue()}%.2f",
      onString = s => s,
      onTemporal = t => t.toString,
      onObject = o => s"Object(${o.size})",
      onMulti = m => s"Array(${m.size})",
      onEmpty = "N/A"
    )
    
    val error = result.error.getOrElse("")
    
    formatRow(Seq(status, result.aggType.toString, result.field, value, error))
  }
  
  (Seq(separator, headerRow, separator) ++ dataRows ++ Seq(separator)).mkString("\n")
}

// Usage
client.aggregate(sqlQuery).foreach {
  case ElasticSuccess(results) =>
    println(formatAsTable(results))
    
  case ElasticFailure(error) =>
    println(s"❌ Error: ${error.message}")
}
```

---

## Testing

### Test Basic Aggregation

```scala
import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

class AggregateApiSpec extends AsyncFlatSpec with Matchers {
  
  "AggregateApi" should "calculate average correctly" in {
    val testIndex = "test-aggregation"
    
    for {
      // Setup
      _ <- client.createIndexAsync(testIndex)
      _ <- client.indexAsync(testIndex, "1", """{"price": 100}""")
      _ <- client.indexAsync(testIndex, "2", """{"price": 200}""")
      _ <- client.indexAsync(testIndex, "3", """{"price": 150}""")
      _ <- client.refreshAsync(testIndex)
      
      // Test
      query = SQLQuery(
        query = s"SELECT AVG(price) as avg_price FROM $testIndex"
      )
      result <- client.aggregate(query)
      
      // Assertions
      _ = result match {
        case ElasticSuccess(results) =>
          results should not be empty
          
          val avgResult = results.find(_.field == "avg_price")
          avgResult shouldBe defined
          
          avgResult.foreach { r =>
            r.hasError shouldBe false
            r.isEmpty shouldBe false
            
            r.asDoubleSafe.toOption shouldBe Some(150.0)
          }
          
        case ElasticFailure(error) =>
          fail(s"Aggregation failed: ${error.message}")
      }
      
      // Cleanup
      _ <- client.deleteIndexAsync(testIndex)
    } yield succeed
  }
}
```

---

### Test Multiple Aggregations

```scala
"AggregateApi" should "handle multiple aggregations" in {
  val testIndex = "test-multi-agg"
  
  for {
    // Setup
    _ <- client.createIndexAsync(testIndex)
    _ <- Future.sequence((1 to 10).map { i =>
      client.indexAsync(testIndex, i.toString, s"""{"price": ${i * 10}, "stock": ${i * 5}}""")
    })
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(
      query = s"""
        SELECT 
          COUNT(*) as count,
          AVG(price) as avg_price,
          MIN(price) as min_price,
          MAX(price) as max_price,
          SUM(stock) as total_stock
        FROM $testIndex
      """
    )
    result <- client.aggregate(query)
    
    // Assertions
    _ = result match {
      case ElasticSuccess(results) =>
        results should have size 5
        
        val resultMap = results.map(r => r.field -> r).toMap
        
        // Verify COUNT
        resultMap.get("count").flatMap(_.asLongSafe.toOption) shouldBe Some(10L)
        
        // Verify AVG
        resultMap.get("avg_price").flatMap(_.asDoubleSafe.toOption) shouldBe Some(55.0)
        
        // Verify MIN
        resultMap.get("min_price").flatMap(_.asDoubleSafe.toOption) shouldBe Some(10.0)
        
        // Verify MAX
        resultMap.get("max_price").flatMap(_.asDoubleSafe.toOption) shouldBe Some(100.0)
        
        // Verify SUM
        resultMap.get("total_stock").flatMap(_.asLongSafe.toOption) shouldBe Some(275L)
        
      case ElasticFailure(error) =>
        fail(s"Aggregation failed: ${error.message}")
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Error Handling

```scala
"AggregateApi" should "handle errors gracefully" in {
  val testIndex = "test-agg-error"
  
  for {
    // Setup - create index with incompatible types
    _ <- client.createIndexAsync(testIndex)
    _ <- client.indexAsync(testIndex, "1", """{"price": 100}""")
    _ <- client.indexAsync(testIndex, "2", """{"price": "invalid"}""")
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(
      query = s"SELECT AVG(price) as avg_price FROM $testIndex"
    )
    result <- client.aggregate(query)
    
    // Assertions
    _ = result match {
      case ElasticSuccess(results) =>
        results should not be empty
        
        // Should either have error or handle gracefully
        results.foreach { r =>
          if (r.hasError) {
            r.error shouldBe defined
            r.error.get should not be empty
          }
        }
        
      case ElasticFailure(error) =>
        // Error is acceptable in this case
        error.message should not be empty
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

### Test Empty Results

```scala
"AggregateApi" should "handle empty results" in {
  val testIndex = "test-empty-agg"
  
  for {
    // Setup - create empty index
    _ <- client.createIndexAsync(testIndex)
    _ <- client.refreshAsync(testIndex)
    
    // Test
    query = SQLQuery(
      query = s"SELECT AVG(price) as avg_price FROM $testIndex"
    )
    result <- client.aggregate(query)
    
    // Assertions
    _ = result match {
      case ElasticSuccess(results) =>
        results.foreach { r =>
          // Should be empty or have appropriate value
          if (r.isEmpty) {
            r.value shouldBe EmptyValue
          }
        }
        
      case ElasticFailure(_) =>
        // Empty result is acceptable
        succeed
    }
    
    // Cleanup
    _ <- client.deleteIndexAsync(testIndex)
  } yield succeed
}
```

---

## Best Practices

### 1. Always Use Safe Extraction

```scala
// ❌ BAD: Unsafe extraction
val avgPrice = result.value.asInstanceOf[NumericValue].value.doubleValue()

// ✅ GOOD: Safe extraction with error handling
val avgPrice = result.asDoubleSafe match {
  case Success(price) => price
  case Failure(ex) => 
    logger.error(s"Failed to extract price: ${ex.getMessage}")
    0.0
}

// ✅ BETTER: Using getOrElse
val avgPrice = result.getOrElse(0.0) {
  case NumericValue(n) => Some(n.doubleValue())
  case _ => None
}
```

---

### 2. Check for Errors Before Processing

```scala
// ✅ GOOD: Check errors first
results.foreach { result =>
  if (result.hasError) {
    logger.error(s"Aggregation error in ${result.field}: ${result.error.get}")
  } else if (result.isEmpty) {
    logger.warn(s"No data for ${result.field}")
  } else {
    // Process valid result
    processResult(result)
  }
}
```

---

### 3. Use Pattern Matching with fold

```scala
// ✅ GOOD: Exhaustive pattern matching
val formatted = result.fold(
  onBoolean = b => s"Boolean: $b",
  onNumeric = n => f"Numeric: ${n.doubleValue()}%.2f",
  onString = s => s"String: $s",
  onTemporal = t => s"Temporal: $t",
  onObject = o => s"Object: $o",
  onMulti = m => s"Multi: $m",
  onEmpty = "Empty"
)
```

---

### 4. Handle Async Operations Properly

```scala
// ✅ GOOD: Proper async handling
def getStats(category: String): Future[Option[Stats]] = {
  val query = buildStatsQuery(category)
  
  client.aggregate(query).map {
    case ElasticSuccess(results) =>
      extractStats(results)
      
    case ElasticFailure(error) =>
      logger.error(s"Aggregation failed: ${error.message}")
      None
  }.recover {
    case ex: Exception =>
      logger.error(s"Unexpected error: ${ex.getMessage}", ex)
      None
  }
}
```

---

### 5. Create Type-Safe Result Extractors

```scala
// ✅ GOOD: Type-safe extractor
object AggregationExtractor {
  def extractDouble(
    results: Seq[SingleValueAggregateResult],
    field: String
  ): Option[Double] = {
    results
      .find(_.field == field)
      .filter(!_.hasError)
      .flatMap(_.asDoubleSafe.toOption)
  }
  
  def extractLong(
    results: Seq[SingleValueAggregateResult],
    field: String
  ): Option[Long] = {
    results
      .find(_.field == field)
      .filter(!_.hasError)
      .flatMap(_.asLongSafe.toOption)
  }
  
  def extractString(
    results: Seq[SingleValueAggregateResult],
    field: String
  ): Option[String] = {
    results
      .find(_.field == field)
      .filter(!_.hasError)
      .flatMap(_.asStringSafe.toOption)
  }
}

// Usage
val avgPrice = AggregationExtractor.extractDouble(results, "avg_price")
val count = AggregationExtractor.extractLong(results, "count")
```

---

### 6. Use Descriptive Field Names

```scala
// ❌ BAD: Unclear field names
"SELECT AVG(price) as a, COUNT(*) as c FROM products"

// ✅ GOOD: Clear field names
"""SELECT 
  AVG(price) as avg_price,
  COUNT(*) as product_count,
  SUM(stock) as total_inventory
FROM products"""
```

---

### 7. Log Aggregation Queries for Debugging

```scala
// ✅ GOOD: Log queries
def executeAggregation(query: SQLQuery): Future[ElasticResult[Seq[SingleValueAggregateResult]]] = {
  logger.info(s"Executing aggregation: ${query.query}")
  
  val startTime = System.currentTimeMillis()
  
  client.aggregate(query).map { result =>
    val duration = System.currentTimeMillis() - startTime
    logger.info(s"Aggregation completed in ${duration}ms")
    
    result match {
      case ElasticSuccess(results) =>
        logger.debug(s"Got ${results.size} aggregation results")
        results.foreach(r => logger.debug(r.prettyPrint))
        
      case ElasticFailure(error) =>
        logger.error(s"Aggregation failed: ${error.message}")
    }
    
    result
  }
}
```

---

### 8. Create Reusable Aggregation Builders

```scala
// ✅ GOOD: Reusable builders
object AggregationQueryBuilder {
  def statsQuery(index: String, field: String, filters: Map[String, String] = Map.empty): SQLQuery = {
    val whereClause = if (filters.nonEmpty) {
      "WHERE " + filters.map { case (k, v) => s"$k = '$v'" }.mkString(" AND ")
    } else ""
    
    SQLQuery(
      query = s"""
        SELECT 
          COUNT(*) as count,
          AVG($field) as avg_value,
          MIN($field) as min_value,
          MAX($field) as max_value,
          SUM($field) as total_value
        FROM $index
        $whereClause
      """
    )
  }
  
}

// Usage
val priceStats = AggregationQueryBuilder.statsQuery(
  "products",
  "price",
  Map("category" -> "electronics")
)
```

---

## Summary

The **Aggregate API** provides:

✅ **Type-safe aggregation results** with comprehensive value types  
✅ **Safe extraction methods** with `Try` based error handling  
✅ **Pattern matching support** via the `fold` method  
✅ **Error handling** at both query and result levels  
✅ **Pretty printing** for debugging and logging  
✅ **Flexible value types** including scalars, arrays, and objects  
✅ **SQL-based queries** for familiar aggregation syntax

**Key Takeaways:**

1. Always use safe extraction methods (`asDoubleSafe`, `asLongSafe`, etc.)
2. Check for errors before processing results
3. Use `fold` for exhaustive pattern matching
4. Handle async operations properly with Future
5. Create reusable extractors and builders
6. Log queries and results for debugging
7. Use descriptive field names in SQL queries
8. Test edge cases (empty results, errors, type mismatches)

---


---

[Back to index](README.md)
