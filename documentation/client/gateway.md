[Back to index](README.md)

# 📘 GatewayApi

The `GatewayApi` provides a unified entry point for executing SQL statements (DQL, DML, DDL, and Pipeline operations) against Elasticsearch.  
It exposes a single method:

```scala
def run(sql: String): Future[ElasticResult[QueryResult]]
```

which accepts any SQL statement supported by the SQL Gateway.

This API is designed to offer a familiar SQL interface while leveraging Elasticsearch’s indexing, search, aggregation, and mapping capabilities under the hood.

---

## Table of Contents

- [Overview](#overview)
- [Statement Hierarchy](#statement-hierarchy)
- [Supported SQL Statements](#supported-sql-statements)
- [Executing SQL](#executing-sql)
	- [Single Statement](#single-statement)
	- [Multiple Statements](#multiple-statements)
- [QueryResult Types](#queryresult-types)
	- [DQL Results](#dql-results)
	- [DML Results](#dml-results)
	- [DDL Results](#ddl-results)
- [Examples](#examples)
- [Error Handling](#error-handling)
- [Notes](#notes)

---

## Overview

`GatewayApi` is a high‑level SQL interface built on top of the Softnetwork Elasticsearch client.  
It routes SQL statements to the appropriate executor:

| SQL Type                                                 | Executor            |
|----------------------------------------------------------|---------------------|
| DQL (SELECT)                                             | `DqlExecutor`       |
| DML (INSERT / UPDATE / DELETE / COPY INTO)               | `DmlExecutor`       |
| DDL (CREATE / ALTER / DROP / TRUNCATE / SHOW / DESCRIBE) | `DdlRouterExecutor` |

The API automatically:

- normalizes SQL (removes comments, trims whitespace)
- splits multiple statements separated by `;` — quote-aware, so a `;` inside a string literal or a
  quoted identifier is not a statement boundary
- parses each statement into AST nodes — each must be **complete on its own**: anything left over
  after a statement is a parse error rather than being silently discarded
- dispatches to the correct executor
- runs the statements sequentially, stopping at the first failure, and returns the **last**
  statement's typed `QueryResult`

---

## Statement Hierarchy

The SQL Gateway classifies SQL statements into three main categories:

- **DQL** — SELECT queries
- **DML** — INSERT, UPDATE, DELETE, COPY INTO
- **DDL** — schema and pipeline definitions

DDL statements are further divided into:

- **Table statements** (`TableStatement`)  
	CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE TABLE, DESCRIBE TABLE, SHOW TABLE, SHOW CREATE TABLE

- **Pipeline statements** (`PipelineStatement`)  
	CREATE PIPELINE, ALTER PIPELINE, DROP PIPELINE, DESCRIBE PIPELINE, SHOW PIPELINE, SHOW CREATE PIPELINE

Both kinds of statements extend `DdlStatement`.

---

## Supported SQL Statements

### DQL

[DQL Statements](../sql/dql_statements.md) supported by the SQL Gateway include:
- `SELECT … FROM …`
- `JOIN UNNEST`
- `GROUP BY`, `HAVING`
- `ORDER BY`, `LIMIT`, `OFFSET`
- Window functions (`OVER`)
- Parent‑level aggregations on nested arrays

### DML

[DML Statements](../sql/dml_statements.md) supported by the SQL Gateway include:
- `INSERT INTO … VALUES`
- `INSERT INTO … AS SELECT`
- `UPDATE … SET …`
- `DELETE FROM …`
- `COPY INTO table FROM file`

### DDL

[DDL Statements](../sql/ddl_statements.md) supported by the SQL Gateway include:
- `CREATE TABLE`
- `ALTER TABLE`
- `DROP TABLE`
- `TRUNCATE TABLE`
- `DESCRIBE TABLE`
- `SHOW TABLE`
- `SHOW CREATE TABLE`

- `CREATE PIPELINE`
- `ALTER PIPELINE`
- `DROP PIPELINE`
- `DESCRIBE PIPELINE`
- `SHOW PIPELINE`
- `SHOW CREATE PIPELINE`

---

## Executing SQL

### Single Statement

```scala
gateway.run("SELECT * FROM dql_users")
```

### Multiple Statements

Statements separated by `;` are executed **sequentially**:

```scala
gateway.run("""
  CREATE TABLE users (...);
  INSERT INTO users VALUES (...);
  SELECT * FROM users;
""")
```

The result of the **last** statement is returned.

---

## QueryResult Types

The Gateway returns one of the following:

---

### DQL Results

#### `QueryRows`

Materialized rows:

```scala
QueryRows(Seq(Map("id" -> 1, "name" -> "Alice")))
```

#### `QueryStream`

Streaming rows using scroll:

```scala
QueryStream(Source[(Map[String, Any], ScrollMetrics)])
```

#### `QueryStructured`

Raw Elasticsearch response:

```scala
QueryStructured(ElasticResponse)
```

#### The result schema

Every SELECT result carries its **declared schema**, known before any row is read:

```scala
case class ResultColumn(
  name: String,                        // the column's output name
  sqlType: SQLType,                    // VARCHAR, INT, BIGINT, DOUBLE, REAL, BOOLEAN, DATE, TIMESTAMP, ...
  nullable: Boolean = true,            // always true in this release
  fields: Seq[ResultColumn] = Nil,     // a STRUCT's fields (by name), a GEO_POINT's lat / lon
  element: Option[ResultColumn] = None // an ARRAY's element
)
case class ResultSchema(columns: Seq[ResultColumn])

QueryRows(rows, truncation, schema: Option[ResultSchema])
QueryStream(stream, truncation, schema: Option[ResultSchema])
QueryStructured(response, truncation).schema // = response.schema
```

- The columns are in SELECT order; `SELECT *` lists the mapping's columns in their table order.
  A row keeps its document's key order: read a value **by name**, present it in schema order.
- `schema` is `None` when no mapping is known (an index that cannot be read, an alias on the ES 8/9
  clients) and for DDL / DML / SHOW results.
- A positional pattern binds every field: `case QueryRows(rows, _, _)`. The shape immune to a future
  field is `case q: QueryRows => q.rows`.

`describe(sql)` answers the schema `run(sql)` would carry **without running the query** (no search,
no subquery; a mapping may be read):

```scala
gateway.describe("SELECT id, ts FROM events") // ElasticSuccess(Some(ResultSchema(...)))
```

It accepts one query (a SELECT, a set operation, a FROM-less SELECT) and refuses anything else with
a `400`; a statement `run` refuses before executing anything is refused with the same status and
message. A statement handled by another extension than core's (a cross-index JOIN under the arrow
extension) answers `None`.

#### Value classes

Each value is read by its column's declared type, where it is first read:

| Column type | Value class |
| --- | --- |
| TINYINT, SMALLINT, INT | `java.lang.Integer` |
| BIGINT | `java.lang.Long` |
| DOUBLE | `java.lang.Double` |
| REAL | `java.lang.Float` |
| BOOLEAN | `java.lang.Boolean` |
| VARCHAR (keyword, text) | `String` -- a number stored in a keyword stays its text |
| DATE | `java.time.LocalDate` |
| TIMESTAMP | `java.time.ZonedDateTime` at UTC, read with the field's mapping format |
| TIME | `java.time.LocalTime` |
| VARBINARY | `Array[Byte]`, the decoded bytes (its text is still the base64) |
| STRUCT | `ListMap` of its declared fields |
| GEO_POINT | `ListMap("lat" -> Double, "lon" -> Double)`, whatever spelling was indexed |
| ARRAY | `List` of its elements |

A statement whose SELECT items are all computed -- literals, arithmetic, functions, casts, `CASE`
(`SELECT 2 AS k FROM t`, `SELECT n + 1 AS v FROM t`) -- asks Elasticsearch for no `_source`
(*since `0.25.0`*: the whole document was fetched): its script fields read doc values, so its rows
hold only its columns on every route (one-shot, async, scroll / PIT, `UNION ALL` legs), with a
schema or without. An `INSERT … SELECT` of such a statement writes only its columns, and is refused
by name when it does not project the target's PRIMARY KEY or PARTITION BY column; an entity read
(`searchAs…`) leaves a field the statement does not select at `None` (its default), and fails on a
required one.

A one-element array is its element, an empty array NULL. A date field whose format core cannot read
for sure (a zone name, a region id) is a VARCHAR, its text as stored; a date value Elasticsearch
ignored (`ignore_malformed`) is NULL, and the other values of the same array keep theirs. An
integral column reads a fractional number or a numeric text as Elasticsearch indexed it, truncated
toward zero. A value a typed column cannot read is answered as stored: a value Elasticsearch did not
index (a number outside its type's range, a `long` text with surrounding spaces) is never narrowed.

Aggregation values follow the same table:

- A GROUP BY key is read by its column's type: a `boolean` key is a `Boolean`, a date key the
  `java.time` value of its epoch milliseconds at UTC (whatever the mapping format), a `long` key a
  `Long`, a keyword key its text (a keyword spelling a date or a time stays text). A `DATE` key that
  is not exactly a day, or a `TIME` key that is not a time of day of 1970-01-01, keeps the reading
  `0.24.0` gave it (a `DATE` key: a timestamp at UTC under the default format, its text in the
  column's format under a format other than ISO).
- A metric is read by its column's type, exactly: `COUNT` is a `Long` on every client, `MIN` /
  `MAX` of an integer an `Integer` (a `Long` for a `long`), of a date its `java.time` value at UTC,
  of a boolean a `Boolean`. A value the class cannot hold unchanged (a sum beyond `Long`, a
  fractional result over an integer column) is answered as Elasticsearch computed it, never
  rounded.
- `SUM` is declared `BIGINT` over an integer column of any width and `DOUBLE` over a `REAL` or a
  `DOUBLE` column, as DuckDB widens a sum: `SELECT SUM(n)` over an `integer` field answers a
  `Long`, a `CREATE TABLE ... AS SELECT SUM(n) ...` maps the column `long`.
- A `FIRST_VALUE` / `LAST_VALUE` / `ARRAY_AGG` value is read by its column's type (an
  `ARRAY_AGG(DISTINCT d)` keeps one value per instant).
- A window-enriched statement (`... OVER (PARTITION BY p ...)`) reads its rows by type and
  partitions on the same values: a `BOOLEAN`, `TIMESTAMP`, `BIGINT` or whole-day `DATE` partition
  column is answered, and the rows of a statement without `*` hold only the SELECT's columns. A
  partition column the SELECT does not project is read when the statement selects `*`, or no plain
  column at all (`SELECT UPPER(id) AS u, SUM(n) OVER (PARTITION BY p)`: its partition columns'
  `_source` is fetched, and nothing else); beside a plain column every window of the statement
  reads NULL, and so does the column each one aggregates.
  A `date` partition column (not `date_nanos`) read as its stored text (a format with a zone name)
  is matched by its instant, as Elasticsearch buckets it, whether the SELECT projects it under its
  own name or not at all, unless another window's partition column is not projected; a projected
  one still shows its stored text. Projected under another name (`pz AS pz2`), or shadowed by a
  computed column of the SELECT that takes its name, it is not matched that way.

---

### DML Results

#### `DmlResult`

```scala
DmlResult(
  inserted = 10,
  updated = 0,
  deleted = 0,
  rejected = 0
)
```

---

### DDL Results

DDL operations return one of the following:

- `DdlResult(success = true)`  
	Returned for CREATE / ALTER / DROP / TRUNCATE (tables or pipelines).

- `TableResult(table: Table`  
	Returned only by `SHOW TABLE`.

- `PipelineResult(pipeline: IngestPipeline)`  
	Returned only by `SHOW PIPELINE`.

- `QueryRows`  
	Returned by:
	- `DESCRIBE TABLE`
	- `DESCRIBE PIPELINE`

- `SQLResult(sql: String)`  
	Returned by:
	- `SHOW CREATE TABLE`
	- `SHOW CREATE PIPELINE`

---

## **Architecture Diagram**

```text
+-------------------------------------------------------------+
|                         GatewayApi                          |
|-------------------------------------------------------------|
| - run(sql: String): Future[ElasticResult[QueryResult]]      |
+-------------------------------+-----------------------------+
                                |
                                v
                 Normalize / strip comments / split on ';'
                                |
                                v
                        +----------------+
                        |    Parser      |
                        |  (SQL → AST)   |
                        +----------------+
                                |
                                v
                    +------------------------+
                    |       Statement        |
                    +------------------------+
                      /          |          \
                     /           |           \
                    v            v            v
          +----------------+  +----------------+  +----------------+
          |  DqlStatement  |  |  DmlStatement  |  |  DdlStatement  |
          +----------------+  +----------------+  +----------------+
                                                       |
                                                       v
                                      +-------------------------------+
                                      |        DdlRouterExecutor      |
                                      +-------------------------------+
                                      | - execute(ddl: DdlStatement)  |
                                      +-------------------------------+
                                       /                          \
                                      /                            \
                                     v                              v
                      +---------------------------+    +---------------------------+
                      |      TableStatement       |    |    PipelineStatement      |
                      +---------------------------+    +---------------------------+
                      |       TableExecutor       |    |      PipelineExecutor     |
                      +---------------------------+    +---------------------------+
                                   |                             |
                                   v                             v
                          +----------------+             +----------------+
                          | Elasticsearch  |             | Elasticsearch  |
                          +----------------+             +----------------+

DQL path:
---------

DqlStatement
    |
    v
+----------------+
|  DqlExecutor   |
+----------------+
        |
        v
+----------------+
| Elasticsearch  |
+----------------+
        |
        v
+---------------------------------------------+
| DQL QueryResult:                            |
|   - QueryRows                               |
|   - QueryStream                             |
|   - QueryStructured                         |
+---------------------------------------------+


DML path:
---------

DmlStatement
    |
    v
+----------------+
|  DmlExecutor   |
+----------------+
        |
        v
+----------------+
| Elasticsearch  |
+----------------+
        |
        v
+---------------------------------------------+
| DML QueryResult:                            |
|   - DmlResult                               |
+---------------------------------------------+


DDL (Tables) path:
------------------

TableStatement
    |
    v
+----------------+
| TableExecutor  |
+----------------+
        |
        v
+----------------+
| Elasticsearch  |
+----------------+
        |
        v
+------------------------------------------------------+
| DDL Table QueryResult:                               |
|   - DdlResult        (CREATE / ALTER / DROP / TRUNC) |
|   - TableResult      (SHOW TABLE)                    |
|   - SQLResult        (SHOW CREATE TABLE)             |
|   - QueryRows        (DESCRIBE TABLE, etc.)          |
+------------------------------------------------------+


DDL (Pipelines) path:
---------------------

PipelineStatement
    |
    v
+--------------------+
|  PipelineExecutor  |
+--------------------+
        |
        v
+----------------+
| Elasticsearch  |
+----------------+
        |
        v
+------------------------------------------------------+
| DDL Pipeline QueryResult:                            |
|   - DdlResult        (CREATE / ALTER / DROP)         |
|   - PipelineResult   (SHOW PIPELINE)                 |
|   - SQLResult        (SHOW CREATE PIPELINE)          |
|   - QueryRows        (DESCRIBE PIPELINE, etc.)       |
+------------------------------------------------------+


Final API surface:
------------------

gateway.run(sql: String)
    → Future[ElasticResult[QueryResult]]
```

---

## Examples

---

### SELECT

```scala
gateway.run("""
  SELECT id, name, age
  FROM dql_users
  WHERE age >= 18
  ORDER BY age DESC
""")
```

Result:

```scala
ElasticSuccess(QueryRows(rows))
```

---

### INSERT

```scala
gateway.run("""
  INSERT INTO dml_users (id, name, age)
  VALUES (1, 'Alice', 30)
""")
```

Result:

```scala
ElasticSuccess(DmlResult(inserted = 1))
```

---

### UPDATE

```scala
gateway.run("""
  UPDATE dml_users
  SET age = 31
  WHERE id = 1
""")
```

Result:

```scala
ElasticSuccess(DmlResult(updated = 1))
```

---

### DELETE

```scala
gateway.run("DELETE FROM dml_users WHERE id = 1")
```

Result:

```scala
ElasticSuccess(DmlResult(deleted = 1))
```

---

### CREATE TABLE

```scala
gateway.run("""
  CREATE TABLE dml_users (
    id INT,
    name TEXT,
    age INT,
    PRIMARY KEY (id)
  )
""")
```

---

### ALTER TABLE

```scala
gateway.run("""
  ALTER TABLE dml_users
  ADD COLUMN email TEXT
""")
```

---

### DROP TABLE

```scala
gateway.run("DROP TABLE dml_users")
```

---

### TRUNCATE TABLE

```scala
gateway.run("TRUNCATE TABLE dml_users")
```

---

### COPY INTO

```scala
gateway.run("""
  COPY INTO dml_users
  FROM '/data/users.json'
""")
```

Result:

```scala
ElasticSuccess(DmlResult(inserted = 100))
```

---

## Error Handling

Errors are returned as:

```scala
ElasticFailure(ElasticError(...))
```

Typical cases:

- SQL parsing errors
- Unsupported statements
- Mapping conflicts
- Invalid DDL operations
- Elasticsearch indexing errors

Example:

```scala
gateway.run("BAD SQL")
→ ElasticFailure(
    ElasticError(
      message   = "Error parsing SQL statement [BAD SQL]: <parser reason>",
      statusCode = Some(400),
      operation  = Some("sql")
    )
  )
```

The statement and the parser's reason are each rendered on a single line and capped at
200 characters; a longer one keeps its head and its tail with `...` between them. A `...` inside
the message is therefore a display bound, not the parser's own text.

---

## Notes

- SQL comments (`-- ...`) are stripped automatically.
- Empty statements return `EmptyResult`.
- Multiple statements are executed sequentially.
- The last statement’s result is returned.
- Streaming queries (`QueryStream`) require an active `ActorSystem`.

---

[Back to index](README.md)
