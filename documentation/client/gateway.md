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
- splits multiple statements separated by `;`
- parses SQL into AST nodes
- dispatches to the correct executor
- returns a typed `QueryResult`

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
  FROM 'classpath:/data/users.json'
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
→ ElasticFailure(ElasticError(message = "Error parsing schema DDL statement: ..."))
```

---

## Notes

- SQL comments (`-- ...`) are stripped automatically.
- Empty statements return `EmptyResult`.
- Multiple statements are executed sequentially.
- The last statement’s result is returned.
- Streaming queries (`QueryStream`) require an active `ActorSystem`.

---

[Back to index](README.md)
