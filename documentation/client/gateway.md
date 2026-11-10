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
- `schema` is `None` when no mapping is known (an index that neither its mapping nor `_field_caps` can type, an alias on the ES 8/9
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

#### Elasticsearch mapping types

*Since `0.25.0`* (they read `ANY` before, and `constant_keyword` answered NULL), the mapping types
core reads but never writes have a declared type (*Declared*, the column's type in the table, as
`DESCRIBE TABLE` shows it) and a reported type (*Reported*, the type the result schema, JDBC and
Arrow report):

| Mapping type | Declared | Reported | Value class |
| --- | --- | --- | --- |
| `scaled_float` | DOUBLE | DOUBLE | `Double`, the value as stored in `_source` |
| `half_float` | REAL | REAL | `Float` |
| `unsigned_long` (7.10+) | BIGINT | BIGINT | `Long`; a value above 2^63-1 is kept as stored, a `java.math.BigInteger` |
| `date_nanos` (7.0+) | TIMESTAMP | TIMESTAMP | `ZonedDateTime` at UTC, nanoseconds kept |
| `ip` | KEYWORD | VARCHAR | `String` |
| `constant_keyword` (7.7+) | KEYWORD | VARCHAR | `String`, the mapping's value, read from its doc value |
| `flattened` (7.3+) | ANY | ANY | as read |

The column keeps its own mapping type beside its SQL type: `DESCRIBE TABLE` and `SHOW CREATE TABLE`
show it in the column's options (`type = 'ip'`), an `ALTER TABLE` re-sends the field as it is mapped,
and a sort or a `GROUP BY` key filter over it is decided by that type, as before. A type rule applies
by the SQL type, as over a column declared with it: `ip + 1` is refused, as over any `KEYWORD`, and
so is `HAVING hf = 0.1` on a `half_float` key (a `float` key's rule); a comparison with another type
family (`u = k` with `k` a `keyword`, `ip > n` with `n` a `long`, `sf > d` with `d` a `date`) is
refused, and `CAST(u AS VARCHAR) = k` compares as text; `dn * 2` and `dn / 2` are refused, as over
any TIMESTAMP; a `CASE`, `COALESCE`, `NULLIF`, `GREATEST` or `LEAST` whose arms have no common type
(`COALESCE(ip, 2)`, `COALESCE(sf, k)`, `GREATEST(dn, n)`) is refused. `dn + 1` moves a `date_nanos`
value by a day and keeps its nanoseconds. `ALTER COLUMN dn SET DATA TYPE DATE` reindexes a
`date_nanos` field into a `date`; `ALTER COLUMN ip SET DATA TYPE VARCHAR` maps it as `text`; a
`constant_keyword` column keeps its type (its value is in the mapping, not in the documents).

A `type` written in a column's options (`CREATE TABLE t (c VARCHAR OPTIONS (type = 'keyword'))`)
maps the field with it, as it did before `0.25.0`, and the column reads back with that type: an
`ALTER TABLE` re-sends it, `SHOW CREATE TABLE` prints it. Such a field is decided by its own
mapping type, like one core reads but never writes: beside another mapping type in an index
pattern, a native one included, it has no type. A sort over it carries the `unmapped_type` of its
declared type, as before, when the `type` reads as another SQL type (`KEYWORD` mapped `wildcard`,
`INT` mapped `long`), and none when it reads as the declared one (`DOUBLE` mapped `scaled_float`,
`TIMESTAMP` mapped `date_nanos`, `KEYWORD` mapped `ip`): *since `0.25.0`*, `ORDER BY` such a column,
or one a pattern leaves with no type, over a list or pattern where an index lacks it fails ("No
mapping found for [c] in order to sort on"), where it answered before. `ALTER COLUMN c SET DATA
TYPE` to another type drops it; `ARRAY<X>` of the column's own type keeps it.

#### Index patterns

A field the indices of a pattern (`FROM logs-*`) or of a list map differently takes the type a
`UNION ALL` of the two gives it (*since `0.25.0`*): a `keyword` beside a `long` or a `date` is a
VARCHAR, read by the keyword index's rules (`k + 1` is refused, as over that index), and every value
is its text (`"007"`, `"7"`); an `integer` beside a `long` is a BIGINT; a DATE beside a TIMESTAMP of
the same format a TIMESTAMP. A field left with no type answers as before (`ANY`): a `boolean` beside a
number, two date formats, a type a `UNION ALL` refuses (a date beside a number, a struct beside a
scalar), and a field one index maps with a type core reads but never writes (`ip`, `scaled_float`,
`date_nanos`, `constant_keyword`, ...) beside another type.

#### A user who may not read the mapping

A user holding only the index `read` privilege is refused `GET <index>` (403) but may ask
`_field_caps`. *Since `0.25.0`* (everything read `ANY` before, and an un-`LIMIT`ed statement failed
with "Failed to get ES version"), such a user's queries are typed from `_field_caps`, once per source
per schema-cache TTL, and its statements answer what a privileged user's answer, except:

- `_field_caps` holds no `_meta`: a column declared `DATE` reads TIMESTAMP, an `ARRAY<X>` declaration
  reads X (a several-valued cell answers the `List` of its values, never the several-values
  failure), and there is no comment and no default;
- it holds no date format: ISO text is read as a TIMESTAMP, any other value (a custom format's text,
  an epoch) is answered as stored, and a temporal literal in `WHERE` is forwarded to Elasticsearch
  verbatim;
- it reports `constant_keyword` as `keyword`: its projection and `SELECT *` read NULL from `_source`,
  as before, while `WHERE`, `GROUP BY` and conditionals over it answer its value, and a sort over
  it carries the `unmapped_type` of a keyword, so `ORDER BY ck` over a list where one index lacks
  `ck` answers where a privileged user's fails ("No mapping found for [ck] in order to sort on", as
  before); a runtime field is listed as a mapped keyword, so `SELECT *` holds a NULL column for it;
- DDL (`SHOW CREATE TABLE`, `DESCRIBE TABLE`, `ALTER TABLE`, CTAS) still needs the mapping, and
  answers its 403;
- an un-`LIMIT`ed statement pages by the Elasticsearch major the client is built for (`GET /` is
  refused too): search_after on the ES 6 and ES 7 clients, PIT on the ES 8 and ES 9 clients, unsliced
  where `_settings` is refused. A privilege granted later is seen once the schema-cache TTL expires
  (the version: once the client restarts).

A statement whose SELECT items are all computed -- literals, arithmetic, functions, casts, `CASE`
(`SELECT 2 AS k FROM t`, `SELECT n + 1 AS v FROM t`) -- asks Elasticsearch for no `_source`
(*since `0.25.0`*: the whole document was fetched): its script fields read doc values, so its rows
hold only its columns on every route (one-shot, async, scroll / PIT, `UNION ALL` legs), with a
schema or without. An `INSERT … SELECT` of such a statement writes only its columns, and is refused
by name when it does not project the target's PRIMARY KEY or PARTITION BY column; an entity read
(`searchAs…`) leaves a field the statement does not select at `None` (its default), and fails on a
required one.

In a column not declared a list, a one-element array is its element and an empty array NULL, and
several values fail the statement (*since `0.25.0`*, see
[Columns holding several values](../sql/dql_statements.md#columns-holding-several-values)); a column declared
`ARRAY<X>` reads a single value as a list of one. A date field whose format core cannot read
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
