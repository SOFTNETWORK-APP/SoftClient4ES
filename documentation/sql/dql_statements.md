[Back to index](README.md)

# 📘 DQL Statements — SQL Gateway for Elasticsearch

## Introduction

The SQL Gateway provides a Data Query Language (DQL) on top of Elasticsearch, centered around the `SELECT` statement.  
It offers a familiar SQL experience while translating queries into Elasticsearch search, aggregations, and scroll APIs.

DQL supports:

- `SELECT` with expressions, aliases, nested fields, STRUCT and ARRAY<STRUCT>
- `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- `UNION ALL`
- `JOIN UNNEST` on `ARRAY<STRUCT>`
- aggregations, parent-level aggregations on nested arrays
- window functions with `OVER`
- rich function support (numeric, string, date/time, geo, conditional, type conversion)

---

## Table of Contents

- [SELECT](#select)
- [WHERE](#where)
- [ORDER BY](#order-by)
- [LIMIT / OFFSET](#limit--offset)
- [UNION ALL](#union-all)
- [JOIN UNNEST](#join-unnest)
- [Aggregations](#aggregations)
- [Parent-Level Aggregations on Nested Arrays](#parent-level-aggregations-on-nested-arrays)
- [Window Functions](#window-functions)
- [Functions](#functions)
- [Scroll & Pagination](#scroll--pagination)
- [Version Compatibility](#version-compatibility)
- [Limitations](#limitations)
- [SHOW TABLES](#show-tables)
- [SHOW TABLE](#show-table)
- [SHOW CREATE TABLE](#show-create-table)
- [DESCRIBE TABLE](#describe-table)
- [SHOW PIPELINES](#show-pipelines)
- [SHOW PIPELINE](#show-pipeline)
- [SHOW CREATE PIPELINE](#show-create-pipeline)
- [DESCRIBE PIPELINE](#describe-pipeline)
- [SHOW WATCHERS](#show-watchers)
- [SHOW WATCHER STATUS](#show-watcher-status)
- [SHOW ENRICH POLICIES](#show-enrich-policies)
- [SHOW ENRICH POLICY](#show-enrich-policy)

---

## SELECT

#### Basic syntax

```sql
SELECT [DISTINCT] expr1, expr2, ...
FROM table_name [alias]
[WHERE condition]
[GROUP BY expr1, expr2, ...]
[HAVING condition]
[ORDER BY expr1 [ASC|DESC], ...]
[LIMIT n]
[OFFSET m];
```

#### Nested fields and aliases

```sql
SELECT id,
       name AS full_name,
       profile.city AS city,
       profile.followers AS followers
FROM dql_users
ORDER BY id ASC;
```

- `profile` is a `STRUCT` column.
- `profile.city` and `profile.followers` access nested fields.
- Aliases (`AS full_name`, `AS city`) are returned as column names.

---

## WHERE

The `WHERE` clause supports:

- comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- logical operators: `AND`, `OR`, `NOT`
- `IN`, `NOT IN`
- `BETWEEN`
- `IS NULL`, `IS NOT NULL`
- `LIKE`, `RLIKE` (regex)
- conditions on nested fields (`profile.city`, `profile.followers`)

**Example**

```sql
SELECT id, name, age
FROM dql_users
WHERE (age > 20 AND profile.followers >= 100)
   OR (profile.city = 'Lyon' AND age < 50)
ORDER BY age DESC;
```

Another example with multiple operators:

```sql
SELECT id,
       age + 10 AS age_plus_10,
       name
FROM dql_users
WHERE age BETWEEN 20 AND 50
  AND name IN ('Alice', 'Bob', 'Chloe')
  AND name IS NOT NULL
  AND (name LIKE 'A%' OR name RLIKE '.*o.*');
```

---

## ORDER BY

`ORDER BY` sorts the result set by one or more expressions.

- Supports multiple sort keys
- Supports `ASC` and `DESC`
- Supports expressions and nested fields (e.g., `profile.city`)
- When used inside a window function (`OVER`), `ORDER BY` defines the logical ordering of the window

**Example**

```sql
SELECT id, name, age
FROM dql_users
ORDER BY age DESC, name ASC
LIMIT 2 OFFSET 1;
```

---

## LIMIT / OFFSET

- `LIMIT n` restricts the number of returned rows.
- `OFFSET m` skips the first `m` rows.
- Translated to Elasticsearch `from` + `size`.

Example:

```sql
SELECT id, name, age
FROM dql_users
ORDER BY age DESC
LIMIT 10 OFFSET 20;
```

---

## UNION ALL

`UNION ALL` combines the results of multiple `SELECT` queries **without removing duplicates**.

All SELECT statements in a UNION ALL must be **strictly compatible**:

- **same number of columns**
- **same column names** (after alias resolution)
- **same or implicitly compatible types**

If these conditions are not met, the Gateway raises a validation error before executing the query.

**Example**

```sql
SELECT id, name FROM dql_users WHERE age > 30
UNION ALL
SELECT id, name FROM dql_users WHERE age <= 30;
```

### Execution model

The SQL Gateway executes `UNION ALL` using **Elasticsearch Multi‑Search (`_msearch`)**:

1. Each SELECT query is translated into an independent ES search request.
2. All requests are sent in a single `_msearch` call.
3. The Gateway concatenates the results **in order**, without deduplication.
4. ORDER BY, LIMIT, OFFSET apply **per SELECT**, not globally (unless wrapped in a subquery, which is not supported).

### Notes

- `UNION ALL` does **not** sort or deduplicate results.
- Column names in the final output are taken from the **first SELECT**.
- All subsequent SELECTs must produce columns with the **same names**.
- Type mismatches should result in a validation error before execution. (⚠️ not implemented yet)

---

## JOIN UNNEST

The Gateway supports a specific form of join: `JOIN UNNEST` on `ARRAY<STRUCT>` columns.

### Table definition

```sql
CREATE TABLE IF NOT EXISTS dql_orders (
  id INT NOT NULL,
  customer_id INT,
  items ARRAY<STRUCT> FIELDS(
    product VARCHAR OPTIONS (fielddata = true),
    quantity INT,
    price DOUBLE
  ) OPTIONS (include_in_parent = false)
);
```

### Query with JOIN UNNEST and window function

```sql
SELECT
  o.id,
  items.product,
  items.quantity,
  SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o
JOIN UNNEST(o.items) AS items
WHERE items.quantity >= 1
ORDER BY o.id ASC;
```

`JOIN UNNEST` is intended to behave like a standard SQL UNNEST operation, where each element of an `ARRAY<STRUCT>` becomes a logical row that can participate in expressions, filters, and window functions.

⚠️ **Current status:**  
The SQL Gateway already supports reading and filtering nested array elements through `JOIN UNNEST`, and supports parent-level aggregations using window functions.  
However, **full row-level expansion (one output row per array element)** is **not implemented yet**.

This means:
- expressions such as `items.price` and `items.quantity` are fully usable
- window functions over `PARTITION BY parent_id` work
- parent-level aggregations can be computed
- but the final output still returns **one row per parent**, not one row per item

---

## Aggregations

Supported aggregate functions include:

- `COUNT(*)`, `COUNT(expr)`
- `SUM(expr)`
- `AVG(expr)`
- `MIN(expr)`
- `MAX(expr)`

### GROUP BY and HAVING

```sql
SELECT profile.city AS city,
       COUNT(*) AS cnt,
       AVG(age) AS avg_age
FROM dql_users
GROUP BY profile.city
HAVING COUNT(*) >= 1
ORDER BY COUNT(*) DESC;
```

- `GROUP BY` supports nested fields (`profile.city`).
- `HAVING` filters groups based on aggregate conditions.
- Translated to Elasticsearch aggregations.

---

## Parent-Level Aggregations on Nested Arrays

The SQL Gateway supports computing aggregations **over nested arrays** (e.g., `ARRAY<STRUCT>`) **without exploding them into multiple rows**.

This pattern:

- reads the nested array (`JOIN UNNEST`)
- computes aggregations per parent document (`PARTITION BY parent_id`)
- **returns one row per parent**
- **preserves the original nested array**
- **adds the aggregated value as a top-level field**

**Example**

```sql
SELECT
  o.id,
  o.items,
  SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o
JOIN UNNEST(o.items) AS items
WHERE items.quantity >= 1
ORDER BY o.id ASC;
```

**Result**

```json
[
  {
    "id": 1,
    "items": [
      {"product": "A", "quantity": 2, "price": 10.0},
      {"product": "B", "quantity": 1, "price": 20.0}
    ],
    "total_price": 40.0
  },
  {
    "id": 2,
    "items": [
      {"product": "C", "quantity": 3, "price": 5.0}
    ],
    "total_price": 15.0
  }
]
```

### Notes

- This is **not** a standard SQL window function (which would return one row per item).
- This is **not** an Elasticsearch nested aggregation (which would not return the items).
- This is a **hybrid parent-level aggregation**, unique to the SQL Gateway.

---

## Window Functions

Window functions operate over a logical window of rows defined by `OVER (PARTITION BY ... ORDER BY ...)`.

Supported window functions include:

- `SUM(expr) OVER (...)`
- `COUNT(expr) OVER (...)`
- `FIRST_VALUE(expr) OVER (...)`
- `LAST_VALUE(expr) OVER (...)`
- `ARRAY_AGG(expr) OVER (...)`

#### Basic window example

```sql
SELECT
  product,
  customer,
  amount,
  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product
FROM dql_sales
ORDER BY product, ts;
```

#### FIRST_VALUE / LAST_VALUE / ARRAY_AGG

```sql
SELECT
  product,
  customer,
  amount,
  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product,
  FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS first_amount,
  LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS last_amount,
  ARRAY_AGG(amount) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS amounts_array
FROM dql_sales
ORDER BY product, ts;
```

Notes:

- `PARTITION BY` defines the grouping key.
- `ORDER BY` inside `OVER` defines the window ordering.
- `LIMIT` inside `ARRAY_AGG` restricts the collected values.
- Frame clauses (`ROWS BETWEEN ...`) are not exposed; the engine uses a default frame per function semantics.

---

## Functions

The SQL Gateway provides a rich set of SQL functions covering:

- numeric and trigonometric operations
- string manipulation
- date and time extraction, arithmetic, formatting and parsing
- geospatial functions
- conditional expressions
- type conversion

All functions operate on Elasticsearch documents and are evaluated by the SQL engine.

---

#### Numeric & Trigonometric

##### **Arithmetic:**

| Function      | Description          |
|---------------|----------------------|
| `ABS(x)`      | Absolute value       |
| `CEIL(x)`     | Round up             |
| `FLOOR(x)`    | Round down           |
| `ROUND(x, n)` | Round to n decimals  |
| `SQRT(x)`     | Square root          |
| `POW(x, y)`   | Power                |
| `EXP(x)`      | Exponential          |
| `LOG(x)`      | Natural logarithm    |
| `LOG10(x)`    | Base-10 logarithm    |
| `SIGN(x)`     | Sign of x (−1, 0, 1) |


##### **Trigonometric:**

| Function      | Description                      |
|---------------|----------------------------------|
| `SIN(x)`      | Sine                             |
| `COS(x)`      | Cosine                           |
| `TAN(x)`      | Tangent                          |
| `ASIN(x)`     | Arc-sine                         |
| `ACOS(x)`     | Arc-cosine                       |
| `ATAN(x)`     | Arc-tangent                      |
| `ATAN2(y, x)` | Arc-tangent of y/x with quadrant |
| `PI()`        | π constant                       |
| `RADIANS(x)`  | Degrees → radians                |
| `DEGREES(x)`  | Radians → degrees                |

**Example**

```sql
SELECT id,
       ABS(age) AS abs_age,
       SQRT(age) AS sqrt_age,
       POW(age, 2) AS pow_age,
       LOG(age) AS log_age,
       SIN(age) AS sin_age,
       ATAN2(age, 10) AS atan2_val
FROM dql_users;
```

---

#### String

##### **Manipulation:**


| Function                     | Description                   |
|------------------------------|-------------------------------|
| `CONCAT(a, b, ...)`          | Concatenate strings           |
| `SUBSTRING(str, start, len)` | Extract substring             |
| `LOWER(str)`                 | Lowercase                     |
| `UPPER(str)`                 | Uppercase                     |
| `TRIM(str)`                  | Trim both sides               |
| `LTRIM(str)`                 | Trim left                     |
| `RTRIM(str)`                 | Trim right                    |
| `LENGTH(str)`                | String length                 |
| `REPLACE(str, from, to)`     | Replace substring             |
| `LEFT(str, n)`               | Left n chars                  |
| `RIGHT(str, n)`              | Right n chars                 |
| `REVERSE(str)`               | Reverse string                |
| `POSITION(substr IN str)`    | 1-based position of substring |

##### **Pattern matching:**

| Function                     | Description                                         |
|------------------------------|-----------------------------------------------------|
| `REGEXP_LIKE(str, pattern)`  | True if regex matches                               |
| `MATCH(str) AGAINST (query)` | Full-text match (backed by ES query_string / match) |

**Example:**

```sql
SELECT id,
       CONCAT(name.raw, '_suffix') AS name_concat,
       SUBSTRING(name.raw, 1, 2) AS name_sub,
       LOWER(name.raw) AS name_lower,
       LTRIM(name.raw) AS name_ltrim,
       POSITION('o' IN name.raw) AS pos_o,
       REGEXP_LIKE(name.raw, '.*o.*') AS has_o
FROM dql_users
ORDER BY id ASC;
```

---

#### Date & Time

##### **Current :**

| Function                                  | Description                 |
|-------------------------------------------|-----------------------------|
| `CURRENT_DATE`                            | Current date (UTC)          |
| `TODAY()`                                 | Alias for CURRENT_DATE      |
| `CURRENT_TIMESTAMP` \| `CURRENT_DATETIME` | Current timestamp (UTC)     |
| `NOW()`                                   | Alias for CURRENT_TIMESTAMP |
| `CURRENT_TIME`                            | Current time (UTC)          |

##### **Extraction:**

| Function          | Description  |
|-------------------|--------------|
| `YEAR(date)`      | Year         |
| `MONTH(date)`     | Month        |
| `DAY(date)`       | Day of month |
| `WEEKDAY(date)`   | Day of week  |
| `YEARDAY(date)`   | Day of year  |
| `HOUR(ts)`        | Hour         |
| `MINUTE(ts)`      | Minute       |
| `SECOND(ts)`      | Second       |
| `MILLISECOND(ts)` | Millisecond  |
| `MICROSECOND(ts)` | Microsecond  |
| `NANOSECOND(ts)`  | Nanosecond   |

##### **EXTRACT:**

```sql
EXTRACT(unit FROM date_or_timestamp)
```

Supported units include: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, etc.

**Example:**

```sql
SELECT id,
       EXTRACT(YEAR FROM birthdate) AS year_b,
       EXTRACT(MONTH FROM birthdate) AS month_b
FROM dql_users;
```

##### **Arithmetic:**

| Function                            | Description                      |
|-------------------------------------|----------------------------------|
| `DATE_ADD(date, INTERVAL n unit)`   | Add interval                     |
| `DATE_SUB(date, INTERVAL n unit)`   | Subtract interval                |
| `DATETIME_ADD(ts, INTERVAL n unit)` | Add interval to timestamp        |
| `DATETIME_SUB(ts, INTERVAL n unit)` | Subtract interval from timestamp |
| `DATE_DIFF(date1, date2, unit)`     | Difference in units              |
| `DATE_TRUNC(date, unit)`            | Truncate to unit                 |

##### **Formatting & parsing:**

| Function                       | Description                 |
|--------------------------------|-----------------------------|
| `DATE_FORMAT(ts, pattern)`     | Format date as string       |
| `DATE_PARSE(str, pattern)`     | Parse string into date      |
| `DATETIME_FORMAT(ts, pattern)` | Format timestamp as string  |
| `DATETIME_PARSE(str, pattern)` | Parse string into timestamp |

**Supported MySQL-style Date/Time Patterns**

##### **Special functions:**

| Function               | Description                   |
|------------------------|-------------------------------|
| `LAST_DAY(date)`       | Last day of month             |
| `EPOCHDAY(date)`       | Days since epoch (1970-01-01) |
| `OFFSET_SECONDS(date)` | Epoch seconds                 |

**Example:**

```sql
SELECT id,
       YEAR(CURRENT_DATE) AS current_year,
       MONTH(CURRENT_DATE) AS current_month,
       DAY(CURRENT_DATE) AS current_day,
       YEAR(birthdate) AS year_b,
       DATE_DIFF(CURRENT_DATE, birthdate, YEAR) AS diff_years,
       DATE_TRUNC(birthdate, MONTH) AS trunc_month,
       DATETIME_FORMAT(birthdate, '%Y-%m-%d') AS birth_str
FROM dql_users;
```

---

#### Geospatial

##### **POINT:**

```sql
POINT(longitude, latitude)
```

##### **ST_DISTANCE:**

```sql
ST_DISTANCE(location, POINT(2.3522, 48.8566))
```

Example:

```sql
CREATE TABLE IF NOT EXISTS dql_geo (
  id INT NOT NULL,
  location GEO_POINT,
  PRIMARY KEY (id)
);

SELECT id,
       ST_DISTANCE(location, POINT(2.3522, 48.8566)) AS dist_paris
FROM dql_geo;
```

---

#### Conditional

##### CASE WHEN

```sql
CASE
  WHEN condition THEN value
  [WHEN condition2 THEN value2]
  [ELSE default]
END
```

##### COALESCE

```sql
COALESCE(a, b, c)
```

Returns the first non-null value.

##### NULLIF

```sql
NULLIF(a, b)
```

Returns NULL if `a = b`, otherwise `a`.

**Example:**

```sql
SELECT id,
       CASE
         WHEN age >= 50 THEN 'senior'
         WHEN age >= 30 THEN 'adult'
         ELSE 'young'
       END AS age_group,
       COALESCE(name, 'unknown') AS safe_name
FROM dql_users;
```

---

#### Type Conversion

##### CAST

```sql
CAST(value AS TYPE)
```

##### TRY_CAST

Returns NULL instead of failing on invalid conversion.

```sql
TRY_CAST('123' AS INT)
```

##### SAFE_CAST

Alias for TRY_CAST.

##### PostgreSQL-style operator

```sql
value::TYPE
```

**Example:**

```sql
SELECT id,
       age::BIGINT AS age_bigint,
       CAST(age AS DOUBLE) AS age_double,
       TRY_CAST('123' AS INT) AS try_cast_ok,
       SAFE_CAST('abc' AS INT) AS safe_cast_null
FROM dql_users;
```

---

## Scroll & Pagination

For large result sets, the Gateway uses Elasticsearch scroll or search-after mechanisms depending on backend capabilities ([Scroll Search](../client/scroll.md)).

Notes:

- `LIMIT` and `OFFSET` are applied by the SQL engine after retrieving documents from Elasticsearch
- Deep pagination may require scroll
- When using `search_after`, an explicit `ORDER BY` clause is required for deterministic pagination
- Without `ORDER BY`, result ordering is not guaranteed

---

## Version Compatibility

| Feature                        | ES6 | ES7 | ES8 | ES9 |
|--------------------------------|-----|-----|-----|-----|
| Basic SELECT                   | ✔   | ✔   | ✔   | ✔   |
| Nested fields                  | ✔   | ✔   | ✔   | ✔   |
| UNION ALL                      | ✔   | ✔   | ✔   | ✔   |
| JOIN UNNEST                    | ✔   | ✔   | ✔   | ✔   |
| Aggregations                   | ✔   | ✔   | ✔   | ✔   |
| Parent-level nested array aggs | ✔   | ✔   | ✔   | ✔   |
| Window functions               | ✔   | ✔   | ✔   | ✔   |
| Geospatial functions           | ✔   | ✔   | ✔   | ✔   |
| Date/time functions            | ✔   | ✔   | ✔   | ✔   |
| String / math functions        | ✔   | ✔   | ✔   | ✔   |

---

## Limitations

Even though the DQL engine is powerful, some SQL features are not (yet) supported:

- No traditional SQL joins (only `JOIN UNNEST` on `ARRAY<STRUCT>`)
- No correlated subqueries
- No arbitrary subqueries in `SELECT` or `WHERE` (except `INSERT ... AS SELECT` in DML)
- No `GROUPING SETS`, `CUBE`, `ROLLUP`
- No `DISTINCT ON`
- No explicit window frame clauses (`ROWS BETWEEN ...`)

These constraints keep the translation to Elasticsearch efficient and predictable.

---

## SHOW TABLES

```sql
SHOW TABLES [LIKE 'pattern'];
```

Returns a list of all tables with summary information (name, type, primary key, partitioning).

May be filtered using `LIKE` with SQL wildcard `%`.

**Example:*

```sql
CREATE TABLE IF NOT EXISTS show_users (
  id INT NOT NULL,
  name VARCHAR FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  age INT DEFAULT 0,
  PRIMARY KEY (id)
);

SHOW TABLES LIKE 'show_%';
```

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| show_users | REGULAR | id |             |
📊 1 row(s) (7ms)

---

## SHOW TABLE

Returns:

- schema summary
- primary key
- partitioning
- settings
- mappings
- ddl

```sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL COMMENT 'user identifier',
  name VARCHAR FIELDS(raw Keyword COMMENT 'sortable') DEFAULT 'anonymous' OPTIONS (analyzer = 'french', search_analyzer = 'french'),
  birthdate DATE,
  age INT SCRIPT AS (DATEDIFF(birthdate, CURRENT_DATE, YEAR)),
  ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
  profile STRUCT FIELDS(
    bio VARCHAR,
    followers INT,
    join_date DATE,
    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY))
  ) COMMENT 'user profile',
  PRIMARY KEY (id)
) PARTITION BY birthdate (MONTH), OPTIONS (mappings = (dynamic = false));

SHOW TABLE users;
```

📋 Table: users [Regular]

| Field             | Type      | Null | Key | Default           | Comment         | Script                                          | Extra                                             |
|-------------------|-----------|------|-----|-------------------|-----------------|-------------------------------------------------|---------------------------------------------------|
| age               | INT       | yes  |     | NULL              |                 | DATE_DIFF(birthdate, CURRENT_DATE, YEAR)        | ()                                                |
| birthdate         | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| id                | INT       | no   | PRI | NULL              | user identifier |                                                 | ()                                                |
| ingested_at       | TIMESTAMP | yes  |     | _ingest.timestamp |                 |                                                 | ()                                                |
| name              | VARCHAR   | yes  |     | anonymous         |                 |                                                 | (analyzer = "french", search_analyzer = "french") |
| name.raw          | KEYWORD   | yes  |     | NULL              | sortable        |                                                 | ()                                                |
| profile           | STRUCT    | yes  |     | NULL              | user profile    |                                                 | ()                                                |
| profile.seniority | INT       | yes  |     | NULL              |                 | DATE_DIFF(profile.join_date, CURRENT_DATE, DAY) | ()                                                |
| profile.join_date | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.followers | INT       | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.bio       | VARCHAR   | yes  |     | NULL              |                 |                                                 | ()                                                |

🔑 PRIMARY KEY id
📅 PARTITION BY birthdate (MONTH)

⚙️ Settings:
default_pipeline: 'users_ddl_default_pipeline'


🗺️ Mappings:
dynamic: false
_meta: (primary_key = ('id'), partition_by = (column = 'birthdate', granularity = 'M'), columns = (...), type = 'regular', materialized_views = ())


📝 DDL:
```sql
CREATE OR REPLACE TABLE users (
	age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)),
	birthdate DATE,
	id INT NOT NULL COMMENT 'user identifier',
	ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
	name VARCHAR FIELDS (
		raw KEYWORD COMMENT 'sortable'
	) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french"),
	profile STRUCT FIELDS (
		seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY)),
		join_date DATE,
		followers INT,
		bio VARCHAR
	) COMMENT 'user profile',
	PRIMARY KEY (id)
)
PARTITION BY birthdate (MONTH),
OPTIONS = (
	mappings = (dynamic = false, _meta = (primary_key = ["id"], partition_by = (column = "birthdate", granularity = "M"), columns = (...), type = "regular", materialized_views = [])),
	settings = (default_pipeline = "users_ddl_default_pipeline")
)
```

---

## SHOW CREATE TABLE

Returns the full, normalized DDL statement used to create the table, including all fields, types, options, comments, and scripts.

```sql
SHOW CREATE TABLE users;
```

```sql
CREATE OR REPLACE TABLE users (
	age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)),
	birthdate DATE,
	id INT NOT NULL COMMENT 'user identifier',
	ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
	name VARCHAR FIELDS (
		raw KEYWORD COMMENT 'sortable'
	) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french"),
	profile STRUCT FIELDS (
		seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY)),
		join_date DATE,
		followers INT,
		bio VARCHAR
	) COMMENT 'user profile',
	PRIMARY KEY (id)
)
PARTITION BY birthdate (MONTH),
OPTIONS = (
	mappings = (dynamic = false, _meta = (primary_key = ["id"], partition_by = (column = "birthdate", granularity = "M"), columns = (...), type = "regular", materialized_views = [])),
	settings = (default_pipeline = "users_ddl_default_pipeline")
)
```

## DESCRIBE TABLE

```sql
DESCRIBE TABLE users;
```

Returns the **normalized SQL schema**, including :

- fields
- types
- nulls
- keys
- defaults
- comments
- scripts
- STRUCT fields
- options

| Field             | Type      | Null | Key | Default           | Comment         | Script                                          | Extra                                             |
|-------------------|-----------|------|-----|-------------------|-----------------|-------------------------------------------------|---------------------------------------------------|
| age               | INT       | yes  |     | NULL              |                 | DATE_DIFF(birthdate, CURRENT_DATE, YEAR)        | ()                                                |
| birthdate         | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| id                | INT       | no   | PRI | NULL              | user identifier |                                                 | ()                                                |
| ingested_at       | TIMESTAMP | yes  |     | _ingest.timestamp |                 |                                                 | ()                                                |
| name              | VARCHAR   | yes  |     | anonymous         |                 |                                                 | (analyzer = "french", search_analyzer = "french") |
| name.raw          | KEYWORD   | yes  |     | NULL              | sortable        |                                                 | ()                                                |
| profile           | STRUCT    | yes  |     | NULL              | user profile    |                                                 | ()                                                |
| profile.seniority | INT       | yes  |     | NULL              |                 | DATE_DIFF(profile.join_date, CURRENT_DATE, DAY) | ()                                                |
| profile.join_date | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.followers | INT       | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.bio       | VARCHAR   | yes  |     | NULL              |                 |                                                 | ()                                                |

---

## SHOW PIPELINES

```sql
SHOW PIPELINES;
```

**Description**

- Returns a list of all user-defined pipelines with summary information (name, number of processors)

**Example**

```sql
SHOW PIPELINES;
```

| name                                             | processors_count |
|--------------------------------------------------|------------------|
| users_alter4_ddl_default_pipeline                | 1                |
| user_pipeline                                    | 6                |
| metrics-apm.transaction@default-pipeline         | 3                |
| users_alter6_ddl_default_pipeline                | 1                |
| tmp_truncate_ddl_default_pipeline                | 1                |
| users_alter5_ddl_default_pipeline                | 3                |
| logs@default-pipeline                            | 2                |
| dql_users_ddl_default_pipeline                   | 1                |
| apm@pipeline                                     | 4                |
| logs-apm.error@default-pipeline                  | 3                |
| metrics-apm.service_transaction@default-pipeline | 3                |
| users_cr_ddl_default_pipeline                    | 1                |
| users_alter2_ddl_default_pipeline                | 1                |
| metrics-apm.internal@default-pipeline            | 6                |
| traces-apm.rum@default-pipeline                  | 3                |
| metrics-apm.app@default-pipeline                 | 3                |
| users_alter1_ddl_default_pipeline                | 2                |
| tmp_drop_ddl_default_pipeline                    | 1                |
| dql_sales_ddl_default_pipeline                   | 1                |
| ent-search-generic-ingestion                     | 6                |
| logs@json-message                                | 4                |
| users_alter3_ddl_default_pipeline                | 1                |
| dml_users_ddl_default_pipeline                   | 1                |
| traces-apm@default-pipeline                      | 3                |
| metrics-apm@pipeline                             | 3                |
| users_alter8_ddl_default_pipeline                | 5                |
| dml_chain_ddl_default_pipeline                   | 1                |
| users_ddl_default_pipeline                       | 6                |
| copy_into_test_ddl_default_pipeline              | 1                |
| accounts_src_ddl_default_pipeline                | 1                |
| users_alter7_ddl_default_pipeline                | 1                |
| dml_accounts_ddl_default_pipeline                | 1                |
| reindex-data-stream-pipeline                     | 1                |
| behavioral_analytics-events-final_pipeline       | 9                |
| logs@json-pipeline                               | 4                |
| logs-default-pipeline                            | 2                |
| dql_orders_ddl_default_pipeline                  | 1                |
| dml_logs_ddl_default_pipeline                    | 1                |
| search-default-ingestion                         | 6                |
| accounts_ddl_default_pipeline                    | 1                |
| dql_geo_ddl_default_pipeline                     | 1                |
| show_users_ddl_default_pipeline                  | 2                |
| logs-apm.app@default-pipeline                    | 3                |
| traces-apm@pipeline                              | 7                |
| desc_users_ddl_default_pipeline                  | 2                |
| metrics-apm.service_summary@default-pipeline     | 3                |
| metrics-apm.service_destination@default-pipeline | 3                |
📊 47 row(s) (10ms)

---

## SHOW PIPELINE

```sql
SHOW PIPELINE pipeline_name;
```

**Description**

- Returns a high‑level view of the pipeline processors

**Example**

```sql
SHOW PIPELINE user_pipeline;
```

🔄 Pipeline: user_pipeline

Processors: (6)
| processor_type  | description                                                                       | field             | ignore_failure | options                                                                                                                                                                                                                                                                                             |
|-----------------|-----------------------------------------------------------------------------------|-------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| set             | DEFAULT 'anonymous'                                                               | name              | yes            | (value = "anonymous", if = "ctx.name == null")                                                                                                                                                                                                                                                      |
| script          | age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))                      | age               | yes            | (lang = "painless", source = "def param1 = ctx.birthdate; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.age = (param1 == null) ? null : Long.valueOf(ChronoUnit.YEARS.between(param1, param2))")                       |
| set             | DEFAULT _ingest.timestamp                                                         | ingested_at       | yes            | (value = "_ingest.timestamp", if = "ctx.ingested_at == null")                                                                                                                                                                                                                                       |
| script          | profile.seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY)) | profile.seniority | yes            | (lang = "painless", source = "def param1 = ctx.profile?.join_date; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (param1 == null) ? null : Long.valueOf(ChronoUnit.DAYS.between(param1, param2))") |
| date_index_name | PARTITION BY birthdate (MONTH)                                                    | birthdate         | yes            | (date_rounding = "M", date_formats = ["yyyy-MM"], index_name_prefix = "users-")                                                                                                                                                                                                                     |
| set             | PRIMARY KEY (id)                                                                  | _id               | no             | (value = "{{id}}", ignore_empty_value = false)                                                                                                                                                                                                                                                      |

📝 DDL:
```sql
CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
	SET(
		description = "DEFAULT 'anonymous'", 
		field = "name", 
		ignore_failure = true, 
		value = "anonymous", 
		if = "ctx.name == null"
	), 
	SCRIPT(
		description = "age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	SET(
		description = "DEFAULT _ingest.timestamp", 
		field = "ingested_at", 
		ignore_failure = true, 
		value = "_ingest.timestamp", 
		if = "ctx.ingested_at == null"
	), 
	SCRIPT(
		description = "profile.seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	DATE_INDEX_NAME(
		description = "PARTITION BY birthdate (MONTH)", 
		field = "birthdate", 
		date_rounding = "M", 
		date_formats = ["yyyy-MM"], 
		index_name_prefix = "users-", 
		ignore_failure = true
	), 
	SET(
		description = "PRIMARY KEY (id)", 
		field = "_id", 
		value = "{{id}}", 
		ignore_failure = false, 
		ignore_empty_value = false)
	)
)
```

---

## SHOW CREATE PIPELINE

```sql
SHOW CREATE PIPELINE pipeline_name;
```

**Description**

- Returns the full, normalized DDL statement used to create the pipeline, including all processors, options, and flags.

**Example**

```sql
SHOW CREATE PIPELINE user_pipeline;
```

```sql
CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
	SET(
		description = "DEFAULT 'anonymous'", 
		field = "name", 
		ignore_failure = true, 
		value = "anonymous", 
		if = "ctx.name == null"
	), 
	SCRIPT(
		description = "age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	SET(
		description = "DEFAULT _ingest.timestamp", 
		field = "ingested_at", 
		ignore_failure = true, 
		value = "_ingest.timestamp", 
		if = "ctx.ingested_at == null"
	), 
	SCRIPT(
		description = "profile.seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	DATE_INDEX_NAME(
		description = "PARTITION BY birthdate (MONTH)", 
		field = "birthdate", 
		date_rounding = "M", 
		date_formats = ["yyyy-MM"], 
		index_name_prefix = "users-", 
		ignore_failure = true
	), 
	SET(
		description = "PRIMARY KEY (id)", 
		field = "_id", 
		value = "{{id}}", 
		ignore_failure = false, 
		ignore_empty_value = false)
	)
)
```

---

## DESCRIBE PIPELINE

```sql
DESCRIBE PIPELINE pipeline_name;
```

**Description**

- Returns the full, normalized definition of the pipeline:
	- processors in execution order
	- full configuration of each processor (`SET`, `SCRIPT`, `REMOVE`, `RENAME`, `DATE_INDEX_NAME`, etc.)
	- flags such as `ignore_failure`, `if`, `description`

**Example**

```sql
DESCRIBE PIPELINE user_pipeline;
```

| processor_type  | description                                                                       | field             | ignore_failure | options                                                                                                                                                                                                                                                                                             |
|-----------------|-----------------------------------------------------------------------------------|-------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| set             | DEFAULT 'anonymous'                                                               | name              | yes            | (value = "anonymous", if = "ctx.name == null")                                                                                                                                                                                                                                                      |
| script          | age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))                      | age               | yes            | (lang = "painless", source = "def param1 = ctx.birthdate; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.age = (param1 == null) ? null : Long.valueOf(ChronoUnit.YEARS.between(param1, param2))")                       |
| set             | DEFAULT _ingest.timestamp                                                         | ingested_at       | yes            | (value = "_ingest.timestamp", if = "ctx.ingested_at == null")                                                                                                                                                                                                                                       |
| script          | profile.seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY)) | profile.seniority | yes            | (lang = "painless", source = "def param1 = ctx.profile?.join_date; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (param1 == null) ? null : Long.valueOf(ChronoUnit.DAYS.between(param1, param2))") |
| date_index_name | PARTITION BY birthdate (MONTH)                                                    | birthdate         | yes            | (date_rounding = "M", date_formats = ["yyyy-MM"], index_name_prefix = "users-")                                                                                                                                                                                                                     |
| set             | PRIMARY KEY (id)                                                                  | _id               | no             | (value = "{{id}}", ignore_empty_value = false)                                                                                                                                                                                                                                                      |
📊 6 row(s) (1ms)

---

## SHOW WATCHERS

```sql
SHOW WATCHERS;
```

Returns a list of all watchers with summary information (name, activation state, last execution time, ...).

**Example:**

```sql
CREATE OR REPLACE WATCHER my_watcher_interval AS
 EVERY 5 SECONDS
 FROM my_index WITHIN 1 MINUTE
 ALWAYS DO
 log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
 END;

CREATE OR REPLACE WATCHER my_watcher_cron AS
 AT SCHEDULE '* * * * * ?'
 WITH INPUTS search_data AS FROM my_index WITHIN 1 MINUTE, http_data AS GET "https://jsonplaceholder.typicode.com/todos/1" HEADERS ("Accept" = "application/json") TIMEOUT (connection = "5s", read = "10s")
 WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
 DO
 log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
 END;

SHOW WATCHERS;
```

| id                  | active | status  | status_emoji | severity | is_healthy | is_operational | last_checked | time_since_last_check_seconds | frequency_seconds | created_at               | execution_status | execution_status_emoji | execution_severity | overall_status | overall_status_emoji | overall_severity |
|---------------------|--------|---------|--------------|----------|------------|----------------|--------------|-------------------------------|-------------------|--------------------------|------------------|------------------------|--------------------|----------------|----------------------|------------------|
| my_watcher_interval | true   | Healthy | 🟢           | 0        | true       | true           | never        | -1                            | 5                 | 2026-02-11T12:11:02.542Z | NULL             | NULL                   | -1                 | Healthy        | 🟢                   | 0                |
| my_watcher_cron     | true   | Healthy | 🟢           | 0        | true       | true           | never        | -1                            | 1                 | 2026-02-11T12:11:02.816Z | NULL             | NULL                   | -1                 | Healthy        | 🟢                   | 0                |
📊 2 row(s) (9ms)

**Query watchers require Elasticsearch 7.11+.**

---

## SHOW WATCHER STATUS

```sql
SHOW WATCHER STATUS watcher_name;
```

Returns:
- Activation state (active/inactive)
- Last execution time
- Last condition met time
- Execution statistics

**Example:**

```sql
SHOW WATCHER STATUS auto_refresh_orders_with_customers_mv_enrich_policies;
```

| id                                                    | active | status  | status_emoji | severity | is_healthy | is_operational | last_checked             | time_since_last_check_seconds | frequency_seconds | created_at               | execution_status | execution_status_emoji | execution_severity | overall_status | overall_status_emoji | overall_severity |
|-------------------------------------------------------|--------|---------|--------------|----------|------------|----------------|--------------------------|-------------------------------|-------------------|--------------------------|------------------|------------------------|--------------------|----------------|----------------------|------------------|
| auto_refresh_orders_with_customers_mv_enrich_policies | true   | Healthy | 🟢           | 0        | true       | true           | 2026-02-11T10:28:20.581Z | 5                             | 8                 | 2026-02-11T10:28:12.174Z | Executed         | 🟢                     | 0                  | Healthy        | 🟢                   | 0                |
📊 1 row(s) (9ms)

---

## SHOW ENRICH POLICIES

```sql
SHOW ENRICH POLICIES;
```

Returns a list of all enrich policies with their configurations.

**Example:*

```sql
CREATE TABLE IF NOT EXISTS dql_users (
  id INT NOT NULL,
  name VARCHAR FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  age INT,
  birthdate DATE,
  profile STRUCT FIELDS(
    city VARCHAR OPTIONS (fielddata = true),
    followers INT
  )
);

CREATE OR REPLACE ENRICH POLICY my_policy
FROM dql_users
ON id
ENRICH name, profile.city
WHERE age > 10;

SHOW ENRICH POLICIES;

```

| name      | type  | indices   | match_field | enrich_fields     | query                                             |
|-----------|-------|-----------|-------------|-------------------|---------------------------------------------------|
| my_policy | match | dql_users | id          | name,profile.city | {"bool":{"filter":[{"range":{"age":{"gt":10}}}]}} |
📊 1 row(s) (4ms)


---

## SHOW ENRICH POLICY

```sql
SHOW ENRICH POLICY policy_name;
```

Returns policy details, including:
- Name
- Type
- Indices
- Match field
- Enrich fields
- Query criteria (if any)

**Example:**

```sql
SHOW ENRICH POLICY my_policy;
```

| name      | type  | indices   | match_field | enrich_fields     | query                                             |
|-----------|-------|-----------|-------------|-------------------|---------------------------------------------------|
| my_policy | match | dql_users | id          | name,profile.city | {"bool":{"filter":[{"range":{"age":{"gt":10}}}]}} |
📊 1 row(s) (4ms)

---

[Back to index](README.md)
