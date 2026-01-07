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

`ORDER BY` supports:

- single or multiple fields
- `ASC` / `DESC`
- expressions and nested fields

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

- `JOIN UNNEST(o.items)` flattens the `items` array.
- Each element should become a row with `items.product`, `items.quantity`, `items.price`. (⚠️ not implemented yet)
- Window function computes per-order totals.

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

##### **Current time:**

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
       FORMAT_DATETIME(birthdate, '%Y-%m-%d') AS birth_str
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

For large result sets, the Gateway uses Elasticsearch scroll or search-after mechanisms ([Scroll Search](../client/scroll.md)).

- `ORDER BY` is strongly recommended for deterministic pagination.
- `LIMIT`/`OFFSET` are translated to `from`/`size` when feasible; deep pagination may rely on scroll.

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

[Back to index](README.md)
