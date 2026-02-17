[Back to index](README.md)

# Materialized Views — SQL Gateway for Elasticsearch

---

## Introduction

Materialized views provide **precomputed, automatically refreshed query results** stored as Elasticsearch indices. They are ideal for:

- **Denormalizing joins** — flatten data from multiple indices into a single queryable index
- **Precomputing aggregations** — store GROUP BY results for fast dashboard queries
- **Enriching data** — combine lookup data with transactional data
- **Computed columns** — add scripted fields to the materialized result

Unlike regular views, materialized views persist their results and refresh automatically at a configurable interval.

---

## Architecture

Under the hood, a materialized view translates into a pipeline of Elasticsearch primitives:

| SQL Concept             | Elasticsearch Primitive                                           |
|-------------------------|-------------------------------------------------------------------|
| Source tables            | Source indices                                                   |
| JOIN                    | Enrich policies + ingest pipelines                                |
| Computed columns        | Script processors in ingest pipelines                             |
| Continuous refresh      | Transforms (latest mode) with configurable frequency              |
| Aggregations (GROUP BY) | Transforms (pivot mode)                                          |
| Auto-refresh watcher    | Watcher (re-executes enrich policies on source data changes)      |

### Deployment Sequence

When a materialized view is created, the engine deploys artifacts in this order:

1. **Alter source schemas** — add changelog tracking fields (`_updated_at`)
2. **Create intermediate indices** — changelog indices, enriched indices, final view index
3. **Preload changelogs** — copy existing data into changelog indices
4. **Create enrich policies** — define lookup enrichment from source indices
5. **Create watcher** — schedule automatic re-execution of enrich policies
6. **Execute enrich policies** — build initial enrich indices
7. **Create ingest pipelines** — define enrichment + computed field processors
8. **Create transforms** — changelog, enrichment, computed fields, aggregation
9. **Start transforms** — sequentially, with checkpoint waits between groups
10. **Save metadata** — persist MV definition for SHOW/DESCRIBE/DROP

Rollback is automatic on deployment failure.

---

## Table of Contents

- [CREATE MATERIALIZED VIEW](#create-materialized-view)
- [DROP MATERIALIZED VIEW](#drop-materialized-view)
- [REFRESH MATERIALIZED VIEW](#refresh-materialized-view)
- [DESCRIBE MATERIALIZED VIEW](#describe-materialized-view)
- [SHOW MATERIALIZED VIEW](#show-materialized-view)
- [SHOW CREATE MATERIALIZED VIEW](#show-create-materialized-view)
- [SHOW MATERIALIZED VIEW STATUS](#show-materialized-view-status)
- [SHOW MATERIALIZED VIEWS](#show-materialized-views)
- [Complete Example](#complete-example)
- [Version Compatibility](#version-compatibility)
- [Limitations](#limitations)
- [Quick Reference](#quick-reference)

---

## CREATE MATERIALIZED VIEW

### Syntax

```sql
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] view_name
[REFRESH EVERY interval time_unit]
[WITH (option = value [, ...])]
AS select_statement
```

| Component          | Required | Description                                                    |
|--------------------|----------|----------------------------------------------------------------|
| `view_name`        | Yes      | Unique name for the materialized view                          |
| `OR REPLACE`       | No       | Replace existing view (drops and recreates artifacts)          |
| `IF NOT EXISTS`    | No       | Skip creation if view already exists                           |
| `REFRESH EVERY`    | No       | Automatic refresh interval (default: engine-defined)           |
| `WITH (...)`       | No       | Additional options (see below)                                 |
| `AS select`        | Yes      | The SELECT query defining the view                             |

### Refresh Interval

The `REFRESH EVERY` clause controls how frequently transforms check for new data.

```sql
REFRESH EVERY 10 SECONDS
REFRESH EVERY 5 MINUTES
REFRESH EVERY 1 HOUR
```

**Supported time units:** `MILLISECOND(S)`, `SECOND(S)`, `MINUTE(S)`, `HOUR(S)`, `DAY(S)`, `WEEK(S)`, `MONTH(S)`, `YEAR(S)`

### Options

| Option          | Type     | Description                                              | Example     |
|-----------------|----------|----------------------------------------------------------|-------------|
| `delay`         | Interval | Delay before processing new data (allows late arrivals)  | `'5s'`      |
| `user_latency`  | Interval | Maximum acceptable query latency for users               | `'1s'`      |

```sql
WITH (delay = '5s', user_latency = '1s')
```

---

### Simple Materialized View (no JOIN)

```sql
CREATE MATERIALIZED VIEW active_orders_mv
REFRESH EVERY 30 SECONDS
AS
SELECT id, amount, status, created_at
FROM orders
WHERE status = 'active';
```

---

### Materialized View with JOIN

```sql
CREATE OR REPLACE MATERIALIZED VIEW orders_with_customers_mv
REFRESH EVERY 8 SECONDS
WITH (delay = '2s', user_latency = '1s')
AS
SELECT
  o.id,
  o.amount,
  c.name AS customer_name,
  c.email,
  c.department.zip_code AS customer_zip,
  UPPER(c.name) AS customer_name_upper,
  COALESCE(
    NULLIF(o.createdAt, DATE_PARSE('2025-09-11', '%Y-%m-%d') - INTERVAL 2 DAY),
    CURRENT_DATE
  ) AS effective_date
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id
WHERE o.status = 'completed';
```

This creates:
- Changelog transforms to track changes in `orders` and `customers`
- An enrich policy on `customers` matching on `id`
- An ingest pipeline with enrich processor + script processors for computed columns
- An enrichment transform to apply the pipeline
- A compute fields transform (if scripted columns exist)
- The final materialized view index `orders_with_customers_mv`

---

### Materialized View with Aggregations

```sql
CREATE OR REPLACE MATERIALIZED VIEW orders_by_city_mv
AS
SELECT
  c.city,
  c.country,
  COUNT(*) AS order_count,
  SUM(o.amount) AS total_amount,
  AVG(o.amount) AS avg_amount,
  MAX(o.amount) AS max_amount
FROM orders o
JOIN customers c ON o.customer_id = c.id
WHERE o.status = 'completed'
GROUP BY c.city, c.country
HAVING SUM(o.amount) > 10000
ORDER BY total_amount DESC
LIMIT 100;
```

When a `GROUP BY` clause is present, the engine generates an additional **pivot transform** for the aggregation step.

---

## DROP MATERIALIZED VIEW

### Syntax

```sql
DROP MATERIALIZED VIEW [IF EXISTS] view_name;
```

Drops the materialized view and all associated artifacts:
- Transforms (changelog, enrichment, compute, aggregate)
- Intermediate indices
- Ingest pipelines
- Enrich policies

**Example:**

```sql
DROP MATERIALIZED VIEW IF EXISTS orders_with_customers_mv;
```

---

## REFRESH MATERIALIZED VIEW

### Syntax

```sql
REFRESH MATERIALIZED VIEW [IF EXISTS] view_name [WITH SCHEDULE NOW];
```

Forces an immediate refresh of the materialized view by:
1. Refreshing changelog indices
2. Re-executing enrich policies

| Option               | Description                                        |
|----------------------|----------------------------------------------------|
| `IF EXISTS`          | Skip if view does not exist                        |
| `WITH SCHEDULE NOW`  | Schedule transforms for immediate execution        |

**Example:**

```sql
REFRESH MATERIALIZED VIEW orders_with_customers_mv WITH SCHEDULE NOW;
```

---

## DESCRIBE MATERIALIZED VIEW

### Syntax

```sql
DESCRIBE MATERIALIZED VIEW view_name;
```

Returns the schema of the materialized view index, showing columns, types, nullability, and other metadata.

**Example:**

```sql
DESCRIBE MATERIALIZED VIEW orders_with_customers_mv;
```

| Field                | Type      | Null | Key | Default | Comment | Script | Extra |
|----------------------|-----------|------|-----|---------|---------|--------|-------|
| id                   | INT       | yes  |     | NULL    |         |        | ()    |
| amount               | DOUBLE    | yes  |     | NULL    |         |        | ()    |
| customer_name        | VARCHAR   | yes  |     | NULL    |         |        | ()    |
| email                | VARCHAR   | yes  |     | NULL    |         |        | ()    |
| customer_zip         | VARCHAR   | yes  |     | NULL    |         |        | ()    |
| customer_name_upper  | KEYWORD   | yes  |     | NULL    |         |        | ()    |
| effective_date       | TIMESTAMP | yes  |     | NULL    |         |        | ()    |

---

## SHOW MATERIALIZED VIEW

### Syntax

```sql
SHOW MATERIALIZED VIEW view_name;
```

Returns metadata about the materialized view including its definition, refresh configuration, transform details, and step information.

**Example:**

```sql
SHOW MATERIALIZED VIEW orders_with_customers_mv;
```

---

## SHOW CREATE MATERIALIZED VIEW

### Syntax

```sql
SHOW CREATE MATERIALIZED VIEW view_name;
```

Returns the normalized SQL statement that would recreate the materialized view.

**Example:**

```sql
SHOW CREATE MATERIALIZED VIEW orders_with_customers_mv;
```

Returns:

```sql
CREATE OR REPLACE MATERIALIZED VIEW orders_with_customers_mv
REFRESH EVERY 8 SECONDS
WITH (delay = '2s', user_latency = '1s')
AS
SELECT o.id, o.amount, c.name AS customer_name, c.email, ...
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id
WHERE o.status = 'completed'
```

---

## SHOW MATERIALIZED VIEW STATUS

### Syntax

```sql
SHOW MATERIALIZED VIEW STATUS view_name;
```

Returns the runtime status of all transforms backing the materialized view, including:
- Transform state (`started`, `stopped`, `failed`)
- Documents processed and indexed
- Index failures and search failures
- Last checkpoint number
- Operations behind
- Processing time

**Example:**

```sql
SHOW MATERIALIZED VIEW STATUS orders_with_customers_mv;
```

| stepNumber | sourceTable | targetTable                              | state   | documentsProcessed | documentsIndexed | lastCheckpoint |
|------------|-------------|------------------------------------------|---------|--------------------|------------------|----------------|
| 1          | orders      | orders_with_customers_mv_orders_changelog | started | 1500               | 1500             | 3              |
| 2          | ...         | ...                                      | started | 1500               | 1500             | 3              |

---

## SHOW MATERIALIZED VIEWS

### Syntax

```sql
SHOW MATERIALIZED VIEWS;
```

Returns a list of all materialized views registered in the system.

---

## Complete Example

This example demonstrates a full materialized view workflow: creating source tables, loading data, creating the view, and querying it.

### 1. Create source tables

```sql
CREATE TABLE IF NOT EXISTS orders (
  id INT NOT NULL,
  customer_id INT NOT NULL,
  amount DOUBLE,
  status KEYWORD DEFAULT 'pending',
  items ARRAY<STRUCT> FIELDS (
    product_id INT,
    quantity INT,
    price DOUBLE
  ),
  createdAt TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS customers (
  id INT NOT NULL,
  name VARCHAR,
  email KEYWORD,
  department STRUCT FIELDS (
    name VARCHAR,
    zip_code KEYWORD
  ),
  PRIMARY KEY (id)
);
```

### 2. Load data

```sql
COPY INTO orders FROM '/data/orders.json' WITH (format = 'json');
COPY INTO customers FROM '/data/customers.json' WITH (format = 'json');
```

### 3. Create the materialized view

```sql
CREATE OR REPLACE MATERIALIZED VIEW orders_with_customers_mv
REFRESH EVERY 8 SECONDS
WITH (delay = '2s', user_latency = '1s')
AS
SELECT
  o.id,
  o.amount,
  c.name AS customer_name,
  c.email,
  c.department.zip_code AS customer_zip,
  UPPER(c.name) AS customer_name_upper,
  COALESCE(
    NULLIF(o.createdAt, DATE_PARSE('2025-09-11', '%Y-%m-%d') - INTERVAL 2 DAY),
    CURRENT_DATE
  ) AS effective_date
FROM orders AS o
JOIN customers AS c ON o.customer_id = c.id
WHERE o.status = 'completed';
```

### 4. Inspect the view

```sql
-- View the schema
DESCRIBE MATERIALIZED VIEW orders_with_customers_mv;

-- View the normalized SQL
SHOW CREATE MATERIALIZED VIEW orders_with_customers_mv;

-- Check transform status
SHOW MATERIALIZED VIEW STATUS orders_with_customers_mv;

-- View associated enrich policies
SHOW ENRICH POLICIES;
SHOW ENRICH POLICY orders_with_customers_mv_customers_enrich_policy;

-- View associated watchers
SHOW WATCHERS;
SHOW WATCHER STATUS orders_with_customers_mv_watcher;
```

### 5. Query the materialized view

Once the transforms have completed their first checkpoint, the materialized view can be queried like a regular table:

```sql
SELECT * FROM orders_with_customers_mv
WHERE customer_name = 'Alice'
ORDER BY amount DESC
LIMIT 10;
```

### 6. Force a refresh

```sql
REFRESH MATERIALIZED VIEW orders_with_customers_mv WITH SCHEDULE NOW;
```

### 7. Drop the view

```sql
DROP MATERIALIZED VIEW IF EXISTS orders_with_customers_mv;
```

---

## Version Compatibility

| Feature                  | ES6 | ES7  | ES8 | ES9 |
|--------------------------|-----|------|-----|-----|
| Materialized Views       | No  | Yes* | Yes | Yes |
| `WITH SCHEDULE NOW`      | No  | No   | Yes | Yes |

\* Requires Elasticsearch 7.5+ (transforms and enrich policies)

---

## Limitations

| Limitation                                  | Details                                                              |
|---------------------------------------------|----------------------------------------------------------------------|
| **UNNEST JOIN**                             | Not supported in materialized views                                  |
| **Quota limits**                            | Community edition: max 3 views. Pro: limited. Enterprise: unlimited  |
| **Watcher dependency (ES license)**         | Automatic enrich policy re-execution relies on Elasticsearch Watchers, which require an Elasticsearch Platinum or Enterprise license (see below) |
| **Eventual consistency**                    | Data is eventually consistent based on refresh frequency and delay   |
| **Join cardinality**                        | JOINs use enrich policies which match on a single field              |

### Watcher Dependency and Elasticsearch Licensing

Materialized views with JOINs rely on **enrich policies** to denormalize data from lookup tables into the view. When data in a lookup table (e.g. `customers`) changes, the corresponding enrich policy must be **re-executed** so that new documents flowing through the ingest pipeline pick up the updated values.

To automate this re-execution, the engine creates an **Elasticsearch Watcher** that periodically triggers `EXECUTE ENRICH POLICY` calls. However, **Watchers require an Elasticsearch Platinum or Enterprise license** (or an active Trial license). This is an Elasticsearch-side requirement, independent of the JDBC driver license.

**Impact:**
- **With Elasticsearch Platinum/Enterprise/Trial license**: Fully automatic — the watcher handles enrich policy re-execution transparently
- **Without Elasticsearch Platinum license**: The watcher cannot be created. Changes to lookup tables will **not** be reflected in the materialized view until the enrich policies are manually re-executed

**Workaround for clusters without Watcher support:**

Use an external scheduled job (cron, Kubernetes CronJob, Airflow, etc.) to periodically re-execute the enrich policies via SQL:

```sql
-- Re-execute enrich policies manually
EXECUTE ENRICH POLICY orders_with_customers_mv_customers_enrich_policy;

-- Or trigger a full refresh
REFRESH MATERIALIZED VIEW orders_with_customers_mv;
```

Note that **transforms** (which power the continuous data sync) and **enrich policies** themselves are available in the free/basic Elasticsearch license starting from ES 7.5+. Only the **Watcher** component requires a paid Elasticsearch license.

---

## Quick Reference

### Syntax Summary

```sql
-- Create
CREATE [OR REPLACE] MATERIALIZED VIEW [IF NOT EXISTS] name
  [REFRESH EVERY n {MILLISECONDS|SECONDS|MINUTES|HOURS|DAYS|WEEKS|MONTHS|YEARS}]
  [WITH (delay = 'interval', user_latency = 'interval')]
  AS SELECT ...

-- Drop
DROP MATERIALIZED VIEW [IF EXISTS] name;

-- Refresh
REFRESH MATERIALIZED VIEW [IF EXISTS] name [WITH SCHEDULE NOW];

-- Inspect
DESCRIBE MATERIALIZED VIEW name;
SHOW MATERIALIZED VIEW name;
SHOW CREATE MATERIALIZED VIEW name;
SHOW MATERIALIZED VIEW STATUS name;
SHOW MATERIALIZED VIEWS;
```

---

[Back to index](README.md)
