# 📘 **DDL Statements — SQL Gateway for Elasticsearch**

---

## Introduction

The SQL Gateway provides a full Data Definition Language (DDL) layer on top of Elasticsearch.  
It allows defining tables, schemas, pipelines, mappings, settings, watchers, enrich policies using a relational syntax while generating the appropriate Elasticsearch structures:

- **indices** (for non-partitioned tables)
- **index templates** (for partitioned tables)
- **ingest pipelines** (default or user-defined)
- **mappings** and **settings**
- **metadata** (primary key, generated columns, comments, options)
- **watchers** (scheduled monitoring and alerting)
- **enrich policies** (data enrichment from lookup indices)

The DDL engine is:

- **version-aware** (ES6 → ES9)
- **client-agnostic** (Jest, RHLC, Java Client)
- **schema-driven**
- **round-trip safe** (SHOW CREATE returns normalized SQL)

---

## Table of Contents

- [Table Model](#table-model)
- [Column Types & Mapping](#column-types--mapping)
- [Constraints & Column Options](#constraints--column-options)
- [Partitioning](#partitioning)
- [CREATE TABLE](#create-table)
- [CREATE TABLE AS SELECT](#create-table-as-select)
- [ALTER TABLE](#alter-table)
- [DROP TABLE](#drop-table)
- [TRUNCATE TABLE](#truncate-table)
- [CREATE PIPELINE](#create-pipeline)
- [ALTER PIPELINE](#alter-pipeline)
- [DROP PIPELINE](#drop-pipeline)
- [WATCHERS](#watchers)
- [CREATE WATCHER](#create-watcher)
- [DROP WATCHER](#drop-watcher)
- [ENRICH POLICIES](#enrich-policies)
- [CREATE ENRICH POLICY](#create-enrich-policy)
- [EXECUTE ENRICH POLICY](#execute-enrich-policy)
- [DROP ENRICH POLICY](#drop-enrich-policy)
- [USING ENRICH POLICIES IN PIPELINES](#using-enrich-policies-in-pipelines)
- [Index Migration Workflow](#index-migration-workflow)
- [VERSION COMPATIBILITY](#version-compatibility)
- [QUICK REFERENCE](#quick-reference)

---

## Table Model

A SQL table corresponds to:

| SQL Definition                          | Elasticsearch Structure                            |
|-----------------------------------------|----------------------------------------------------|
| `CREATE TABLE` without `PARTITIONED BY` | **Concrete index**                                 |
| `CREATE TABLE` with `PARTITIONED BY`    | **Index template** (legacy ES6 or composable ES7+) |

### Index-backed table (no partitioning)

```sql
CREATE TABLE users (
  id INT,
  name VARCHAR,
  PRIMARY KEY (id)
);
```

Creates:

- index `users`
- default pipeline `users_ddl_default_pipeline`
- mapping + settings

### Template-backed table (with partitioning)

```sql
CREATE TABLE users (
  id INT,
  birthdate DATE,
  PRIMARY KEY (id)
)
PARTITIONED BY (birthdate MONTH);
```

Creates:

- template `users`
- default pipeline with `date_index_name`
- indices generated dynamically:
	- `users-2025-01`
	- `users-2025-02`

---

## Column Types & Mapping

The SQL Gateway supports the following type system:

| SQL Type            | Elasticsearch Mapping                |
|---------------------|--------------------------------------|
| `NULL`              | `null`                               |
| `TINYINT`           | `byte`                               |
| `SMALLINT`          | `short`                              |
| `INT`               | `integer`                            |
| `BIGINT`            | `long`                               |
| `DOUBLE`            | `double`                             |
| `REAL`              | `float`                              |
| `BOOLEAN`           | `boolean`                            |
| `VARCHAR` \| `TEXT` | `text` + optional `keyword` subfield |
| `KEYWORD`           | `keyword`                            |
| `DATE`              | `date`                               |
| `TIMESTAMP`         | `date`                               |
| `STRUCT`            | `object` with nested properties      |
| `ARRAY<STRUCT>`     | `nested`                             |
| `GEO_POINT`         | `geo_point`                          |

---

### 🧩 Nested and Structured Data

#### FIELDS for Multi-fields

`FIELDS (...)` can be used to define **multi-fields** for text columns.  
This allows indexing the same column in multiple ways (e.g., with different analyzers).

**Example:**
```sql
CREATE TABLE docs (
  content VARCHAR FIELDS (
    keyword VARCHAR OPTIONS (analyzer = 'keyword'),
    english VARCHAR OPTIONS (analyzer = 'english')
  )
)
```

- `content` is indexed as text.
- `content.keyword` is a keyword sub-field.
- `content.english` is a text sub-field with the English analyzer.

---

#### FIELDS for STRUCT or NESTED OBJECTS

`FIELDS (...)` also enables the definition of **STRUCT** types, representing hierarchical data.

**Example:**

```sql
CREATE TABLE users (
  id INT NOT NULL,
  profile STRUCT FIELDS (
    first_name VARCHAR NOT NULL,
    last_name VARCHAR NOT NULL,
    address STRUCT FIELDS (
      street VARCHAR,
      city VARCHAR,
      zip VARCHAR
    ),
    join_date DATE,
    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY))
  )
)
```

- `profile` is a `STRUCT` column containing multiple fields.
- `address` is a nested `STRUCT` inside `profile`.

---

#### FIELDS for ARRAY<STRUCT>

**Example:**
```sql
CREATE TABLE store (
  id INT NOT NULL,
  products ARRAY<STRUCT> FIELDS (
    name VARCHAR NOT NULL,
    description VARCHAR NOT NULL,
    price BIGINT NOT NULL
  )
)
```

- `products` is an `ARRAY<STRUCT>` column.
- Maps naturally to Elasticsearch `nested`.

---

#### Notes

- On `VARCHAR` → defines **multi-fields**
- On `STRUCT` → defines **object fields**
- On `ARRAY<STRUCT>` → defines **nested fields**
- Sub-fields support:
	- nested `FIELDS`
	- `DEFAULT`
	- `NOT NULL`
	- `COMMENT`
	- `OPTIONS`
	- `SCRIPT AS` (except inside ARRAY\<STRUCT>)
- Multi-level nesting is supported.

---

## Constraints & Column Options

### Primary Key

```sql
id INT,
PRIMARY KEY (id)
```

Used for:

- document ID generation
- upsert semantics
- COPY INTO conflict resolution

---

### 🔑 Composite Primary Keys

SoftClient4ES supports composite primary keys in SQL.

#### Syntax

```sql
CREATE TABLE users (
  id INT NOT NULL,
  birthdate DATE NOT NULL,
  name VARCHAR,
  PRIMARY KEY (id, birthdate)
);
```

#### Elasticsearch Translation

```json
{
  "processors": [
    {
      "set": {
        "field": "_id",
        "value": "{{id}}||{{birthdate}}"
      }
    }
  ]
}
```

#### Notes

- Composite PK fields must be immutable.
- Avoid long `_id` values.
- Mapping rules:
	- `PRIMARY KEY (id)` → `_id = id`
	- `PRIMARY KEY (id, birthdate)` → `_id = "{{id}}-{{birthdate}}"`

---

## Partitioning

Partitioning routes documents to time-based indices using `date_index_name`.

### Supported Granularities

| SQL Granularity | ES `date_rounding` | Example Index Name        |
|-----------------|--------------------|---------------------------|
| YEAR            | "y"                | users-2025                |
| MONTH           | "M"                | users-2025-12             |
| DAY (default)   | "d"                | users-2025-12-10          |
| HOUR            | "h"                | users-2025-12-10-09       |
| MINUTE          | "m"                | users-2025-12-10-09-46    |
| SECOND          | "s"                | users-2025-12-10-09-46-30 |

---

## CREATE TABLE

### Basic Example

```sql
CREATE TABLE users (
  id INT,
  name VARCHAR DEFAULT 'anonymous',
  birthdate DATE,
  age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)),
  PRIMARY KEY (id)
);
```

### Partitioned Example

```sql
CREATE TABLE users (
  id INT,
  birthdate DATE,
  PRIMARY KEY (id)
)
PARTITIONED BY (birthdate MONTH);
```

---

## CREATE TABLE AS SELECT

```sql
CREATE TABLE new_users AS
SELECT id, name FROM users;
```

The gateway:

- infers the schema
- generates mappings
- creates index or template
- **populates data using the Bulk API**

---

## ALTER TABLE

**Supported statements:**

- `ADD COLUMN [IF NOT EXISTS] column_definition`
- `DROP COLUMN [IF EXISTS] column_name`
- `RENAME COLUMN old_name TO new_name`
- `ALTER COLUMN column_name SET SCRIPT AS (sql)`
- `ALTER COLUMN column_name DROP SCRIPT`
- `ALTER COLUMN column_name SET|ADD OPTION (key = value)`
- `ALTER COLUMN column_name DROP OPTION key`
- `ALTER COLUMN column_name SET COMMENT 'comment'`
- `ALTER COLUMN column_name DROP COMMENT`
- `ALTER COLUMN column_name SET DEFAULT value`
- `ALTER COLUMN column_name DROP DEFAULT`
- `ALTER COLUMN column_name SET NOT NULL`
- `ALTER COLUMN column_name DROP NOT NULL`
- `ALTER COLUMN column_name SET DATA TYPE new_type`
- `ALTER COLUMN column_name SET|ADD FIELD field_definition`
- `ALTER COLUMN column_name DROP FIELD field_name`
- `ALTER COLUMN column_name SET FIELDS (...)`
- `SET|ADD MAPPING (key = value)`
- `DROP MAPPING key`
- `SET|ADD SETTING (key = value)`
- `DROP SETTING key`

### Type Changes and Safety

When applying `ALTER COLUMN column_name SET DATA TYPE new_type`, the SQL Gateway computes a structural diff between the current schema and the target schema.

Type changes fall into two categories:

- **Convertible types** (`SQLTypeUtils.canConvert(from, to) = true`)  
	The change is allowed but requires a full **reindex** of the underlying data.  
	The Gateway automatically performs the reindex operation and swaps aliases when complete.  
	These changes are classified as `UnsafeReindex`.

- **Incompatible types** (`SQLTypeUtils.canConvert(from, to) = false`)  
	The change is **not allowed** and the `ALTER TABLE` statement fails.  
	These changes are classified as `Impossible`.

This is the only case where an `ALTER TABLE` operation can be rejected for safety reasons.  
All other ALTER operations (adding/dropping columns, renaming, modifying options, modifying nested fields, etc.) are allowed.

---

## DROP TABLE

```sql
DROP TABLE IF EXISTS users;
```

Deletes:

- index (non-partitioned)
- template (partitioned)

---

## TRUNCATE TABLE

```sql
TRUNCATE TABLE users;
```

Deletes all documents while keeping:

- mapping
- settings
- pipeline
- template (if any)

---

## CREATE PIPELINE

```sql
CREATE OR REPLACE PIPELINE user_pipeline
WITH PROCESSORS (
    SET (
        field = "name",
        if = "ctx.name == null",
        description = "DEFAULT 'anonymous'",
        ignore_failure = true,
        value = "anonymous"
    ),
    SCRIPT (
        description = "age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))",
        lang = "painless",
        source = "...",
        ignore_failure = true
    ),
    DATE_INDEX_NAME (
        field = "birthdate",
        index_name_prefix = "users-",
        date_formats = ["yyyy-MM"],
        date_rounding = "M",
        separator = "-",
        ignore_failure = true
    )
);
```

## DROP PIPELINE

```sql
DROP PIPELINE IF EXISTS user_pipeline;
```

## ALTER PIPELINE

```sql
ALTER PIPELINE IF EXISTS user_pipeline (
    ADD PROCESSOR SET (
        field = "status",
        if = "ctx.status == null",
        description = "status DEFAULT 'active'",
        ignore_failure = true,
        value = "active"
    ),
    DROP PROCESSOR SET (_id)
);
```

---

## Watchers

Watchers provide scheduled monitoring and alerting on Elasticsearch data.  
A watcher consists of:

- **Trigger** — scheduling (cron or interval)
- **Input** — data source (search, simple, HTTP, chain, or none)
- **Condition** — evaluation logic (always, never, compare, script)
- **Actions** — what to do when condition is met (logging, webhook)

---

### ⚠️ Limitations

The following Elasticsearch Watcher features are **not supported** in this DSL:

| Feature                      | Status          |
|------------------------------|-----------------|
| **Email action**             | ❌ Not supported |
| **Slack action**             | ❌ Not supported |
| **PagerDuty action**         | ❌ Not supported |
| **Jira action**              | ❌ Not supported |
| **Index action**             | ❌ Not supported |
| **Transform** (root level)   | ❌ Not supported |
| **Transform** (action level) | ❌ Not supported |

Only **logging** and **webhook** actions are currently implemented.

---

## CREATE WATCHER

### Syntax

```sql
CREATE [OR REPLACE] WATCHER watcher_name AS
  trigger_clause
  input_clause
  condition_clause
  DO
  action_name [AS] action_definition [, ...]
  END
```

> **Note:** The `AS` keyword before action definitions is **optional**.

---

### Triggers

Triggers define when the watcher executes.

| Trigger Type  | Syntax                     | Example                       |
|---------------|----------------------------|-------------------------------|
| Interval      | `EVERY n unit`             | `EVERY 5 MINUTES`             |
| Cron          | `AT SCHEDULE 'expression'` | `AT SCHEDULE '0 */5 * * * ?'` |

**Supported time units:**

| Unit         | Singular      | Plural         |
|--------------|---------------|----------------|
| Milliseconds | `MILLISECOND` | `MILLISECONDS` |
| Seconds      | `SECOND`      | `SECONDS`      |
| Minutes      | `MINUTE`      | `MINUTES`      |
| Hours        | `HOUR`        | `HOURS`        |
| Days         | `DAY`         | `DAYS`         |
| Weeks        | `WEEK`        | `WEEKS`        |
| Months       | `MONTH`       | `MONTHS`       |
| Years        | `YEAR`        | `YEARS`        |

**Examples:**

```sql
-- Execute every 30 seconds
EVERY 30 SECONDS

-- Execute every 5 minutes
EVERY 5 MINUTES

-- Execute daily
EVERY 1 DAY

-- Execute with cron expression (every day at 9 AM)
AT SCHEDULE '0 0 9 * * ?'
```

---

### Inputs

Inputs define the data source for the watcher.

#### No Input

```sql
WITH NO INPUT
```

No data is loaded. The watcher executes with an empty payload.

---

#### Simple Input

```sql
WITH INPUT (key1 = value1, key2 = value2, ...)
```

Provides a static payload.

**Example:**

```sql
WITH INPUT (keys = ["value1", "value2"], threshold = 10)
```

**Generates:**

```json
{
  "input": {
    "simple": {
      "keys": ["value1", "value2"],
      "threshold": 10
    }
  }
}
```

---

#### Search Input

```sql
FROM index_name [, index_name2, ...] [WHERE criteria] [WITHIN n unit]
```

Queries Elasticsearch indices.

**Example:**

```sql
FROM my_index WITHIN 2 MINUTES
```

```sql
FROM logs-*, metrics-* WHERE level = 'ERROR' WITHIN 5 MINUTES
```

**Generates:**

```json
{
  "input": {
    "search": {
      "request": {
        "indices": ["my_index"],
        "body": {
          "query": {"match_all": {}}
        }
      },
      "timeout": "2m"
    }
  }
}
```

---

#### HTTP Input

HTTP requests can be defined in **two ways**:

##### Option 1: Full URL String

```sql
WITH INPUT method "full_url" [HEADERS (...)] [BODY "body"] [TIMEOUT (...)]
```

**Example:**

```sql
WITH INPUT GET "https://api.example.com:443/data?param=value" 
  HEADERS ("Authorization" = "Bearer token") 
  TIMEOUT (connection = "5s", read = "30s")
```

##### Option 2: Decomposed URL Components

```sql
WITH INPUT method PROTOCOL protocol HOST "hostname" [PORT port] [PATH "path"] [PARAMS (...)] [HEADERS (...)] [BODY "body"] [TIMEOUT (...)]
```

**Example:**

```sql
WITH INPUT GET PROTOCOL https HOST "api.example.com" PORT 443 PATH "/data" 
  PARAMS (param = "value")
  HEADERS ("Authorization" = "Bearer token") 
  TIMEOUT (connection = "5s", read = "30s")
```

**Components:**

| Component         | Required  | Description                                         |
|-------------------|-----------|-----------------------------------------------------|
| `method`          | ✔         | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `HEAD` |
| URL or `PROTOCOL` | ✔         | Either full URL string or protocol (`http`/`https`) |
| `HOST`            | ✔*        | Hostname (required if using decomposed format)      |
| `PORT`            | ✖         | Port number (default: 80 for http, 443 for https)   |
| `PATH`            | ✖         | URL path                                            |
| `PARAMS`          | ✖         | Query parameters as key-value pairs                 |
| `HEADERS`         | ✖         | HTTP headers as key-value pairs                     |
| `BODY`            | ✖         | Request body (quoted string)                        |
| `TIMEOUT`         | ✖         | Connection and read timeouts                        |

> ⚠️ **Note:** Only `HEAD`, `GET`, `POST`, `PUT`, and `DELETE` methods are supported by Elasticsearch.

**Generates:**

```json
{
  "input": {
    "http": {
      "request": {
        "scheme": "https",
        "host": "api.example.com",
        "port": 443,
        "method": "get",
        "path": "/data",
        "params": {"param": "value"},
        "headers": {"Authorization": "Bearer token"},
        "connection_timeout": "5s",
        "read_timeout": "30s"
      }
    }
  }
}
```

---

#### Chain Input

```sql
WITH INPUTS name1 [AS] input1, name2 [AS] input2, ...
```

Combines multiple inputs. Each input is named and can reference previous inputs in the chain.

> **Note:** The `AS` keyword is **optional**.

**Example:**

```sql
WITH INPUTS 
  search_data FROM my_index WITHIN 2 MINUTES, 
  http_data GET "https://api.example.com/enrich" 
    HEADERS ("Authorization" = "Bearer token") 
    TIMEOUT (connection = "5s")
```

**Generates:**

```json
{
  "input": {
    "chain": {
      "inputs": [
        {
          "search_data": {
            "search": {
              "request": {
                "indices": ["my_index"],
                "body": {"query": {"match_all": {}}}
              },
              "timeout": "2m"
            }
          }
        },
        {
          "http_data": {
            "http": {
              "request": {
                "scheme": "https",
                "host": "api.example.com",
                "port": 443,
                "method": "get",
                "path": "/enrich",
                "headers": {"Authorization": "Bearer token"},
                "connection_timeout": "5s"
              }
            }
          }
        }
      ]
    }
  }
}
```

---

### Conditions

Conditions determine whether actions should execute.

| Condition Type | Syntax | Description |
|----------------|--------|-------------|
| `ALWAYS` | `ALWAYS DO` | Always triggers actions |
| `NEVER` | `NEVER DO` | Never triggers (useful for testing) |
| Compare | `WHEN path operator value DO` | Compare field to value |
| Compare (date math) | `WHEN path operator date_function DO` | Compare field to date math expression |
| Script | `WHEN SCRIPT '...' ... RETURNS TRUE DO` | Custom Painless condition |

---

#### Always / Never Conditions

```sql
ALWAYS DO
-- actions
END
```

```sql
NEVER DO
-- actions
END
```

---

#### Compare Condition

```sql
WHEN ctx.payload.hits.total > 100 DO
-- actions
END
```

**Supported operators:** `>`, `>=`, `<`, `<=`, `=`, `!=`

**With date math:**

```sql
WHEN ctx.execution_time > DATE_SUB(CURRENT_DATE, INTERVAL 5 DAY) DO
-- actions
END
```

**Generates:**

```json
{
  "condition": {
    "compare": {
      "ctx.execution_time": {
        "gt": "now-5d/d"
      }
    }
  }
}
```

---

#### Script Condition

```sql
WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' 
  USING LANG 'painless' 
  WITH PARAMS (threshold = 10) 
  RETURNS TRUE 
DO
-- actions
END
```

**Generates:**

```json
{
  "condition": {
    "script": {
      "source": "ctx.payload.hits.total > params.threshold",
      "lang": "painless",
      "params": {
        "threshold": 10
      }
    }
  }
}
```

---

### Actions

Actions define what happens when the condition is met.

> **Note:** The `AS` keyword before action definitions is **optional**.

---

#### Logging Action

```sql
action_name [AS] LOG "message" [AT level] [FOREACH "path"] [LIMIT n]
```

| Component | Required | Description |
|-----------|----------|-------------|
| Message | ✔ | Log message (supports Mustache templates) |
| `AT level` | ✖ | Log level: `DEBUG`, `INFO`, `WARN`, `ERROR` (default: `INFO`) |
| `FOREACH` | ✖ | Path to iterate over |
| `LIMIT` | ✖ | Maximum iterations (default: 100) |

**Example:**

```sql
log_action LOG "Alert: {{ctx.payload.hits.total}} errors detected" AT ERROR FOREACH "ctx.payload.hits.hits" LIMIT 500
```

**Generates:**

```json
{
  "actions": {
    "log_action": {
      "foreach": "ctx.payload.hits.hits",
      "max_iterations": 500,
      "logging": {
        "text": "Alert: {{ctx.payload.hits.total}} errors detected",
        "level": "error"
      }
    }
  }
}
```

---

#### Webhook Action

Webhook requests can be defined in **two ways**, similar to HTTP inputs:

##### Option 1: Full URL String

```sql
action_name [AS] WEBHOOK method "full_url" [HEADERS (...)] [BODY "body"] [TIMEOUT (...)] [FOREACH "path"] [LIMIT n]
```

**Example:**

```sql
webhook_action WEBHOOK POST "https://api.example.com:443/alert?source={{ctx.watch_id}}" 
  HEADERS ("Content-Type" = "application/json") 
  BODY "{\"message\": \"Alert triggered\"}" 
  TIMEOUT (connection = "10s", read = "30s")
```

##### Option 2: Decomposed URL Components

```sql
action_name [AS] WEBHOOK method PROTOCOL protocol HOST "hostname" [PORT port] [PATH "path"] [PARAMS (...)] [HEADERS (...)] [BODY "body"] [TIMEOUT (...)] [FOREACH "path"] [LIMIT n]
```

**Example:**

```sql
webhook_action WEBHOOK POST PROTOCOL https HOST "api.example.com" PORT 443 PATH "/alert"
  PARAMS (source = "{{ctx.watch_id}}")
  HEADERS ("Content-Type" = "application/json") 
  BODY "{\"message\": \"Alert triggered\"}" 
  TIMEOUT (connection = "10s", read = "30s")
```

| Component         | Required  | Description                                         |
|-------------------|-----------|-----------------------------------------------------|
| Method            | ✔         | HTTP method: `GET`, `POST`, `PUT`, `DELETE`, `HEAD` |
| URL or `PROTOCOL` | ✔         | Either full URL string or protocol                  |
| `HOST`            | ✔*        | Hostname (required if using decomposed format)      |
| `PORT`            | ✖         | Port number                                         |
| `PATH`            | ✖         | URL path                                            |
| `PARAMS`          | ✖         | Query parameters                                    |
| `HEADERS`         | ✖         | HTTP headers as key-value pairs                     |
| `BODY`            | ✖         | Request body (quoted string, supports Mustache)     |
| `TIMEOUT`         | ✖         | Connection and read timeouts                        |
| `FOREACH`         | ✖         | Path to iterate over                                |
| `LIMIT`           | ✖         | Maximum iterations (default: 100)                   |

> ⚠️ **Note:** Only `HEAD`, `GET`, `POST`, `PUT`, and `DELETE` methods are supported by Elasticsearch.

**Generates:**

```json
{
  "actions": {
    "webhook_action": {
      "webhook": {
        "scheme": "https",
        "host": "api.example.com",
        "port": 443,
        "method": "post",
        "path": "/alert",
        "params": {"source": "{{ctx.watch_id}}"},
        "headers": {"Content-Type": "application/json"},
        "body": "{\"message\": \"Alert triggered\"}",
        "connection_timeout": "10s",
        "read_timeout": "30s"
      }
    }
  }
}
```

---

### Complete Examples

#### Monitor with search input and logging

```sql
CREATE OR REPLACE WATCHER high_error_rate AS
  EVERY 5 MINUTES
  FROM logs-* WHERE level = 'ERROR' WITHIN 5 MINUTES
  WHEN ctx.payload.hits.total > 100 DO
  notify LOG "High error rate: {{ctx.payload.hits.total}} errors in the last 5 minutes" AT ERROR
END
```

---

#### Webhook with simple input and script condition

```sql
CREATE OR REPLACE WATCHER threshold_alert AS
  EVERY 1 HOUR
  WITH INPUT (keys = ["server1", "server2", "server3"])
  WHEN SCRIPT 'ctx.payload.keys.size() > params.threshold' 
    USING LANG 'painless' 
    WITH PARAMS (threshold = 2) 
    RETURNS TRUE 
  DO
  alert WEBHOOK POST "https://hooks.example.com/webhook" 
    HEADERS ("Content-Type" = "application/json") 
    BODY "{\"text\": \"Threshold exceeded for {{ctx.payload._value}}\"}" 
    FOREACH "ctx.payload.keys" 
    LIMIT 10
END
```

---

#### Daily report with cron trigger

```sql
CREATE OR REPLACE WATCHER daily_report AS
  AT SCHEDULE '0 0 9 * * ?'
  FROM orders WHERE status = 'completed' WITHIN 1 DAY
  ALWAYS DO
  send_report WEBHOOK POST "https://api.company.com/reports" 
    HEADERS ("Authorization" = "Bearer {{ctx.metadata.api_key}}") 
    BODY "{\"date\": \"{{ctx.execution_time}}\", \"total_orders\": {{ctx.payload.hits.total}}}" 
    TIMEOUT (connection = "10s", read = "60s")
END
```

---

#### Chain input with multiple actions

```sql
CREATE OR REPLACE WATCHER enriched_alert AS
  AT SCHEDULE '0 */15 * * * ?'
  WITH INPUTS 
    alerts FROM alerts-* WHERE severity = 'critical' WITHIN 15 MINUTES,
    context GET PROTOCOL https HOST "api.internal.com" PATH "/context" 
      HEADERS ("X-API-Key" = "secret123")
  WHEN ctx.payload.alerts.hits.total > 0 DO
  log_alert LOG "Critical alerts: {{ctx.payload.alerts.hits.total}}" AT ERROR,
  notify_ops WEBHOOK POST "https://alerting.example.com/alert" 
    HEADERS ("Content-Type" = "application/json") 
    BODY "{\"alerts\": {{ctx.payload.alerts.hits.total}}, \"context\": \"{{ctx.payload.context.environment}}\"}"
END
```

---

## DROP WATCHER

```sql
DROP WATCHER [IF EXISTS] watcher_name;
```

Deletes the watcher and stops its execution.

**Example:**

```sql
DROP WATCHER IF EXISTS high_error_rate;
```

---

## Enrich Policies

Enrich policies allow you to add data from existing indices to incoming documents during ingest.  
This is useful for:

- Adding user profile information to events
- Enriching logs with geo-location data
- Adding product details to order documents

---

### ⚠️ Requirements

- Enrich policies require **Elasticsearch 7.5+**
- The source index must exist before creating the policy
- The policy must be **executed** before it can be used in a pipeline

---

## CREATE ENRICH POLICY

### Syntax

```sql
CREATE [OR REPLACE] ENRICH POLICY policy_name
  [TYPE { MATCH | GEO_MATCH | RANGE }]
  FROM source_index [, source_index2, ...]
  ON match_field
  ENRICH field1, field2, ...
  [WHERE criteria]
```

| Component     | Required  | Description                                 |
|---------------|-----------|---------------------------------------------|
| `policy_name` | ✔         | Unique name for the policy                  |
| `TYPE`        | ✖         | Policy type (default: `MATCH`)              |
| `FROM`        | ✔         | Source index(es) containing enrichment data |
| `ON`          | ✔         | Field used to match documents               |
| `ENRICH`      | ✔         | Fields to add from the source index         |
| `WHERE`       | ✖         | Filter criteria for source documents        |

---

### Policy Types

| Type | Description | Use Case |
|------|-------------|----------|
| `MATCH` | Exact value matching (default) | User IDs, product codes |
| `GEO_MATCH` | Geo-shape matching | Location-based enrichment |
| `RANGE` | Range-based matching | IP ranges, numeric ranges |

---

### Examples

#### Basic match policy (default type)

```sql
CREATE ENRICH POLICY user_enrichment
  FROM users
  ON user_id
  ENRICH name, email, department
```

**Generates:**

```json
{
  "match": {
    "indices": "users",
    "match_field": "user_id",
    "enrich_fields": ["name", "email", "department"]
  }
}
```

---

#### Match policy with WHERE clause

```sql
CREATE OR REPLACE ENRICH POLICY active_user_enrichment
  FROM users
  ON user_id
  ENRICH name, email, department
  WHERE account_status = 'active' AND email_verified = true
```

**Generates:**

```json
{
  "match": {
    "indices": "users",
    "match_field": "user_id",
    "enrich_fields": ["name", "email", "department"],
    "query": {
      "bool": {
        "must": [
          {"term": {"account_status": "active"}},
          {"term": {"email_verified": true}}
        ]
      }
    }
  }
}
```

---

#### Policy with multiple source indices

```sql
CREATE ENRICH POLICY contact_enrichment
  FROM users, customers, partners
  ON contact_id
  ENRICH name, email, company
```

---

#### Geo-match policy

```sql
CREATE ENRICH POLICY geo_enrichment
  TYPE GEO_MATCH
  FROM postal_codes
  ON location
  ENRICH city, region, country, timezone
```

**Generates:**

```json
{
  "geo_match": {
    "indices": "postal_codes",
    "match_field": "location",
    "enrich_fields": ["city", "region", "country", "timezone"]
  }
}
```

---

#### Range policy

```sql
CREATE ENRICH POLICY ip_enrichment
  TYPE RANGE
  FROM ip_ranges
  ON ip_range
  ENRICH network_name, datacenter, owner
```

**Generates:**

```json
{
  "range": {
    "indices": "ip_ranges",
    "match_field": "ip_range",
    "enrich_fields": ["network_name", "datacenter", "owner"]
  }
}
```

---

#### Complex WHERE clause

```sql
CREATE ENRICH POLICY premium_users
  FROM users
  ON user_id
  ENRICH name, email, tier, subscription_end_date
  WHERE status = 'active'
    AND tier IN ('premium', 'enterprise')
    AND created_at > '2023-01-01'
    AND (country = 'US' OR country = 'CA')
```

---

## EXECUTE ENRICH POLICY

After creating an enrich policy, you must **execute** it to create the enrich index.

```sql
EXECUTE ENRICH POLICY policy_name;
```

**Example:**

```sql
EXECUTE ENRICH POLICY user_enrichment;
```

> ⚠️ **Note:** The policy must be re-executed whenever the source index data changes.

---

## DROP ENRICH POLICY

```sql
DROP ENRICH POLICY [IF EXISTS] policy_name;
```

Deletes the enrich policy and its associated enrich index.

**Example:**

```sql
DROP ENRICH POLICY IF EXISTS user_enrichment;
```

---

## Using Enrich Policies in Pipelines

Once an enrich policy is created and executed, use it in an ingest pipeline with the `ENRICH` processor.

### Syntax

```sql
CREATE PIPELINE my_pipeline
WITH PROCESSORS (
    ENRICH (
        policy_name = "user_enrichment",
        field = "user_id",
        target_field = "user_info",
        max_matches = 1,
        ignore_missing = true
    )
)
```

| Option           | Required  | Description                                                   |
|------------------|-----------|---------------------------------------------------------------|
| `policy_name`    | ✔         | Name of the enrich policy                                     |
| `field`          | ✔         | Field in the incoming document to match                       |
| `target_field`   | ✔         | Field to store enrichment data                                |
| `max_matches`    | ✖         | Maximum matching documents (default: 1)                       |
| `ignore_missing` | ✖         | Ignore if match field is missing (default: false)             |
| `override`       | ✖         | Override existing target field (default: true)                |
| `shape_relation` | ✖         | For geo_match: `INTERSECTS`, `DISJOINT`, `WITHIN`, `CONTAINS` |

---

### Complete Example

#### 1. Create source index

```sql
CREATE TABLE users (
  user_id KEYWORD,
  name VARCHAR,
  email VARCHAR,
  department KEYWORD,
  PRIMARY KEY (user_id)
);

INSERT INTO users VALUES 
  ('U001', 'Alice Smith', 'alice@example.com', 'Engineering'),
  ('U002', 'Bob Jones', 'bob@example.com', 'Marketing');
```

#### 2. Create enrich policy

```sql
CREATE ENRICH POLICY user_enrichment
  FROM users
  ON user_id
  ENRICH name, email, department;
```

#### 3. Execute the policy

```sql
EXECUTE ENRICH POLICY user_enrichment;
```

#### 4. Create pipeline with enrich processor

```sql
CREATE PIPELINE events_enriched
WITH PROCESSORS (
    ENRICH (
        policy_name = "user_enrichment",
        field = "user_id",
        target_field = "user",
        max_matches = 1,
        ignore_missing = true
    )
);
```

#### 5. Create table with pipeline

```sql
CREATE TABLE events (
  id INT,
  user_id KEYWORD,
  event_type KEYWORD,
  timestamp TIMESTAMP,
  PRIMARY KEY (id)
) WITH (default_pipeline = "events_enriched");
```

#### 6. Insert data

```sql
INSERT INTO events VALUES (1, 'U001', 'login', '2025-02-05T10:00:00Z');
```

#### Result

The document is enriched with user data:

```json
{
  "id": 1,
  "user_id": "U001",
  "event_type": "login",
  "timestamp": "2025-02-05T10:00:00Z",
  "user": {
    "user_id": "U001",
    "name": "Alice Smith",
    "email": "alice@example.com",
    "department": "Engineering"
  }
}
```

---

## Index Migration Workflow

### Initial Creation

```sql
CREATE TABLE users (...);
```

Creates:

- index or template
- default pipeline
- mapping + settings
- metadata (PK, defaults, scripts)

---

### Schema Evolution

#### Add a column

```sql
ALTER TABLE users ADD COLUMN last_login TIMESTAMP;
```

#### Modify a column

```sql
ALTER TABLE users ALTER COLUMN name SET OPTIONS (analyzer = 'french');
```

#### Add a STRUCT field

```sql
ALTER TABLE users ALTER COLUMN profile ADD FIELD followers INT;
```

#### Drop a column

```sql
ALTER TABLE users DROP COLUMN old_field;
```

---

### Migration Safety

The Gateway ensures:

- non-destructive updates
- mapping compatibility checks
- pipeline regeneration when needed
- template updates for partitioned tables
- index updates for non-partitioned tables

---

### Full Replacement (CTAS)

```sql
CREATE OR REPLACE TABLE users AS
SELECT id, name FROM old_users;
```

Steps:

1. infer schema
2. create new index/template
3. bulk-copy data
4. atomically replace

---

## Version Compatibility

| Feature              | ES6  | ES7  | ES8  | ES9  |
|----------------------|------|------|------|------|
| Legacy templates     | ✔    | ✔    | ✖    | ✖    |
| Composable templates | ✖    | ✔    | ✔    | ✔    |
| date_index_name      | ✔    | ✔    | ✔    | ✔    |
| Generated scripts    | ✔    | ✔    | ✔    | ✔    |
| STRUCT               | ✔    | ✔    | ✔    | ✔    |
| ARRAY\<STRUCT>       | ✔    | ✔    | ✔    | ✔    |
| Watchers             | ✔    | ✔    | ✔    | ✔    |
| Enrich Policies      | ✖    | ✔*   | ✔    | ✔    |
| Materialized Views   | ✖    | ✔*   | ✔    | ✔    |

\* Enrich policies and materialized views require ES 7.5+

See [Materialized Views](materialized_views.md) for full documentation on materialized views.

---

## Quick Reference

### Watcher Syntax Summary

```sql
CREATE [OR REPLACE] WATCHER name AS
  -- Trigger (required)
  EVERY n {MILLISECONDS|SECONDS|MINUTES|HOURS|DAYS|WEEKS|MONTHS|YEARS}
  | AT SCHEDULE 'cron'
  
  -- Input (required)
  WITH NO INPUT
  | WITH INPUT (key = value, ...)
  | FROM index [WHERE criteria] [WITHIN n unit]
  | WITH INPUT method "url" [HEADERS (...)] [BODY "..."] [TIMEOUT (...)]
  | WITH INPUT method PROTOCOL scheme HOST "host" [PORT n] [PATH "path"] [PARAMS (...)] [HEADERS (...)] [BODY "..."] [TIMEOUT (...)]
  | WITH INPUTS name [AS] input, name [AS] input, ...
  
  -- Condition (required)
  NEVER DO | ALWAYS DO
  | WHEN path op value DO
  | WHEN SCRIPT '...' USING LANG '...' WITH PARAMS (...) RETURNS TRUE DO
  
  -- Actions (at least one, AS is optional)
  name [AS] LOG "message" [AT level] [FOREACH "path"] [LIMIT n]
  | name [AS] WEBHOOK method "url" [HEADERS (...)] [BODY "..."] [TIMEOUT (...)] [FOREACH "path"] [LIMIT n]
  | name [AS] WEBHOOK method PROTOCOL scheme HOST "host" [PORT n] [PATH "path"] [PARAMS (...)] [HEADERS (...)] [BODY "..."] [TIMEOUT (...)] [FOREACH "path"] [LIMIT n]
  [, ...]
END
```

**Supported HTTP methods:** `HEAD`, `GET`, `POST`, `PUT`, `DELETE`

### Enrich Policy Syntax Summary

```sql
-- Create policy
CREATE [OR REPLACE] ENRICH POLICY name
  [TYPE {MATCH|GEO_MATCH|RANGE}]
  FROM source_index [, source_index2, ...]
  ON match_field
  ENRICH field1, field2, ...
  [WHERE criteria]

-- Execute policy (required before use)
EXECUTE ENRICH POLICY name;

-- Drop policy
DROP ENRICH POLICY [IF EXISTS] name;

-- Show policy
SHOW ENRICH POLICY name;
```

---

[Back to index](README.md)