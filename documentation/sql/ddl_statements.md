# 📘 **DDL Statements — SQL Gateway for Elasticsearch**

---

## Introduction

The SQL Gateway provides a full Data Definition Language (DDL) layer on top of Elasticsearch.  
It allows defining tables, schemas, pipelines, mappings, and settings using a relational syntax while generating the appropriate Elasticsearch structures:

- **indices** (for non-partitioned tables)
- **index templates** (for partitioned tables)
- **ingest pipelines** (default or user-defined)
- **mappings** and **settings**
- **metadata** (primary key, generated columns, comments, options)

The DDL engine is:

- **version-aware** (ES6 → ES9)
- **client-agnostic** (Jest, RHLC, Java Client)
- **schema-driven**
- **round-trip safe** (DESCRIBE returns normalized SQL)

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
	- `SCRIPT AS` (except inside ARRAY<STRUCT>)
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

## Pipelines in DDL

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

## SHOW TABLE

```sql
SHOW TABLE users;
```

Returns:

- index or template metadata
- primary key
- partitioning
- pipeline
- mapping summary

---

## DESCRIBE TABLE

```sql
DESCRIBE TABLE users;
```

Returns the **normalized SQL schema**, including:

- columns
- types
- defaults
- scripts
- STRUCT fields
- PK
- options
- comments

---

## CREATE TABLE AS SELECT (CTAS)

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

## 🔄 Index Migration Workflow

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
| ARRAY<STRUCT>        | ✔    | ✔    | ✔    | ✔    |

---
