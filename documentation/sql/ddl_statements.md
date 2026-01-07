# Data Definition Language (DDL) Support
[Back to index](README.md)

This document describes the SQL statements supported by the API, focusing on **Data Definition Language (DDL)**. Each section provides syntax, examples, and notes on behavior.

---

## 📐 Table (DDLs)

### CREATE TABLE
Create a new table with explicit column definitions or from a `SELECT` query.

**Syntax:**
```sql
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] table_name (
  column_name data_type [SCRIPT AS (sql) | FIELDS (...)] [DEFAULT value] [NOT NULL] [COMMENT 'comment'] [OPTIONS (...)],
  [... more columns ...],
  [PRIMARY KEY (column1, column2, ...)]
) [PARTITION BY column_name] OPTIONS ([mappings = (...)] , [settings = (...)] );
```

- `FIELDS (...)` can define **multi‑fields** (alternative analyzers for text) or **STRUCT** (nested objects).

**Examples:**
```sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL COMMENT 'user identifier',
  name VARCHAR FIELDS(raw Keyword COMMENT 'sortable') DEFAULT 'anonymous' OPTIONS (analyzer = 'french', search_analyzer = 'french'),
  birthdate DATE,
  age INT SCRIPT AS (DATEDIFF(birthdate, CURRENT_DATE, YEAR)),
  ingested_at TIMESTAMP DEFAULT _ingest.timestamp, -- special field
  PRIMARY KEY (id)
) PARTITION BY birthdate (MONTH), OPTIONS (mappings = (dynamic = false));

CREATE OR REPLACE TABLE users AS SELECT id, name FROM accounts;
```

---

## 📝 Notes
- **Types**: Supported SQL types include `INT`, `BIGINT`, `VARCHAR`, `BOOLEAN`, `DATE`, `TIMESTAMP`, etc.
- **Constraints**: `NOT NULL` and `DEFAULT` are supported. Other relational constraints (e.g., `PRIMARY KEY`) are not enforced by Elasticsearch.
- **SCRIPT AS (sql)**: Defines a scripted column computed at ingestion time.
- **FIELDS**: Dual purpose — multi‑fields for text analysis and STRUCT for nested data modeling.
- **Comments**: Column comments can be added via `COMMENT 'text'`.
- **Options**: Column options can be specified via `OPTIONS (...)`.
- The **partition key must be of type `DATE`** and the partition column must be explicitly defined in the table schema.

---

### ALTER TABLE
Modify an existing table. Multiple statements can be grouped inside parentheses.

**Supported statements:**
- `ADD COLUMN [IF NOT EXISTS] column_definition` → Add a new column.
- `DROP COLUMN [IF EXISTS] column_name` → Remove an existing column.
- `RENAME COLUMN old_name TO new_name` → Rename an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET SCRIPT AS (sql)` → Define or update a scripted column.
- `ALTER COLUMN [IF EXISTS] column_name DROP SCRIPT` → Remove a scripted column.
- `ALTER COLUMN [IF EXISTS] column_name SET|ADD OPTION (key = value)` → Set a specific option for an existing column.
- `ALTER COLUMN [IF EXISTS] column_name DROP OPTION key` → Remove a specific option from an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET COMMENT 'comment'` → Set or update the comment for an existing column.
- `ALTER COLUMN [IF EXISTS] column_name DROP COMMENT` → Remove the comment from an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET DEFAULT value` → Set or update the default value for an existing column.
- `ALTER COLUMN [IF EXISTS] column_name DROP DEFAULT` → Remove the default value from an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET NOT NULL` → Make an existing column NOT NULL.
- `ALTER COLUMN [IF EXISTS] column_name DROP NOT NULL` → Remove the NOT NULL constraint from an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET DATA TYPE new_type` → Change the data type of an existing column.
- `ALTER COLUMN [IF EXISTS] column_name SET|ADD FIELD field_definition` → Add or update a field inside a STRUCT or multi‑field.
- `ALTER COLUMN [IF EXISTS] column_name DROP FIELD field_name` → Remove a field from a STRUCT or multi‑field.
- `ALTER COLUMN [IF EXISTS] column_name SET FIELDS &#40;...&#41;` → Allows defining nested fields &#40;STRUCT&#41; or multi‑fields inside an existing column.
- `SET|ADD MAPPING (key = value)`	→ Set table‑level mapping.
- `DROP MAPPING key` → Remove table‑level mapping.
- `SET|ADD SETTING (key = value)` → Set table‑level setting.
- `DROP SETTING key` → Remove table‑level setting.

[//]: # (- `ALTER COLUMN [IF EXISTS] column_name SET OPTIONS &#40;...&#41;`)
[//]: # (	→ Set multiple options for an existing column.)

**Examples:**
```sql
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0;

ALTER TABLE users
  RENAME COLUMN name TO full_name;

ALTER TABLE users (
  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0,
  RENAME COLUMN name TO full_name,
  ALTER COLUMN IF EXISTS status SET DEFAULT 'active',
  ALTER COLUMN IF EXISTS profile SET FIELDS (
    description VARCHAR DEFAULT 'N/A',
    visibility BOOLEAN DEFAULT true
  )
);
```

---

### DROP TABLE
Remove an existing table.

**Syntax:**
```sql
DROP TABLE [IF EXISTS] table_name [CASCADE]
```

**Example:**
```sql
DROP TABLE IF EXISTS users CASCADE;
```

---

### TRUNCATE TABLE
Delete all rows from a table without removing its definition.

**Syntax:**
```sql
TRUNCATE TABLE table_name
```

**Example:**
```sql
TRUNCATE TABLE users;
```

---

## 🧩 Nested and Structured Data

### FIELDS for Multi‑fields
`FIELDS (...)` can be used to define **multi‑fields** for text columns. This allows you to index the same column in multiple ways (e.g., with different analyzers).

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
- `content.keyword` is a keyword sub‑field.
- `content.english` is a text sub‑field with the English analyzer.

---

### FIELDS for STRUCT or NESTED OBJECTS
`FIELDS (...)` also enables the definition of **STRUCT** types, which may represent either object or nested objects with their own fields. This is how you model hierarchical data.

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

This maps naturally to **Elasticsearch** `object` type.

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

- `products` is an `ARRAY<STRUCT>` column containing multiple fields.

This maps naturally to **Elasticsearch** `nested` type.

### 📝 Notes
- The meaning of `FIELDS (...)` depends on the column type:
  - On `VARCHAR` types, it defines **multi‑fields**.
  - On `STRUCT`, it defines the **fields of the struct**.
  - On `ARRAY<STRUCT>`, it defines the **fields of each element**.
- Sub‑fields defined inside `FIELDS (...)` support the full DDL syntax:
	- nested `FIELDS`
  - `SCRIPT AS (sql)` (for scripted sub‑fields)
	- `DEFAULT`
	- `NOT NULL`
	- `COMMENT`
	- `OPTIONS`
- Multi‑level nesting is supported.
- `SCRIPT AS (sql)` can not be used for `ARRAY<STRUCT>`.

---

## 🔄 MappingApi Migration Workflow

The `MappingApi` provides intelligent mapping management with **automatic migration, validation, and rollback capabilities**. This ensures that SQL commands such as `ALTER TABLE … ALTER COLUMN SET TYPE …` are safely translated into Elasticsearch operations.

### ✨ Features
- ✅ **Automatic Change Detection**: Compares existing mappings with new ones
- ✅ **Safe Migration Strategy**: Creates temporary indices, reindexes, and renames atomically
- ✅ **Automatic Rollback**: Reverts to original state if migration fails
- ✅ **Backup & Restore**: Preserves original mappings and settings
- ✅ **Progress Tracking**: Detailed logging of migration steps
- ✅ **Validation**: Strict JSON validation with error reporting

---

### 📊 Migration Workflow

```
SQL Command: ALTER TABLE users ALTER COLUMN age SET TYPE BIGINT
        │
        ▼
MappingApi Execution:
  1. Backup current mapping and settings
  2. Create temporary index with new mapping (age: long)
  3. Reindex data from original → temporary
  4. Delete original index
  5. Recreate original index with new mapping
  6. Reindex data from temporary → original
  7. Delete temporary index
  8. Rollback if any step fails
```

---

### 📝 Notes
- **Atomicity**: The workflow ensures that schema changes are applied safely without downtime.
- **Transparency**: Users only see the SQL command; the migration logic is handled internally.
- **Consistency**: All data is reindexed into the new mapping, guaranteeing type correctness.
- **Resilience**: Rollback and backup mechanisms prevent data loss in case of errors.

---

##  📅 Partitioned Tables

SoftClient4ES supports **partitioned tables** via the `PARTITION BY` clause in `CREATE TABLE`.  
This feature allows automatic routing of documents into indices partitioned by the value of a date column.

---

### ✅ Supported Syntax

```sql
CREATE TABLE [IF NOT EXISTS] table_name (
  column_definitions
) PARTITION BY column_name [(granularity)]
```

- The **partition key must be of type `DATE`**.
- The partition column must be explicitly defined in the table schema.
- Granularity is optional. If omitted, defaults to `DAY`.
- Granularity can be explicitly set with `PARTITION BY column (YEAR|MONTH|DAY|HOUR|MINUTE|SECOND)`.
- Partitioning is typically used for time‑based data (e.g., `birthdate`, `event_date`).

---

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

### 📌 Example

```sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL,
  name VARCHAR DEFAULT 'anonymous',
  birthdate DATE
) PARTITION BY birthdate (MONTH);
```

- Creates a table `users` partitioned by the `birthdate` column.
- Documents are routed into indices partitioned by **month** of `birthdate`.  
	For `birthdate = 2025-12-10`, the target index will be `users-2025-12`.

---

### ⚙️ Elasticsearch Translation

- **Mapping**: the partition key must be declared as a `date` field in the index mapping.
- **Pipeline**: SoftClient4ES uses the [`date_index_name`](https://www.elastic.co/guide/en/elasticsearch/reference/current/date-index-name-processor.html) processor to route documents into partitioned indices.
- **Index Templates**: SoftClient4ES uses it to ensure consistent mappings and settings across all partitioned indices.

---

### 📝 Notes

- **Granularity**: controlled via `date_rounding` (`y`, `M`, `d`, etc.).
- **Migration**: existing documents are reindexed to be redistributed 🔀 into the correct partitions.
- **SQL vs ES**: in SQL, `PARTITION BY` is a logical clause; in Elasticsearch, it is implemented via ingest pipelines and index naming.

---

## 🔑 Composite Primary Keys

SoftClient4ES supports composite primary keys in SQL.  
In SQL, a primary key can be defined on multiple columns (`PRIMARY KEY (col1, col2)`), ensuring uniqueness across the combination of values.  
In Elasticsearch, uniqueness is enforced by the special `_id` field. To emulate composite primary keys, `_id` is constructed from multiple fields using the `set` processor.

---

### ✅ Syntax

```sql
CREATE TABLE users (
  id INT NOT NULL,
  birthdate DATE NOT NULL,
  name VARCHAR,
  PRIMARY KEY (id, birthdate)
);
```

---

### ⚙️ Elasticsearch Translation

A pipeline is automatically created to set `_id` as a concatenation of the primary key columns:

```curl
PUT _ingest/pipeline/users-composite-id
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

- `_id` is built from the values of `id` and `birthdate`.
- Example: `id = 42`, `birthdate = 2025-12-10` → `_id = "42|2025-12-10"`.
- The separator (`||`) can be customized to avoid collisions (sql.composite-key-separator).

---

### 📝 Notes
- **Stability**: chosen fields must be immutable to preserve uniqueness.
- **Performance**: avoid overly long `_id` values.
- **SQL ↔ ES Mapping**:
	- `PRIMARY KEY (id)` → `_id = id`
	- `PRIMARY KEY (id, birthdate)` → `_id = "{{id}}-{{birthdate}}"`

---

## Scripted Columns 🧮

SoftClient4ES supports scripted columns in `CREATE TABLE`.  
These columns are **computed at ingestion time** using an ingest pipeline `script` processor.  
The value is persisted in `_source` and behaves like any other field.

---

### ✅ Syntax

```sql
CREATE TABLE table_name (
  column_definitions,
  scripted_column TYPE SCRIPT AS (sql_expression)
);
```

- `scripted_column` is a regular column name.
- `TYPE` defines the target type (`INT`, `VARCHAR`, etc.).
- `SCRIPT AS (sql_expression)` defines the computation in SQL syntax.
- The expression is translated into **Painless** for Elasticsearch.

---

### 📌 SQL → ES Translation

**SQL:**
```sql
CREATE TABLE users (
  id INT NOT NULL,
	name VARCHAR,
  birthdate DATE,
  age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate)),
  PRIMARY KEY (id)
) PARTITION BY birthdate (MONTH);
```

**Elasticsearch pipeline:**
```curl
PUT _ingest/pipeline/users-pipeline
{
  "processors": [
    {
      "script": {
        "source": "ctx.age = ChronoUnit.YEARS.between(ctx.birthdate, Instant.now())"
      }
    }
  ]
}
```

---

### 📖 Examples

| SQL Script Expression                      | ES Painless Translation (ingest)                                   |
|--------------------------------------------|--------------------------------------------------------------------|
| `YEAR(CURRENT_DATE) - YEAR(birthdate)`     | `ctx.age = ChronoUnit.YEARS.between(ctx.birthdate, Instant.now())` |
| `UPPER(name)`                              | `ctx.name_upper = ctx.name.toUpperCase()`                          |
| `CONCAT(firstname, ' ', lastname)`         | `ctx.fullname = ctx.firstname + ' ' + ctx.lastname`                |
| `CASE WHEN status = 'X' THEN 1 ELSE 0 END` | `ctx.flag = ctx.status == 'X' ? 1 : 0`                             |

---

### 📝 Notes
- Scripted columns are **evaluated once at ingestion**.
- They are **persisted** in `_source`, unlike runtime fields.
- SQL expressions are translated into equivalent **Painless** code.
- This feature allows declarative enrichment directly in the DDL.

---

## 🧩 Pipeline DDLs

Pipelines define ordered ingestion processors that transform documents before they are indexed.  
The SQL‑ES dialect supports three pipeline‑related statements:

- `CREATE PIPELINE`
- `DROP PIPELINE`
- `ALTER PIPELINE`

These statements allow users to declare, replace, remove, or modify ingestion pipelines in a declarative SQL‑style syntax.

---

### 🚀 CREATE PIPELINE

#### **Syntax**

```sql
CREATE [OR REPLACE] PIPELINE pipeline_name
[IF NOT EXISTS]
WITH PROCESSORS (
    processor_1,
    processor_2,
    ...
);
```

#### **Description**

- Creates a new ingestion pipeline.
- `OR REPLACE` overwrites an existing pipeline.
- `IF NOT EXISTS` prevents an error if the pipeline already exists.
- Processors are executed **in the order they are declared**.

#### **Supported Processor Types**

| SQL Processor | Elasticsearch Equivalent | Purpose |
|--------------|---------------------------|---------|
| `SET` | `set` | Assigns a value to a field in `ctx` |
| `SCRIPT` | `script` | Executes a Painless script |
| `REMOVE` | `remove` | Removes a field |
| `RENAME` | `rename` | Renames a field |
| `DATE_INDEX_NAME` | `date_index_name` | Generates an index name based on a date field |

#### **Example**

```sql
CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
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

---

### 🗑️ DROP PIPELINE

#### **Syntax**

```sql
DROP PIPELINE [IF EXISTS] pipeline_name;
```

#### **Description**

- Deletes a pipeline.
- `IF EXISTS` prevents an error if the pipeline does not exist.

#### **Example**

```sql
DROP PIPELINE IF EXISTS user_pipeline;
```

---

### 🔧 ALTER PIPELINE

#### **Syntax**

```sql
ALTER PIPELINE [IF EXISTS] pipeline_name (
    alter_action_1,
    alter_action_2,
    ...
);
```

#### **Supported Actions**

| Action | Description |
|--------|-------------|
| `ADD PROCESSOR <processor>` | Appends a processor to the pipeline |
| `DROP PROCESSOR <type> (<field>)` | Removes a processor identified by its type and field |
| *(Optional future extension)* `ALTER PROCESSOR` | Modify an existing processor |

#### **Example**

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

### 📐 Semantic Rules

#### **Processor Identity**

A processor is uniquely identified by:

```
(processor_type, field)
```

This means:

- Changing the type or field is equivalent to removing the old processor and adding a new one.
- Property changes are treated as processor modifications.

#### **Processor Ordering**

Ordering matters for certain processors:

- `DATE_INDEX_NAME` must appear last.
- `SET _id` must appear last.
- `RENAME` and `REMOVE` should appear before `SCRIPT`.

Other processors may be reordered without semantic impact.

---

### 📦 Summary of Supported Pipeline DDLs

| Statement | Purpose |
|----------|----------|
| `CREATE PIPELINE` | Define or replace a pipeline |
| `DROP PIPELINE` | Remove a pipeline |
| `ALTER PIPELINE` | Add or remove processors |

---

[Back to index](README.md)
