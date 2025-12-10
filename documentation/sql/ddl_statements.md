# DDL Support
[Back to index](README.md)

This document describes the SQL statements supported by the API, focusing on **Data Definition Language (DDL)**. Each section provides syntax, examples, and notes on behavior.

---

## 📐 Data Definition Language (DDL)

### CREATE TABLE
Create a new table with explicit column definitions or from a `SELECT` query.

**Syntax:**
```sql
CREATE [OR REPLACE] TABLE [IF NOT EXISTS] table_name (
  column_name data_type [NOT NULL] [DEFAULT value] [OPTIONS (...)] [FIELDS (...)],
  [... more columns ...],
  [PRIMARY KEY (column1, column2, ...)]
) [PARTITION BY column_name]
```

- `FIELDS (...)` can define **multi‑fields** (alternative analyzers for text) or **STRUCT** (nested objects).

**Examples:**
```sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL,
  name VARCHAR DEFAULT 'anonymous'
);

CREATE OR REPLACE TABLE users AS SELECT id, name FROM accounts;
```

---

## 📝 Notes
- **Types**: Supported SQL types include `INT`, `BIGINT`, `VARCHAR`, `BOOLEAN`, `DATE`, `TIMESTAMP`, etc.
- **Constraints**: `NOT NULL` and `DEFAULT` are supported. Other relational constraints (e.g., `PRIMARY KEY`) are not enforced by Elasticsearch.
- **FIELDS**: Dual purpose — multi‑fields for text analysis and STRUCT for nested data modeling.
- **Options**: Column options can be specified via `OPTIONS (...)`.
- The **partition key must be of type `DATE`** and the partition column must be explicitly defined in the table schema.

---

### ALTER TABLE
Modify an existing table. Multiple statements can be grouped inside parentheses.

**Supported statements:**
- `ADD COLUMN [IF NOT EXISTS] column_definition`
- `DROP COLUMN [IF EXISTS] column_name`
- `RENAME COLUMN old_name TO new_name`
- `ALTER COLUMN [IF EXISTS] column_name SET OPTIONS (...)`
- `ALTER COLUMN [IF EXISTS] column_name SET DEFAULT value`
- `ALTER COLUMN [IF EXISTS] column_name DROP DEFAULT`
- `ALTER COLUMN [IF EXISTS] column_name SET NOT NULL`
- `ALTER COLUMN [IF EXISTS] column_name DROP NOT NULL`
- `ALTER COLUMN [IF EXISTS] column_name SET DATA TYPE new_type`
- `ALTER COLUMN [IF EXISTS] column_name SET FIELDS (...)`  
	→ Allows defining nested fields (STRUCT) or multi‑fields inside an existing column.

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

### FIELDS for STRUCT
`FIELDS (...)` also enables the definition of **STRUCT** types, which represent nested objects with their own fields. This is how you model hierarchical data.

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
    )
  )
)
```

- `profile` is a `STRUCT` column containing multiple fields.
- `address` is a nested `STRUCT` inside `profile`.

This maps naturally to:
- **Elasticsearch**: `object` or `nested` type.
- **Avro**: `record`.

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

| SQL Granularity | ES `date_rounding` | Recommended `date_formats`         | Example Index Name |
|-----------------|--------------------|------------------------------------|--------------------|
| YEAR            | "y"                | ["yyyy"]                           | users-2025         |
| MONTH           | "M"                | ["yyyy-MM", "yyyy-MM-dd"]          | users-2025-12      |
| DAY (default)   | "d"                | ["yyyy-MM-dd"]                     | users-2025-12-10   |
| HOUR            | "h"                | ["yyyy-MM-dd'T'HH", "yyyy-MM-dd HH"] | users-2025-12-10-09 |
| MINUTE          | "m"                | ["yyyy-MM-dd'T'HH:mm"]             | users-2025-12-10-09-46 |
| SECOND          | "s"                | ["yyyy-MM-dd'T'HH:mm:ss"]          | users-2025-12-10-09-46-30 |

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
        "value": "{{id}}|{{birthdate}}"
      }
    }
  ]
}
```

- `_id` is built from the values of `id` and `birthdate`.
- Example: `id = 42`, `birthdate = 2025-12-10` → `_id = "42|2025-12-10"`.
- The separator (`|`) can be customized to avoid collisions (elastic.composite-key-separator).

---

### 📝 Notes
- **Stability**: chosen fields must be immutable to preserve uniqueness.
- **Performance**: avoid overly long `_id` values.
- **SQL ↔ ES Mapping**:
	- `PRIMARY KEY (id)` → `_id = id`
	- `PRIMARY KEY (id, birthdate)` → `_id = "{{id}}-{{birthdate}}"`

---

[Back to index](README.md)
