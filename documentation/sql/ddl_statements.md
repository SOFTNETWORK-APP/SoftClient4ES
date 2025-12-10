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
  column_name data_type [NOT NULL] [DEFAULT value] [OPTIONS (...)] [FIELDS (...)]
)
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

## 📝 Notes
- **Types**: Supported SQL types include `INT`, `BIGINT`, `VARCHAR`, `BOOLEAN`, `DATE`, `TIMESTAMP`, etc.
- **Constraints**: `NOT NULL` and `DEFAULT` are supported. Other relational constraints (e.g., `PRIMARY KEY`) are not enforced by Elasticsearch.
- **FIELDS**: Dual purpose — multi‑fields for text analysis and STRUCT for nested data modeling.
- **Options**: Column and table options can be specified via `OPTIONS (...)`.

---

[Back to index](README.md)
