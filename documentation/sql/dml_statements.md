[Back to index](README.md)

# 📘 DML Statements — SQL Gateway for Elasticsearch

---

## Introduction

The SQL Gateway provides a Data Manipulation Language (DML) layer on top of Elasticsearch.  
It supports:

- **INSERT**
- **INSERT ... AS SELECT**
- **UPDATE**
- **DELETE**
- **COPY INTO** (bulk ingestion)

The DML engine is:

- **schema-aware** (PK, defaults, scripts, STRUCT, ARRAY<STRUCT>)
- **pipeline-aware** (default pipeline + user pipelines)
- **partition-aware** (date-based routing)
- **primary-key-aware** (upsert semantics)
- **version-aware** (ES6 → ES9)

Each DML statement returns:

```scala
  case class DmlResult(
    inserted: Long = 0L,
    updated: Long = 0L,
    deleted: Long = 0L,
    rejected: Long = 0L
  ) extends QueryResult
```

---

## INSERT

### Standard Syntax

```sql
INSERT INTO table_name (col1, col2, ...)
VALUES (v1, v2, ...), (v3, v4, ...), ...;
```

INSERT operations:

- use the table’s **primary key** to generate `_id`
- pass documents through the **table pipeline**
- support:
	- STRUCT
	- ARRAY<STRUCT>
	- DEFAULT values
	- SCRIPT AS generated columns
	- NOT NULL constraints

---

### Full Example: INSERT INTO with STRUCT

```sql
CREATE TABLE IF NOT EXISTS dql_users (
  id INT NOT NULL,
  name VARCHAR FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate)),
  birthdate DATE,
  profile STRUCT FIELDS(
    city VARCHAR OPTIONS (fielddata = true),
    followers INT
  )
);

INSERT INTO dql_users (id, name, birthdate, profile) VALUES
  (1, 'Alice', '1994-01-01', {city = "Paris", followers = 100}),
  (2, 'Bob',   '1984-05-10', {city = "Lyon",  followers = 50}),
  (3, 'Chloe', '1999-07-20', {city = "Paris", followers = 200}),
  (4, 'David', '1974-03-15', {city = "Marseille", followers = 10});
```

**Behavior**

- `profile` → Elasticsearch `object`
- `name.raw` → multi-field
- DEFAULT, SCRIPT, NOT NULL → applied via pipeline
- PK → `_id` generated automatically

---

### INSERT INTO ... AS SELECT

```sql
INSERT INTO orders (order_id, customer_id, order_date, total)
AS SELECT
  id AS order_id,
  cust AS customer_id,
  date AS order_date,
  amount AS total
FROM staging_orders;
```

**Behavior**

- The SELECT is executed by the Gateway
- Results are inserted using the Bulk API
- Table pipelines are applied
- PK ensures upsert semantics

---

### INSERT INTO ... AS SELECT — Validation Workflow**

Before executing an `INSERT ... AS SELECT`, the Gateway performs a full validation pipeline to ensure schema correctness and safe upsert behavior.

### **1. Index Validation**
Ensures the target index name is valid.  
Invalid names return a 400 error.

### **2. SQL Parsing**
Extracts:
- target columns
- SELECT statement
- ON CONFLICT clause
- DO UPDATE flag
- conflict target columns

### **3. Load Index Metadata**
Loads the real Elasticsearch schema:
- primary key
- partitioning
- mapping
- settings

### **3.b Determine Effective Insert Columns**
If the INSERT column list is omitted, the Gateway derives it from the SELECT output.

### **3.c Validate ON CONFLICT Rules**
- If the index has a primary key:
	- conflict target must match the PK exactly
	- INSERT must include all PK columns
- If the index has no primary key:
	- ON CONFLICT requires an explicit conflict target
	- all conflict target columns must be included in INSERT

### **3.d Validate SELECT Output Columns**
Ensures:
- every INSERT column exists in the SELECT output
- aliases are resolved
- SELECT is valid

Otherwise, a 400 error is returned.

### **4. Derive Bulk Options**
Determines:
- `_id` generation (PK or composite PK)
- partitioning suffix
- upsert behavior (`update = DO UPDATE`)

### **5. Build Document Source**
- For VALUES: convert to JSON array
- For SELECT: scroll the query and convert rows to JSON

### **6. Bulk Insert**
Uses the Bulk API with:
- PK-based `_id`
- partitioning
- pipeline execution
- upsert if DO UPDATE

Returns:

```scala
DmlResult(inserted = N, rejected = M)
```

---

## UPDATE

### Syntax

```sql
UPDATE table_name
SET col1 = expr1, col2 = expr2, ...
WHERE condition;
```

**Behavior**

- UPDATE uses **update_by_query**
- Supports:
	- nested fields
	- STRUCT fields
	- ARRAY<STRUCT> (via full replacement)
	- expressions
	- PK-based filtering

---

## DELETE

**Syntax**

```sql
DELETE FROM table_name
WHERE condition;
```

**Behavior**

- DELETE uses **delete_by_query**
- Supports:
	- PK-based deletion
	- nested conditions
	- date filters

---

## COPY INTO (Bulk Ingestion)

COPY INTO is a **DML operator** that loads documents from external files into an Elasticsearch index.  
It uses **only the Bulk API**, not the SQL engine.

**Syntax**

```sql
COPY INTO table_name
FROM 'path/to/file'
[FILE_FORMAT = 'JSON' | 'JSON_ARRAY' | 'PARQUET' | 'DELTA_LAKE']
[ON CONFLICT (pk_column) DO UPDATE];
```

**Behavior**

COPY INTO performs:

1. **Index name validation**
2. **Loading of the real Elasticsearch schema**
	- mapping
	- primary key
	- partitioning
3. **Primary key extraction**
	- PK → `_id`
	- composite PK → concatenated `_id`
4. **Partitioning extraction**
	- suffix index name based on date
5. **Bulk ingestion via `bulkFromFile`**
6. **Pipeline execution**
7. **Return of `DmlResult`**
	- `inserted` = successfully indexed docs
	- `rejected` = Bulk failures

There are **no** strategies like `insertAfter`, `updateAfter`, or `deleteAfter`.  
COPY INTO **does not** perform SQL-level operations — everything is Bulk.

**Full Example**

- **Table Definition**

```sql
CREATE TABLE IF NOT EXISTS copy_into_test (
  uuid KEYWORD NOT NULL,
  name VARCHAR,
  birthDate DATE,
  childrenCount INT,
  PRIMARY KEY (uuid)
);
```

- **Data File** (`example_data.json`)

```
{"uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0}
{"uuid": "A14", "name": "Moe Szyslak", "birthDate": "1967-11-21", "childrenCount": 0}
{"uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09", "childrenCount": 2}
```

- **COPY INTO Statement**

```sql
COPY INTO copy_into_test
FROM 's3://my-bucket/path/to/example_data.json'
FILE_FORMAT = 'JSON'
ON CONFLICT (uuid) DO UPDATE;
```

**Behavior**

- PK = `uuid` → `_id = uuid`
- ON CONFLICT DO UPDATE → Bulk upsert
- Table pipeline is applied
- Partitioning is applied if defined
- Returns `DmlResult(inserted = N, rejected = 0)`

---

## DML Lifecycle Example

```sql
INSERT INTO dml_chain (id, value) VALUES
  (1, 10),
  (2, 20),
  (3, 30);

UPDATE dml_chain
SET value = 50
WHERE id IN (1, 3);

DELETE FROM dml_chain
WHERE value > 40;

SELECT * FROM dml_chain ORDER BY id ASC;
```

---

## Version Compatibility

| Feature          | ES6  | ES7  | ES8  | ES9  |
|------------------|------|------|------|------|
| INSERT           | ✔    | ✔    | ✔    | ✔    |
| INSERT AS SELECT | ✔    | ✔    | ✔    | ✔    |
| UPDATE           | ✔    | ✔    | ✔    | ✔    |
| DELETE           | ✔    | ✔    | ✔    | ✔    |
| COPY INTO        | ✔    | ✔    | ✔    | ✔    |
| JSON_ARRAY       | ✔    | ✔    | ✔    | ✔    |
| PARQUET          | ✔    | ✔    | ✔    | ✔    |
| DELTA_LAKE       | ✔    | ✔    | ✔    | ✔    |

---

[Back to index](README.md)
