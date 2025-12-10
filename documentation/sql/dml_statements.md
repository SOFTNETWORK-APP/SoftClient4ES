# DML Support
[Back to index](README.md)

This document describes the SQL statements supported by the API, focusing on **Data Manipulation Language (DML)**. Each section provides syntax, examples, and notes on behavior.

---

## 📊 Data Manipulation Language (DML)

### INSERT
Insert new rows into a table, either with explicit values or from a `SELECT`.

**Syntax:**
```sql
INSERT INTO table_name (col1, col2, ...)
VALUES (val1, val2, ...);

INSERT INTO table_name
SELECT ...
```

**Examples:**
```sql
INSERT INTO users (id, name) VALUES (1, 'Alice');

INSERT INTO users SELECT id, name FROM old_users;
```

---

### UPDATE
Update existing rows in a table.

**Syntax:**
```sql
UPDATE table_name
SET col1 = val1, col2 = val2, ...
[WHERE condition]
```

**Example:**
```sql
UPDATE users SET name = 'Bob', age = 42 WHERE id = 1;
```

---

### DELETE
Delete rows from a table.

**Syntax:**
```sql
DELETE FROM table_name
[WHERE condition]
```

**Example:**
```sql
DELETE FROM users WHERE age > 30;
```

---

## 🔄 DML Execution Strategy

The SQL DML statements (`INSERT`, `UPDATE`, `DELETE`) are automatically translated into Elasticsearch operations.  
The execution path depends on the **number of impacted rows**:

- **Single row impacted** → direct ES operation:
	- `INSERT` → `_index`
	- `UPDATE` → `_update`
	- `DELETE` → `_delete`

- **Multiple rows impacted** → bulk ingestion:
	- All operations are batched and executed via the `_bulk` API.
	- Bulk execution is implemented using **Akka Streams**, ensuring efficient back‑pressure handling, parallelism, and resilience for large datasets.

---

### 📌 Example Translation

**SQL:**
```sql
INSERT INTO users (id, name) VALUES (1, 'Alice');
```

**ES:**
```curl
PUT users/_doc/1
{
  "id": 1,
  "name": "Alice"
}
```

---

**SQL:**
```sql
UPDATE users SET name = 'Bob' WHERE id = 1;
```

**ES:**
```curl
POST users/_update/1
{
  "doc": { "name": "Bob" }
}
```

---

**SQL:**
```sql
DELETE FROM users WHERE id = 1;
```

**ES:**
```curl
DELETE users/_doc/1
```

---

**SQL (multi‑row):**
```sql
INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
```

**ES (bulk via Akka Streams):**
```curl
POST _bulk
{ "index": { "_index": "users", "_id": "1" } }
{ "id": 1, "name": "Alice" }
{ "index": { "_index": "users", "_id": "2" } }
{ "id": 2, "name": "Bob" }
```

---

### ✅ Notes
- The API automatically chooses between **single‑doc operations** and **bulk operations**.
- Bulk execution is stream‑based, scalable, and fault‑tolerant thanks to Akka Streams.
- This strategy ensures optimal performance while keeping SQL semantics transparent for the user.

---

[Back to index](README.md)
