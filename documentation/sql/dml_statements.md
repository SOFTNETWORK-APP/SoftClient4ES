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

[Back to index](README.md)
