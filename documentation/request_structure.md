[Back to index](./README.md)

# Query Structure

**Navigation:** [Operators](./operators.md) · [Functions — Aggregate](./functions_aggregate.md) · [Keywords](./keywords.md)

This page documents the SQL clauses supported by the engine and how they map to Elasticsearch.

---

## SELECT
**Description:**  
Projection of fields, expressions and computed values.

**Behavior:**  
- `_source` includes for plain fields.  
- Computed expressions are translated into `script_fields` (Painless) when push-down is not otherwise possible.  
- Aggregates are translated to ES aggregations and the top-level `size` is often set to `0` for aggregation-only queries.

**Example:**
```sql
SELECT department, COUNT(*) AS cnt
FROM emp
GROUP BY department;
```

---

## FROM
**Description:**  
Source index (one or more). Translates to the Elasticsearch index parameter.

**Example:**
```sql
SELECT * FROM employees;
```

---

## UNNEST
**Description:**  
Expand an array / nested field into rows. Mapped to Elasticsearch `nested` and inner hits where necessary.

**Example:**
```sql
SELECT id, phone
FROM customers
UNNEST(phones) AS phone;
```

---

## WHERE
**Description:**  
Row-level predicates. Mapped to `bool` queries; complex expressions become `script` queries (Painless).

**Example:**
```sql
SELECT * FROM emp WHERE salary > 50000 AND department = 'IT';
```

---

## GROUP BY
**Description:**  
Aggregation buckets. Mapped to `terms`/`date_histogram` and nested sub-aggregations.  
Non-aggregated selected fields are disallowed unless included in the `GROUP BY` (standard SQL semantics).

**Example:**
```sql
SELECT department, AVG(salary) AS avg_salary
FROM emp
GROUP BY department;
```

---

## HAVING
**Description:**  
Filter groups using aggregate expressions. Implemented with pipeline aggregations and `bucket_selector` where possible, or client-side filtering if required.

**Example:**
```sql
SELECT department, COUNT(*) AS cnt
FROM emp
GROUP BY department
HAVING COUNT(*) > 10;
```

---

## ORDER BY
**Description:**  
Sorting of final rows or ordering used inside window/aggregations (pushed to `sort` or `top_hits`).

**Example:**
```sql
SELECT name, salary FROM emp ORDER BY salary DESC;
```

---

## LIMIT / OFFSET
**Description:**  
Limit and paging. For pure aggregations, `size` is typically set to 0 and `limit` applies to aggregations or outer rows.

**Example:**
```sql
SELECT * FROM emp ORDER BY hire_date DESC LIMIT 10 OFFSET 20;
```

[Back to index](./README.md)
