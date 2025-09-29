[Back to index](./README.md)

# Operators (detailed)

**Navigation:** [Query Structure](./query-structure.md) · [Operator Precedence](./operator_precedence.md) · [Keywords](./keywords.md)

This file provides a per-operator description and a concrete SQL example for each operator supported by the engine.

---

### Operator: `+`
**Description:** Arithmetic addition.
**Example:**
```sql
SELECT salary + bonus AS total_comp FROM emp;
-- result example: if salary=50000 and bonus=10000 -> total_comp = 60000
```

### Operator: `-`
**Description:** Arithmetic subtraction or unary negation when used with single operand.
**Example:**
```sql
SELECT salary - tax AS net FROM emp;
SELECT -balance AS negative_balance FROM accounts;
```

### Operator: `*`
**Description:** Multiplication.
**Example:**
```sql
SELECT quantity * price AS revenue FROM sales;
```

### Operator: `/`
**Description:** Division; division by zero must be guarded (NULLIF), engine returns NULL for invalid arithmetic.
**Example:**
```sql
SELECT total / NULLIF(count,0) AS avg FROM table;
```

### Operator: `%` (MOD)
**Description:** Remainder/modulo operator.
**Example:**
```sql
SELECT id % 10 AS bucket FROM users;
```

---

### Operator: `=`
**Description:** Equality comparison.
**Example:**
```sql
SELECT * FROM emp WHERE department = 'IT';
```

### Operator: `<>`, `!=`
**Description:** Inequality comparison (both synonyms supported).
**Example:**
```sql
SELECT * FROM emp WHERE status <> 'terminated';
```

### Operator: `<`, `<=`, `>`, `>=`
**Description:** Relational comparisons.
**Example:**
```sql
SELECT * FROM emp WHERE age >= 21 AND age < 65;
```

### Operator: `IN`
**Description:** Membership in a set of literal or numeric values or results of subquery (subquery support depends on implementation).
**Example:**
```sql
SELECT * FROM emp WHERE department IN ('Sales', 'IT', 'HR');
SELECT * FROM emp WHERE status IN (1, 2);
```

### Operator: `NOT IN`
**Description:** Negated membership.
**Example:**
```sql
SELECT * FROM emp WHERE department NOT IN ('HR','Legal');
```

### Operator: `BETWEEN ... AND ...`
**Description:** Inclusive range test: `expr BETWEEN a AND b` ⇔ `expr >= a AND expr <= b`.
**Example:**
```sql
SELECT * FROM emp WHERE salary BETWEEN 40000 AND 80000;
```

### Operator: `IS NULL`
**Description:** Null check predicate.
**Example:**
```sql
SELECT * FROM emp WHERE manager IS NULL;
```

### Operator: `IS NOT NULL`
**Description:** Negated null check.
**Example:**
```sql
SELECT * FROM emp WHERE manager IS NOT NULL;
```

---

### Operator: `LIKE`
**Description:** Pattern match using `%` and `_`. Engine converts `%` → `.*` and `_` → `.` for underlying regex matching.
**Example:**
```sql
SELECT * FROM emp WHERE name LIKE 'Jo%';
```

### Operator: `RLIKE`
**Description:** Regular-expression match (Java regex semantics).
**Example:**
```sql
SELECT * FROM users WHERE email RLIKE '.*@example\.com$';
```

---

### Operator: `AND`
**Description:** Logical conjunction.
**Example:**
```sql
SELECT * FROM emp WHERE dept = 'IT' AND salary > 50000;
```

### Operator: `OR`
**Description:** Logical disjunction.
**Example:**
```sql
SELECT * FROM emp WHERE dept = 'IT' OR dept = 'Sales';
```

### Operator: `NOT`
**Description:** Logical negation.
**Example:**
```sql
SELECT * FROM emp WHERE NOT active;
```

---

### Operator-like: `CAST(...)` / `CONVERT(...)`
**Description:** Type conversion operator/function. See Type Conversion functions page for details and examples.
**Example:**
```sql
SELECT CAST(hire_date AS DATE) FROM emp;
```

[Back to index](./README.md)
