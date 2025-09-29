[Back to index](./README.md)

# Operators (detailed)

**Navigation:** [Query Structure](./request_structure.md) · [Operator Precedence](./operator_precedence.md) · [Keywords](./keywords.md)

This file provides a per-operator description and a concrete SQL example for each operator supported by the engine.

---

### Math operators

#### Operator: `+`
**Description:** 

Arithmetic addition.

**Example:**
```sql
SELECT salary + bonus AS total_comp FROM emp;
-- result example: if salary=50000 and bonus=10000 -> total_comp = 60000
```

#### Operator: `-`
**Description:** 

Arithmetic subtraction or unary negation when used with single operand.

**Example:**
```sql
SELECT salary - tax AS net FROM emp;
SELECT -balance AS negative_balance FROM accounts;
```

#### Operator: `*`
**Description:** 

Multiplication.

**Example:**
```sql
SELECT quantity * price AS revenue FROM sales;
```

#### Operator: `/`
**Description:** 

Division; division by zero must be guarded (NULLIF), engine returns NULL for invalid arithmetic.

**Example:**
```sql
SELECT total / NULLIF(count, 0) AS avg FROM table;
```

#### Operator: `%` (MOD)
**Description:** 

Remainder/modulo operator.

**Example:**
```sql
SELECT id % 10 AS bucket FROM users;
```

---

### Comparison operators

#### Operator: `=`
**Description:** 

Equality comparison.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE department = 'IT';
```

#### Operator: `<>`, `!=`
**Description:** 

Inequality comparison (both synonyms supported).

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE status <> 'terminated';
```

#### Operator: `<`, `<=`, `>`, `>=`
**Description:** 

Relational comparisons.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE age >= 21 AND age < 65;
```

#### Operator: `IN`
**Description:** 

Membership in a set of literal or numeric values or results of subquery (subquery support depends on implementation).

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE department IN ('Sales', 'IT', 'HR');
SELECT * FROM emp WHERE status IN (1, 2);
```

#### Operator: `NOT IN`
**Description:** 

Negated membership.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE department NOT IN ('HR','Legal');
```

#### Operator: `BETWEEN ... AND ...`

**Description:**

Checks if an expression lies between two boundaries (inclusive). 

For numeric expressions, `BETWEEN` works as standard SQL.

For distance expressions (`ST_DISTANCE`), it supports units (`m`, `km`, `mi`, etc.).

**Return type:**

- `BOOLEAN`

**Examples:**

- Numeric BETWEEN
```sql
SELECT age
FROM users
WHERE age BETWEEN 18 AND 30;
```

- Distance BETWEEN (using meters)

```sql
SELECT id
FROM locations
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) 
BETWEEN 4000 AND 5000;
```

- Distance BETWEEN (with explicit units)

```sql
SELECT id
FROM locations
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 km AND 5000 km;
```

👉 In Elasticsearch translation, the last 2 examples are optimized into a combination of:
- a **script filter** for the lower bound
- a `geo_distance` **query** for the upper bound (native ES optimization)

#### Operator: `IS NULL`
**Description:** 

Null check predicate.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE manager IS NULL;
```

#### Operator: `IS NOT NULL`
**Description:** 

Negated null check.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE manager IS NOT NULL;
```

#### Operator: `LIKE`
**Description:** 

Pattern match using `%` and `_`. Engine converts `%` → `.*` and `_` → `.` for underlying regex matching.

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM emp WHERE name LIKE 'Jo%';
```

#### Operator: `RLIKE`
**Description:** 

Regular-expression match (Java regex semantics).

**Return type:**

- `BOOLEAN`

**Example:**
```sql
SELECT * FROM users WHERE email RLIKE '.*@example\.com$';
```

---

### Logical operators

#### Operator: `AND`
**Description:** 

Logical conjunction.

**Example:**
```sql
SELECT * FROM emp WHERE dept = 'IT' AND salary > 50000;
```

#### Operator: `OR`
**Description:** 

Logical disjunction.

**Example:**
```sql
SELECT * FROM emp WHERE dept = 'IT' OR dept = 'Sales';
```

#### Operator: `NOT`
**Description:** 

Logical negation.

**Example:**
```sql
SELECT * FROM emp WHERE NOT active;
```

---

### Cast operators

#### Operator : `::`

**Description:** 

Provides an alternative syntax to the [CAST](./functions_type_conversion.md#function-cast-aliases-convert) function.

**Inputs:**
- `expr`
- `TYPE` (`DATE`, `TIMESTAMP`, `VARCHAR`, `INT`, `DOUBLE`, etc.)

**Return type:**

- `TYPE`

**Examples:**
```sql
SELECT hire_date::DATE FROM emp;
```

[Back to index](./README.md)
