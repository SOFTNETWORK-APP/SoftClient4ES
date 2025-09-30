[Back to index](./README.md)

# Aggregate Functions

**Navigation:** [Functions — Date / Time](./functions_date_time.md) · [Functions — Conditional](./functions_conditional.md)

This page documents aggregate functions.

---

### Function: COUNT
**Description:**  
Count rows or non-null expressions.  
With `DISTINCT` counts distinct values.

**Inputs:** 
- `expr` or `*`; optional `DISTINCT`

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT COUNT(*) AS total FROM emp;
-- Result: total = 42

SELECT COUNT(DISTINCT salary) AS distinct_salaries FROM emp;
-- Result: 8
```

---

### Function: SUM
**Description:**  
Sum of values.

**Inputs:** 
- `expr` (`NUMERIC`)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT SUM(salary) AS total_salary FROM emp;
```

---

### Function: AVG
**Description:**  
Average of values.

**Inputs:** 
- `expr` (`NUMERIC`)

**Output:** 
- `DOUBLE`

**Example:**
```sql
SELECT AVG(salary) AS avg_salary FROM emp;
```

---

### Function: MIN
**Description:**  
Minimum value in group.

**Inputs:** 
- `expr` (comparable)

**Output:** 
- same as input

**Example:**
```sql
SELECT MIN(hire_date) AS earliest FROM emp;
```

---

### Function: MAX
**Description:**  
Maximum value in group.

**Inputs:** 
- `expr` (comparable)

**Output:** 
- same as input

**Example:**
```sql
SELECT MAX(salary) AS top_salary FROM emp;
```

---

### Function: FIRST_VALUE
**Description:**  
Window: first value ordered by `ORDER BY`. Pushed as `top_hits size=1` to ES when possible.

**Inputs:** 
- `expr` with optional `OVER (PARTITION BY ... ORDER BY ...)`  
If `OVER` is not provided, only the expr column name is used for the sorting.

**Output:** 
- same as input

**Example:**
```sql
SELECT FIRST_VALUE(salary) 
OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
) AS first_salary
FROM emp;
```

---

### Function: LAST_VALUE
**Description:**  
Window: last value ordered by `ORDER BY. Pushed to ES by flipping sort order in `top_hits`.

**Inputs:** 
- `expr` with optional `OVER (PARTITION BY ... ORDER BY ...)`  
If `OVER` is not provided, only the expr column name is used for the sorting.

**Output:** 
- same as input

**Example:**
```sql
SELECT LAST_VALUE(salary) 
OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
) AS last_salary
FROM emp;
```

---

### Function: ARRAY_AGG
**Description:**  
Collect values into an array for each partition. Implemented using `OVER` and pushed to ES as `top_hits`. Post-processing converts hits to an array of scalars.

**Inputs:** 
- `expr` with optional `OVER (PARTITION BY ... ORDER BY ... LIMIT n)`  
If `OVER` is not provided, only the expr column name is used for the sorting.

**Output:** 
- `ARRAY<type_of_expr>`

**Example:**
```sql
SELECT department, 
ARRAY_AGG(name) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC 
    LIMIT 100
) AS employees
FROM emp;
-- Result: employees as an array of name values 
-- per department (sorted and limited)
```

[Back to index](./README.md)
