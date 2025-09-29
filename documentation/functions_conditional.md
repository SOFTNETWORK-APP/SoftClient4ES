[Back to index](./README.md)

# Conditional Functions

This page documents conditional expressions. Two syntaxes for `CASE` are supported and described below.

---

### Function: CASE (searched form)  
**Name & Aliases:** `CASE WHEN ... THEN ... ELSE ... END` (searched CASE form)

**Description:**  
Evaluates boolean WHEN expressions in order; returns the result expression corresponding to the first true condition; if none match, returns the ELSE expression (or NULL if ELSE omitted).

**Inputs:**  
- One or more `WHEN condition THEN result` pairs. Optional `ELSE result`.

**Output:**  
- Type coerced from result expressions (THEN/ELSE).

**Example:**
```sql
SELECT CASE
         WHEN salary > 100000 THEN 'very_high'
         WHEN salary > 50000 THEN 'high'
         ELSE 'normal'
       END AS salary_band
FROM emp;
-- Result: 'very_high' / 'high' / 'normal'
```

---

### Function: CASE (simple / expression form)  
**Name & Aliases:** `CASE expr WHEN val1 THEN r1 WHEN val2 THEN r2 ... ELSE rN END` (simple CASE)

**Description:**  
Compare `expr` to `valN` sequentially using equality; returns corresponding `rN` for first match; else `ELSE` result or NULL.

**Inputs:**  
- `expr` (any comparable type) and pairs `WHEN value THEN result`.

**Output:**  
- Type coerced from result expressions.

**Example:**
```sql
SELECT CASE department
         WHEN 'IT' THEN 'tech'
         WHEN 'Sales' THEN 'revenue'
         ELSE 'other'
       END AS dept_category
FROM emp;
-- Result: 'tech', 'revenue', or 'other' depending on department
```

**Implementation notes:**  
- The simple form evaluates by comparing `expr = value` for each WHEN.  
- Both CASE forms are parsed and translated into nested conditional Painless scripts for `script_fields` when used outside an aggregation push-down.

---

### Function: COALESCE
**Description:** Return first non-null argument.
**Inputs:** `expr1, expr2, ...`
**Output:** Value of first non-null expression (coerced)
**Example:**
```sql
SELECT COALESCE(nickname, firstname, 'N/A') AS display FROM users;
-- Result: 'Jo' or 'John' or 'N/A'
```

---

### Function: NULLIF
**Description:** Return NULL if expr1 = expr2; otherwise return expr1.
**Inputs:** `expr1, expr2`
**Output:** Type of `expr1`
**Example:**
```sql
SELECT NULLIF(status, 'unknown') AS status_norm FROM events;
```

---

### Predicate: IS NULL / IS NOT NULL (also exposed as functions ISNULL / ISNOTNULL)
**Description:** Test nullness.
**Inputs:** `expr`
**Output:** BOOLEAN
**Example:**
```sql
SELECT ISNULL(manager) AS manager_missing FROM emp;
```

[Back to index](./README.md)
