[Back to index](README.md)

# Type Conversion Functions and Operators

## Function: CAST (Alias: CONVERT)

**Description:**  
Converts a value to a specified SQL type. Fails if the conversion is invalid.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQL type: `INT`, `BIGINT`, `DOUBLE`, `DATE`, `DATETIME`, `TIMESTAMP`, `VARCHAR`, etc.)  

**Output:**  
- `targetType`  

**Example:**
```sql
SELECT CAST('2025-09-11' AS DATE) AS d FROM logs;
-- Result: 2025-09-11
```

---

## Function: TRY_CAST (Alias: SAFE_CAST)

**Description:**  
Attempts to convert a value to a specified SQL type. Returns `NULL` if the conversion fails instead of raising an error.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQL type: `INT`, `BIGINT`, `DOUBLE`, `DATE`, `DATETIME`, etc.)  

**Output:**  
- `targetType` (nullable)  

**Example:**
```sql
SELECT TRY_CAST('invalid-date' AS DATE) AS d FROM logs;
-- Result: NULL
```

---

## Operator: `::` (Cast Operator)

**Description:**  
Shorthand operator for casting. Equivalent to `CAST(value AS type)`.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQLType)  

**Output:**  
- `targetType`  

**Example:**
```sql
SELECT '2025-09-11'::DATE AS d, '125'::BIGINT AS b FROM logs;
-- Result: 2025-09-11, 125
```

---

## Behavior Notes

- `CAST` (`CONVERT`) will raise errors on invalid conversions.  
- `TRY_CAST` (`SAFE_CAST`) returns `NULL` instead of failing.  
- `::` is syntactic sugar, easier to read in queries.  
- Type inference relies on `baseType`, and explicit `CAST`/`TRY_CAST`/`::` updates the type context for following functions.  

---

## Restriction: cast the input of an aggregate, not its result

A cast cannot be applied to the **result** of an aggregate function. An aggregate has to be the
first function in a column's chain, and a cast wrapping it would sit ahead of it:

```sql
-- Rejected: "Aggregation function must be the first function in the chain"
SELECT MAX(salary)::BIGINT AS m FROM emp GROUP BY dept;
SELECT CAST(MAX(salary) AS BIGINT) AS m FROM emp GROUP BY dept;
```

All four spellings behave the same way — `::`, `CAST`, `TRY_CAST` and `CONVERT`.

Cast the aggregate's **input** instead. This is equivalent for every aggregate whose result type
follows its argument (`MIN`, `MAX`, `SUM`, `AVG`, the `STDDEV` / `VARIANCE` family, the percentiles):

```sql
-- Both supported
SELECT MAX(salary::BIGINT) AS m FROM emp GROUP BY dept;
SELECT MAX(CAST(salary AS BIGINT)) AS m FROM emp GROUP BY dept;
```

Casting is unrestricted everywhere an aggregate is not involved — the SELECT list, `WHERE`,
`ORDER BY`, and inside another function:

```sql
SELECT YEAR(createdAt)::VARCHAR AS y FROM logs;
SELECT id FROM logs WHERE tries::INT > 3 ORDER BY tries::INT;
```

[Back to index](README.md)
