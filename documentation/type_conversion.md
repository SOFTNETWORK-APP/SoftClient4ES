[Back to index](./README.md)

# Type Conversion Functions and Operators

## Function: CAST (Alias: NONE)

**Description:**  
Converts a value to a specified SQL type. Fails if the conversion is invalid.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQL type: `INT`, `BIGINT`, `DOUBLE`, `DATE`, `DATETIME`, `TIMESTAMP`, `VARCHAR`, etc.)  

**Output:**  
- `targetType`  

**Example:**
```sql
SELECT CAST('2025-09-11' AS DATE) AS d;
-- Result: 2025-09-11
```

---

## Function: TRY_CAST (Aliases: SAFE_CAST)

**Description:**  
Attempts to convert a value to a specified SQL type. Returns `NULL` if the conversion fails instead of raising an error.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQL type: `INT`, `BIGINT`, `DOUBLE`, `DATE`, `DATETIME`, etc.)  

**Output:**  
- `targetType` (nullable)  

**Example:**
```sql
SELECT TRY_CAST('invalid-date' AS DATE) AS d;
-- Result: NULL
```

---

## Function: CONVERT (Alias: NONE)

**Description:**  
Converts a value to a specified SQL type. Equivalent to `CAST`, but uses function syntax instead of `CAST ... AS ...`.

**Inputs:**  
- `value` (ANY type)  
- `targetType` (SQL type)  

**Output:**  
- `targetType`  

**Example:**
```sql
SELECT CONVERT('125', BIGINT) AS b;
-- Result: 125
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
SELECT '2025-09-11'::DATE AS d, '125'::BIGINT AS b;
-- Result: 2025-09-11, 125
```

---

## Behavior Notes

- `CAST` and `CONVERT` will raise errors on invalid conversions.  
- `TRY_CAST` (`SAFE_CAST`) returns `NULL` instead of failing.  
- `::` is syntactic sugar, easier to read in queries.  
- Type inference relies on `baseType`, and explicit `CAST`/`CONVERT`/`::` updates the type context for following functions.  

[Back to index](./README.md)
