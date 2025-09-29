[Back to index](./README.md)

# Type Conversion Functions

---

### Function: CAST (Aliases: CONVERT)
**Description:** 

Cast expression to a target SQL type.

**Inputs:** 
- `expr`
- `TYPE` (`DATE`, `TIMESTAMP`, `VARCHAR`, `INT`, `DOUBLE`, etc.)

**Output:** 
- `TYPE`

**Example:**
```sql
SELECT CAST(salary AS DOUBLE) AS s FROM emp;
-- Result: 12345.0
```

### Function: TRY_CAST (Aliases: none)
**Description:** 

Attempt a cast and return NULL on failure (safer alternative).

**Inputs:** 
- `expr`
- `TYPE` (`DATE`, `TIMESTAMP`, `VARCHAR`, `INT`, `DOUBLE`, etc.)

**Output:** 

- `TYPE`or `NULL`

**Example:**
```sql
SELECT TRY_CAST('not-a-number' AS INT) AS maybe_null;
-- Result: NULL
```

[Back to index](./README.md)
