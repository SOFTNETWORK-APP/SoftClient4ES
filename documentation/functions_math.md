[Back to index](./README.md)

# Mathematical Functions

**Navigation:** [Functions — Aggregate](./functions_aggregate.md) · [Functions — String](./functions_string.md)

---

### Function: ABS
**Description:**  
Absolute value.

**Inputs:** 
- `x` (`NUMERIC`)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT ABS(-5) AS a;
-- Result: 5
```

### Function: ROUND
**Description:**  
Round to n decimals (optional).

**Inputs:** `x` (`NUMERIC`), optional `n` (`INT`)

**Output:** 
- `DOUBLE`

**Example:**
```sql
SELECT ROUND(123.456, 2) AS r;
-- Result: 123.46
```

### Function: FLOOR
**Description:**  
Greatest `BIGINT` ≤ x.

**Inputs:** 
- `x` (`NUMERIC`)

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT FLOOR(3.9) AS f;
-- Result: 3
```

### Function: CEIL (Alias: CEILING)
**Description:**  
Smallest `BIGINT` ≥ x.

**Inputs:** 
- `x` (`NUMERIC`)

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT CEIL(3.1) AS c;
-- Result: 4
```

### Function: POWER (Alias: POW)
**Description:**  
x^y.

**Inputs:** 
- `x` (`NUMERIC`), `y` (`NUMERIC`)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT POWER(2, 10) AS p;
-- Result: 1024
```

### Function: SQRT
**Description:**  
Square root.

**Inputs:** 
- `x` (`NUMERIC` >= 0)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT SQRT(16) AS s;
-- Result: 4
```

### Function: LOG (Alias: LN)
**Description:**  
Natural logarithm.

**Inputs:** 
- `x` (`NUMERIC` > 0)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT LOG(EXP(1)) AS l;
-- Result: 1
```

### Function: LOG10
**Description:**  
Base-10 logarithm.

**Inputs:** 
- `x` (`NUMERIC` > 0)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT LOG10(1000) AS l10;
-- Result: 3
```

### Function: EXP
**Description:**  
e^x.

**Inputs:** 
- `x` (`NUMERIC`)

**Output:** 
- `NUMERIC`

**Example:**
```sql
SELECT EXP(1) AS e;
-- Result: 2.71828...
```

### Function: SIGN (Alias SGN)
**Description:**  
Returns -1, 0, or 1 according to sign.

**Inputs:** 
- `x` (`NUMERIC`)

**Output:** 
- `TINYINT`

**Example:**
```sql
SELECT SIGN(-10) AS s;
-- Result: -1
```

### Trigonometric functions: COS, ACOS, SIN, ASIN, TAN, ATAN, ATAN2
**Description:**  
Standard trigonometric functions. Inputs in radians.

**Inputs:** 
- `x` or (`y`, `x` for ATAN2)

**Output:** 
- `DOUBLE`

**Example:**
```sql
SELECT COS(PI()/3) AS c;
-- Result: 0.5
```

[Back to index](./README.md)
