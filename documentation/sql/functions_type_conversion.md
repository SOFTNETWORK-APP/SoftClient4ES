[Back to index](README.md)
## Type Conversion Functions

---

### CAST / CONVERT

Cast expression to a target SQL type.

**Syntax:**
```sql
CAST(expr AS TYPE)
CONVERT(expr, TYPE)
```

**Inputs:**
- `expr` - Expression to convert
- `TYPE` - Target data type:
  - `VARCHAR` / `STRING` / `CHAR` / `TEXT` / `KEYWORD`
  - `INT` / `INTEGER` / `BIGINT` / `SMALLINT` / `TINYINT` / `SIGNED` / `UNSIGNED`
  - `DOUBLE` / `FLOAT` / `REAL` / `DECIMAL` / `NUMERIC` / `DEC`
  - `BOOLEAN`
  - `DATE`
  - `TIMESTAMP` / `DATETIME`
  - `TIME`
  - `BINARY` / `VARBINARY`

A length, precision or scale may be written on any type SQL parameterises — `CHAR(10)`,
`VARCHAR(255)`, `DECIMAL(10,2)`, `INT(11)`, `TIMESTAMP(3)` — and is **accepted and ignored**.

Three limitations, stated plainly rather than hidden:

> - `SIGNED` and `UNSIGNED` are both 64-bit **signed** integers (`BIGINT`). Elasticsearch's
>   `unsigned_long` is not addressable from a cast, so a value above `Long.MAX_VALUE` is not
>   representable.
> - `DECIMAL` / `NUMERIC` / `DEC` are **approximate**: they map to `DOUBLE`. Elasticsearch has no
>   exact decimal type.
> - A precision or length is **ignored** — no rounding, no truncation. Nothing pretends otherwise:
>   the statement re-renders **without** it (`CAST(x AS DECIMAL(10,2))` becomes
>   `CAST(x AS DOUBLE)`, `CHAR(10)` becomes `CHAR`), so every artifact that echoes a statement —
>   `SHOW CREATE TABLE`, a materialized view's `.sql`, a log line — shows the type the engine
>   actually applied.
>
> `BOOL` is still not a cast target; write `BOOLEAN`. `BLOB`, `CLOB`, `BIT`, `MONEY`, `UUID` and
> `INTERVAL` as a type are rejected.

`CONVERT(expr USING <charset>)` is accepted as a synonym for `CONVERT(expr, VARCHAR)`:
Elasticsearch stores UTF-8 throughout, so the charset is parsed and dropped.

> ⚠️ A cast whose operand is a **column** does not yet emit a conversion — the executing query
> carries no schema, so the engine cannot see the column's type and the value is returned as
> stored. Casts over **literals** convert as documented. Cast the literal, or convert in the
> client, until this is addressed.

**Output:**
- Value converted to target `TYPE`

**Behavior:**
- Throws error if conversion fails
- Use `TRY_CAST` for safe conversion
- A cast cannot wrap an **aggregate result** — cast the aggregate's input instead
  (`MAX(salary::BIGINT)`, not `MAX(salary)::BIGINT`). See
  [Type Conversion](type_conversion.md#restriction-cast-the-input-of-an-aggregate-not-its-result).

**Examples:**

**Numeric Conversions:**
```sql
-- Convert to DOUBLE. WARNING - COLUMN operand: see the limitation above. The cast is accepted and
-- the value comes back AS STORED, not converted. Cast a literal, or convert in the client.
SELECT CAST(salary AS DOUBLE) AS s FROM emp;
-- Result: 12345 (the stored value, unconverted)

-- Integer to DOUBLE
SELECT CAST(100 AS DOUBLE) AS d;
-- Result: 100.0

-- String to INT
SELECT CAST('123' AS INT) AS i;
-- Result: 123

-- String to DOUBLE
SELECT CAST('123.45' AS DOUBLE) AS d;
-- Result: 123.45

-- DOUBLE to INT (truncates toward zero)
SELECT CAST(123.99 AS INT) AS i;
-- Result: 123

-- Integer to a NARROWER integer: the high-order bits are discarded, i.e. it WRAPS. It does not
-- clamp to the target's range and it does not raise - Java/Painless cast semantics. Engines that
-- clamp, or reject, an out-of-range narrowing will disagree.
SELECT CAST(300 AS TINYINT) AS b;
-- Result: 44

-- Using CONVERT alias. WARNING - COLUMN operand, same limitation as above.
SELECT CONVERT(salary, DOUBLE) AS s FROM emp;
-- Result: 12345 (the stored value, unconverted)
```

**String Conversions:**
```sql
-- Number to VARCHAR
SELECT CAST(12345 AS VARCHAR) AS str;
-- Result: '12345'

-- DOUBLE to VARCHAR
SELECT CAST(123.45 AS VARCHAR) AS str;
-- Result: '123.45'

-- Boolean to VARCHAR
SELECT CAST(true AS VARCHAR) AS str;
-- Result: 'true'

-- Date to VARCHAR
SELECT CAST(CURRENT_DATE AS VARCHAR) AS date_str;
-- Result: '2025-10-27'

-- Timestamp to VARCHAR
SELECT CAST(CURRENT_TIMESTAMP AS VARCHAR) AS ts_str;
-- Result: '2025-10-27 16:10:21'
```

**Date and Time Conversions:**
```sql
-- String to DATE
SELECT CAST('2025-01-10' AS DATE) AS d;
-- Result: 2025-01-10

-- String to TIMESTAMP. WARNING - the conversion is pinned to ISO_ZONED_DATE_TIME, so a
-- SPACE-separated timestamp with no zone RAISES. Write it in ISO form, or use DATETIME_PARSE.
SELECT CAST('2025-01-10T14:30:00Z' AS TIMESTAMP) AS ts;
-- Result: 2025-01-10T14:30:00Z

-- Timestamp to DATE
SELECT CAST(CURRENT_TIMESTAMP AS DATE) AS d;
-- Result: 2025-10-27

-- WARNING - a slash-separated date does NOT convert: the DATE conversion is pinned to the pattern
-- yyyy-MM-dd, so this RAISES rather than returning a date. Rewrite the value in ISO form.
-- SELECT CAST('2025/01/10' AS DATE) AS d;   -- error

-- Epoch MILLISECONDS to TIMESTAMP. WARNING - the operand is read as milliseconds, not seconds, so
-- a seconds-precision epoch lands in 1970. Multiply by 1000, or use a millisecond epoch.
SELECT CAST(1704902400000 AS TIMESTAMP) AS ts;
-- Result: 2024-01-10 12:00:00
```

**Boolean Conversions:**

> WARNING - a conversion TO `BOOLEAN` is currently a no-op: the value is returned unchanged
> (`CAST(1 AS BOOLEAN)` yields `1`, `CAST('true' AS BOOLEAN)` yields the string `'true'`). Only the
> conversions FROM boolean below are applied. Use a comparison (`col = 1`) instead.

```sql
-- Number to BOOLEAN - NOT APPLIED, returns the number unchanged
SELECT CAST(1 AS BOOLEAN) AS b;
-- Result: 1

SELECT CAST(0 AS BOOLEAN) AS b;
-- Result: 0

-- String to BOOLEAN - NOT APPLIED, returns the string unchanged
SELECT CAST('true' AS BOOLEAN) AS b;
-- Result: 'true'

SELECT CAST('false' AS BOOLEAN) AS b;
-- Result: 'false'

-- Boolean to INT
SELECT CAST(true AS INT) AS i;
-- Result: 1

SELECT CAST(false AS INT) AS i;
-- Result: 0
```

**Decimal/Numeric Conversions:**

> The precision and scale are parsed and IGNORED - there is no rounding and no padding, and the
> target is `DOUBLE`. The statement re-renders as `CAST(x AS DOUBLE)` so what the engine applied is
> always visible.

```sql
-- The scale is NOT applied: no rounding to 2 decimal places
SELECT CAST(123.456 AS DECIMAL(10, 2)) AS dec;
-- Result: 123.456

-- String to DECIMAL
SELECT CAST('123.456' AS DECIMAL(10, 3)) AS dec;
-- Result: 123.456

-- INT to DECIMAL: widened to a double, NOT padded to the scale
SELECT CAST(100 AS DECIMAL(10, 2)) AS dec;
-- Result: 100.0
```

**Practical Examples:**

**1. Normalize data types in queries:**
```sql
-- Ensure consistent numeric types
SELECT 
  product_id,
  CAST(price AS DECIMAL(10, 2)) AS price,
  CAST(quantity AS INT) AS quantity
FROM products;
```

**2. Convert for calculations:**
```sql
-- Avoid integer division
SELECT 
  order_id,
  total_items,
  total_price,
  CAST(total_price AS DOUBLE) / CAST(total_items AS DOUBLE) AS avg_price
FROM orders;
```

**3. Format output:**
```sql
-- Convert numbers to strings for display
SELECT 
  product_id,
  CONCAT('$', CAST(price AS VARCHAR)) AS formatted_price,
  CONCAT(CAST(quantity AS VARCHAR), ' units') AS stock_info
FROM products;
```

**4. Date string parsing:**
```sql
-- Parse date strings from different formats
SELECT 
  order_id,
  order_date_str,
  CAST(order_date_str AS DATE) AS order_date
FROM orders
WHERE CAST(order_date_str AS DATE) >= '2025-01-01';
```

**5. Type conversion in JOIN conditions:**
```sql
-- Convert types for joining
SELECT 
  o.order_id,
  p.product_name
FROM orders o
JOIN products p ON CAST(o.product_id AS VARCHAR) = p.product_code;
```

**6. Boolean logic:**
```sql
-- Convert flags to boolean
SELECT 
  user_id,
  username,
  CAST(is_active AS BOOLEAN) AS active,
  CAST(is_verified AS BOOLEAN) AS verified
FROM users
WHERE CAST(is_active AS BOOLEAN) = true;
```

**Error Cases (will throw errors):**
```sql
-- Invalid string to INT
SELECT CAST('not-a-number' AS INT);
-- ERROR: Cannot cast 'not-a-number' to INT

-- Invalid date format
SELECT CAST('invalid-date' AS DATE);
-- ERROR: Cannot parse date

-- Overflow
SELECT CAST(999999999999999999 AS TINYINT);
-- ERROR: Value out of range
```

---

### TRY_CAST / SAFE_CAST

Attempt a cast and return NULL on failure (safer alternative).

**Syntax:**
```sql
TRY_CAST(expr AS TYPE)
SAFE_CAST(expr AS TYPE)
```

**Inputs:**
- `expr` - Expression to convert
- `TYPE` - Target data type (same as CAST)

**Output:**
- Value converted to target `TYPE`, or `NULL` if conversion fails

**Behavior:**
- Returns `NULL` instead of throwing error on failure
- Useful for handling dirty/inconsistent data
- Safe for data validation and cleaning

**Examples:**

**Safe Numeric Conversions:**
```sql
-- Invalid string to INT returns NULL
SELECT TRY_CAST('not-a-number' AS INT) AS maybe_null;
-- Result: NULL

-- Valid conversion works normally
SELECT TRY_CAST('123' AS INT) AS valid;
-- Result: 123

-- Using SAFE_CAST alias
SELECT SAFE_CAST('invalid' AS DOUBLE) AS result;
-- Result: NULL

-- Mixed valid/invalid data
SELECT 
  value,
  TRY_CAST(value AS INT) AS parsed_value
FROM (
  SELECT '123' AS value
  UNION ALL SELECT 'abc'
  UNION ALL SELECT '456'
) AS data;
-- Results:
-- '123' -> 123
-- 'abc' -> NULL
-- '456' -> 456
```

**Safe Date Conversions:**
```sql
-- Invalid date returns NULL
SELECT TRY_CAST('invalid-date' AS DATE) AS d;
-- Result: NULL

-- Valid date works
SELECT TRY_CAST('2025-01-10' AS DATE) AS d;
-- Result: 2025-01-10

-- Handle multiple date formats
SELECT 
  date_str,
  TRY_CAST(date_str AS DATE) AS parsed_date
FROM dates_table;
```

**Safe Boolean Conversions:**
```sql
-- Invalid boolean returns NULL
SELECT TRY_CAST('maybe' AS BOOLEAN) AS b;
-- Result: NULL

-- Valid values work
SELECT TRY_CAST('true' AS BOOLEAN) AS b1,
       TRY_CAST('false' AS BOOLEAN) AS b2,
       TRY_CAST('1' AS BOOLEAN) AS b3;
-- Results: true, false, true
```

**Practical Examples:**

**1. Data validation and cleaning:**
```sql
-- Identify and filter invalid data
SELECT 
  product_id,
  price_str,
  TRY_CAST(price_str AS DOUBLE) AS price,
  CASE 
    WHEN TRY_CAST(price_str AS DOUBLE) IS NULL 
    THEN 'Invalid'
    ELSE 'Valid'
  END AS validation_status
FROM raw_products;
```

**2. Handle missing or corrupt data:**
```sql
-- Use COALESCE with TRY_CAST for defaults
SELECT 
  order_id,
  COALESCE(TRY_CAST(quantity_str AS INT), 0) AS quantity,
  COALESCE(TRY_CAST(price_str AS DOUBLE), 0.0) AS price
FROM orders_import;
```

**3. Filter out invalid records:**
```sql
-- Only process valid conversions
SELECT 
  user_id,
  signup_date_str,
  TRY_CAST(signup_date_str AS DATE) AS signup_date
FROM users_raw
WHERE TRY_CAST(signup_date_str AS DATE) IS NOT NULL;
```

**4. Data quality reporting:**
```sql
-- Count valid vs invalid conversions
SELECT 
  COUNT(*) AS total_records,
  COUNT(TRY_CAST(price AS DOUBLE)) AS valid_prices,
  COUNT(*) - COUNT(TRY_CAST(price AS DOUBLE)) AS invalid_prices,
  ROUND(
    COUNT(TRY_CAST(price AS DOUBLE)) * 100.0 / COUNT(*), 
    2
  ) AS valid_percentage
FROM products_import;
```

**5. Safe type conversion in calculations:**
```sql
-- Avoid errors in calculations
SELECT 
  product_id,
  name,
  TRY_CAST(price AS DOUBLE) * TRY_CAST(quantity AS INT) AS total_value
FROM products
WHERE TRY_CAST(price AS DOUBLE) IS NOT NULL
  AND TRY_CAST(quantity AS INT) IS NOT NULL;
```

**6. Multiple conversion attempts:**
```sql
-- Try multiple formats
SELECT 
  date_field,
  COALESCE(
    TRY_CAST(date_field AS DATE),
    TRY_CAST(REPLACE(date_field, '/', '-') AS DATE),
    TRY_CAST(CONCAT(date_field, '-01') AS DATE)
  ) AS parsed_date
FROM date_strings;
```

**7. ETL data validation:**
```sql
-- Validate imported data before processing
SELECT 
  row_id,
  customer_id,
  order_date,
  amount,
  CASE
    WHEN TRY_CAST(customer_id AS INT) IS NULL 
      THEN 'Invalid customer_id'
    WHEN TRY_CAST(order_date AS DATE) IS NULL 
      THEN 'Invalid order_date'
    WHEN TRY_CAST(amount AS DOUBLE) IS NULL 
      THEN 'Invalid amount'
    ELSE 'Valid'
  END AS validation_error
FROM staging_orders
WHERE TRY_CAST(customer_id AS INT) IS NULL
   OR TRY_CAST(order_date AS DATE) IS NULL
   OR TRY_CAST(amount AS DOUBLE) IS NULL;
```

**8. Safe aggregations:**
```sql
-- Aggregate only valid numeric values
SELECT 
  category,
  COUNT(*) AS total_products,
  COUNT(TRY_CAST(price AS DOUBLE)) AS products_with_valid_price,
  AVG(TRY_CAST(price AS DOUBLE)) AS avg_price,
  SUM(TRY_CAST(price AS DOUBLE)) AS total_value
FROM products_raw
GROUP BY category;
```

**9. Conditional conversion:**
```sql
-- Apply different conversions based on conditions
SELECT 
  field_name,
  field_value,
  CASE 
    WHEN field_name = 'age' 
      THEN TRY_CAST(field_value AS INT)
    WHEN field_name = 'salary' 
      THEN TRY_CAST(field_value AS DOUBLE)
    WHEN field_name = 'hire_date' 
      THEN TRY_CAST(field_value AS DATE)
    ELSE field_value
  END AS converted_value
FROM dynamic_fields;
```

**10. Data migration with validation:**
```sql
-- Migrate data with quality checks
INSERT INTO products_clean (
  product_id,
  name,
  price,
  quantity,
  created_date
)
SELECT 
  product_id,
  name,
  TRY_CAST(price_str AS DECIMAL(10, 2)) AS price,
  TRY_CAST(quantity_str AS INT) AS quantity,
  TRY_CAST(created_date_str AS DATE) AS created_date
FROM products_staging
WHERE TRY_CAST(price_str AS DECIMAL(10, 2)) IS NOT NULL
  AND TRY_CAST(quantity_str AS INT) IS NOT NULL
  AND TRY_CAST(created_date_str AS DATE) IS NOT NULL;
```

---

### CAST vs TRY_CAST Comparison

| Feature               | CAST                  | TRY_CAST              |
|-----------------------|-----------------------|-----------------------|
| On conversion failure | Throws error          | Returns NULL          |
| Use case              | Clean, validated data | Dirty, uncertain data |
| Performance           | Slightly faster       | Slightly slower       |
| Data validation       | Manual required       | Built-in              |
| Error handling        | Must use try-catch    | Automatic             |
| NULL handling         | Propagates NULL       | Propagates NULL       |

**When to use CAST:**
- Data is already validated
- You want to catch errors explicitly
- Performance is critical
- Conversion should never fail

**When to use TRY_CAST:**
- Working with user input
- Importing external data
- Data quality is uncertain
- Need graceful error handling
- ETL/data cleaning operations
- Want to filter invalid data

---

### Common Type Conversion Patterns

**1. String to Number (safe):**
```sql
SELECT 
  COALESCE(TRY_CAST(value AS INT), 0) AS safe_int,
  COALESCE(TRY_CAST(value AS DOUBLE), 0.0) AS safe_double
FROM data_table;
```

**2. Number to String (formatted):**
```sql
SELECT 
  CONCAT('$', CAST(ROUND(price, 2) AS VARCHAR)) AS formatted_price
FROM products;
```

**3. Date String Parsing (flexible):**
```sql
SELECT 
  COALESCE(
    TRY_CAST(date_str AS DATE),
    TRY_CAST(REPLACE(date_str, '/', '-') AS DATE)
  ) AS parsed_date
FROM dates;
```

**4. Boolean Flags:**
```sql
SELECT 
  CAST(CASE 
    WHEN status = 'active' THEN 1 
    ELSE 0 
  END AS BOOLEAN) AS is_active
FROM users;
```

**5. Type-safe Calculations:**
```sql
SELECT 
  CAST(numerator AS DOUBLE) / CAST(denominator AS DOUBLE) AS ratio
FROM calculations
WHERE denominator != 0;
```

---

### Type Conversion Summary Table

| From Type  | To Type   | CAST Example                  | Notes                           |
|------------|-----------|-------------------------------|---------------------------------|
| VARCHAR    | INT       | `CAST('123' AS INT)`          | Must be valid integer string    |
| VARCHAR    | DOUBLE    | `CAST('123.45' AS DOUBLE)`    | Must be valid number string     |
| VARCHAR    | DATE      | `CAST('2025-01-10' AS DATE)`  | Must be valid date format       |
| VARCHAR    | BOOLEAN   | `CAST('true' AS BOOLEAN)`     | Accepts 'true'/'false', '1'/'0' |
| INT        | VARCHAR   | `CAST(123 AS VARCHAR)`        | Always succeeds                 |
| INT        | DOUBLE    | `CAST(123 AS DOUBLE)`         | Always succeeds                 |
| INT        | BOOLEAN   | `CAST(1 AS BOOLEAN)`          | 0=false, non-zero=true          |
| DOUBLE     | INT       | `CAST(123.99 AS INT)`         | Truncates decimal               |
| DOUBLE     | VARCHAR   | `CAST(123.45 AS VARCHAR)`     | Always succeeds                 |
| DATE       | VARCHAR   | `CAST(date_col AS VARCHAR)`   | Format: 'YYYY-MM-DD'            |
| DATE       | TIMESTAMP | `CAST(date_col AS TIMESTAMP)` | Time set to 00:00:00            |
| TIMESTAMP  | DATE      | `CAST(ts_col AS DATE)`        | Drops time component            |
| TIMESTAMP  | VARCHAR   | `CAST(ts_col AS VARCHAR)`     | Format: 'YYYY-MM-DD HH:MI:SS'   |
| BOOLEAN    | INT       | `CAST(true AS INT)`           | true=1, false=0                 |
| BOOLEAN    | VARCHAR   | `CAST(true AS VARCHAR)`       | 'true' or 'false'               |

---

### Best Practices

**1. Use TRY_CAST for user input:**
```sql
-- Good
SELECT TRY_CAST(user_input AS INT) FROM form_data;

-- Avoid
SELECT CAST(user_input AS INT) FROM form_data;  -- May fail
```

**2. Validate before CAST:**
```sql
-- Good
SELECT CAST(price AS DOUBLE)
FROM products
WHERE price IS NOT NULL 
  AND price REGEXP '^[0-9]+\\.?[0-9]*$';
```

**3. Provide defaults for failed conversions:**
```sql
-- Good
SELECT COALESCE(TRY_CAST(value AS INT), -1) AS safe_value
FROM data_table;
```

**4. Use appropriate precision for DECIMAL:**
```sql
-- Good
SELECT CAST(price AS DECIMAL(10, 2))  -- 10 digits, 2 decimal places
FROM products;

-- Avoid
SELECT CAST(price AS DECIMAL(5, 2))  -- May overflow
```

**5. Document conversion logic:**
```sql
-- Good: Clear intent
SELECT 
  order_id,
  -- Convert string price to numeric for calculations
  CAST(price_str AS DECIMAL(10, 2)) AS price
FROM orders;
```

[Back to index](README.md)
