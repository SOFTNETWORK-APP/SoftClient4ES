[Back to index](README.md)

# Type Conversion Functions and Operators

## Function: CAST (Alias: CONVERT)

**Description:**  
Converts a value to a specified SQL type. Fails if the conversion is invalid.

**Inputs:**  
- `value` (ANY type)  
- `targetType` — see the target-type list in
  [Type Conversion Functions](functions_type_conversion.md#cast--convert), which is the single
  authoritative list. It covers the character, numeric, temporal and binary families, the MySQL
  spellings `SIGNED` / `UNSIGNED` and `DECIMAL` / `NUMERIC` / `DEC`, and the parameterised forms
  (`CHAR(10)`, `DECIMAL(10,2)`, `INT(11)`) whose parameters are accepted and ignored.

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
- *Since `0.25.0`.* A `BOOLEAN` cannot be cast to a `DATE`, a `TIME` or a `TIMESTAMP` — in a query,
  a computed column and a pipeline alike, with `CAST`, `TRY_CAST` or `::` — as DuckDB refuses it:
  `CAST(b AS DATE)` used to answer the boolean itself in a date column.
- *Since `0.25.0`.* A comparison between values of different types reads them as DuckDB casts them
  (see [Comparing values of different types](operators.md#comparing-values-of-different-types)): a text compared with a
  number is read as one, rounding half away from zero for a whole type. `CAST(k AS BIGINT)` keeps
  its strict parse (`'1.5'` fails the cast).
- *Since `0.25.0`.* A string function (`UPPER`, `LOWER`, `TRIM`, `LENGTH`, `SUBSTRING`, `LEFT`,
  `RIGHT`, `REPLACE`, `REVERSE`) over a number or a boolean reads its text: `UPPER(n)` and
  `LENGTH(n)` answer (they failed the query), and over a `CASE` they read the whole `CASE`'s value.
  The same holds in a computed column (`SCRIPT AS`), whose pipeline `ALTER TABLE` re-creates (see
  [Conditional functions](functions_conditional.md)).
- *Since `0.25.0`.* A text that spells a number past the `DOUBLE` range (`'1e400'`, `'-1e400'`) is
  read in two ways, as `0.24.0`'s literal rule and DuckDB read it. As a quoted literal it reads as no
  number, like `'NaN'` and `'Infinity'`: `COALESCE(x, '1e400')` answers the text and
  `GREATEST(x, '1e400')` is refused (DuckDB answers `1.5` and `inf`). As a stored text compared with
  a `DOUBLE` column it is `+Infinity` or `-Infinity`, DuckDB's value: `k = x` is false and `k <> x`
  true. A stored `'NaN'` or `'Infinity'` fails the query.

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

Cast the aggregate's **input** instead. This is equivalent for `MIN` and `MAX`, whose result type
follows their argument:

```sql
-- Both supported
SELECT MAX(salary::BIGINT) AS m FROM emp GROUP BY dept;
SELECT MAX(CAST(salary AS BIGINT)) AS m FROM emp GROUP BY dept;
```

`COUNT`, `SUM`, `AVG`, the `STDDEV` / `VARIANCE` family and the percentiles produce a type of their
own (see [Aggregate functions](functions_aggregate.md#overview)): `COUNT` is a `BIGINT`; `AVG`, the
`STDDEV` / `VARIANCE` family and the percentiles are `DOUBLE`, whatever their input; `SUM` is a
`BIGINT` over an input of any whole-number type and a `DOUBLE` over a `REAL` or a `DOUBLE` (*since
`0.25.0`*: it kept the summed column's type, which overflowed), so `SUM(CAST(x AS INT))` is a
`BIGINT`, not an `INT`. For these, casting the input converts each value before it is aggregated,
which is not the same as converting the result.

Casting is unrestricted everywhere an aggregate is not involved — the SELECT list, `WHERE`,
`ORDER BY`, and inside another function:

```sql
SELECT YEAR(createdAt)::VARCHAR AS y FROM logs;
SELECT id FROM logs WHERE tries::INT > 3 ORDER BY tries::INT;
```

[Back to index](README.md)
