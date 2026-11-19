[Back to index](README.md)

## Mathematical Functions

**Navigation:** [Aggregate Functions](functions_aggregate.md) · [String Functions](functions_string.md)

---

### Basic Arithmetic Functions

#### ABS

Absolute value.

**Syntax:**
```sql
ABS(x)
```

**Inputs:**
- `x` - `NUMERIC`

**Output:**
- The type of `x`, as PostgreSQL's `abs`: `ABS` of an `INT` is an `INT` (a whole number), of a
  `DOUBLE` a `DOUBLE`. Over a column of no known type it is `ANY`, over `NULL` a `BIGINT`, over a
  text, a boolean or a date a `DOUBLE`, the class its script answers (*since `0.25.0`*: `NUMERIC`)

**Examples:**
```sql
-- Absolute value of negative number
SELECT ABS(-5) AS a;
-- Result: 5

-- Absolute value of positive number
SELECT ABS(5) AS a;
-- Result: 5

-- Absolute value of zero
SELECT ABS(0) AS a;
-- Result: 0

-- With decimals
SELECT ABS(-123.456) AS a;
-- Result: 123.456

-- In WHERE clause
SELECT * FROM transactions
WHERE ABS(amount) > 1000;

-- Absolute difference, over price_gap DOUBLE SCRIPT AS (estimated_price - actual_price): an
-- arithmetic expression is not accepted as a function argument
SELECT
  order_id,
  ABS(price_gap) AS price_difference
FROM orders;
```

---

#### SIGN

Returns -1, 0, or 1 according to sign.

**Syntax:**
```sql
SIGN(x)
```

**Inputs:**
- `x` - `NUMERIC`

**Output:**
- `TINYINT` (-1, 0, or 1)

**Examples:**
```sql
-- Sign of negative number
SELECT SIGN(-10) AS s;
-- Result: -1

-- Sign of positive number
SELECT SIGN(10) AS s;
-- Result: 1

-- Sign of zero
SELECT SIGN(0) AS s;
-- Result: 0

-- A fractional value
SELECT SIGN(-5.5) AS s;
-- Result: -1

-- Classify values
SELECT 
  transaction_id,
  amount,
  CASE SIGN(amount)
    WHEN 1 THEN 'Credit'
    WHEN -1 THEN 'Debit'
    ELSE 'Zero'
  END AS transaction_type
FROM transactions;

-- Count positive and negative values
SELECT 
  COUNT(CASE WHEN SIGN(balance) = 1 THEN 1 END) AS positive_count,
  COUNT(CASE WHEN SIGN(balance) = -1 THEN 1 END) AS negative_count,
  COUNT(CASE WHEN SIGN(balance) = 0 THEN 1 END) AS zero_count
FROM accounts;
```

---

### Rounding Functions

#### ROUND

Round to n decimals (optional).

**Syntax:**
```sql
ROUND(x)
ROUND(x, n)
```

**Inputs:**
- `x` - `NUMERIC`
- `n` (optional) - `INT` (number of decimal places, default: 0)

**Output:**
- the type of `x`, as DuckDB types it: a `TINYINT`, `SMALLINT`, `INT` or `BIGINT` operand gives
  that type, a `REAL` a `REAL`, a `DOUBLE` (or a `DECIMAL` cast, a `DOUBLE` here) a `DOUBLE`; an
  operand that is a column of no known type gives `ANY`, `NULL` a `BIGINT`, and a text, a boolean,
  a date or a time the class its script answers, a `BIGINT`, with an `n` other than 0 a `DOUBLE` (*since
  `0.25.0`*: `NUMERIC` for every operand). No result column reports `NUMERIC` (`describe`, JDBC,
  Arrow): an operand known to be a number, but not which one, gives `ANY` there.
  To a negative `n` a `TINYINT`, `SMALLINT` or `INT` operand gives a `BIGINT`, which its rounded
  value always fits (`ROUND(CAST(2147483647 AS INT), -1)` is the `BIGINT` `2147483650`), where
  DuckDB keeps the operand's type and wraps the value. A `GROUP BY ROUND(n)` key over an integer
  column is an integer, a `CREATE TABLE … AS SELECT` maps the column by that type, and
  `SUM(ROUND(n))` over an integer column is a `BIGINT`.

A half is rounded away from zero, as DuckDB rounds: `ROUND(2.5)` is `3`, `ROUND(-2.5)` is `-3`,
`ROUND(-1.25, 1)` is `-1.3`. With decimal places the value is DuckDB's `round(x * 10^n) / 10^n`
(`ROUND(2.46, 1)` is `2.5`), in a `SELECT`, a `WHERE`, a `GROUP BY` key and its `HAVING` and a
computed column alike. `ROUND` of `NULL` is `NULL`. Over a `REAL` (`float`) operand with decimal
places the rounding is computed in single precision, as DuckDB rounds a `FLOAT`: `ROUND(r, 1)` of
the float `7.75` is the float `7.8`, read back as `7.800000190734863` (*since `0.24.0`*).

To tens, hundreds, … (a negative `n`) the value is DuckDB's `round(x / 10^-n) * 10^-n`, exact for
every whole number: `ROUND(423789293, -5)` is `423800000` (*since `0.25.0`*: `423799999` over an
`INT` column). In a query, a `BIGINT` operand whose rounded value leaves the `BIGINT` range (the
`BIGINT` `9223372036854775807` rounded to tens) fails the statement with *"Overflow in ROUND of
integer"*, where DuckDB 1.5.5 wraps it.

**Examples:**
```sql
-- Round to 2 decimals
SELECT ROUND(123.456, 2) AS r;
-- Result: 123.46

-- Round to nearest integer
SELECT ROUND(123.456) AS r;
-- Result: 123

-- Round to 1 decimal
SELECT ROUND(123.456, 1) AS r;
-- Result: 123.5

-- Round negative number
SELECT ROUND(-123.456, 2) AS r;
-- Result: -123.46

-- Round to tens place (negative decimals)
SELECT ROUND(123.456, -1) AS r;
-- Result: 120.0 (a DOUBLE operand gives a DOUBLE)

-- Round prices
SELECT
  product_id,
  price,
  ROUND(price, 1) AS rounded_price
FROM products;
```

---

#### FLOOR

Greatest `BIGINT` ≤ x (round down).

**Syntax:**
```sql
FLOOR(x)
```

**Inputs:**
- `x` - `NUMERIC`

**Output:**
- `BIGINT`

**Examples:**
```sql
-- Floor of positive number
SELECT FLOOR(3.9) AS f;
-- Result: 3

-- Floor of negative number
SELECT FLOOR(-3.1) AS f;
-- Result: -4

-- Floor of integer
SELECT FLOOR(5) AS f;
-- Result: 5

-- Floor with decimals
SELECT FLOOR(123.999) AS f;
-- Result: 123
```

---

#### CEIL / CEILING

Smallest `BIGINT` ≥ x (round up).

**Syntax:**
```sql
CEIL(x)
CEILING(x)
```

**Inputs:**
- `x` - `NUMERIC`

**Output:**
- `BIGINT`

**Examples:**
```sql
-- Ceiling of positive number
SELECT CEIL(3.1) AS c;
-- Result: 4

-- Ceiling of negative number
SELECT CEIL(-3.9) AS c;
-- Result: -3

-- Using CEILING alias
SELECT CEILING(2.001) AS c;
-- Result: 3

-- Ceiling of integer
SELECT CEIL(5) AS c;
-- Result: 5

-- Round up prices
SELECT 
  product_id,
  CEIL(price) AS rounded_price
FROM products;
```

---

### Power and Root Functions

#### POWER / POW

x raised to the power of y (x^y).

**Syntax:**
```sql
POWER(x, y)
POW(x, y)
```

**Inputs:**
- `x` - `NUMERIC` (base)
- `y` - `NUMERIC` (exponent)

**Output:**
- `DOUBLE`, whatever `x`, as DuckDB types it (*since `0.25.0`*: `NUMERIC`, which a
  `CREATE TABLE … AS SELECT` could not map)

**Examples:**
```sql
-- 2 to the power of 10
SELECT POWER(2, 10) AS p;
-- Result: 1024

-- Using POW alias
SELECT POW(3, 4) AS p;
-- Result: 81

-- Square
SELECT POWER(5, 2) AS square;
-- Result: 25

-- Cube
SELECT POWER(3, 3) AS cube;
-- Result: 27

-- Negative exponent
SELECT POWER(2, -3) AS p;
-- Result: 0.125 (1/8)
```

---

#### SQRT

Square root.

**Syntax:**
```sql
SQRT(x)
```

**Inputs:**
- `x` - `NUMERIC` (≥ 0)

**Output:**
- `DOUBLE`, whatever `x`, as DuckDB types it (*since `0.25.0`*: `NUMERIC`, which a
  `CREATE TABLE … AS SELECT` could not map)

**Examples:**
```sql
-- Square root of 16
SELECT SQRT(16) AS s;
-- Result: 4

-- Square root of 2
SELECT SQRT(2) AS s;
-- Result: 1.414213562...

-- Square root of 0
SELECT SQRT(0) AS s;
-- Result: 0

-- Standard deviation per category: SQRT(AVG(POWER(price - avg_price, 2))) does not parse (an
-- arithmetic expression is not accepted as a function argument), so use the aggregate itself
SELECT category, STDDEV_POP(price) AS std_dev
FROM products
GROUP BY category;
```

---

### Logarithmic and Exponential Functions

> **Outside their domain these functions fail the statement**, as DuckDB does (*since `0.24.0`*):
> `LOG` / `LOG10` of zero (*"cannot take logarithm of zero"*) or of a negative number (*"cannot take
> logarithm of a negative number"*), `SQRT` of a negative number (*"cannot take square root of a
> negative number"*), and `ASIN` / `ACOS` outside `[-1, 1]` (*"ASIN is undefined outside [-1,1]"*,
> and the same for `ACOS`). The search fails with that reason and the status Elasticsearch gives a
> script error: `400` on Elasticsearch 8, `500` on Elasticsearch 6.8. Before `0.24.0` such a row
> answered `NaN` or `-Infinity`.

#### LOG

Natural logarithm (base e).

**Syntax:**
```sql
LOG(x)
```

`LOG` is the natural logarithm; there is no `LN` spelling (`LN(x)` is not a function).

**Inputs:**
- `x` - `NUMERIC` (> 0)

**Output:**
- `DOUBLE`, whatever `x`, as DuckDB types it (*since `0.25.0`*: `NUMERIC`, which a
  `CREATE TABLE … AS SELECT` could not map)

**Examples:**
```sql
-- Natural log of e
SELECT LOG(EXP(1)) AS l;
-- Result: 1

-- Natural log of 10
SELECT LOG(10) AS l;
-- Result: 2.302585...

-- Natural log of 1
SELECT LOG(1) AS l;
-- Result: 0

-- Logarithmic scale
SELECT 
  value,
  LOG(value) AS log_value
FROM measurements
WHERE value > 0;
```

---

#### LOG10

Base-10 logarithm.

**Syntax:**
```sql
LOG10(x)
```

**Inputs:**
- `x` - `NUMERIC` (> 0)

**Output:**
- `DOUBLE`, whatever `x`, as DuckDB types it (*since `0.25.0`*: `NUMERIC`, which a
  `CREATE TABLE … AS SELECT` could not map)

**Examples:**
```sql
-- Log base 10 of 1000
SELECT LOG10(1000) AS l10;
-- Result: 3

-- Log base 10 of 100
SELECT LOG10(100) AS l10;
-- Result: 2

-- Log base 10 of 10
SELECT LOG10(10) AS l10;
-- Result: 1

-- Log base 10 of 1
SELECT LOG10(1) AS l10;
-- Result: 0

-- Calculate order of magnitude
SELECT 
  value,
  FLOOR(LOG10(ABS(value))) AS magnitude
FROM measurements
WHERE value != 0;
```

---

#### EXP

Exponential function (e^x).

**Syntax:**
```sql
EXP(x)
```

**Inputs:**
- `x` - `NUMERIC`

**Output:**
- `DOUBLE`, whatever `x`, as DuckDB types it (*since `0.25.0`*: `NUMERIC`, which a
  `CREATE TABLE … AS SELECT` could not map)

**Examples:**
```sql
-- e to the power of 1
SELECT EXP(1) AS e;
-- Result: 2.718281828...

-- e to the power of 0
SELECT EXP(0) AS e;
-- Result: 1

-- e to the power of 2
SELECT EXP(2) AS e;
-- Result: 7.389056...

-- Negative exponent
SELECT EXP(-1) AS e;
-- Result: 0.367879...
```

---

### Trigonometric Functions

All trigonometric functions use **radians** as input/output units.

#### COS

Cosine function.

**Syntax:**
```sql
COS(x)
```

**Inputs:**
- `x` - `DOUBLE` (angle in radians)

**Output:**
- `DOUBLE`

**Examples:**
```sql
-- Cosine of π/3 (60 degrees)
SELECT COS(RADIANS(60)) AS c;
-- Result: 0.5000000000000001 (0.5, to floating-point rounding)

-- Cosine of 0
SELECT COS(0) AS c;
-- Result: 1

-- Cosine of π/2 (90 degrees)
SELECT COS(RADIANS(90)) AS c;
-- Result: 0 (approximately)

-- Cosine of π (180 degrees)
SELECT COS(PI) AS c;
-- Result: -1

-- Convert degrees to radians and calculate
SELECT COS(RADIANS(60)) AS c;
-- Result: 0.5000000000000001 (0.5, to floating-point rounding)
```

---

#### ACOS

Arc cosine (inverse cosine).

**Syntax:**
```sql
ACOS(x)
```

**Inputs:**
- `x` - `DOUBLE` (value between -1 and 1)

**Output:**
- `DOUBLE` (angle in radians, range [0, π])

**Examples:**
```sql
-- Arc cosine of 0.5
SELECT ACOS(0.5) AS ac;
-- Result: 1.047197... (π/3 or 60 degrees)

-- Arc cosine of 1
SELECT ACOS(1) AS ac;
-- Result: 0

-- Arc cosine of -1
SELECT ACOS(-1) AS ac;
-- Result: 3.141592... (π or 180 degrees)

-- Arc cosine of 0
SELECT ACOS(0) AS ac;
-- Result: 1.570796... (π/2 or 90 degrees)

-- Convert result to degrees
SELECT DEGREES(ACOS(0.5)) AS angle_degrees;
-- Result: 60.00000000000001 (60, to floating-point rounding)
```

---

#### SIN

Sine function.

**Syntax:**
```sql
SIN(x)
```

**Inputs:**
- `x` - `DOUBLE` (angle in radians)

**Output:**
- `DOUBLE`

**Examples:**
```sql
-- Sine of π/6 (30 degrees)
SELECT SIN(RADIANS(30)) AS s;
-- Result: 0.49999999999999994 (0.5, to floating-point rounding)

-- Sine of 0
SELECT SIN(0) AS s;
-- Result: 0

-- Sine of π/2 (90 degrees)
SELECT SIN(RADIANS(90)) AS s;
-- Result: 1

-- Sine of π (180 degrees)
SELECT SIN(PI) AS s;
-- Result: 0 (approximately)

-- Convert degrees to radians
SELECT SIN(RADIANS(30)) AS s;
-- Result: 0.49999999999999994 (0.5, to floating-point rounding)
```

---

#### ASIN

Arc sine (inverse sine).

**Syntax:**
```sql
ASIN(x)
```

**Inputs:**
- `x` - `DOUBLE` (value between -1 and 1)

**Output:**
- `DOUBLE` (angle in radians, range [-π/2, π/2])

**Examples:**
```sql
-- Arc sine of 0.5
SELECT ASIN(0.5) AS a_sin;
-- Result: 0.523598... (π/6 or 30 degrees)

-- Arc sine of 1
SELECT ASIN(1) AS a_sin;
-- Result: 1.570796... (π/2 or 90 degrees)

-- Arc sine of -1
SELECT ASIN(-1) AS a_sin;
-- Result: -1.570796... (-π/2 or -90 degrees)

-- Arc sine of 0
SELECT ASIN(0) AS a_sin;
-- Result: 0

-- Convert to degrees
SELECT DEGREES(ASIN(0.5)) AS angle_degrees;
-- Result: 30.000000000000004 (30, to floating-point rounding)
```

---

#### TAN

Tangent function.

**Syntax:**
```sql
TAN(x)
```

**Inputs:**
- `x` - `DOUBLE` (angle in radians)

**Output:**
- `DOUBLE`

**Examples:**
```sql
-- Tangent of π/4 (45 degrees)
SELECT TAN(RADIANS(45)) AS t;
-- Result: 0.9999999999999999 (1, to floating-point rounding)

-- Tangent of 0
SELECT TAN(0) AS t;
-- Result: 0

-- Tangent of π/6 (30 degrees)
SELECT TAN(RADIANS(30)) AS t;
-- Result: 0.577350... (1/√3)

-- Convert degrees to radians
SELECT TAN(RADIANS(45)) AS t;
-- Result: 0.9999999999999999 (1, to floating-point rounding)
```

---

#### ATAN

Arc tangent (inverse tangent).

**Syntax:**
```sql
ATAN(x)
```

**Inputs:**
- `x` - `DOUBLE`

**Output:**
- `DOUBLE` (angle in radians, range [-π/2, π/2])

**Examples:**
```sql
-- Arc tangent of 1
SELECT ATAN(1) AS a_tan;
-- Result: 0.785398... (π/4 or 45 degrees)

-- Arc tangent of 0
SELECT ATAN(0) AS a_tan;
-- Result: 0

-- Arc tangent of √3
SELECT ATAN(SQRT(3)) AS a_tan;
-- Result: 1.047197... (π/3 or 60 degrees)

-- Convert to degrees
SELECT DEGREES(ATAN(1)) AS angle_degrees;
-- Result: 45
```

---

#### ATAN2

Two-argument arc tangent (returns angle in correct quadrant).

**Syntax:**
```sql
ATAN2(y, x)
```

**Inputs:**
- `y` - `DOUBLE` (y-coordinate)
- `x` - `DOUBLE` (x-coordinate)

**Output:**
- `DOUBLE` (angle in radians, range [-π, π])

**Examples:**
```sql
-- Angle to point (1, 1) - 45 degrees
SELECT ATAN2(1, 1) AS angle;
-- Result: 0.785398... (π/4)

-- Angle to point (1, 0) - 90 degrees
SELECT ATAN2(1, 0) AS angle;
-- Result: 1.570796... (π/2)

-- Angle to point (0, 1) - 0 degrees
SELECT ATAN2(0, 1) AS angle;
-- Result: 0

-- Angle to point (-1, -1) - 225 degrees (3rd quadrant)
SELECT ATAN2(-1, -1) AS angle;
-- Result: -2.356194... (-3π/4)

-- Convert to degrees
SELECT DEGREES(ATAN2(1, 1)) AS angle_degrees;
-- Result: 45
```

---

### Helper Functions for Trigonometry

#### PI

Returns the value of π (pi).

**Syntax:**
```sql
PI
```

`PI` is a constant, written without parentheses: `PI()` does not parse.

**Output:**
- `DOUBLE` (3.141592653589793)

**Examples:**
```sql
-- Value of π
SELECT PI AS pi_value;
-- Result: 3.141592653589793

-- Calculate circle area
SELECT
  radius,
  PI * radius * radius AS area
FROM circles;

-- Calculate circle circumference
SELECT 
  radius,
  2 * PI * radius AS circumference
FROM circles;

-- Convert degrees to radians
SELECT 
  degrees,
  degrees * PI / 180 AS radians
FROM angles;
```

---

#### RADIANS

Convert degrees to radians.

**Syntax:**
```sql
RADIANS(degrees)
```

**Inputs:**
- `degrees` - `DOUBLE`

**Output:**
- `DOUBLE` (radians)

**Examples:**
```sql
-- Convert 180 degrees to radians
SELECT RADIANS(180) AS rad;
-- Result: 3.141592... (π)

-- Convert 90 degrees to radians
SELECT RADIANS(90) AS rad;
-- Result: 1.570796... (π/2)

-- Convert 45 degrees to radians
SELECT RADIANS(45) AS rad;
-- Result: 0.785398... (π/4)

-- Use in trigonometric functions
SELECT SIN(RADIANS(30)) AS sine_30_degrees;
-- Result: 0.49999999999999994 (0.5, to floating-point rounding)
```

---

#### DEGREES

Convert radians to degrees.

**Syntax:**
```sql
DEGREES(radians)
```

**Inputs:**
- `radians` - `DOUBLE`

**Output:**
- `DOUBLE` (degrees)

**Examples:**
```sql
-- Convert π radians to degrees
SELECT DEGREES(PI) AS deg;
-- Result: 180

-- Convert π/2 radians to degrees
SELECT DEGREES(RADIANS(90)) AS deg;
-- Result: 90

-- Convert π/4 radians to degrees
SELECT DEGREES(RADIANS(45)) AS deg;
-- Result: 45

-- Convert result of ACOS to degrees
SELECT DEGREES(ACOS(0.5)) AS angle_degrees;
-- Result: 60.00000000000001 (60, to floating-point rounding)
```

---

### Practical Mathematical Examples

**1. Standard Deviation:**
```sql
SELECT
  category,
  STDDEV_POP(value) AS std_dev
FROM measurements
GROUP BY category;
```

**2. Calculate Circle Properties:**
```sql
SELECT
  circle_id,
  radius,
  2 * PI * radius AS circumference,
  PI * radius * radius AS area
FROM circles;
```

An arithmetic expression is not accepted as a function argument (`SQRT(POWER(x, 2) + POWER(y, 2))`
does not parse), and today a `POWER` or `ROUND` result under an arithmetic operator
(`POWER(dx, 2) + POWER(dy, 2)`, `PI * POWER(radius, 2)`, `ROUND(x, 1) * 2`) and some functions nested
over a column (`DEGREES(ATAN2(y, x))`, `EXP(LOG(x))`, `FLOOR(LOG10(x))`) fail in Elasticsearch: select
the parts and combine them from the returned rows. See
[Known Limitations](known_limitations.md#arithmetic-is-not-accepted-inside-a-function-a-cast-or-a-case-branch).

---

### Mathematical Function Summary

| Function      | Description         | Example                      |
|---------------|---------------------|------------------------------|
| `ABS(x)`      | Absolute value      | `ABS(-5)` → 5                |
| `SIGN(x)`     | Sign (-1, 0, 1)     | `SIGN(-10)` → -1             |
| `ROUND(x, n)` | Round to n decimals | `ROUND(123.456, 2)` → 123.46 |
| `FLOOR(x)`    | Round down          | `FLOOR(3.9)` → 3             |
| `CEIL(x)`     | Round up            | `CEIL(3.1)` → 4              |
| `POWER(x, y)` | x^y                 | `POWER(2, 10)` → 1024        |
| `SQRT(x)`     | Square root         | `SQRT(16)` → 4               |
| `LOG(x)`      | Natural log         | `LOG(EXP(1))` → 1            |
| `LOG10(x)`    | Base-10 log         | `LOG10(1000)` → 3            |
| `EXP(x)`      | e^x                 | `EXP(1)` → 2.718...          |
| `COS(x)`      | Cosine              | `COS(RADIANS(60))` → 0.5          |
| `ACOS(x)`     | Arc cosine          | `ACOS(0.5)` → π/3            |
| `SIN(x)`      | Sine                | `SIN(RADIANS(30))` → 0.5          |
| `ASIN(x)`     | Arc sine            | `ASIN(0.5)` → π/6            |
| `TAN(x)`      | Tangent             | `TAN(RADIANS(45))` → 1            |
| `ATAN(x)`     | Arc tangent         | `ATAN(1)` → π/4              |
| `ATAN2(y, x)` | Two-arg arc tangent | `ATAN2(1, 1)` → π/4          |
| `PI`          | Value of π          | `PI` → 3.14159...            |
| `RADIANS(x)`  | Degrees to radians  | `RADIANS(180)` → π           |
| `DEGREES(x)`  | Radians to degrees  | `DEGREES(PI)` → 180        |

---

[Back to index](README.md)
