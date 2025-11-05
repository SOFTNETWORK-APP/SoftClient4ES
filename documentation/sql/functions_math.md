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
- `NUMERIC`

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

-- Calculate absolute difference
SELECT 
  order_id,
  estimated_price,
  actual_price,
  ABS(estimated_price - actual_price) AS price_difference
FROM orders;
```

---

#### SIGN / SGN

Returns -1, 0, or 1 according to sign.

**Syntax:**
```sql
SIGN(x)
SGN(x)
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

-- Using SGN alias
SELECT SGN(-5.5) AS s;
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
- `DOUBLE`

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
-- Result: 120

-- Round prices
SELECT 
  product_id,
  price,
  ROUND(price * 1.2, 2) AS price_with_tax
FROM products;

-- Round averages
SELECT 
  category,
  ROUND(AVG(price), 2) AS avg_price
FROM products
GROUP BY category;
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

-- Calculate age in complete years
SELECT 
  user_id,
  name,
  FLOOR(DATEDIFF(CURRENT_DATE, birth_date, DAY) / 365.25) AS age
FROM users;

-- Bucket values
SELECT 
  FLOOR(price / 10) * 10 AS price_bucket,
  COUNT(*) AS count
FROM products
GROUP BY price_bucket
ORDER BY price_bucket;
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

-- Calculate required packages
SELECT 
  order_id,
  total_items,
  CEIL(total_items / 10.0) AS packages_needed
FROM orders;

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
- `NUMERIC`

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

-- Fractional exponent (root)
SELECT POWER(16, 0.5) AS p;
-- Result: 4 (square root)

-- Negative exponent
SELECT POWER(2, -3) AS p;
-- Result: 0.125 (1/8)

-- Calculate compound interest
SELECT 
  initial_amount,
  ROUND(initial_amount * POWER(1 + interest_rate, years), 2) AS final_amount
FROM investments;

-- Exponential growth
SELECT 
  day,
  POWER(1.05, day) AS growth_factor
FROM generate_series(0, 10) AS day;
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
- `NUMERIC`

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

-- Calculate standard deviation component
SELECT 
  category,
  SQRT(AVG(POWER(price - avg_price, 2))) AS std_dev
FROM products
CROSS JOIN (
  SELECT AVG(price) AS avg_price FROM products
) AS avg_calc
GROUP BY category;

-- Euclidean distance (2D)
SELECT 
  SQRT(POWER(x2 - x1, 2) + POWER(y2 - y1, 2)) AS distance
FROM coordinates;

-- Calculate hypotenuse
SELECT 
  side_a,
  side_b,
  SQRT(POWER(side_a, 2) + POWER(side_b, 2)) AS hypotenuse
FROM triangles;
```

---

### Logarithmic and Exponential Functions

#### LOG / LN

Natural logarithm (base e).

**Syntax:**
```sql
LOG(x)
LN(x)
```

**Inputs:**
- `x` - `NUMERIC` (> 0)

**Output:**
- `NUMERIC`

**Examples:**
```sql
-- Natural log of e
SELECT LOG(EXP(1)) AS l;
-- Result: 1

-- Using LN alias
SELECT LN(10) AS l;
-- Result: 2.302585...

-- Natural log of 1
SELECT LOG(1) AS l;
-- Result: 0

-- Calculate log returns
SELECT 
  date,
  price,
  LOG(price / LAG(price) OVER (ORDER BY date)) AS log_return
FROM stock_prices;

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
- `NUMERIC`

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

-- Logarithmic binning
SELECT 
  FLOOR(LOG10(population)) AS log_bucket,
  COUNT(*) AS city_count
FROM cities
WHERE population > 0
GROUP BY log_bucket
ORDER BY log_bucket;
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
- `NUMERIC`

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

-- Calculate exponential growth
SELECT 
  time_period,
  initial_value * EXP(growth_rate * time_period) AS projected_value
FROM projections;

-- Inverse of natural log
SELECT 
  x,
  LOG(x) AS log_x,
  EXP(LOG(x)) AS back_to_x
FROM values_table;
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
SELECT COS(PI() / 3) AS c;
-- Result: 0.5

-- Cosine of 0
SELECT COS(0) AS c;
-- Result: 1

-- Cosine of π/2 (90 degrees)
SELECT COS(PI() / 2) AS c;
-- Result: 0 (approximately)

-- Cosine of π (180 degrees)
SELECT COS(PI()) AS c;
-- Result: -1

-- Convert degrees to radians and calculate
SELECT COS(RADIANS(60)) AS c;
-- Result: 0.5
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
-- Result: 60
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
SELECT SIN(PI() / 6) AS s;
-- Result: 0.5

-- Sine of 0
SELECT SIN(0) AS s;
-- Result: 0

-- Sine of π/2 (90 degrees)
SELECT SIN(PI() / 2) AS s;
-- Result: 1

-- Sine of π (180 degrees)
SELECT SIN(PI()) AS s;
-- Result: 0 (approximately)

-- Convert degrees to radians
SELECT SIN(RADIANS(30)) AS s;
-- Result: 0.5
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
SELECT ASIN(0.5) AS as;
-- Result: 0.523598... (π/6 or 30 degrees)

-- Arc sine of 1
SELECT ASIN(1) AS as;
-- Result: 1.570796... (π/2 or 90 degrees)

-- Arc sine of -1
SELECT ASIN(-1) AS as;
-- Result: -1.570796... (-π/2 or -90 degrees)

-- Arc sine of 0
SELECT ASIN(0) AS as;
-- Result: 0

-- Convert to degrees
SELECT DEGREES(ASIN(0.5)) AS angle_degrees;
-- Result: 30
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
SELECT TAN(PI() / 4) AS t;
-- Result: 1

-- Tangent of 0
SELECT TAN(0) AS t;
-- Result: 0

-- Tangent of π/6 (30 degrees)
SELECT TAN(PI() / 6) AS t;
-- Result: 0.577350... (1/√3)

-- Convert degrees to radians
SELECT TAN(RADIANS(45)) AS t;
-- Result: 1

-- Calculate slope
SELECT 
  rise,
  run,
  TAN(ATAN(rise / run)) AS slope_verified
FROM slopes;
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
SELECT ATAN(1) AS at;
-- Result: 0.785398... (π/4 or 45 degrees)

-- Arc tangent of 0
SELECT ATAN(0) AS at;
-- Result: 0

-- Arc tangent of √3
SELECT ATAN(SQRT(3)) AS at;
-- Result: 1.047197... (π/3 or 60 degrees)

-- Convert to degrees
SELECT DEGREES(ATAN(1)) AS angle_degrees;
-- Result: 45

-- Calculate angle from slope
SELECT 
  rise,
  run,
  DEGREES(ATAN(rise / run)) AS angle_degrees
FROM slopes;
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

-- Calculate bearing/azimuth
SELECT 
  point_id,
  DEGREES(ATAN2(delta_y, delta_x)) AS bearing_degrees
FROM coordinates;

-- Calculate angle between two points
SELECT 
  DEGREES(ATAN2(y2 - y1, x2 - x1)) AS angle
FROM point_pairs;
```

---

### Helper Functions for Trigonometry

#### PI

Returns the value of π (pi).

**Syntax:**
```sql
PI()
```

**Output:**
- `DOUBLE` (3.141592653589793)

**Examples:**
```sql
-- Value of π
SELECT PI() AS pi;
-- Result: 3.141592653589793

-- Calculate circle area
SELECT 
  radius,
  PI() * POWER(radius, 2) AS area
FROM circles;

-- Calculate circle circumference
SELECT 
  radius,
  2 * PI() * radius AS circumference
FROM circles;

-- Convert degrees to radians
SELECT 
  degrees,
  degrees * PI() / 180 AS radians
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
-- Result: 0.5
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
SELECT DEGREES(PI()) AS deg;
-- Result: 180

-- Convert π/2 radians to degrees
SELECT DEGREES(PI() / 2) AS deg;
-- Result: 90

-- Convert π/4 radians to degrees
SELECT DEGREES(PI() / 4) AS deg;
-- Result: 45

-- Convert result of ACOS to degrees
SELECT DEGREES(ACOS(0.5)) AS angle_degrees;
-- Result: 60
```

---

### Practical Mathematical Examples

**1. Calculate Distance Using Pythagorean Theorem:**
```sql
SELECT 
  point_id,
  SQRT(POWER(x2 - x1, 2) + POWER(y2 - y1, 2)) AS distance
FROM coordinates;
```

**2. Compound Interest Calculation:**
```sql
SELECT 
  account_id,
  principal,
  rate,
  years,
  ROUND(principal * POWER(1 + rate, years), 2) AS final_amount
FROM investments;
```

**3. Standard Deviation:**
```sql
SELECT 
  category,
  ROUND(SQRT(AVG(POWER(value - avg_value, 2))), 2) AS std_dev
FROM (
  SELECT 
    category,
    value,
    AVG(value) OVER (PARTITION BY category) AS avg_value
  FROM measurements
) AS calc
GROUP BY category;
```

**4. Normalize Values (0-1 range):**
```sql
SELECT 
  id,
  value,
  ROUND(
    (value - MIN(value) OVER ()) / 
    (MAX(value) OVER () - MIN(value) OVER ()),
    4
  ) AS normalized_value
FROM data_table;
```

**5. Calculate Angle Between Vectors:**
```sql
SELECT 
  vector_id,
  DEGREES(ATAN2(y, x)) AS angle_degrees,
  SQRT(POWER(x, 2) + POWER(y, 2)) AS magnitude
FROM vectors;
```

**6. Exponential Moving Average:**
```sql
SELECT 
  date,
  value,
  ROUND(
    value * EXP(-1 * days_diff / smoothing_factor),
    2
  ) AS ema_weight
FROM time_series;
```

**7. Price Buckets (Logarithmic):**
```sql
SELECT 
  POWER(10, FLOOR(LOG10(price))) AS price_bucket_start,
  POWER(10, CEIL(LOG10(price))) AS price_bucket_end,
  COUNT(*) AS product_count
FROM products
WHERE price > 0
GROUP BY FLOOR(LOG10(price)), CEIL(LOG10(price))
ORDER BY price_bucket_start;
```

**8. Calculate Circle Properties:**
```sql
SELECT 
  circle_id,
  radius,
  ROUND(2 * PI() * radius, 2) AS circumference,
  ROUND(PI() * POWER(radius, 2), 2) AS area
FROM circles;
```

---

### Mathematical Function Summary

| Function | Description | Example |
|----------|-------------|---------|
| `ABS(x)` | Absolute value | `ABS(-5)` → 5 |
| `SIGN(x)` | Sign (-1, 0, 1) | `SIGN(-10)` → -1 |
| `ROUND(x, n)` | Round to n decimals | `ROUND(123.456, 2)` → 123.46 |
| `FLOOR(x)` | Round down | `FLOOR(3.9)` → 3 |
| `CEIL(x)` | Round up | `CEIL(3.1)` → 4 |
| `POWER(x, y)` | x^y | `POWER(2, 10)` → 1024 |
| `SQRT(x)` | Square root | `SQRT(16)` → 4 |
| `LOG(x)` | Natural log | `LOG(EXP(1))` → 1 |
| `LOG10(x)` | Base-10 log | `LOG10(1000)` → 3 |
| `EXP(x)` | e^x | `EXP(1)` → 2.718... |
| `COS(x)` | Cosine | `COS(PI()/3)` → 0.5 |
| `ACOS(x)` | Arc cosine | `ACOS(0.5)` → π/3 |
| `SIN(x)` | Sine | `SIN(PI()/6)` → 0.5 |
| `ASIN(x)` | Arc sine | `ASIN(0.5)` → π/6 |
| `TAN(x)` | Tangent | `TAN(PI()/4)` → 1 |
| `ATAN(x)` | Arc tangent | `ATAN(1)` → π/4 |
| `ATAN2(y, x)` | Two-arg arc tangent | `ATAN2(1, 1)` → π/4 |
| `PI()` | Value of π | `PI()` → 3.14159... |
| `RADIANS(x)` | Degrees to radians | `RADIANS(180)` → π |
| `DEGREES(x)` | Radians to degrees | `DEGREES(PI())` → 180 |

---

[Back to index](README.md)
