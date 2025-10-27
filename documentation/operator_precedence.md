[Back to index](./README.md)

## Operator Precedence

This page lists operator precedence used by the parser and evaluator. Operators are evaluated in order from highest precedence (top) to lowest precedence (bottom).

---

### Precedence Order

| Precedence | Operator(s) | Category | Description |
|------------|-------------|----------|-------------|
| **1** (Highest) | `(...)` | Parentheses | Grouping and explicit precedence |
| **2** | `-`, `+`, `NOT` | Unary | Negation, unary plus, logical NOT |
| **3** | `*`, `/`, `%` | Multiplicative | Multiplication, division, modulo |
| **4** | `+`, `-` | Additive | Addition, subtraction |
| **5** | `<`, `<=`, `>`, `>=` | Comparison | Less than, less or equal, greater than, greater or equal |
| **6** | `=`, `!=`, `<>` | Equality | Equal, not equal |
| **7** | `BETWEEN`, `IN`, `LIKE`, `RLIKE` | Membership & Pattern | Range, set membership, pattern matching |
| **8** | `AND` | Logical AND | Logical conjunction |
| **9** (Lowest) | `OR` | Logical OR | Logical disjunction |

---

### 1. Parentheses `(...)`

**Highest precedence** - Used to explicitly control evaluation order.

**Examples:**
```sql
-- Without parentheses
SELECT 1 + 2 * 3 AS v;
-- Result: 7 (multiplication first: 1 + 6)

-- With parentheses
SELECT (1 + 2) * 3 AS v;
-- Result: 9 (addition first: 3 * 3)

-- Complex expression
SELECT 10 - 2 * 3 AS v1,
       (10 - 2) * 3 AS v2;
-- v1 = 4 (10 - 6)
-- v2 = 24 (8 * 3)

-- Multiple levels
SELECT ((5 + 3) * 2 - 4) / 2 AS result;
-- Result: 6
-- Step 1: (5 + 3) = 8
-- Step 2: 8 * 2 = 16
-- Step 3: 16 - 4 = 12
-- Step 4: 12 / 2 = 6
```

**Best Practice:**
```sql
-- Use parentheses for clarity even when not required
SELECT price * (1 + tax_rate) AS total_price
FROM products;

-- Better than ambiguous:
SELECT price * 1 + tax_rate AS total_price  -- Unclear intent
FROM products;
```

---

### 2. Unary Operators: `-`, `+`, `NOT`

**Second highest precedence** - Applied to single operands.

**Examples:**

**Unary Minus (Negation):**
```sql
-- Negate a value
SELECT -5 AS neg_value;
-- Result: -5

-- Negate column value
SELECT product_id, -price AS negative_price
FROM products;

-- In calculations
SELECT 10 + -5 AS result;
-- Result: 5

-- With parentheses for clarity
SELECT 10 + (-5) AS result;
-- Result: 5

-- Double negation
SELECT -(-10) AS result;
-- Result: 10
```

**Unary Plus:**
```sql
-- Explicit positive (rarely used)
SELECT +5 AS pos_value;
-- Result: 5

SELECT +price AS positive_price
FROM products;
```

**Logical NOT:**
```sql
-- Negate boolean expression
SELECT * FROM users
WHERE NOT is_active;
-- Same as: WHERE is_active = false

-- NOT with comparison
SELECT * FROM products
WHERE NOT (price > 100);
-- Same as: WHERE price <= 100

-- NOT with IN
SELECT * FROM orders
WHERE NOT status IN ('cancelled', 'refunded');
-- Same as: WHERE status NOT IN ('cancelled', 'refunded')

-- Multiple NOT
SELECT * FROM users
WHERE NOT (NOT is_verified);
-- Same as: WHERE is_verified
```

---

### 3. Multiplicative: `*`, `/`, `%`

**Third precedence** - Multiplication, division, and modulo operations.

**Examples:**

**Multiplication:**
```sql
-- Basic multiplication
SELECT 5 * 3 AS result;
-- Result: 15

-- In expressions
SELECT price * quantity AS total
FROM order_items;

-- Multiple multiplications (left to right)
SELECT 2 * 3 * 4 AS result;
-- Result: 24 (evaluated as (2 * 3) * 4)
```

**Division:**
```sql
-- Basic division
SELECT 10 / 2 AS result;
-- Result: 5

-- Integer division
SELECT 10 / 3 AS result;
-- Result: 3 (truncated if both operands are integers)

-- Float division
SELECT 10.0 / 3 AS result;
-- Result: 3.333...

-- Avoid division by zero
SELECT 
  CASE 
    WHEN quantity != 0 THEN total / quantity 
    ELSE 0 
  END AS avg_price
FROM orders;
```

**Modulo:**
```sql
-- Basic modulo
SELECT 10 % 3 AS remainder;
-- Result: 1

-- Even/odd check
SELECT 
  number,
  CASE WHEN number % 2 = 0 THEN 'Even' ELSE 'Odd' END AS parity
FROM numbers;

-- Cycling values
SELECT 
  day_number,
  day_number % 7 AS day_of_week
FROM calendar;
```

**Mixed Operations:**
```sql
-- Multiplication and division (left to right)
SELECT 10 * 2 / 4 AS result;
-- Result: 5 (evaluated as (10 * 2) / 4 = 20 / 4)

-- With modulo
SELECT 17 % 5 * 2 AS result;
-- Result: 4 (evaluated as (17 % 5) * 2 = 2 * 2)
```

---

### 4. Additive: `+`, `-`

**Fourth precedence** - Addition and subtraction operations.

**Examples:**

**Addition:**
```sql
-- Basic addition
SELECT 5 + 3 AS result;
-- Result: 8

-- Multiple additions
SELECT 10 + 20 + 30 AS result;
-- Result: 60

-- With columns
SELECT 
  base_price + tax + shipping AS total_cost
FROM orders;
```

**Subtraction:**
```sql
-- Basic subtraction
SELECT 10 - 3 AS result;
-- Result: 7

-- Multiple subtractions (left to right)
SELECT 100 - 20 - 10 AS result;
-- Result: 70 (evaluated as (100 - 20) - 10)

-- Date arithmetic
SELECT 
  order_date,
  DATE_SUB(order_date, INTERVAL 7 DAY) AS week_ago
FROM orders;
```

**Mixed with Multiplicative:**
```sql
-- Multiplication before addition
SELECT 1 + 2 * 3 AS result;
-- Result: 7 (evaluated as 1 + (2 * 3) = 1 + 6)

-- Use parentheses to change order
SELECT (1 + 2) * 3 AS result;
-- Result: 9

-- Complex expression
SELECT 10 + 5 * 2 - 3 AS result;
-- Result: 17 (evaluated as 10 + (5 * 2) - 3 = 10 + 10 - 3)

-- With parentheses
SELECT (10 + 5) * (2 - 3) AS result;
-- Result: -15 (evaluated as 15 * (-1))
```

---

### 5. Comparison: `<`, `<=`, `>`, `>=`

**Fifth precedence** - Relational comparisons.

**Examples:**

**Less Than / Greater Than:**
```sql
-- Basic comparisons
SELECT 5 < 10 AS result;
-- Result: true

SELECT 5 > 10 AS result;
-- Result: false

-- In WHERE clause
SELECT * FROM products
WHERE price < 100;

SELECT * FROM users
WHERE age >= 18;
```

**With Arithmetic:**
```sql
-- Arithmetic evaluated first
SELECT * FROM products
WHERE price + tax > 100;
-- Evaluated as: (price + tax) > 100

SELECT * FROM orders
WHERE quantity * price < 1000;
-- Evaluated as: (quantity * price) < 1000
```

**Multiple Comparisons:**
```sql
-- Chained comparisons require AND
SELECT * FROM products
WHERE price > 50 AND price < 100;

-- NOT this (syntax error in most SQL):
-- WHERE 50 < price < 100

-- Use BETWEEN instead
SELECT * FROM products
WHERE price BETWEEN 50 AND 100;
```

---

### 6. Equality: `=`, `!=`, `<>`

**Sixth precedence** - Equality and inequality checks.

**Examples:**

**Equality:**
```sql
-- Basic equality
SELECT 5 = 5 AS result;
-- Result: true

-- In WHERE clause
SELECT * FROM users
WHERE status = 'active';

-- NULL handling
SELECT * FROM users
WHERE email = NULL;  -- Always false!
-- Use: WHERE email IS NULL
```

**Inequality:**
```sql
-- Not equal (two forms)
SELECT 5 != 3 AS result;
-- Result: true

SELECT 5 <> 3 AS result;
-- Result: true

-- In WHERE clause
SELECT * FROM orders
WHERE status != 'cancelled';

SELECT * FROM products
WHERE category <> 'discontinued';
```

**With Comparisons:**
```sql
-- Comparison before equality
SELECT * FROM products
WHERE price > 50 = true;
-- Evaluated as: (price > 50) = true

-- More readable:
SELECT * FROM products
WHERE (price > 50) = true;

-- Or simply:
SELECT * FROM products
WHERE price > 50;
```

---

### 7. Membership & Pattern: `BETWEEN`, `IN`, `LIKE`, `RLIKE`

**Seventh precedence** - Range, set membership, and pattern matching.

**Examples:**

**BETWEEN:**
```sql
-- Range check
SELECT * FROM products
WHERE price BETWEEN 50 AND 100;
-- Equivalent to: price >= 50 AND price <= 100

-- With OR (lower precedence)
SELECT * FROM products
WHERE price BETWEEN 50 AND 100 OR category = 'sale';
-- Evaluated as: (price BETWEEN 50 AND 100) OR (category = 'sale')

-- NOT BETWEEN
SELECT * FROM products
WHERE price NOT BETWEEN 50 AND 100;
-- Equivalent to: price < 50 OR price > 100
```

**IN:**
```sql
-- Set membership
SELECT * FROM orders
WHERE status IN ('pending', 'processing', 'shipped');

-- With OR (lower precedence)
SELECT * FROM products
WHERE category IN ('electronics', 'computers') OR on_sale = true;
-- Evaluated as: (category IN (...)) OR (on_sale = true)

-- NOT IN
SELECT * FROM users
WHERE country NOT IN ('US', 'CA', 'MX');
```

**LIKE:**
```sql
-- Pattern matching
SELECT * FROM products
WHERE name LIKE '%phone%';

-- With OR
SELECT * FROM products
WHERE name LIKE '%phone%' OR name LIKE '%tablet%';
-- Evaluated as: (name LIKE '%phone%') OR (name LIKE '%tablet%')

-- NOT LIKE
SELECT * FROM products
WHERE name NOT LIKE '%discontinued%';
```

**RLIKE (Regular Expression):**
```sql
-- Regex pattern matching
SELECT * FROM products
WHERE name RLIKE '^[A-Z].*Pro$';

-- With OR
SELECT * FROM products
WHERE name RLIKE 'iPhone|iPad' OR category = 'Apple';
-- Evaluated as: (name RLIKE 'iPhone|iPad') OR (category = 'Apple')
```

**Mixed with AND:**
```sql
-- BETWEEN with AND (AND has lower precedence)
SELECT * FROM products
WHERE price BETWEEN 50 AND 100 AND category = 'electronics';
-- Evaluated as: (price BETWEEN 50 AND 100) AND (category = 'electronics')
```

---

### 8. Logical AND

**Eighth precedence** - Logical conjunction (both conditions must be true).

**Examples:**

**Basic AND:**
```sql
-- Both conditions must be true
SELECT * FROM users
WHERE is_active = true AND is_verified = true;

-- Multiple AND conditions
SELECT * FROM products
WHERE price > 50 
  AND price < 100 
  AND in_stock = true;
```

**AND with OR (OR has lower precedence):**
```sql
-- AND evaluated before OR
SELECT * FROM products
WHERE category = 'electronics' AND price < 100 OR on_sale = true;
-- Evaluated as: ((category = 'electronics') AND (price < 100)) OR (on_sale = true)
-- Matches: (electronics under $100) OR (anything on sale)

-- Use parentheses for different logic
SELECT * FROM products
WHERE category = 'electronics' AND (price < 100 OR on_sale = true);
-- Evaluated as: (category = 'electronics') AND ((price < 100) OR (on_sale = true))
-- Matches: electronics that are (under $100 OR on sale)
```

**Complex AND Expressions:**
```sql
-- Multiple AND with comparisons
SELECT * FROM orders
WHERE status = 'shipped' 
  AND shipped_date >= '2025-01-01'
  AND total_amount > 100
  AND customer_id IN (SELECT id FROM premium_customers);
```

---

### 9. Logical OR

**Lowest precedence** - Logical disjunction (at least one condition must be true).

**Examples:**

**Basic OR:**
```sql
-- At least one condition must be true
SELECT * FROM users
WHERE country = 'US' OR country = 'CA';

-- Multiple OR conditions
SELECT * FROM products
WHERE category = 'electronics' 
   OR category = 'computers' 
   OR category = 'phones';
```

**OR with AND (AND has higher precedence):**
```sql
-- Example from specification
SELECT * FROM products
WHERE category = 'electronics' AND price < 100 OR on_sale = true;
-- Evaluated as: ((category = 'electronics') AND (price < 100)) OR (on_sale = true)

-- Explicit parentheses for clarity
SELECT * FROM products
WHERE (category = 'electronics' AND price < 100) OR on_sale = true;

-- Different logic with parentheses
SELECT * FROM products
WHERE category = 'electronics' AND (price < 100 OR on_sale = true);
```

**BETWEEN with OR:**
```sql
-- Example from specification
SELECT * FROM products
WHERE price BETWEEN 50 AND 100 OR category = 'sale';
-- Evaluated as: (price BETWEEN 50 AND 100) OR (category = 'sale')

-- Multiple BETWEEN with OR
SELECT * FROM products
WHERE price BETWEEN 10 AND 50 
   OR price BETWEEN 200 AND 300 
   OR category = 'clearance';
```

**Complex OR Expressions:**
```sql
-- OR with multiple conditions
SELECT * FROM orders
WHERE status = 'cancelled' 
   OR status = 'refunded' 
   OR (status = 'pending' AND created_date < DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY));
```

---

### Practical Examples

**Example 1: Arithmetic Precedence**
```sql
-- Without parentheses (follows precedence)
SELECT 1 + 2 * 3 AS v;
-- Result: 7
-- Evaluation: 1 + (2 * 3) = 1 + 6 = 7

-- With parentheses (override precedence)
SELECT (1 + 2) * 3 AS v;
-- Result: 9
-- Evaluation: (1 + 2) * 3 = 3 * 3 = 9

-- Complex calculation
SELECT 10 + 5 * 2 - 8 / 4 AS result;
-- Result: 18
-- Evaluation: 10 + (5 * 2) - (8 / 4) = 10 + 10 - 2 = 18
```

**Example 2: Comparison and Logical Operators**
```sql
-- BETWEEN with OR
SELECT * FROM products
WHERE price BETWEEN 50 AND 100 OR category = 'sale';
-- Evaluated as: (price BETWEEN 50 AND 100) OR (category = 'sale')

-- AND before OR
SELECT * FROM products
WHERE category = 'electronics' AND price < 100 OR on_sale = true;
-- Evaluated as: ((category = 'electronics') AND (price < 100)) OR (on_sale = true)

-- Use parentheses for clarity
SELECT * FROM products
WHERE category = 'electronics' AND (price < 100 OR on_sale = true);
-- Evaluated as: (category = 'electronics') AND ((price < 100) OR (on_sale = true))
```

**Example 3: NOT Operator**
```sql
-- NOT with high precedence
SELECT * FROM users
WHERE NOT is_active AND is_verified;
-- Evaluated as: (NOT is_active) AND (is_verified)

-- Use parentheses for different logic
SELECT * FROM users
WHERE NOT (is_active AND is_verified);
-- Evaluated as: NOT ((is_active) AND (is_verified))
```

**Example 4: Complex Business Logic**
```sql
-- Find premium or high-value orders
SELECT * FROM orders
WHERE customer_type = 'premium' 
  AND total_amount > 500
  OR total_amount > 1000 
  AND status = 'completed';
-- Evaluated as:
-- ((customer_type = 'premium') AND (total_amount > 500))
-- OR
-- ((total_amount > 1000) AND (status = 'completed'))

-- More readable with parentheses
SELECT * FROM orders
WHERE (customer_type = 'premium' AND total_amount > 500)
   OR (total_amount > 1000 AND status = 'completed');
```

**Example 5: IN with OR and AND**
```sql
-- IN with AND and OR
SELECT * FROM products
WHERE category IN ('electronics', 'computers') 
  AND price < 500 
  OR on_sale = true;
-- Evaluated as:
-- ((category IN ('electronics', 'computers')) AND (price < 500))
-- OR
-- (on_sale = true)

-- Clearer with parentheses
SELECT * FROM products
WHERE (category IN ('electronics', 'computers') AND price < 500)
   OR on_sale = true;
```

---

### Best Practices

**1. Use Parentheses for Clarity**
```sql
-- Good: Explicit and clear
SELECT * FROM orders
WHERE (status = 'pending' AND created_date < '2025-01-01')
   OR (priority = 'high');

-- Avoid: Relies on precedence knowledge
SELECT * FROM orders
WHERE status = 'pending' AND created_date < '2025-01-01'
   OR priority = 'high';
```

**2. Group Related Conditions**
```sql
-- Good: Logical grouping
SELECT * FROM products
WHERE (category = 'electronics' AND price < 100)
   OR (category = 'books' AND price < 20)
   OR on_sale = true;

-- Avoid: Flat structure
SELECT * FROM products
WHERE category = 'electronics' AND price < 100
   OR category = 'books' AND price < 20
   OR on_sale = true;
```

[//]: # (**3. Break Complex Expressions into CTEs**)

[//]: # (```sql)

[//]: # (-- Good: Readable with CTE)

[//]: # (WITH affordable_electronics AS &#40;)

[//]: # (  SELECT * FROM products)

[//]: # (  WHERE category = 'electronics' AND price < 100)

[//]: # (&#41;,)

[//]: # (sale_items AS &#40;)

[//]: # (  SELECT * FROM products)

[//]: # (  WHERE on_sale = true)

[//]: # (&#41;)

[//]: # (SELECT * FROM affordable_electronics)

[//]: # (UNION)

[//]: # (SELECT * FROM sale_items;)

[//]: # ()
[//]: # (-- Avoid: Complex single query)

[//]: # (SELECT * FROM products)

[//]: # (WHERE category = 'electronics' AND price < 100 OR on_sale = true;)

[//]: # (```)

**3. Document Complex Logic**
```sql
-- Good: Commented for clarity
SELECT * FROM orders
WHERE 
  -- High priority orders
  (priority = 'high' AND status = 'pending')
  -- OR old pending orders
  OR (status = 'pending' AND created_date < DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY))
  -- OR VIP customer orders
  OR customer_id IN (SELECT id FROM vip_customers);
```

---

### Common Mistakes

**Mistake 1: Forgetting AND before OR precedence**
```sql
-- Wrong interpretation
SELECT * FROM products
WHERE category = 'electronics' AND price < 100 OR on_sale = true;
-- Might think: electronics that are (under $100 OR on sale)
-- Actually means: (electronics under $100) OR (anything on sale)

-- Correct with parentheses
SELECT * FROM products
WHERE category = 'electronics' AND (price < 100 OR on_sale = true);
```

**Mistake 2: Arithmetic order confusion**
```sql
-- Wrong
SELECT price + tax * quantity FROM orders;
-- Evaluated as: price + (tax * quantity)

-- Correct
SELECT (price + tax) * quantity FROM orders;
```

**Mistake 3: NOT operator scope**
```sql
-- Wrong interpretation
SELECT * FROM users
WHERE NOT is_active AND is_verified;
-- Evaluated as: (NOT is_active) AND (is_verified)

-- If you want: NOT (is_active AND is_verified)
SELECT * FROM users
WHERE NOT (is_active AND is_verified);
```

**Mistake 4: Multiple comparisons**
```sql
-- Wrong (syntax error)
SELECT * FROM products
WHERE 10 < price < 100;

-- Correct
SELECT * FROM products
WHERE price > 10 AND price < 100;

-- Or use BETWEEN
SELECT * FROM products
WHERE price BETWEEN 10 AND 100;
```

---

### Precedence Summary Table

| Level | Operators | Associativity | Example |
|-------|-----------|---------------|---------|
| 1 | `(...)` | N/A | `(a + b) * c` |
| 2 | `-`, `+`, `NOT` | Right | `-a`, `NOT b` |
| 3 | `*`, `/`, `%` | Left | `a * b / c` |
| 4 | `+`, `-` | Left | `a + b - c` |
| 5 | `<`, `<=`, `>`, `>=` | Left | `a < b` |
| 6 | `=`, `!=`, `<>` | Left | `a = b` |
| 7 | `BETWEEN`, `IN`, `LIKE`, `RLIKE` | N/A | `a BETWEEN 1 AND 10` |
| 8 | `AND` | Left | `a AND b AND c` |
| 9 | `OR` | Left | `a OR b OR c` |

**Associativity:**
- **Left**: Operators of same precedence evaluate left-to-right: `a - b - c` = `(a - b) - c`
- **Right**: Operators evaluate right-to-left: `-a` applies to `a` first
- **N/A**: Not applicable for grouping or special operators

[Back to index](./README.md)
