[Back to index](README.md)

# Operators (detailed)

**Navigation:** [Query Structure](request_structure.md) · [Operator Precedence](operator_precedence.md) · [Keywords](keywords.md)

This file provides a per-operator description and concrete SQL examples for each operator supported by the engine.

---

## Table of Contents

1. [Math Operators](#math-operators)
2. [Comparison Operators](#comparison-operators)
3. [Logical Operators](#logical-operators)
4. [Cast Operators](#cast-operators)

---

## Math Operators

### Operator: `+`

**Description:**  
Arithmetic addition.

**Syntax:**
```sql
expr1 + expr2
```

**Inputs:**
- `expr1`, `expr2` - Numeric expressions (`INT`, `DOUBLE`, `DECIMAL`, etc.)

**Output:**
- Numeric type (result type depends on operand types)

**Examples:**

**Basic Addition:**
```sql
-- Add two numbers
SELECT 5 + 3 AS result;
-- Result: 8

-- Add column values
SELECT salary + bonus AS total_comp FROM emp;
-- Result example: if salary=50000 and bonus=10000 -> total_comp = 60000

-- Multiple additions
SELECT base_price + tax + shipping AS total_cost
FROM orders;
```

**With Different Types:**
```sql
-- Integer addition
SELECT 10 + 20 AS sum;
-- Result: 30

-- Float addition
SELECT 10.5 + 20.3 AS sum;
-- Result: 30.8

-- Mixed types (INT + DOUBLE)
SELECT 10 + 20.5 AS sum;
-- Result: 30.5 (promoted to DOUBLE)
```

**NULL Handling:**
```sql
-- NULL propagation
SELECT salary + NULL AS result FROM emp;
-- Result: NULL

-- Use COALESCE for default
SELECT salary + COALESCE(bonus, 0) AS total FROM emp;
```

---

### Operator: `-`

**Description:**  
Arithmetic subtraction or unary negation when used with single operand.

**Syntax:**
```sql
-- Binary (subtraction)
expr1 - expr2

-- Unary (negation)
-expr
```

**Inputs:**
- `expr1`, `expr2` - Numeric expressions
- `expr` - Numeric expression (for unary)

**Output:**
- Numeric type

**Examples:**

**Subtraction:**
```sql
-- Basic subtraction
SELECT 10 - 3 AS result;
-- Result: 7

-- Column subtraction
SELECT salary - tax AS net FROM emp;

-- Multiple subtractions
SELECT revenue - cost - overhead AS profit
FROM financials;
```

**Unary Negation:**
```sql
-- Negate a value
SELECT -balance AS negative_balance FROM accounts;

-- Negate column
SELECT -price AS negated_price FROM products;

-- In expressions
SELECT 100 + (-50) AS result;
-- Result: 50
```

**Date Arithmetic:**
```sql
-- Date subtraction (days between)
SELECT order_date - ship_date AS days_to_ship
FROM orders;

-- With INTERVAL
SELECT order_date - INTERVAL 7 DAY AS week_ago
FROM orders;
```

---

### Operator: `*`

**Description:**  
Multiplication.

**Syntax:**
```sql
expr1 * expr2
```

**Inputs:**
- `expr1`, `expr2` - Numeric expressions

**Output:**
- Numeric type

**Examples:**

**Basic Multiplication:**
```sql
-- Multiply two numbers
SELECT 5 * 3 AS result;
-- Result: 15

-- Calculate revenue
SELECT quantity * price AS revenue FROM sales;

-- Multiple multiplications
SELECT length * width * height AS volume
FROM boxes;
```

**With Different Types:**
```sql
-- Integer multiplication
SELECT 10 * 5 AS product;
-- Result: 50

-- Float multiplication
SELECT 10.5 * 2.0 AS product;
-- Result: 21.0

-- Mixed types
SELECT 10 * 2.5 AS product;
-- Result: 25.0
```

**Practical Examples:**
```sql
-- Calculate total with tax
SELECT price * (1 + tax_rate) AS total_price
FROM products;

-- Calculate discount
SELECT price * (1 - discount_percent / 100) AS discounted_price
FROM products;

-- Area calculation
SELECT width * height AS area FROM rectangles;
```

---

### Operator: `/`

**Description:**  
Division; division by zero must be guarded (using NULLIF). Engine returns NULL for invalid arithmetic.

**Syntax:**
```sql
expr1 / expr2
```

**Inputs:**
- `expr1`, `expr2` - Numeric expressions

**Output:**
- Numeric type (NULL if division by zero)

**Examples:**

**Basic Division:**
```sql
-- Divide two numbers
SELECT 10 / 2 AS result;
-- Result: 5

-- Calculate average
SELECT total / NULLIF(count, 0) AS avg FROM table;

-- Per-unit price
SELECT total_price / quantity AS unit_price
FROM order_items;
```

**Integer vs Float Division:**
```sql
-- Integer division (truncates)
SELECT 10 / 3 AS result;
-- Result: 3 (if both are integers)

-- Float division
SELECT 10.0 / 3 AS result;
-- Result: 3.333...

-- Force float division
SELECT CAST(10 AS DOUBLE) / 3 AS result;
-- Result: 3.333...
```

**Division by Zero Protection:**
```sql
-- Using NULLIF (recommended)
SELECT total / NULLIF(count, 0) AS avg
FROM statistics;
-- Returns NULL if count = 0

-- Using CASE
SELECT 
  CASE 
    WHEN count != 0 THEN total / count 
    ELSE 0 
  END AS avg
FROM statistics;

-- Using COALESCE for default
SELECT COALESCE(total / NULLIF(count, 0), 0) AS avg
FROM statistics;
```

**Practical Examples:**
```sql
-- Calculate percentage
SELECT (passed / NULLIF(total, 0)) * 100 AS pass_rate
FROM exam_results;

-- Average order value
SELECT 
  SUM(total) / NULLIF(COUNT(*), 0) AS avg_order_value
FROM orders;

-- Split cost
SELECT total_cost / NULLIF(num_people, 0) AS cost_per_person
FROM expenses;
```

---

### Operator: `%` (MOD)

**Description:**  
Remainder/modulo operator.

**Syntax:**
```sql
expr1 % expr2
```

**Inputs:**
- `expr1`, `expr2` - Integer expressions

**Output:**
- Integer (remainder of division)

**Examples:**

**Basic Modulo:**
```sql
-- Get remainder
SELECT 10 % 3 AS remainder;
-- Result: 1

-- Bucket users by ID
SELECT id % 10 AS bucket FROM users;

-- Check if number is even
SELECT 
  number,
  CASE WHEN number % 2 = 0 THEN 'Even' ELSE 'Odd' END AS parity
FROM numbers;
```

**Practical Examples:**
```sql
-- Distribute data across shards
SELECT 
  user_id,
  user_id % 5 AS shard_id
FROM users;

-- Find every Nth record
SELECT * FROM logs
WHERE log_id % 100 = 0;  -- Every 100th record

-- Cycle through values
SELECT 
  day_number,
  day_number % 7 AS day_of_week
FROM calendar;

-- Alternate row colors (even/odd)
SELECT 
  row_number,
  CASE WHEN row_number % 2 = 0 THEN 'even-row' ELSE 'odd-row' END AS css_class
FROM data_table;
```

**With Negative Numbers:**
```sql
-- Modulo with negative numbers
SELECT -10 % 3 AS result;
-- Result: -1 (sign follows dividend)

SELECT 10 % -3 AS result;
-- Result: 1
```

---

## Comparison Operators

### Operator: `=`

**Description:**  
Equality comparison.

**Syntax:**
```sql
expr1 = expr2
```

**Inputs:**
- `expr1`, `expr2` - Any comparable types

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic Equality:**
```sql
-- Compare values
SELECT 5 = 5 AS result;
-- Result: true

-- Filter by department
SELECT * FROM emp WHERE department = 'IT';

-- Compare columns
SELECT * FROM orders
WHERE customer_id = shipping_customer_id;
```

**String Comparison:**
```sql
-- Case-sensitive string comparison
SELECT * FROM users WHERE username = 'john_doe';

-- Compare with column
SELECT * FROM products
WHERE category = 'Electronics';
```

**Numeric Comparison:**
```sql
-- Integer equality
SELECT * FROM products WHERE stock_quantity = 0;

-- Decimal equality
SELECT * FROM orders WHERE total_amount = 99.99;
```

**Date Comparison:**
```sql
-- Date equality
SELECT * FROM orders WHERE order_date = '2025-01-10';

-- Timestamp equality
SELECT * FROM events 
WHERE event_timestamp = '2025-01-10 14:30:00';
```

**NULL Handling:**
```sql
-- NULL comparison always returns NULL (not true or false)
SELECT * FROM emp WHERE manager = NULL;  -- Returns no rows!

-- Use IS NULL instead
SELECT * FROM emp WHERE manager IS NULL;
```

---

### Operator: `<>`, `!=`

**Description:**  
Inequality comparison (both synonyms supported).

**Syntax:**
```sql
expr1 <> expr2
expr1 != expr2
```

**Inputs:**
- `expr1`, `expr2` - Any comparable types

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic Inequality:**
```sql
-- Not equal
SELECT 5 <> 3 AS result;
-- Result: true

SELECT 5 != 3 AS result;
-- Result: true

-- Filter by status
SELECT * FROM emp WHERE status <> 'terminated';
SELECT * FROM emp WHERE status != 'terminated';
```

**String Inequality:**
```sql
-- Exclude specific values
SELECT * FROM products WHERE category != 'Discontinued';

-- Multiple exclusions (use NOT IN instead)
SELECT * FROM orders 
WHERE status <> 'cancelled' 
  AND status <> 'refunded';
```

**Numeric Inequality:**
```sql
-- Not equal to zero
SELECT * FROM products WHERE stock_quantity != 0;

-- Exclude specific value
SELECT * FROM users WHERE age <> 18;
```

**NULL Handling:**
```sql
-- NULL inequality returns NULL (not true)
SELECT * FROM emp WHERE manager != NULL;  -- Returns no rows!

-- Use IS NOT NULL instead
SELECT * FROM emp WHERE manager IS NOT NULL;
```

---

### Operator: `<`, `<=`, `>`, `>=`

**Description:**  
Relational comparisons.

**Syntax:**
```sql
expr1 < expr2   -- Less than
expr1 <= expr2  -- Less than or equal
expr1 > expr2   -- Greater than
expr1 >= expr2  -- Greater than or equal
```

**Inputs:**
- `expr1`, `expr2` - Comparable types (numeric, string, date, etc.)

**Return type:**
- `BOOLEAN`

**Examples:**

**Numeric Comparisons:**
```sql
-- Greater than
SELECT * FROM products WHERE price > 100;

-- Less than or equal
SELECT * FROM users WHERE age <= 18;

-- Age range
SELECT * FROM emp WHERE age >= 21 AND age < 65;

-- Between alternative
SELECT * FROM products 
WHERE price >= 50 AND price <= 100;
```

**String Comparisons:**
```sql
-- Lexicographic comparison
SELECT * FROM users WHERE username > 'M';  -- Names starting with M-Z

-- Alphabetical range
SELECT * FROM products 
WHERE name >= 'A' AND name < 'D';
```

**Date Comparisons:**
```sql
-- After specific date
SELECT * FROM orders WHERE order_date > '2025-01-01';

-- Before or on date
SELECT * FROM events WHERE event_date <= CURRENT_DATE;

-- Date range
SELECT * FROM logs
WHERE log_date >= '2025-01-01' 
  AND log_date < '2025-02-01';
```

**Timestamp Comparisons:**
```sql
-- Recent records
SELECT * FROM activities
WHERE created_at >= DATE_SUB(CURRENT_TIMESTAMP, INTERVAL 1 HOUR);

-- Within time range
SELECT * FROM events
WHERE event_time >= '2025-01-10 09:00:00'
  AND event_time <= '2025-01-10 17:00:00';
```

---

### Operator: `IN`

**Description:**  
Membership in a set of literal or numeric values, or results of subquery (subquery support depends on implementation).

**Syntax:**
```sql
expr IN (value1, value2, ...)
expr IN (subquery)
```

**Inputs:**
- `expr` - Expression to test
- `value1, value2, ...` - List of values
- `subquery` - Subquery returning single column

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic IN with Literals:**
```sql
-- String values
SELECT * FROM emp WHERE department IN ('Sales', 'IT', 'HR');

-- Numeric values
SELECT * FROM emp WHERE status IN (1, 2);

-- Single value (equivalent to =)
SELECT * FROM products WHERE category IN ('Electronics');
```

**Multiple Value Types:**
```sql
-- Integer list
SELECT * FROM orders WHERE order_id IN (100, 101, 102, 103);

-- String list
SELECT * FROM users 
WHERE country IN ('US', 'CA', 'MX', 'UK');

-- Date list
SELECT * FROM events
WHERE event_date IN ('2025-01-01', '2025-01-15', '2025-01-31');
```

**With Subquery:**
```sql
-- Subquery returning IDs
SELECT * FROM orders
WHERE customer_id IN (
  SELECT id FROM customers WHERE status = 'premium'
);

-- Nested subquery
SELECT * FROM products
WHERE category_id IN (
  SELECT id FROM categories WHERE active = true
);
```

**Empty List:**
```sql
-- Empty IN list returns false
SELECT * FROM products WHERE id IN ();
-- Returns no rows
```

**NULL Handling:**
```sql
-- NULL in list
SELECT * FROM users WHERE status IN ('active', NULL);
-- NULL is ignored in the list

-- Column with NULL
SELECT * FROM users WHERE email IN ('test@example.com');
-- Rows with NULL email are not matched
```

---

### Operator: `NOT IN`

**Description:**  
Negated membership.

**Syntax:**
```sql
expr NOT IN (value1, value2, ...)
expr NOT IN (subquery)
```

**Inputs:**
- `expr` - Expression to test
- `value1, value2, ...` - List of values to exclude
- `subquery` - Subquery returning single column

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic NOT IN:**
```sql
-- Exclude departments
SELECT * FROM emp WHERE department NOT IN ('HR', 'Legal');

-- Exclude statuses
SELECT * FROM orders WHERE status NOT IN ('cancelled', 'refunded');

-- Exclude IDs
SELECT * FROM products WHERE product_id NOT IN (1, 2, 3);
```

**With Subquery:**
```sql
-- Exclude customers who have orders
SELECT * FROM customers
WHERE id NOT IN (
  SELECT DISTINCT customer_id FROM orders
);

-- Exclude inactive categories
SELECT * FROM products
WHERE category_id NOT IN (
  SELECT id FROM categories WHERE active = false
);
```

**NULL Handling (Important!):**
```sql
-- NOT IN with NULL in list returns NULL (not true!)
SELECT * FROM users WHERE id NOT IN (1, 2, NULL);
-- Returns no rows because comparison with NULL is NULL

-- Safe alternative: filter NULLs in subquery
SELECT * FROM customers
WHERE id NOT IN (
  SELECT customer_id FROM orders WHERE customer_id IS NOT NULL
);

-- Or use NOT EXISTS
SELECT * FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.id
);
```

---

### Operator: `BETWEEN ... AND ...`

**Description:**  
Checks if an expression lies between two boundaries (inclusive).
- For numeric expressions, `BETWEEN` works as standard SQL.
- For distance expressions (`ST_DISTANCE`), it supports units (`m`, `km`, `mi`, etc.).

**Syntax:**
```sql
expr BETWEEN lower_bound AND upper_bound
```

**Inputs:**
- `expr` - Expression to test
- `lower_bound`, `upper_bound` - Boundary values (inclusive)

**Return type:**
- `BOOLEAN`

**Examples:**

**Numeric BETWEEN:**
```sql
-- Age range
SELECT age FROM users WHERE age BETWEEN 18 AND 30;
-- Equivalent to: age >= 18 AND age <= 30

-- Price range
SELECT * FROM products WHERE price BETWEEN 50 AND 100;

-- Quantity range
SELECT * FROM inventory WHERE stock_quantity BETWEEN 10 AND 100;
```

**Temporal BETWEEN:**
```sql
-- Date range
SELECT * FROM orders
WHERE order_date BETWEEN '2025-01-01' AND '2025-01-31';

-- With date functions
SELECT * FROM users
WHERE createdAt BETWEEN CURRENT_DATE - INTERVAL 1 MONTH AND CURRENT_DATE;

-- Complex temporal range
SELECT * FROM users
WHERE createdAt BETWEEN CURRENT_DATE - INTERVAL 1 MONTH AND CURRENT_DATE
  AND lastUpdated BETWEEN LAST_DAY('2025-09-11'::DATE) AND DATE_TRUNC(CURRENT_TIMESTAMP, DAY);

-- Timestamp range
SELECT * FROM events
WHERE event_timestamp BETWEEN '2025-01-10 00:00:00' AND '2025-01-10 23:59:59';
```

**Distance BETWEEN (Geospatial):**

**Using meters (default):**
```sql
-- Distance in meters
SELECT id FROM locations 
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 AND 5000;
-- Finds locations between 4km and 5km away
```

**With explicit units:**
```sql
-- Distance with km units
SELECT id FROM locations 
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 km AND 5000 km;

-- Distance with miles
SELECT id FROM locations 
WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 2.5 mi AND 3.1 mi;
```

**Elasticsearch Optimization:**
> 👉 In Elasticsearch translation, distance BETWEEN queries are optimized into a combination of:
> - A **script filter** for the lower bound
> - A `geo_distance` **query** for the upper bound (native ES optimization)

**String BETWEEN:**
```sql
-- Lexicographic range
SELECT * FROM products WHERE name BETWEEN 'A' AND 'D';
-- Names starting with A, B, or C

-- Date strings (if stored as strings)
SELECT * FROM logs WHERE log_date BETWEEN '2025-01' AND '2025-03';
```

**NOT BETWEEN:**
```sql
-- Outside range
SELECT * FROM products WHERE price NOT BETWEEN 50 AND 100;
-- Equivalent to: price < 50 OR price > 100

-- Exclude date range
SELECT * FROM orders
WHERE order_date NOT BETWEEN '2024-12-20' AND '2025-01-05';
```

**NULL Handling:**
```sql
-- NULL expression returns NULL (not true or false)
SELECT * FROM products WHERE NULL BETWEEN 10 AND 20;
-- Returns no rows

-- Column with NULL
SELECT * FROM products WHERE price BETWEEN 10 AND 20;
-- Rows with NULL price are excluded
```

---

### Operator: `IS NULL`

**Description:**  
Null check predicate.

**Syntax:**
```sql
expr IS NULL
```

**Inputs:**
- `expr` - Expression to test for NULL

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic NULL Check:**
```sql
-- Find rows with NULL manager
SELECT * FROM emp WHERE manager IS NULL;

-- Find products without description
SELECT * FROM products WHERE description IS NULL;

-- Find users without email
SELECT * FROM users WHERE email IS NULL;
```

**Multiple NULL Checks:**
```sql
-- Check multiple columns
SELECT * FROM contacts
WHERE phone IS NULL AND email IS NULL;

-- Either column is NULL
SELECT * FROM users
WHERE first_name IS NULL OR last_name IS NULL;
```

**In SELECT:**
```sql
-- Flag NULL values
SELECT 
  name,
  email,
  CASE WHEN email IS NULL THEN 'No Email' ELSE 'Has Email' END AS email_status
FROM users;

-- Count NULLs
SELECT 
  COUNT(*) AS total_rows,
  COUNT(email) AS rows_with_email,
  SUM(CASE WHEN email IS NULL THEN 1 ELSE 0 END) AS rows_without_email
FROM users;
```

**With COALESCE:**
```sql
-- Provide default for NULL
SELECT 
  name,
  COALESCE(email, 'no-email@example.com') AS email
FROM users
WHERE email IS NULL;
```

---

### Operator: `IS NOT NULL`

**Description:**  
Negated null check.

**Syntax:**
```sql
expr IS NOT NULL
```

**Inputs:**
- `expr` - Expression to test for non-NULL

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic NOT NULL Check:**
```sql
-- Find rows with manager assigned
SELECT * FROM emp WHERE manager IS NOT NULL;

-- Find products with description
SELECT * FROM products WHERE description IS NOT NULL;

-- Find users with email
SELECT * FROM users WHERE email IS NOT NULL;
```

**Multiple NOT NULL Checks:**
```sql
-- Both columns must have values
SELECT * FROM contacts
WHERE phone IS NOT NULL AND email IS NOT NULL;

-- At least one column has value
SELECT * FROM users
WHERE first_name IS NOT NULL OR last_name IS NOT NULL;
```

**Data Quality Checks:**
```sql
-- Complete records only
SELECT * FROM orders
WHERE customer_id IS NOT NULL
  AND order_date IS NOT NULL
  AND total_amount IS NOT NULL;

-- Count complete records
SELECT 
  COUNT(*) AS total,
  COUNT(CASE WHEN email IS NOT NULL AND phone IS NOT NULL THEN 1 END) AS complete_contacts
FROM users;
```

**In Aggregations:**
```sql
-- Aggregate only non-NULL values
SELECT 
  category,
  COUNT(*) AS total_products,
  COUNT(CASE WHEN price IS NOT NULL THEN 1 END) AS products_with_price
FROM products
GROUP BY category;
```

---

### Operator: `LIKE`

**Description:**  
Pattern match using `%` and `_`.
- `%` matches zero or more characters (converted to `.*` in regex)
- `_` matches exactly one character (converted to `.` in regex)

**Syntax:**
```sql
expr LIKE pattern
```

**Inputs:**
- `expr` - String expression to test
- `pattern` - Pattern string with `%` and `_` wildcards

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic LIKE Patterns:**
```sql
-- Starts with
SELECT * FROM emp WHERE name LIKE 'Jo%';
-- Matches: 'John', 'Joe', 'Joseph', etc.

-- Ends with
SELECT * FROM products WHERE name LIKE '%phone';
-- Matches: 'iPhone', 'smartphone', 'telephone', etc.

-- Contains
SELECT * FROM articles WHERE title LIKE '%tutorial%';
-- Matches any title containing 'tutorial'

-- Exact length with _
SELECT * FROM codes WHERE code LIKE '___';
-- Matches exactly 3 characters: 'ABC', '123', etc.
```

**Complex Patterns:**
```sql
-- Starts with and ends with
SELECT * FROM products WHERE name LIKE 'iPhone%Pro';
-- Matches: 'iPhone 14 Pro', 'iPhone 15 Pro Max', etc.

-- Multiple wildcards
SELECT * FROM emails WHERE address LIKE '%@%.com';
-- Matches emails ending with .com

-- Single character wildcard
SELECT * FROM users WHERE username LIKE 'user_0_';
-- Matches: 'user_01', 'user_02', ..., 'user_99'

-- Pattern with specific positions
SELECT * FROM phone_numbers WHERE number LIKE '555-____';
-- Matches: '555-1234', '555-5678', etc.
```

**Case Sensitivity:**
```sql
-- Case-insensitive (depends on collation)
SELECT * FROM users WHERE name LIKE 'john%';
-- May or may not match 'JOHN', 'John', 'john'

-- Force case-insensitive with LOWER
SELECT * FROM users WHERE LOWER(name) LIKE LOWER('john%');
-- Matches all case variations
```

**NOT LIKE:**
```sql
-- Exclude pattern
SELECT * FROM products WHERE name NOT LIKE '%discontinued%';

-- Exclude multiple patterns
SELECT * FROM articles 
WHERE title NOT LIKE '%draft%' 
  AND title NOT LIKE '%temp%';
```

**Escaping Special Characters:**
```sql
-- Literal % or _ (if supported)
SELECT * FROM products WHERE name LIKE '100\% cotton' ESCAPE '\';
-- Matches: '100% cotton'

-- Literal underscore
SELECT * FROM codes WHERE code LIKE 'CODE\_123' ESCAPE '\';
-- Matches: 'CODE_123'
```

**Performance Note:**
```sql
-- Leading wildcard prevents index usage
SELECT * FROM products WHERE name LIKE '%phone';  -- Slow

-- Prefix search can use index
SELECT * FROM products WHERE name LIKE 'phone%';  -- Fast

-- Consider full-text search for complex patterns
SELECT * FROM products WHERE MATCH(name) AGAINST ('phone');
```

---

### Operator: `RLIKE`

**Description:**  
Regular-expression match (Java regex semantics).

**Syntax:**
```sql
expr RLIKE pattern
```

**Inputs:**
- `expr` - String expression to test
- `pattern` - Regular expression pattern (Java regex syntax)

**Return type:**
- `BOOLEAN`

**Examples:**

**Basic Regex Patterns:**
```sql
-- Email validation
SELECT * FROM users WHERE email RLIKE '.*@example\\.com$';
-- Matches emails ending with @example.com

-- Phone number pattern
SELECT * FROM contacts WHERE phone RLIKE '^\\d{3}-\\d{3}-\\d{4}$';
-- Matches: '555-123-4567'

-- Starts with pattern
SELECT * FROM products WHERE name RLIKE '^iPhone';
-- Matches names starting with 'iPhone'

-- Ends with pattern
SELECT * FROM files WHERE filename RLIKE '\\.(jpg|png|gif)$';
-- Matches image files
```

**Character Classes:**
```sql
-- Alphanumeric
SELECT * FROM codes WHERE code RLIKE '^[A-Z0-9]+$';
-- Matches uppercase letters and numbers only

-- Digits only
SELECT * FROM ids WHERE id RLIKE '^\\d+$';
-- Matches numeric IDs

-- Letters only
SELECT * FROM names WHERE name RLIKE '^[a-zA-Z]+$';
-- Matches alphabetic names only
```

**Quantifiers:**
```sql
-- Exact count
SELECT * FROM zipcodes WHERE code RLIKE '^\\d{5}$';
-- Matches exactly 5 digits: '12345'

-- Range
SELECT * FROM passwords WHERE password RLIKE '^.{8,20}$';
-- Matches 8 to 20 characters

-- One or more
SELECT * FROM tags WHERE tag RLIKE '^#[a-z]+$';
-- Matches hashtags: '#tag', '#example'

-- Zero or more
SELECT * FROM urls WHERE url RLIKE '^https?://.*';
-- Matches http:// or https:// URLs
```

**Grouping and Alternation:**
```sql
-- Multiple options
SELECT * FROM products WHERE name RLIKE 'iPhone|iPad|iPod';
-- Matches any Apple i-device

-- Grouped patterns
SELECT * FROM users WHERE email RLIKE '^(admin|support|info)@.*';
-- Matches emails starting with admin@, support@, or info@

-- Complex grouping
SELECT * FROM codes WHERE code RLIKE '^(US|CA|MX)-[0-9]{4}$';
-- Matches: 'US-1234', 'CA-5678', 'MX-9012'
```

**Anchors:**
```sql
-- Start of string
SELECT * FROM usernames WHERE username RLIKE '^admin';
-- Matches usernames starting with 'admin'

-- End of string
SELECT * FROM emails WHERE email RLIKE '@company\\.com$';
-- Matches emails ending with @company.com

-- Whole string match
SELECT * FROM codes WHERE code RLIKE '^[A-Z]{3}-[0-9]{4}$';
-- Matches exactly: 'ABC-1234'
```

**Advanced Patterns:**
```sql
-- URL validation
SELECT * FROM links 
WHERE url RLIKE '^https?://[a-zA-Z0-9.-]+\\.[a-z]{2,}(/.*)?$';

-- IPv4 address
SELECT * FROM servers 
WHERE ip_address RLIKE '^([0-9]{1,3}\\.){3}[0-9]{1,3}$';

-- Credit card (basic pattern)
SELECT * FROM payments 
WHERE card_number RLIKE '^[0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4}$';

-- Date format (YYYY-MM-DD)
SELECT * FROM records 
WHERE date_str RLIKE '^[0-9]{4}-[0-9]{2}-[0-9]{2}$';
```

**NOT RLIKE:**
```sql
-- Exclude pattern
SELECT * FROM users WHERE email NOT RLIKE '.*@spam\\.com$';

-- Exclude invalid formats
SELECT * FROM phone_numbers 
WHERE number NOT RLIKE '^[0-9-]+$';
```

**Case Insensitive:**
```sql
-- Use (?i) flag for case-insensitive
SELECT * FROM products WHERE name RLIKE '(?i)^iphone';
-- Matches: 'iPhone', 'IPHONE', 'iphone', etc.
```

**Comparison with LIKE:**
```sql
-- LIKE (simpler, limited wildcards)
SELECT * FROM products WHERE name LIKE 'iPhone%';

-- RLIKE (powerful, full regex)
SELECT * FROM products WHERE name RLIKE '^iPhone (1[0-5]|[0-9]) (Pro|Max).*';
-- Matches: 'iPhone 14 Pro', 'iPhone 15 Pro Max', etc.
```

---

## Logical Operators

### Operator: `AND`

**Description:**  
Logical conjunction. Returns true only if both operands are true.

**Syntax:**
```sql
condition1 AND condition2
```

**Inputs:**
- `condition1`, `condition2` - Boolean expressions

**Return type:**
- `BOOLEAN`

**Truth Table:**
| A | B | A AND B |
|---|---|---------|
| true | true | true |
| true | false | false |
| false | true | false |
| false | false | false |
| true | NULL | NULL |
| false | NULL | false |
| NULL | true | NULL |
| NULL | false | false |
| NULL | NULL | NULL |

**Examples:**

**Basic AND:**
```sql
-- Both conditions must be true
SELECT * FROM emp WHERE dept = 'IT' AND salary > 50000;

-- Multiple AND conditions
SELECT * FROM products
WHERE category = 'Electronics'
  AND price > 100
  AND in_stock = true;
```

**Combining Different Comparisons:**
```sql
-- Range with AND
SELECT * FROM users
WHERE age >= 18 AND age <= 65;

-- Multiple field checks
SELECT * FROM orders
WHERE status = 'shipped'
  AND total_amount > 100
  AND customer_id IS NOT NULL;
```

**AND with Complex Expressions:**
```sql
-- With calculations
SELECT * FROM employees
WHERE salary + bonus > 100000
  AND department IN ('Sales', 'IT');

-- With pattern matching
SELECT * FROM products
WHERE name LIKE '%Pro%'
  AND price BETWEEN 500 AND 2000;
```

**NULL Handling:**
```sql
-- NULL in AND expression
SELECT * FROM users
WHERE is_active = true AND email IS NOT NULL;
-- Only returns rows where both conditions are true

-- NULL propagation
SELECT * FROM products
WHERE price > 100 AND discount IS NULL;
-- Returns products with price > 100 and no discount
```

**Chaining Multiple AND:**
```sql
-- Long AND chain
SELECT * FROM orders
WHERE status = 'completed'
  AND payment_status = 'paid'
  AND shipping_status = 'delivered'
  AND customer_rating >= 4
  AND order_date >= '2025-01-01';
```

**Performance Tip:**
```sql
-- Put most selective conditions first
SELECT * FROM large_table
WHERE rare_condition = true      -- Filters most rows
  AND common_condition = 'value'  -- Filters fewer rows
  AND another_condition > 100;
```

---

### Operator: `OR`

**Description:**  
Logical disjunction. Returns true if at least one operand is true.

**Syntax:**
```sql
condition1 OR condition2
```

**Inputs:**
- `condition1`, `condition2` - Boolean expressions

**Return type:**
- `BOOLEAN`

**Truth Table:**
| A | B | A OR B |
|---|---|--------|
| true | true | true |
| true | false | true |
| false | true | true |
| false | false | false |
| true | NULL | true |
| false | NULL | NULL |
| NULL | true | true |
| NULL | false | NULL |
| NULL | NULL | NULL |

**Examples:**

**Basic OR:**
```sql
-- At least one condition must be true
SELECT * FROM emp WHERE dept = 'IT' OR dept = 'Sales';

-- Multiple OR conditions
SELECT * FROM products
WHERE category = 'Electronics'
   OR category = 'Computers'
   OR category = 'Phones';
```

**OR with Different Types:**
```sql
-- Numeric OR
SELECT * FROM users
WHERE age < 18 OR age > 65;

-- String OR
SELECT * FROM orders
WHERE status = 'pending' OR status = 'processing';

-- Mixed conditions
SELECT * FROM products
WHERE price < 10 OR on_sale = true;
```

**OR with AND (Precedence):**
```sql
-- AND has higher precedence than OR
SELECT * FROM products
WHERE category = 'Electronics' AND price < 100 OR on_sale = true;
-- Evaluated as: ((category = 'Electronics') AND (price < 100)) OR (on_sale = true)

-- Use parentheses for clarity
SELECT * FROM products
WHERE category = 'Electronics' AND (price < 100 OR on_sale = true);
-- Evaluated as: (category = 'Electronics') AND ((price < 100) OR (on_sale = true))
```

**Multiple OR Conditions:**
```sql
-- Status check
SELECT * FROM orders
WHERE status = 'cancelled'
   OR status = 'refunded'
   OR status = 'failed';

-- Better: Use IN instead
SELECT * FROM orders
WHERE status IN ('cancelled', 'refunded', 'failed');
```

**OR with NULL:**
```sql
-- NULL in OR expression
SELECT * FROM users
WHERE email IS NULL OR phone IS NULL;
-- Returns users missing email OR phone (or both)

-- TRUE OR NULL = TRUE
SELECT * FROM products
WHERE in_stock = true OR discount IS NULL;
-- Returns in-stock products regardless of discount
```

**Complex OR Expressions:**
```sql
-- Combining multiple conditions
SELECT * FROM employees
WHERE (department = 'Sales' AND salary > 80000)
   OR (department = 'IT' AND years_experience > 5)
   OR (is_manager = true);
```

**Performance Consideration:**
```sql
-- OR can prevent index usage
SELECT * FROM users
WHERE first_name = 'John' OR last_name = 'Doe';
-- May require full table scan

-- Alternative: UNION (if indexes exist)
SELECT * FROM users WHERE first_name = 'John'
UNION
SELECT * FROM users WHERE last_name = 'Doe';
```

---

### Operator: `NOT`

**Description:**  
Logical negation. Inverts the boolean value.

**Syntax:**
```sql
NOT condition
```

**Inputs:**
- `condition` - Boolean expression

**Return type:**
- `BOOLEAN`

**Truth Table:**
| A | NOT A |
|---|-------|
| true | false |
| false | true |
| NULL | NULL |

**Examples:**

**Basic NOT:**
```sql
-- Negate boolean column
SELECT * FROM emp WHERE NOT active;
-- Same as: WHERE active = false

-- Negate comparison
SELECT * FROM products WHERE NOT (price > 100);
-- Same as: WHERE price <= 100
```

**NOT with IN:**
```sql
-- Exclude values
SELECT * FROM orders WHERE NOT status IN ('cancelled', 'refunded');
-- Same as: WHERE status NOT IN ('cancelled', 'refunded')

-- Explicit NOT
SELECT * FROM products WHERE NOT (category IN ('Discontinued', 'Obsolete'));
```

**NOT with BETWEEN:**
```sql
-- Outside range
SELECT * FROM products WHERE NOT (price BETWEEN 50 AND 100);
-- Same as: WHERE price NOT BETWEEN 50 AND 100
-- Same as: WHERE price < 50 OR price > 100
```

**NOT with LIKE:**
```sql
-- Exclude pattern
SELECT * FROM users WHERE NOT (email LIKE '%@spam.com');
-- Same as: WHERE email NOT LIKE '%@spam.com'

-- Multiple NOT LIKE
SELECT * FROM products
WHERE NOT (name LIKE '%discontinued%')
  AND NOT (name LIKE '%obsolete%');
```

**NOT with IS NULL:**
```sql
-- Has value
SELECT * FROM emp WHERE NOT (manager IS NULL);
-- Same as: WHERE manager IS NOT NULL

-- Both fields have values
SELECT * FROM contacts
WHERE NOT (email IS NULL OR phone IS NULL);
-- Same as: WHERE email IS NOT NULL AND phone IS NOT NULL
```

**NOT with AND/OR:**
```sql
-- De Morgan's Law: NOT (A AND B) = (NOT A) OR (NOT B)
SELECT * FROM users
WHERE NOT (is_active = true AND is_verified = true);
-- Same as: WHERE is_active = false OR is_verified = false

-- De Morgan's Law: NOT (A OR B) = (NOT A) AND (NOT B)
SELECT * FROM products
WHERE NOT (category = 'Discontinued' OR in_stock = false);
-- Same as: WHERE category != 'Discontinued' AND in_stock = true
```

**Double Negation:**
```sql
-- NOT NOT = identity
SELECT * FROM users WHERE NOT (NOT is_active);
-- Same as: WHERE is_active

-- Can be confusing, avoid in practice
SELECT * FROM products WHERE NOT (NOT (price > 100));
-- Same as: WHERE price > 100
```

**NOT with Complex Expressions:**
```sql
-- Negate entire condition
SELECT * FROM orders
WHERE NOT (
  status = 'completed' 
  AND payment_status = 'paid' 
  AND total_amount > 1000
);
-- Returns orders that don't meet ALL three conditions

-- Negate with parentheses
SELECT * FROM employees
WHERE NOT (department = 'Sales' AND salary < 50000);
-- Returns non-Sales employees OR Sales employees earning >= 50000
```

**NOT with EXISTS:**
```sql
-- Find customers without orders
SELECT * FROM customers c
WHERE NOT EXISTS (
  SELECT 1 FROM orders o WHERE o.customer_id = c.id
);
```

**Practical Examples:**
```sql
-- Exclude inactive and unverified users
SELECT * FROM users
WHERE NOT (is_active = false OR is_verified = false);
-- Same as: WHERE is_active = true AND is_verified = true

-- Products not in specific categories
SELECT * FROM products
WHERE NOT (category IN ('Discontinued', 'Clearance', 'Obsolete'));

-- Orders not in terminal states
SELECT * FROM orders
WHERE NOT (status IN ('completed', 'cancelled', 'refunded'));

-- Users without complete profile
SELECT * FROM users
WHERE NOT (
  email IS NOT NULL 
  AND phone IS NOT NULL 
  AND address IS NOT NULL
);
```

**NULL Handling:**
```sql
-- NOT NULL = NULL (not false!)
SELECT * FROM products WHERE NOT (discount IS NULL);
-- Same as: WHERE discount IS NOT NULL

-- NOT with NULL comparison
SELECT * FROM users WHERE NOT (status = NULL);
-- Always returns no rows (NULL comparison is always NULL)
-- Use: WHERE status IS NOT NULL
```

**Best Practices:**
```sql
-- Good: Use positive logic when possible
SELECT * FROM users WHERE is_active = true;

-- Avoid: Double negatives
SELECT * FROM users WHERE NOT (is_active = false);

-- Good: Use specific operators
SELECT * FROM products WHERE category NOT IN ('A', 'B');

-- Avoid: NOT with IN
SELECT * FROM products WHERE NOT (category IN ('A', 'B'));
```

---

### Combining Logical Operators

**AND + OR + NOT:**
```sql
-- Complex business logic
SELECT * FROM orders
WHERE (
    (status = 'pending' AND created_date < DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY))
    OR (status = 'processing' AND priority = 'high')
  )
  AND NOT (customer_type = 'blocked')
  AND total_amount > 0;
```

**Precedence Reminder:**
1. `NOT` (highest)
2. `AND`
3. `OR` (lowest)

```sql
-- Without parentheses (follows precedence)
SELECT * FROM products
WHERE NOT in_stock AND price < 100 OR on_sale = true;
-- Evaluated as: ((NOT in_stock) AND (price < 100)) OR (on_sale = true)

-- With parentheses (explicit)
SELECT * FROM products
WHERE NOT (in_stock AND price < 100) OR on_sale = true;
-- Evaluated as: (NOT (in_stock AND price < 100)) OR (on_sale = true)
```

**De Morgan's Laws:**
```sql
-- NOT (A AND B) = (NOT A) OR (NOT B)
SELECT * FROM users
WHERE NOT (is_active = true AND is_verified = true);
-- Equivalent to:
SELECT * FROM users
WHERE is_active = false OR is_verified = false;

-- NOT (A OR B) = (NOT A) AND (NOT B)
SELECT * FROM products
WHERE NOT (category = 'A' OR category = 'B');
-- Equivalent to:
SELECT * FROM products
WHERE category != 'A' AND category != 'B';
-- Or better:
SELECT * FROM products
WHERE category NOT IN ('A', 'B');
```

---

## Cast Operators

### Operator: `::`

**Description:**  
Provides an alternative syntax to the [CAST](functions_type_conversion.md#type-conversion-functions) function. PostgreSQL-style type casting.

**Syntax:**
```sql
expr::TYPE
```

**Inputs:**
- `expr` - Expression to convert
- `TYPE` - Target data type (`DATE`, `TIMESTAMP`, `VARCHAR`, `INT`, `DOUBLE`, etc.)

**Return type:**
- `TYPE`

**Examples:**

**Basic Type Casting:**
```sql
-- String to DATE
SELECT hire_date::DATE FROM emp;

-- String to INT
SELECT '123'::INT AS num;
-- Result: 123

-- String to DOUBLE
SELECT '123.45'::DOUBLE AS num;
-- Result: 123.45

-- INT to VARCHAR
SELECT 12345::VARCHAR AS str;
-- Result: '12345'
```

**Date and Time Casting:**
```sql
-- String to DATE
SELECT '2025-01-10'::DATE AS d;
-- Result: 2025-01-10

-- String to TIMESTAMP
SELECT '2025-01-10 14:30:00'::TIMESTAMP AS ts;
-- Result: 2025-01-10 14:30:00

-- TIMESTAMP to DATE
SELECT CURRENT_TIMESTAMP::DATE AS today;
-- Result: 2025-10-27

-- Date string with explicit cast
SELECT order_date::DATE FROM orders;
```

**Numeric Casting:**
```sql
-- INT to DOUBLE
SELECT 100::DOUBLE AS d;
-- Result: 100.0

-- DOUBLE to INT (truncates)
SELECT 123.99::INT AS i;
-- Result: 123

-- String to DECIMAL
SELECT '123.45'::DECIMAL(10, 2) AS dec;
-- Result: 123.45
```

**Boolean Casting:**
```sql
-- String to BOOLEAN
SELECT 'true'::BOOLEAN AS b;
-- Result: true

SELECT 'false'::BOOLEAN AS b;
-- Result: false

-- INT to BOOLEAN
SELECT 1::BOOLEAN AS b;
-- Result: true

SELECT 0::BOOLEAN AS b;
-- Result: false
```

**In WHERE Clause:**
```sql
-- Cast for comparison
SELECT * FROM orders
WHERE order_date::DATE >= '2025-01-01'::DATE;

-- Cast string to number
SELECT * FROM products
WHERE price_str::DOUBLE > 100;

-- Cast to timestamp
SELECT * FROM events
WHERE event_time::TIMESTAMP >= '2025-01-10 00:00:00'::TIMESTAMP;
```

**In Calculations:**
```sql
-- Force float division
SELECT total::DOUBLE / count::DOUBLE AS average
FROM statistics;

-- Cast for arithmetic
SELECT (price_str::DOUBLE * quantity_str::INT) AS total
FROM order_items;
```

**Chained Casting:**
```sql
-- Multiple casts
SELECT hire_date::VARCHAR::DATE FROM emp;
-- First to VARCHAR, then to DATE

-- Cast then manipulate
SELECT (salary::VARCHAR || ' USD') AS formatted_salary
FROM employees;
```

**Comparison with CAST Function:**
```sql
-- Using :: operator (PostgreSQL style)
SELECT hire_date::DATE FROM emp;

-- Using CAST function (standard SQL)
SELECT CAST(hire_date AS DATE) FROM emp;

-- Both are equivalent, choose based on preference
-- :: is shorter and more readable
-- CAST is more standard and portable
```

**Complex Examples:**
```sql
-- Cast in JOIN condition
SELECT o.*, p.*
FROM orders o
JOIN products p ON o.product_id::VARCHAR = p.product_code;

-- Cast in GROUP BY
SELECT 
  order_date::DATE AS order_day,
  COUNT(*) AS order_count
FROM orders
GROUP BY order_date::DATE;

-- Cast in CASE expression
SELECT 
  product_id,
  CASE 
    WHEN stock_str::INT > 100 THEN 'High'
    WHEN stock_str::INT > 50 THEN 'Medium'
    ELSE 'Low'
  END AS stock_level
FROM inventory;
```

**Error Handling:**
```sql
-- Invalid cast throws error
SELECT 'not-a-number'::INT;
-- ERROR: Cannot cast 'not-a-number' to INT

-- Use TRY_CAST for safe casting
SELECT TRY_CAST('not-a-number' AS INT);
-- Result: NULL

-- Note: :: operator doesn't have a "try" version
-- Use CAST/TRY_CAST for error handling
```

**NULL Handling:**
```sql
-- NULL cast returns NULL
SELECT NULL::INT AS result;
-- Result: NULL

-- Cast NULL column
SELECT price::DOUBLE FROM products;
-- NULL prices remain NULL
```

**Best Practices:**
```sql
-- Good: Use :: for readability
SELECT created_at::DATE FROM users;

-- Good: Use CAST for compatibility
SELECT CAST(created_at AS DATE) FROM users;

-- Good: Cast both sides of comparison
SELECT * FROM orders
WHERE order_date::DATE = '2025-01-10'::DATE;

-- Avoid: Implicit type conversion (may cause issues)
SELECT * FROM orders WHERE order_date = '2025-01-10';
```

---

### Summary: Cast Operators

| Syntax                   | Example                  | Notes                       |
|--------------------------|--------------------------|-----------------------------|
| `::TYPE`                 | `'123'::INT`             | PostgreSQL style, shorter   |
| `CAST(expr AS TYPE)`     | `CAST('123' AS INT)`     | Standard SQL, more portable |
| `CONVERT(expr, TYPE)`    | `CONVERT('123', INT)`    | MySQL style (if supported)  |
| `TRY_CAST(expr AS TYPE)` | `TRY_CAST('abc' AS INT)` | Returns NULL on error       |

**When to use `::`:**
- PostgreSQL-compatible systems
- Quick, readable casts
- When you're confident the cast will succeed

**When to use `CAST`:**
- Maximum SQL portability
- When writing cross-database queries
- Corporate/enterprise environments

**When to use `TRY_CAST`:**
- Uncertain data quality
- User input
- ETL/data import operations
- When you want NULL instead of errors

[Back to index](README.md)
