[Back to index](./README.md)

# Conditional Functions

This page documents conditional expressions.

---

### Function: CASE (searched form)  
**Name & Aliases:** `CASE WHEN ... THEN ... ELSE ... END` (searched CASE form)

**Description:**  
Evaluates boolean WHEN expressions in order; returns the result expression corresponding to the first true condition; if none match, returns the ELSE expression (or NULL if ELSE omitted).

**Syntax:**
```sql
CASE
  WHEN condition1 THEN result1
  WHEN condition2 THEN result2
  ...
  ELSE default_result
END
```

**Inputs:**  
- One or more `WHEN condition THEN result` pairs. Optional `ELSE result`.

**Output:**  
- Type coerced from result expressions (THEN/ELSE).

**Examples:**

**1. Salary banding:**
```sql
SELECT 
  name,
  salary,
  CASE
    WHEN salary > 100000 THEN 'very_high'
    WHEN salary > 50000 THEN 'high'
    ELSE 'normal'
  END AS salary_band
FROM emp
```

**2. Product status:**
```sql
SELECT 
  title,
  stock,
  CASE
    WHEN stock = 0 THEN 'Out of Stock'
    WHEN stock < 10 THEN 'Low Stock'
    WHEN stock < 50 THEN 'In Stock'
    ELSE 'Well Stocked'
  END AS stock_status
FROM products
```

**3. Discount calculation:**
```sql
SELECT 
  title,
  price,
  CASE
    WHEN price > 1000 THEN price * 0.85  -- 15% off
    WHEN price > 500 THEN price * 0.90   -- 10% off
    WHEN price > 100 THEN price * 0.95   -- 5% off
    ELSE price
  END AS discounted_price
FROM products
```

**4. Without ELSE (returns NULL if no match):**
```sql
SELECT 
  name,
  CASE
    WHEN age < 18 THEN 'Minor'
    WHEN age < 65 THEN 'Adult'
  END AS age_group
FROM persons
-- Returns NULL for age >= 65
```

---

### Function: CASE (simple / expression form)  
**Name & Aliases:** `CASE expr WHEN val1 THEN r1 WHEN val2 THEN r2 ... ELSE rN END` (simple CASE)

**Description:**  
Compare `expr` to `valN` sequentially using equality; returns corresponding `rN` for first match; else `ELSE` result or NULL.

**Syntax:**
```sql
CASE expression
  WHEN value1 THEN result1
  WHEN value2 THEN result2
  ...
  ELSE default_result
END
```

**Inputs:**  
- `expr` (any comparable type) and pairs `WHEN value THEN result`.

**Output:**  
- Type coerced from result expressions.

**Implementation notes:**  
The simple form evaluates by comparing `expr = value` for each WHEN. 
Both CASE forms are parsed and translated into nested conditional Painless scripts for `script_fields` when used outside an aggregation push-down.

**Examples:**

**1. Department categorization:**
```sql
SELECT 
  name,
  department,
  CASE department
    WHEN 'IT' THEN 'tech'
    WHEN 'Sales' THEN 'revenue'
    WHEN 'Marketing' THEN 'revenue'
    WHEN 'Engineering' THEN 'tech'
    ELSE 'other'
  END AS dept_category
FROM emp
```

**2. Status mapping:**
```sql
SELECT 
  order_id,
  CASE status
    WHEN 'P' THEN 'Pending'
    WHEN 'S' THEN 'Shipped'
    WHEN 'D' THEN 'Delivered'
    WHEN 'C' THEN 'Cancelled'
    ELSE 'Unknown'
  END AS status_label
FROM orders
```

**3. Priority levels:**
```sql
SELECT 
  ticket_id,
  CASE priority
    WHEN 1 THEN 'Critical'
    WHEN 2 THEN 'High'
    WHEN 3 THEN 'Medium'
    WHEN 4 THEN 'Low'
    ELSE 'Undefined'
  END AS priority_name
FROM tickets
```

**4. Numeric to text conversion:**
```sql
SELECT 
  product_id,
  CASE rating
    WHEN 5 THEN '★★★★★'
    WHEN 4 THEN '★★★★☆'
    WHEN 3 THEN '★★★☆☆'
    WHEN 2 THEN '★★☆☆☆'
    WHEN 1 THEN '★☆☆☆☆'
    ELSE 'No rating'
  END AS star_display
FROM reviews
```
---

### COALESCE

Returns the first non-null argument.

**Syntax:**
```sql
COALESCE(expr1, expr2, ...)
```

**Inputs:**
- One or more expressions

**Output:**
- Value of first non-null expression (coerced to common type)

**Examples:**

**1. Display name fallback:**
```sql
SELECT 
  COALESCE(nickname, firstname, 'N/A') AS display_name
FROM users

-- If nickname = 'Jo': returns 'Jo'
-- If nickname = NULL, firstname = 'John': returns 'John'
-- If both NULL: returns 'N/A'
```

**2. Default values:**
```sql
SELECT 
  title,
  COALESCE(discount_price, price) AS final_price
FROM products
-- Uses discount_price if available, otherwise regular price
```

**3. Multiple fallbacks:**
```sql
SELECT 
  COALESCE(mobile_phone, work_phone, home_phone, 'No phone') AS contact_phone
FROM customers
```

**4. Handling missing data:**
```sql
SELECT 
  name,
  COALESCE(email, 'no-email@example.com') AS email,
  COALESCE(country, 'Unknown') AS country
FROM users
```

---

### NULLIF

Returns NULL if expr1 = expr2; otherwise returns expr1.

**Syntax:**
```sql
NULLIF(expr1, expr2)
```

**Inputs:**
- `expr1` - first expression
- `expr2` - second expression

**Output:**
- Type of `expr1`, or NULL if equal

**Examples:**

**1. Normalize unknown values:**
```sql
SELECT 
  NULLIF(status, 'unknown') AS status_norm
FROM events

-- If status = 'unknown': returns NULL
-- If status = 'active': returns 'active'
```

**2. Handle sentinel values:**
```sql
SELECT 
  title,
  NULLIF(price, 0) AS valid_price
FROM products
-- Converts 0 prices to NULL
```

**3. Avoid division by zero:**
```sql
SELECT 
  total_sales / NULLIF(total_orders, 0) AS avg_order_value
FROM sales_summary
-- Returns NULL instead of error when total_orders = 0
```

**4. Clean data:**
```sql
SELECT 
  name,
  NULLIF(TRIM(description), '') AS description
FROM products
-- Converts empty strings to NULL after trimming
```

---

### ISNULL

Tests if expression is NULL.

**Syntax:**
```sql
ISNULL(expr)
```

**Inputs:**
- `expr` - expression to test

**Output:**
- `BOOLEAN` - TRUE if NULL, FALSE otherwise

**Examples:**

**1. Check for missing manager:**
```sql
SELECT 
  name,
  ISNULL(manager) AS manager_missing
FROM emp

-- Result: TRUE if manager is NULL, else FALSE
```

**2. Filter NULL values:**
```sql
SELECT * 
FROM products
WHERE ISNULL(description)
-- Returns products without description
```

**3. Count NULLs:**
```sql
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN ISNULL(email) THEN 1 ELSE 0 END) as missing_emails
FROM users
```

**4. Conditional logic:**
```sql
SELECT 
  name,
  CASE
    WHEN ISNULL(last_login) THEN 'Never logged in'
    ELSE 'Active user'
  END AS user_status
FROM users
```

---

### ISNOTNULL

Tests if expression is NOT NULL.

**Syntax:**
```sql
ISNOTNULL(expr)
```

**Inputs:**
- `expr` - expression to test

**Output:**
- `BOOLEAN` - TRUE if NOT NULL, FALSE if NULL

**Examples:**

**1. Check for existing manager:**
```sql
SELECT 
  name,
  ISNOTNULL(manager) AS has_manager
FROM emp

-- Result: TRUE if manager is NOT NULL, else FALSE
```

**2. Filter non-NULL values:**
```sql
SELECT * 
FROM products
WHERE ISNOTNULL(description)
-- Returns products with description
```

**3. Count non-NULLs:**
```sql
SELECT 
  COUNT(*) as total,
  SUM(CASE WHEN ISNOTNULL(email) THEN 1 ELSE 0 END) as with_emails
FROM users
```

**4. Required fields validation:**
```sql
SELECT 
  product_id,
  CASE
    WHEN ISNOTNULL(title) AND ISNOTNULL(price) AND ISNOTNULL(category) 
    THEN 'Valid'
    ELSE 'Incomplete'
  END AS validation_status
FROM products
```

[Back to index](./README.md)
