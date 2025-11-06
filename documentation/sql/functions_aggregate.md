[Back to index](README.md)

# Aggregate Functions

**Navigation:** [Functions — Date / Time](functions_date_time.md) · [Functions — Conditional](functions_conditional.md)

This page documents aggregate functions for summarizing and analyzing data.

---

## Table of Contents

1. [COUNT](#function-count)
2. [SUM](#function-sum)
3. [AVG](#function-avg)
4. [MIN](#function-min)
5. [MAX](#function-max)
6. [FIRST_VALUE](#function-first_value)
7. [LAST_VALUE](#function-last_value)
8. [ARRAY_AGG](#function-array_agg)

---

## Overview

Aggregate functions perform calculations on sets of rows and return a single result. They are commonly used with `GROUP BY` clauses to summarize data by categories.

**Key Concepts:**
- **Aggregate Functions**: Operate on multiple rows to produce a single result
- **GROUP BY**: Groups rows that have the same values in specified columns
- **HAVING**: Filters groups based on aggregate conditions
- **Window Functions**: Perform calculations across rows related to the current row

---

## Function: COUNT

**Description:**  
Count rows or non-null expressions. With `DISTINCT` counts distinct values.

**Syntax:**
```sql
COUNT(*)
COUNT(expr)
COUNT(DISTINCT expr)
```

**Inputs:**
- `*` - Count all rows (including NULLs)
- `expr` - Count non-NULL values in expression
- `DISTINCT expr` - Count distinct non-NULL values

**Output:**
- `BIGINT` - Number of rows/values

**NULL Handling:**
- `COUNT(*)` includes rows with NULL values
- `COUNT(expr)` excludes NULL values
- `COUNT(DISTINCT expr)` excludes NULL values

**Examples:**

**Basic COUNT:**
```sql
-- Count all rows
SELECT COUNT(*) AS total FROM emp;
-- Result: total = 42

-- Count non-NULL values
SELECT COUNT(manager) AS employees_with_manager FROM emp;

-- Count with WHERE
SELECT COUNT(*) AS it_employees 
FROM emp 
WHERE department = 'IT';
```

**COUNT DISTINCT:**
```sql
-- Count distinct salaries
SELECT COUNT(DISTINCT salary) AS distinct_salaries FROM emp;
-- Result: 8

-- Count distinct departments
SELECT COUNT(DISTINCT department) AS dept_count FROM emp;

-- Count distinct non-NULL emails
SELECT COUNT(DISTINCT email) AS unique_emails FROM users;
```

**COUNT with GROUP BY:**
```sql
-- Count employees per department
SELECT 
  department,
  COUNT(*) AS employee_count
FROM emp
GROUP BY department;

-- Count orders per customer
SELECT 
  customer_id,
  COUNT(*) AS order_count
FROM orders
GROUP BY customer_id;

-- Count with multiple columns
SELECT 
  department,
  job_title,
  COUNT(*) AS count
FROM emp
GROUP BY department, job_title;
```

**COUNT with HAVING:**
```sql
-- Departments with more than 10 employees
SELECT 
  department,
  COUNT(*) AS count
FROM emp
GROUP BY department
HAVING COUNT(*) > 10;

-- Customers with multiple orders
SELECT 
  customer_id,
  COUNT(*) AS order_count
FROM orders
GROUP BY customer_id
HAVING COUNT(*) > 1;
```

**COUNT vs COUNT(*):**
```sql
-- COUNT(*) counts all rows
SELECT COUNT(*) AS total_rows FROM emp;
-- Result: 42 (includes rows with NULL values)

-- COUNT(column) counts non-NULL values
SELECT COUNT(email) AS rows_with_email FROM emp;
-- Result: 38 (excludes 4 NULL emails)

-- Difference shows NULL count
SELECT 
  COUNT(*) AS total,
  COUNT(email) AS with_email,
  COUNT(*) - COUNT(email) AS without_email
FROM emp;
```

**Practical Examples:**

**1. Data quality check:**
```sql
SELECT 
  COUNT(*) AS total_records,
  COUNT(email) AS records_with_email,
  COUNT(phone) AS records_with_phone,
  COUNT(CASE WHEN email IS NOT NULL AND phone IS NOT NULL THEN 1 END) AS complete_records
FROM contacts;
```

**2. Completion rate:**
```sql
SELECT 
  department,
  COUNT(*) AS total,
  COUNT(performance_review) AS reviewed,
  ROUND(COUNT(performance_review) * 100.0 / COUNT(*), 2) AS review_completion_rate
FROM employees
GROUP BY department;
```

**3. Active users:**
```sql
SELECT 
  DATE_TRUNC('month', login_date) AS month,
  COUNT(DISTINCT user_id) AS active_users
FROM user_logins
WHERE login_date >= DATE_SUB(CURRENT_DATE, INTERVAL 6 MONTH)
GROUP BY DATE_TRUNC('month', login_date)
ORDER BY month;
```

---

## Function: SUM

**Description:**  
Sum of values.

**Syntax:**
```sql
SUM(expr)
SUM(DISTINCT expr)
```

**Inputs:**
- `expr` - Numeric expression (`INT`, `DOUBLE`, `DECIMAL`, etc.)
- `DISTINCT expr` - Sum of distinct values only

**Output:**
- `NUMERIC` - Sum of values (same type as input, or promoted)

**NULL Handling:**
- NULL values are ignored
- If all values are NULL, returns NULL
- Empty set returns NULL

**Examples:**

**Basic SUM:**
```sql
-- Total salary
SELECT SUM(salary) AS total_salary FROM emp;

-- Total revenue
SELECT SUM(amount) AS total_revenue FROM sales;

-- Sum with WHERE
SELECT SUM(salary) AS it_total_salary 
FROM emp 
WHERE department = 'IT';
```

**SUM with GROUP BY:**
```sql
-- Total salary per department
SELECT 
  department,
  SUM(salary) AS total_salary
FROM emp
GROUP BY department;

-- Revenue per product
SELECT 
  product_id,
  SUM(quantity * price) AS total_revenue
FROM order_items
GROUP BY product_id;

-- Monthly sales
SELECT 
  DATE_TRUNC('month', order_date) AS month,
  SUM(total_amount) AS monthly_revenue
FROM orders
GROUP BY DATE_TRUNC('month', order_date)
ORDER BY month;
```

**SUM DISTINCT:**
```sql
-- Sum of distinct salaries (removes duplicates)
SELECT SUM(DISTINCT salary) AS sum_distinct_salaries FROM emp;

-- Sum of unique prices
SELECT SUM(DISTINCT price) AS sum_unique_prices FROM products;
```

**SUM with Calculations:**
```sql
-- Total compensation (salary + bonus)
SELECT 
  department,
  SUM(salary + COALESCE(bonus, 0)) AS total_compensation
FROM emp
GROUP BY department;

-- Total order value with tax
SELECT 
  SUM(subtotal + tax + shipping) AS total_order_value
FROM orders;

-- Weighted average preparation
SELECT 
  SUM(score * weight) AS weighted_sum,
  SUM(weight) AS total_weight
FROM test_scores;
```

**SUM with CASE:**
```sql
-- Conditional sum
SELECT 
  SUM(CASE WHEN status = 'completed' THEN amount ELSE 0 END) AS completed_revenue,
  SUM(CASE WHEN status = 'pending' THEN amount ELSE 0 END) AS pending_revenue
FROM orders;

-- Sum by category
SELECT 
  department,
  SUM(CASE WHEN gender = 'M' THEN salary ELSE 0 END) AS male_total,
  SUM(CASE WHEN gender = 'F' THEN salary ELSE 0 END) AS female_total
FROM emp
GROUP BY department;
```

**Practical Examples:**

**1. Financial summary:**
```sql
SELECT 
  DATE_TRUNC('quarter', order_date) AS quarter,
  SUM(total_amount) AS revenue,
  SUM(cost) AS expenses,
  SUM(total_amount - cost) AS profit
FROM orders
GROUP BY DATE_TRUNC('quarter', order_date)
ORDER BY quarter;
```

**2. Inventory value:**
```sql
SELECT 
  category,
  SUM(quantity * unit_price) AS inventory_value
FROM inventory
GROUP BY category
ORDER BY inventory_value DESC;
```

**3. Running total (with window function):**
```sql
SELECT 
  order_date,
  amount,
  SUM(amount) OVER (ORDER BY order_date) AS running_total
FROM orders
ORDER BY order_date;
```

---

## Function: AVG

**Description:**  
Average of values.

**Syntax:**
```sql
AVG(expr)
AVG(DISTINCT expr)
```

**Inputs:**
- `expr` - Numeric expression
- `DISTINCT expr` - Average of distinct values only

**Output:**
- `DOUBLE` - Average value

**NULL Handling:**
- NULL values are ignored
- If all values are NULL, returns NULL
- Empty set returns NULL

**Examples:**

**Basic AVG:**
```sql
-- Average salary
SELECT AVG(salary) AS avg_salary FROM emp;

-- Average with WHERE
SELECT AVG(salary) AS avg_it_salary 
FROM emp 
WHERE department = 'IT';

-- Average order value
SELECT AVG(total_amount) AS avg_order_value FROM orders;
```

**AVG with GROUP BY:**
```sql
-- Average salary per department
SELECT 
  department,
  AVG(salary) AS avg_salary
FROM emp
GROUP BY department;

-- Average rating per product
SELECT 
  product_id,
  AVG(rating) AS avg_rating,
  COUNT(*) AS review_count
FROM reviews
GROUP BY product_id;

-- Average daily sales
SELECT 
  DATE_TRUNC('day', order_date) AS day,
  AVG(total_amount) AS avg_daily_order
FROM orders
GROUP BY DATE_TRUNC('day', order_date)
ORDER BY day;
```

**AVG DISTINCT:**
```sql
-- Average of distinct salaries
SELECT AVG(DISTINCT salary) AS avg_distinct_salary FROM emp;

-- Average of unique prices
SELECT 
  category,
  AVG(DISTINCT price) AS avg_unique_price
FROM products
GROUP BY category;
```

**AVG with Rounding:**
```sql
-- Round average to 2 decimals
SELECT 
  department,
  ROUND(AVG(salary), 2) AS avg_salary
FROM emp
GROUP BY department;

-- Format as currency
SELECT 
  CONCAT('$', ROUND(AVG(salary), 2)) AS avg_salary_formatted
FROM emp;
```

**AVG vs Manual Calculation:**
```sql
-- Using AVG function
SELECT AVG(salary) AS avg_salary FROM emp;

-- Manual calculation (equivalent)
SELECT SUM(salary) / COUNT(salary) AS avg_salary FROM emp;

-- Difference with NULL handling
SELECT 
  AVG(bonus) AS avg_with_function,
  SUM(bonus) / COUNT(*) AS avg_manual_all,
  SUM(bonus) / COUNT(bonus) AS avg_manual_non_null
FROM emp;
```

**Practical Examples:**

**1. Performance metrics:**
```sql
SELECT 
  employee_id,
  AVG(sales_amount) AS avg_sale,
  AVG(customer_rating) AS avg_rating,
  COUNT(*) AS total_sales
FROM sales
WHERE sale_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 MONTH)
GROUP BY employee_id
HAVING AVG(sales_amount) > 1000;
```

**2. Grade analysis:**
```sql
SELECT 
  course_id,
  AVG(score) AS avg_score,
  MIN(score) AS min_score,
  MAX(score) AS max_score,
  COUNT(*) AS student_count
FROM exam_results
GROUP BY course_id
ORDER BY avg_score DESC;
```

**3. Response time analysis:**
```sql
SELECT 
  service_name,
  AVG(response_time_ms) AS avg_response,
  PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY response_time_ms) AS p95_response
FROM api_logs
WHERE log_date >= CURRENT_DATE - INTERVAL 1 DAY
GROUP BY service_name;
```

---

## Function: MIN

**Description:**  
Minimum value in group.

**Syntax:**
```sql
MIN(expr)
```

**Inputs:**
- `expr` - Any comparable type (numeric, string, date, etc.)

**Output:**
- Same type as input

**NULL Handling:**
- NULL values are ignored
- If all values are NULL, returns NULL
- Empty set returns NULL

**Examples:**

**Basic MIN:**
```sql
-- Minimum salary
SELECT MIN(salary) AS min_salary FROM emp;

-- Earliest hire date
SELECT MIN(hire_date) AS earliest FROM emp;

-- Lowest price
SELECT MIN(price) AS lowest_price FROM products;
```

**MIN with Different Types:**
```sql
-- Numeric MIN
SELECT MIN(age) AS youngest FROM users;

-- Date MIN
SELECT MIN(order_date) AS first_order FROM orders;

-- String MIN (alphabetically first)
SELECT MIN(name) AS first_alphabetically FROM products;

-- Timestamp MIN
SELECT MIN(created_at) AS earliest_record FROM logs;
```

**MIN with GROUP BY:**
```sql
-- Minimum salary per department
SELECT 
  department,
  MIN(salary) AS min_salary
FROM emp
GROUP BY department;

-- Earliest order per customer
SELECT 
  customer_id,
  MIN(order_date) AS first_order_date
FROM orders
GROUP BY customer_id;

-- Lowest price per category
SELECT 
  category,
  MIN(price) AS min_price,
  MAX(price) AS max_price
FROM products
GROUP BY category;
```

**MIN with WHERE:**
```sql
-- Minimum salary in IT department
SELECT MIN(salary) AS min_it_salary 
FROM emp 
WHERE department = 'IT';

-- Earliest order in 2025
SELECT MIN(order_date) AS first_2025_order 
FROM orders 
WHERE YEAR(order_date) = 2025;
```

**Practical Examples:**

**1. Find oldest/newest records:**
```sql
SELECT 
  customer_id,
  MIN(order_date) AS first_order,
  MAX(order_date) AS last_order,
  DATEDIFF(MAX(order_date), MIN(order_date)) AS customer_lifetime_days
FROM orders
GROUP BY customer_id;
```

**2. Price range analysis:**
```sql
SELECT 
  category,
  MIN(price) AS min_price,
  AVG(price) AS avg_price,
  MAX(price) AS max_price,
  MAX(price) - MIN(price) AS price_range
FROM products
GROUP BY category;
```

**3. Performance bounds:**
```sql
SELECT 
  server_name,
  MIN(response_time) AS best_response,
  AVG(response_time) AS avg_response,
  MAX(response_time) AS worst_response
FROM server_logs
WHERE log_date = CURRENT_DATE
GROUP BY server_name;
```

---

## Function: MAX

**Description:**  
Maximum value in group.

**Syntax:**
```sql
MAX(expr)
```

**Inputs:**
- `expr` - Any comparable type (numeric, string, date, etc.)

**Output:**
- Same type as input

**NULL Handling:**
- NULL values are ignored
- If all values are NULL, returns NULL
- Empty set returns NULL

**Examples:**

**Basic MAX:**
```sql
-- Maximum salary
SELECT MAX(salary) AS top_salary FROM emp;

-- Latest hire date
SELECT MAX(hire_date) AS most_recent FROM emp;

-- Highest price
SELECT MAX(price) AS highest_price FROM products;
```

**MAX with Different Types:**
```sql
-- Numeric MAX
SELECT MAX(age) AS oldest FROM users;

-- Date MAX
SELECT MAX(order_date) AS last_order FROM orders;

-- String MAX (alphabetically last)
SELECT MAX(name) AS last_alphabetically FROM products;

-- Timestamp MAX
SELECT MAX(updated_at) AS latest_update FROM records;
```

**MAX with GROUP BY:**
```sql
-- Maximum salary per department
SELECT 
  department,
  MAX(salary) AS max_salary
FROM emp
GROUP BY department;

-- Latest order per customer
SELECT 
  customer_id,
  MAX(order_date) AS last_order_date
FROM orders
GROUP BY customer_id;

-- Highest price per category
SELECT 
  category,
  MAX(price) AS max_price
FROM products
GROUP BY category;
```

**MAX with WHERE:**
```sql
-- Maximum salary in IT department
SELECT MAX(salary) AS max_it_salary 
FROM emp 
WHERE department = 'IT';

-- Latest order in 2025
SELECT MAX(order_date) AS last_2025_order 
FROM orders 
WHERE YEAR(order_date) = 2025;
```

**Practical Examples:**

**1. Find top performers:**
```sql
SELECT 
  department,
  MAX(salary) AS top_salary,
  AVG(salary) AS avg_salary,
  MAX(salary) - AVG(salary) AS gap_to_top
FROM emp
GROUP BY department;
```

**2. Latest activity:**
```sql
SELECT 
  user_id,
  MAX(login_date) AS last_login,
  DATEDIFF(CURRENT_DATE, MAX(login_date)) AS days_since_login
FROM user_logins
GROUP BY user_id
HAVING DATEDIFF(CURRENT_DATE, MAX(login_date)) > 30;
```

**3. Peak values:**
```sql
SELECT 
  DATE_TRUNC('day', timestamp) AS day,
  MAX(cpu_usage) AS peak_cpu,
  MAX(memory_usage) AS peak_memory,
  MAX(active_connections) AS peak_connections
FROM system_metrics
WHERE timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 7 DAY)
GROUP BY DATE_TRUNC('day', timestamp)
ORDER BY day;
```

---

## Function: FIRST_VALUE

**Description:**  
Window function: returns the first value in an ordered partition. Pushed as `top_hits size=1` to Elasticsearch when possible.

**Syntax:**
```sql
FIRST_VALUE(expr) OVER (
  [PARTITION BY partition_expr, ...]
  [ORDER BY order_expr [ASC|DESC], ...]
)
```

**Inputs:**
- `expr` - Expression to return
- `PARTITION BY` - Optional grouping columns
- `ORDER BY` - Ordering specification (if not provided, only expr column name is used for sorting)

**Output:**
- Same type as input expression

**Behavior:**
- Returns the first value based on `ORDER BY` within each partition
- If `OVER` is not provided, only the expr column name is used for sorting
- Optimized to Elasticsearch `top_hits` aggregation with `size=1`

**Examples:**

**Basic FIRST_VALUE:**
```sql
-- First salary in each department (ordered by hire date)
SELECT 
  department,
  name,
  salary,
  FIRST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS first_salary
FROM emp;
```

**Without PARTITION BY:**
```sql
-- First hire across entire company
SELECT 
  name,
  hire_date,
  FIRST_VALUE(name) OVER (ORDER BY hire_date ASC) AS first_hired_employee
FROM emp;
```

**Without OVER clause:**
```sql
-- Uses expr column name for sorting
SELECT 
  department,
  FIRST_VALUE(salary) AS first_salary_value
FROM emp;
```

**Multiple Partitions:**
```sql
-- First employee hired in each department and job title
SELECT 
  department,
  job_title,
  name,
  hire_date,
  FIRST_VALUE(name) OVER (
    PARTITION BY department, job_title
    ORDER BY hire_date ASC
  ) AS first_in_role
FROM emp;
```

**Practical Examples:**

**1. First purchase per customer:**
```sql
SELECT 
  customer_id,
  order_id,
  order_date,
  total_amount,
  FIRST_VALUE(total_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date ASC
  ) AS first_order_amount
FROM orders;
```

**2. Initial stock price:**
```sql
SELECT 
  stock_symbol,
  trade_date,
  closing_price,
  FIRST_VALUE(closing_price) OVER (
    PARTITION BY stock_symbol
    ORDER BY trade_date ASC
  ) AS initial_price
FROM stock_prices;
```

**3. Baseline metrics:**
```sql
SELECT 
  server_name,
  timestamp,
  cpu_usage,
  FIRST_VALUE(cpu_usage) OVER (
    PARTITION BY server_name
    ORDER BY timestamp ASC
  ) AS baseline_cpu
FROM server_metrics
WHERE DATE(timestamp) = CURRENT_DATE;
```

**Comparison with MIN:**
```sql
-- FIRST_VALUE (order-dependent)
SELECT 
  department,
  FIRST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS first_hired_salary
FROM emp;

-- MIN (value-dependent)
SELECT 
  department,
  MIN(salary) AS lowest_salary
FROM emp
GROUP BY department;
```

---

## Function: LAST_VALUE

**Description:**  
Window function: returns the last value in an ordered partition. Pushed to Elasticsearch by flipping sort order in `top_hits`.

**Syntax:**
```sql
LAST_VALUE(expr) OVER (
  [PARTITION BY partition_expr, ...]
  [ORDER BY order_expr [ASC|DESC], ...]
)
```

**Inputs:**
- `expr` - Expression to return
- `PARTITION BY` - Optional grouping columns
- `ORDER BY` - Ordering specification (if not provided, only expr column name is used for sorting)

**Output:**
- Same type as input expression

**Behavior:**
- Returns the last value based on `ORDER BY` within each partition
- If `OVER` is not provided, only the expr column name is used for sorting
- Optimized to Elasticsearch `top_hits` by reversing sort order

**Examples:**

**Basic LAST_VALUE:**
```sql
-- Last salary in each department (ordered by hire date)
SELECT 
  department,
  name,
  salary,
  LAST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS last_salary
FROM emp;
```

**Without PARTITION BY:**
```sql
-- Most recent hire across entire company
SELECT 
  name,
  hire_date,
  LAST_VALUE(name) OVER (ORDER BY hire_date ASC) AS last_hired_employee
FROM emp;
```

**Without OVER clause:**
```sql
-- Uses expr column name for sorting
SELECT 
  department,
  LAST_VALUE(salary) AS last_salary_value
FROM emp;
```

**Multiple Partitions:**
```sql
-- Last employee hired in each department and job title
SELECT 
  department,
  job_title,
  name,
  hire_date,
  LAST_VALUE(name) OVER (
    PARTITION BY department, job_title
    ORDER BY hire_date ASC
  ) AS last_in_role
FROM emp;
```

**Practical Examples:**

**1. Most recent purchase per customer:**
```sql
SELECT 
  customer_id,
  order_id,
  order_date,
  total_amount,
  LAST_VALUE(total_amount) OVER (
    PARTITION BY customer_id
    ORDER BY order_date ASC
  ) AS last_order_amount
FROM orders;
```

**2. Latest stock price:**
```sql
SELECT 
  stock_symbol,
  trade_date,
  closing_price,
  LAST_VALUE(closing_price) OVER (
    PARTITION BY stock_symbol
    ORDER BY trade_date ASC
  ) AS current_price
FROM stock_prices;
```

**3. Current status:**
```sql
SELECT 
  user_id,
  status_change_date,
  status,
  LAST_VALUE(status) OVER (
    PARTITION BY user_id
    ORDER BY status_change_date ASC
  ) AS current_status
FROM user_status_history;
```

**Comparison with MAX:**
```sql
-- LAST_VALUE (order-dependent)
SELECT 
  department,
  LAST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS last_hired_salary
FROM emp;

-- MAX (value-dependent)
SELECT 
  department,
  MAX(salary) AS highest_salary
FROM emp
GROUP BY department;
```

**FIRST_VALUE vs LAST_VALUE:**
```sql
-- Compare first and last values
SELECT 
  department,
  FIRST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS first_hire_salary,
  LAST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS last_hire_salary,
  LAST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) - FIRST_VALUE(salary) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS salary_change
FROM emp;
```

---

## Function: ARRAY_AGG

**Description:**  
Collect values into an array for each partition. Implemented using `OVER` and pushed to Elasticsearch as `top_hits`. Post-processing converts hits to an array of scalars.

**Syntax:**
```sql
ARRAY_AGG(expr) OVER (
  [PARTITION BY partition_expr, ...]
  [ORDER BY order_expr [ASC|DESC], ...]
)
```

**Inputs:**
- `expr` - Expression to collect
- `PARTITION BY` - Optional grouping columns
- `ORDER BY` - Optional ordering (if not provided, only expr column name is used for sorting)

**Output:**
- `ARRAY<type_of_expr>` - Array of collected values

**Behavior:**
- Collects all values of `expr` within each partition into an array
- If `OVER` is not provided, only the expr column name is used for sorting
- Optimized to Elasticsearch `top_hits` aggregation
- Post-processing converts hits to array of scalars

**Examples:**

**Basic ARRAY_AGG:**
```sql
-- Collect employee names per department
SELECT 
  department,
  ARRAY_AGG(name) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS employees
FROM emp
LIMIT 100;
-- Result: employees as an array of name values per department (sorted and limited)
```

**Without PARTITION BY:**
```sql
-- Collect all employee names (ordered)
SELECT 
  ARRAY_AGG(name) OVER (ORDER BY hire_date ASC) AS all_employees
FROM emp;
```

**Without OVER clause:**
```sql
-- Uses expr column name for sorting
SELECT 
  department,
  ARRAY_AGG(name) AS employee_list
FROM emp;
```

**With Multiple Columns:**
```sql
-- Collect salaries per department
SELECT 
  department,
  ARRAY_AGG(salary) OVER (
    PARTITION BY department 
    ORDER BY salary DESC
  ) AS salary_list
FROM emp;
```

**Ordered Collection:**
```sql
-- Collect products by price (high to low)
SELECT 
  category,
  ARRAY_AGG(product_name) OVER (
    PARTITION BY category 
    ORDER BY price DESC
  ) AS products_by_price
FROM products
LIMIT 100;
```

**Practical Examples:**

**1. Customer order history:**
```sql
SELECT 
  customer_id,
  ARRAY_AGG(order_id) OVER (
    PARTITION BY customer_id 
    ORDER BY order_date DESC
  ) AS order_history,
  ARRAY_AGG(total_amount) OVER (
    PARTITION BY customer_id 
    ORDER BY order_date DESC
  ) AS amount_history
FROM orders
LIMIT 1000;
```

**2. Product tags:**
```sql
SELECT 
  product_id,
  product_name,
  ARRAY_AGG(tag) OVER (
    PARTITION BY product_id 
    ORDER BY tag ASC
  ) AS tags
FROM product_tags
LIMIT 500;
```

**3. Timeline of events:**
```sql
SELECT 
  user_id,
  ARRAY_AGG(event_type) OVER (
    PARTITION BY user_id 
    ORDER BY event_timestamp ASC
  ) AS event_timeline,
  ARRAY_AGG(event_timestamp) OVER (
    PARTITION BY user_id 
    ORDER BY event_timestamp ASC
  ) AS timestamp_timeline
FROM user_events
WHERE event_timestamp >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
LIMIT 1000;
```

**4. Hierarchical data:**
```sql
SELECT 
  manager_id,
  ARRAY_AGG(employee_name) OVER (
    PARTITION BY manager_id 
    ORDER BY hire_date ASC
  ) AS direct_reports
FROM employees
WHERE manager_id IS NOT NULL
LIMIT 100;
```

**LIMIT Consideration:**
```sql
-- Always use LIMIT with ARRAY_AGG to prevent memory issues
SELECT 
  department,
  ARRAY_AGG(name) OVER (
    PARTITION BY department 
    ORDER BY hire_date ASC
  ) AS employees
FROM emp
LIMIT 100;  -- Important: limits result set size
```

**Comparison with STRING_AGG (if available):**
```sql
-- ARRAY_AGG returns array
SELECT 
  department,
  ARRAY_AGG(name) OVER (PARTITION BY department) AS name_array
FROM emp;
-- Result: ['John', 'Jane', 'Bob']

-- STRING_AGG returns string (if supported)
SELECT 
  department,
  STRING_AGG(name, ', ') AS name_string
FROM emp
GROUP BY department;
-- Result: 'John, Jane, Bob'
```

---

## Aggregate Functions Summary

| Function               | Purpose               | Input      | Output        | NULL Handling    |
|------------------------|-----------------------|------------|---------------|------------------|
| `COUNT(*)`             | Count all rows        | Any        | `BIGINT`      | Includes NULLs   |
| `COUNT(expr)`          | Count non-NULL values | Any        | `BIGINT`      | Excludes NULLs   |
| `COUNT(DISTINCT expr)` | Count distinct values | Any        | `BIGINT`      | Excludes NULLs   |
| `SUM(expr)`            | Sum values            | Numeric    | Numeric       | Ignores NULLs    |
| `AVG(expr)`            | Average values        | Numeric    | `DOUBLE`      | Ignores NULLs    |
| `MIN(expr)`            | Minimum value         | Comparable | Same as input | Ignores NULLs    |
| `MAX(expr)`            | Maximum value         | Comparable | Same as input | Ignores NULLs    |
| `FIRST_VALUE(expr)`    | First value (ordered) | Any        | Same as input | Depends on ORDER |
| `LAST_VALUE(expr)`     | Last value (ordered)  | Any        | Same as input | Depends on ORDER |
| `ARRAY_AGG(expr)`      | Collect into array    | Any        | `ARRAY<type>` | Includes NULLs   |

[Back to index](README.md)
