[Back to index](README.md)

# Conditional Functions

This page documents conditional expressions.

### The type of a conditional

*Since `0.24.0`.* `CASE`, `COALESCE`, `NULLIF`, `GREATEST` and `LEAST` answer **one type per
column**: the type they report, which is DuckDB's common type of the values they can return
(`DATE` with `DATE` is a `DATE`, `DATE` with `TIMESTAMP` a `TIMESTAMP`, `INT` with `DOUBLE` a
`DOUBLE`, a `BOOLEAN` with a number a number), and every row is converted to it. `NULLIF` reports
the type of its first argument. An argument whose type is not known — a column no mapping types — beside a `DATE` makes a
`TIMESTAMP`, as a `date_nanos` column does (a `TIMESTAMP` since `0.25.0`).

- **Arguments with no common type are refused**, once the column types are known, with DuckDB's
  reason: `CASE WHEN n > 0 THEN d ELSE n END` (a `DATE` beside an `INT`) answers *"Cannot mix values
  of type INT and DATE in CASE expression - an explicit cast is required"*, and `COALESCE` *"… in
  COALESCE operator …"*. That covers a number beside a date, a text column beside a number or a
  date, and a `TIME` beside a date or a timestamp. A `NULL`, a string literal, an aggregate and an
  argument of unknown type take no part in the check. Cast one side to the type you want.
- **A `NULL` literal argument is folded**, as DuckDB and PostgreSQL do: `NULLIF(x, NULL)` is `x`,
  `NULLIF(NULL, x)` is `NULL`, `COALESCE` drops a `NULL` argument, and `GREATEST` / `LEAST` skip
  one. With no other argument left, the result is `NULL`.
- *Since `0.25.0`.* **A quoted argument is read as the type of the arguments beside it**, as DuckDB
  reads it, in `COALESCE`, `GREATEST`, `LEAST` and the results of a `CASE`: beside a number it is
  the number it spells (beside a whole type, the whole number it rounds to, half away from zero),
  beside a `DATE` or a `TIMESTAMP` the date or the timestamp it names, beside a `BOOLEAN` the
  boolean it spells. `GREATEST(n, '1.5')` over an `INT` column answers `2`, an `INT` (it was
  refused); `COALESCE(n, '3')` answers `3`, an `INT`, where it answered the text `'3'` on every row;
  `COALESCE(d, '2024-01-15')` is a `DATE` (it was text). The literal takes the column's type —
  `GREATEST(t, '1.5')` over a `TINYINT` is a `TINYINT` — and a whole number past that type's range
  makes a `BIGINT` (`GREATEST(t, '200')` is `200`). Every reader of the type follows: `DESCRIBE`,
  JDBC and Arrow metadata, a `CREATE TABLE … AS SELECT` mapping, `ORDER BY`, a `GROUP BY` key, a
  comparison or a cast of the result. A quoted argument that does not spell a value of that type
  (`'abc'` beside a number, `'3'` beside a date) leaves the whole expression as it was:
  `COALESCE(n, 'abc')` answers text and `GREATEST(n, 'abc')` is refused.
- *Since `0.25.0`.* **A `BOOLEAN` beside whole numbers answers `1` or `0`**, their type, as DuckDB
  reads it: `COALESCE(b, 1)` answers `1` / `0` in a query (it answered `true` / `false` in an
  integer column), and `GREATEST(b, 1)` / `LEAST(b, 1)` answer in a query and in a computed column
  (they were refused in a query; a computed column stored no value). Beside a fractional number a
  `BOOLEAN` is left as it was, and `GREATEST` over booleans alone is refused.
- In a computed column (`SCRIPT AS`) and in a materialized view's computed field, a conditional is
  rendered as before `0.24.0`, so a stored pipeline does not change — except where a quoted argument
  is now read as a value (`COALESCE(n, '3')` stores the number `3`, and `'1.5'` beside an integer
  column stores `2` where Elasticsearch truncated the text to `1`), for `GREATEST` / `LEAST`
  over a `BOOLEAN` beside whole numbers, and for a `CASE` that compares a `BOOLEAN` with a number
  (below): `ALTER TABLE` re-creates such a pipeline. A quoted argument read as a date or a
  timestamp keeps the expression's `0.24.0` text type there (in a pipeline and in a transform
  alike), so a function over it parses it as before — `DATE_ADD(COALESCE(d, '2024-01-15'), INTERVAL
  1 DAY)` stores `2024-01-16` where `d` is missing — and a comparison, an `IN`, a `BETWEEN`, a
  `NULLIF` or a `CASE … WHEN` compares that text as before (`CASE WHEN COALESCE(d, '2024-01-15') =
  '2024-01-15' THEN 1 ELSE 0 END` keeps its pipeline). `CREATE TABLE` and `ALTER TABLE` judge the
  column on that text type too, as before: over a `KEYWORD` `k` and a `DATE` `d2`,
  `NULLIF(COALESCE(d, '2024-01-15'), k)` is accepted and `NULLIF(COALESCE(d, '2024-01-15'), d2)`
  is refused; `NULLIF(COALESCE(d, '2024-01-15'), k) - d2` and
  `COALESCE(NULLIF(COALESCE(d, '2024-01-15'), k), d2) - d2` are accepted (they were refused; the
  pipeline subtracts the dates as `COALESCE(d, '2024-01-15') - d2` does). A quoted argument read as
  a number makes the conditional a number there too, so `NULLIF(COALESCE(n, '3'), k)`,
  `NULLIF(k, COALESCE(n, '3'))`, `NULLIF(COALESCE(n, '3'), '2024-01-15')`,
  `COALESCE(NULLIF(COALESCE(n, '3'), k), k)` and `COALESCE(NULLIF(COALESCE(n, '3'), k), g)` are
  refused by `CREATE TABLE` and `ALTER TABLE` — including any `ALTER TABLE` of a table that holds
  one — where the pipeline compared the number with the text and stored no value on every
  document holding both. Beside a `DATE` it is stored as its date
  (`'2024-01-15 10:30:00'` stores `'2024-01-15'`, which the `date` field reads; it stored the text
  as written), beside a `TIMESTAMP` as written. In `CREATE TABLE` and `ALTER TABLE` this also holds
  under a cast: `CAST(CASE WHEN … THEN d ELSE '2024-01-15 10:30:00' END AS VARCHAR)` stores
  `'2024-01-15'` (it stored the text as written); its `AS DATE` and `AS TIMESTAMP` twins are refused
  by Elasticsearch when the pipeline is created, as before.
- *Since `0.25.0`.* **A `CASE` that compares a `BOOLEAN` with a number in a computed column**
  compares the boolean as `1` / `0`, as a query does (see
  [Comparing values of different types](operators.md#comparing-values-of-different-types)). The pipeline compared the boolean
  itself with the number and stored a wrong value or none (`b = 1` was never true, `b < 1` stored no
  value, `b BETWEEN 1 AND 1` failed every document, `b <> 1` stored `1` for a `"true"` source).
  With `b` a `BOOLEAN`, `n` an `INT`, `l` a `BIGINT` and `x` a `DOUBLE` column, these `SCRIPT AS`
  shapes render a new pipeline, the same list in `CREATE TABLE` and in `ALTER TABLE` (which
  re-creates it):
  - `CASE WHEN b op n` and `CASE WHEN n op b`, and the same two over `l`, for every `op` of `=`,
    `<>`, `!=`, `<`, `<=`, `>`, `>=`;
  - `CASE WHEN b op x` and `CASE WHEN x op b` for `=`, `<>`, `!=` (an ordering against a `DOUBLE`
    keeps its pipeline);
  - `CASE WHEN b op 1` for every `op` of `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`, and
    `CASE WHEN b IN (1)`, `CASE WHEN b NOT IN (1)`, `CASE WHEN b BETWEEN 1 AND 1`,
    `CASE WHEN b NOT BETWEEN 1 AND 1`; the same eleven with `0`;
  - `CASE WHEN b op 1.5` for `=`, `<>`, `!=`, and `CASE WHEN b IN (1.5)`,
    `CASE WHEN b NOT IN (1.5)`;
  - `CASE b WHEN v` for `v` of `n`, `l`, `x`, `1`, `0`, `1.5`, and `CASE v WHEN b` for `v` of `n`,
    `l`, `x`;
  - `GREATEST(b, v)` and `LEAST(b, v)` for `v` of `n`, `l`, `1`, `0`, and `GREATEST(v, b)`,
    `LEAST(v, b)` for `v` of `n`, `l` (above);
  - `NULLIF(b, v)` for `v` of `n`, `l`, `x`, `1`, `0`, `1.5`, and `NULLIF(v, b)` for `v` of `n`,
    `l`, `x`, which were refused and are accepted.
- *Since `0.25.0`.* **A string function over a number, a boolean or a conditional in a computed
  column** reads the value's text, as a query does (see
  [Type conversion](type_conversion.md)): `UPPER(n)`, `LENGTH(n)`, `SUBSTRING(n, 1, 1)`,
  `REVERSE(b)`, `UPPER(COALESCE(n, '3'))`, and `UPPER`, `LOWER`, `TRIM` or `LENGTH` over a `CASE`,
  or over a `COALESCE` of a number, a date or a timestamp, render a new pipeline, which
  `ALTER TABLE` re-creates. `UPPER(n)` stored no value;
  `LOWER(CASE WHEN … THEN 'A' ELSE 'B' END)` stored `'A'` unchanged, lowering only the `ELSE`.
- *Since `0.25.0`.* The new types reach every consumer: a `UNION ALL` branch (`COALESCE(n, '3')`
  is an `INT` slot, it was `VARCHAR`), a window function's argument, a nested conditional
  (`COALESCE(b, NULL, 'true')` takes `COALESCE(b, NULL, TRUE)`'s type), a cast
  (`CAST(CASE WHEN … THEN ti ELSE '1.5' END AS BIGINT)` converts the number `2` where it parsed the
  text `'1.5'` and failed), an arithmetic (`COALESCE(n, '1.5') + 1` answers; it was refused), and a
  string function, which reads the value's text (`UPPER(CASE WHEN … THEN n ELSE '3' END)`). Over an
  `ARRAY<X>` column the conditional reads its element in a query: `GREATEST(an, '1.5')` answers
  (it was refused), as `GREATEST(an, 2)` does. A consumer that cannot take the new type is refused
  as its unquoted twin is: `COALESCE(n, '3') LIKE '1%'`, `CAST(COALESCE(n, '3') AS DATE)`,
  `COALESCE(n, '3') IN ('3', 'abc')` and, over a `KEYWORD` `k`, `COALESCE(d, '2024-01-15') = k`,
  `COALESCE(n, '3') > k` and `CASE WHEN … THEN n ELSE '3' END > k` are refused (they failed on
  every row, or answered wrong rows, before: `COALESCE(n, '3') > k` compared a number with a text,
  which Painless refuses). So are, over a `KEYWORD` `k` and a `DATE` `d2`, a conditional holding a
  quoted date or timestamp compared with `k` — `NULLIF(F, k)`, `NULLIF(k, F)`,
  `CASE F WHEN k THEN …`, `F = k`, and the expressions over them, such as
  `CASE WHEN NULLIF(F, k) = '2024-01-15' THEN …`, for `F` one of `COALESCE(d, '2024-01-15')`,
  `COALESCE(d, '2024-01-15 10:30:00')`, `COALESCE(ts, '2024-01-15')`,
  `COALESCE(ts, '2024-01-15 10:30:00')`, `COALESCE(d, ts, '2024-01-15 10:30:00')`,
  `CASE WHEN … THEN d ELSE '2024-01-15' END`, `CASE n WHEN … THEN d ELSE '2024-01-15' END`,
  `CASE WHEN … THEN ts ELSE '2024-01-15 10:30:00' END`, `NULLIF(COALESCE(d, '2024-01-15'), k)`,
  `NULLIF(k, COALESCE(d, '2024-01-15'))` and `NULLIF(COALESCE(d, '2024-01-15'), '2024-01-15')`,
  and the literal-first forms with a time part or beside a timestamp,
  `COALESCE('2024-01-15 10:30:00', d)`, `COALESCE('2024-01-15', ts)` and
  `COALESCE('2024-01-15 10:30:00', ts)` — and, over a number read so,
  `CASE WHEN COALESCE(n, '3') = '2024-01-15' THEN …`,
  `CASE WHEN COALESCE(n, '3') > '2024-01-10' THEN …`,
  `CASE WHEN COALESCE(n, '3') BETWEEN d2 AND '2024-12-31' THEN …`,
  `NULLIF(COALESCE(n, '3'), '2024-01-15')` and
  `CASE WHEN NULLIF(COALESCE(n, '3'), k) = '2024-01-15' THEN …`: each compared a date or a number
  with a text before. Over the three `CASE` operands and the literal-first forms with a time part
  or beside a timestamp that answered wrong values on rows that tell the answers apart; over the
  other operands it failed. Two forms answer instead, as the next two items say: a literal-first
  `COALESCE('2024-01-15', d)` compared for equality with a text, and
  `COALESCE(NULLIF(F, k), k)`.
- *Since `0.25.0`.* **A literal-first `COALESCE` whose literal is a date as `yyyy-MM-dd` compares
  that literal's text for equality with a text**: `COALESCE('2024-01-15', d)` can only answer its
  literal, so beside a text operand that is no literal (a `KEYWORD` `k`, `UPPER(k)`), in a query,
  `=`, `<>`, `!=`, `NULLIF` and `CASE … WHEN` compare the text `'2024-01-15'`, and `NULLIF`
  answers it, a `VARCHAR`: `CASE WHEN COALESCE('2024-01-15', d) = k THEN 1 ELSE 0 END`,
  `NULLIF(COALESCE('2024-01-15', d), k)`, `NULLIF(k, COALESCE('2024-01-15', d))`,
  `CASE COALESCE('2024-01-15', d) WHEN k THEN …`, `CASE k WHEN COALESCE('2024-01-15', d) THEN …`,
  `COALESCE(NULLIF(COALESCE('2024-01-15', d), k), k)` and `WHERE COALESCE('2024-01-15', d) = k`
  answer as before, the same values (a `NULL` argument before the literal, or a second literal
  after it, too). DuckDB refuses them. A text that spells the same date differently is compared
  as text, as before: `k = '2024-01-15T00:00:00Z'` or `'3'` does not equal it. Ordering
  (`COALESCE('2024-01-15', d) < k`, `>= k`) stays refused: it failed. `BETWEEN` is the exception:
  between two text bounds, `CASE WHEN COALESCE('2024-01-15', d) BETWEEN k AND g THEN 1 ELSE 0 END`
  is still accepted, and still fails when the query runs, as before. Outside a query, beside a
  number or a date, or where a `CASE` compares its operand with values that are not all text, the
  conditional keeps its `DATE`.
- *Since `0.25.0`.* **`COALESCE(NULLIF(F, k), k)` answers `COALESCE(F, k)`**, in a query, where
  `F` is a conditional holding a quoted date or timestamp and `k` a text: `F`'s text where `F` is
  not `NULL`, else `k` — `COALESCE(NULLIF(CASE WHEN … THEN d ELSE '2024-01-15' END, k), k)` (and
  over `ts`, and `CASE n WHEN …`), `COALESCE(NULLIF(COALESCE(d, '2024-01-15'), k), k)` (and over
  `ts`, a time part, or `COALESCE(d, ts, …)`), `COALESCE(NULLIF(NULLIF(COALESCE(d, '2024-01-15'),
  '2024-01-15'), k), k)`, `COALESCE(NULLIF(k, F), F)`, with more arguments after or before
  (`COALESCE(NULLIF(F, k), k, g)`, `COALESCE(g, NULLIF(F, k), k)`) and under a string function.
  Those over the three `CASE` operands answered the right dates before, in another text: a date
  now answers `'2024-01-15'` where it answered `'2024-01-15T00:00Z'`, and the literal of a
  timestamp `CASE` answers `'2024-01-15T10:30Z'` where it answered `'2024-01-15 10:30:00'`; the
  others failed. Where `k` spells `F`'s date or instant in another text
  (`k = '2024-01-15T00:00:00Z'` beside a date), the answer is `F`'s text, the same date. A `NULLIF`
  that is not followed by its second argument, or whose argument is a column or a literal, keeps
  its refusal (`COALESCE(NULLIF(F, k), g)`, `COALESCE(NULLIF(d, k), k)`).
- *Since `0.25.0`.* **`COALESCE` over a `GREATEST` or `LEAST` holding a literal answers**: such an
  argument is never `NULL`, so `COALESCE` ends there, in a query, a computed column
  (`CREATE TABLE`, `ALTER TABLE`, `UPDATE … SET`) and a transform. `COALESCE(GREATEST(n, 3), l)`,
  `COALESCE(LEAST(x, 1.5), x)`, `COALESCE(GREATEST(n, 3), n, 7)` and their computed columns failed
  to compile (`Cannot cast from [long] to [java.lang.Object]`), as did a computed column
  `c VARCHAR SCRIPT AS (COALESCE(GREATEST(n, '3'), k))` (`Cannot cast from [java.lang.String] to
  [double]`); the quoted forms, refused before — `COALESCE(GREATEST(n, '3'), n)`,
  `COALESCE(LEAST(x, '1.5'), x)` over a `TINYINT`, an `INT` or a `DOUBLE` with `'3'`, `'1.5'` or
  `'200'`, and `COALESCE(GREATEST(n, '2'), k)` — answer too. Per group the same rule renders,
  but `SELECT g, COALESCE(GREATEST(MAX(n), 3), 0) … GROUP BY g` is refused before it runs, as
  before. A `GREATEST` with no literal (`COALESCE(GREATEST(n, l), x)`) and a nested
  `COALESCE(COALESCE(GREATEST(n, 3), l), x)` render as before.
- *Since `0.25.0`.* **Nested beside a text argument, in a query, a conditional that reads a quoted
  argument answers text, as before**: over a `KEYWORD` `k`, `COALESCE(COALESCE(n, '3'), k)`,
  `COALESCE(k, COALESCE(n, '3'))`, `CASE WHEN … THEN COALESCE(n, '3') ELSE k END`,
  `COALESCE(CASE WHEN … THEN n ELSE '3' END, k)`, `COALESCE(COALESCE(d, '2024-01-15'), k)`,
  `COALESCE(COALESCE(n, '3'), CAST(n AS VARCHAR))` or
  `COALESCE(COALESCE(n, '3'), COALESCE(d, '2024-01-15'))` are a `VARCHAR`, each nested value
  answered as the text of the value it is read as: `COALESCE(COALESCE(n, '1.5'), k)` answers `'2'`
  where `n` is missing (it answered `'1.5'`), and a date its ISO date. A nested `GREATEST`, `LEAST`
  or `NULLIF` value reading a quoted argument answers its text too, which also makes answered, over
  a `DATE` `d2`, statements that were refused because the nested expression mixed a text with a
  number or a date: `COALESCE(GREATEST(n, '2'), k)`, `COALESCE(GREATEST(d, '2024-01-15'), k)`,
  `COALESCE(COALESCE(GREATEST(d, '2024-01-15'), d2), k)`,
  `COALESCE(COALESCE(COALESCE(d, '2024-01-15'), d2), k)`,
  `COALESCE(COALESCE(d2, COALESCE(d, '2024-01-15')), k)`,
  `COALESCE(CASE WHEN … THEN COALESCE(d, '2024-01-15') ELSE d2 END, k)` and
  `COALESCE(NULLIF(COALESCE(d, '2024-01-15'), d2), k)`. Beside a number or a date the nested value
  keeps the type it is read as (`COALESCE(COALESCE(n, '3'), x)` is a `DOUBLE`), and arguments that
  mix either way stay refused (`COALESCE(COALESCE(n, '3'), d)`). A computed column stores the
  value the nested conditional reads (`COALESCE(COALESCE(n, '1.5'), k)` stores `2` where `n` is
  missing).
- *Since `0.25.0`.* **`COALESCE` over a `CASE` tests the whole `CASE` for `NULL`**:
  `COALESCE(CASE WHEN c THEN a ELSE b END, k)` answers `k` where `c` holds and `a` is `NULL`. It
  tested the `ELSE` alone and answered `NULL` there, and a `NULL` `ELSE`
  (`CASE WHEN c THEN a ELSE NULL END`) compared `null` with `null`. Every `COALESCE` with a `CASE`
  argument before its last one renders new bytes — in a query, in a computed column (`CREATE TABLE`,
  `ALTER TABLE`, which re-creates the pipeline, and `UPDATE … SET`) and in a transform — for
  instance `COALESCE(CASE WHEN … THEN d ELSE '2024-01-15' END, k)`,
  `COALESCE(CASE n WHEN 1 THEN d ELSE '2024-01-15' END, d2)` and
  `COALESCE(CASE WHEN … THEN n ELSE l END, 7)`.

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
- Type of `expr1` — a quoted literal first is the value it is read as beside `expr2`, and has its
  type: `NULLIF('2024-01-31', ts)` is a `TIMESTAMP`, `NULLIF('3', n)` a number — or NULL if equal

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

**3. Guard a zero divisor — needed for `%`, not for `/`:**
```sql
-- Since 0.24.0 the engine returns NULL for a zero divisor of `/` by itself, so write this:
SELECT 
  total_sales / total_orders AS avg_order_value
FROM sales_summary
-- NULL on the rows where total_orders = 0

-- `%` is not guarded by itself: NULLIF is the guard
SELECT 
  total_sales % NULLIF(total_orders, 0) AS remainder
FROM sales_summary
-- NULL on the rows where total_orders = 0 or is missing
```

> `total_sales / NULLIF(total_orders, 0)` — the idiom this section used to recommend — answers
> `NULL` on the rows where `total_orders` is `0` or missing, in a search and in a computed column,
> and so does `total_sales % NULLIF(total_orders, 0)`. Before `0.24.0` both threw a
> `null_pointer_exception` in a search on exactly those rows, and silently dropped the computed
> column in an ingest pipeline. See the [`/` and `%` operators](operators.md) for the zero-divisor
> rules.

**4. Clean data:**
```sql
SELECT 
  name,
  NULLIF(TRIM(description), '') AS description
FROM products
-- Converts empty strings to NULL after trimming
```

**Notes**

- The comparison follows what each argument **renders**, not the type of the column it came from.
  `NULLIF(CAST(zip_code AS BIGINT), 0)` compares two numbers even though `zip_code` is a `KEYWORD`.
  Before **0.24.0** it was compared as text and the generated script was rejected by Elasticsearch
  wherever it appeared — a computed column, a projection or a `WHERE`. A function argument is
  evaluated once from 0.24.0 as well; `NULLIF(UPPER(name), 'X')` used to call `UPPER` three times,
  and a `CASE` as the first argument could return the wrong value.
- The two arguments must be **comparable**, and from **0.24.0** the engine says so itself. Two
  arguments are comparable when both render text, both render a temporal type, both render a
  number, both render a boolean, or one of them is NULL or a column whose type is not known. Any
  other pair is refused, because the engine has no way to compare them: `NULLIF(status, 0)` is
  refused with

  ```
  NULLIF(status, 0) compares KEYWORD with BIGINT: NULLIF requires two arguments of comparable
  types, so cast one of them
  ```

  — at `CREATE TABLE` / `ALTER TABLE … SET SCRIPT AS` for a computed column, and before the query
  is sent for a projection or a `WHERE`. Earlier versions built a script around the mismatch
  instead: in a query Elasticsearch rejected it, and in a computed column the ingest processor's
  `ignore_failure` swallowed the failure, so **the column was simply missing from every document**
  and the `CREATE TABLE` still answered `200`. Write the comparison in the type you mean:
  `NULLIF(status, '0')` or `NULLIF(CAST(status AS BIGINT), 0)`.

  A cast can *create* the mismatch as well as cure it: `NULLIF(CAST(qty AS KEYWORD), 0)` renders
  text against a number and is refused, even though `qty` is numeric.

  *Since `0.25.0`*, a **query** compares a text with a number or a boolean, and a boolean with a
  number, as DuckDB casts them (see
  [Comparing values of different types](operators.md#comparing-values-of-different-types)): `NULLIF(status, 0)` reads
  `status` as a `BIGINT` and answers `status`, NULL where it reads as `0`; a `status` that spells
  no number fails the query. `NULLIF(flag, 0)` over a `BOOLEAN` compares it as `1` / `0`, in a query
  and in a computed column. A **computed column** keeps the refusal above for a text against a
  number or a boolean: an ingest pipeline would silently drop the document whose text converts to
  nothing.

  The same rule refuses a **date against a number** (`NULLIF(created_at, 0)`) and a **boolean
  against a date** (`NULLIF(is_active, created_at)`); a boolean against a number
  (`NULLIF(is_active, 0)`), refused before `0.25.0`, is now compared as `1` / `0` (above). Before **0.24.0** these were accepted
  and compared with Painless `==`, which answers `false` for a date against a number without ever
  failing — so the `NULLIF` returned its first argument for every row and the query answered `200`
  with the wrong values. If you relied on one of these, write the comparison in the type you mean.

- A **string literal against a `date` column** is read by what it is compared with, as in every
  comparison: `NULLIF(created_at, '2024-01-15')` compares `created_at` with the date 2024-01-15,
  and `NULLIF(qty, '3')` compares `qty` with the number 3 — in a query and in a computed column
  alike (before **0.24.0** the first was refused, and a computed column compared a date with a
  string, which never matched). A literal that spells no date is refused, and the message depends
  on the literal: a malformed one is named as such — `NULLIF(created_at, 'yesterday')` reports
  that `'yesterday'` is not a date for that field's format, checked against the column's mapping
  exactly as a `WHERE` comparison is — and Elasticsearch date math (`'now-1d'`), which no script
  can evaluate, is refused as not supported.
- `expr2` being NULL is not a match: SQL says the comparison is then UNKNOWN, so the answer is
  `expr1`, not NULL.

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

---

### GREATEST

Returns the largest value among the given expressions — numbers, or dates and timestamps. NULL arguments are ignored (PostgreSQL-style NULL handling); the result is NULL only if every argument is NULL.

**Syntax:**
```sql
GREATEST(expression1, expression2, ...)
```

**Inputs:**
- One or more expressions of one kind: all numeric, or all `DATE` / `TIMESTAMP` / `DATETIME`

**Output:**
- Over numbers, the common type of all the arguments (a quoted argument counting as the number it
  is read as), as PostgreSQL types it, and every row has it: the widest whole type when every argument is a whole number, a `DOUBLE` as soon as one is
  fractional (a `REAL` when the fractional ones are all `REAL`); over dates, a `DATE` when every
  argument is a `DATE` and a `TIMESTAMP` when they mix; NULL if every argument is NULL

**Examples:**

**1. Highest of multiple prices:**
```sql
SELECT GREATEST(price_us, price_eu, price_uk) AS max_price
FROM products
```

**2. Floor on a computed value:**
```sql
SELECT GREATEST(0, base_price - rebate) AS net
FROM orders
-- Never lets the net go below zero
```

**3. The latest of two dates, and the days since the first:**
```sql
SELECT GREATEST(ordered_on, shipped_on) AS last_step,
       GREATEST(ordered_on, shipped_on) - ordered_on AS days_to_last_step
FROM orders
```

**4. Per group, over aggregates:**
```sql
SELECT region, GREATEST(COUNT(*), 10) AS at_least_ten
FROM orders
GROUP BY region

-- the regions whose last activity is this year (a group filter)
SELECT region
FROM orders
GROUP BY region
HAVING GREATEST(MAX(ordered_on), MAX(shipped_on)) >= CAST('2025-01-01' AS DATE)
```

**Notes:**
- Ignores NULL arguments (PostgreSQL-style NULL handling); returns NULL only if every argument is NULL.
- The arguments are all numeric, or all dates and timestamps, as in every SQL engine. *Since
  `0.25.0`*, a quoted argument counts as the type of the arguments beside it (`GREATEST(n, '1.5')` is
  an `INT`, `GREATEST(d, '2024-01-15')` a `DATE`), and a `BOOLEAN` beside whole numbers counts as
  `1` / `0` (see [The type of a conditional](#the-type-of-a-conditional)). Any other argument (a
  quoted text that spells no value of that type, such as `GREATEST(n, 'abc')`, or a `BOOLEAN` alone
  or beside a fractional number), and numbers mixed with dates, are refused by name once the column
  types are known.
- Dates compare as instants, a `DATE` as the start of its day (UTC); the result is a date like any
  other: `GREATEST(d, d2) - d` is a number of days (see [Date arithmetic](operators.md)).
- Over aggregates it is computed per group. ⚠️ In a `SELECT`, over an aggregate that can be NULL —
  `MAX`, `MIN`, `AVG`, the `STDDEV` / `VARIANCE` family, a percentile — it is **refused**, per
  group as over the whole table (`GREATEST(MAX(n), 2)`): in a group with no value of the column,
  Elasticsearch does not run the calculation (its `bucket_script` skips a group with a missing
  metric), so the group would answer NULL where SQL answers the other arguments' largest. Select
  the aggregate on its own and apply `GREATEST` to the result outside the query. Over a `COUNT` or
  a `SUM` (`0` and `0.0` over no value) it is answered. Written inline in `HAVING`, the group
  filter reads every aggregate and skips a NULL one, and is answered.
- See also: `LEAST`, `COALESCE`.

---

### LEAST

Returns the smallest value among the given expressions — numbers, or dates and timestamps. NULL arguments are ignored (PostgreSQL-style NULL handling); the result is NULL only if every argument is NULL.

**Syntax:**
```sql
LEAST(expression1, expression2, ...)
```

**Inputs:**
- One or more expressions of one kind: all numeric, or all `DATE` / `TIMESTAMP` / `DATETIME`

**Output:**
- Over numbers, the common type of all the arguments (a quoted argument counting as the number it
  is read as), as PostgreSQL types it, and every row has it: the widest whole type when every argument is a whole number, a `DOUBLE` as soon as one is
  fractional (a `REAL` when the fractional ones are all `REAL`); over dates, a `DATE` when every
  argument is a `DATE` and a `TIMESTAMP` when they mix; NULL if every argument is NULL

**Examples:**

**1. Lowest of multiple prices:**
```sql
SELECT LEAST(price_us, price_eu, price_uk) AS min_price
FROM products
```

**2. Cap on a computed value:**
```sql
SELECT LEAST(rebate, base_price) AS applied_rebate
FROM orders
-- Never lets the rebate exceed the base price
```

**3. The earlier of a date and a timestamp:**
```sql
SELECT LEAST(due_on, delivered_at) AS first_event
FROM orders
-- a TIMESTAMP: the arguments mix a DATE and a TIMESTAMP
```

**Notes:**
- Ignores NULL arguments (PostgreSQL-style NULL handling); returns NULL only if every argument is NULL.
- The arguments are all numeric, or all dates and timestamps, as for `GREATEST` (a quoted argument counting as the type of the arguments beside it, and a `BOOLEAN` beside whole
  numbers as `1` / `0`); see `GREATEST` for dates, and for
  `LEAST` over aggregates per group.
- See also: `GREATEST`, `COALESCE`.

[Back to index](README.md)
