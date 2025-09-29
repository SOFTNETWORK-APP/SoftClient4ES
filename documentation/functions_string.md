[Back to index](./README.md)

# String Functions

---

### Function: UPPER / UCASE
**Description:**  
Convert string to upper case.

**Inputs:** 
- `str` (`VARCHAR`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT UPPER('hello') AS up;
-- Result: 'HELLO'
```

### Function: LOWER / LCASE
**Description:**  
Convert string to lower case.

**Inputs:** 
- `str` (`VARCHAR`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT LOWER('Hello') AS lo;
-- Result: 'hello'
```

### Function: TRIM
**Description:**  
Trim whitespace both sides.

**Inputs:** 
- `str` (`VARCHAR`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT TRIM('  abc  ') AS t;
-- Result: 'abc'
```

### Function: LTRIM
**Description:**  
Trim whitespace left side.

**Inputs:**
- `str` (`VARCHAR`)

**Output:**
- `VARCHAR`

**Example:**
```sql
SELECT LTRIM('  abc  ') AS t;
-- Result: 'abc  '
```

### Function: RTRIM
**Description:**  
Trim whitespace right side.

**Inputs:**
- `str` (`VARCHAR`)

**Output:**
- `VARCHAR`

**Example:**
```sql
SELECT RTRIM('  abc  ') AS t;
-- Result: '  abc'
```

### Function: LENGTH / LEN
**Description:**  
Character length.

**Inputs:** 
- `str` (`VARCHAR`)

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT LENGTH('abc') AS l;
-- Result: 3
```

### Function: SUBSTRING / SUBSTR
**Description:**  
SQL 1-based substring.

**Inputs:** 
- `str` (`VARCHAR`), `start` (`INT` >=1), optional `length` (`INT`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT SUBSTRING('abcdef', 2, 3) AS s;
-- Result: 'bcd'
```

### Function: CONCAT
**Description:**  
Concatenate values into a string.

**Inputs:** 
- `expr1, expr2, ...` (coercible to `VARCHAR`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT CONCAT(firstName, ' ', lastName) AS full FROM users;
```

### Function: REPLACE
**Description:**  
Replace substring occurrences.

**Inputs:** 
- `str, search, replace`

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT REPLACE('Mr. John', 'Mr. ', '') AS r;
-- Result: 'John'
```

### Function: POSITION / STRPOS
**Description:**  
1-based index, 0 if not found.  
The first position of the `substr` in the `str`.

**Inputs:** 
- `substr` `IN` `str`

**Output:** 
- `INT`

**Example:**
```sql
SELECT POSITION('lo' IN 'hello') AS pos;
-- Result: 4
```

### Function: REGEXP_LIKE / RLIKE
**Description:**  
Regex match predicate.

**Inputs:** 
- `str, pattern`

**Output:** 
- `BOOLEAN`

**Example:**
```sql
SELECT REGEXP_LIKE(email, '.*@example\.com') AS ok FROM users;
```

[Back to index](./README.md)
