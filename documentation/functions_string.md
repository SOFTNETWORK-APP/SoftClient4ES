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
- `str` (`VARCHAR`) `,`|`FROM` `start` (`INT` >= 1) optional `,`|`FOR` `length` (`INT`)

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT SUBSTRING('abcdef', 2, 3) AS s;
-- Result: 'bcd'

SELECT SUBSTRING('abcdef' FROM 2 FOR 3) AS s;
-- Result: 'bcd'

SELECT SUBSTRING('abcdef' FROM 4) AS s;
-- Result: 'def'
```

### Function: LEFT
**Description:**  
Leftmost characters.

**Inputs:** 
- `str` (`VARCHAR`) `,`|`FOR` `length` (`INT`)

**Output:**
- `VARCHAR`

**Example:**
```sql
SELECT LEFT('abcdef', 3) AS l;
-- Result: 'abc'
```

### Function: RIGHT
**Description:**  
Rightmost characters.

**Inputs:**
- `str` (`VARCHAR`) `,`|`FOR` `length` (`INT`)

**Output:**
- `VARCHAR`

**Example:**
```sql
SELECT RIGHT('abcdef', 3) AS r;
-- Result: 'def'

SELECT RIGHT('abcdef' FOR 10) AS r;
-- Result: 'abcdef'
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
The first position of the `substr` in the `str`, starting at the optional `FROM` position (1-based).

**Inputs:**
- `substr` `,` | `IN` `str` optional `,` | `FROM` `INT`

**Output:**
- `BIGINT`

**Example:**
```sql
SELECT POSITION('lo', 'hello') AS pos;
-- Result: 4

SELECT POSITION('a' IN 'Elasticsearch' FROM 5) AS pos;
-- Result: 10

SELECT POSITION('z' IN 'Elasticsearch') AS pos;
-- Result: 0
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
