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
Returns the leftmost characters from a string.

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
Returns the rightmost characters from a string.  
If `length` exceeds the string size, the implementation returns the full string.  
If `length = 0`, an empty string is returned.  
If `length < 0`, a validation error is raised.

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
Replaces all occurrences of a substring with another substring.

**Inputs:** 
- `str, search, replace`

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT REPLACE('Mr. John', 'Mr. ', '') AS r;
-- Result: 'John'
```

### Function: REVERSE
**Description:**  
Reverses the characters in a string.

**Inputs:** 
- `str` (`VARCHAR`)

**Output:**
- `VARCHAR`

**Example:**
```sql
SELECT REVERSE('abcdef') AS r;
-- Result: 'fedcba'
```

### Function: POSITION / STRPOS
**Description:**  
Returns the 1-based position of the first occurrence of a substring in a string.  
If the substring is not found, returns 0.  
An optional FROM position (1-based) can be provided to start the search.

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
`REGEXP_LIKE(string, pattern [, match_param])`

Returns `TRUE` if the input string matches the regular expression `pattern`.  
By default, the match is case-sensitive.

**Inputs:** 
- `string`: The input string to test.
- `pattern`: A regular expression pattern.
- `match_param` *(optional)*: A string controlling the regex matching behavior.
    - `'i'`: Case-insensitive match.
    - `'c'`: Case-sensitive match (default).
    - `'m'`: Multi-line mode.
    - `'n'`: Allows the `.` to match newline characters.

**Output:** 
- `BOOLEAN`

**Examples:**
```sql
SELECT REGEXP_LIKE('Hello', 'HEL');         -- false
SELECT REGEXP_LIKE('Hello', 'HEL', 'i');    -- true
SELECT REGEXP_LIKE('abc\nxyz', '^xyz', 'm') -- true
```

[Back to index](./README.md)
