[Back to index](README.md)

## String Functions

---

### Case Conversion Functions

#### UPPER / UCASE

Convert string to upper case.

**Syntax:**
```sql
UPPER(str)
UCASE(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Basic uppercase conversion
SELECT UPPER('hello') AS up;
-- Result: 'HELLO'

-- Using UCASE alias
SELECT UCASE('world') AS up;
-- Result: 'WORLD'

-- Mixed case
SELECT UPPER('Hello World') AS up;
-- Result: 'HELLO WORLD'

-- With special characters
SELECT UPPER('café') AS up;
-- Result: 'CAFÉ'

-- Normalize user input
SELECT 
  user_id,
  UPPER(email) AS normalized_email
FROM users;

-- Case-insensitive comparison
SELECT * FROM products
WHERE UPPER(name) = UPPER('iPhone');

-- Uppercase in concatenation
SELECT CONCAT(UPPER(first_name), ' ', UPPER(last_name)) AS full_name
FROM employees;
```

---

#### LOWER / LCASE

Convert string to lower case.

**Syntax:**
```sql
LOWER(str)
LCASE(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Basic lowercase conversion
SELECT LOWER('Hello') AS lo;
-- Result: 'hello'

-- Using LCASE alias
SELECT LCASE('WORLD') AS lo;
-- Result: 'world'

-- Mixed case
SELECT LOWER('Hello World') AS lo;
-- Result: 'hello world'

-- With special characters
SELECT LOWER('CAFÉ') AS lo;
-- Result: 'café'

-- Normalize email addresses
SELECT 
  user_id,
  LOWER(email) AS email_lower
FROM users;

-- Case-insensitive search
SELECT * FROM articles
WHERE LOWER(title) LIKE '%elasticsearch%';

-- Lowercase tags
SELECT 
  article_id,
  LOWER(tag) AS normalized_tag
FROM article_tags;
```

---

### Trimming Functions

#### TRIM

Trim whitespace from both sides.

**Syntax:**
```sql
TRIM(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Basic trim
SELECT TRIM('  abc  ') AS t;
-- Result: 'abc'

-- Trim tabs and spaces
SELECT TRIM('	abc	') AS t;
-- Result: 'abc'

-- No whitespace
SELECT TRIM('abc') AS t;
-- Result: 'abc'

-- Clean user input
SELECT 
  user_id,
  TRIM(username) AS clean_username
FROM users;

-- Trim in WHERE clause
SELECT * FROM products
WHERE TRIM(name) = 'iPhone';

-- Trim before comparison
SELECT 
  order_id,
  TRIM(status) AS status
FROM orders
WHERE TRIM(status) IN ('pending', 'shipped');
```

---

#### LTRIM

Trim whitespace from left side.

**Syntax:**
```sql
LTRIM(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Left trim
SELECT LTRIM('  abc  ') AS t;
-- Result: 'abc  '

-- Remove leading spaces
SELECT LTRIM('   hello') AS t;
-- Result: 'hello'

-- No leading whitespace
SELECT LTRIM('abc   ') AS t;
-- Result: 'abc   '

-- Clean prefixes
SELECT 
  product_id,
  LTRIM(code) AS trimmed_code
FROM products;
```

---

#### RTRIM

Trim whitespace from right side.

**Syntax:**
```sql
RTRIM(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Right trim
SELECT RTRIM('  abc  ') AS t;
-- Result: '  abc'

-- Remove trailing spaces
SELECT RTRIM('hello   ') AS t;
-- Result: 'hello'

-- No trailing whitespace
SELECT RTRIM('   abc') AS t;
-- Result: '   abc'

-- Clean suffixes
SELECT 
  product_id,
  RTRIM(description) AS trimmed_desc
FROM products;
```

---

### String Measurement Functions

#### LENGTH / LEN

Character length of string.

**Syntax:**
```sql
LENGTH(str)
LEN(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `BIGINT`

**Examples:**
```sql
-- Basic length
SELECT LENGTH('abc') AS l;
-- Result: 3

-- Using LEN alias
SELECT LEN('hello') AS l;
-- Result: 5

-- Empty string
SELECT LENGTH('') AS l;
-- Result: 0

-- With spaces
SELECT LENGTH('hello world') AS l;
-- Result: 11

-- Unicode characters
SELECT LENGTH('café') AS l;
-- Result: 4

-- Filter by length
SELECT * FROM products
WHERE LENGTH(name) > 20;

-- Validate input length
SELECT 
  user_id,
  username,
  LENGTH(username) AS username_length
FROM users
WHERE LENGTH(username) < 3;

-- Average length
SELECT 
  category,
  ROUND(AVG(LENGTH(description)), 2) AS avg_desc_length
FROM products
GROUP BY category;
```

---

### String Extraction Functions

#### SUBSTRING / SUBSTR

SQL 1-based substring extraction.

**Syntax:**
```sql
SUBSTRING(str, start)
SUBSTRING(str, start, length)
SUBSTRING(str FROM start)
SUBSTRING(str FROM start FOR length)
SUBSTR(str, start, length)
```

**Inputs:**
- `str` - `VARCHAR`
- `start` - `INT` (≥ 1, 1-based index)
- `length` (optional) - `INT`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Extract with start and length
SELECT SUBSTRING('abcdef', 2, 3) AS s;
-- Result: 'bcd'

-- Using FROM...FOR syntax
SELECT SUBSTRING('abcdef' FROM 2 FOR 3) AS s;
-- Result: 'bcd'

-- Extract from position to end
SELECT SUBSTRING('abcdef' FROM 4) AS s;
-- Result: 'def'

-- Using SUBSTR alias
SELECT SUBSTR('hello world', 7, 5) AS s;
-- Result: 'world'

-- Extract first character
SELECT SUBSTRING('hello', 1, 1) AS s;
-- Result: 'h'

-- Extract year from date string
SELECT SUBSTRING('2025-01-10', 1, 4) AS year;
-- Result: '2025'

-- Extract domain from email
SELECT 
  email,
  SUBSTRING(email FROM POSITION('@' IN email) + 1) AS domain
FROM users;

-- Extract area code from phone
SELECT 
  phone,
  SUBSTRING(phone, 1, 3) AS area_code
FROM contacts;

-- Extract product code
SELECT 
  product_id,
  SUBSTRING(product_id FROM 1 FOR 3) AS category_code
FROM products;
```

---

#### LEFT

Returns the leftmost characters from a string.

**Syntax:**
```sql
LEFT(str, length)
LEFT(str FOR length)
```

**Inputs:**
- `str` - `VARCHAR`
- `length` - `INT`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Extract first 3 characters
SELECT LEFT('abcdef', 3) AS l;
-- Result: 'abc'

-- Using FOR syntax
SELECT LEFT('abcdef' FOR 3) AS l;
-- Result: 'abc'

-- Extract first character
SELECT LEFT('hello', 1) AS l;
-- Result: 'h'

-- Length exceeds string
SELECT LEFT('abc', 10) AS l;
-- Result: 'abc'

-- Extract prefix
SELECT 
  product_code,
  LEFT(product_code, 2) AS category
FROM products;

-- Extract initials
SELECT 
  first_name,
  LEFT(first_name, 1) AS initial
FROM users;

-- First word approximation
SELECT 
  title,
  LEFT(title, POSITION(' ' IN title) - 1) AS first_word
FROM articles;
```

---

#### RIGHT

Returns the rightmost characters from a string.

**Syntax:**
```sql
RIGHT(str, length)
RIGHT(str FOR length)
```

**Inputs:**
- `str` - `VARCHAR`
- `length` - `INT` (must be ≥ 0)

**Output:**
- `VARCHAR`

**Notes:**
- If `length` exceeds string size, returns the full string
- If `length = 0`, returns empty string
- If `length < 0`, raises validation error

**Examples:**
```sql
-- Extract last 3 characters
SELECT RIGHT('abcdef', 3) AS r;
-- Result: 'def'

-- Using FOR syntax
SELECT RIGHT('abcdef' FOR 3) AS r;
-- Result: 'def'

-- Length exceeds string
SELECT RIGHT('abcdef' FOR 10) AS r;
-- Result: 'abcdef'

-- Extract last character
SELECT RIGHT('hello', 1) AS r;
-- Result: 'o'

-- Zero length
SELECT RIGHT('hello', 0) AS r;
-- Result: ''

-- Extract file extension
SELECT 
  filename,
  RIGHT(filename, LENGTH(filename) - POSITION('.' IN filename)) AS extension
FROM files;

-- Extract last 4 digits
SELECT 
  credit_card,
  RIGHT(credit_card, 4) AS last_four
FROM payments;

-- Extract suffix
SELECT 
  product_code,
  RIGHT(product_code, 3) AS variant
FROM products;
```

---

### String Manipulation Functions

#### CONCAT

Concatenate values into a string.

**Syntax:**
```sql
CONCAT(expr1, expr2, ...)
```

**Inputs:**
- `expr1, expr2, ...` - Values coercible to `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Basic concatenation
SELECT CONCAT('Hello', ' ', 'World') AS greeting;
-- Result: 'Hello World'

-- Concatenate names
SELECT CONCAT(firstName, ' ', lastName) AS full FROM users;

-- Multiple values
SELECT CONCAT('Product: ', name, ' - Price: ', price) AS info
FROM products;

-- With NULL handling
SELECT CONCAT('Hello', NULL, 'World') AS result;
-- Result: NULL (any NULL makes entire result NULL)

-- Build full address
SELECT 
  CONCAT(street, ', ', city, ', ', state, ' ', zip) AS full_address
FROM addresses;

-- Create email
SELECT 
  CONCAT(LOWER(first_name), '.', LOWER(last_name), '@company.com') AS email
FROM employees;

-- Build URL
SELECT 
  CONCAT('https://example.com/products/', product_id) AS url
FROM products;

-- Format currency
SELECT 
  product_id,
  CONCAT('$', ROUND(price, 2)) AS formatted_price
FROM products;
```

---

#### REPLACE

Replaces all occurrences of a substring with another substring.

**Syntax:**
```sql
REPLACE(str, search, replace)
```

**Inputs:**
- `str` - `VARCHAR` (source string)
- `search` - `VARCHAR` (substring to find)
- `replace` - `VARCHAR` (replacement substring)

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Remove prefix
SELECT REPLACE('Mr. John', 'Mr. ', '') AS r;
-- Result: 'John'

-- Replace word
SELECT REPLACE('Hello World', 'World', 'Universe') AS r;
-- Result: 'Hello Universe'

-- Remove all spaces
SELECT REPLACE('a b c d', ' ', '') AS r;
-- Result: 'abcd'

-- Replace multiple occurrences
SELECT REPLACE('aaa', 'a', 'b') AS r;
-- Result: 'bbb'

-- Clean phone numbers
SELECT 
  phone,
  REPLACE(REPLACE(REPLACE(phone, '-', ''), '(', ''), ')', '') AS clean_phone
FROM contacts;

-- Normalize URLs
SELECT 
  url,
  REPLACE(url, 'http://', 'https://') AS secure_url
FROM links;

-- Remove special characters
SELECT 
  product_name,
  REPLACE(REPLACE(product_name, '/', '-'), '&', 'and') AS clean_name
FROM products;

-- Mask sensitive data
SELECT 
  email,
  REPLACE(email, SUBSTRING(email FROM 2 FOR 3), '***') AS masked_email
FROM users;
```

---

#### REVERSE

Reverses the characters in a string.

**Syntax:**
```sql
REVERSE(str)
```

**Inputs:**
- `str` - `VARCHAR`

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Basic reverse
SELECT REVERSE('abcdef') AS r;
-- Result: 'fedcba'

-- Reverse word
SELECT REVERSE('hello') AS r;
-- Result: 'olleh'

-- Palindrome check
SELECT 
  word,
  word = REVERSE(word) AS is_palindrome
FROM words;

-- Reverse for encoding
SELECT 
  text,
  REVERSE(text) AS reversed
FROM messages;
```

---

### String Search Functions

#### POSITION / STRPOS

Returns the 1-based position of the first occurrence of a substring in a string.

**Syntax:**
```sql
POSITION(substr, str)
POSITION(substr IN str)
POSITION(substr IN str FROM start)
STRPOS(str, substr)
```

**Inputs:**
- `substr` - `VARCHAR` (substring to find)
- `str` - `VARCHAR` (string to search in)
- `start` (optional) - `INT` (1-based starting position)

**Output:**
- `BIGINT` (returns 0 if not found)

**Examples:**
```sql
-- Find substring position
SELECT POSITION('lo', 'hello') AS pos;
-- Result: 4

-- Using IN syntax
SELECT POSITION('lo' IN 'hello') AS pos;
-- Result: 4

-- Start search from position
SELECT POSITION('a' IN 'Elasticsearch' FROM 5) AS pos;
-- Result: 10

-- Not found
SELECT POSITION('z' IN 'Elasticsearch') AS pos;
-- Result: 0

-- Using STRPOS alias
SELECT STRPOS('hello world', 'world') AS pos;
-- Result: 7

-- Find @ in email
SELECT 
  email,
  POSITION('@' IN email) AS at_position
FROM users;

-- Check if substring exists
SELECT * FROM products
WHERE POSITION('pro' IN LOWER(name)) > 0;

-- Extract domain from email
SELECT 
  email,
  SUBSTRING(email FROM POSITION('@' IN email) + 1) AS domain
FROM users;

-- Find first space
SELECT 
  full_name,
  POSITION(' ' IN full_name) AS space_pos
FROM contacts;

-- Multiple searches
SELECT 
  text,
  POSITION('error' IN LOWER(text)) AS error_pos,
  POSITION('warning' IN LOWER(text)) AS warning_pos
FROM logs;
```

---

### Regular Expression Functions

#### REGEXP_LIKE / REGEXP

Returns `TRUE` if the input string matches the regular expression pattern.

**Syntax:**
```sql
REGEXP_LIKE(string, pattern)
REGEXP_LIKE(string, pattern, match_param)
REGEXP(string, pattern)
```

**Inputs:**
- `string` - `VARCHAR` (input string to test)
- `pattern` - `VARCHAR` (regular expression pattern)
- `match_param` (optional) - `VARCHAR` (matching behavior control)
  - `'i'` - Case-insensitive match
  - `'c'` - Case-sensitive match (default)
  - `'m'` - Multi-line mode
  - `'n'` - Allows `.` to match newline characters

**Output:**
- `BOOLEAN`

**Examples:**
```sql
-- Case-sensitive match (default)
SELECT REGEXP_LIKE('Hello', 'HEL');
-- Result: false

-- Case-insensitive match
SELECT REGEXP_LIKE('Hello', 'HEL', 'i');
-- Result: true

-- Multi-line mode
SELECT REGEXP_LIKE('abc\nxyz', '^xyz', 'm');
-- Result: true

-- Using REGEXP alias
SELECT REGEXP('test@example.com', '^[a-z]+@[a-z]+\\.com$', 'i');
-- Result: true

-- Match email pattern
SELECT * FROM users
WHERE REGEXP_LIKE(email, '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z]{2,}$', 'i');

-- Match phone pattern
SELECT * FROM contacts
WHERE REGEXP_LIKE(phone, '^\\d{3}-\\d{3}-\\d{4}$');

-- Match alphanumeric
SELECT * FROM products
WHERE REGEXP_LIKE(sku, '^[A-Z]{3}[0-9]{4}$');

-- Match URL pattern
SELECT * FROM links
WHERE REGEXP_LIKE(url, '^https?://[a-z0-9.-]+\\.[a-z]{2,}', 'i');

-- Find specific words
SELECT * FROM articles
WHERE REGEXP_LIKE(content, '\\b(elasticsearch|kibana|logstash)\\b', 'i');

-- Validate format
SELECT 
  customer_id,
  REGEXP_LIKE(zip_code, '^\\d{5}(-\\d{4})?$') AS valid_zip
FROM customers;

-- Pattern matching with alternation
SELECT * FROM logs
WHERE REGEXP_LIKE(message, 'error|warning|critical', 'i');

-- Match beginning and end
SELECT * FROM products
WHERE REGEXP_LIKE(name, '^iPhone.*Pro$', 'i');
```

---

### Full-Text Search Function

#### MATCH ... AGAINST

Performs full-text search using Elasticsearch's match query capabilities. Follows MySQL-style syntax.

**Syntax:**
```sql
MATCH(field1, field2, ...) AGAINST (query_text)
```

**Inputs:**
- `field1, field2, ...` - Column names to search in (one or more fields)
- `query_text` - `VARCHAR` (text to search for)

**Output:**
- `BOOLEAN` (when used in WHERE clause)
- `DOUBLE` (relevance score when used in SELECT)

**Examples:**

**Basic Single Field Search:**
```sql
-- Search in one field
SELECT * FROM articles
WHERE MATCH(content) AGAINST ('elasticsearch');

-- Search in title
SELECT * FROM articles
WHERE MATCH(title) AGAINST ('tutorial');
```

**Multiple Field Search:**
```sql
-- Search in multiple fields
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST ('elasticsearch tutorial');

-- Search across three fields
SELECT * FROM products
WHERE MATCH(name, description, tags) AGAINST ('wireless headphones');

-- Search in all text fields
SELECT * FROM documents
WHERE MATCH(title, abstract, content, keywords) AGAINST ('machine learning');
```

**With Relevance Score:**
```sql
-- Get relevance score in results
SELECT 
  title,
  author,
  MATCH(title, content) AGAINST ('elasticsearch') AS relevance_score
FROM articles
WHERE MATCH(title, content) AGAINST ('elasticsearch')
ORDER BY relevance_score DESC;

-- Score from multiple fields
SELECT 
  product_id,
  name,
  MATCH(name, description) AGAINST ('laptop') AS score
FROM products
WHERE MATCH(name, description) AGAINST ('laptop')
ORDER BY score DESC
LIMIT 20;
```

**Multi-Term Search:**
```sql
-- Search for multiple terms (any can match)
SELECT * FROM articles
WHERE MATCH(content) AGAINST ('elasticsearch kibana logstash');

-- Multiple terms across multiple fields
SELECT * FROM products
WHERE MATCH(name, description) AGAINST ('wireless bluetooth speaker');

-- Natural language query
SELECT * FROM documents
WHERE MATCH(content) AGAINST ('how to use elasticsearch for search');
```

**Practical Examples:**

**1. E-commerce Product Search:**
```sql
-- Search products across multiple fields
SELECT 
  product_id,
  name,
  price,
  MATCH(name, description, category) AGAINST ('wireless headphones') AS relevance
FROM products
WHERE MATCH(name, description, category) AGAINST ('wireless headphones')
  AND price BETWEEN 50 AND 200
  AND in_stock = true
ORDER BY relevance DESC
LIMIT 20;
```

**2. Blog Article Search:**
```sql
-- Search articles with scoring
SELECT 
  article_id,
  title,
  author,
  published_date,
  MATCH(title, content, tags) AGAINST ('elasticsearch tutorial') AS score
FROM articles
WHERE MATCH(title, content, tags) AGAINST ('elasticsearch tutorial')
  AND status = 'published'
  AND published_date >= DATE_SUB(CURRENT_DATE, INTERVAL 1 YEAR)
ORDER BY score DESC, published_date DESC
LIMIT 10;
```

**3. Documentation Search:**
```sql
-- Search documentation
SELECT 
  doc_id,
  title,
  section,
  MATCH(title, content) AGAINST ('query syntax') AS relevance
FROM documentation
WHERE MATCH(title, content) AGAINST ('query syntax')
ORDER BY relevance DESC;
```

**4. User Search:**
```sql
-- Search users by name, email, bio
SELECT 
  user_id,
  username,
  email,
  MATCH(username, email, bio) AGAINST ('john developer') AS score
FROM users
WHERE MATCH(username, email, bio) AGAINST ('john developer')
ORDER BY score DESC
LIMIT 10;
```

**5. Multi-Field with Weighted Scoring:**
```sql
-- Boost title matches more than content matches
SELECT 
  article_id,
  title,
  (MATCH(title) AGAINST ('elasticsearch') * 3 + 
   MATCH(content) AGAINST ('elasticsearch') * 1) AS weighted_score
FROM articles
WHERE MATCH(title, content) AGAINST ('elasticsearch')
ORDER BY weighted_score DESC;

-- Different weights for different fields
SELECT 
  product_id,
  name,
  (MATCH(name) AGAINST ('laptop gaming') * 5 +
   MATCH(description) AGAINST ('laptop gaming') * 2 +
   MATCH(tags) AGAINST ('laptop gaming') * 1) AS total_score
FROM products
WHERE MATCH(name, description, tags) AGAINST ('laptop gaming')
ORDER BY total_score DESC;
```

**6. Search with Filters:**
```sql
-- Full-text search combined with exact filters
SELECT * FROM products
WHERE MATCH(name, description) AGAINST ('laptop gaming')
  AND category = 'electronics'
  AND price <= 2000
  AND rating >= 4.0
  AND in_stock = true
ORDER BY MATCH(name, description) AGAINST ('laptop gaming') DESC;
```

**7. Search Across All Text Fields:**
```sql
-- Comprehensive search
SELECT 
  id,
  title,
  MATCH(title, subtitle, description, content, tags, author, category) 
    AGAINST ('data science python') AS score
FROM articles
WHERE MATCH(title, subtitle, description, content, tags, author, category) 
    AGAINST ('data science python')
  AND published = true
ORDER BY score DESC
LIMIT 50;
```

**8. Search with Pagination:**
```sql
-- Paginated search results
SELECT 
  product_id,
  name,
  description,
  MATCH(name, description) AGAINST ('smartphone') AS relevance
FROM products
WHERE MATCH(name, description) AGAINST ('smartphone')
ORDER BY relevance DESC
LIMIT 20 OFFSET 0;  -- First page

-- Second page
SELECT 
  product_id,
  name,
  description,
  MATCH(name, description) AGAINST ('smartphone') AS relevance
FROM products
WHERE MATCH(name, description) AGAINST ('smartphone')
ORDER BY relevance DESC
LIMIT 20 OFFSET 20;  -- Second page
```

**9. Search with Multiple Conditions:**
```sql
-- Combine multiple searches
SELECT * FROM articles
WHERE (MATCH(title) AGAINST ('elasticsearch') 
   OR MATCH(content) AGAINST ('elasticsearch'))
  AND author = 'John Doe'
  AND published_date >= '2024-01-01';

-- Search in different field combinations
SELECT 
  doc_id,
  title,
  MATCH(title, tags) AGAINST ('tutorial') AS title_tag_score,
  MATCH(content) AGAINST ('tutorial') AS content_score
FROM documentation
WHERE MATCH(title, tags, content) AGAINST ('tutorial')
ORDER BY (title_tag_score * 2 + content_score) DESC;
```

**10. Search with Aggregations:**
```sql
-- Count results by category
SELECT 
  category,
  COUNT(*) AS result_count
FROM products
WHERE MATCH(name, description) AGAINST ('wireless')
GROUP BY category
ORDER BY result_count DESC;

-- Average relevance score by category
SELECT 
  category,
  AVG(MATCH(name, description) AGAINST ('laptop')) AS avg_relevance,
  COUNT(*) AS product_count
FROM products
WHERE MATCH(name, description) AGAINST ('laptop')
GROUP BY category
ORDER BY avg_relevance DESC;
```

**11. Search with Date Ranges:**
```sql
-- Recent articles matching search
SELECT 
  article_id,
  title,
  published_date,
  MATCH(title, content) AGAINST ('machine learning') AS score
FROM articles
WHERE MATCH(title, content) AGAINST ('machine learning')
  AND published_date >= DATE_SUB(CURRENT_DATE, INTERVAL 3 MONTH)
ORDER BY score DESC, published_date DESC;
```

**12. Case-Insensitive Search (Automatic):**
```sql
-- Full-text search is automatically case-insensitive
SELECT * FROM products
WHERE MATCH(name) AGAINST ('iPhone');  -- Matches 'iphone', 'IPHONE', 'iPhone'

SELECT * FROM articles
WHERE MATCH(title) AGAINST ('ELASTICSEARCH');  -- Matches 'elasticsearch', 'Elasticsearch', etc.
```

**13. Phrase Search:**
```sql
-- Search for phrases
SELECT * FROM articles
WHERE MATCH(content) AGAINST ('machine learning algorithms');

-- Multi-word product search
SELECT * FROM products
WHERE MATCH(name, description) AGAINST ('wireless bluetooth headphones');
```

**14. Search with NULL Handling:**
```sql
-- MATCH handles NULL fields gracefully
SELECT 
  product_id,
  name,
  description,
  MATCH(name, description, notes) AGAINST ('laptop') AS score
FROM products
WHERE MATCH(name, description, notes) AGAINST ('laptop')
ORDER BY score DESC;
-- Works even if 'notes' is NULL for some records
```

**Performance Tips:**

```sql
-- Good: Use MATCH on full-text indexed fields
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST ('search query')
  AND category = 'technology'  -- Combine with exact filters
  AND published = true;

-- Good: Limit results for better performance
SELECT * FROM products
WHERE MATCH(name, description) AGAINST ('laptop')
ORDER BY MATCH(name, description) AGAINST ('laptop') DESC
LIMIT 100;

-- Good: Use specific fields when possible
SELECT * FROM articles
WHERE MATCH(title) AGAINST ('elasticsearch')  -- Search only in title
ORDER BY MATCH(title) AGAINST ('elasticsearch') DESC;

-- Avoid: Don't use MATCH on non-text fields
-- Bad: WHERE MATCH(product_id) AGAINST ('12345')
-- Good: WHERE product_id = 12345

-- Avoid: Too many fields can impact performance
-- Consider limiting to most relevant fields
-- Bad: MATCH(field1, field2, ..., field20) AGAINST ('query')
-- Good: MATCH(title, content, tags) AGAINST ('query')
```

**Comparison with LIKE:**

```sql
-- LIKE (slower, no relevance scoring, exact substring)
SELECT * FROM articles
WHERE title LIKE '%elasticsearch%' 
   OR content LIKE '%elasticsearch%';

-- MATCH AGAINST (faster, relevance scoring, full-text)
SELECT * FROM articles
WHERE MATCH(title, content) AGAINST ('elasticsearch')
ORDER BY MATCH(title, content) AGAINST ('elasticsearch') DESC;
```

**Common Use Cases:**

```sql
-- 1. Simple keyword search
SELECT * FROM products
WHERE MATCH(name) AGAINST ('laptop');

-- 2. Multi-field search
SELECT * FROM articles
WHERE MATCH(title, content, tags) AGAINST ('elasticsearch tutorial');

-- 3. Ranked results
SELECT 
  title,
  MATCH(title, content) AGAINST ('data science') AS relevance
FROM articles
WHERE MATCH(title, content) AGAINST ('data science')
ORDER BY relevance DESC
LIMIT 10;

-- 4. Search with filters
SELECT * FROM products
WHERE MATCH(name, description) AGAINST ('smartphone')
  AND price BETWEEN 200 AND 800
  AND brand = 'Samsung';

-- 5. Weighted multi-field search
SELECT 
  id,
  title,
  (MATCH(title) AGAINST ('python') * 3 +
   MATCH(content) AGAINST ('python')) AS score
FROM articles
WHERE MATCH(title, content) AGAINST ('python')
ORDER BY score DESC;
```

---

### MATCH AGAINST vs Other String Functions

| Feature                   | MATCH AGAINST    | LIKE       | REGEXP_LIKE  |
|---------------------------|------------------|------------|--------------|
| Full-text search          | ✅ Yes            | ❌ No       | ❌ No         |
| Relevance scoring         | ✅ Yes            | ❌ No       | ❌ No         |
| Multiple fields           | ✅ Yes            | ❌ No       | ❌ No         |
| Stemming/Lemmatization    | ✅ Yes            | ❌ No       | ❌ No         |
| Performance on large text | ✅ Fast           | ❌ Slow     | ❌ Slow       |
| Exact substring           | ⚠️ Use for words | ✅ Yes      | ✅ Yes        |
| Case sensitivity          | ❌ Insensitive    | ⚠️ Depends | ⚠️ Optional  |
| Natural language          | ✅ Yes            | ❌ No       | ❌ No         |

**When to use MATCH AGAINST:**
- Full-text search across documents
- Need relevance scoring
- Search multiple fields simultaneously
- Natural language queries
- Large text content
- Need stemming/language analysis
- Search engine-like functionality

**When to use LIKE:**
- Exact substring matching
- Simple pattern matching
- Single field search
- Short strings
- Exact character sequences

**When to use REGEXP_LIKE:**
- Complex pattern matching
- Need regex capabilities
- Format validation
- Precise pattern requirements

---

### Function Summary

```sql
-- Basic syntax
MATCH(field) AGAINST ('query')

-- Multiple fields
MATCH(field1, field2, field3) AGAINST ('query')

-- With scoring
SELECT field, MATCH(field) AGAINST ('query') AS score
FROM table
WHERE MATCH(field) AGAINST ('query')
ORDER BY score DESC;

-- Combined with filters
SELECT * FROM table
WHERE MATCH(field1, field2) AGAINST ('query')
  AND other_field = 'value'
ORDER BY MATCH(field1, field2) AGAINST ('query') DESC;
```

---

[Back to index](README.md)
