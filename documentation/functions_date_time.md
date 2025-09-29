[Back to index](./README.md)

# Date / Time / Datetime / Timestamp / Interval Functions

**Navigation:** [Aggregate functions](./functions_aggregate.md) · [Operator Precedence](./operator_precedence.md)

This page documents TEMPORAL functions.

---

### Function: CURRENT_TIMESTAMP (Aliases: NOW, CURRENT_DATETIME)
**Description:**  
Returns current datetime (ZonedDateTime) in UTC.

**Inputs:** 
- none

**Output:** 
- `TIMESTAMP` / `DATETIME`

**Example:**
```sql
SELECT CURRENT_TIMESTAMP AS now;
-- Result: 2025-09-26T12:34:56Z
```

---

### Function: CURRENT_DATE (Aliases: CURDATE, TODAY)
**Description:**  
Returns current date as `DATE`.

**Inputs:** 
- none

**Output:** 
- `DATE`

**Example:**
```sql
SELECT CURRENT_DATE AS today;
-- Result: 2025-09-26
```

---

### Function: CURRENT_TIME (Aliases: CURTIME)
**Description:**  
Returns current time-of-day.

**Inputs:** 
- none

**Output:** 
- `TIME`

**Example:**
```sql
SELECT CURRENT_TIME AS t;
-- Result: 12:34:56
```

---

### Function: DATE_ADD / DATEADD (Aliases: DATEADD)
**Description:**  
Adds interval to `DATE`.

**Inputs:** 
- `date_expr` (`DATE`), `INTERVAL n UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`)

**Output:** 
- `DATE`

**Example:**
```sql
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 MONTH) AS next_month;
-- Result: 2025-02-10
```

---

### Function: DATE_SUB / DATESUB
**Description:**  
Subtract interval from `DATE`.

**Inputs:** 
- `date_expr` (`DATE`), `INTERVAL n UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`)

**Output:** 
- `DATE`

**Example:**
```sql
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 7 DAY) AS week_before;
-- Result: 2025-01-03
```

---

### Function: DATETIME_ADD / DATETIMEADD
**Description:**  
Adds interval to `DATETIME` / `TIMESTAMP` 

**Inputs:** 
- `datetime_expr` (`DATETIME`), `INTERVAL n UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

**Output:** 
- `DATETIME`

**Example:**
```sql
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 DAY) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z
```

---

### Function: DATETIME_SUB / DATETIMESUB
**Description:**  
Subtract interval from `DATETIME` / `TIMESTAMP`.

**Inputs:** 
- `datetime_expr`, `INTERVAL n UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

**Output:** 
- `DATETIME`

**Example:**
```sql
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 2 HOUR) AS earlier;
-- Result: 2025-01-10T10:00:00Z
```

---

### Function: DATEDIFF / DATE_DIFF
**Description:**  
Difference between 2 dates (date1 - date2) in the specified time unit.

**Inputs:** 
- `date1` (`DATE` or `DATETIME`), `date2` (`DATE` or `DATETIME`), `unit` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT DATEDIFF('2025-01-10'::DATE, '2025-01-01'::DATE) AS diff;
-- Result: 9
```

---

### Function: DATE_FORMAT
**Description:**  
Format `DATE` / `DATETIME` to `VARCHAR`.

**Inputs:** 
- `date_expr`, `pattern`

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT DATE_FORMAT('2025-01-10'::DATE, 'yyyy-MM-dd') AS fmt;
-- Result: '2025-01-10'
```

---

### Function: DATE_PARSE
**Description:**  
Parse `VARCHAR` into `DATE`.

**Inputs:** 
- `VARCHAR`, `pattern`

**Output:** 
- `DATE`

**Example:**
```sql
SELECT DATE_PARSE('2025-01-10','yyyy-MM-dd') AS d;
-- Result: 2025-01-10
```

---

### Function: DATETIME_PARSE
**Description:**  
Parse `VARCHAR` into `DATETIME` / `TIMESTAMP`.

**Inputs:** 
- `VARCHAR`, `pattern`

**Output:** 
- `DATETIME`

**Example:**
```sql
SELECT DATETIME_PARSE('2025-01-10T12:00:00Z','yyyy-MM-dd''T''HH:mm:ssZ') AS dt;
-- Result: 2025-01-10T12:00:00Z
```

---

### Function: DATETIME_FORMAT
**Description:**  
Format `DATETIME` / `TIMESTAMP` to `VARCHAR` with pattern.

**Inputs:** `datetime_expr`, `pattern`

**Output:** 
- `VARCHAR`

**Example:**
```sql
SELECT DATETIME_FORMAT('2025-01-10T12:00:00Z'::TIMESTAMP,'yyyy-MM-dd HH:mm:ss') AS s;
```

---

### Function: DATE_TRUNC
**Description:**  
Truncate date/datetime to a `unit` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`).

**Inputs:** 
- `date_or_datetime_expr`, `unit`

**Output:** 
- `DATE` or `DATETIME`

**Example:**
```sql
SELECT DATE_TRUNC('2025-01-15'::DATE, MONTH) AS start_month;
-- Result: 2025-01-01
```

---

### Function: EXTRACT
**Description:**  
Extract field from date or datetime.

**Inputs:** 
- `unit FROM date_expr`

**Output:** 
- `INT` / `BIGINT`

**Example:**
```sql
SELECT EXTRACT(YEAR FROM '2025-01-10T12:00:00Z'::TIMESTAMP) AS y;
-- Result: 2025
```

---

### Function: LAST_DAY
**Description:**  
Last day of month for a date.

**Inputs:** 
- `date_expr`

**Output:** 
- `DATE`

**Example:**
```sql
SELECT LAST_DAY('2025-02-15'::DATE) AS ld;
-- Result: 2025-02-28
```

---

### Function: WEEK
**Description:**  
ISO week number (1..53)

**Inputs:** 
- `date_expr`

**Output:** 
- `INT`

**Example:**
```sql
SELECT WEEK('2025-01-01'::DATE) AS w;
-- Result: 1
```

---

### Function: QUARTER
**Description:**  
Quarter number (1..4)

**Inputs:** 
- `date_expr`

**Output:** 
- `INT`

**Example:**
```sql
SELECT QUARTER('2025-05-10'::DATE) AS q;
-- Result: 2
```

---

### Function: NANOSECOND / MICROSECOND / MILLISECOND
**Description:**  
Sub-second extraction.

**Inputs:** 
- `datetime_expr`

**Output:** 
- `INT`

**Example:**
```sql
SELECT MILLISECOND('2025-01-01T12:00:00.123Z'::TIMESTAMP) AS ms;
-- Result: 123
```

---

### Function: EPOCHDAY
**Description:**  
Days since epoch.

**Inputs:** 
- `date_expr`

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT EPOCHDAY('1970-01-02'::DATE) AS d;
-- Result: 1
```

---

### Function: OFFSET / OFFSET_SECONDS
**Description:**  
Timezone offset in seconds.

**Inputs:** 
- `timestamp_expr`

**Output:** 
- `INT`

**Example:**
```sql
SELECT OFFSET('2025-01-01T12:00:00+02:00'::TIMESTAMP) AS off;
-- Result: 7200
```

[Back to index](./README.md)
