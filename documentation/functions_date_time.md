[Back to index](./README.md)

# Date / Time / Datetime / Interval Functions

**Navigation:** [Aggregate functions](./functions_aggregate.md) · [Operator Precedence](./operator_precedence.md)

This page documents DATE vs DATETIME functions clearly, with DATETIME_* variants separated.

---

### Function: CURRENT_TIMESTAMP (Aliases: NOW, CURRENT_DATETIME)
**Description:** Returns current datetime (ZonedDateTime) in UTC.
**Inputs:** none
**Output:** TIMESTAMP / DATETIME
**Example:**
```sql
SELECT CURRENT_TIMESTAMP AS now;
-- Result: 2025-09-26T12:34:56Z
```

---

### Function: CURRENT_DATE (Aliases: CURDATE, TODAY)
**Description:** Returns current date as DATE.
**Inputs:** none
**Output:** DATE
**Example:**
```sql
SELECT CURRENT_DATE AS today;
-- Result: 2025-09-26
```

---

### Function: CURRENT_TIME (Aliases: CURTIME)
**Description:** Returns current time-of-day.
**Inputs:** none
**Output:** TIME
**Example:**
```sql
SELECT CURRENT_TIME AS t;
```

---

### Function: DATE_ADD / DATEADD (Aliases: DATEADD)
**Description:** Adds interval to DATE (LocalDate arithmetic). Use for DATE only.
**Inputs:** `date_expr` (DATE), `INTERVAL n UNIT` (YEAR|MONTH|WEEK|DAY)
**Output:** DATE
**Example:**
```sql
SELECT DATE_ADD(DATE '2025-01-10', INTERVAL 1 MONTH) AS next_month;
-- Result: 2025-02-10
```

---

### Function: DATE_SUB / DATESUB
**Description:** Subtract interval from DATE.
**Inputs:** `date_expr` (DATE), `INTERVAL n UNIT`
**Output:** DATE
**Example:**
```sql
SELECT DATE_SUB(DATE '2025-01-10', INTERVAL 7 DAY) AS week_before;
-- Result: 2025-01-03
```

---

### Function: DATETIME_ADD / DATETIMEADD
**Description:** Adds interval to DATETIME/TIMESTAMP (ZonedDateTime arithmetic). Use for DATETIME only.
**Inputs:** `datetime_expr` (DATETIME), `INTERVAL n UNIT` (including HOUR|MINUTE|SECOND)
**Output:** DATETIME
**Example:**
```sql
SELECT DATETIME_ADD(TIMESTAMP '2025-01-10T12:00:00Z', INTERVAL 1 DAY) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z
```

---

### Function: DATETIME_SUB / DATETIMESUB
**Description:** Subtract interval from DATETIME/TIMESTAMP.
**Inputs:** `datetime_expr`, `INTERVAL n UNIT`
**Output:** DATETIME
**Example:**
```sql
SELECT DATETIME_SUB(TIMESTAMP '2025-01-10T12:00:00Z', INTERVAL 2 HOUR) AS earlier;
```

---

### Function: DATEDIFF / DATE_DIFF
**Description:** Difference in days (date1 - date2).
**Inputs:** `date1`, `date2` (DATE or DATETIME)
**Output:** BIGINT
**Example:**
```sql
SELECT DATEDIFF(DATE '2025-01-10', DATE '2025-01-01') AS diff;
-- Result: 9
```

---

### Function: DATE_FORMAT
**Description:** Format DATE/DATETIME to string using Java DateTimeFormatter.
**Inputs:** `date_expr`, `pattern`
**Output:** VARCHAR
**Example:**
```sql
SELECT DATE_FORMAT(DATE '2025-01-10', 'yyyy-MM-dd') AS fmt;
-- Result: '2025-01-10'
```

---

### Function: DATE_PARSE
**Description:** Parse string into DATE.
**Inputs:** `string`, `pattern`
**Output:** DATE
**Example:**
```sql
SELECT DATE_PARSE('2025-01-10','yyyy-MM-dd') AS d;
```

---

### Function: DATETIME_PARSE
**Description:** Parse string into DATETIME/TIMESTAMP.
**Inputs:** `string`, `pattern`
**Output:** DATETIME (ZonedDateTime)
**Example:**
```sql
SELECT DATETIME_PARSE('2025-01-10T12:00:00Z','yyyy-MM-dd''T''HH:mm:ssX') AS dt;
```

---

### Function: DATETIME_FORMAT
**Description:** Format DATETIME/TIMESTAMP to string with pattern.
**Inputs:** `datetime_expr`, `pattern`
**Output:** VARCHAR
**Example:**
```sql
SELECT DATETIME_FORMAT(TIMESTAMP '2025-01-10T12:00:00Z','yyyy-MM-dd HH:mm:ss') AS s;
```

---

### Function: DATE_TRUNC
**Description:** Truncate date/datetime to a unit.
**Inputs:** `date_or_datetime_expr`, `unit`
**Output:** DATE or DATETIME
**Example:**
```sql
SELECT DATE_TRUNC(DATE '2025-01-15', MONTH) AS start_month;
-- Result: 2025-01-01
```

---

### Function: EXTRACT
**Description:** Extract field from date or datetime.
**Inputs:** `unit FROM date_expr`
**Output:** INTEGER / BIGINT
**Example:**
```sql
SELECT EXTRACT(YEAR FROM TIMESTAMP '2025-01-10T12:00:00Z') AS y;
-- Result: 2025
```

---

### Function: LAST_DAY
**Description:** Last day of month for a date.
**Inputs:** `date_expr`
**Output:** DATE
**Example:**
```sql
SELECT LAST_DAY(DATE '2025-02-15') AS ld;
-- Result: 2025-02-28
```

---

### Function: WEEK
**Description:** ISO week number (1..53)
**Inputs:** `date_expr`
**Output:** INTEGER
**Example:**
```sql
SELECT WEEK(DATE '2025-01-01') AS w;
-- Result: 1
```

---

### Function: QUARTER
**Description:** Quarter number (1..4)
**Inputs:** `date_expr`
**Output:** INTEGER
**Example:**
```sql
SELECT QUARTER(DATE '2025-05-10') AS q;
-- Result: 2
```

---

### Function: NANOSECOND / MICROSECOND / MILLISECOND
**Description:** Sub-second extraction.
**Inputs:** `datetime_expr`
**Output:** INTEGER
**Example:**
```sql
SELECT MILLISECOND(TIMESTAMP '2025-01-01T12:00:00.123Z') AS ms;
-- Result: 123
```

---

### Function: EPOCHDAY
**Description:** Days since epoch.
**Inputs:** `date_expr`
**Output:** BIGINT
**Example:**
```sql
SELECT EPOCHDAY(DATE '1970-01-02') AS d;
-- Result: 1
```

---

### Function: OFFSET / OFFSET_SECONDS
**Description:** Timezone offset in seconds.
**Inputs:** `timestamp_expr`
**Output:** INTEGER
**Example:**
```sql
SELECT OFFSET(TIMESTAMP '2025-01-01T12:00:00+02:00') AS off;
-- Result: 7200
```

[Back to index](./README.md)
