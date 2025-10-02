[Back to index](./README.md)

# Date / Time / Datetime / Timestamp / Interval Functions

**Navigation:** [Aggregate functions](./functions_aggregate.md) · [Operator Precedence](./operator_precedence.md)

This page documents TEMPORAL functions.

---

### Function: CURRENT_TIMESTAMP (Alias: NOW, CURRENT_DATETIME)
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

### Function: CURRENT_DATE (Alias: CURDATE, TODAY)
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

### Function: CURRENT_TIME (Alias: CURTIME)
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

### Function: INTERVAL
**Description:**  
Literal syntax for time intervals.  

**Inputs:**
- n (`INT`)
- `UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`|`MILLISECOND`|`MICROSECOND`|`NANOSECOND`)

**Output:**
- `INTERVAL`
- Note: `INTERVAL` is not a standalone type, it can only be used as part of date/datetime arithmetic functions.

**Example:**
```sql
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 MONTH);
-- Result: 2025-02-10
```

### Function: DATE_ADD (Alias: DATEADD)
**Description:**  
Adds interval to `DATE`.

**Inputs:** 
- `date_expr` (`DATE`)
- `INTERVAL` n (`INT`) `UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`)

**Output:** 
- `DATE`

**Example:**
```sql
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 MONTH) AS next_month;
-- Result: 2025-02-10
```

---

### Function: DATE_SUB (Alias: DATESUB)
**Description:**  
Subtract interval from `DATE`.

**Inputs:** 
- `date_expr` (`DATE`)
- `INTERVAL` n (`INT`) `UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`)

**Output:** 
- `DATE`

**Example:**
```sql
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 7 DAY) AS week_before;
-- Result: 2025-01-03
```

---

### Function: DATETIME_ADD (Alias: DATETIMEADD)
**Description:**  
Adds interval to `DATETIME` / `TIMESTAMP` 

**Inputs:** 
- `datetime_expr` (`DATETIME`)
- `INTERVAL` n (`INT`) `UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

**Output:** 
- `DATETIME`

**Example:**
```sql
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 DAY) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z
```

---

### Function: DATETIME_SUB (Alias: DATETIMESUB)
**Description:**  
Subtract interval from `DATETIME` / `TIMESTAMP`.

**Inputs:** 
- `datetime_expr`
- `INTERVAL` n (`INT`) `UNIT` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

**Output:** 
- `DATETIME`

**Example:**
```sql
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 2 HOUR) AS earlier;
-- Result: 2025-01-10T10:00:00Z
```

---

### Function: DATEDIFF (Alias: DATE_DIFF)
**Description:**  
Difference between 2 dates (date1 - date2) in the specified time unit.

**Inputs:** 
- `date1` (`DATE` or `DATETIME`)
- `date2` (`DATE` or `DATETIME`), 
- optional `unit` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`), `DAY` by default

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
Format `DATE` to `VARCHAR`.

**Inputs:** 
- `date_expr` (`DATE`)
- `pattern` (`VARCHAR`)
- Note: pattern follows [MySQL-style](#supported-mysql-style-datetime-patterns).

**Output:** 
- `VARCHAR`

**Example:**
```sql
-- Simple date formatting
SELECT DATE_FORMAT('2025-01-10'::DATE, '%Y-%m-%d') AS fmt;
-- Result: '2025-01-10'

-- Day of the week (%W)
SELECT DATE_FORMAT('2025-01-10'::DATE, '%W') AS weekday;
-- Result: 'Friday'
```

---

### Function: DATE_PARSE
**Description:**  
Parse `VARCHAR` into `DATE`.

**Inputs:** 
- `VARCHAR`
- `pattern` (`VARCHAR`)
- Note: pattern follows [MySQL-style](#supported-mysql-style-datetime-patterns).

**Output:** 
- `DATE`

**Example:**
```sql
-- Parse ISO-style date
SELECT DATE_PARSE('2025-01-10','%Y-%m-%d') AS d;
-- Result: 2025-01-10

-- Parse with day of week (%W)
SELECT DATE_PARSE('Friday 2025-01-10','%W %Y-%m-%d') AS d;
-- Result: 2025-01-10
```

---

### Function: DATETIME_PARSE
**Description:**  
Parse `VARCHAR` into `DATETIME` / `TIMESTAMP`.

**Inputs:** 
- `VARCHAR`
- `pattern` (`VARCHAR`)
- Note: pattern follows [MySQL-style](#supported-mysql-style-datetime-patterns).

**Output:** 
- `DATETIME`

**Example:**
```sql
-- Parse full datetime with microseconds (%f)
SELECT DATETIME_PARSE('2025-01-10 12:00:00.123456','%Y-%m-%d %H:%i:%s.%f') AS dt;
-- Result: 2025-01-10T12:00:00.123456Z

-- Parse 12-hour clock with AM/PM (%p)
SELECT DATETIME_PARSE('2025-01-10 01:45:30 PM','%Y-%m-%d %h:%i:%s %p') AS dt;
-- Result: 2025-01-10T13:45:30Z
```

---

### Function: DATETIME_FORMAT
**Description:**  
Format `DATETIME` / `TIMESTAMP` to `VARCHAR` with pattern.

**Inputs:** 
- `datetime_expr` (`DATETIME` or `TIMESTAMP`)
- `pattern` (`VARCHAR`)
- Note: pattern follows [MySQL-style](#supported-mysql-style-datetime-patterns).

**Output:** 
- `VARCHAR`

**Example:**
```sql
-- Format with seconds and microseconds
SELECT DATETIME_FORMAT('2025-01-10T12:00:00.123456Z'::TIMESTAMP,'%Y-%m-%d %H:%i:%s.%f') AS s;
-- Result: '2025-01-10 12:00:00.123456'

-- Format 12-hour clock with AM/PM
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP,'%Y-%m-%d %h:%i:%s %p') AS s;
-- Result: '2025-01-10 01:45:30 PM'

-- Format with full weekday name
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP,'%W, %Y-%m-%d') AS s;
-- Result: 'Friday, 2025-01-10'
```

---

### Function: DATE_TRUNC
**Description:**  
Truncate date/datetime to a `unit`.

**Inputs:** 
- `date_or_datetime_expr` (`DATE` or `DATETIME`)
- `unit` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`)

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
- `unit` (`YEAR`|`QUARTER`|`MONTH`|`WEEK`|`DAY`|`HOUR`|`MINUTE`|`SECOND`) `FROM` `date_expr` (`DATE` or `DATETIME`)

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
- `date_expr` (`DATE`)

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
- `date_expr` (`DATE`)

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
- `date_expr` (`DATE`)

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
- `datetime_expr` (`DATETIME` or `TIMESTAMP`)

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
- `date_expr` (`DATE`)

**Output:** 
- `BIGINT`

**Example:**
```sql
SELECT EPOCHDAY('1970-01-02'::DATE) AS d;
-- Result: 1
```

---

### Function: OFFSET_SECONDS
**Description:**  
Timezone offset in seconds.

**Inputs:** 
- `timestamp_expr` (`TIMESTAMP` with timezone)

**Output:** 
- `INT`

**Example:**
```sql
SELECT OFFSET_SECONDS('2025-01-01T12:00:00+02:00'::TIMESTAMP) AS off;
-- Result: 7200
```

---

### Supported MySQL-style Date/Time Patterns

| Pattern | Description                  | Example Output |
|---------|------------------------------|----------------|
| `%Y`    | Year (4 digits)              | `2025`         |
| `%y`    | Year (2 digits)              | `25`           |
| `%m`    | Month (2 digits)             | `01`           |
| `%c`    | Month (1–12)                 | `1`            |
| `%M`    | Month name (full)            | `January`      |
| `%b`    | Month name (abbrev)          | `Jan`          |
| `%d`    | Day of month (2 digits)      | `10`           |
| `%e`    | Day of month (1–31)          | `9`            |
| `%W`    | Weekday name (full)          | `Friday`       |
| `%a`    | Weekday name (abbrev)        | `Fri`          |
| `%H`    | Hour (00–23)                 | `13`           |
| `%h`    | Hour (01–12)                 | `01`           |
| `%I`    | Hour (01–12, synonym for %h) | `01`           |
| `%i`    | Minutes (00–59)              | `45`           |
| `%s`    | Seconds (00–59)              | `30`           |
| `%f`    | Microseconds (000–999)       | `123`          |
| `%p`    | AM/PM marker                 | `AM` / `PM`    |

[Back to index](./README.md)
