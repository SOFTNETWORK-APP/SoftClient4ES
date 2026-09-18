[Back to index](README.md)

# Date / Time / Datetime / Timestamp / Interval Functions

**Navigation:** [Aggregate functions](functions_aggregate.md) · [Operator Precedence](operator_precedence.md)

## Date/Time Functions

This page documents TEMPORAL functions.

---

### Current Date/Time Functions

#### CURRENT_TIMESTAMP / NOW / CURRENT_DATETIME

Returns current datetime (ZonedDateTime) in UTC.

**Syntax:**
```sql
CURRENT_TIMESTAMP
NOW()
CURRENT_DATETIME
```

**Inputs:**
- None

**Output:**
- `TIMESTAMP` / `DATETIME`

**Examples:**
```sql
SELECT CURRENT_TIMESTAMP AS now;
-- Result: 2025-09-26T12:34:56Z

SELECT NOW() AS current_time;
-- Result: 2025-09-26T12:34:56Z

SELECT CURRENT_DATETIME AS dt;
-- Result: 2025-09-26T12:34:56Z
```

---

#### CURRENT_DATE / CURDATE / TODAY

Returns current date as `DATE`.

**Syntax:**
```sql
CURRENT_DATE
CURDATE()
TODAY()
```

**Inputs:**
- None

**Output:**
- `DATE`

**Examples:**
```sql
SELECT CURRENT_DATE AS today;
-- Result: 2025-09-26

SELECT CURDATE() AS today;
-- Result: 2025-09-26

SELECT TODAY() AS today;
-- Result: 2025-09-26
```

---

#### CURRENT_TIME / CURTIME

Returns current time-of-day.

**Syntax:**
```sql
CURRENT_TIME
CURTIME()
```

**Inputs:**
- None

**Output:**
- `TIME`

**Examples:**
```sql
SELECT CURRENT_TIME AS t;
-- Result: 12:34:56

SELECT CURTIME() AS current_time;
-- Result: 12:34:56
```

---

### Date/Time Arithmetic Functions

#### INTERVAL

Literal syntax for time intervals.

**Syntax:**
```sql
INTERVAL n UNIT
```

**Inputs:**
- `n` - `INT` value
- `UNIT` - One of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, `MILLISECOND`, `MICROSECOND`, `NANOSECOND`

**Output:**
- `INTERVAL`

**Note:** `INTERVAL` is not a standalone type; it can only be used as part of date/datetime arithmetic functions.

**Examples:**
```sql
-- Used with DATE_ADD
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 MONTH);
-- Result: 2025-02-10

-- Used with DATETIME_SUB
SELECT DATETIME_SUB(NOW(), INTERVAL 7 DAY);
-- Result: 7 days ago

-- Various intervals
INTERVAL 1 YEAR
INTERVAL 3 MONTH
INTERVAL 7 DAY
INTERVAL 2 HOUR
INTERVAL 30 MINUTE
INTERVAL 45 SECOND
```

---

#### DATE_ADD / DATEADD

Adds interval to `DATE`.

**Syntax:**
```sql
DATE_ADD(date_expr, INTERVAL n UNIT)
DATEADD(date_expr, INTERVAL n UNIT)
```

**Inputs:**
- `date_expr` - `DATE`
- `INTERVAL n UNIT` - where `UNIT` is one of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`

**Output:**
- `DATE`

**Examples:**
```sql
-- Add 1 month
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 MONTH) AS next_month;
-- Result: 2025-02-10

-- Add 7 days
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 7 DAY) AS next_week;
-- Result: 2025-01-17

-- Add 1 year
SELECT DATEADD('2025-01-10'::DATE, INTERVAL 1 YEAR) AS next_year;
-- Result: 2026-01-10

-- Add 2 weeks
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 2 WEEK) AS two_weeks_later;
-- Result: 2025-01-24

-- Add 1 quarter
SELECT DATE_ADD('2025-01-10'::DATE, INTERVAL 1 QUARTER) AS next_quarter;
-- Result: 2025-04-10
```

---

#### DATE_SUB / DATESUB

Subtract interval from `DATE`.

**Syntax:**
```sql
DATE_SUB(date_expr, INTERVAL n UNIT)
DATESUB(date_expr, INTERVAL n UNIT)
```

**Inputs:**
- `date_expr` - `DATE`
- `INTERVAL n UNIT` - where `UNIT` is one of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`

**Output:**
- `DATE`

**Examples:**
```sql
-- Subtract 7 days
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 7 DAY) AS week_before;
-- Result: 2025-01-03

-- Subtract 1 month
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 1 MONTH) AS last_month;
-- Result: 2024-12-10

-- Subtract 1 year
SELECT DATESUB('2025-01-10'::DATE, INTERVAL 1 YEAR) AS last_year;
-- Result: 2024-01-10

-- Subtract 2 weeks
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 2 WEEK) AS two_weeks_ago;
-- Result: 2024-12-27

-- Subtract 1 quarter
SELECT DATE_SUB('2025-01-10'::DATE, INTERVAL 1 QUARTER) AS last_quarter;
-- Result: 2024-10-10
```

---

#### DATETIME_ADD / DATETIMEADD / TIMESTAMPADD

Adds interval to `DATETIME` / `TIMESTAMP`.

Two argument orders are accepted, and they mean the same thing. The second — **unit first, with a
plain count instead of an `INTERVAL`** — is the ODBC/JDBC and T-SQL order, which is what a BI tool
emits. `TIMESTAMPADD` is the ODBC name for it.

**Syntax:**
```sql
DATETIME_ADD(datetime_expr, INTERVAL n UNIT)
DATETIMEADD(datetime_expr, INTERVAL n UNIT)

DATETIME_ADD(UNIT, n, datetime_expr)
TIMESTAMPADD(UNIT, n, datetime_expr)
```

**Inputs:**
- `datetime_expr` - `DATETIME` or `TIMESTAMP`
- `INTERVAL n UNIT` - where `UNIT` is one of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`
- In the unit-first form, `n` is a plain integer and may be **negative**; the ODBC interval names
  `SQL_TSI_YEAR`, `SQL_TSI_QUARTER`, `SQL_TSI_MONTH`, `SQL_TSI_WEEK`, `SQL_TSI_DAY`, `SQL_TSI_HOUR`,
  `SQL_TSI_MINUTE` and `SQL_TSI_SECOND` are accepted as spellings of the units above

> There is **no `TIMESTAMPSUB`** — it exists in neither ODBC nor MySQL. Subtract by passing a
> negative count: `TIMESTAMPADD(DAY, -89, CURRENT_DATE)`. (`DATETIME_SUB` is available for the
> `INTERVAL` form.)

**Output:**
- `DATETIME`

**Examples:**
```sql
-- Add 1 day
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 DAY) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z

-- The same thing in the ODBC unit-first order
SELECT TIMESTAMPADD(DAY, 1, '2025-01-10T12:00:00Z'::TIMESTAMP) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z

-- ODBC interval name, and a negative count instead of a subtraction
SELECT TIMESTAMPADD(SQL_TSI_DAY, 1, '2025-01-10T12:00:00Z'::TIMESTAMP) AS tomorrow;
-- Result: 2025-01-11T12:00:00Z
SELECT TIMESTAMPADD(DAY, -89, CURRENT_DATE) AS ninety_days_ago;

-- Add 2 hours
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 2 HOUR) AS later;
-- Result: 2025-01-10T14:00:00Z

-- Add 30 minutes
SELECT DATETIMEADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 30 MINUTE) AS half_hour_later;
-- Result: 2025-01-10T12:30:00Z

-- Add 45 seconds
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 45 SECOND) AS seconds_later;
-- Result: 2025-01-10T12:00:45Z

-- Add 1 month
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 MONTH) AS next_month;
-- Result: 2025-02-10T12:00:00Z

-- Add 1 year
SELECT DATETIME_ADD('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 YEAR) AS next_year;
-- Result: 2026-01-10T12:00:00Z
```

---

#### DATETIME_SUB / DATETIMESUB

Subtract interval from `DATETIME` / `TIMESTAMP`.

**Syntax:**
```sql
DATETIME_SUB(datetime_expr, INTERVAL n UNIT)
DATETIMESUB(datetime_expr, INTERVAL n UNIT)
```

**Inputs:**
- `datetime_expr` - `DATETIME` or `TIMESTAMP`
- `INTERVAL n UNIT` - where `UNIT` is one of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`

**Output:**
- `DATETIME`

**Examples:**
```sql
-- Subtract 2 hours
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 2 HOUR) AS earlier;
-- Result: 2025-01-10T10:00:00Z

-- Subtract 1 day
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 DAY) AS yesterday;
-- Result: 2025-01-09T12:00:00Z

-- Subtract 30 minutes
SELECT DATETIMESUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 30 MINUTE) AS half_hour_ago;
-- Result: 2025-01-10T11:30:00Z

-- Subtract 7 days
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 7 DAY) AS week_ago;
-- Result: 2025-01-03T12:00:00Z

-- Subtract 1 month
SELECT DATETIME_SUB('2025-01-10T12:00:00Z'::TIMESTAMP, INTERVAL 1 MONTH) AS last_month;
-- Result: 2024-12-10T12:00:00Z
```

---

### Date/Time Difference Functions

#### DATE_DIFF / TIMESTAMPDIFF

Difference between 2 dates, in the specified time unit. **The result is `date2 - date1`** — the
first argument is the START and the second is the END. That is what makes
`DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` an age rather than a negative one, and it is how MySQL
defines `TIMESTAMPDIFF(unit, dt1, dt2)` too.

`TIMESTAMPDIFF` is the ODBC/JDBC spelling and takes the **unit first**.

> ⚠️ **`DATEDIFF` is a different function** — MySQL's — and subtracts the other way round. It has
> its own entry below. Do not assume the two names are interchangeable.

**Syntax:**
```sql
DATE_DIFF(date1, date2)
DATE_DIFF(date1, date2, unit)

DATE_DIFF(unit, date1, date2)
TIMESTAMPDIFF(unit, date1, date2)
```

> The unit-first form is the one the engine **renders back**, so `TIMESTAMPDIFF(DAY, a, b)` reads
> as `DATE_DIFF(DAY, a, b)` wherever a statement is echoed to you.

**Inputs:**
- `date1` - `DATE` or `DATETIME` — the **start**
- `date2` - `DATE` or `DATETIME` — the **end**
- `unit` (optional in the date-first form) - One of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`
  - Default: `DAY`
  - The ODBC `SQL_TSI_*` spellings are accepted here too

**Output:**
- `BIGINT` — negative when `date2` is earlier than `date1`

**Examples:**
```sql
-- Difference in days (default)
SELECT DATE_DIFF('2025-01-01'::DATE, '2025-01-10'::DATE) AS diff;
-- Result: 9

-- Reverse the arguments and the sign reverses
SELECT DATE_DIFF('2025-01-10'::DATE, '2025-01-01'::DATE, DAY) AS diff;
-- Result: -9

-- Weeks, months, years
SELECT DATE_DIFF('2025-01-01'::DATE, '2025-01-31'::DATE, WEEK) AS diff_weeks;
-- Result: 4
SELECT DATE_DIFF('2025-01-01'::DATE, '2025-06-01'::DATE, MONTH) AS diff_months;
-- Result: 5
SELECT DATE_DIFF('2025-01-01'::DATE, '2027-01-01'::DATE, YEAR) AS diff_years;
-- Result: 2

-- Hours, minutes, seconds
SELECT DATE_DIFF('2025-01-10T12:00:00Z'::TIMESTAMP, '2025-01-10T14:00:00Z'::TIMESTAMP, HOUR) AS diff_hours;
-- Result: 2
SELECT DATE_DIFF('2025-01-10T12:00:00Z'::TIMESTAMP, '2025-01-10T12:30:00Z'::TIMESTAMP, MINUTE) AS diff_minutes;
-- Result: 30
SELECT DATE_DIFF('2025-01-10T12:00:00Z'::TIMESTAMP, '2025-01-10T12:00:45Z'::TIMESTAMP, SECOND) AS diff_seconds;
-- Result: 45

-- The ODBC spelling, unit first - same answer, same sign
SELECT TIMESTAMPDIFF(DAY, '2025-01-01'::DATE, '2025-01-10'::DATE) AS diff_days;
-- Result: 9

-- Age: start = birthdate, end = today
SELECT DATE_DIFF(birthdate, CURRENT_DATE, YEAR) AS age FROM users;
```

---

#### DATEDIFF

MySQL's day-difference function. **`DATEDIFF(expr1, expr2)` returns `expr1 - expr2`** — the opposite
subtraction from `DATE_DIFF` above, because that is what MySQL defines and what a statement written
for MySQL expects.

> 🔴 **The two-argument and three-argument forms subtract in opposite directions, and this is the
> one place in the dialect where that is true.**
>
> | call | result | why |
> | --- | --- | --- |
> | `DATEDIFF(a, b)` | `a - b` | MySQL's function, days only — this is the whole of what MySQL defines |
> | `DATEDIFF(a, b, unit)` | `b - a` | **not MySQL** — this engine's own extension, which follows `DATE_DIFF` |
>
> So adding a unit to a two-argument `DATEDIFF` — which looks purely clarifying — **reverses the
> sign**. If you want a unit, prefer `DATE_DIFF(start, end, unit)` or
> `TIMESTAMPDIFF(unit, start, end)`, where one rule holds at every arity.

**Syntax:**
```sql
DATEDIFF(expr1, expr2)
DATEDIFF(date1, date2, unit)
```

**Inputs:**
- `expr1`, `expr2` - `DATE` or `DATETIME`
- `unit` (three-argument form only) - as for `DATE_DIFF` above

**Output:**
- `BIGINT`

**Examples:**
```sql
-- MySQL's own two documented examples, and this engine agrees with them
SELECT DATEDIFF('2007-12-31'::DATE, '2007-12-30'::DATE) AS diff;
-- Result: 1
SELECT DATEDIFF('2010-11-30'::DATE, '2010-12-31'::DATE) AS diff;
-- Result: -31

-- The two arities disagree, on purpose
SELECT DATEDIFF('2025-01-10'::DATE, '2025-01-01'::DATE) AS diff;
-- Result: 9
SELECT DATEDIFF('2025-01-10'::DATE, '2025-01-01'::DATE, DAY) AS diff;
-- Result: -9
```

> **Before engine `0.24.0`, `DATEDIFF` returned the opposite sign** — it was an alias of `DATE_DIFF`
> and inherited its order, so MySQL's `DATEDIFF('2007-12-31','2007-12-30')` answered `-1` here
> instead of `1`. Statements written against the old behaviour need their arguments swapped.

---

### Date/Time Formatting Functions

#### DATE_FORMAT

Format `DATE` to `VARCHAR`.

**Syntax:**
```sql
DATE_FORMAT(date_expr, pattern)
```

**Inputs:**
- `date_expr` - `DATE`
- `pattern` - `VARCHAR` (MySQL-style pattern)

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Simple date formatting
SELECT DATE_FORMAT('2025-01-10'::DATE, '%Y-%m-%d') AS fmt;
-- Result: '2025-01-10'

-- Day of the week (full name)
SELECT DATE_FORMAT('2025-01-10'::DATE, '%W') AS weekday;
-- Result: 'Friday'

-- Month name (full)
SELECT DATE_FORMAT('2025-01-10'::DATE, '%M') AS month_name;
-- Result: 'January'

-- Custom format
SELECT DATE_FORMAT('2025-01-10'::DATE, '%W, %M %d, %Y') AS formatted;
-- Result: 'Friday, January 10, 2025'

-- Short format
SELECT DATE_FORMAT('2025-01-10'::DATE, '%m/%d/%y') AS short_date;
-- Result: '01/10/25'

-- Day of month without leading zero
SELECT DATE_FORMAT('2025-01-09'::DATE, '%e') AS day;
-- Result: '9'

-- Abbreviated weekday and month
SELECT DATE_FORMAT('2025-01-10'::DATE, '%a, %b %d') AS abbrev;
-- Result: 'Fri, Jan 10'
```

---

#### DATE_PARSE

Parse `VARCHAR` into `DATE`.

**Syntax:**
```sql
DATE_PARSE(string, pattern)
```

**Inputs:**
- `string` - `VARCHAR`
- `pattern` - `VARCHAR` (MySQL-style pattern)

**Output:**
- `DATE`

**Examples:**
```sql
-- Parse ISO-style date
SELECT DATE_PARSE('2025-01-10', '%Y-%m-%d') AS d;
-- Result: 2025-01-10

-- Parse with day of week
SELECT DATE_PARSE('Friday 2025-01-10', '%W %Y-%m-%d') AS d;
-- Result: 2025-01-10

-- Parse US format
SELECT DATE_PARSE('01/10/2025', '%m/%d/%Y') AS d;
-- Result: 2025-01-10

-- Parse with month name
SELECT DATE_PARSE('January 10, 2025', '%M %d, %Y') AS d;
-- Result: 2025-01-10

-- Parse with abbreviated month
SELECT DATE_PARSE('Jan 10, 2025', '%b %d, %Y') AS d;
-- Result: 2025-01-10

-- Parse 2-digit year
SELECT DATE_PARSE('10/01/25', '%d/%m/%y') AS d;
-- Result: 2025-01-10
```

---

#### DATETIME_FORMAT

Format `DATETIME` / `TIMESTAMP` to `VARCHAR` with pattern.

**Syntax:**
```sql
DATETIME_FORMAT(datetime_expr, pattern)
```

**Inputs:**
- `datetime_expr` - `DATETIME` or `TIMESTAMP`
- `pattern` - `VARCHAR` (MySQL-style pattern)

**Output:**
- `VARCHAR`

**Examples:**
```sql
-- Format with seconds and microseconds
SELECT DATETIME_FORMAT('2025-01-10T12:00:00.123456Z'::TIMESTAMP, '%Y-%m-%d %H:%i:%s.%f') AS s;
-- Result: '2025-01-10 12:00:00.123456'

-- Format 12-hour clock with AM/PM
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%Y-%m-%d %h:%i:%s %p') AS s;
-- Result: '2025-01-10 01:45:30 PM'

-- Format with full weekday name
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%W, %Y-%m-%d') AS s;
-- Result: 'Friday, 2025-01-10'

-- Full datetime with day name and month name
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%W, %M %d, %Y at %h:%i %p') AS s;
-- Result: 'Friday, January 10, 2025 at 01:45 PM'

-- ISO 8601 format
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%Y-%m-%dT%H:%i:%s') AS s;
-- Result: '2025-01-10T13:45:30'

-- 24-hour format
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%H:%i:%s') AS s;
-- Result: '13:45:30'

-- 12-hour format
SELECT DATETIME_FORMAT('2025-01-10T13:45:30Z'::TIMESTAMP, '%h:%i:%s %p') AS s;
-- Result: '01:45:30 PM'
```

---

#### DATETIME_PARSE

Parse `VARCHAR` into `DATETIME` / `TIMESTAMP`.

**Syntax:**
```sql
DATETIME_PARSE(string, pattern)
```

**Inputs:**
- `string` - `VARCHAR`
- `pattern` - `VARCHAR` (MySQL-style pattern)

**Output:**
- `DATETIME`

**Examples:**
```sql
-- Parse full datetime with microseconds
SELECT DATETIME_PARSE('2025-01-10 12:00:00.123456', '%Y-%m-%d %H:%i:%s.%f') AS dt;
-- Result: 2025-01-10T12:00:00.123456Z

-- Parse 12-hour clock with AM/PM
SELECT DATETIME_PARSE('2025-01-10 01:45:30 PM', '%Y-%m-%d %h:%i:%s %p') AS dt;
-- Result: 2025-01-10T13:45:30Z

-- Parse ISO 8601 format
SELECT DATETIME_PARSE('2025-01-10T13:45:30', '%Y-%m-%dT%H:%i:%s') AS dt;
-- Result: 2025-01-10T13:45:30Z

-- Parse with full day and month names
SELECT DATETIME_PARSE('Friday, January 10, 2025 at 01:45 PM', '%W, %M %d, %Y at %h:%i %p') AS dt;
-- Result: 2025-01-10T13:45:00Z

-- Parse 24-hour format
SELECT DATETIME_PARSE('2025-01-10 13:45:30', '%Y-%m-%d %H:%i:%s') AS dt;
-- Result: 2025-01-10T13:45:30Z

-- Parse abbreviated names
SELECT DATETIME_PARSE('Fri, Jan 10, 2025 1:45 PM', '%a, %b %d, %Y %h:%i %p') AS dt;
-- Result: 2025-01-10T13:45:00Z
```

---

### Date/Time Truncation Function

#### DATE_TRUNC

Truncate date/datetime to a `unit`.

**Syntax:**
```sql
DATE_TRUNC(date_or_datetime_expr, unit)
```

**Inputs:**
- `date_or_datetime_expr` - `DATE` or `DATETIME`
- `unit` - One of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`

**Output:**
- `DATE` or `DATETIME` (same type as input)

**Examples:**
```sql
-- Truncate to start of month
SELECT DATE_TRUNC('2025-01-15'::DATE, MONTH) AS start_month;
-- Result: 2025-01-01

-- Truncate to start of year
SELECT DATE_TRUNC('2025-06-15'::DATE, YEAR) AS start_year;
-- Result: 2025-01-01

-- Truncate to start of quarter
SELECT DATE_TRUNC('2025-05-15'::DATE, QUARTER) AS start_quarter;
-- Result: 2025-04-01

-- Truncate to start of week
SELECT DATE_TRUNC('2025-01-15'::DATE, WEEK) AS start_week;
-- Result: 2025-01-13 (Monday)

-- Truncate datetime to hour
SELECT DATE_TRUNC('2025-01-10T12:34:56Z'::TIMESTAMP, HOUR) AS start_hour;
-- Result: 2025-01-10T12:00:00Z

-- Truncate datetime to minute
SELECT DATE_TRUNC('2025-01-10T12:34:56Z'::TIMESTAMP, MINUTE) AS start_minute;
-- Result: 2025-01-10T12:34:00Z

-- Truncate datetime to day
SELECT DATE_TRUNC('2025-01-10T12:34:56Z'::TIMESTAMP, DAY) AS start_day;
-- Result: 2025-01-10T00:00:00Z
```

---

### Date/Time Extraction Functions

#### EXTRACT

Extract field from date or datetime.

**Syntax:**
```sql
EXTRACT(unit FROM date_expr)
```

**Inputs:**
- `unit` - One of: `YEAR`, `QUARTER`, `MONTH`, `WEEK`, `DAY`, `HOUR`, `MINUTE`, `SECOND`
- `date_expr` - `DATE` or `DATETIME`

**Output:**
- `INT` / `BIGINT`

**Examples:**
```sql
-- Extract year
SELECT EXTRACT(YEAR FROM '2025-01-10T12:00:00Z'::TIMESTAMP) AS y;
-- Result: 2025

-- Extract month
SELECT EXTRACT(MONTH FROM '2025-01-10T12:00:00Z'::TIMESTAMP) AS m;
-- Result: 1

-- Extract day
SELECT EXTRACT(DAY FROM '2025-01-10T12:00:00Z'::TIMESTAMP) AS d;
-- Result: 10

-- Extract hour
SELECT EXTRACT(HOUR FROM '2025-01-10T12:34:56Z'::TIMESTAMP) AS h;
-- Result: 12

-- Extract minute
SELECT EXTRACT(MINUTE FROM '2025-01-10T12:34:56Z'::TIMESTAMP) AS min;
-- Result: 34

-- Extract second
SELECT EXTRACT(SECOND FROM '2025-01-10T12:34:56Z'::TIMESTAMP) AS sec;
-- Result: 56

-- Extract quarter
SELECT EXTRACT(QUARTER FROM '2025-05-10'::DATE) AS q;
-- Result: 2

-- Extract week
SELECT EXTRACT(WEEK FROM '2025-01-15'::DATE) AS w;
-- Result: 3
```

---

#### Individual Extraction Functions

**YEAR / QUARTER / MONTH / WEEK / DAY**

**Syntax:**
```sql
YEAR(date_expr)
QUARTER(date_expr)
MONTH(date_expr)
WEEK(date_expr)
DAY(date_expr)
```

**Examples:**
```sql
-- Extract year
SELECT YEAR('2025-01-10'::DATE) AS year;
-- Result: 2025

-- Extract quarter (1-4)
SELECT QUARTER('2025-05-10'::DATE) AS q;
-- Result: 2

-- Extract month (1-12)
SELECT MONTH('2025-01-10'::DATE) AS month;
-- Result: 1

-- Extract ISO week number (1-53)
SELECT WEEK('2025-01-01'::DATE) AS w;
-- Result: 1

-- Extract day of month (1-31)
SELECT DAY('2025-01-10'::DATE) AS day;
-- Result: 10
```

**HOUR / MINUTE / SECOND**

**Syntax:**
```sql
HOUR(timestamp)
MINUTE(timestamp)
SECOND(timestamp)
```

**Examples:**
```sql
-- Extract hour (0-23)
SELECT HOUR('2025-01-10T12:34:56Z'::TIMESTAMP) AS hour;
-- Result: 12

-- Extract minute (0-59)
SELECT MINUTE('2025-01-10T12:34:56Z'::TIMESTAMP) AS minute;
-- Result: 34

-- Extract second (0-59)
SELECT SECOND('2025-01-10T12:34:56Z'::TIMESTAMP) AS second;
-- Result: 56
```

---

#### Sub-Second Extraction Functions

**NANOSECOND / MICROSECOND / MILLISECOND**

Sub-second extraction from timestamps.

**Syntax:**
```sql
NANOSECOND(datetime_expr)
MICROSECOND(datetime_expr)
MILLISECOND(datetime_expr)
```

**Inputs:**
- `datetime_expr` - `DATETIME` or `TIMESTAMP`

**Output:**
- `INT`

**Examples:**
```sql
-- Extract milliseconds
SELECT MILLISECOND('2025-01-01T12:00:00.123Z'::TIMESTAMP) AS ms;
-- Result: 123

-- Extract microseconds
SELECT MICROSECOND('2025-01-01T12:00:00.123456Z'::TIMESTAMP) AS us;
-- Result: 123456

-- Extract nanoseconds
SELECT NANOSECOND('2025-01-01T12:00:00.123456789Z'::TIMESTAMP) AS ns;
-- Result: 123456789
```

---

### Special Date/Time Functions

#### LAST_DAY

Last day of month for a date.

**Syntax:**
```sql
LAST_DAY(date_expr)
```

**Inputs:**
- `date_expr` - `DATE`

**Output:**
- `DATE`

**Examples:**
```sql
-- Last day of February (non-leap year)
SELECT LAST_DAY('2025-02-15'::DATE) AS ld;
-- Result: 2025-02-28

-- Last day of February (leap year)
SELECT LAST_DAY('2024-02-15'::DATE) AS ld;
-- Result: 2024-02-29

-- Last day of January
SELECT LAST_DAY('2025-01-10'::DATE) AS ld;
-- Result: 2025-01-31

-- Last day of current month
SELECT LAST_DAY(CURRENT_DATE) AS month_end;
```

---

#### EPOCHDAY

Days since epoch (1970-01-01).

**Syntax:**
```sql
EPOCHDAY(date_expr)
```

**Inputs:**
- `date_expr` - `DATE`

**Output:**
- `BIGINT`

**Examples:**
```sql
-- Day after epoch
SELECT EPOCHDAY('1970-01-02'::DATE) AS d;
-- Result: 1

-- Epoch day
SELECT EPOCHDAY('1970-01-01'::DATE) AS d;
-- Result: 0

-- Days since epoch for a recent date
SELECT EPOCHDAY('2025-01-10'::DATE) AS d;
-- Result: 20098
```

---

#### OFFSET_SECONDS

Timezone offset in seconds.

**Syntax:**
```sql
OFFSET_SECONDS(timestamp_expr)
```

**Inputs:**
- `timestamp_expr` - `TIMESTAMP` with timezone

**Output:**
- `INT`

**Examples:**
```sql
-- UTC+2 (7200 seconds = 2 hours)
SELECT OFFSET_SECONDS('2025-01-01T12:00:00+02:00'::TIMESTAMP) AS off;
-- Result: 7200

-- UTC (0 seconds)
SELECT OFFSET_SECONDS('2025-01-01T12:00:00Z'::TIMESTAMP) AS off;
-- Result: 0

-- UTC-5 (-18000 seconds = -5 hours)
SELECT OFFSET_SECONDS('2025-01-01T12:00:00-05:00'::TIMESTAMP) AS off;
-- Result: -18000
```

---

### Supported MySQL-style Date/Time Patterns

The following patterns are supported in `DATE_FORMAT`, `DATE_PARSE`, `DATETIME_FORMAT`, and `DATETIME_PARSE` functions:

| Patternc | Descriptionc                         | Example Outputc |
|----------|--------------------------------------|-----------------|
| `%Y`     | Year (4 digits)                      | `2025`          |
| `%y`     | Year (2 digits)                      | `25`            |
| `%m`     | Month (2 digits, 01-12)              | `01`            |
| `%c`     | Month (1-12, no leading zero)        | `1`             |
| `%M`     | Month name (full)                    | `January`       |
| `%b`     | Month name (abbreviated)             | `Jan`           |
| `%d`     | Day of month (2 digits, 01-31)       | `10`            |
| `%e`     | Day of month (1-31, no leading zero) | `9`             |
| `%W`     | Weekday name (full)                  | `Friday`        |
| `%a`     | Weekday name (abbreviated)           | `Fri`           |
| `%H`     | Hour (00-23, 24-hour format)         | `13`            |
| `%h`     | Hour (01-12, 12-hour format)         | `01`            |
| `%I`     | Hour (01-12, synonym for %h)         | `01`            |
| `%i`     | Minutes (00-59)                      | `45`            |
| `%s`     | Seconds (00-59)                      | `30`            |
| `%f`     | Fractional seconds, any precision    | `123456`        |
| `%p`     | AM/PM marker                         | `AM` / `PM`     |

**Pattern Combination Examples:**

```sql
-- Full date and time
'%Y-%m-%d %H:%i:%s'          -- 2025-01-10 13:45:30

-- US format with 12-hour time
'%m/%d/%Y %h:%i %p'           -- 01/10/2025 01:45 PM

-- Long format with names
'%W, %M %d, %Y'               -- Friday, January 10, 2025

-- ISO 8601 with microseconds
'%Y-%m-%dT%H:%i:%s.%f'        -- 2025-01-10T13:45:30.123456
```

> **`%f` is variable width.** It formats a value at its actual precision — `.123456` for
> microseconds, `.123` for milliseconds — and when parsing it accepts any number of fractional
> digits, or none at all. A decimal point written immediately before it belongs to the fraction, so
> a value with no fractional part formats as `12:00:00` rather than `12:00:00.`.

```

-- Short format
'%d-%b-%y'                    -- 10-Jan-25

-- Time only (24-hour)
'%H:%i:%s'                    -- 13:45:30

-- Time only (12-hour)
'%h:%i:%s %p'                 -- 01:45:30 PM

-- European format
'%d/%m/%Y'                    -- 10/01/2025

-- Year and month
'%Y-%m'                       -- 2025-01

-- Month and day with names
'%b %d'                       -- Jan 10
```

[Back to index](README.md)
