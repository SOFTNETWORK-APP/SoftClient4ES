[Back to index](README.md)

# Keywords

The words the parser recognises. Two different sets live on this page, and the difference matters when
you name a column:

- **Recognised** — the word has a meaning in the grammar. Everything listed below is recognised.
- **Reserved** — the word additionally **cannot be used as a bare identifier**. Most, but *not all*, of
  the words below are reserved.

`EXISTS` is reserved, so `SELECT exists FROM t` is a parse error. `ANY` and `SOME` are **deliberately not
reserved**, so `SELECT any, some FROM t WHERE any = 1` parses as columns — even though `x = ANY (SELECT …)`
is real grammar. If you have a column whose name collides with a reserved word, **quote it** rather than
renaming it: `SELECT "exists" FROM t` works, and so does the backtick spelling — see
[Quoted identifiers](dql_statements.md#quoted-identifiers).

## Main clauses
COPY
SELECT  
INSERT  
UPDATE  
DELETE  
CREATE  
ALTER  
DROP  
TRUNCATE  
FROM  
JOIN  
UNNEST  
WHERE  
GROUP BY  
HAVING  
ORDER BY  
NULLS FIRST  
NULLS LAST  
OFFSET  
LIMIT
ON
CONFLICT
DO
UNION ALL
SHOW
DESCRIBE
EVERY
AT
NEVER
ALWAYS
FOREACH
WITHIN

## Aliases and type conversion
AS  
CAST  
CONVERT  
TRY_CAST  
SAFE_CAST  
::  

## Aggregates
COUNT  
DISTINCT  
SUM  
AVG  
MIN  
MAX  
OVER  
PARTITION BY  
FIRST_VALUE  
LAST_VALUE  
ARRAY_AGG  
STDDEV  
STDDEV_POP  
STDDEV_SAMP  
VARIANCE  
VAR_POP  
VAR_SAMP  
PERCENTILE_CONT  
PERCENTILE_DISC  
WITHIN GROUP  
ROW_NUMBER  
RANK  
DENSE_RANK

## String functions
UPPER  
UCASE  
LOWER  
LCASE  
TRIM  
LTRIM  
RTRIM  
LENGTH  
SUBSTRING  
SUBSTR  
CONCAT  
POSITION  
REGEXP_LIKE  
REGEXP  
MATCH ... AGAINST  
REPLACE  
REVERSE

## Math functions
ABS  
ROUND  
FLOOR  
CEIL  
CEILING  
POWER  
POW  
SQRT  
LOG  
LOG10  
EXP  
SIGN  
COS  
ACOS  
SIN  
ASIN  
TAN  
ATAN  
ATAN2

## Conditional functions
CASE  
WHEN  
THEN  
ELSE  
END  
COALESCE  
ISNULL  
ISNOTNULL  
NULLIF  
GREATEST  
LEAST

## Date/Time/Datetime/Timestamp functions
[//]: # (YEAR  )
[//]: # (QUARTER  )
[//]: # (MONTH  )
[//]: # (WEEK  )
[//]: # (DAY  )
[//]: # (HOUR  )
[//]: # (MINUTE  )
[//]: # (SECOND  )
[//]: # (MILLISECOND  )
[//]: # (MICROSECOND  )
[//]: # (NANOSECOND  )
[//]: # (EPOCHDAY  )
[//]: # (OFFSET_SECONDS  )
[//]: # (LAST_DAY  )
[//]: # (LASTDAY)
[//]: # (WEEKDAY  )
[//]: # (YEARDAY  )
INTERVAL  
CURRENT_DATE  
CURDATE  
TODAY  
NOW  
CURRENT_TIME  
CURTIME  
CURRENT_DATETIME  
CURRENT_TIMESTAMP  
DATE_ADD  
DATEADD  
DATE_SUB  
DATESUB  
DATETIME_ADD  
DATETIMEADD  
DATETIME_SUB  
DATETIMESUB  
DATE_DIFF  
DATEDIFF  
DATE_FORMAT  
DATE_PARSE  
DATETIME_FORMAT  
DATETIME_PARSE  
DATE_TRUNC  
EXTRACT  

## Geo functions
POINT  
ST_DISTANCE  
DISTANCE  

## Conditional operators
LIKE  
RLIKE  
IN  
BETWEEN  
NOT IN  
NOT BETWEEN  
IS NULL  
IS NOT NULL  
EXISTS  
NOT EXISTS  
ALL  
ANY  
SOME  

`EXISTS` and `ALL` are **reserved** — a column of either name must be quoted. `ANY` and `SOME` are
recognised but **not reserved**, on purpose: a column called `any` keeps parsing, and the grammar tells the
two readings apart by what follows.

These five words introduce the subquery predicates. `= ANY` and `= SOME` mean `IN`, and `<> ALL` means
`NOT IN` — the engine normalises them, so `WHERE customer_id = ANY (SELECT id FROM customers)` is stored
and re-rendered as `WHERE customer_id IN (SELECT id FROM customers)`. The ordering quantifiers
(`> ALL`, `>= ANY`, `< ALL`, …) keep their own spelling. See
[Subqueries and derived tables](known_limitations.md#subqueries-and-derived-tables).

## Logical operators
AND  
OR  
NOT  

[Back to index](README.md)
