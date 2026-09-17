[Back to index](README.md)

# Keywords

The words the parser recognises. Two different sets live on this page, and the difference matters when
you name a column:

- **Recognised** — the word has a meaning in the grammar. Everything listed below is recognised.
- **Reserved** — the word additionally **cannot be used as a bare identifier**. A minority of the words
  below are reserved; the table says which, word by word. (Don't count them by hand — the generated
  heading above the table gives both numbers.)

`EXISTS` is reserved, so `SELECT exists FROM t` is a parse error. `ANY` and `SOME` are **deliberately not
reserved**, so `SELECT any, some FROM t WHERE any = 1` parses as columns — even though `x = ANY (SELECT …)`
is real grammar. If you have a column whose name collides with a reserved word, **quote it** rather than
renaming it: `SELECT "exists" FROM t` works, and so does the backtick spelling — see
[Quoted identifiers](dql_statements.md#quoted-identifiers).

## How to read the table

This page is **generated** from the engine itself — `SQLKeywords` supplies the words, and the parser's
own reserved-word list supplies the `Reserved` column — so it cannot drift from what the parser accepts.
Do not edit the tables by hand; run `sbt regenerateKeywordsPage` and commit the result. The prose on this
page *is* hand-written and is preserved by regeneration.

- **Keyword** — one accepted spelling, alphabetically. Aliases get a row of their own: `UCASE`,
  `CEILING` and `SAFE_CAST` are each listed, because the parser accepts them just as it accepts
  `UPPER`, `CEIL` and `TRY_CAST`.
- **Reserved** — three values, because two are not enough.
  - `yes` — a bare occurrence of the word is **never** read as a column of that name. Usually the
    statement is rejected outright; for a few (`NOW`, `PI`, `TRUE`, `CURRENT_DATE`, …) it parses as the
    keyword instead. Either way, quote it. The check is case-insensitive and applies to the **first**
    part of a dotted name, which is why `t.from` and `doc.count` parse. It also catches a bare name
    that merely *starts* with the word followed by `-`: `in-stock` and `all-time` are rejected, while
    `foo-bar` and `instock` are fine. Quote those too.
  - `no` — a bare occurrence *is* the column. Nothing to do.
  - `no (shadowed)` — the word is not reserved, and a bare occurrence still isn't your column: a
    zero-argument function or a literal takes precedence, **silently**. `SELECT curdate FROM t` returns
    the current date, not your `curdate` column. Quote it. This value is measured by running the word
    through the parser, not inferred from a list.
- **Kind** — which surface of the engine the word arrives from, not a taxonomy of what it does: it is
  the registry list the word is declared in, refined by the package it is declared in. That is why
  `UNION` reads `operator` while `EXCEPT` reads `clause`, and why `CASE`/`WHEN`/`OVER` read `clause`
  rather than naming a function family. A word can have more than one kind — `LIMIT` is a clause
  keyword *and* a statement keyword. `reserved only` marks a word that is reserved but backed by no
  grammar surface at this baseline: reserving it costs you the name and buys nothing.

Multi-word constructs carried by a single keyword (`ORDER BY`, `IS NOT NULL`, `UNION ALL`, `LEFT OUTER`)
are listed under [Compound phrases](#compound-phrases); each of their component words also has its own
row. Constructions assembled from two separate keywords are listed just below.

## Multi-keyword constructions

These are real syntax, but each is built from two keywords that the grammar carries independently, so
they cannot appear in the generated tables — their component words do, individually.

- `NOT IN`, `NOT BETWEEN`, `NOT LIKE` — negated forms of the corresponding operator.
- `MATCH ... AGAINST` — full-text match, e.g. `WHERE MATCH(title) AGAINST ('coffee')`.
- `WITHIN GROUP` — the ordered-set aggregate clause, e.g.
  `PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount)`.

## What this page does not list

The tables cover *word* keywords and the one symbolic operator that survived into the grammar. They
deliberately do not cover: the comparison and arithmetic symbols (`=`, `<>`, `+`, …); the parameter
placeholder `?`; the metadata pseudo-columns `_id` and `_ingest.timestamp`; and the geo distance unit
codes (`km`, `mi`, `m`, `cm`, `mm`, `yd`, `ft`, `nmi`) accepted inside `ST_DISTANCE` comparisons. The
string-concatenation token `||` is also absent, deliberately: it exists in the lexer but no production
accepts it, so `SELECT a || b FROM t` is a parse error — use `CONCAT(a, b)`.

## Subquery predicates

`EXISTS` and `ALL` are **reserved** — a column of either name must be quoted. `ANY` and `SOME` are
recognised but **not reserved**, on purpose: a column called `any` keeps parsing, and the grammar tells the
two readings apart by what follows.

These five words introduce the subquery predicates. `= ANY` and `= SOME` mean `IN`, and `<> ALL` means
`NOT IN` — the engine normalises them, so `WHERE customer_id = ANY (SELECT id FROM customers)` is stored
and re-rendered as `WHERE customer_id IN (SELECT id FROM customers)`. The ordering quantifiers
(`> ALL`, `>= ANY`, `< ALL`, …) keep their own spelling. See
[Subqueries and derived tables](known_limitations.md#subqueries-and-derived-tables).

<!-- BEGIN GENERATED KEYWORDS -->
<!--
  GENERATED - DO NOT EDIT THIS SECTION BY HAND.
  Regenerate with: sbt regenerateKeywordsPage
  Source: sql/src/test/scala/app/softnetwork/elastic/sql/doc/KeywordsPage.scala,
  derived from SQLKeywords (words), Parser.reservedKeywords (reserved flag) and
  the parser itself (shadowed flag).
  Text OUTSIDE the two markers is hand-written and is preserved by regeneration.
-->

## Keyword index

332 keywords, of which 134 are reserved.

| Keyword | Reserved | Kind |
| --- | --- | --- |
| `ABS` | yes | math function |
| `ACOS` | yes | math function |
| `ADD` | no | statement |
| `AGAINST` | yes | operator |
| `ALIAS` | no | statement |
| `ALL` | yes | operator |
| `ALTER` | yes | statement |
| `ALWAYS` | yes | statement |
| `AND` | yes | operator |
| `ANY` | no | operator |
| `ARRAY` | no | aggregate function, type |
| `ARRAY_AGG` | yes | aggregate function |
| `AS` | yes | clause, statement |
| `ASC` | no | clause |
| `ASIN` | yes | math function |
| `AT` | yes | statement |
| `ATAN` | yes | math function |
| `ATAN2` | yes | math function |
| `AVG` | yes | aggregate function |
| `BETWEEN` | yes | operator |
| `BIGINT` | no | type |
| `BINARY` | no | type |
| `BOOLEAN` | no | type |
| `BY` | yes | clause, statement |
| `BYTE` | no | type |
| `CACHE` | no | statement |
| `CASE` | yes | clause |
| `CAST` | yes | conversion function |
| `CEIL` | yes | math function |
| `CEILING` | no | math function |
| `CHAR` | no | type |
| `CHILD` | no | operator |
| `CLUSTER` | no | statement |
| `COALESCE` | yes | conditional function |
| `COLUMN` | yes | statement |
| `COMMENT` | no | statement |
| `CONCAT` | yes | string function |
| `CONFLICT` | yes | statement |
| `CONVERT` | no | conversion function |
| `COPY` | yes | statement |
| `COS` | yes | math function |
| `COUNT` | yes | aggregate function |
| `CREATE` | yes | statement |
| `CROSS` | yes | clause |
| `CURDATE` | no (shadowed) | temporal function |
| `CURRENT_DATE` | yes | temporal function |
| `CURRENT_DATETIME` | yes | reserved only |
| `CURRENT_TIME` | yes | temporal function |
| `CURRENT_TIMESTAMP` | yes | temporal function |
| `CURTIME` | no (shadowed) | temporal function |
| `DATA` | no | statement |
| `DATE` | no | type |
| `DATEADD` | no | temporal function |
| `DATEDIFF` | no | temporal function |
| `DATEFORMAT` | no | temporal function |
| `DATEPARSE` | no | temporal function |
| `DATESUB` | no | temporal function |
| `DATETIME` | no | type |
| `DATETIMEADD` | no | temporal function |
| `DATETIMEPARSE` | no | temporal function |
| `DATETIMESUB` | no | temporal function |
| `DATETIME_ADD` | yes | temporal function |
| `DATETIME_FORMAT` | no | temporal function |
| `DATETIME_PARSE` | no | temporal function |
| `DATETIME_SUB` | yes | temporal function |
| `DATETRUNC` | no | temporal function |
| `DATE_ADD` | yes | temporal function |
| `DATE_DIFF` | yes | temporal function |
| `DATE_FORMAT` | no | temporal function |
| `DATE_PARSE` | no | temporal function |
| `DATE_SUB` | yes | temporal function |
| `DATE_TRUNC` | yes | temporal function |
| `DAY` | no | statement, temporal function |
| `DAYOFMONTH` | no | temporal function |
| `DAYOFWEEK` | no | temporal function |
| `DAYOFYEAR` | no | temporal function |
| `DAYS` | no | time unit |
| `DEC` | no | type |
| `DECIMAL` | no | type |
| `DEFAULT` | no | statement |
| `DEGREES` | no | math function |
| `DELETE` | yes | statement |
| `DELTA_LAKE` | no | statement |
| `DENSE_RANK` | yes | aggregate function |
| `DESC` | no | clause, statement |
| `DESCRIBE` | yes | statement |
| `DISTANCE` | yes | geo function |
| `DISTINCT` | yes | clause |
| `DO` | yes | statement |
| `DOUBLE` | no | type |
| `DROP` | yes | statement |
| `E` | no | literal |
| `ELSE` | yes | clause |
| `END` | yes | clause, statement |
| `ENRICH` | no | statement |
| `EPOCHDAY` | no | temporal function |
| `EPOCH_DAY` | no | temporal function |
| `EVERY` | yes | statement |
| `EXCEPT` | yes | clause |
| `EXECUTE` | no | statement |
| `EXISTS` | yes | operator, statement |
| `EXP` | yes | math function |
| `EXTRACT` | yes | temporal function |
| `FALSE` | yes | literal |
| `FIELD` | no | statement |
| `FIELDS` | no | statement |
| `FILE_FORMAT` | no | statement |
| `FIRST` | yes | aggregate function, clause |
| `FIRST_VALUE` | yes | aggregate function |
| `FLOAT` | no | type |
| `FLOOR` | yes | math function |
| `FOR` | no | clause |
| `FOREACH` | yes | statement |
| `FORMAT` | no | temporal function |
| `FORMAT_DATE` | yes | reserved only |
| `FORMAT_DATETIME` | yes | reserved only |
| `FROM` | yes | clause, statement |
| `FULL` | yes | clause |
| `GEOPOINT` | no | type |
| `GEO_MATCH` | no | statement |
| `GEO_POINT` | no | type |
| `GLOBAL` | no | statement |
| `GREATEST` | yes | conditional function |
| `GROUP` | yes | clause |
| `HAVING` | yes | clause |
| `HOUR` | no | statement, temporal function |
| `HOUROFDAY` | no | temporal function |
| `HOURS` | no | time unit |
| `HOUR_OF_DAY` | no | temporal function |
| `IF` | no | statement |
| `IN` | yes | operator |
| `INDEX` | no | statement |
| `INNER` | yes | clause |
| `INPUT` | no | statement |
| `INPUTS` | no | statement |
| `INSERT` | yes | statement |
| `INT` | no | type |
| `INTEGER` | no | type |
| `INTERVAL` | yes | clause |
| `INTO` | no | statement |
| `IS` | no | operator |
| `ISNOTNULL` | yes | conditional function |
| `ISNULL` | yes | conditional function |
| `JOIN` | yes | clause |
| `JSON` | no | statement |
| `JSON_ARRAY` | no | statement |
| `KEY` | no | statement |
| `KEYWORD` | no | type |
| `LANG` | no | statement |
| `LAST` | yes | aggregate function, clause |
| `LASTDAY` | no | temporal function |
| `LAST_DAY` | no | temporal function |
| `LAST_VALUE` | yes | aggregate function |
| `LCASE` | no | string function |
| `LEAST` | yes | conditional function |
| `LEFT` | yes | clause, string function |
| `LEN` | no | string function |
| `LENGTH` | yes | string function |
| `LICENSE` | no | statement |
| `LIKE` | yes | operator, statement |
| `LIMIT` | yes | clause, statement |
| `LOCAL` | no | statement |
| `LOCATE` | no | string function |
| `LOG` | yes | math function, statement |
| `LOG10` | yes | math function |
| `LONG` | no | type |
| `LOWER` | yes | string function |
| `LTRIM` | yes | string function |
| `MAPPING` | no | statement |
| `MATCH` | yes | operator, statement |
| `MATERIALIZED` | no | statement |
| `MAX` | yes | aggregate function |
| `MICROOFSECOND` | no | temporal function |
| `MICROSECOND` | no | temporal function |
| `MICRO_OF_SECOND` | no | temporal function |
| `MILLIOFSECOND` | no | temporal function |
| `MILLISECOND` | no | temporal function |
| `MILLI_OF_SECOND` | no | temporal function |
| `MIN` | yes | aggregate function |
| `MINUTE` | no | statement, temporal function |
| `MINUTEOFHOUR` | no | temporal function |
| `MINUTES` | no | time unit |
| `MINUTE_OF_HOUR` | no | temporal function |
| `MONTH` | no | statement, temporal function |
| `MONTHOFYEAR` | no | temporal function |
| `MONTHS` | no | time unit |
| `MONTH_OF_YEAR` | no | temporal function |
| `NAME` | no | statement |
| `NANOOFSECOND` | no | temporal function |
| `NANOSECOND` | no | temporal function |
| `NANO_OF_SECOND` | no | temporal function |
| `NESTED` | no | operator |
| `NEVER` | yes | statement |
| `NOT` | yes | operator, statement |
| `NOTHING` | no | statement |
| `NOW` | yes | statement, temporal function |
| `NULL` | no (shadowed) | literal, operator, statement |
| `NULLIF` | yes | conditional function |
| `NULLS` | no | clause |
| `NUMERIC` | no | type |
| `OFFSET` | yes | clause |
| `OFFSETSECONDS` | no | temporal function |
| `OFFSET_SECONDS` | no | temporal function |
| `ON` | yes | clause, statement |
| `OPTION` | no | statement |
| `OPTIONS` | no | statement |
| `OR` | yes | operator, statement |
| `ORDER` | yes | clause |
| `OUTER` | yes | clause |
| `OVER` | no | clause |
| `PARAMS` | no | statement |
| `PARENT` | no | operator |
| `PARQUET` | no | statement |
| `PARSE_DATE` | yes | temporal function |
| `PARSE_DATETIME` | yes | temporal function |
| `PARTITION` | no | clause, statement |
| `PERCENTILE_CONT` | yes | aggregate function |
| `PERCENTILE_DISC` | yes | aggregate function |
| `PI` | yes | literal |
| `PIPELINE` | no | statement |
| `PIPELINES` | no | statement |
| `POINT` | no | geo function |
| `POLICIES` | no | statement |
| `POLICY` | no | statement |
| `POSITION` | no | string function |
| `POW` | yes | math function |
| `POWER` | no | math function |
| `PRIMARY` | no | statement |
| `PROCESSOR` | no | statement |
| `PROCESSORS` | no | statement |
| `QUARTER` | no | temporal function |
| `QUARTEROFYEAR` | no | temporal function |
| `QUARTERS` | no | time unit |
| `QUARTER_OF_YEAR` | no | temporal function |
| `RADIANS` | no | math function |
| `RANDOM` | no (shadowed) | literal |
| `RANGE` | no | statement |
| `RANK` | yes | aggregate function |
| `REAL` | no | type |
| `RECURSIVE` | no | statement |
| `REFRESH` | no | statement |
| `REGEXP` | no | string function |
| `REGEXP_LIKE` | no | string function |
| `RENAME` | no | statement |
| `REPLACE` | yes | statement, string function |
| `RETURNS` | no | statement |
| `REVERSE` | no | string function |
| `RIGHT` | yes | clause, string function |
| `RLIKE` | no | operator |
| `ROUND` | yes | math function |
| `ROW_NUMBER` | yes | aggregate function |
| `RTRIM` | yes | string function |
| `SAFE_CAST` | no | conversion function |
| `SCHEDULE` | no | statement |
| `SCHEMA` | no | statement |
| `SCRIPT` | no | statement |
| `SECOND` | no | statement, temporal function |
| `SECONDOFMINUTE` | no | temporal function |
| `SECONDS` | no | time unit |
| `SECOND_OF_MINUTE` | no | temporal function |
| `SELECT` | yes | clause |
| `SET` | no | statement |
| `SETTING` | no | statement |
| `SHORT` | no | type |
| `SHOW` | yes | statement |
| `SIGN` | yes | math function |
| `SIGNED` | no | type |
| `SIN` | yes | math function |
| `SMALLINT` | no | type |
| `SOME` | no | operator |
| `SQRT` | yes | math function |
| `STATUS` | no | statement |
| `STDDEV` | yes | aggregate function |
| `STDDEV_POP` | yes | aggregate function |
| `STDDEV_SAMP` | yes | aggregate function |
| `STORED` | no | statement |
| `STRING` | no | type |
| `STRPOS` | no | string function |
| `STRUCT` | no | type |
| `STR_REPLACE` | no | string function |
| `ST_DISTANCE` | no | geo function |
| `SUBSTR` | yes | string function |
| `SUBSTRING` | yes | string function |
| `SUM` | yes | aggregate function |
| `TABLE` | no | statement |
| `TABLES` | no | statement |
| `TAN` | yes | math function |
| `TEMPORARY` | no | statement |
| `TEXT` | no | type |
| `THEN` | yes | clause |
| `TIME` | no | type |
| `TIMESTAMP` | no | type |
| `TINYINT` | no | type |
| `TO` | yes | statement |
| `TODAY` | yes | temporal function |
| `TO_DATE` | no | temporal function |
| `TO_TIMESTAMP` | no | temporal function |
| `TRIM` | yes | string function |
| `TRUE` | yes | literal, statement |
| `TRUNCATE` | yes | statement |
| `TRY_CAST` | no | conversion function |
| `TTL` | no | statement |
| `TYPE` | no | statement |
| `UCASE` | no | string function |
| `UNION` | yes | operator |
| `UNNEST` | yes | clause |
| `UNSIGNED` | no | type |
| `UPDATE` | yes | statement |
| `UPPER` | yes | string function |
| `USING` | no | statement |
| `VALUES` | no | statement |
| `VARBINARY` | no | type |
| `VARCHAR` | no | type |
| `VARIANCE` | yes | aggregate function |
| `VAR_POP` | yes | aggregate function |
| `VAR_SAMP` | yes | aggregate function |
| `VIEW` | no | statement |
| `VIEWS` | no | statement |
| `WATCHER` | no | statement |
| `WATCHERS` | no | statement |
| `WEBHOOK` | no | statement |
| `WEEK` | no | temporal function |
| `WEEKDAY` | no | temporal function |
| `WEEKOFYEAR` | no | temporal function |
| `WEEKS` | no | time unit |
| `WHEN` | yes | clause, statement |
| `WHERE` | yes | clause |
| `WITH` | no | statement |
| `WITHIN` | yes | statement |
| `YEAR` | no | statement, temporal function |
| `YEARDAY` | no | temporal function |
| `YEARS` | no | time unit |

## Compound phrases

11 multi-word phrases carried by a single keyword token. Each component word has its own row above. Constructions assembled from two separate keywords are listed under [Multi-keyword constructions](#multi-keyword-constructions).

| Phrase |
| --- |
| `FULL OUTER` |
| `GROUP BY` |
| `IS NOT NULL` |
| `IS NULL` |
| `LEFT OUTER` |
| `NULLS FIRST` |
| `NULLS LAST` |
| `ORDER BY` |
| `PARTITION BY` |
| `RIGHT OUTER` |
| `UNION ALL` |

## Symbolic operators

| Operator | Meaning |
| --- | --- |
| `::` | cast operator, e.g. `'125'::BIGINT` |

<!-- END GENERATED KEYWORDS -->

[Back to index](README.md)
