[Back to index](README.md)

# 📘 DQL Statements — SQL Gateway for Elasticsearch

## Introduction

The SQL Gateway provides a Data Query Language (DQL) on top of Elasticsearch, centered around the `SELECT` statement.  
It offers a familiar SQL experience while translating queries into Elasticsearch search, aggregations, and scroll APIs.

DQL supports:

- `SELECT` with expressions, aliases, nested fields, STRUCT and ARRAY<STRUCT>
- `WHERE`, `GROUP BY`, `HAVING`, `ORDER BY`, `LIMIT`, `OFFSET`
- `UNION ALL`
- cross-index JOINs (`INNER` / `LEFT` / `RIGHT` / `FULL OUTER`) across indices and clusters — see [Cross-Index JOIN](joins.md)
- `JOIN UNNEST` on `ARRAY<STRUCT>` (the single-index nested form, handled natively inside one index)
- aggregations, parent-level aggregations on nested arrays
- window functions with `OVER`
- rich function support (numeric, string, date/time, geo, conditional, type conversion)

---

## Table of Contents

- [SELECT](#select)
- [Quoted identifiers](#quoted-identifiers)
- [Qualified and quoted table names](#qualified-and-quoted-table-names)
- [FROM-less SELECT (connection handshake)](#from-less-select-connection-handshake)
- [WHERE](#where)
- [ORDER BY](#order-by)
- [LIMIT / OFFSET](#limit--offset)
- [UNION ALL](#union-all)
- [JOIN UNNEST](#join-unnest)
- [Aggregations](#aggregations)
- [Parent-Level Aggregations on Nested Arrays](#parent-level-aggregations-on-nested-arrays)
- [Window Functions](#window-functions)
- [Functions](#functions)
- [Scroll & Pagination](#scroll--pagination)
- [Version Compatibility](#version-compatibility)
- [Limitations](#limitations)
- [SHOW TABLES](#show-tables)
- [SHOW TABLE](#show-table)
- [SHOW CREATE TABLE](#show-create-table)
- [DESCRIBE TABLE](#describe-table)
- [SHOW PIPELINES](#show-pipelines)
- [SHOW PIPELINE](#show-pipeline)
- [SHOW CREATE PIPELINE](#show-create-pipeline)
- [DESCRIBE PIPELINE](#describe-pipeline)
- [SHOW WATCHERS](#show-watchers)
- [SHOW WATCHER STATUS](#show-watcher-status)
- [SHOW ENRICH POLICIES](#show-enrich-policies)
- [SHOW ENRICH POLICY](#show-enrich-policy)
- [SHOW CLUSTER NAME](#show-cluster-name)
- [SHOW LICENSE](#show-license)
- [REFRESH LICENSE](#refresh-license)

---

## SELECT

#### Basic syntax

```sql
SELECT [DISTINCT] expr1, expr2, ...
FROM table_name [alias]
[WHERE condition]
[GROUP BY expr1, expr2, ...]
[HAVING condition]
[ORDER BY expr1 [ASC|DESC] [NULLS FIRST|NULLS LAST], ...]
[LIMIT n]
[OFFSET m];
```

#### Nested fields and aliases

```sql
SELECT id,
       name AS full_name,
       profile.city AS city,
       profile.followers AS followers
FROM dql_users
ORDER BY id ASC;
```

- `profile` is a `STRUCT` column.
- `profile.city` and `profile.followers` access nested fields.
- Aliases (`AS full_name`, `AS city`) are returned as column names.

---

## Quoted identifiers

A column name or an alias may be written **quoted**, in either of two spellings — the ANSI SQL-92
double quote or the MySQL backtick. Both are accepted everywhere an identifier is accepted, and both
denote the same column.

```sql
SELECT `category`, COUNT(id) AS `n`
FROM bi_events
WHERE "category" IS NOT NULL
GROUP BY `category`
ORDER BY `category` ASC;
```

Quoting is what lets a name be used that the bare spelling cannot express:

| Written | Meaning |
| ------- | ------- |
| `` SELECT `select` `` | a column literally named `select` — a quoted name bypasses the reserved-word rule |
| `` SELECT `my col` `` | a column whose name contains a space |
| `` SELECT `Category` `` | case is preserved **verbatim**; Elasticsearch field names are case-sensitive |
| `` SELECT `1` `` | the column *named* `1`, never the first column by position |
| `` SELECT `e`.`category` FROM bi_events e `` | each part of a qualified name may be quoted independently — `` e.`category` `` and `` `e`.category `` are the same thing |

#### Escaping

The delimiter is escaped by **doubling** it, in both spellings:

```sql
SELECT `a``b`,  -- the column named  a`b
       "a""b"   -- the column named  a"b
FROM t;
```

Inside a **double-quoted** name a backslash also escapes the next character (`"a\"b"` is the column
`a"b`). That form is not standard and is kept only because this engine has always accepted it; it is
deliberately **not** available inside backticks, where a backslash is an ordinary character.

An empty pair of double quotes (`""`) is **not** an identifier — it is the empty string literal, the
same as `''`. An empty pair of backticks is rejected.

#### How a quoted name is rendered back

Statements are re-rendered with **one** canonical delimiter, the ANSI double quote, whichever
spelling was written. A rendered statement always re-parses to the same query, so
`` SELECT `category` `` comes back as `SELECT "category"`. Only names that were *written* quoted are
re-emitted quoted; a bare name stays bare.

#### Double quotes are also string delimiters, in value position

This engine accepts a double-quoted string literal, so `"x"` is read as a **string** wherever a
value is expected and as a **column** wherever a name is expected:

```sql
SELECT MAX("amount")            -- column  amount
FROM   bi_events
WHERE  category = "premium";    -- string  'premium'
```

Use single quotes for strings and backticks for names if you would rather not rely on position.

---

## String literals

A string literal is written between **single quotes**. Double quotes also delimit a string, but only
in value position — see *Double quotes are also string delimiters* above — so single quotes are the
unambiguous spelling.

### Escaping the quote

The delimiter is escaped by **doubling** it, exactly as it is inside a quoted identifier. This is the
SQL standard and what every client and BI tool emits:

```sql
SELECT 'O''Brien' AS name FROM t;                 -- the value  O'Brien
SELECT id FROM t WHERE greeting = "say ""hi""";   -- the value  say "hi"
```

⚠️ The second example is in a **value** position. In a SELECT list the same lexeme is a **column**:
`SELECT "say ""hi""" FROM t` reads the field *named* `say "hi"`, per *Double quotes are also string
delimiters* above — and a reference to a field that does not exist returns nulls, not an error.
Single quotes have only one reading and are the safe spelling for a string.

A backslash before the delimiter or before another backslash is also accepted (`'it\'s'`, `'C:\\'`).
That form is not standard and is kept only because this engine has always accepted it. Any other
backslash sequence is **literal**: `'a\nb'` is the four characters `a`, `\`, `n`, `b` — there is no
`\n` newline escape, and a path such as `'C:\logs'` keeps its separator.

A value that ends in a single backslash must be written `'C:\\'`: a lone trailing backslash escapes
the closing quote and the literal never terminates.

### How a string literal is rendered back

Statements are re-rendered with the **backslash** form, whichever spelling was written, so
`SELECT 'O''Brien'` comes back as `SELECT 'O\'Brien'`. Both spellings parse, and a rendered
statement always re-parses to the same query.

---

## Qualified and quoted table names

The name after `FROM` (and after `JOIN`, and after `DELETE FROM`) may be written quoted, in either
spelling, and may carry a qualifier:

```sql
SELECT category FROM `bi_events`;
SELECT category FROM "bi_events";
SELECT category FROM `elastic`.`bi_events` `bi_events`;
SELECT category FROM "elastic"."bi_events" AS e;
SELECT o.id FROM `elastic`.`orders` o JOIN `elastic`.`customers` c ON o.cid = c.id;
```

### Quoting is what makes a dot a qualifier

Elasticsearch index names may themselves contain dots (`logs-2025.03`), so a bare dot can never be
a separator. **A qualifier is a leading run of parts that are each quoted AND each followed by a
dot; the index name is everything after that run.** Nothing else separates the two readings.

| Written | Index read | Qualifier |
| ------- | ---------- | --------- |
| `FROM bi_events` | `bi_events` | — |
| `FROM elastic.bi_events` | `elastic.bi_events` | — (a bare dot is part of the name) |
| `FROM logs-2025.03` | `logs-2025.03` | — |
| `FROM "elastic".bi_events` | `bi_events` | `elastic` |
| ``FROM `elastic`.bi_events`` | `bi_events` | `elastic` |
| `FROM "elastic"."bi_events"` | `bi_events` | `elastic` |
| `FROM "logs-2025.03"` | `logs-2025.03` | — (the dot is *inside* the quotes) |
| `FROM "elasticsearch"."prod-cluster"."bi_events"` | `bi_events` | `elasticsearch`, `prod-cluster` |

The rule is the same in `FROM`, in every `JOIN` form and in `DELETE FROM`, so one statement can
never read one index on one leg and a different one on another.

> ⚠️ **The two quote styles are interchangeable to the engine, but not yet on the federation
> path.** Federation recognises a cross-cluster prefix by matching ``` `catalog`.table ``` on the
> raw SQL before parsing — a BACKTICKED qualifier followed by a BARE table name. A fully quoted
> ``FROM `prod_us`.`orders` `` is not matched, so the leg is forwarded to the default cluster
> rather than the one named. Until that is fixed, leave the table name itself unquoted when you
> qualify it for Federation. See [known limitations](known_limitations.md).

### What the qualifier means

**The engine records it and does not interpret it.** The qualifier never becomes part of the index
name and is never resolved by the parser: what a leading qualifier *means* depends on who is
asking. The JDBC driver and the Flight SQL producer advertise it as the **schema** (the cluster
name; the catalog is the constant `elasticsearch`); Federation reads it as a **catalog** — a
`servers.<name>` alias, as in [joins.md](joins.md). A BI tool simply echoes back whatever schema
our own driver advertised, which is why a qualifier that names nothing in particular is accepted
rather than rejected.

Practically: send the qualifier your tool generates, and the engine will read the index you meant.

### What changed for a JOIN leg

Before this release a `JOIN` source went through the column-name rules, which JOIN the parts of a
dotted name — so `JOIN "prod_us".customers c` read the index `prod_us.customers` while the `FROM`
leg of the same statement dropped its qualifier. Both legs now follow the table rule and read
`customers`. **If you were relying on a quoted `JOIN` qualifier becoming part of the index name,
that statement now reads a different index** — write the dotted name unquoted
(`JOIN prod_us.customers c`) to keep the old reading.

### It is preserved when the statement is rendered back

Qualifiers used to be dropped from the re-rendered SQL. They are not any more — a rendered
statement carries the qualifier the original had, canonicalised to the ANSI double quote:

```sql
SELECT category FROM `elastic`.`bi_events`
-- renders as
SELECT category FROM "elastic"."bi_events"
```

Unlike a column name, a table name is quoted as **one lexeme** — `FROM "logs-2025.03"`, never
`FROM "logs-2025"."03"` — because a table name's dots are literal while a column name's separate
the alias from the field.

### The same rules bind DML and DDL

Everything above applies unchanged to `INSERT`, `UPDATE`, `CREATE`, `DROP`, `TRUNCATE`, `ALTER`,
`COPY INTO` and every `SHOW`/`DESCRIBE` — for the **table** name and for **column** names alike.
There is no per-statement-kind exception: a name you may write in a `SELECT` you may write
anywhere.

```sql
INSERT INTO `prod_eu`.dest SELECT a FROM src;
INSERT INTO "prod_eu".dest (`c`) VALUES ('a');
UPDATE `orders` SET "a" = 1 WHERE id = 1;
CREATE TABLE "dest" ("c" INTEGER);
DROP TABLE IF EXISTS `#Tableau_sid_1_Connect_Chec`;
ALTER TABLE "dest" ALTER COLUMN "c" SET DATA TYPE BIGINT;
DROP MATERIALIZED VIEW "mv1";
CREATE TABLE dest (c INTEGER) OPTIONS (`number_of_shards` = 1);
```

Three details worth knowing:

- **A column list is not a table reference.** A column name has no qualifier run, so every part of
  a dotted column name is kept: `INSERT INTO tbl ("sch".c)` names the single column `sch.c` — the
  same reading `SELECT "e"."c"` gives on the expression surface.
- **Renderings carry the quoting back.** A table, view, pipeline, watcher or enrich-policy name
  keeps the qualifier and the quoting it was written with, canonicalised to the ANSI double quote;
  a column name is re-quoted only when the bare spelling could not be read back as the same name
  (so `("c")` renders `(c)`, while `("my col")` renders `("my col")`).
- **Bare names are unchanged.** Every statement that parsed before this release still parses to the
  same statement, including the ones that name a table or column with a word this dialect reserves
  elsewhere (`DROP TABLE count`, `CREATE TABLE t (min INTEGER)`). Quoting is now simply also
  available for them.

Two spellings that were rejected before and still are: `CREATE LOCAL TEMPORARY TABLE …` (there is
no `LOCAL TEMPORARY` clause, quoted or not) and an empty quoted lexeme in any name position — an
empty pair of delimiters is a string literal, never a name.

---

## FROM-less SELECT (connection handshake)

`SELECT` without a `FROM` clause is the connection/health idiom of the JDBC/SQLAlchemy
ecosystem: Tableau re-issues `SELECT 1` on every interaction, Superset's connection test sends
`SELECT 1`, its engine probe sends `SELECT 1 LIMIT 100`, and connection pools use it as
`connectionTestQuery`. All of these are supported:

```sql
SELECT 1;
SELECT 1 AS x;
SELECT 1 LIMIT 100;
SELECT 1 AS ok, UPPER('x') AS u, 1+1 AS two;
SELECT CURRENT_TIMESTAMP AS ts;
SELECT '125'::BIGINT AS c;
```

Each returns **exactly one row**. Column names follow the usual convention: an explicit alias
wins, otherwise the rendered expression (`SELECT 1` yields a column named `1`).

### Connection-check semantics

A FROM-less `SELECT` **executes against the Elasticsearch cluster** — the select-list is
translated to Painless and evaluated by ES, exactly like the same expressions in a FROM-ful
query. Consequently, **with the cluster unreachable, `SELECT 1` fails** with the propagated
connection error. A green handshake genuinely means "connected"; a pool's
`connectionTestQuery = SELECT 1` is a real connection test.

### The handshake index

The first FROM-less `SELECT` on a client lazily creates a dedicated index in the cluster:

- name: `softclient4es_handshake`
- settings: 1 shard, 0 replicas (single-node clusters stay green); `index.hidden: true` on
  ES ≥ 7.7
- mapping: a single `dummy` keyword field
- content: one seeded document (`PUT /softclient4es_handshake/_doc/1 {"dummy": "dummy"}`)

Creation is race-safe and idempotent, and happens once per client lifecycle. The index is
**never listed by `SHOW TABLES`** (any pattern) — which also keeps it out of JDBC
`DatabaseMetaData.getTables` and Arrow Flight `GET_TABLES` browsing. `DESCRIBE TABLE
softclient4es_handshake` still works, deliberately, for debuggability. The index is never
deleted automatically.

#### Read-only BI service accounts

If the account the BI tool connects with cannot create indices, have an administrator
pre-create and seed the index once — lazy creation then becomes a no-op existence probe, and
the read-only account only needs the `read` privilege on `softclient4es_handshake`.

Through SoftClient4ES itself (REPL, JDBC, or any connected client, with a privileged account):

```sql
CREATE TABLE IF NOT EXISTS softclient4es_handshake (dummy KEYWORD)
OPTIONS (settings = (number_of_shards = "1", number_of_replicas = "0"));
INSERT INTO softclient4es_handshake (dummy) VALUES ('dummy');
```

The `CREATE TABLE IF NOT EXISTS` is a no-op when the index already exists; re-running the
`INSERT` just adds another row, which is harmless — the handshake reads a single one.

Or directly against Elasticsearch:

```
PUT /softclient4es_handshake
{
  "settings": {"number_of_shards": 1, "number_of_replicas": 0},
  "mappings": {"properties": {"dummy": {"type": "keyword"}}}
}

PUT /softclient4es_handshake/_doc/1
{"dummy": "dummy"}
```

Without pre-creation, a read-only session's first `SELECT 1` fails with the cluster's own
security error (status preserved) plus an appended message naming both routes of this
guidance.

### What stays rejected

The select-list must be constant scalar expressions — literals or Painless-translatable
functions of literals. Rejected with a named reason (`... requires a FROM clause`):

- column references — `SELECT col`, including embedded ones (`SELECT UPPER(col)`)
- `SELECT *`
- aggregations (`SELECT COUNT(*)`) and window functions
- `EXCEPT(...)`, duplicate output column names, unbound `?` parameters, array literals,
  negative `LIMIT`/`OFFSET`

Rejected at the grammar level: `WHERE` / `GROUP BY` / `HAVING` / `ORDER BY` / `UNION ALL`
after a FROM-less select-list, and `DISTINCT` literals. A constant cast works in every
spelling — `CAST('125' AS BIGINT)`, `CONVERT('125', BIGINT)` and `'125'::BIGINT` all parse.
Prefer `TRY_CAST('125' AS BIGINT)` when the value may not convert: `::` is always the
unsafe form and raises on a bad value.

---

## WHERE

The `WHERE` clause supports:

- comparison operators: `=`, `!=`, `<`, `<=`, `>`, `>=`
- logical operators: `AND`, `OR`, `NOT`
- `IN`, `NOT IN`
- `BETWEEN`
- `IS NULL`, `IS NOT NULL`
- `LIKE`, `RLIKE` (regex)
- conditions on nested fields (`profile.city`, `profile.followers`)

> **A function in a `WHERE` predicate, and documents that do not carry the field.** A predicate that
> applies a function to a column (`WHERE UPPER(status) = 'A'`, `WHERE ABS(amount) > 10`) is executed
> by Elasticsearch as a Painless script. Since engine **0.23.0** such a predicate follows ANSI
> three-valued logic for a document in which the field is **absent**: the comparison is NULL, so the
> document does not match — and it does not match the negated form either (`WHERE NOT UPPER(status)
> = 'A'` leaves it out, because `NOT NULL` is NULL, not TRUE). Before 0.23.0 the emitted script did
> not compile at all and Elasticsearch rejected the whole query (`script_exception: compile error`,
> caused by `class_cast_exception: Cannot cast from [boolean] to [java.lang.Object]`), so no such
> predicate ever ran.
>
> This holds for the comparisons listed here, not only `=`: `<`, `>`, `<>`, `LIKE`, `NOT LIKE`,
> `IN`, `NOT IN`, `BETWEEN` and `NOT BETWEEN` over a function all follow the same rule,
> and a `NOT` written after `AND` / `OR` (`WHERE ABS(amount) > 10 AND NOT UPPER(status) = 'A'`)
> negates the criterion it qualifies, not the whole composite — so the same predicate returns the
> same rows whichever way round you write it. Before **0.23.0** several of these did not run at all:
> `LIKE` over a function produced an uncompilable script, `NOT LIKE` failed inside the engine, and
> `IN` / `BETWEEN` over a function were sent to Elasticsearch with an empty field name and rejected.
>
> A predicate with **no** function is not scripted — it becomes a term/range query — and `NOT` over
> it is Elasticsearch's `must_not`, which **does** return documents that lack the field. The two
> routes therefore differ for absent fields; use `IS NULL` / `IS NOT NULL` when that distinction
> matters.
>
> A **projected** function keeps its `NULL`: `SELECT UPPER(status) AS u` returns `u = NULL` for a
> document with no `status`, and a `GROUP BY UPPER(status)` has no bucket for it. The collapse to
> "no match" applies to a **condition**, never to a value.
>
> 🔴 **`ORDER BY` over a function of a column some documents do not carry LOSES ROWS SILENTLY.** The
> engine emits a null-preserving sort script; Elasticsearch then fails the shard while building the
> comparator (`null_pointer_exception`). What you see depends on the shard count, and the dangerous
> case is the normal one:
>
> - on a **single-shard** index the whole search is rejected — you get an error;
> - on a **multi-shard** index the search returns **HTTP 200** and the failing shard's documents are
>   simply **absent from the result**. MEASURED on Elasticsearch 8.18.3, 3 shards, 7 documents with
>   one lacking the field: `_shards.failed: 1`, `hits.total: 5` — two rows gone, no error anywhere.
>   The engine does not surface `_shards.failures`, so nothing reaches the caller.
>
> Until that is fixed, sort by the bare column, or keep the field present on every document. Do not
> rely on getting an error. The same applies to `ORDER BY` over a `CASE … END` with no `ELSE`, which
> is NULL-valued for the rows no branch matches.
>
> ⚠️ Two limits of the rule above, stated rather than implied. `NOT <function>(x) IS NULL` is a
> PARSE rejection — the grammar takes a bare name after `NOT` there — so the rule covers the
> comparisons listed, not literally every clause you can write. And on a **multi-valued** field the
> scripted and non-scripted routes differ for a reason that has nothing to do with NULL: the native
> query matches if ANY value matches, while the script reads a single value.
>
> ⚠️ **When a `LIKE` over a function needs a regular expression.** The engine compiles such a
> predicate to whitelisted string operations when the pattern contains **no `_`** and uses `%`
> **only at the ends** (`'A%'`, `'%A'`, `'%A%'`, `'A'`, `''`, `'%'`). Every other pattern — including
> one made only of `%`, such as `'A%B'` — compiles to a Painless regular expression, and
> Elasticsearch **6.8** disables those by default (`script.painless.regex.enabled`), answering
> `Regexes are disabled`. On 7.x and later every pattern works.
>
> 🔴 **Changed in 0.23.0 — `LIKE` reads only `%` and `_` as wildcards.** Every other character in a
> pattern is now matched literally, on the scripted **and** the native path. `WHERE status LIKE
> 'A.B%'` previously matched `AXB1`, because `.` reached Elasticsearch as a regular-expression
> wildcard; it now matches only values that really begin with `A.B`. Patterns that relied on the old
> reading must be rewritten with `_` (any single character) or `%` (any sequence). `RLIKE` is
> unaffected — its operand is a regular expression by definition.

**Example**

```sql
SELECT id, name, age
FROM dql_users
WHERE (age > 20 AND profile.followers >= 100)
   OR (profile.city = 'Lyon' AND age < 50)
ORDER BY age DESC;
```

Another example with multiple operators:

```sql
SELECT id,
       age + 10 AS age_plus_10,
       name
FROM dql_users
WHERE age BETWEEN 20 AND 50
  AND name IN ('Alice', 'Bob', 'Chloe')
  AND name IS NOT NULL
  AND (name LIKE 'A%' OR name RLIKE '.*o.*');
```

### Temporal literals against `date` columns

A string literal compared to a column mapped as `date` (with `=`, `<>`, `!=`, `<`, `<=`, `>`, `>=`,
`BETWEEN` or `IN`) is resolved against the column's mapping **format** before the query is sent to
Elasticsearch, so the SQL-standard spelling a BI tool emits selects the same rows as the ISO one:

```sql
WHERE event_ts >= '2026-06-04 00:00:00.000000'   -- what Superset / SQLAlchemy render
WHERE event_ts >= '2026-06-04 00:00:00'
WHERE event_ts >= '2026-06-04T00:00:00'          -- what Elasticsearch's default format accepts
```

- Under a format that accepts ISO dates (the default `strict_date_optional_time||epoch_millis`,
  `date_optional_time`, `strict_date_optional_time_nanos`) the space separator is rewritten to `T`;
  fraction digits and a trailing zone are preserved. ISO literals, date-only literals, epoch
  numbers and date math (`now-1d/d`, `2026-06-04||/M` -- whose date part is normalised the same way)
  are forwarded verbatim.
- A column with a custom `format` (for example `yyyy-MM-dd HH:mm:ss`) keeps working as before: a
  literal its format already parses is never rewritten.
- Under the default (strict) format a literal that cannot be a date at all (`'not-a-date'`) or
  carries an invalid calendar or time value (`'2026-02-30'`, `'2026-06-04 24:00:00'`) fails with
  an error naming the literal and the field (HTTP 400) instead of a raw Elasticsearch
  `search_phase_execution_exception`. A literal that starts like a date but has a shape the
  resolver does not model (a zone id, a signed year, `2026-6-4`) is forwarded verbatim and
  Elasticsearch decides; under `date_optional_time` or a custom format nothing is ever rejected.
- The same resolution applies to the `WHERE` clause of `UPDATE` and `DELETE`.
- `keyword`/`text` columns, `LIKE`/`RLIKE` patterns, function-wrapped columns (`YEAR(event_ts)`),
  `date_nanos` columns, columns qualified with a `JOIN` alias (the FROM table's own columns are
  resolved) and `HAVING` conditions are never touched.
- The resolution needs the index mapping, loaded through the schema cache (one lookup per index per
  TTL — `elastic.schema-cache.ttl`, 5 minutes by default, and an index may set its own with
  `ALTER TABLE … SET SCHEMA CACHE TTL`). An
  **index alias over exactly one index** resolves to that index's mapping (`SHOW TABLE` /
  `DESCRIBE` through such an alias resolve the same way); an alias over **several** indices is
  ambiguous and is treated as unresolvable. When the statement reads several indices or a wildcard,
  or the mapping cannot be loaded, the literal is forwarded verbatim as in previous releases, and a
  failed mapping lookup is remembered for the DEFAULT TTL (a miss has no index metadata to read a
  per-index one from) so it is not retried on every statement.

---

## ORDER BY

`ORDER BY` sorts the result set by one or more expressions.

- Supports multiple sort keys
- Supports `ASC` and `DESC`
- Supports `NULLS FIRST` / `NULLS LAST` per sort key (see below)
- Supports expressions and nested fields (e.g., `profile.city`)
- When used inside a window function (`OVER`), `ORDER BY` defines the logical ordering of the window

**Example**

```sql
SELECT id, name, age
FROM dql_users
ORDER BY age DESC, name ASC
LIMIT 2 OFFSET 1;
```

### NULLS FIRST / NULLS LAST

Each sort key may declare where `NULL` values appear in the result:

```sql
SELECT id, name, bonus
FROM dql_users
ORDER BY bonus DESC NULLS LAST;
```

Mapped to Elasticsearch's `sort.missing` parameter:

- `NULLS FIRST` → `"missing": "_first"`
- `NULLS LAST`  → `"missing": "_last"`

When `NULLS FIRST` / `NULLS LAST` is omitted, defaults follow the Elasticsearch
convention:

- `ASC`  → nulls last
- `DESC` → nulls first

Different null orderings can be combined within a single query:

```sql
SELECT id, name, bonus, hire_date
FROM dql_users
ORDER BY bonus DESC NULLS LAST, hire_date ASC NULLS FIRST;
```

**Caveat (ES6 Jest client)**: scroll / `search_after` queries in the ES6 Jest
client do not propagate `NULLS FIRST` / `NULLS LAST` reliably across batches
(search_after's null handling is implementation-defined in Jest). For ES6
scroll/search_after, prefer client-side null-bucketing or upgrade to ES7+.

---

## LIMIT / OFFSET

- `LIMIT n` restricts the number of returned rows.
- `OFFSET m` skips the first `m` rows.
- Translated to Elasticsearch `from` + `size`.

Example:

```sql
SELECT id, name, age
FROM dql_users
ORDER BY age DESC
LIMIT 10 OFFSET 20;
```

---

## UNION ALL

`UNION ALL` combines the results of multiple `SELECT` queries **without removing duplicates**.

> Bare `UNION` (with row de-duplication) is **not supported** and is rejected at parse time — the
> two are not synonyms. It used to be accepted silently, returning only the first leg's rows.

All SELECT statements in a UNION ALL must project:

- the **same number of columns**, matched **by position** (the result takes the first SELECT's
  column names);
- **types that combine**, column by column (see [Column types](#column-types) below).

If these conditions are not met, the Gateway raises a validation error before executing the query.

**Example**

```sql
SELECT id, name FROM dql_users WHERE age > 30
UNION ALL
SELECT id, name FROM dql_users WHERE age <= 30;
```

### Execution model

The SQL Gateway executes `UNION ALL` using **Elasticsearch Multi‑Search (`_msearch`)**:

1. Each SELECT query is translated into an independent ES search request.
2. All requests are sent in a single `_msearch` call.
3. The Gateway concatenates the results **in order**, without deduplication.
4. ORDER BY, LIMIT, OFFSET apply **per SELECT**, not globally (unless wrapped in a subquery, which is not supported).

### Notes

- `UNION ALL` does **not** sort or deduplicate results.
- Column names in the final output are taken from the **first SELECT**; the columns of the other
  SELECTs are matched by position, whatever they are called.

### Column types

Since `0.24.0`, a set operation is typed by ONE rule, the same at every depth: a column, each field
of an object (a `STRUCT`, a `GEO_POINT`), each element of a list (an `ARRAY`), and so on inside
them. The rule gives the type DuckDB gives — DuckDB being the engine that runs `UNION`,
`INTERSECT` and `EXCEPT`, so a statement answers the same types on both paths — `VARCHAR` where
DuckDB gives none, and refuses only what the relational engine fails on when the statement runs.
At one position, over the branches of one node (below):

| Branches | Type | Example |
|---|---|---|
| numbers, and `BOOLEAN` | the widest: `TINYINT` < `SMALLINT` < `INT` < `BIGINT` < `REAL` < `DOUBLE`; a `BOOLEAN` reads as `1` / `0` | `n` (`INT`) with `x` (`DOUBLE`) is `DOUBLE` |
| a `DATE` and a `TIMESTAMP` | `TIMESTAMP`, the date at midnight UTC | `d` with `ts` |
| a text branch (`KEYWORD`, `TEXT`, a string literal) with any other type | `VARCHAR`, each value spelled as DuckDB spells its type (below) | `id` with `n`, `1` with `'a'`, `ts` with `s` |
| a `VARBINARY` with text branches only | `VARBINARY` (bytes), DuckDB's `BLOB`: a binary value is the bytes of its base64 text, a text value is cast to bytes (below) | `payload` with `name` |
| a `NULL` literal | the other branches' type | `n` with `NULL` is `INT` |
| a `DATE` or a `TIMESTAMP` with a number or a `BOOLEAN`, and neither a text nor a `VARBINARY` branch | refused | `d` with `n` |
| any other pair DuckDB gives no type: a `VARBINARY` with a number, a temporal or a `BOOLEAN`; a `TIME` with a `DATE`, a `TIMESTAMP`, a number or a `BOOLEAN` | `VARCHAR`, each value spelled as text (below) | `payload` with `price`, `CAST(ts AS TIME)` with `d` |
| an `ARRAY`, a `STRUCT` or a `GEO_POINT` with a type of another family, text included | refused | `location` with `name` |
| lists | a list of the type this rule gives their elements | `ARRAY<INT>` with `ARRAY<KEYWORD>` is `ARRAY<VARCHAR>` |
| objects and points | DuckDB's merged `STRUCT`: every field of every branch, in the order it first appears, matched whatever its case; a field a branch does not have is `NULL`, and each field takes the type this rule gives it over the branches that have it; points alone stay a `GEO_POINT` | `location` with `address` |

So a field `a` that is an `INT` in one object and a `TIME` in another is a `VARCHAR`, as two such
columns are, and an `INT` beside a `DATE` is refused in a field as in a column, the field named by
its path. A `VARBINARY` counts as text beside a date and a number — the relational engine reads a
`binary` field as its base64 text — so `payload`, `d` and `n` make a `VARCHAR` column.

A `GEO_POINT` is DuckDB's `STRUCT(lat DOUBLE, lon DOUBLE)`: in a column of points, or of points and
objects, every point is a `{lat, lon}` map whatever way Elasticsearch stored it — an object, a
`"lat,lon"` text, an array `[lon, lat]`, a geohash (the south-west corner of its cell, as
Elasticsearch reads it), a WKT `POINT (lon lat)` or a GeoJSON point. A `STRUCT`'s fields are taken
by name, in name order. A value that cannot be read as its branch's point or object, or several of
them in one row, fails the statement. A column read without a type (`ANY`), as an `ARRAY<INT>`
column of a mapping is read today, refuses nothing.

A whole number literal is an `INT` (`SELECT 1`), a `BIGINT` past the `INT` range. The type
compares every branch, so `d`, `n` and `'1'` make a `VARCHAR` column although `d` and `n` alone
are refused; and each branch's value goes to the column's type from its OWN type (`d`, `ts` and
`'1'` spell `d` as a date, not as a midnight timestamp).

A chain that mixes operators is typed as a tree, as DuckDB types it: `INTERSECT` binds first, then
the operators apply left to right, `INTERSECT` and `EXCEPT` typing their two sides, and a run of
ONE union kind (`UNION ALL` after `UNION ALL`, or `UNION` after `UNION`) is typed as one node,
whatever its order; a `UNION` beside a `UNION ALL`, a derived table, are separate nodes. Each node
of the tree takes the rule above, at every depth, over all its branches at once — a subtree's type
being one of them — so their order never changes the type. So `n UNION d UNION s` and `payload
INTERSECT d INTERSECT n` are `VARCHAR` columns, while `n INTERSECT d INTERSECT s`, `d INTERSECT n
INTERSECT payload` and `n EXCEPT x UNION ALL d` are refused: `n INTERSECT d`, and `n EXCEPT x`
beside `d`, are a number beside a date with no text. The relational engine runs these operators;
core runs `UNION ALL` itself.

Every value holds its position's type, at every depth: wherever the branches' types differ at a
position — a column, a field, an element — every branch's value there is converted, and the
position holds ONE class: a `DOUBLE` a `Double`, a `BIGINT` a `Long`, a `TIMESTAMP` a
`ZonedDateTime` (a date-only, zone-less or epoch-milliseconds value included), a `VARBINARY`
bytes, a `VARCHAR` a string, a list a list of its element's type, an object a map of its fields
in order — which is the type JDBC and Arrow report, since they read it from the first value. A
position whose branches already have one type is returned as each branch reads it, nothing
converted: a branch over an aggregate keeps the form an aggregate answers, a `Double`, so `SELECT
MAX(n) ... UNION ALL SELECT n ...` answers `10.0` beside `3` and `5`.

Each value is first read as the type its field is declared with, as Elasticsearch indexed it, and
only then converted: a value Elasticsearch accepted as text is the value its queries see. In an
integer field `"5"`, `"5.0"`, `"5.5"` and `5.5` are `5` (truncated toward zero), `"5e2"` is `500`;
in a `DOUBLE` field `"5"` is `5.0` (spelled `5.0` in a `VARCHAR` position) and `"1e3"` is `1000.0`;
in a `BOOLEAN` field `"true"` is `true` and an empty text `false`; an empty text in a number field
is no value.

At a position that converts, a date field's value is read with the field's mapping `format` —
its own, or Elasticsearch's default `strict_date_optional_time||epoch_millis` — as the Elasticsearch major the statement runs
on reads that format: Elasticsearch 6 parses with Joda-Time, Elasticsearch 7 and later with
`java.time`, and the same mapping reads some texts differently across them. A `TIMESTAMP` is the
instant that major indexed, to the millisecond, as Elasticsearch keeps it, and a `DATE` its day, in
UTC. So `1706697000` in an `epoch_second` field is `2024-01-31 10:30:00`, not a moment of 1970;
`"1706697000.5"` there keeps its half second on Elasticsearch 7 and 8 and is truncated to the
second on Elasticsearch 6; `"2024/01/31"` in a `yyyy/MM/dd` field is a date, not a text; a format's
`||` alternatives are tried in order, and the first one that parses a value decides; in the default
format `"2024"` and `2024` are the year 2024. A value computed by the statement (a function, a
`CAST`) has no format: there, digits are epoch milliseconds and an ISO text is its instant.

A value is read only where its reading is the same on every Java version core runs on (8, 11, 17
and 21) and equal to the instant the Elasticsearch major indexed, measured value by value on
Elasticsearch 6.8.23, 7.17.29 and 8.18.3. That covers the built-in formats of each major's
documentation (the epoch, ISO, ordinal, week, time and `basic_` formats; in camel case on
Elasticsearch 6 and 7; with a leading `8`), and custom patterns — the pattern letters at each
width, alone and in the combinations measured, with `java.time` on Elasticsearch 7 and later and
in Elasticsearch 6.8's `8`-prefixed mode, with Joda-Time on Elasticsearch 6 — with a zone written
as `Z`, an offset, `UTC`, `GMT` or `UT`, together with epochs, negative ones included, and JSON
numbers. Elasticsearch's own readings are kept, however odd: in a `YYYY-MM-dd` field (a week-based year)
Elasticsearch 8 indexes `2024-01-31` as `2023-12-31`, the first day of the week-based year's first
week, and Elasticsearch 7 as `2024-01-01`; a year with no month and an hour (`2024T10` in a
`strict_date_optional_time` field) is `1970-01-01 10:00:00`.

A value its field's format does not read fails the statement, naming the value, the field and the
format, and so does a value core cannot read without guessing, rather than be read as a date it
may not be:

- a pattern letter, a width or a combination of letters that no measurement covers — two zone
  letters included (an offset beside a zone id, Joda-Time's `ZZ ZZZ`) — and a name of
  Elasticsearch 6.8's `8`-prefixed mode;
- a zone id or a zone name other than `UTC`, `GMT` and `UT`, in every format, the default
  included: a region id (`Europe/Paris` in a `VV` or a Joda-Time `ZZZ` letter, in the default
  format, or in brackets after an offset, `+05:00[Europe/Paris]`) and an abbreviation or a long
  name (`CET`, `PST`, `Pacific Time`). Its offset comes from zone data, and the zone data core's
  Java version holds is not the one Elasticsearch indexed the value with: `2023-07-01T12:00:00
  America/Mexico_City` is `17:00:00Z` on Elasticsearch 8.18.3 and `18:00:00Z` with older zone data,
  and `IST` is UTC on Elasticsearch 8 and Israel time on Elasticsearch 7;
- a value whose reading would depend on the Java version core runs on: a localized offset (`O`,
  `OOOO`, `ZZZZ`: `GMT+1`), which Java 8 parses differently from later versions; a fraction right
  after digits of a variable width (`yyyyMMddHHmmssSSS`); and, in Elasticsearch 6.8's `8`-prefixed
  mode, an AM/PM with no hour (`8yyyy a`), which Java 16 and later set to 06:00 or 18:00, and a week
  past the last of its week-based year (`2020-53-7` in a `8YYYY-ww-e` field), which Java 21
  rejects;
- a value the response already gives as a date or a time, without the offset or the digits its
  format reads (`10:30:00Z` in a `time` field);
- a fractional JSON number (`1706697000.0`) whose digits, which Elasticsearch read, the number no
  longer pins to the millisecond.

```sql
-- ts is a TIMESTAMP field of format 'yyyy/MM/dd' holding '31-01-2024', s a KEYWORD
SELECT ts AS k FROM t UNION ALL SELECT s AS k FROM t;
-- Error: Conversion Error: Could not read '31-01-2024' as a TIMESTAMP when casting from source column k: the date format 'yyyy/MM/dd' of field ts does not read it as Elasticsearch 8 does
-- a zone name
-- Error: Conversion Error: Could not read '2024-01-31T10:30:00 CET' as a TIMESTAMP when casting from source column k: core cannot read it with the date format 'yyyy-MM-dd'T'HH:mm:ss z' of field ts as Elasticsearch 8 does: the zone 'CET' is a region id or a zone name, whose offset comes from the zone data of the JVM reading it, not Elasticsearch's: core reads only Z, an offset, UTC, GMT and UT
```

The readings were measured on Elasticsearch 6.8.23, 7.17.29 and 8.18.3. The other minor versions
of a major are read as its measured one, and Elasticsearch 9 as Elasticsearch 8.

A value with several values — a multi-valued field — has no one value of its position's type, so
at a position that converts its values it fails the statement, in the words DuckDB uses for the
same failure: `Conversion Error: Unimplemented type for cast (INTEGER -> VARCHAR[]) when casting
from source column k` for a multi-valued keyword beside an `INT`. A position whose branches have
one type converts nothing, and answers such a value as its branch reads it.

```sql
-- id is a KEYWORD, n an INT: one VARCHAR column ('r1', ..., '3', ...)
SELECT id AS k FROM t UNION ALL SELECT n AS k FROM t;
-- n is an INT, x a DOUBLE: one DOUBLE column (3.0, ..., 0.25, ...)
SELECT n AS k FROM t UNION ALL SELECT x AS k FROM t;
-- refused, by name, before anything runs
SELECT d AS k FROM t UNION ALL SELECT n AS k FROM t;
-- Error: Set operation branches must project compatible types at column 1: branch 1 'k' is DATE, branch 2 'k' is INT
-- two objects whose field a is an INT and a DATE: refused, naming the field
SELECT oi AS k FROM t UNION ALL SELECT od AS k FROM t;
-- Error: Set operation branches must project compatible types at column 1: branch 1 'k' field 'a' is INT, branch 2 'k' field 'a' is DATE
```

Inside a `VARCHAR` column, each value is spelled as DuckDB spells its own type when it casts it to
text, so both paths answer the same string:

| Branch type | Text | Examples |
|---|---|---|
| a whole number | its digits | `3`, `-2`, `3000000000` |
| `DOUBLE`, `REAL` | the shortest digits, with a fractional part; an exponent below `1e-04` or from `1e+16` | `250000000000.0`, `-2.5`, `5.0`, `1e-05`, `1e+16` |
| `BOOLEAN` | `true` / `false` | |
| `DATE` | `YYYY-MM-DD` | `2024-01-31`, `0044-03-15 (BC)` |
| `TIMESTAMP` | `YYYY-MM-DD HH:MM:SS`, in UTC, the fraction of a second only when there is one | `2024-01-31 10:30:00`, `2023-12-25 23:59:59.999`, `2024-01-31 00:00:00` for a date-only value |
| `TIME` | `HH:MM:SS`, the same fraction | `10:30:00`, `06:00:00.5` |
| `VARBINARY` | its base64 text, as Elasticsearch holds a `binary` field | `YWI=` |

```sql
-- ts is a TIMESTAMP, s a KEYWORD: one VARCHAR column ('2024-01-31 10:30:00', ..., 'a', ...)
SELECT ts AS k FROM t UNION ALL SELECT s AS k FROM t;
-- x is a DOUBLE: ('250000000000.0', '1e-05', ..., 'a')
SELECT x AS k FROM t UNION ALL SELECT 'a' AS k FROM t;
```

A text value becomes bytes in a `VARBINARY` column as DuckDB casts a `VARCHAR` to a `BLOB`: each
ASCII character is its byte and `\xHH` (two hexadecimal digits) is the byte `HH`; a character
outside ASCII, or a backslash not followed by `x` and two hexadecimal digits, fails the statement,
as it fails in DuckDB (`Conversion Error: Invalid byte encountered in STRING -> BLOB conversion`).

A `DOUBLE` is spelled as DuckDB spells it, digit for digit, including the few values its algorithm
does not shorten (`7.168e25` is `7.1680000000000004e+25`). A decimal literal is a `DOUBLE` here
(`1.50` is `1.5`), where DuckDB keeps it a `DECIMAL` (`1.50`). A value Elasticsearch returns as
text, in a column of another type, stays that text.

The types are compared once each branch's mapping is read; with no mapping to read, nothing is
refused on types and Elasticsearch answers.

---

## JOIN UNNEST

The Gateway supports a specific form of join: `JOIN UNNEST` on `ARRAY<STRUCT>` columns.

### Table definition

```sql
CREATE TABLE IF NOT EXISTS dql_orders (
  id INT NOT NULL,
  customer_id INT,
  items ARRAY<STRUCT> FIELDS(
    product VARCHAR OPTIONS (fielddata = true),
    quantity INT,
    price DOUBLE
  ) OPTIONS (include_in_parent = false)
);
```

### Query with JOIN UNNEST and window function

```sql
SELECT
  o.id,
  items.product,
  items.quantity,
  SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o
JOIN UNNEST(o.items) AS items
WHERE items.quantity >= 1
ORDER BY o.id ASC;
```

`JOIN UNNEST` behaves like a standard SQL UNNEST operation: each element of an `ARRAY<STRUCT>` becomes a separate output row, with parent fields duplicated — exactly like a relational unnest.

This means:

- the array's columns (`items.price`, `items.quantity`) can be projected and filtered
- a window over an arithmetic expression of UNNEST columns works (e.g.
  `SUM(items.price * items.quantity) OVER …`); a window over a bare UNNEST column currently returns
  NULL
- parent-level aggregations can be computed
- **full row-level expansion** produces one output row per array element
- multi-level nesting is handled recursively

### Functions over an UNNEST column

Each array element is a separate (nested) Elasticsearch document, and a computed value is evaluated
against ONE document — so:

- **In SELECT**, a function over an UNNEST column (`UPPER(items.product)`,
  `items.price * items.quantity`, `CAST(items.product AS VARCHAR)`) is **refused**, beside a window
  function too: it would be computed once per parent document, where the element's columns are not
  visible. Select the columns and compute the value from the returned rows.
- **In WHERE**, a condition whose function is applied directly to the UNNEST column —
  `CAST(items.quantity AS VARCHAR) = '2'`, `ISNULL(items.price) = FALSE`, a date part such as
  `YEAR(<column>) = 2025` or `EXTRACT(MONTH FROM <column>) = 2` — is evaluated on each element and
  works. A condition whose function is not (`UPPER(items.product) = 'A'`, `ABS(items.quantity) = 2`,
  a `CASE`, `COALESCE`, arithmetic), or that is evaluated on each element but also reads a parent
  column, is **refused**: filter the returned rows instead — unless the UNNEST column is only a
  `COALESCE` argument after a non-null literal, which `COALESCE` never returns
  (`COALESCE('n/a', items.product) = 'n/a'` works).
- A condition without a function (`items.quantity >= 1`, `items.product IN ('A', 'B')`), an
  aggregate over UNNEST columns, a window function over an UNNEST column, over arithmetic of UNNEST
  columns or over a function applied directly to one (`SUM(CAST(items.quantity AS DOUBLE)) OVER …`),
  and statements planned by the relational engine (cross-index JOIN, derived table, CTE) are not
  affected by this refusal.

> ⚠️ A **derived table** over `JOIN UNNEST` is answered by the arrow extension (the JDBC and ADBC
> drivers, the Flight SQL sidecar, federation, and the REPL when the extension is not excluded).
> An inner column without an alias is named by its **short name**, the last part of the reference:
> `items.product` is `product` — the name every clause of the outer query uses, and the label
> `SELECT *` gives the column. Two inner columns whose short names coincide (`o.id` and `items.id`)
> are refused with a message asking for an alias: alias one of them (`items.id AS item_id`).
> Otherwise the inner query needs no alias and no `LIMIT` — within the elements an UNNEST projection
> returns per parent (100 by default, see below). Measured on Elasticsearch 8.18 and 6.8:
>
> ```sql
> SELECT id, UPPER(product) AS product, total_price
> FROM (SELECT o.id, items.product,
>              SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
>       FROM dql_orders o JOIN UNNEST(o.items) AS items) d;
> ```

```sql
-- refused: A function over an UNNEST column is not supported in SELECT: UPPER(items.product) is
-- evaluated per parent document, where items.product is not visible. Select its columns and
-- compute it from the returned rows.
SELECT o.id, UPPER(items.product) AS product FROM dql_orders o JOIN UNNEST(o.items) AS items;

-- answered: select the column, upper-case it in the returned rows
SELECT o.id, items.product FROM dql_orders o JOIN UNNEST(o.items) AS items LIMIT 100;

-- refused as well: beside a window, the rows are computed the same way
SELECT o.id, UPPER(items.product) AS product,
       SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o JOIN UNNEST(o.items) AS items;

-- answered: the window stays, only UPPER moves to the returned rows
SELECT o.id, items.product,
       SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o JOIN UNNEST(o.items) AS items LIMIT 100;
```

Without a `LIMIT`, an UNNEST projection returns up to 100 elements per parent, or up to the index's
own `index.max_inner_result_window` when that setting is lower. A parent holding more elements is
cut without an error: measured on Elasticsearch 6.8, 7.17, 8.18 and 9.0, a 101-element parent
returns 100 rows, its last element dropped.

---

## Aggregations

Supported aggregate functions include:

- `COUNT(*)`, `COUNT(expr)`
- `SUM(expr)`
- `AVG(expr)`
- `MIN(expr)`
- `MAX(expr)`
- `STDDEV(expr)` / `STDDEV_SAMP(expr)` / `STDDEV_POP(expr)`
- `VARIANCE(expr)` / `VAR_SAMP(expr)` / `VAR_POP(expr)`

`STDDEV` defaults to **sample** standard deviation (Bessel-corrected, `STDDEV ≡ STDDEV_SAMP`) and
`VARIANCE` defaults to **sample** variance (`VARIANCE ≡ VAR_SAMP`). This matches PostgreSQL and
Snowflake; users coming from MySQL 5.5 or earlier should note that those releases defaulted
`STDDEV` to population.

```sql
SELECT department,
       STDDEV(salary)   AS sd,
       VAR_POP(salary)  AS vp
FROM emp
GROUP BY department;
```

All six map to a single Elasticsearch `extended_stats` aggregation per call; the requested field
(`std_deviation_sampling`, `variance_sampling` for the sample variants; the un-suffixed
`std_deviation`, `variance` for the population variants) is projected from the response. Sample
variants require **Elasticsearch 7.7+**; population variants work on Elasticsearch 6+.

Over a **transformed** operand (`STDDEV(YEAR(hire_date))`, `VARIANCE(ABS(salary))`, plain or
windowed) the statistic is computed over the transform on **Elasticsearch 7 and later**; on
Elasticsearch 6 the query is **refused** with a `400` naming the release, because the client library
cannot emit the aggregation script there and used to return the statistic of the raw field silently.
See [STDDEV / VARIANCE family](functions_aggregate.md#function-stddev--variance-family).

### Percentiles — `PERCENTILE_CONT` / `PERCENTILE_DISC`

- `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY column)` — ANSI ordered-set aggregate (optionally with a top-level `GROUP BY`)
- `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY column) OVER (PARTITION BY ...)` — value column from `WITHIN GROUP`, partition from `OVER`
- `PERCENTILE_CONT(p) OVER (PARTITION BY ... ORDER BY column)` — value column from the `OVER` `ORDER BY`
- `PERCENTILE_CONT(column, p)` — column-first shorthand (many BI tools emit it)

The percentile literal `p` is a value in `[0, 1]` (e.g. `0.99` for p99); a value outside that range is
rejected at parse time. The **value column** is given by the `ORDER BY` clause (`WITHIN GROUP` or `OVER`),
or the shorthand's first argument; **grouping** is given by `OVER (PARTITION BY ...)` or a top-level
`GROUP BY` (or neither — a single percentile over the whole result set). Both functions map to the
Elasticsearch `percentiles` aggregation (TDigest). Elasticsearch has no native discrete percentile, so
`PERCENTILE_DISC` is **continuous-backed** — it returns the same interpolated value as `PERCENTILE_CONT`
rather than the nearest actual data point. All forms work on Elasticsearch 6+.

```sql
-- p99 request latency per endpoint (SRE latency analysis)
SELECT endpoint,
       PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY duration_ms) AS p99
FROM requests
GROUP BY endpoint;
```

### GROUP BY and HAVING

```sql
SELECT profile.city AS city,
       COUNT(*) AS cnt,
       AVG(age) AS avg_age
FROM dql_users
GROUP BY profile.city
HAVING COUNT(*) >= 1
ORDER BY COUNT(*) DESC;
```

- `GROUP BY` supports nested fields (`profile.city`).
- `HAVING` filters groups based on aggregate conditions.
- Translated to Elasticsearch aggregations.
- An aggregate referenced only in `HAVING` or `ORDER BY` needs no alias and no `SELECT` item: it is
  computed for the filter or the sort and kept out of the result columns. Distinct aggregates over
  the same column stay distinct (`HAVING COUNT(age) >= 1 AND MAX(age) > 45`), and the aggregate may
  wrap a transform (`HAVING MAX(YEAR(birthdate)) > 1990`, `ORDER BY MAX(ABS(age)) DESC`).
- Arithmetic over aggregates is computed per group (`MAX(price) - MIN(price) AS price_range`); the
  operands are computed as hidden aggregations of the group.
- With **no `GROUP BY`**, the whole table is one group and the statement answers **one row**,
  calculations included (since `0.24.0`): `SELECT MAX(price) - MIN(price) AS price_range FROM t`,
  `COUNT(*) * 2`, `MAX(created) + 1` (a `DATE`), `DATEDIFF(MAX(created), MIN(created))`,
  `GREATEST(MAX(created), MAX(updated))`. A calculation is evaluated as it is per group, with the
  same rules. Every `SELECT` item must then be an aggregate, a calculation over aggregates or a
  constant.
- Over **no matching document**, with no `GROUP BY`: a `MAX`, a `MIN` or an `AVG` is NULL, and so
  is a calculation that reads one; `COUNT(*)` is `0` (`COUNT(*) * 2` is `0`, and
  `HAVING COUNT(*) = 0` keeps the one row). A calculation over `COUNT(column)`,
  `COUNT(DISTINCT column)` or `SUM(column)` is evaluated too — `COUNT(n) * 2` is `0`, `SUM(x) * 2`
  is `0.0`, `SUM` over no value being `0.0` — on Elasticsearch 7.17 and later. Elasticsearch 6.8
  cannot evaluate it (it refuses the `keep_values` gap policy), so such a calculation is NULL
  there.
- `HAVING` may reference a `SELECT` aggregate by its alias (`COUNT(*) AS cnt ... HAVING cnt > 1`),
  including the alias of an arithmetic expression over aggregates (`... AS price_range ... HAVING
  price_range > 10`); `BETWEEN`, `IN` and `NOT` apply to aggregates as to columns.
- Rejected with an explicit error: arithmetic over aggregates written inline in `HAVING`
  (`HAVING MAX(price) - MIN(price) > 10` — alias it in `SELECT` and reference the alias), an
  aggregate function inside `WHERE` (use `HAVING`, and this covers a wrapped one such as
  `WHERE ABS(COUNT(*)) > 1`), an alias that names one aggregate in `SELECT` and a different one
  in `HAVING` / `ORDER BY`, and a full-text `MATCH ... AGAINST` in `HAVING` over an aggregate
  (`HAVING MATCH (MAX(title)) AGAINST ('x')`, or over its `SELECT` alias) or over a column that is
  neither an aggregate nor a `GROUP BY` key (`GROUP BY city HAVING MATCH (title) AGAINST ('x')`)
  — put the `MATCH` in `WHERE`.
- A **function of an aggregate** in `HAVING` is applied to the group, or the statement is rejected
  by name — it is never ignored. `COALESCE`, `GREATEST`, `LEAST` and `SIGN` over an aggregate filter
  the groups (`HAVING COALESCE(COUNT(*), 0) > 30`, `HAVING GREATEST(MAX(price), 0) > 100`), on
  either side of the comparison and under `NOT`; `BETWEEN` and `IN` support them in the tested
  POSITION (`GREATEST(COUNT(*), 0) BETWEEN 1 AND 5`) but not as a BOUND
  (`COUNT(*) BETWEEN 1 AND ABS(MAX(price))` is refused). Everything the engine cannot evaluate as a
  group filter is refused with the reason:
  - a rendering that can be NULL — `HAVING NULLIF(COUNT(*), 0) > 1`;
  - a rendering that needs a local variable — `HAVING ROUND(SUM(price), 2) > 10`;
  - a rendering that boxes a number — `HAVING ABS(COUNT(*)) > 1` and the rest of the numeric
    function family (`FLOOR`, `CEIL`, `SQRT`, `EXP`, `LOG`, `POWER`). This is a deliberate
    over-approximation: the boxing conversion compiles in *some* positions of a group-filter script
    and not others, so the engine refuses it in all of them rather than guess. The same functions
    work normally in `WHERE`, in the `SELECT` list and in `ORDER BY`;
  - `CASE ... END` in `HAVING`, which needs a document and a group filter has none;
  - a function applied to a `SELECT` aggregate **alias** — `COUNT(*) AS c ... HAVING NULLIF(c, 0) > 1`;
  - an aggregate computed outside a nested grouping — `... JOIN UNNEST(t.emails) AS e GROUP BY
    e.name HAVING COALESCE(MAX(amount), 0) > 1`, where no level of the aggregation can read the
    metric.

  In every refused case the remedy is the same: **compare the aggregate itself** —
  `HAVING SUM(price) > 10` rather than `HAVING ROUND(SUM(price), 2) > 10` — and apply the function
  to the result outside the query. Aliasing the expression in `SELECT` does NOT help: a function of
  an aggregate is not a valid `SELECT` item under a `GROUP BY` either
  (`SELECT ABS(COUNT(*)) AS a ... GROUP BY city` is rejected as a non-aggregated field). That
  differs from arithmetic over aggregates, which IS a valid `SELECT` item
  (`MAX(price) - MIN(price) AS price_range`) and is the reason the rule above tells you to alias
  THAT one.

#### Comparing a DATE aggregate

⚠️ **A comparison between a date aggregate and a date literal is refused**, function or not:
`HAVING MAX(created) > '2019-01-01'` and `HAVING MIN(created) < '2020-01-01'` are rejected at parse
time. A group filter reads every metric as a number — a date as epoch milliseconds — so the
generated comparison is text against a number and Elasticsearch fails the whole search with a
`class_cast_exception`. Compare in `WHERE` instead, or filter the result outside the query.

### Conditions on the GROUP BY key

A `HAVING` condition over the grouping key filters GROUPS, and it is applied by the `terms` filter —
so it is correct for a multi-valued field, where one document belongs to several groups.

```sql
SELECT city, COUNT(*) AS cnt FROM dql_users GROUP BY city HAVING city = 'Paris';
SELECT city, COUNT(*) AS cnt FROM dql_users GROUP BY city HAVING city LIKE 'P%';
SELECT city, COUNT(*) AS cnt FROM dql_users GROUP BY city HAVING city <> 'Lyon';
```

- Supported: a direct comparison of the key — `=`, `<>`, `IN`, `LIKE` / `RLIKE`.
- ⚠️ **Which COMBINATIONS are supported follows from how Elasticsearch applies them.** The `terms`
  filter carries one list of kept values and one list of removed values, and each is a UNION:

  | combination | supported | why |
  |---|---|---|
  | `city = 'Paris' OR city = 'Lyon'` | ✅ | the kept list is a union, i.e. a disjunction |
  | `city <> 'Paris' AND city <> 'Lyon'` | ✅ | not-in-A and not-in-B is not-in-(A ∪ B) |
  | `city = 'Paris' AND city <> 'Lyon'` | ✅ | one kept list and one removed list, applied together |
  | `city LIKE 'P%' AND city NOT LIKE 'L%'` | ✅ | one pattern in each of the two lists |
  | `city <> 'Paris' OR city <> 'Lyon'` | ❌ refused | a union of removals is a conjunction, so this would be executed as one |
  | `city = 'Paris' AND city = 'Lyon'` | ❌ refused | a union of kept values is a disjunction, so this would be executed as one |
  | `city = 'Paris' OR city <> 'Lyon'` | ❌ refused | the two lists are applied together, i.e. ANDed |
  | `city = 'Paris' OR city LIKE 'L%'` | ❌ refused | ⚠️ a pattern REPLACES the list — see below |
  | `city LIKE 'P%' OR city LIKE 'L%'` | ❌ refused | one list holds one pattern, so the second is lost |

  ⚠️ **Being a union is necessary but not sufficient.** Each of the two lists holds either a set of
  values or ONE pattern (`LIKE` / `RLIKE`), and a pattern replaces the set — so a pattern meeting
  anything else in the SAME list loses a side, even where the combination itself is a disjunction.
  `HAVING city = 'Paris' OR city LIKE 'L%'` used to return only the `L…` groups. Use a single
  `RLIKE` covering both alternatives, or split the query.

  The refused rows previously returned a plausible-looking but WRONG set of groups. Otherwise:
  split the query, or restate the condition as an `OR` of equalities or an `AND` of inequalities.
- ⚠️ **A FUNCTION of the key is refused** (`HAVING UPPER(city) = 'PARIS'`,
  `HAVING LENGTH(status) = 1`). The terms filter can only express a direct comparison, and the
  alternatives are unsound: filtering documents instead would keep or drop a multi-valued document
  WHOLE, and would change the counts of surviving groups whenever the key is itself a function of
  the column (`GROUP BY DAY(d) HAVING YEAR(d) = 2025`). Compare the key itself, or filter in
  `WHERE`.
- A predicate naming a column that is **neither** the `GROUP BY` key **nor** an aggregate is refused
  (`HAVING UPPER(name) = 'X'` when the grouping is by `city`) — with or without a `GROUP BY`.
- ⚠️ An `OR` whose branches need **different stages** is refused, because Elasticsearch applies
  the stages one inside the other, which is a conjunction:
  - different MECHANISMS — a group filter (`bucket_selector`), a key filter (`terms`) and a nested
    filter: `HAVING COUNT(*) > 1 OR city = 'Paris'`;
  - different GROUPING KEYS — the two `terms` aggregations are NESTED, so
    `GROUP BY country, city HAVING country = 'FR' OR city = 'Paris'` would return only the groups
    matching BOTH. An `OR` on ONE key, within one mechanism, is supported subject to the table
    above; the corresponding `AND` is always fine, because the nesting IS the conjunction.
- An `AND` across mechanisms is fine — each stage applies its own half.
- A group whose compared metric has no value (for instance `MAX(age)` over a group whose documents
  all lack `age`) never passes a `HAVING` comparison, in either direction: the generated filter
  script null-checks every metric before comparing it. For such a group `IS NULL` is true and
  `IS NOT NULL` is false (written `ISNULL(MAX(age))` and `ISNOTNULL(MAX(age))` in `HAVING`).

> 🔴 **Changed in 0.24.0 — an empty group's aggregate is NULL in `HAVING`.** Before 0.24.0, `<>`, a
> negated comparison (`NOT MAX(age) = 30`, `NOT BETWEEN`, `NOT IN`), `IS [NOT] NULL` and `COALESCE`
> over the `MIN`, `MAX`, `AVG` or a percentile of a group with no value could keep or drop the wrong groups:
> the group filter was handed Elasticsearch's placeholder for an aggregate over no value (`NaN`),
> not NULL.

#### GROUP BY without an aggregate

`GROUP BY` with no aggregate in the `SELECT` list returns **one row per group** — the SQL-standard
spelling of `DISTINCT`:

```sql
SELECT profile.city AS city
FROM dql_users
GROUP BY profile.city;
```

- Positions are supported: `GROUP BY 1` and `ORDER BY 1` name the first `SELECT` item. A position
  that names nothing (`0`, a negative, or one past the end of the `SELECT` list) is a parse error
  naming the position. A column genuinely named with digits is unaffected, and a column named `1`
  is addressed as `` GROUP BY `1` ``.
- `SELECT` aliases are supported: `SELECT country AS pays ... GROUP BY pays` groups by `country`,
  and `ORDER BY pays` / `HAVING pays <> 'x'` address that group by the same name.
- A constant is legal beside a `GROUP BY` and carries its value on every row
  (`SELECT category, 2 AS flag FROM t GROUP BY category`) — it does not vary within a group, so it
  needs no grouping.
- Grouping **by** a constant is also legal and means exactly one group
  (`SELECT 2 AS flag ... GROUP BY flag`, or the equivalent position `... GROUP BY 1`). It needs a
  `SELECT` alias, because the alias is the only name that group can be given.
- `LIMIT` on a `GROUP BY` bounds the number of **groups**, not the number of rows — it is pushed
  down as the Elasticsearch `terms` size. On a multi-column `GROUP BY` it bounds **each level**, so
  the row count can exceed it.
- `OFFSET` is **not supported** with `GROUP BY` and is rejected: group results are not paginated.
- With no `LIMIT`, every level is sized at Elasticsearch's `search.max_buckets` ceiling (65,536):
  a grouping wider than that fails loudly rather than truncating silently.

---

## Parent-Level Aggregations on Nested Arrays

The SQL Gateway supports computing aggregations **over nested arrays** (e.g., `ARRAY<STRUCT>`) while keeping one row per parent document (the original nested array is preserved).

This pattern:

- reads the nested array (`JOIN UNNEST`)
- computes aggregations per parent document (`PARTITION BY parent_id`)
- **returns one row per parent**
- **preserves the original nested array**
- **adds the aggregated value as a top-level field**

**Example**

```sql
SELECT
  o.id,
  o.items,
  SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
FROM dql_orders o
JOIN UNNEST(o.items) AS items
WHERE items.quantity >= 1
ORDER BY o.id ASC;
```

**Result**

```json
[
  {
    "id": 1,
    "items": [
      {"product": "A", "quantity": 2, "price": 10.0},
      {"product": "B", "quantity": 1, "price": 20.0}
    ],
    "total_price": 40.0
  },
  {
    "id": 2,
    "items": [
      {"product": "C", "quantity": 3, "price": 5.0}
    ],
    "total_price": 15.0
  }
]
```

### Notes

- This is **not** a standard SQL window function (which would return one row per item).
- This is **not** an Elasticsearch nested aggregation (which would not return the items).
- This is a **hybrid parent-level aggregation**, unique to the SQL Gateway.

---

## Window Functions

Window functions operate over a logical window of rows defined by `OVER (PARTITION BY ... ORDER BY ...)`.

Supported window functions include:

- `SUM(expr) OVER (PARTITION BY ...)`
- `AVG(expr) OVER (PARTITION BY ...)`
- `MIN(expr) OVER (PARTITION BY ...)` / `MAX(expr) OVER (PARTITION BY ...)`
- `COUNT(expr) OVER (PARTITION BY ...)`, including `COUNT(DISTINCT expr) OVER (PARTITION BY ...)`
- `STDDEV(expr) OVER (PARTITION BY ...)` and its `_SAMP` / `_POP` variants
- `VARIANCE(expr) OVER (PARTITION BY ...)` and its `_SAMP` / `_POP` variants
- `FIRST_VALUE(expr) OVER (...)`
- `LAST_VALUE(expr) OVER (...)`
- `ARRAY_AGG(expr) OVER (...)`
- `ROW_NUMBER() OVER ([PARTITION BY ...] ORDER BY ...)`
- `RANK() OVER ([PARTITION BY ...] ORDER BY ...)`
- `DENSE_RANK() OVER ([PARTITION BY ...] ORDER BY ...)`
- `PERCENTILE_CONT(p) OVER (PARTITION BY ... ORDER BY column)` and `PERCENTILE_DISC(p) OVER (PARTITION BY ... ORDER BY column)`

`PERCENTILE_CONT` / `PERCENTILE_DISC` accept four equivalent spellings — the `OVER (... ORDER BY column)`
form above, `WITHIN GROUP (ORDER BY column)`, the two combined, and the `(column, p)` shorthand. All four
**normalize to the same canonical rendering**, `PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY column) [OVER
(PARTITION BY ...)]`, so a statement round-tripped through the engine comes back in that form rather than
the one you typed. See [Percentiles](#percentiles--percentile_cont--percentile_disc) below for the spellings themselves, and [Aggregate Functions](functions_aggregate.md#function-percentile_cont--percentile_disc) for the full reference.

#### Basic window example

```sql
SELECT
  product,
  customer,
  amount,
  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product
FROM dql_sales
ORDER BY product, ts;
```

#### ROW_NUMBER / RANK / DENSE_RANK (ranking windows)

`ORDER BY` is REQUIRED inside `OVER` for ranking functions (ANSI). `PARTITION BY`
is optional — when absent, the entire result set is treated as one partition.

```sql
SELECT name, salary,
  ROW_NUMBER() OVER (PARTITION BY department ORDER BY salary DESC) AS rn,
  RANK()       OVER (PARTITION BY department ORDER BY salary DESC) AS r,
  DENSE_RANK() OVER (PARTITION BY department ORDER BY salary DESC) AS dr
FROM emp;
```

Tie semantics:

- `ROW_NUMBER` — sequential within partition; no ties recognized (1, 2, 3, 4, …)
- `RANK` — ties share rank, next rank skips (1, 2, 2, 4, …)
- `DENSE_RANK` — ties share rank, next rank does NOT skip (1, 2, 2, 3, …)

##### Top-N per group (push-down via `LIMIT` inside `OVER`)

Inline `LIMIT N` inside the OVER clause to limit the number of rows ranked per
partition. The engine pushes `N` down to the underlying Elasticsearch
`top_hits.size` parameter so only the top-N rows per partition are
materialised:

```sql
SELECT name, salary,
  RANK() OVER (PARTITION BY department ORDER BY salary DESC LIMIT 3) AS r
FROM emp;
```

Without an explicit `LIMIT`, `top_hits.size` defaults to 100 — the
Elasticsearch `index.max_inner_result_window` default. For larger partitions
either supply `LIMIT N` inline or raise the index setting.

#### FIRST_VALUE / LAST_VALUE / ARRAY_AGG

```sql
SELECT
  product,
  customer,
  amount,
  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product,
  FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS first_amount,
  LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS last_amount,
  ARRAY_AGG(amount) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS amounts_array
FROM dql_sales
ORDER BY product, ts;
```

Notes:

- `PARTITION BY` defines the grouping key.
- `ORDER BY` inside `OVER` defines the window ordering.
- `LIMIT` inside `ARRAY_AGG` restricts the collected values.
- Frame clauses (`ROWS BETWEEN ...`) are not exposed; the engine uses a default frame per function semantics.

---

## Functions

The SQL Gateway provides a rich set of SQL functions covering:

- numeric and trigonometric operations
- string manipulation
- date and time extraction, arithmetic, formatting and parsing
- geospatial functions
- conditional expressions
- type conversion

All functions operate on Elasticsearch documents and are evaluated by the SQL engine.

---

#### Numeric & Trigonometric

##### **Arithmetic:**

| Function      | Description          |
|---------------|----------------------|
| `ABS(x)`      | Absolute value       |
| `CEIL(x)`     | Round up             |
| `FLOOR(x)`    | Round down           |
| `ROUND(x, n)` | Round to n decimals  |
| `SQRT(x)`     | Square root          |
| `POW(x, y)`   | Power                |
| `EXP(x)`      | Exponential          |
| `LOG(x)`      | Natural logarithm    |
| `LOG10(x)`    | Base-10 logarithm    |
| `SIGN(x)`     | Sign of x (−1, 0, 1) |


##### **Trigonometric:**

| Function      | Description                      |
|---------------|----------------------------------|
| `SIN(x)`      | Sine                             |
| `COS(x)`      | Cosine                           |
| `TAN(x)`      | Tangent                          |
| `ASIN(x)`     | Arc-sine                         |
| `ACOS(x)`     | Arc-cosine                       |
| `ATAN(x)`     | Arc-tangent                      |
| `ATAN2(y, x)` | Arc-tangent of y/x with quadrant |
| `PI()`        | π constant                       |
| `RADIANS(x)`  | Degrees → radians                |
| `DEGREES(x)`  | Radians → degrees                |

**Example**

```sql
SELECT id,
       ABS(age) AS abs_age,
       SQRT(age) AS sqrt_age,
       POW(age, 2) AS pow_age,
       LOG(age) AS log_age,
       SIN(age) AS sin_age,
       ATAN2(age, 10) AS atan2_val
FROM dql_users;
```

---

#### String

##### **Manipulation:**


| Function                     | Description                   |
|------------------------------|-------------------------------|
| `CONCAT(a, b, ...)`          | Concatenate strings           |
| `SUBSTRING(str, start, len)` | Extract substring             |
| `LOWER(str)`                 | Lowercase                     |
| `UPPER(str)`                 | Uppercase                     |
| `TRIM(str)`                  | Trim both sides               |
| `LTRIM(str)`                 | Trim left                     |
| `RTRIM(str)`                 | Trim right                    |
| `LENGTH(str)`                | String length                 |
| `REPLACE(str, from, to)`     | Replace substring             |
| `LEFT(str, n)`               | Left n chars                  |
| `RIGHT(str, n)`              | Right n chars                 |
| `REVERSE(str)`               | Reverse string                |
| `POSITION(substr IN str)`    | 1-based position of substring |

##### **Pattern matching:**

| Function                     | Description                                         |
|------------------------------|-----------------------------------------------------|
| `REGEXP_LIKE(str, pattern)`  | True if regex matches                               |
| `MATCH(str) AGAINST (query)` | Full-text match (backed by ES query_string / match) |

**Example:**

```sql
SELECT id,
       CONCAT(name.raw, '_suffix') AS name_concat,
       SUBSTRING(name.raw, 1, 2) AS name_sub,
       LOWER(name.raw) AS name_lower,
       LTRIM(name.raw) AS name_ltrim,
       POSITION('o' IN name.raw) AS pos_o,
       REGEXP_LIKE(name.raw, '.*o.*') AS has_o
FROM dql_users
ORDER BY id ASC;
```

---

#### Date & Time

##### **Current :**

| Function                                  | Description                 |
|-------------------------------------------|-----------------------------|
| `CURRENT_DATE`                            | Current date (UTC)          |
| `TODAY()`                                 | Alias for CURRENT_DATE      |
| `CURRENT_TIMESTAMP` \| `CURRENT_DATETIME` | Current timestamp (UTC)     |
| `NOW()`                                   | Alias for CURRENT_TIMESTAMP |
| `CURRENT_TIME`                            | Current time (UTC)          |

##### **Extraction:**

| Function          | Description  |
|-------------------|--------------|
| `YEAR(date)`      | Year         |
| `MONTH(date)`     | Month        |
| `DAY(date)`       | Day of month |
| `WEEKDAY(date)`   | Day of week  |
| `YEARDAY(date)`   | Day of year  |
| `HOUR(ts)`        | Hour         |
| `MINUTE(ts)`      | Minute       |
| `SECOND(ts)`      | Second       |
| `MILLISECOND(ts)` | Millisecond  |
| `MICROSECOND(ts)` | Microsecond  |
| `NANOSECOND(ts)`  | Nanosecond   |

##### **EXTRACT:**

```sql
EXTRACT(unit FROM date_or_timestamp)
```

Supported units include: `YEAR`, `MONTH`, `DAY`, `HOUR`, `MINUTE`, `SECOND`, etc.

**Example:**

```sql
SELECT id,
       EXTRACT(YEAR FROM birthdate) AS year_b,
       EXTRACT(MONTH FROM birthdate) AS month_b
FROM dql_users;
```

##### **Arithmetic:**

| Function                            | Description                                                                                   |
|-------------------------------------|-----------------------------------------------------------------------------------------------|
| `DATE_ADD(date, INTERVAL n unit)`   | Add interval                                                                                  |
| `DATE_SUB(date, INTERVAL n unit)`   | Subtract interval                                                                             |
| `DATETIME_ADD(ts, INTERVAL n unit)` | Add interval to timestamp                                                                     |
| `DATETIME_SUB(ts, INTERVAL n unit)` | Subtract interval from timestamp                                                              |
| `DATE_DIFF(date1, date2, unit)`     | `date1 - date2` in units: the calendar boundaries crossed (UTC, weeks start on Monday)        |
| `DATE_TRUNC(date, unit)`            | Truncate to unit                                                                              |

##### **Formatting & parsing:**

| Function                       | Description                 |
|--------------------------------|-----------------------------|
| `DATE_FORMAT(ts, pattern)`     | Format date as string       |
| `DATE_PARSE(str, pattern)`     | Parse string into date      |
| `DATETIME_FORMAT(ts, pattern)` | Format timestamp as string  |
| `DATETIME_PARSE(str, pattern)` | Parse string into timestamp |

**Supported MySQL-style Date/Time Patterns**

##### **Special functions:**

| Function               | Description                   |
|------------------------|-------------------------------|
| `LAST_DAY(date)`       | Last day of month             |
| `EPOCHDAY(date)`       | Days since epoch (1970-01-01) |
| `OFFSET_SECONDS(date)` | Epoch seconds                 |

**Example:**

```sql
SELECT id,
       YEAR(CURRENT_DATE) AS current_year,
       MONTH(CURRENT_DATE) AS current_month,
       DAY(CURRENT_DATE) AS current_day,
       YEAR(birthdate) AS year_b,
       DATE_DIFF(CURRENT_DATE, birthdate, YEAR) AS diff_years,
       DATE_TRUNC(birthdate, MONTH) AS trunc_month,
       DATETIME_FORMAT(birthdate, '%Y-%m-%d') AS birth_str
FROM dql_users;
```

---

#### Geospatial

##### **POINT:**

```sql
POINT(latitude, longitude)
```

##### **ST_DISTANCE:**

```sql
ST_DISTANCE(location, POINT(48.8566, 2.3522))
```

Example:

```sql
CREATE TABLE IF NOT EXISTS dql_geo (
  id INT NOT NULL,
  location GEO_POINT,
  PRIMARY KEY (id)
);

SELECT id,
       ST_DISTANCE(location, POINT(48.8566, 2.3522)) AS dist_paris
FROM dql_geo;
```

---

#### Conditional

##### CASE WHEN

```sql
CASE
  WHEN condition THEN value
  [WHEN condition2 THEN value2]
  [ELSE default]
END
```

##### COALESCE

```sql
COALESCE(a, b, c)
```

Returns the first non-null value.

##### NULLIF

```sql
NULLIF(a, b)
```

Returns NULL if `a = b`, otherwise `a`.

##### GREATEST / LEAST

```sql
GREATEST(e1, e2, ...)
LEAST(e1, e2, ...)
```

`GREATEST` returns the largest non-null value among the given expressions; `LEAST`
returns the smallest. The arguments are all numeric, or all dates and timestamps (a `DATE`
compares as the start of its day, UTC; the result is a `DATE` over dates and a `TIMESTAMP` when
they mix). Over numbers the result is the common type of all the arguments on every row: the
widest whole type when every argument is a whole number, a `DOUBLE` as soon as one is fractional.
NULL arguments are ignored (ANSI semantics); the result is NULL only
when every argument is NULL. Both are emitted as Painless ternary chains — over whole numbers the
winner is chosen by a comparison, over fractional numbers by `Math.max` / `Math.min`, over dates
over the epoch milliseconds each one denotes. They are
conditional functions, not aggregates — `GREATEST(...) OVER (...)` is not supported — but over
aggregates they are computed per group (`GREATEST(MAX(a), MAX(b))` beside a `GROUP BY`).

```sql
SELECT GREATEST(price_us, price_eu, price_uk) AS max_price FROM products;
SELECT LEAST(0, base_price - rebate)          AS net       FROM orders;
```

**Example:**

```sql
SELECT id,
       CASE
         WHEN age >= 50 THEN 'senior'
         WHEN age >= 30 THEN 'adult'
         ELSE 'young'
       END AS age_group,
       COALESCE(name, 'unknown') AS safe_name
FROM dql_users;
```

---

#### Type Conversion

##### CAST

```sql
CAST(value AS TYPE)
```

##### TRY_CAST

Returns NULL instead of failing on invalid conversion.

```sql
TRY_CAST('123' AS INT)
```

##### SAFE_CAST

Alias for TRY_CAST.

##### PostgreSQL-style operator

```sql
value::TYPE
```

**Example:**

```sql
SELECT id,
       age::BIGINT AS age_bigint,
       CAST(age AS DOUBLE) AS age_double,
       TRY_CAST('123' AS INT) AS try_cast_ok,
       SAFE_CAST('abc' AS INT) AS safe_cast_null
FROM dql_users;
```

---

## Scroll & Pagination

For large result sets, the Gateway uses Elasticsearch scroll or search-after mechanisms depending on backend capabilities ([Scroll Search](../client/scroll.md)).

Notes:

- `LIMIT` and `OFFSET` are applied by the SQL engine after retrieving documents from Elasticsearch
- Deep pagination may require scroll
- When using `search_after`, an explicit `ORDER BY` clause is required for deterministic pagination
- Without `ORDER BY`, result ordering is not guaranteed

---

## Version Compatibility

| Feature                        | ES6 | ES7 | ES8 | ES9 |
|--------------------------------|-----|-----|-----|-----|
| Basic SELECT                   | ✔   | ✔   | ✔   | ✔   |
| Nested fields                  | ✔   | ✔   | ✔   | ✔   |
| UNION ALL                      | ✔   | ✔   | ✔   | ✔   |
| Cross-index JOINs              | ✔   | ✔   | ✔   | ✔   |
| JOIN UNNEST                    | ✔   | ✔   | ✔   | ✔   |
| Aggregations                   | ✔   | ✔   | ✔   | ✔   |
| Parent-level nested array aggs | ✔   | ✔   | ✔   | ✔   |
| Window functions               | ✔   | ✔   | ✔   | ✔   |
| Geospatial functions           | ✔   | ✔   | ✔   | ✔   |
| Date/time functions            | ✔   | ✔   | ✔   | ✔   |
| String / math functions        | ✔   | ✔   | ✔   | ✔   |

---

## Limitations

For the full picture of what works in R1, what's coming in R2a/R2b, and BI-tool workarounds, see [Known Limitations & Roadmap](known_limitations.md).

Even though the DQL engine is powerful, some SQL features are not (yet) supported:

- Cross-index JOINs (`INNER` / `LEFT` / `RIGHT` / `FULL OUTER`) are supported across indices and clusters — see [Cross-Index JOIN](joins.md). `JOIN UNNEST` on `ARRAY<STRUCT>` is the single-index nested form, handled natively inside one index.
- No correlated subqueries
- No arbitrary subqueries in `SELECT` or `WHERE` (except `INSERT ... AS SELECT` in DML)
- No `GROUPING SETS`, `CUBE`, `ROLLUP`
- No `DISTINCT ON`
- No explicit window frame clauses (`ROWS BETWEEN ...`)

These constraints keep the translation to Elasticsearch efficient and predictable.

---

## SHOW TABLES

```sql
SHOW TABLES [LIKE 'pattern'];
```

Returns a list of all tables with summary information (name, type, primary key, partitioning).

May be filtered using `LIKE` with SQL wildcard `%`.

**Example:*

```sql
CREATE TABLE IF NOT EXISTS show_users (
  id INT NOT NULL,
  name VARCHAR FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  age INT DEFAULT 0,
  PRIMARY KEY (id)
);

SHOW TABLES LIKE 'show_%';
```

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| show_users | TABLE   | id |             |
📊 1 row(s) (7ms)

---

## SHOW TABLE

Returns:

- schema summary
- primary key
- partitioning
- settings
- mappings
- ddl

```sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL COMMENT 'user identifier',
  name VARCHAR FIELDS(raw Keyword COMMENT 'sortable') DEFAULT 'anonymous' OPTIONS (analyzer = 'french', search_analyzer = 'french'),
  birthdate DATE,
  age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE)),
  ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
  profile STRUCT FIELDS(
    bio VARCHAR,
    followers INT,
    join_date DATE,
    seniority INT SCRIPT AS (DATEDIFF(CURRENT_DATE, profile.join_date, DAY))
  ) COMMENT 'user profile',
  PRIMARY KEY (id)
) PARTITION BY birthdate (MONTH), OPTIONS (mappings = (dynamic = false));

SHOW TABLE users;
```

📋 Table: users [TABLE]

| Field             | Type      | Null | Key | Default           | Comment         | Script                                          | Extra                                             |
|-------------------|-----------|------|-----|-------------------|-----------------|-------------------------------------------------|---------------------------------------------------|
| age               | INT       | yes  |     | NULL              |                 | TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE)    | ()                                                |
| birthdate         | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| id                | INT       | no   | PRI | NULL              | user identifier |                                                 | ()                                                |
| ingested_at       | TIMESTAMP | yes  |     | _ingest.timestamp |                 |                                                 | ()                                                |
| name              | VARCHAR   | yes  |     | anonymous         |                 |                                                 | (analyzer = "french", search_analyzer = "french") |
| name.raw          | KEYWORD   | yes  |     | NULL              | sortable        |                                                 | ()                                                |
| profile           | STRUCT    | yes  |     | NULL              | user profile    |                                                 | ()                                                |
| profile.seniority | INT       | yes  |     | NULL              |                 | DATE_DIFF(CURRENT_DATE, profile.join_date, DAY) | ()                                                |
| profile.join_date | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.followers | INT       | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.bio       | VARCHAR   | yes  |     | NULL              |                 |                                                 | ()                                                |

🔑 PRIMARY KEY id
📅 PARTITION BY birthdate (MONTH)

⚙️ Settings:
default_pipeline: 'users_ddl_default_pipeline'


🗺️ Mappings:
dynamic: false
_meta: (primary_key = ('id'), partition_by = (column = 'birthdate', granularity = 'M'), columns = (...), type = 'regular', materialized_views = ())


📝 DDL:
```sql
CREATE OR REPLACE TABLE users (
	age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE)),
	birthdate DATE,
	id INT NOT NULL COMMENT 'user identifier',
	ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
	name VARCHAR FIELDS (
		raw KEYWORD COMMENT 'sortable'
	) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french"),
	profile STRUCT FIELDS (
		seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY)),
		join_date DATE,
		followers INT,
		bio VARCHAR
	) COMMENT 'user profile',
	PRIMARY KEY (id)
)
PARTITION BY birthdate (MONTH),
OPTIONS = (
	mappings = (dynamic = false, _meta = (primary_key = ["id"], partition_by = (column = "birthdate", granularity = "M"), columns = (...), type = "regular", materialized_views = [])),
	settings = (default_pipeline = "users_ddl_default_pipeline")
)
```

---

## SHOW CREATE TABLE

Returns the full, normalized DDL statement used to create the table, including all fields, types, options, comments, and scripts.

```sql
SHOW CREATE TABLE users;
```

```sql
CREATE OR REPLACE TABLE users (
	age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE)),
	birthdate DATE,
	id INT NOT NULL COMMENT 'user identifier',
	ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
	name VARCHAR FIELDS (
		raw KEYWORD COMMENT 'sortable'
	) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french"),
	profile STRUCT FIELDS (
		seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY)),
		join_date DATE,
		followers INT,
		bio VARCHAR
	) COMMENT 'user profile',
	PRIMARY KEY (id)
)
PARTITION BY birthdate (MONTH),
OPTIONS = (
	mappings = (dynamic = false, _meta = (primary_key = ["id"], partition_by = (column = "birthdate", granularity = "M"), columns = (...), type = "regular", materialized_views = [])),
	settings = (default_pipeline = "users_ddl_default_pipeline")
)
```

## DESCRIBE TABLE

```sql
DESCRIBE TABLE users;
```

Returns the **normalized SQL schema**, including :

- fields
- types
- nulls
- keys
- defaults
- comments
- scripts
- STRUCT fields
- options

| Field             | Type      | Null | Key | Default           | Comment         | Script                                          | Extra                                             |
|-------------------|-----------|------|-----|-------------------|-----------------|-------------------------------------------------|---------------------------------------------------|
| age               | INT       | yes  |     | NULL              |                 | TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE)    | ()                                                |
| birthdate         | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| id                | INT       | no   | PRI | NULL              | user identifier |                                                 | ()                                                |
| ingested_at       | TIMESTAMP | yes  |     | _ingest.timestamp |                 |                                                 | ()                                                |
| name              | VARCHAR   | yes  |     | anonymous         |                 |                                                 | (analyzer = "french", search_analyzer = "french") |
| name.raw          | KEYWORD   | yes  |     | NULL              | sortable        |                                                 | ()                                                |
| profile           | STRUCT    | yes  |     | NULL              | user profile    |                                                 | ()                                                |
| profile.seniority | INT       | yes  |     | NULL              |                 | DATE_DIFF(CURRENT_DATE, profile.join_date, DAY) | ()                                                |
| profile.join_date | DATE      | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.followers | INT       | yes  |     | NULL              |                 |                                                 | ()                                                |
| profile.bio       | VARCHAR   | yes  |     | NULL              |                 |                                                 | ()                                                |

---

## SHOW PIPELINES

```sql
SHOW PIPELINES;
```

**Description**

- Returns a list of all user-defined pipelines with summary information (name, number of processors)

**Example**

```sql
SHOW PIPELINES;
```

| name                                             | processors_count |
|--------------------------------------------------|------------------|
| users_alter4_ddl_default_pipeline                | 1                |
| user_pipeline                                    | 6                |
| metrics-apm.transaction@default-pipeline         | 3                |
| users_alter6_ddl_default_pipeline                | 1                |
| tmp_truncate_ddl_default_pipeline                | 1                |
| users_alter5_ddl_default_pipeline                | 3                |
| logs@default-pipeline                            | 2                |
| dql_users_ddl_default_pipeline                   | 1                |
| apm@pipeline                                     | 4                |
| logs-apm.error@default-pipeline                  | 3                |
| metrics-apm.service_transaction@default-pipeline | 3                |
| users_cr_ddl_default_pipeline                    | 1                |
| users_alter2_ddl_default_pipeline                | 1                |
| metrics-apm.internal@default-pipeline            | 6                |
| traces-apm.rum@default-pipeline                  | 3                |
| metrics-apm.app@default-pipeline                 | 3                |
| users_alter1_ddl_default_pipeline                | 2                |
| tmp_drop_ddl_default_pipeline                    | 1                |
| dql_sales_ddl_default_pipeline                   | 1                |
| ent-search-generic-ingestion                     | 6                |
| logs@json-message                                | 4                |
| users_alter3_ddl_default_pipeline                | 1                |
| dml_users_ddl_default_pipeline                   | 1                |
| traces-apm@default-pipeline                      | 3                |
| metrics-apm@pipeline                             | 3                |
| users_alter8_ddl_default_pipeline                | 5                |
| dml_chain_ddl_default_pipeline                   | 1                |
| users_ddl_default_pipeline                       | 6                |
| copy_into_test_ddl_default_pipeline              | 1                |
| accounts_src_ddl_default_pipeline                | 1                |
| users_alter7_ddl_default_pipeline                | 1                |
| dml_accounts_ddl_default_pipeline                | 1                |
| reindex-data-stream-pipeline                     | 1                |
| behavioral_analytics-events-final_pipeline       | 9                |
| logs@json-pipeline                               | 4                |
| logs-default-pipeline                            | 2                |
| dql_orders_ddl_default_pipeline                  | 1                |
| dml_logs_ddl_default_pipeline                    | 1                |
| search-default-ingestion                         | 6                |
| accounts_ddl_default_pipeline                    | 1                |
| dql_geo_ddl_default_pipeline                     | 1                |
| show_users_ddl_default_pipeline                  | 2                |
| logs-apm.app@default-pipeline                    | 3                |
| traces-apm@pipeline                              | 7                |
| desc_users_ddl_default_pipeline                  | 2                |
| metrics-apm.service_summary@default-pipeline     | 3                |
| metrics-apm.service_destination@default-pipeline | 3                |
📊 47 row(s) (10ms)

---

## SHOW PIPELINE

```sql
SHOW PIPELINE pipeline_name;
```

**Description**

- Returns a high‑level view of the pipeline processors

**Example**

```sql
SHOW PIPELINE user_pipeline;
```

🔄 Pipeline: user_pipeline

Processors: (6)
| processor_type  | description                                                                       | field             | ignore_failure | options                                                                                                                                                                                                                                                                                             |
|-----------------|-----------------------------------------------------------------------------------|-------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| set             | DEFAULT 'anonymous'                                                               | name              | yes            | (value = "anonymous", if = "ctx.name == null")                                                                                                                                                                                                                                                      |
| script          | age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE))                  | age               | yes            | (lang = "painless", source = "def param1 = ctx.birthdate; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.age = (param1 == null) ? null : Long.valueOf(ChronoUnit.YEARS.between(param1, param2))")                       |
| set             | DEFAULT _ingest.timestamp                                                         | ingested_at       | yes            | (value = "_ingest.timestamp", if = "ctx.ingested_at == null")                                                                                                                                                                                                                                       |
| script          | profile.seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY)) | profile.seniority | yes            | (lang = "painless", source = "def param1 = ctx.profile?.join_date; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (param1 == null) ? null : Long.valueOf(ChronoUnit.DAYS.between(param1, param2))") |
| date_index_name | PARTITION BY birthdate (MONTH)                                                    | birthdate         | yes            | (date_rounding = "M", date_formats = ["yyyy-MM"], index_name_prefix = "users-")                                                                                                                                                                                                                     |
| set             | PRIMARY KEY (id)                                                                  | _id               | no             | (value = "{{id}}", ignore_empty_value = false)                                                                                                                                                                                                                                                      |

📝 DDL:
```sql
CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
	SET(
		description = "DEFAULT 'anonymous'", 
		field = "name", 
		ignore_failure = true, 
		value = "anonymous", 
		if = "ctx.name == null"
	), 
	SCRIPT(
		description = "age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	SET(
		description = "DEFAULT _ingest.timestamp", 
		field = "ingested_at", 
		ignore_failure = true, 
		value = "_ingest.timestamp", 
		if = "ctx.ingested_at == null"
	), 
	SCRIPT(
		description = "profile.seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	DATE_INDEX_NAME(
		description = "PARTITION BY birthdate (MONTH)", 
		field = "birthdate", 
		date_rounding = "M", 
		date_formats = ["yyyy-MM"], 
		index_name_prefix = "users-", 
		ignore_failure = true
	), 
	SET(
		description = "PRIMARY KEY (id)", 
		field = "_id", 
		value = "{{id}}", 
		ignore_failure = false, 
		ignore_empty_value = false)
	)
)
```

---

## SHOW CREATE PIPELINE

```sql
SHOW CREATE PIPELINE pipeline_name;
```

**Description**

- Returns the full, normalized DDL statement used to create the pipeline, including all processors, options, and flags.

**Example**

```sql
SHOW CREATE PIPELINE user_pipeline;
```

```sql
CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
	SET(
		description = "DEFAULT 'anonymous'", 
		field = "name", 
		ignore_failure = true, 
		value = "anonymous", 
		if = "ctx.name == null"
	), 
	SCRIPT(
		description = "age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	SET(
		description = "DEFAULT _ingest.timestamp", 
		field = "ingested_at", 
		ignore_failure = true, 
		value = "_ingest.timestamp", 
		if = "ctx.ingested_at == null"
	), 
	SCRIPT(
		description = "profile.seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY))", 
		lang = "painless", 
		source = "...", 
		ignore_failure = true
	), 
	DATE_INDEX_NAME(
		description = "PARTITION BY birthdate (MONTH)", 
		field = "birthdate", 
		date_rounding = "M", 
		date_formats = ["yyyy-MM"], 
		index_name_prefix = "users-", 
		ignore_failure = true
	), 
	SET(
		description = "PRIMARY KEY (id)", 
		field = "_id", 
		value = "{{id}}", 
		ignore_failure = false, 
		ignore_empty_value = false)
	)
)
```

---

## DESCRIBE PIPELINE

```sql
DESCRIBE PIPELINE pipeline_name;
```

**Description**

- Returns the full, normalized definition of the pipeline:
	- processors in execution order
	- full configuration of each processor (`SET`, `SCRIPT`, `REMOVE`, `RENAME`, `DATE_INDEX_NAME`, etc.)
	- flags such as `ignore_failure`, `if`, `description`

**Example**

```sql
DESCRIBE PIPELINE user_pipeline;
```

| processor_type  | description                                                                       | field             | ignore_failure | options                                                                                                                                                                                                                                                                                             |
|-----------------|-----------------------------------------------------------------------------------|-------------------|----------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| set             | DEFAULT 'anonymous'                                                               | name              | yes            | (value = "anonymous", if = "ctx.name == null")                                                                                                                                                                                                                                                      |
| script          | age INT SCRIPT AS (TIMESTAMPDIFF(YEAR, birthdate, CURRENT_DATE))                  | age               | yes            | (lang = "painless", source = "def param1 = ctx.birthdate; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.age = (param1 == null) ? null : Long.valueOf(ChronoUnit.YEARS.between(param1, param2))")                       |
| set             | DEFAULT _ingest.timestamp                                                         | ingested_at       | yes            | (value = "_ingest.timestamp", if = "ctx.ingested_at == null")                                                                                                                                                                                                                                       |
| script          | profile.seniority INT SCRIPT AS (DATE_DIFF(CURRENT_DATE, profile.join_date, DAY)) | profile.seniority | yes            | (lang = "painless", source = "def param1 = ctx.profile?.join_date; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (param1 == null) ? null : Long.valueOf(ChronoUnit.DAYS.between(param1, param2))") |
| date_index_name | PARTITION BY birthdate (MONTH)                                                    | birthdate         | yes            | (date_rounding = "M", date_formats = ["yyyy-MM"], index_name_prefix = "users-")                                                                                                                                                                                                                     |
| set             | PRIMARY KEY (id)                                                                  | _id               | no             | (value = "{{id}}", ignore_empty_value = false)                                                                                                                                                                                                                                                      |
📊 6 row(s) (1ms)

---

## SHOW WATCHERS

```sql
SHOW WATCHERS;
```

Returns a list of all watchers with summary information (name, activation state, last execution time, ...).

**Example:**

```sql
CREATE OR REPLACE WATCHER my_watcher_interval AS
 EVERY 5 SECONDS
 FROM my_index WITHIN 1 MINUTE
 ALWAYS DO
 log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
 END;

CREATE OR REPLACE WATCHER my_watcher_cron AS
 AT SCHEDULE '* * * * * ?'
 WITH INPUTS search_data AS FROM my_index WITHIN 1 MINUTE, http_data AS GET "https://jsonplaceholder.typicode.com/todos/1" HEADERS ("Accept" = "application/json") TIMEOUT (connection = "5s", read = "10s")
 WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
 DO
 log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
 END;

SHOW WATCHERS;
```

| id                  | active | status  | status_emoji | severity | is_healthy | is_operational | last_checked | time_since_last_check_seconds | frequency_seconds | created_at               | execution_status | execution_status_emoji | execution_severity | overall_status | overall_status_emoji | overall_severity |
|---------------------|--------|---------|--------------|----------|------------|----------------|--------------|-------------------------------|-------------------|--------------------------|------------------|------------------------|--------------------|----------------|----------------------|------------------|
| my_watcher_interval | true   | Healthy | 🟢           | 0        | true       | true           | never        | -1                            | 5                 | 2026-02-11T12:11:02.542Z | NULL             | NULL                   | -1                 | Healthy        | 🟢                   | 0                |
| my_watcher_cron     | true   | Healthy | 🟢           | 0        | true       | true           | never        | -1                            | 1                 | 2026-02-11T12:11:02.816Z | NULL             | NULL                   | -1                 | Healthy        | 🟢                   | 0                |
📊 2 row(s) (9ms)

**Query watchers require Elasticsearch 7.11+.**

---

## SHOW WATCHER STATUS

```sql
SHOW WATCHER STATUS watcher_name;
```

Returns:
- Activation state (active/inactive)
- Last execution time
- Last condition met time
- Execution statistics

**Example:**

```sql
SHOW WATCHER STATUS auto_refresh_orders_with_customers_mv_enrich_policies;
```

| id                                                    | active | status  | status_emoji | severity | is_healthy | is_operational | last_checked             | time_since_last_check_seconds | frequency_seconds | created_at               | execution_status | execution_status_emoji | execution_severity | overall_status | overall_status_emoji | overall_severity |
|-------------------------------------------------------|--------|---------|--------------|----------|------------|----------------|--------------------------|-------------------------------|-------------------|--------------------------|------------------|------------------------|--------------------|----------------|----------------------|------------------|
| auto_refresh_orders_with_customers_mv_enrich_policies | true   | Healthy | 🟢           | 0        | true       | true           | 2026-02-11T10:28:20.581Z | 5                             | 8                 | 2026-02-11T10:28:12.174Z | Executed         | 🟢                     | 0                  | Healthy        | 🟢                   | 0                |
📊 1 row(s) (9ms)

---

## SHOW ENRICH POLICIES

```sql
SHOW ENRICH POLICIES;
```

Returns a list of all enrich policies with their configurations.

**Example:*

```sql
CREATE TABLE IF NOT EXISTS dql_users (
  id INT NOT NULL,
  name VARCHAR FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  age INT,
  birthdate DATE,
  profile STRUCT FIELDS(
    city VARCHAR OPTIONS (fielddata = true),
    followers INT
  )
);

CREATE OR REPLACE ENRICH POLICY my_policy
FROM dql_users
ON id
ENRICH name, profile.city
WHERE age > 10;

SHOW ENRICH POLICIES;

```

| name      | type  | indices   | match_field | enrich_fields     | query                                             |
|-----------|-------|-----------|-------------|-------------------|---------------------------------------------------|
| my_policy | match | dql_users | id          | name,profile.city | {"bool":{"filter":[{"range":{"age":{"gt":10}}}]}} |
📊 1 row(s) (4ms)


---

## SHOW ENRICH POLICY

```sql
SHOW ENRICH POLICY policy_name;
```

Returns policy details, including:
- Name
- Type
- Indices
- Match field
- Enrich fields
- Query criteria (if any)

**Example:**

```sql
SHOW ENRICH POLICY my_policy;
```

| name      | type  | indices   | match_field | enrich_fields     | query                                             |
|-----------|-------|-----------|-------------|-------------------|---------------------------------------------------|
| my_policy | match | dql_users | id          | name,profile.city | {"bool":{"filter":[{"range":{"age":{"gt":10}}}]}} |
📊 1 row(s) (4ms)

---

## SHOW CLUSTER NAME

```sql
SHOW CLUSTER NAME;
```

Returns the name of the Elasticsearch cluster. The cluster name is cached after the first call.

**Example:**

```sql
SHOW CLUSTER NAME;
```

| name           |
|----------------|
| docker-cluster |
📊 1 row(s) (3ms)

---

## SHOW LICENSE

```sql
SHOW LICENSE;
```

Returns the current license type, quota values, expiration date, and grace status.

**Columns returned:**

| Column | Description |
|--------|-------------|
| `license_type` | Current license tier (Community, Pro, Enterprise). Shows "(trial)" suffix for trial licenses, "(degraded)" suffix if degraded from a higher tier. |
| `trial` | `true` if the license is a Pro trial, `false` otherwise |
| `platform` | Platform scope of the current license key (PRODUCTION, STAGING, DEVELOPMENT, INTEGRATION). Defaults to "PRODUCTION" when not platform-scoped. |
| `max_materialized_views` | Maximum number of materialized views allowed, or "unlimited" |
| `max_clusters` | Maximum number of federated clusters allowed, or "unlimited" |
| `max_result_rows` | Maximum rows returned per query, or "unlimited" |
| `max_joins` | Maximum number of JOIN operations allowed per query, or "unlimited" |
| `expires_at` | License expiration timestamp, or "never" for Community |
| `days_remaining` | Days until expiration, or -1 for Community (no expiry) |
| `status` | "Active", or grace period details if expired |

**Example:**

```sql
SHOW LICENSE;
```

| license_type | trial | platform | max_materialized_views | max_clusters | max_result_rows | max_joins | expires_at | days_remaining | status |
|---|---|---|---|---|---|---|---|---|---|
| Community | false | PRODUCTION | 1 | 1 | 10000 | 2 | never | -1 | Active |
📊 1 row(s) (1ms)

---

## REFRESH LICENSE

```sql
REFRESH LICENSE;
```

Forces an immediate license refresh from the backend (API key fetch). Returns the previous and new tier information.

**Columns returned:**

| Column | Description |
|--------|-------------|
| `previous_tier` | License tier before refresh |
| `new_tier` | License tier after refresh |
| `trial` | `true` if the new license is a Pro trial, `false` otherwise |
| `expires_at` | New expiration timestamp |
| `status` | "Refreshed" on success, "Failed" on error |
| `message` | Error details (empty on success) |

**Example (no API key configured):**

```sql
REFRESH LICENSE;
```

| previous_tier | new_tier | trial | expires_at | status | message |
|---|---|---|---|---|---|
| Community | Community | false | never | Failed | License refresh is not supported in Community mode |
📊 1 row(s) (1ms)

> **Note:** Requires API key configuration. Without an API key, returns an informational failure message.

---

[Back to index](README.md)
