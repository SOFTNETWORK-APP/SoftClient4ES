[Back to index](README.md)

# Known Limitations & Roadmap

SoftClient4ES runs a large, practical subset of ANSI SQL on Elasticsearch — including cross-index JOINs that Elasticsearch itself cannot do. A few advanced constructs (subqueries, CTEs, set operators beyond `UNION ALL`) are not in the current release yet. This page tells you exactly what works **as of this release**, what's coming, and how to get unblocked today.

> Great for explicit JOIN SQL — full BI-tool subquery / CTE support is coming in the next release.

## Using a BI tool? Read this first

Two different things can stop a BI tool here, and it is worth separating them.

### One tool has a path nobody has walked yet

- **Power BI** — Power Query's generic connectors are ODBC and OData, never JDBC. The one candidate path
  is a generic Arrow Flight SQL ODBC driver pointed at the sidecar. It is **unproven**: nobody has
  connected it, so treat it as a lead to follow rather than a supported route.
  See [Power BI](../client/bi_tools.md).

### Some tools cannot connect at all — and that is on their side

- **Metabase** — no generic JDBC database type exists; anything not on Metabase's own driver list needs a
  community driver plugin, which is code nobody has written for SoftClient4ES. See [Metabase](../client/bi_tools.md).
- **Looker** — Looker connects only through drivers it maintains itself, and it allowlists JDBC parameters
  per dialect, so a customer-supplied driver cannot be introduced. This gap is **structural, not
  commercial** — a licence would not close it.

Neither is a gap we can close from our side: each needs either a change by the vendor or a driver plugin
that nobody has written.

*(Each blocker checked against the vendor's own connection documentation — Metabase, Microsoft Power Query
and Looker — on 2026-08-31 and 2026-09-01.)*

### Tools that connect, but generate SQL we do not accept yet

Some BI tools auto-generate nested SQL (subqueries / derived tables) even when your logical query has none.
Until the next release lands full subquery support, send **explicit JOIN SQL** instead of letting the tool
compose nested queries — where the tool lets you:

- **Apache Superset / DBeaver / Grafana** — you control the SQL. Write explicit JOINs for anything that
  would otherwise nest, and everything in **Works in this release** below is available to you.
- **Tableau** — connecting and browsing work; queries are the constrained part. Drag-and-drop worksheets
  quote and fully qualify every identifier, a form we do not accept yet, and **Custom SQL is not a way
  around it**: Tableau documents that it *"must wrap the custom SQL statement within a select statement"* (Tableau's Custom SQL
  documentation, checked 2026-09-01),
  which turns your query into a derived table. **Extract** mode narrows the exposure but does not remove
  it — the extract is still built by querying the source.
  See [Tableau](../client/bi_tools.md).

> **General rule:** prefer **explicit JOIN SQL** over tool-generated nested SQL. If you control the query, a
> cross-index JOIN is fully supported in the current release.

**Apache Superset** (dedicated dialect), **DBeaver**, and **Grafana** (via Arrow Flight SQL) are **Tested**.
**Tableau** is **Compatible** — the connection path works, but it is not yet in our formal regression suite.

## Works in this release

- **Cross-index JOINs**: `INNER` / `LEFT` / `RIGHT` / `FULL` / `CROSS`, plus `JOIN UNNEST` on nested arrays — something Elasticsearch cannot do natively. (See the [JOIN matrix walkthrough](joins.md) for the per-tier rows and worked examples.)
- **Aggregations** + `GROUP BY` / `HAVING`.
- **Analytical SQL**: `ROW_NUMBER` / `RANK` / `DENSE_RANK`; the `STDDEV` / `VARIANCE` family (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`); `PERCENTILE_CONT` / `PERCENTILE_DISC`; window aggregates and `FIRST_VALUE` / `LAST_VALUE` / `ARRAY_AGG` over `OVER (PARTITION BY …)`.
- **Conditionals & null handling**: `CASE` / `COALESCE` / `NULLIF` / `GREATEST` / `LEAST` / `ISNULL` / `ISNOTNULL`.
- `ORDER BY … NULLS FIRST | NULLS LAST`.
- `UNION ALL` (concatenate result sets — no de-duplication).
- `SELECT * EXCEPT(col, …)` — drop named columns from `SELECT *`. This is the BigQuery-style **column-exclusion** clause. It is **not** the `EXCEPT` set operator (see below).

## Not in this release (coming in the next release, Quarter 4 2026)

- **Subqueries**: scalar, `IN (SELECT …)`, `EXISTS (SELECT …)`, derived tables `FROM (SELECT …)`.
- **CTEs**: `WITH name AS (SELECT …)` — recursive and non-recursive.
- **Set operators**: `UNION` (with row de-duplication), `INTERSECT`, and the `EXCEPT` **set operator**. The `EXCEPT` set operator is **distinct from** the `SELECT * EXCEPT(cols)` column-exclusion clause above — that one works; the set operator does not.
- **Positional / tiling window functions**: `NTILE`, `LAG`, `LEAD` — not yet implemented; coming with the next release's analytical-SQL work. (Note: `PERCENTILE_CONT` / `PERCENTILE_DISC` — percentile *aggregates* — already work in the current release; the positional/tiling window functions are a different family.)

These arrive in the next release as a driver-side enhancement — single-cluster customers get them by upgrading the driver (JDBC / ADBC / sidecar), with no infrastructure change and no federation server required.

### What a not-yet-supported query looks like

A subquery in a `WHERE` clause is rejected by the parser today:

```sql
-- Not supported in the current release: subqueries are not yet implemented.
SELECT name
FROM employees
WHERE department_id IN (SELECT id FROM departments WHERE region = 'EU');
```

The parser rejects this — `IN` accepts only literal value lists today, not a nested `SELECT`. Rewrite it as an explicit JOIN (fully supported), or wait for the next release where the subquery form lands as-is.

## Quoted identifiers — residual limits

Quoted column names, aliases and **table names** work in both spellings — see
[Quoted identifiers](dql_statements.md#quoted-identifiers) and
[Qualified and quoted table names](dql_statements.md#qualified-and-quoted-table-names). Five things
they do **not** cover yet:

- **`INSERT`, `UPDATE`, `CREATE`, `DROP` and `ALTER` names are not quotable.**
  ``INSERT INTO `prod_eu`.dest``, `INSERT INTO "prod_eu".dest`, ``UPDATE `orders` SET …`` and
  `CREATE TABLE "dest" ("c" INTEGER)` are all rejected — and so are quoted **column** names in
  those statements (`UPDATE tbl SET "a" = 1`). `SELECT` and `DELETE` are unaffected, because both
  route through the `FROM` table surface. This is the next piece of quoting work; until it lands,
  send DML/DDL names bare. The ``INSERT INTO `prod_eu`.dest`` / ``CREATE TABLE `prod_eu`.dest``
  examples in [joins.md](joins.md) belong to that gap; the ``FROM `prod_us`.orders`` ones do not —
  they work.

- **Quoting each part of a dotted index name splits it.** ``FROM `logs-2025`.`03` `` reads index
  `03` under the qualifier `logs-2025`, because a quoted part followed by a dot is a qualifier by
  definition. Write the whole name as one lexeme instead — ``FROM `logs-2025.03` `` or
  `FROM "logs-2025.03"` — or leave it bare (`FROM logs-2025.03`). All three read the index
  `logs-2025.03`.

- **A qualifier must be quoted from the FIRST part.** `FROM elastic."bi_events"` mixes the
  spellings, so the leading run of quoted parts is empty and the whole operand is read as ONE index
  name, `elastic.bi_events`. Quote the first part too (`FROM "elastic"."bi_events"`) if you meant
  `elastic` as a qualifier, or leave both bare if you meant the dotted index name.

- **A dot inside a quoted COLUMN name is still a qualifier.** `` SELECT `a.b` FROM t `` is read as
  the column `b` qualified by `a`, exactly as `SELECT a.b` is — there is no way to address an
  Elasticsearch field whose own name contains a dot. (A quoted *table* name is the opposite: its
  dots are literal.) Quoting makes it *look* as though there should be; there is not.

- **A dot and the name part after it must be adjacent — in a column name.** `SELECT a.b` is a
  qualified name; `SELECT a . b` is rejected, and so is a name left with a trailing dot
  (`ORDER BY b. DESC`). This is deliberate: when the dot was allowed to float, `ORDER BY b. DESC`
  silently parsed as a column named `b.DESC` sorted *ascending*. A **table**-name qualifier is
  deliberately more tolerant (`FROM "elastic" . bi_events` is accepted), because that spelling has
  always been accepted there and tightening it would have moved which index the statement reads.

- **A qualifier shares a namespace with a real dotted index name.** When one `FROM` names the same
  index under two different qualifiers, the engine tells the two apart by their qualified reference
  — so `SELECT a FROM a.orders q, "a".orders o, "b".orders p` uses `a.orders` both as a real index
  (what `q` reads) and as the qualified reference of `"a".orders`. Both readings of that statement
  are wrong, it was already wrong before, and it is not worth machinery: do not qualify two
  same-named indices with a name that is itself a real index.

> ⚠️ **Federation reads a qualifier differently from the engine.** A cross-cluster statement whose
> table names are FULLY quoted — ``FROM `prod_us`.`orders` `` rather than ``FROM `prod_us`.orders``
> — is not recognised by Federation's catalog pre-processor, so it is forwarded to the default
> cluster instead of the one you named. Before this release such a statement failed loudly in the
> parser; now it parses, so the mis-routing is silent. **On the federation path, leave the table
> name itself unquoted** (`` `prod_us`.orders ``) until this is fixed — see
> [joins.md](joins.md#row-2--cross-cluster-conveyor).

## Temporary tables are not supported

`CREATE TEMPORARY TABLE` and `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE`, with or without
`ON COMMIT { PRESERVE | DELETE } ROWS`, are **refused by intent**, and the error names the
construct. Two reasons, both structural:

- **An Elasticsearch index is cluster-global and has no session scope.** It is visible to every
  client that can read the cluster, and there is no session for it to belong to or be cleaned up
  with — the JDBC and ADBC drivers have no server process at all, so nothing can be told that a
  connection ended.
- **`ON COMMIT … ROWS` is transaction semantics, and Elasticsearch has no transactions.** There is
  nothing to honour, and accepting the clause to ignore it would silently change how long your data
  lives.

Use `CREATE TABLE` for a regular index and `DROP TABLE` it when you are done.

Tableau's connection-capability probe issues a `CREATE TABLE` / `DROP TABLE` pair against a
`#`-prefixed name, and its SQL-92 dialect issues `CREATE LOCAL TEMPORARY TABLE`. Every one of those
statements is refused, and Tableau then takes its documented fallback. For what that fallback costs —
and why a `.tdc` customization file cannot skip the probe — see
[Tableau: the temp-table probe](../client/bi_tools.md#tableau-the-temp-table-probe).

A plain `CREATE TABLE` against a probe-shaped name is refused too, but on Elasticsearch's index
naming rules rather than on the temporary-table grammar: an index name must be lowercase and cannot
contain `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, a space, a comma or `#`. The error names every rule
the name breaks.

> **Note on what "not session-scoped" does and does not imply.** It is not that Tableau requires
> session scope — Tableau's own capability `CAP_TEMP_TABLES_NOT_SESSION_SCOPED` exists precisely for
> sources that *"use regular tables to simulate temp tables"*. So a future release could choose to
> serve temporary tables with something other than a session. The reason we do not is a cost and a
> safety argument, not an impossibility: every temporary table would be a cluster-state update
> serialised through the elected master, and anything that failed to clean them up would accumulate
> indices in your production cluster until the per-node shard limit refused **all** index creation,
> yours included.

## `STDDEV` / `VARIANCE` over a transformed expression — Elasticsearch 6 refuses it

`STDDEV(YEAR(hire_date))`, `VARIANCE(ABS(salary))` and the rest of the `extended_stats` family over
a transformed operand (plain or `OVER (PARTITION BY …)`) compute correctly on **Elasticsearch 7, 8
and 9**. On **Elasticsearch 6** the query is refused with a `400` — *"STDDEV/VARIANCE over a
transformed expression is not supported on Elasticsearch 6 …"* — because the client library the
driver builds on drops the aggregation script on that line (elastic4s#4100) and the 6.x line is
unmaintained, so the fix cannot reach it; until this rule, the query silently returned the statistic
of the **raw** field. Aggregate over a raw field there, or use Elasticsearch 7+. That refusal is
permanent. See [STDDEV / VARIANCE family](functions_aggregate.md#function-stddev--variance-family).

## `DATE_FORMAT` / `DATETIME_FORMAT` over a bare date column — Elasticsearch 6.8 refuses it

`DATE_FORMAT(created_at, '%Y-%m-%d')` and `DATETIME_FORMAT(ts, '%Y-%m-%d %H:%i:%s')` — the operand a
**bare `date` column** — work on **Elasticsearch 7, 8 and 9** and are refused on **Elasticsearch 6.8**
with a script error. On that release a `date` doc-value reaches Painless as a
`JodaCompatibleZonedDateTime`, while `DateTimeFormatter.format` requires a `java.time.TemporalAccessor`.

Everything else in the family is unaffected on 6.8, which makes the workaround a small edit:

| Operand | 6.8 |
|---------|-----|
| A literal or a cast — `DATE_FORMAT('2025-01-10'::DATE, …)`, `DATE_FORMAT(CAST(created_at AS DATE), …)` | Works |
| A column wrapped in a date function — `DATE_FORMAT(DATE_TRUNC(created_at, DAY), …)` | Works |
| A bare date column — `DATE_FORMAT(created_at, …)` | **Refused** |

So on 6.8, write `DATE_FORMAT(CAST(created_at AS DATE), '%Y-%m-%d')` or
`DATE_FORMAT(DATE_TRUNC(created_at, DAY), '%Y-%m-%d')`; both return exactly what the bare column
returns on 7+. Every formatting example in the reference already uses a cast, so none of them is
affected.

This is not permanent: the fix is understood and tracked as issue #371, together with the parameter
identity defect (#370) it shares a cause with. It is not in this release because the change moves the
emission of the whole date-format family in every venue, and that needs its own verification pass.
See [DATE_FORMAT](functions_date_time.md#date_format) and
[DATETIME_FORMAT](functions_date_time.md#datetime_format).

## Arithmetic is not accepted inside a function, a `CAST` or a `CASE` branch

An arithmetic expression cannot be the **operand** of a function call, of a `CAST` or of a `CASE`
branch. The restriction is not specific to division, and parenthesising does not help:

| Written | Verdict |
|---------|---------|
| `CAST(a / b AS INTEGER)`, `CAST(a + b AS INTEGER)`, `CAST((a / b) AS INTEGER)` | Parse error |
| `FLOOR(a / b)`, `ABS(a / b)`, `COALESCE(a / b, 0)` | Parse error |
| `CASE WHEN b != 0 THEN a / b ELSE 0 END` | Parse error |
| `a / NULLIF(b, 0)`, `FLOOR(x)`, `CAST(x AS INTEGER)` | Accepted |

The rule is **directional**: arithmetic *over* a function call is fine, a function call *over*
arithmetic is not.

⚠️ **One spelling parses and then ignores the cast.** `(a / b)::INTEGER` is accepted, but the
conversion is discarded: it emits exactly what `a / b` emits, so the result is a DOUBLE even though
the expression reports INTEGER. Do not use it as a workaround for the rejections above — it is the
one shape in this family that fails *silently* rather than loudly. It is pre-existing and applies to
any operator (`(a + b)::DOUBLE`, `(d + 1)::INTEGER`).

The practical consequences are that there is no single-expression way to write a truncated quotient,
and no in-expression guard for `%`:

```sql
-- instead of CAST(n / m AS INTEGER), compute the quotient into a column and cast THAT column
CREATE TABLE t (n INTEGER, m INTEGER, q DOUBLE SCRIPT AS (n / m));
SELECT CAST(q AS INTEGER) AS whole FROM t;
```

## `%` by zero is not guarded

The `0.24.0` rule that makes `a / 0` yield NULL covers `/` only. With integer operands `a % 0`
throws — HTTP 400 in a search, and in a computed column the ingest processor's `ignore_failure`
swallows it so the column is simply absent. With **floating** operands it produces `NaN`, which
Elasticsearch refuses to index, so **the whole document is rejected**.

Because of the restriction above there is no in-expression guard, and `a % NULLIF(b, 0)` throws a
`null_pointer_exception` on exactly the rows the guard is for. Keep a zero divisor out of the data
instead — filter it in `WHERE`, or compute the remainder from an already-filtered index. See
[operators](operators.md#-mod).

## `ORDER BY` over arithmetic on a nullable column

`ORDER BY <arithmetic over a nullable column>` fails. The bridge emits a `number`-typed script sort,
and Elasticsearch rejects a sort script that can return null. Order by a stored column instead, or
make the expression a computed column and sort on that.

## Coming in the upcoming release (Quarter 1 2027)

- **Heterogeneous federation**: JOIN or correlate Elasticsearch with PostgreSQL, MySQL, ClickHouse, Snowflake, and more — plus cross-cluster subqueries (e.g. correlate one cluster's data against another's).
  **Not this**: correlating one Elasticsearch index against **another Elasticsearch index** — `EXISTS` / `NOT EXISTS` / `IN` / `NOT IN` / a scalar comparison against a subquery that reads the outer row — is **single-cluster** and runs through the relational engine shipped in `softclient4es-arrow-extensions`. Its one rule: the outer reference must be **qualified** with the outer table's alias (`… WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)`), because a bare column name inside a subquery is read as the subquery's own column. A venue without that jar refuses the statement with HTTP 400 rather than executing it as if it were self-contained.

## Deferred (a future release, demand-driven — tell us what you need)

- `MERGE`, `RETURNING`, `INFORMATION_SCHEMA`, non-materialized `CREATE VIEW`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL` as a type, and `UUID`. (`DECIMAL` / `NUMERIC` are now accepted as cast targets and column types, but **approximately** — they map to `DOUBLE`, and a precision or scale is accepted and ignored. Elasticsearch has no exact decimal type.) No committed date — these are prioritised by customer demand. (Current-release DML already supports `INSERT … ON CONFLICT` upsert — a different feature from `MERGE`.)

## Roadmap timing

We do not commit firm external dates. The next release is targeted for **Quarter 4 2026**; the upcoming release (heterogeneous federation) for **Quarter 1 2027**; the deferred items are demand-driven with no committed date. Treat the next release's feature list as *planned*, not guaranteed — its scope is gated on a function-library audit.

## See also

- The [JOIN matrix walkthrough](joins.md) — how the three JOIN tiers work, with worked examples.
- The [federation operator guide](../client/federation_operator_guide.md) — multi-cluster federation deployment.

---

*This page describes SoftClient4ES **as of the current release**. Once the next release ships, the "Not in this release" list above shrinks — verify against your installed release.*
