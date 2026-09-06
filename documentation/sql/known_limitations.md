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

- **Subqueries**: scalar, `IN (SELECT …)`, `EXISTS (SELECT …)`, derived tables `FROM (SELECT …)`, and correlated subqueries.
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

Tableau's connection-capability probe issues a `CREATE TABLE` / `DROP TABLE` pair against a
`#`-prefixed name, and its SQL-92 dialect issues `CREATE LOCAL TEMPORARY TABLE`. The
`LOCAL TEMPORARY` form is rejected: an Elasticsearch index is global, permanent and not
session-scoped, so there is nothing for the engine to honestly answer "yes" to. Whether a plain
`CREATE TABLE` against a probe-shaped name should be honoured is a separate open question about
`CREATE TABLE` semantics, not a quoting one.

## Coming in the upcoming release (Quarter 1 2027)

- **Heterogeneous federation**: JOIN or correlate Elasticsearch with PostgreSQL, MySQL, ClickHouse, Snowflake, and more — plus cross-cluster subqueries (e.g. correlate one cluster's data against another's).

## Deferred (a future release, demand-driven — tell us what you need)

- `MERGE`, `RETURNING`, `INFORMATION_SCHEMA`, non-materialized `CREATE VIEW`, `DECIMAL`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL` as a type, and `UUID`. No committed date — these are prioritised by customer demand. (Current-release DML already supports `INSERT … ON CONFLICT` upsert — a different feature from `MERGE`.)

## Roadmap timing

We do not commit firm external dates. The next release is targeted for **Quarter 4 2026**; the upcoming release (heterogeneous federation) for **Quarter 1 2027**; the deferred items are demand-driven with no committed date. Treat the next release's feature list as *planned*, not guaranteed — its scope is gated on a function-library audit.

## See also

- The [JOIN matrix walkthrough](joins.md) — how the three JOIN tiers work, with worked examples.
- The [federation operator guide](../client/federation_operator_guide.md) — multi-cluster federation deployment.

---

*This page describes SoftClient4ES **as of the current release**. Once the next release ships, the "Not in this release" list above shrinks — verify against your installed release.*
