[Back to index](README.md)

# Known Limitations & Roadmap

SoftClient4ES runs a large, practical subset of ANSI SQL on Elasticsearch — including cross-index JOINs that Elasticsearch itself cannot do. A few advanced constructs (subqueries, CTEs, set operators beyond `UNION ALL`) are not in the current release yet. This page tells you exactly what works **as of this release**, what's coming, and how to get unblocked today.

> Great for explicit JOIN SQL — full BI-tool subquery / CTE support is coming in the next release.

## Using a BI tool? Read this first

If your BI tool just failed on a subquery or a CTE, you're in the right place. Some BI tools auto-generate nested SQL (subqueries / derived tables) even when your logical query has none. Until the next release lands full subquery support, send **explicit JOIN SQL** instead of letting the tool compose nested queries:

- **Tableau** — Tableau **live** connections can auto-generate subqueries. Use **Extract** mode (Tableau runs the extract locally, subquery-free), or write **Custom SQL** with explicit JOINs instead of letting Tableau compose the query.
- **Power BI** — DirectQuery / query folding can compose nested SQL. Prefer **Import** mode (folds locally, nothing nested is pushed), or author explicit-JOIN queries; avoid relationships that force generated subqueries until the next release.
- **Looker** — BI tools that build **derived tables / measures** (e.g. Looker) can compose subqueries. Model **explicit JOINs** in the SQL the tool sends rather than relying on tool-composed derived tables; avoid symmetric-aggregate measures that force derived tables until the next release.
- **Metabase** — the GUI Question builder can emit subqueries for multi-stage questions. Use **Native (SQL)** queries with explicit `JOIN … ON …` instead of the visual builder for any query that would otherwise nest.

> **General rule:** prefer **explicit JOIN SQL** over tool-generated nested SQL. If you control the query, a cross-index JOIN is fully supported in the current release.

Tableau, Power BI, and Metabase are **Compatible** (work via the JDBC/ADBC spec, not formally tested by us). **Apache Superset** (dedicated dialect), **DBeaver**, and **Grafana** (via Arrow Flight SQL) are **Tested**.

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
