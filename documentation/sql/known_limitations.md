[Back to index](README.md)

# Known Limitations & Roadmap

SoftClient4ES R1 runs a large, practical subset of ANSI SQL on Elasticsearch — including cross-index JOINs that Elasticsearch itself cannot do. A few advanced constructs (subqueries, CTEs, set operators beyond `UNION ALL`) are not in R1 yet. This page tells you exactly what works **as of R1**, what's coming, and how to get unblocked today.

> Great for explicit JOIN SQL — full BI-tool subquery / CTE support is coming in R2a.

<!-- Scoping marker (top): this page describes R1. When R2a ships, the "Not in R1" list shrinks — re-read this page against your installed release. -->

## Using a BI tool? Read this first

If your BI tool just failed on a subquery or a CTE, you're in the right place. Some BI tools auto-generate nested SQL (subqueries / derived tables) even when your logical query has none. Until R2a lands full subquery support, send **explicit JOIN SQL** instead of letting the tool compose nested queries:

- **Tableau** — Tableau **live** connections can auto-generate subqueries. Use **Extract** mode (Tableau runs the extract locally, subquery-free), or write **Custom SQL** with explicit JOINs instead of letting Tableau compose the query.
- **Power BI** — DirectQuery / query folding can compose nested SQL. Prefer **Import** mode (folds locally, nothing nested is pushed), or author explicit-JOIN queries; avoid relationships that force generated subqueries until R2a.
- **Looker** — BI tools that build **derived tables / measures** (e.g. Looker) can compose subqueries. Model **explicit JOINs** in the SQL the tool sends rather than relying on tool-composed derived tables; avoid symmetric-aggregate measures that force derived tables until R2a.
- **Metabase** — the GUI Question builder can emit subqueries for multi-stage questions. Use **Native (SQL)** queries with explicit `JOIN … ON …` instead of the visual builder for any query that would otherwise nest.

> **General rule:** prefer **explicit JOIN SQL** over tool-generated nested SQL. If you control the query, a cross-index JOIN is fully supported in R1.

Tableau, Power BI, and Metabase are **Compatible** (work via the JDBC/ADBC spec, not formally tested by us). **Apache Superset** (dedicated dialect), **DBeaver**, and **Grafana** (via Arrow Flight SQL) are **Tested**.

## Works in R1

- **Cross-index JOINs**: `INNER` / `LEFT` / `RIGHT` / `FULL` / `CROSS`, plus `JOIN UNNEST` on nested arrays — something Elasticsearch cannot do natively. (See the JOIN matrix walkthrough <!-- pending 17.1: joins.md / /sql/joins/ — forward-declared, target not on release-r1 until 17.1 lands --> for the per-tier rows and worked examples.)
- **Aggregations** + `GROUP BY` / `HAVING`.
- **Analytical SQL**: `ROW_NUMBER` / `RANK` / `DENSE_RANK`; the `STDDEV` / `VARIANCE` family (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`); `PERCENTILE_CONT` / `PERCENTILE_DISC`; window aggregates and `FIRST_VALUE` / `LAST_VALUE` / `ARRAY_AGG` over `OVER (PARTITION BY …)`.
- **Conditionals & null handling**: `CASE` / `COALESCE` / `NULLIF` / `GREATEST` / `LEAST` / `ISNULL` / `ISNOTNULL`.
- `ORDER BY … NULLS FIRST | NULLS LAST`.
- `UNION ALL` (concatenate result sets — no de-duplication).
- `SELECT * EXCEPT(col, …)` — drop named columns from `SELECT *`. This is the BigQuery-style **column-exclusion** clause. It is **not** the `EXCEPT` set operator (see below).

## Not in R1 (coming in R2a, ~5–6 months out)

- **Subqueries**: scalar, `IN (SELECT …)`, `EXISTS (SELECT …)`, derived tables `FROM (SELECT …)`, and correlated subqueries.
- **CTEs**: `WITH name AS (SELECT …)` — recursive and non-recursive.
- **Set operators**: `UNION` (with row de-duplication), `INTERSECT`, and the `EXCEPT` **set operator**. The `EXCEPT` set operator is **distinct from** the `SELECT * EXCEPT(cols)` column-exclusion clause above — that one works; the set operator does not.
- **Positional / tiling window functions**: `NTILE`, `LAG`, `LEAD` — not yet implemented; coming with the R2a analytical-SQL work. (Note: `PERCENTILE_CONT` / `PERCENTILE_DISC` — percentile *aggregates* — already work in R1; the positional/tiling window functions are a different family.)

These arrive in R2a via the new **arrow-bi** module — single-cluster customers get them by upgrading the driver (JDBC / ADBC / sidecar), with no infrastructure change and no federation server required.

### What a not-yet-supported query looks like

A subquery in a `WHERE` clause is rejected by the parser today:

```sql
-- Not supported in R1: subqueries are not yet implemented.
SELECT name
FROM employees
WHERE department_id IN (SELECT id FROM departments WHERE region = 'EU');
```

The parser rejects this — `IN` accepts only literal value lists in R1, not a nested `SELECT`. Rewrite it as an explicit JOIN (fully supported), or wait for R2a where the subquery form lands as-is.

## Coming in R2b (~5–6 months after R2a)

- **Heterogeneous federation**: JOIN or correlate Elasticsearch with PostgreSQL, MySQL, ClickHouse, Snowflake, and more — plus cross-cluster subqueries (e.g. correlate one cluster's data against another's).

## Deferred (R3+, demand-driven — tell us what you need)

- `MERGE`, `RETURNING`, `INFORMATION_SCHEMA`, non-materialized `CREATE VIEW`, `DECIMAL`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL` as a type, and `UUID`. No committed date — these are prioritised by customer demand. (R1 DML already supports `INSERT … ON CONFLICT` upsert — a different feature from `MERGE`.)

## Roadmap timing

We do not commit external dates. R2a is roughly **5–6 months** from R1; R2b roughly **5–6 months** after R2a; R3+ is demand-driven. Treat the R2a feature list as *planned*, not guaranteed — its scope is gated on a function-library audit.

## See also

- The JOIN matrix walkthrough <!-- pending 17.1: joins.md — forward-declared, not on release-r1 until 17.1 lands --> — how the three JOIN tiers work, with worked examples.
- The federation operator guide <!-- pending 16.6/17.2: ../client/federation_operator_guide.md — forward-declared, NOT on release-r1 yet; promote to a live link only after a Glob confirms the target merged. Launch-checklist: strip this marker before R1 launch. --> — multi-cluster federation deployment.

---

*This page describes SoftClient4ES **as of R1**. Once R2a ships, the "Not in R1" list above shrinks — verify against your installed release.*
