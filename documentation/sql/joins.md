# Cross-Index JOIN

**Stop ETL'ing Elasticsearch into your warehouse just to JOIN it.**

Elasticsearch has no native cross-index JOIN — and a stock JDBC driver on top of ES can't add one either. SoftClient4ES gives you two things Elasticsearch can't do on its own:

1. **Query-time cross-index JOIN** — `INNER` / `LEFT` / `RIGHT` / `FULL OUTER` joins across indices (and across clusters), at query time, on **every** surface: the REPL, the JDBC driver, the ADBC driver, the Arrow Flight SQL sidecar, and Federation. The drivers are the **free delivery channel**; the JOIN *depth* is metered.
2. **Persisted [Materialized Views](materialized_views.md)** — the other superpower: denormalize cross-index data once into a queryable view. (That has its own page; this page is about query-time JOINs.)

## The JOIN ladder

Three meters gate how far a JOIN can reach. Think of them as rungs:

- **Query-time depth — `maxJoins`** — how many cross-index JOINs a single query may contain.
- **Cross-cluster reach — `maxClusters`** — how many Elasticsearch clusters a Federation deployment may span.
- **Persisted — `maxMaterializedViews`** — how many denormalized views you may keep materialized.

Every tier *has* every feature. The meters gate **scale**, not on/off switches — see [Licensing & the meters](#licensing--the-meters).

## Which row do I need?

Cross-index JOIN ships in three shapes ("rows"). Pick by where your data lives:

| Row | Name | What it does | Engine | Surfaces |
|---|---|---|---|---|
| **Row 1** | Passthrough / same-cluster | Cross-index JOIN of 2+ indices in **one** ES cluster | Each leg is an ES sub-query; the JOIN runs in-process in embedded **DuckDB** | REPL, JDBC, ADBC, Flight SQL sidecar |
| **Row 2** | Cross-cluster conveyor | JOIN / INSERT / CTAS where **source and target live in different clusters** | Coordinator runs the source SELECT and conveyors the Arrow stream to the target sidecar for bulk-load | Federation |
| **Row 3** | Multi-source coordinator | JOIN across **2+ source clusters** | Coordinator stages each leg to Parquet scratch + a per-query DuckDB view, joins coordinator-local | Federation |

**One sentence to decide:** same cluster → **Row 1**; copy results between two clusters → **Row 2**; join across two or more clusters → **Row 3**.

The rule the engine actually applies: `QueryRouter.routeWriteWithJoin` counts the **distinct source catalogs** in the rewritten `FROM` / `JOIN` clauses versus the target catalog. Same (or no) catalog → Row 1; exactly one source catalog different from the target → Row 2; two or more source catalogs → Row 3.

> **What does NOT work yet:** Cross-index JOINs are first-class in R1, but two things are intentionally **not** here yet: arbitrary **subqueries / CTEs** in a JOIN query land in **R2a**, and **heterogeneous Row-3 sources** (joining ES with Postgres, MySQL, Snowflake, …) land in **R2b** — R1 Row 3 is multi-**Elasticsearch** only. See Known limitations (_pending 17.6_) for the full list.

---

## Row 1 — same-cluster passthrough

A Row-1 JOIN reaches two or more indices in **one** Elasticsearch cluster. Each table in the query becomes its own ES sub-query (with any single-table `WHERE` pushed down into it); the cross-index JOIN itself is executed in-process by an embedded **DuckDB** engine inside the driver, sidecar, or REPL. The first JOIN on a fresh process warms up the DuckDB native library once.

```
        SQL: SELECT ... FROM emp e JOIN dept d ON e.dept_id = d.dept_id
                                  │
                    ┌─────────────┴─────────────┐
                    │   Driver / sidecar / REPL  │
                    │   (embedded DuckDB engine) │
                    └─────────────┬─────────────┘
              sub-query e │                 │ sub-query d
                          ▼                 ▼
                   ┌────────────┐    ┌────────────┐
                   │  ES index  │    │  ES index  │   ← one cluster
                   │    emp     │    │    dept    │
                   └────────────┘    └────────────┘
                          └────► joined in DuckDB ◄────┘ → rows to client
```

All examples below are transcribed verbatim from the JDBC integration suite
`softclient4es-jdbc/testkit/src/main/scala/app/softnetwork/elastic/jdbc/JdbcIntegrationSpec.scala`.
They run against two fixtures: **`jdbc_join_emp`** (`emp_id`, `dept_id`, `name`, `salary` — 6 rows: Alice/1/1/6000, Bob/2/1/4000, Carol/3/1/8000, Dave/4/2/5500, Eve/5/2/3000, Orphan/6/**99**/4500) and **`jdbc_join_dept`** (`dept_id`, `dept_name` — Engineering=1, Marketing=2, Empty=9). The orphan employee (`dept_id`=99) and the empty department (`dept_id`=9) surface the outer-join NULLs.

A few examples use a separate small fixture **`jdbc_test`** (`id`, `name`, `value` — a 3-column table, **not** the employees table; it has no `salary`/`dept_id`).

### SELECT — INNER / LEFT / RIGHT / FULL OUTER

**INNER JOIN** (the canonical first example):

```sql
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
-- 5 rows (orphan employee with dept_id=99 dropped by the INNER JOIN)
```

**LEFT JOIN** with NULLs — unmatched left rows surface NULL on the right (this one uses the `jdbc_test` fixture):

```sql
SELECT t.id, t.name, d.dept_name
FROM jdbc_test t
LEFT JOIN jdbc_join_dept d ON t.id = d.dept_id
ORDER BY t.id ASC;
-- only ids 1→Engineering and 2→Marketing match; the rest get NULL dept_name
```

**RIGHT OUTER** and **FULL OUTER**:

```sql
-- RIGHT OUTER: dept side fully preserved; empty dept 9 → NULL e.name
SELECT e.name, d.dept_name
FROM jdbc_join_emp e
RIGHT OUTER JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
ORDER BY d.dept_name ASC;

-- FULL OUTER: NULLs on both sides (empty dept 9 → NULL name; orphan emp dept 99 → NULL dept_name)
SELECT e.name, d.dept_name
FROM jdbc_join_emp e
FULL OUTER JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
```

### Predicate pushdown

Each single-table `WHERE` predicate is pushed into its own ES sub-query before the JOIN, so a filtered JOIN returns strictly fewer rows:

```sql
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
WHERE e.salary > 5000 AND d.dept_name = 'Engineering';
```

### GROUP BY / HAVING / ORDER BY

Aggregation runs in DuckDB after the JOIN:

```sql
SELECT d.dept_name, COUNT(*) AS cnt
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
GROUP BY d.dept_name
HAVING COUNT(*) > 1
ORDER BY COUNT(*) DESC;
-- Engineering (3), Marketing (2) survive HAVING
```

> **Two ORDER BY gotchas:** the JOIN planner has two ordering restrictions — you cannot `ORDER BY` a **SELECT alias** (use `ORDER BY COUNT(*)`, not `ORDER BY cnt`), and you cannot `ORDER BY` a column that exists on **both** sides of the JOIN (order by a column unique to one side, e.g. `d.dept_name`, not the shared join key `d.dept_id`).

### ORDER BY … LIMIT (top-N)

```sql
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
ORDER BY e.salary DESC LIMIT 3;
-- 8000 / 6000 / 5500
```

### UNNEST + cross-index JOIN

Elasticsearch handles `JOIN UNNEST` on an `ARRAY<STRUCT>` natively inside the sub-query; the cross-index JOIN to the dimension runs in DuckDB. `UNNEST` does **not** count toward `maxJoins`:

```sql
SELECT o.id, oi.product, oi.quantity, d.dept_name
FROM jdbc_join_order_items o
JOIN UNNEST(o.items) AS oi
JOIN jdbc_join_dept d ON o.customer_id = d.dept_id;
-- one row per nested item; the UNNESTed nested fields (product, quantity) populate
```

### INSERT … SELECT … JOIN

Write the joined output into a target index (Row-1 write; orphan dropped → 5 rows):

```sql
INSERT INTO jdbc_row1_insert_join_target
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
```

### CREATE TABLE … AS SELECT … JOIN (CTAS)

```sql
CREATE TABLE jdbc_row1_ctas_join_target AS
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
```

### CREATE OR REPLACE TABLE … AS

Real REPLACE — drops then recreates, so re-running a narrower query shrinks the table:

```sql
CREATE OR REPLACE TABLE jdbc_row1_replace_ctas_target AS
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
```

### INSERT … ON CONFLICT (col) DO UPDATE (upsert)

Idempotent upsert — a stable SHA-1 `_id` is derived from the conflict column, so re-running merges rather than duplicating (count stays 5, not 10):

```sql
INSERT INTO jdbc_row1_insert_join_upsert_target
SELECT e.emp_id, e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
ON CONFLICT (emp_id) DO UPDATE;
```

> **Two ON CONFLICT forms are rejected:** **CTAS + ON CONFLICT** and **INSERT + ON CONFLICT DO NOTHING** are rejected at the boundary — Elasticsearch has no native skip-on-conflict with stable ids. Use `INSERT … ON CONFLICT (col) DO UPDATE` for upserts.

### Prepared statement through a JOIN

A bound parameter is substituted into the sub-query **before** planning, so the same `PreparedStatement` returns different rows for different bindings:

```sql
SELECT e.name, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
WHERE e.salary > ?;   -- bind 3500.0 → more rows; bind 6500.0 → fewer (Carol = 8000 survives)
```

---

## Row 2 — cross-cluster conveyor

A Row-2 operation has its **target** in a different cluster from its **source**. The Federation coordinator runs the source SELECT on the source cluster's sidecar, receives the result as an Arrow stream, and **conveyors** it to the target sidecar for a bulk-load. (The JOIN in 2a/2b is itself single-source — both source tables are in `prod_us` — but the *target* `prod_eu` is a different cluster, and that is what makes it Row 2. A plain catalog-to-catalog `INSERT … SELECT *` with no JOIN is also a Row-2 conveyor.)

```
   ┌──────────────────────────┐
   │  Federation coordinator  │
   └───┬──────────────────▲───┘
   1.  │ run source SELECT │  2. Arrow stream
       ▼                   │
   ┌────────────────────┐  │        ┌────────────────────┐
   │  prod_us  sidecar  │──┘        │  prod_eu  sidecar  │  ← target
   └────────────────────┘           └─────────▲──────────┘
            └──────── 3. bulk-load conveyor ───┘
```

Cross-cluster references use **backtick-quoted catalog prefixes** — the catalog name is the Federation `servers.<name>` alias, which Federation strips before forwarding each leg's SELECT to its source cluster.

Examples are transcribed from
`softclient4es-arrow/federation/src/test/scala/app/softnetwork/elastic/arrow/federation/FederationJoinE2ESpec.scala`.

**Cross-cluster INSERT-with-JOIN** — the source SELECT runs on `prod_us`, conveyed to `prod_eu`:

```sql
INSERT INTO `prod_eu`.dest
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_us`.customers c ON o.id = c.id;
```

**Cross-cluster CTAS-with-JOIN** — the target DDL is inferred from the source FlightInfo schema, then conveyed:

```sql
CREATE TABLE `prod_eu`.dest AS
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_us`.customers c ON o.id = c.id;
```

---

## Row 3 — multi-source coordinator

A Row-3 JOIN reaches **two or more source clusters**. The coordinator stages each leg to Parquet scratch on disk and exposes it as a per-query DuckDB view (named `q_<UUID>` for isolation), then runs the JOIN **coordinator-local**.

> **R1 = multi-Elasticsearch only:** in R1, Row 3 joins across **multiple Elasticsearch clusters**. Joining Elasticsearch against heterogeneous sources (Postgres, MySQL, Snowflake, …) is **coming in R2b** — it is not promised for R1.

```
   ┌──────────────┐   leg 1   ┌──────────────────────────┐
   │  prod_us     │──────────▶│  Federation coordinator  │
   └──────────────┘           │                          │
   ┌──────────────┐   leg 2   │  Parquet scratch + a     │
   │  prod_fr     │──────────▶│  per-query DuckDB view   │
   └──────────────┘           └────────────┬─────────────┘
                                            ▼
                                 coordinator-local JOIN
                                            │
                                            ▼
                              ┌──────────────────────────┐
                              │  prod_eu  (target)        │
                              └──────────────────────────┘
```

**Multi-source SELECT JOIN** (read; the headline three-cluster query — two source catalogs, joined coordinator-local):

```sql
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_eu`.customers c ON o.id = c.id;
-- two source catalogs (prod_us, prod_eu) → coordinator-local join
```

This is the SRE wedge: **correlate logs, metrics, and traces across regional ES clusters in one SQL query.** (The full SRE story lives in the Federation operator guide (_pending 16.6/17.2 merge_) and the `three-region` example topology.)

**Multi-source INSERT-with-JOIN** (sources `prod_us` + `prod_fr`, target `prod_eu`):

```sql
INSERT INTO `prod_eu`.dest
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_fr`.customers c ON o.customer_id = c.id;
```

**Multi-source CREATE OR REPLACE TABLE … AS:**

```sql
CREATE OR REPLACE TABLE `prod_eu`.dest AS
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_fr`.customers c ON o.customer_id = c.id;
```

---

## Performance characteristics

What to expect per row, qualitatively. (Hard latency and throughput numbers belong to the R1.1 Arrow benchmark — link forward, don't pre-empt.)

- **Row 1** — lowest latency of the three rows; the JOIN is in-process DuckDB over ES sub-query results, so the dominant cost is the ES sub-queries plus the cross product. The first JOIN on a fresh process pays a one-time DuckDB native-library warm-up.
- **Row 2** — adds a network hop (coordinator → source SELECT) plus a bulk-load conveyor to the target; throughput is bound by the Arrow stream and target ingest, not by JSON — data crosses the wire as Arrow RecordBatches.
- **Row 3** — adds per-leg Parquet staging on the coordinator before the DuckDB join; cost scales with the **largest** leg plus the join cardinality.
- **Arrow wire format** — for **Flight SQL** and **ADBC** clients specifically, results stream as Arrow RecordBatches (zero JSON on the wire). The JDBC driver surfaces JOIN rows via an in-process Arrow result set and the REPL renders to a console, so the clean zero-JSON-wire property is a Flight-SQL/ADBC trait, not a blanket one. The value here is interoperability and SQL completeness; the full zero-copy speed story is the R1.1 benchmark.

### Row truncation at the result cap

The **joined output** is capped at the tier's `maxQueryResults` (Community 10,000). Over-cap results are **truncated with a warning** — never silently dropped: JDBC and ADBC raise `SQLWarning 01004` (`Result truncated to N rows`), and Flight SQL emits an `x-result-truncated` header.

> **Join inputs are never capped:** the result cap applies to the **joined output only**, never to a JOIN leg. Capping a leg would truncate a JOIN input and produce **silently wrong** results. So a wide Community join can hit the 10,000-row output cap even though none of its inputs were truncated.

## Licensing & the meters

Every tier **has every feature.** The meter gates *scale*, not an on/off switch.

- **`maxJoins` — JOIN depth per query.** A 2-table JOIN counts as **1** JOIN, a 3-table JOIN as **2**, a 4-table JOIN as **3**. Community **2** (up to a 3-table JOIN), Pro **5** (up to a 6-table JOIN), Enterprise **∞**. `UNNEST` does **not** count toward `maxJoins`.
- **`maxClusters` — cross-cluster reach.** Community **1**, Pro **5**, Enterprise **∞**. Single-cluster Federation is free — the **meter**, not a feature flag, is the paywall. **ES-only at R1** (phrased "across N ES clusters").
- **`maxMaterializedViews` — persisted views.** Community **1**, Pro **50**, Enterprise **∞**.

**Community has Federation.** It is capped at 1 cluster — the quota *is* the paywall, not a feature gate. One sidecar / one cluster boots free; a second cluster makes the Federation sidecar fail to start by design.

The same meters, in source-of-truth order (MV / results / clusters / joins):

| Tier | `maxMaterializedViews` | `maxQueryResults` | `maxClusters` | `maxJoins` |
|---|---|---|---|---|
| Community | 1 | 10,000 | 1 | 2 |
| Pro | 50 | 1,000,000 | 5 | 5 |
| Enterprise | ∞ | ∞ | ∞ | ∞ |

### What happens at each cap (verified)

- **`maxJoins` exceeded** → the planner rejects the query. A 4-table query (3 cross-index JOINs) under Community is rejected with a message that names the count, the limit, the next tier, and the upgrade URL:

```sql
-- 4 tables = 3 cross-index JOINs → exceeds Community maxJoins=2 → rejected
SELECT t.id, d.dept_name, r.region_name, m.team_name
FROM jdbc_test t
JOIN jdbc_join_dept d   ON t.id = d.dept_id
JOIN jdbc_join_region r ON t.id = r.region_id
JOIN jdbc_join_team m   ON t.id = m.team_id;
-- Error: "Query contains 3 cross-index JOINs ... maximum of 2 ... Upgrade to Pro ...
--         See: https://portal.softclient4es.com/pricing"
```

  The same query runs unchanged on Pro or Enterprise (higher / no cap).

- **`maxClusters` exceeded** → the Federation sidecar **fails to start** (by design — a CrashLoop) rather than silently dropping a cluster.
- **`maxQueryResults` exceeded** → **truncate-with-warning** on the no-`LIMIT` path (`SQLWarning 01004` / Flight `x-result-truncated`), or an HTTP 402 on an explicit `LIMIT` over quota.

The JOIN engine ships in the Elastic-License extensions — **free to use** (not "source-available").

For the full price matrix and editions, see the licensing & pricing page on the website.

---

## What does NOT work yet

- **Arbitrary subqueries and CTEs** inside a JOIN query — coming in **R2a**.
- **Heterogeneous Row-3 sources** (joining Elasticsearch with Postgres, MySQL, Snowflake, …) — coming in **R2b**; R1 Row 3 is multi-Elasticsearch only.

Full list: Known limitations (_pending 17.6_).

## Try it in 5 minutes

- JDBC quickstart — single-cluster JOIN from any JDBC tool.
- ADBC quickstart (_pending 17.3_) — in-process Arrow.
- Arrow Flight SQL quickstart — columnar streaming.
- Federation operator guide (_pending 16.6/17.2 merge_) — cross-cluster (Rows 2 & 3) with the Helm chart.
- Example topologies live in the [`softclient4es-helm`](https://github.com/SOFTNETWORK-APP/softclient4es-helm) repo: `softclient4es-federation/examples/single-cluster/` and `.../three-region/`. (_pending Epic-16 topologies_)

## See also

- [Materialized Views](materialized_views.md) — superpower #2: persisted, pre-joined data.
- [DQL Support](dql_statements.md) — non-JOIN SELECT and `JOIN UNNEST` detail.
- Licensing & pricing — the full edition / quota matrix (on the website).
- Known limitations (_pending 17.6_).
- Federation operator guide (_pending 16.6/17.2 merge_).
