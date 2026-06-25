# The JOIN matrix, explained: rows 1, 2, 3

![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)

*R1 blog series — Post 2 of 4 (week 1). Previously: "Announcing R1 — Cross-cluster JOIN on Elasticsearch." Next: "SRE cross-cluster incident triage in one SQL query."*

<!-- Channel B (Medium / personal LinkedIn). Hero image staged in elasticsql/linkedin/ at publish time. -->

---

## One feature, three execution shapes

"Cross-index JOIN on Elasticsearch" is one promise, but underneath it runs three different ways depending on *where* the data lives. We call them the three rows of the JOIN matrix. You never pick a row — the planner does, by counting how many distinct clusters your query touches. But understanding the rows tells you what to expect.

---

## Row 1 — same-cluster passthrough

Both indices live in the *same* Elasticsearch cluster. The driver pushes the scans down to ES, hands the rows to an embedded DuckDB engine in-process, and DuckDB does the JOIN. No coordinator, no network hop between clusters.

```sql
SELECT e.emp_id, e.name, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.id;
```

This is the free path in Community. INNER / LEFT / RIGHT / FULL all work; so do `INSERT ... SELECT`, `CREATE TABLE AS SELECT`, `ON CONFLICT` upserts, and prepared statements. The same DuckDB engine backs JDBC, ADBC, Arrow Flight SQL, and the REPL — Row 1 looks identical on every surface.

---

## Row 2 — cross-cluster conveyor

The JOIN spans two clusters, and you are *writing* the result somewhere (an `INSERT` or `CTAS` whose target is on one of them). The coordinator runs the source `SELECT`, streams the rows to the target cluster's sidecar, and bulk-loads them. Source scan → coordinator → target sidecar bulk-load. That conveyor is Row 2.

---

## Row 3 — multi-source coordinator

The JOIN reads from several clusters at once. Each leg is scanned independently, materialized to a per-query scratch view in the coordinator's DuckDB, and joined coordinator-local. The catalog prefix names the cluster:

```sql
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_eu`.customers c ON o.customer_id = c.id;
```

R1's Row 3 is multi-**ES**-cluster. Heterogeneous sources (Postgres, MySQL, Snowflake) are an R2b concern — the architecture is ready, the connectors come later.

---

## Counting JOINs: the meter

The quota counts JOIN operators, not tables. An N-table query has N−1 JOINs:

- 2 tables → 1 JOIN
- 3 tables → 2 JOINs
- 4 tables → 3 JOINs

Community's `maxJoins=2` means up to a 3-table query is free. `UNNEST` (flattening a nested array) does **not** count against the meter. Exceed your tier and the query is rejected with a clear upgrade message — no silent truncation of the JOIN itself.

| Tier | maxJoins | Largest free query |
|---|---|---|
| Community | 2 | 3-table |
| Pro | 5 | 6-table |
| Enterprise | ∞ | unbounded |

---

## Where to go next

- **Run Row 1 yourself in five minutes:** the [JDBC quickstart](https://softclient4es.dev/integrations/jdbc/).
- **See how the meter maps to price:** [editions & pricing](https://softclient4es.dev/licensing/).
<!-- pending 17.1: /sql/joins/ (JOIN matrix walkthrough — web PR #12, base release-r1) -->
- **The full matrix reference, with every variant:** the JOIN matrix walkthrough — `https://softclient4es.dev/sql/joins/`.

Next week: one SQL query that triages an incident spanning three regions.

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/
