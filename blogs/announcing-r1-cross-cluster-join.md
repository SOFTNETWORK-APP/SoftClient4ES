# Announcing R1 — Cross-cluster JOIN on Elasticsearch

![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)

*R1 blog series — Post 1 of 4 (launch day). Next: "The JOIN matrix, explained: rows 1, 2, 3."*

<!-- Channel B (Medium / personal LinkedIn). Hero image staged in elasticsql/linkedin/ at publish time. -->

---

## Elasticsearch can't JOIN. So everybody ETLs.

You have orders in one Elasticsearch index and customers in another. You want the total revenue per customer name. In SQL that is one line. In Elasticsearch it is a project: denormalize at index time, or stand up a pipeline that copies both indices into a warehouse, JOIN them there, and ship the result back.

That is the tax. You bought a search engine and ended up running an ETL job just to answer a question a JOIN would have answered for free.

**R1 ends that.** SoftClient4ES R1 ships query-time **cross-index JOIN** on Elasticsearch — across indices, and across clusters — over the surfaces you already use: JDBC, ADBC, Arrow Flight SQL, and the REPL.

> Stop ETL'ing Elasticsearch into your warehouse just to JOIN it.

---

## Two deployment shapes — pick your scale

R1 has exactly two shapes, and you self-select:

**Single-cluster (free in Community).** Drop in a driver and JOIN across the indices of your existing cluster. No new infrastructure. Up to two cross-index JOINs per query are free.

```sql
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.id;
```

**Multi-cluster federation (Pro+).** Deploy the federation server and JOIN across *separate* regional Elasticsearch clusters from one query. The catalog prefix routes each leg to its cluster:

```sql
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_eu`.customers c ON o.customer_id = c.id;
```

Community gets single-cluster cross-index JOINs for free. Cross-cluster federation is Pro+. The boundary is honest and it is the meter, not a feature switch.

---

## The two ES-impossible superpowers

R1 is built around two things Elasticsearch cannot do natively and a do-it-yourself client cannot do either:

1. **Query-time cross-index JOIN** on every surface — REPL, JDBC, ADBC, Arrow Flight SQL, Federation.
2. **Persisted Materialized Views** — a pre-joined, pre-aggregated index you query instantly.

The drivers are the *free* delivery channel for superpower #1. JOIN depth is *metered* — a ladder, not a wall:

- query-time JOINs (`maxJoins`)
- across clusters (`maxClusters`)
- persisted as a Materialized View (`maxMaterializedViews`)

Community sits at the bottom rung of each (2 JOINs / 1 cluster / 1 MV); Pro and Enterprise climb the ladder.

---

## Arrow-native, end to end

The Arrow Flight SQL surface streams results as Arrow columnar batches — zero-copy, no JSON in the hot path. The full benchmark lands in R1.1; for now, know that the columnar path exists and that DuckDB, Pandas, and Polars consume it directly through ADBC.

---

## Get started

- **Run your first JOIN in five minutes:** the [JDBC quickstart](https://softclient4es.dev/integrations/jdbc/).
- **See what each edition includes:** [editions & pricing](https://softclient4es.dev/licensing/).
<!-- pending 17.1: /sql/joins/ (JOIN matrix walkthrough — web PR #12, base release-r1) -->
- **Understand the JOIN matrix (rows 1, 2, 3):** the JOIN matrix walkthrough — `https://softclient4es.dev/sql/joins/` (publishing alongside R1).

R1 is the release where Elasticsearch learned to JOIN. The next post walks the JOIN matrix row by row.

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/
