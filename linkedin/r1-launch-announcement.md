# LinkedIn — R1 Launch Announcement

> **Angle** : R1 ships cross-index JOIN on Elasticsearch — single-cluster (free) and cross-cluster (Pro+). Stop ETL'ing ES just to JOIN it.
> **Cible** : Data engineers, data analysts, platform/SRE teams on Elasticsearch
> **Demo** : `docker compose --profile superset-flight up`
> **Timing** : J+0 — publier le jour du lancement R1

---

🚀 SoftClient4ES R1 is out — and Elasticsearch can finally JOIN.

You have orders in one index and customers in another. You want revenue per customer. In SQL that's one line. In Elasticsearch it's a project: denormalize, or ETL both indices into a warehouse just to JOIN them there.

R1 ends that.

Cross-index JOIN, query-time, over the surfaces you already use — JDBC, ADBC, Arrow Flight SQL, and the REPL.

Two deployment shapes. You self-select:

✅ Single-cluster — free in Community. Drop in a driver, JOIN across the indices of your existing cluster. No new infrastructure.

```sql
SELECT e.name, d.dept_name
FROM employees e
JOIN departments d ON e.dept_id = d.id;
```

🌍 Multi-cluster federation — Pro+. Deploy the federation server, JOIN across regional clusters from one query:

```sql
SELECT o.id, c.name
FROM `prod_us`.orders o
JOIN `prod_eu`.customers c ON o.customer_id = c.id;
```

Community gets single-cluster cross-index JOINs for free (up to 2 per query). Cross-cluster is Pro+. The meter is the paywall, not a feature switch.

The two things Elasticsearch can't do natively — query-time cross-index JOIN, and persisted Materialized Views — are exactly what R1 is built around.

Run your first JOIN in five minutes 👉 https://softclient4es.dev/integrations/jdbc/
Read the announcement 👉 https://softclient4es.dev/

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/

#Elasticsearch #SQL #DataEngineering #JOIN #OpenSource #ArrowFlightSQL
