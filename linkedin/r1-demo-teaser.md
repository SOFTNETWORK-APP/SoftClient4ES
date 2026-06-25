# LinkedIn — R1 Demo Teaser

> **Angle** : Démo/screenshot — un JOIN single-cluster live dans Superset (ou le REPL), une seule commande docker-compose.
> **Cible** : Data analysts, data engineers, équipes BI sur Elasticsearch
> **Demo** : `docker compose --profile superset-flight up`
> **Timing** : J+7 — une semaine après le lancement

---

📊 A cross-index JOIN on Elasticsearch — running live in Apache Superset. One docker-compose command.

Last week R1 shipped cross-index JOIN. This week, watch it.

```bash
docker compose --profile superset-flight up
```

Superset opens fully provisioned. Point it at the Arrow Flight SQL endpoint and run:

```sql
SELECT e.name, d.dept_name, e.salary
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.id
ORDER BY e.salary DESC;
```

Two Elasticsearch indices. One JOIN. No Lucene, no JSON DSL, no warehouse in between.

[SCREENSHOT: Superset results grid showing the joined employees × departments rows — capture from the demo profile before posting.]

This is the free single-cluster path — up to 2 cross-index JOINs per query in Community. The same query runs unchanged from the REPL, JDBC, and ADBC.

Try it yourself 👉 https://softclient4es.dev/integrations/jdbc/

🔗 GitHub: https://github.com/SOFTNETWORK-APP/SoftClient4ES
💼 Follow for more: https://www.linkedin.com/company/softnetwork-app/

#Elasticsearch #ApacheSuperset #SQL #BI #DataEngineering #ArrowFlightSQL
