# BI Tool Integration

SoftClient4ES connects to common BI and SQL tools through the **JDBC driver** (`jdbc:elastic://host:port`, driver class `app.softnetwork.elastic.jdbc.ElasticDriver`) or the **Arrow Flight SQL** server (`grpc://host:32010`).

Full, step-by-step guides — connect, browse your indices, and run a cross-index JOIN — live on the website. This page is an index; the website carries the screenshots and per-tool detail.

## Tested vs Compatible

**Tested** tools are exercised against SoftClient4ES. **Compatible** tools speak a working protocol but have not been through formal regression (best-effort).

| Tool | Tier | Path | Guide |
|---|---|---|---|
| Apache Superset | Tested (dedicated dialect) | Arrow Flight SQL | https://softclient4es.dev/integrations/superset/ |
| DBeaver | Tested | JDBC or Arrow Flight SQL | https://softclient4es.dev/integrations/dbeaver/ |
| Grafana | Tested (via Arrow Flight SQL) | Arrow Flight SQL | https://softclient4es.dev/integrations/grafana/ |
| Tableau | Compatible (not formally tested) | JDBC | https://softclient4es.dev/integrations/tableau/ |
| Power BI | Compatible (not formally tested) | JDBC | https://softclient4es.dev/integrations/power-bi/ |
| Metabase | Compatible (not formally tested) | JDBC | https://softclient4es.dev/integrations/metabase/ |

## Honest-gap note

Every tool runs the R1 superpower — a **cross-index JOIN** that Elasticsearch can't do — best through explicit `JOIN … ON …` SQL. Some BI tools auto-generate nested subqueries (Tableau live connections, Power BI DirectQuery relationships, the Metabase GUI Question builder); subqueries and CTEs are not in R1 yet. Use Extract / Import / Native-SQL mode with explicit JOINs as the workaround. Full BI-tool subquery / CTE support is coming in R2a. See the website's Known Limitations page for the full picture.
