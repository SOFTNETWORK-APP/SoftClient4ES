# BI Tool Integration

SoftClient4ES connects to common BI and SQL tools through the **JDBC driver** (`jdbc:elastic://host:port`, driver class `app.softnetwork.elastic.jdbc.ElasticDriver`) or the **Arrow Flight SQL** server (`grpc://host:32010`).

Where a supported connection path exists, a full step-by-step guide — connect, browse your indices, and run a cross-index JOIN — lives on the website. This page is an index; the website carries the screenshots and per-tool detail, and states the blocker for the tools that cannot connect.

## Tested, Compatible, and no supported path

**Tested** tools are exercised against SoftClient4ES. **Compatible** tools speak a working protocol but
have not been through formal regression (best-effort). A third group cannot connect at all, for reasons
on the tool's own side.

| Tool | Status | Path | Guide |
|---|---|---|---|
| Apache Superset | Tested (dedicated dialect) | Arrow Flight SQL | https://softclient4es.dev/integrations/superset/ |
| DBeaver | Tested | JDBC or Arrow Flight SQL | https://softclient4es.dev/integrations/dbeaver/ |
| Grafana | Tested (via Arrow Flight SQL) | Arrow Flight SQL | https://softclient4es.dev/integrations/grafana/ |
| Tableau | Compatible (not formally tested) | JDBC | https://softclient4es.dev/integrations/tableau/ |
| Metabase | **No supported connection path** — Metabase has no generic JDBC database type; anything else needs a community driver plugin | — | https://softclient4es.dev/integrations/metabase/ |
| Power BI | **No supported connection path** — Power Query has no JDBC connector (its generic connectors are ODBC and OData); the one candidate, a generic Arrow Flight SQL ODBC driver, is unproven | — | https://softclient4es.dev/integrations/power-bi/ |
| Looker | **No supported connection path** — Looker-maintained drivers only, with a per-dialect JDBC parameter allowlist. Structural, not commercial: a licence would not close it | — | — |
| dbt | **No supported connection path** — a dedicated adapter plugin is mandatory; no generic JDBC/ODBC adapter exists and no SoftClient4ES adapter exists | — | — |

*(Each "no supported connection path" blocker was checked against the vendor's own connection
documentation — Metabase, Microsoft Power Query, Looker and dbt — on 2026-08-31 and 2026-09-01.)*

## Honest-gap note

The superpower of this release is a **cross-index JOIN** that Elasticsearch can't do, and it runs best
through explicit `JOIN … ON …` SQL — from any tool where you control the statement that is sent (Superset
SQL Lab, DBeaver, Grafana). Some BI tools compose SQL for you: subqueries and CTEs are not in this release
yet, and neither is the quoted, fully-qualified identifier form Tableau generates. Tableau's Custom SQL is
not a way around that — Tableau wraps a custom query inside a `SELECT … FROM ( … )`, which is a derived
table (Tableau's Custom SQL documentation, checked 2026-09-01). Full BI-tool subquery / CTE support is coming in the next release (Quarter 4 2026). See the
website's Known Limitations page for the full picture.
