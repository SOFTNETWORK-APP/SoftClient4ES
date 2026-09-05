# BI Tool Integration

SoftClient4ES connects to common BI and SQL tools through the **JDBC driver** (`jdbc:elastic://host:port`, driver class `app.softnetwork.elastic.jdbc.ElasticDriver`) or the **Arrow Flight SQL** server (`grpc://host:32010`).

Where a supported connection path exists, a full step-by-step guide — connect, browse your indices, and run a cross-index JOIN — lives on the website. This page is an index; the website carries the screenshots and per-tool detail, and states the blocker for the tools that cannot connect.

## Tested, Compatible, Unproven, and no path today

Four tiers, each a different claim:

- **Tested** — exercised against SoftClient4ES.
- **Compatible** — speaks a working protocol, but has not been through formal regression (best-effort).
- **Unproven** — a connection path exists on paper, but nobody has connected it yet. Not a promise.
- **No path today** — cannot connect without software that does not exist, for reasons on the tool's
  own side.

| Tool | Status | Path | Guide |
|---|---|---|---|
| Apache Superset | Tested (dedicated dialect) | Arrow Flight SQL | https://softclient4es.dev/integrations/superset/ |
| DBeaver | Tested | JDBC or Arrow Flight SQL | https://softclient4es.dev/integrations/dbeaver/ |
| Grafana | Tested (via Arrow Flight SQL) | Arrow Flight SQL | https://softclient4es.dev/integrations/grafana/ |
| Tableau | Compatible (not formally tested) | JDBC | https://softclient4es.dev/integrations/tableau/ |
| Power BI | **Unproven** — Power Query's generic connectors are ODBC and OData, never JDBC. The one candidate path is a generic Arrow Flight SQL ODBC driver pointed at the sidecar; it has not been connected yet | ODBC (unproven) | https://softclient4es.dev/integrations/power-bi/ |
| Metabase | **No path today** — Metabase has no generic JDBC database type. Connecting needs a community driver plugin, and we do not ship one | — | https://softclient4es.dev/integrations/metabase/ |
| Looker | **No path today** — Looker-maintained drivers only, with a per-dialect JDBC parameter allowlist. Structural, not commercial: a licence would not close it | — | — |

*(Each blocker was checked against the vendor's own connection documentation — Metabase, Microsoft
Power Query and Looker — on 2026-08-31 and 2026-09-01.)*

## Honest-gap note

The superpower of this release is a **cross-index JOIN** that Elasticsearch can't do, and it runs best
through explicit `JOIN … ON …` SQL — from any tool where you control the statement that is sent (Superset
SQL Lab, DBeaver, Grafana). Some BI tools compose SQL for you: subqueries and CTEs are not in this release
yet, and neither is the quoted, fully-qualified identifier form Tableau generates. Tableau's Custom SQL is
not a way around that — Tableau wraps a custom query inside a `SELECT … FROM ( … )`, which is a derived
table (Tableau's Custom SQL documentation, checked 2026-09-01). Full BI-tool subquery / CTE support is coming in the next release (Quarter 4 2026). See the
website's Known Limitations page for the full picture.
