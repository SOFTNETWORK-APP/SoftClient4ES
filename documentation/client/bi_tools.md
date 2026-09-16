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

## Tableau: the temp-table probe

On every connection Tableau checks whether it can create a temporary table, by issuing a
`CREATE TABLE` / `DROP TABLE` pair against a generated name. SoftClient4ES has no temporary tables —
an Elasticsearch index is cluster-global and has no session scope — so that pair is refused with an
HTTP 400 naming the statement and the reason, and Tableau moves on. Refusing is a **supported** path:
Tableau's own connector documentation says that when the temp-table capabilities are disabled
*"Tableau will attempt to generate an alternative query to retrieve the necessary results."*

The probe therefore costs one failed round trip per connection and is not itself a problem. What
follows it is Tableau's alternative for a source without temporary tables, which uses **subqueries** —
and **since engine `0.24.0`** subqueries and derived tables are accepted. Tableau's own documentation warns
that the subquery path *"can be poor, particularly with large datasets"*, so it is a performance
characteristic to watch rather than a refusal. Note that a derived table runs on the relational engine —
since engine `0.24.0` with arrow-extensions `0.3.4` — and the JDBC driver ships it, so a Tableau
connection has it.

**A Tableau datasource customization file (`.tdc`) cannot suppress the probe.** The capability that
would do it, `CAP_SUPPRESS_TEMP_TABLE_CHECKS`, is not among the capabilities Tableau documents for
JDBC connections — [JDBC Capability Customizations
Reference](https://help.tableau.com/current/pro/desktop/en-us/jdbc_capabilities.htm) lists
`CAP_CREATE_TEMP_TABLES` and around sixty others, but not that one; it belongs to the Connector SDK
capability set, which a packaged `.taco` connector declares. We ship no `.tdc`, and writing one is a
dead end worth not walking down.

## Honest-gap note

The superpower of this release is a **cross-index JOIN** that Elasticsearch can't do. It runs from explicit
`JOIN … ON …` SQL and, **since engine `0.24.0`**, from the nested SQL a BI tool composes for you:
**subqueries and derived tables are accepted**, and so is the quoted, fully-qualified identifier form
Tableau generates.
Tableau's Custom SQL wraps your query inside a `SELECT … FROM ( … )` (Tableau's Custom SQL documentation,
checked 2026-09-01) — that wrapper is a derived table, which now runs on the relational engine the JDBC
driver ships.

What is still missing for a tool that composes SQL: **CTEs** (`WITH …`) and **set operators beyond
`UNION ALL`**, neither of which is supported yet. See the website's Known Limitations page for the full
picture, including the subquery forms that are still refused by name.
