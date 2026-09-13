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

## Tableau: skipping the temp-table probe (`.tdc`)

On every connection Tableau checks whether it can create a temporary table, by issuing a
`CREATE TABLE` / `DROP TABLE` pair against a generated name. SoftClient4ES has no temporary tables —
an Elasticsearch index is cluster-global and has no session scope — so that probe is refused with an
HTTP 400. Refusing is a **supported** path: Tableau's own connector documentation says that when the
temp-table capabilities are disabled *"Tableau will attempt to generate an alternative query to
retrieve the necessary results."*

[`tableau/softclient4es.tdc`](tableau/softclient4es.tdc) tells Tableau the answer up front, so it
goes straight to that alternative instead of discovering it by failing. Copy the file into Tableau's
`Datasources` directory and restart Tableau:

| Product | Directory |
|---|---|
| Tableau Desktop (Windows) | `Documents\My Tableau Repository\Datasources` |
| Tableau Desktop (macOS) | `~/Documents/My Tableau Repository/Datasources` |
| Tableau Server (Windows) | `ProgramData\Tableau\Tableau Server\data\tabsvc\vizqlserver\Datasources` |
| Tableau Server (Linux) | `/var/opt/tableau/tableau_server/data/tabsvc/vizqlserver/Datasources` |

The extension must be `.tdc`; the file name itself does not matter. It applies to **Other Databases
(JDBC)** connections made with the SoftClient4ES driver.

**The trade-off, stated:** Tableau's fallback uses subqueries, and Tableau's own documentation says
that path *"can be poor, particularly with large datasets."* SoftClient4ES does not accept subqueries
yet either — relational closure is the next release's work. This file buys a clean, immediate "no"
instead of a failed probe on every connection; it does not make Tableau faster.

### What is verified, and what is not

> ⚠️ **This file has NOT been exercised against a live Tableau Desktop.** It is published because
> the refusal it declares is real and the file can only help; do not read it as a tested integration.

Per capability:

- `CAP_CREATE_TEMP_TABLES` and `CAP_SELECT_INTO` — **documented by Tableau for JDBC connections**, in
  its JDBC Capability Customizations Reference.
- `CAP_SUPPRESS_TEMP_TABLE_CHECKS` — **not documented for JDBC or ODBC**. It appears only in the
  Tableau Connector SDK capability list, whose capabilities are declared by a packaged `.taco`
  connector, and we ship no `.taco`. Whether a plain `.tdc` on a generic JDBC connection honours it
  is unknown to us. It is included because it is harmless if ignored, not because it is known to
  work.

**What would settle it:** connect a real Tableau Desktop through the JDBC driver twice — once with
this file installed, once without — and capture the SQL Tableau actually emits (p6spy on the JDBC
seam is how the statements in our BI corpus were captured). Two things to read off that capture: (1)
whether the `CREATE TABLE` / `DROP TABLE` probe pair disappears, which is the only direct evidence
`CAP_SUPPRESS_TEMP_TABLE_CHECKS` reached a generic JDBC connection at all; and (2) whether the
statement mix Tableau then emits has a **higher or lower** acceptance rate than without the file. If
the suppressed-probe mix is accepted less often, or if any interaction that works today stops
working, the right answer is to ship no `.tdc` — which is a one-line change on the user's side, and
why publishing it by default is safe.

## Honest-gap note

The superpower of this release is a **cross-index JOIN** that Elasticsearch can't do, and it runs best
through explicit `JOIN … ON …` SQL — from any tool where you control the statement that is sent (Superset
SQL Lab, DBeaver, Grafana). Some BI tools compose SQL for you: subqueries and CTEs are not in this release
yet, and neither is the quoted, fully-qualified identifier form Tableau generates. Tableau's Custom SQL is
not a way around that — Tableau wraps a custom query inside a `SELECT … FROM ( … )`, which is a derived
table (Tableau's Custom SQL documentation, checked 2026-09-01). Full BI-tool subquery / CTE support is coming in the next release (Quarter 4 2026). See the
website's Known Limitations page for the full picture.
