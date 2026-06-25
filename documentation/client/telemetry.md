# Telemetry & Privacy

SoftClient4ES sends a small, anonymous daily usage ping so we can answer one question with data instead of guesswork: which parts of SoftClient4ES are actually being used. This page tells you exactly what is sent, what is **never** sent, and how to turn it off in one line — on every surface, at every license tier.

**Short version:** one anonymous ping per running instance per day. No IP addresses. No PII. No SQL. No command text. Turning it off has zero impact on functionality or your license.

## Which products send telemetry

All five SoftClient4ES surfaces emit telemetry, each tagged with a `product` field so we can count distinct deployments per surface:

- **JDBC driver** — embedded in your Java/JVM application
- **ADBC driver** — embedded in your process
- **Arrow Flight SQL sidecar** — the per-cluster server
- **Federation server** — the cross-cluster / cross-source coordinator
- **REPL** (`es{N}cli`) — the interactive SQL client

## What is collected

Each instance sends one ping per day containing only aggregate, non-identifying fields:

| Field | What it is |
|---|---|
| `product` | which surface sent the ping (`jdbc_driver`, `adbc_driver`, `flight_sql`, `federation`, `repl` — `flight_sql` is the Arrow Flight SQL sidecar) |
| `instance_id` | a stable identifier for the running instance — **not** for you, your machine's identity, or any query |
| `version` | the SoftClient4ES version |
| `license_tier` | `Community`, `Pro`, or `Enterprise` |
| `uptime_seconds` | how long the instance has been running |
| `join_query_count` | how many JOIN queries ran since the last ping (a number only) |

Some surfaces add a couple of surface-specific fields:

| Surface | Additional fields |
|---|---|
| Federation | `cluster_count` — how many clusters are registered |
| Sidecar (`flight_sql`) | `cluster_name` — the configured name of the cluster it fronts |
| REPL | `session_duration_seconds`, `commands_executed` (a **count** — never the commands themselves) |
| JDBC / ADBC | none |

## What is NOT collected

We deliberately do **not** collect:

- **No IP addresses** — the receiving endpoint never reads or stores your address.
- **No SQL query text** — we never see the queries you run.
- **No REPL command text** — only how many commands ran, never what they were.
- **No personal data (PII)** — no usernames, no emails, no machine names.
- **No data content** — no index names, no field names, no documents, no results.
- **No organisation identity in the payload** — for paid tiers your organisation is correlated server-side from your license token; Community sends no token and is fully anonymous.
- **No query IDs.**

## Collection vs. dashboards — an important distinction

It is worth being precise, because this is the single most common misunderstanding:

- **Counters increment locally, in real time, inside your instance.** A counter ticks up when a JOIN runs — on your machine, in memory.
- **Only an aggregated daily summary leaves your instance** — once per day, the counts (not the events, never the SQL) are sent with the `instance_id`.
- **We do not, and cannot, watch your queries in real time.** There is no live feed of your SQL. Our dashboards roll the daily numbers into per-day and per-week trends — counts of instances and counts of JOINs, nothing query-level.

In short: real-time local counting is not the same as real-time monitoring of what you do.

## Default posture by tier

Telemetry is **on by default for every tier — Community, Pro, and Enterprise.**

Usage matters at every tier, including the free Community tier — if we ignored Community signal we would be blind to the largest group of SoftClient4ES users and would prioritise the wrong things. That is the only reason Community is included. The ping is anonymous, the opt-out is one line, and opting out costs you nothing.

## How to turn it off (per surface)

Opting out is a single configuration line. The daily-ping setting is **`softclient4es.telemetry.enabled`** and is read identically by every surface.

```properties
softclient4es.telemetry.enabled = false
```

> **Two telemetry keys — which one to use:**
>
> | Key | What it controls |
> |---|---|
> | **`softclient4es.telemetry.enabled`** | **The daily product-instance ping described on this page.** Set to `false` to stop it. |
> | `softclient4es.license.telemetry.enabled` | A separate, pre-existing switch that controls only the *detailed operation metrics* attached to a paid-tier license refresh. It does **not** affect the daily ping. Leave it alone unless you specifically want to trim license-refresh metrics. |
>
> To turn off the daily ping, set **`softclient4es.telemetry.enabled = false`** — that is the key for every surface below.

**Sidecar (Arrow Flight SQL server)** — in the server's HOCON config:
```properties
softclient4es.telemetry.enabled = false
```

**Federation server** — in the federation server's HOCON config:
```properties
softclient4es.telemetry.enabled = false
```

**REPL (`es{N}cli`)** — in a HOCON file on the REPL's classpath (e.g. `application.conf`), or as a JVM flag at launch:
```properties
softclient4es.telemetry.enabled = false
```
```bash
# or pass it on the command line
JAVA_OPTS="-Dsoftclient4es.telemetry.enabled=false" softclient4es --host localhost --port 9200
```

**JDBC driver** — provide the setting via a HOCON file on your application's classpath, or as a JVM system property:
```properties
softclient4es.telemetry.enabled = false
```
```bash
-Dsoftclient4es.telemetry.enabled=false
```

**ADBC driver** — provide the setting via the HOCON config your ADBC database is built from, or as a JVM system property:
```properties
softclient4es.telemetry.enabled = false
```
```bash
-Dsoftclient4es.telemetry.enabled=false
```

> The JDBC driver also accepts `?telemetry=false` directly on the JDBC URL. The ADBC driver has no equivalent connection option — use the HOCON / `-D` form above.

## What happens when you opt out

Nothing else changes. With telemetry disabled:

- **Zero functional impact** — every query, JOIN, DDL/DML, and connection behaves exactly the same.
- **Zero licensing impact** — your tier, quotas, grace period, and entitlements are unaffected. Telemetry is never used to enforce your license.
- The daily ping simply is not sent.

## Cadence

- **Once per day** per running instance.
- **REPL exception:** a short-lived REPL session (under 24 hours) sends a single ping when the session ends; a longer session sends a daily ping while running, plus a final ping on exit.
- **Failures never block you.** Telemetry is fire-and-forget: if the endpoint is unreachable, the instance logs it and moves on — your workload is never delayed or interrupted.

## Why we collect this

- **Measure the R1 launch** — did people actually adopt SoftClient4ES?
- **Prioritise the right features** — which surfaces and capabilities get used most.
- **Understand the mix** — how many JDBC vs. ADBC vs. sidecar vs. federation vs. REPL deployments exist, so we invest where it matters.

## Retention

We keep **aggregate counts** (per-day, per-product, per-tier trends) to see trends over time. What is stored is one idempotent row per instance per UTC day — a same-day re-ping overwrites that day's row, and each new UTC day adds a new row — so the table holds at most a daily roll-up per instance, never a moment-by-moment activity log. We keep these aggregate per-day `instance_ping` rows for **13 months**, then delete them. We never build per-customer profiles.

## Questions or data deletion

Questions about telemetry go to **sales@softclient4es.com**.

Because pings carry no personal data and no account identifier, there is nothing tied to *you* to delete. If you want a specific instance's pings removed, include the `instance_id` value from `~/.softclient4es/instance-id` on the machine in question and we will purge any pings carrying it.

[Back to index](README.md)
