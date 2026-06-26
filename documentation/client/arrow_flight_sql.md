# Arrow Flight SQL

Zero-copy columnar access to Elasticsearch over gRPC — for DuckDB, Python, Apache Superset, and any Arrow Flight SQL client.

---

## Sidecar vs federation

There are two Flight SQL servers, and these quickstarts cover only the first:

- **Single-cluster sidecar** (port `32010`) — one server in front of **one** ES cluster. Cross-index JOINs run in the sidecar's embedded DuckDB engine. **This is what this page covers, and it is free in Community.**
- **Multi-cluster federation server** — a coordinator that fans out across **multiple** ES clusters to JOIN data living in different clusters. This is the **Pro+** path; see the federation operator guide (`federation_operator_guide.md`).

---

## Features

- **gRPC Protocol** — High-performance columnar data access over HTTP/2
- **DBeaver / Superset / DataGrip** — Works with any Arrow Flight SQL client
- **Docker Ready** — Pre-built Docker images for ES 6, 7, 8, and 9
- **Lazy Streaming** — Memory-efficient transfers; only the current batch resides in memory
- **Batch Streaming** — Configurable batch size via `arrow.flight.batch-size`
- **Full SQL** — DDL + DML + DQL, not just SELECT

---

## Quick Start

### Docker

```bash
docker run -p 32010:32010 \
  -e ES_HOST=elasticsearch \
  -e ES_PORT=9200 \
  -e ES_USER=elastic \
  -e ES_PASSWORD=changeme \
  softnetwork/softclient4es8-arrow-flight-sql:latest
```

Available images per ES version:

| Elasticsearch | Docker Image |
|---------------|-------------|
| ES 6.x | `softnetwork/softclient4es6-arrow-flight-sql:latest` |
| ES 7.x | `softnetwork/softclient4es7-arrow-flight-sql:latest` |
| ES 8.x | `softnetwork/softclient4es8-arrow-flight-sql:latest` |
| ES 9.x | `softnetwork/softclient4es9-arrow-flight-sql:latest` |

### Fat JAR

```bash
java -jar softclient4es8-arrow-flight-sql-0.2.0.jar
```

| Elasticsearch | Artifact |
|---------------|----------|
| ES 6.x | `softclient4es6-arrow-flight-sql-0.2.0.jar` |
| ES 7.x | `softclient4es7-arrow-flight-sql-0.2.0.jar` |
| ES 8.x | `softclient4es8-arrow-flight-sql-0.2.0.jar` |
| ES 9.x | `softclient4es9-arrow-flight-sql-0.2.0.jar` |

---

## Python + DuckDB

```python
import adbc_driver_flightsql.dbapi as flight_sql
import duckdb

conn = flight_sql.connect("grpc://localhost:32010")
cursor = conn.cursor()
cursor.execute("SELECT * FROM ecommerce")
table = cursor.fetch_arrow_table()   # zero-copy Arrow table

duckdb.sql("""
  SELECT category, SUM(total_price) AS revenue
  FROM table
  GROUP BY category
  ORDER BY revenue DESC
""")
```

## Your first JOIN

Elasticsearch can't JOIN across indices — the Flight SQL sidecar does. **Free in Community: up to 2 cross-index JOINs per query** (a 3-table JOIN). Both indices live in the **one** ES cluster the sidecar fronts; the JOIN runs in the sidecar's embedded DuckDB engine:

```python
cursor.execute("""
  SELECT e.name, e.salary, d.dept_name
  FROM jdbc_join_emp e
  JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
""")
table = cursor.fetch_arrow_table()
# 5 rows (the orphan employee with dept_id = 99 is dropped by the INNER JOIN)
```

To JOIN across *separate* ES clusters, deploy the federation server instead — see [Going further](#going-further).

---

## Apache Superset

Connect Superset via the Arrow Flight SQL dialect. Then query Elasticsearch directly from any dashboard:

```sql
SELECT
  customer_name,
  SUM(total_price) AS revenue,
  COUNT(*) AS orders
FROM ecommerce
GROUP BY customer_name
ORDER BY revenue DESC;
```

---

## Live Demo

```bash
docker compose --profile duckdb up          # DuckDB + Python pipeline
docker compose --profile superset-flight up # Apache Superset BI dashboards
```

---

## Configuration

```hocon
arrow.flight {
  host       = "0.0.0.0"   # env: ARROW_HOST
  port       = 32010        # env: ARROW_PORT
  batch-size = 1000         # env: ARROW_BATCH_SIZE
}

elastic.credentials {
  host     = "localhost"    # env: ES_HOST
  port     = 9200           # env: ES_PORT
  user     = "elastic"      # env: ES_USER
  password = "changeme"     # env: ES_PASSWORD
}
```

---

## Arrow Type Mapping

| SQL Type        | ES Type     | Arrow Type                  |
|-----------------|-------------|------------------------------|
| `TINYINT`       | `byte`      | `Int(32)`                   |
| `SMALLINT`      | `short`     | `Int(32)`                   |
| `INT`           | `integer`   | `Int(32)`                   |
| `BIGINT`        | `long`      | `Int(64)`                   |
| `REAL`          | `float`     | `Float(SINGLE)`             |
| `DOUBLE`        | `double`    | `Float(DOUBLE)`             |
| `BOOLEAN`       | `boolean`   | `Bool`                      |
| `DATE`          | `datetime`  | `Date(MILLISECOND)`         |
| `TIMESTAMP`     | `datetime`  | `Timestamp(MS)`             |
| `VARCHAR`       | `text`      | `Utf8`                      |
| `KEYWORD`       | `keyword`   | `Utf8`                      |
| `VARBINARY`     | `binary`    | `Binary`                    |
| `STRUCT`        | `object`    | `Struct`                    |
| `GEO_POINT`     | `geo_point` | `Struct{Float64, Float64}`  |
| `ARRAY<*>`      | —           | `List`                      |
| `ARRAY<STRUCT>` | `nested`    | `List<Struct>`              |

---

## In-process alternative: the ADBC driver

If you need columnar Arrow access **without** running a separate server, the in-process **ADBC driver** delivers the same SQL (including cross-index JOIN) inside your JVM — see the [ADBC Driver](adbc_driver.md) page. ADBC is Java/JVM in-process; polyglot clients (Python, Go, DuckDB, C++) connect to *this* Flight SQL server.

---

## Version compatibility

| Driver | Scala | ES versions | Clients | Process model |
|--------|-------|-------------|---------|---------------|
| Arrow Flight SQL | cross-built Scala 2.12 + 2.13 (server) | ES 6.x / 7.x / 8.x / 9.x | Any ADBC/Flight SQL client — Python, Go, DuckDB, C++, Grafana, Superset | Separate server (gRPC) |

The fat JARs are **Scala-version-independent for consumers** — you almost never need to think about the Scala axis.

---

## Licensing & self-selection

All client drivers (JDBC, ADBC, and the REPL) plus the Arrow Flight SQL sidecar are **free in Community**, including up to **2 cross-index JOINs per query** (a 3-table JOIN). A 4-table JOIN (a 3rd cross-index JOIN in one query) is rejected by the planner with a message ending `… Upgrade to Pro … See: https://portal.softclient4es.com/pricing`.

The single-cluster sidecar on this page is the free shape. Multi-cluster **federation** — joining across *separate* ES clusters — is **Pro+** (`maxClusters` 1 / 5 / ∞).

---

## What does NOT work yet

Subqueries (`IN (SELECT …)`, `EXISTS`, scalar, derived tables) and CTEs (`WITH`) are not supported in the current release — they arrive in a later release. Write the JOIN explicitly instead. See the Known Limitations & Roadmap (`../sql/known_limitations.md`) for the full list.

---

## Going further

- [ADBC Driver](adbc_driver.md) — the in-process columnar alternative (no separate server).
- Cross-Index JOIN walkthrough (`../sql/joins.md`) — the full JOIN matrix (rows 1/2/3) with worked examples.
- Multi-cluster federation operator guide (`federation_operator_guide.md`) — the Pro+ path: JOIN across separate ES clusters.
- Known Limitations & Roadmap (`../sql/known_limitations.md`) — what works in the current release vs what's coming.

---

## Telemetry

The Arrow Flight SQL sidecar sends one anonymous usage ping per day (no IP, no SQL, no PII). Opt out with `softclient4es.telemetry.enabled = false` in the server HOCON config, the `SOFTCLIENT4ES_TELEMETRY_ENABLED=false` environment variable, or `-Dsoftclient4es.telemetry.enabled=false`. The sidecar has no connection-string opt-out — the `grpc://` URI carries no `telemetry` option. See [Telemetry & Privacy](telemetry.md) for details.

---

## Known limitations

Subqueries, CTEs (`WITH`), and set operators beyond `UNION ALL` are not in R1 — and some BI tools auto-generate them. See [Known Limitations & Roadmap](../sql/known_limitations.md) for exactly what works today, what's coming in R2a, and the per-tool workaround.

---

## License

Arrow Flight SQL is licensed under the **Elastic License 2.0** — free to use, not open source.
