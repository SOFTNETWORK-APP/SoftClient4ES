# Arrow Flight SQL

Zero-copy columnar access to Elasticsearch over gRPC — for DuckDB, Python, Apache Superset, and any Arrow Flight SQL client.

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
java -jar softclient4es8-arrow-flight-sql-<version>.jar
```

| Elasticsearch | Artifact |
|---------------|----------|
| ES 6.x | `softclient4es6-arrow-flight-sql-<version>.jar` |
| ES 7.x | `softclient4es7-arrow-flight-sql-<version>.jar` |
| ES 8.x | `softclient4es8-arrow-flight-sql-<version>.jar` |
| ES 9.x | `softclient4es9-arrow-flight-sql-<version>.jar` |

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

## License

Arrow Flight SQL is licensed under the **Elastic License 2.0** — free to use, not open source.
