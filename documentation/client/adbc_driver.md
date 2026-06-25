# ADBC Driver (Arrow Database Connectivity)

In-process columnar access to Elasticsearch — direct Arrow format, no separate server required.

---

## Features

- **In-Process** — No separate server required, runs entirely within your JVM
- **Columnar Native** — Direct Arrow format, ideal for analytics and data engineering
- **Lazy Streaming** — Memory-efficient row consumption via `CloseableRowIterator`; only the current batch resides in memory
- **ServiceLoader Discovery** — Automatic driver registration via `AdbcDriverManager`
- **Bulk Ingest** — Arrow-to-SQL bulk data loading
- **Full SQL** — DDL + DML + DQL

---

## Driver Setup

### Fat JAR

Download the self-contained fat JAR for your Elasticsearch version:

| Elasticsearch  | Artifact                                            |
|----------------|-----------------------------------------------------|
| ES 6.x         | `softclient4es6-adbc-driver-0.2.0.jar` |
| ES 7.x         | `softclient4es7-adbc-driver-0.2.0.jar` |
| ES 8.x         | `softclient4es8-adbc-driver-0.2.0.jar` |
| ES 9.x         | `softclient4es9-adbc-driver-0.2.0.jar` |

### Maven / Gradle / sbt

**Maven:**

```xml
<dependency>
  <groupId>app.softnetwork.elastic</groupId>
  <artifactId>softclient4es8-adbc-driver</artifactId>
  <version>0.2.0</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'app.softnetwork.elastic:softclient4es8-adbc-driver:0.2.0'
```

**sbt:**

```scala
libraryDependencies += "app.softnetwork.elastic" % "softclient4es8-adbc-driver" % "0.2.0"
```

---

## Usage

### Java / Scala

```scala
import org.apache.arrow.adbc.core._
import org.apache.arrow.adbc.drivermanager.AdbcDriverManager
import org.apache.arrow.memory.RootAllocator

val allocator = new RootAllocator()
val params = new java.util.HashMap[String, AnyRef]()
params.put(AdbcDriver.PARAM_URI.getKey,
  "adbc:elastic://localhost:9200?user=elastic&password=changeme")

val db   = AdbcDriverManager.getInstance().connect(params, allocator)
val conn = db.connect()
val stmt = conn.createStatement()
stmt.setSqlQuery("SELECT * FROM my_index LIMIT 10")
val result = stmt.executeQuery()
val reader = result.getReader

while (reader.loadNextBatch()) {
  val root = reader.getVectorSchemaRoot
  // Process Arrow columnar data...
}

reader.close(); stmt.close(); conn.close(); db.close(); allocator.close()
```

### Connection URI

```
# Username / password
adbc:elastic://<host>:<port>?user=<user>&password=<password>

# API key
adbc:elastic://<host>:<port>?api-key=<api-key>

# Bearer token
adbc:elastic://<host>:<port>?bearer=<token>
```

---

## Your first JOIN

Elasticsearch can't JOIN across indices — SoftClient4ES does. **Free in Community: up to 2 cross-index JOINs per query** (a 3-table JOIN). ADBC routes the JOIN through the same embedded DuckDB engine as the JDBC driver, so the SQL is identical — only the connection setup differs:

```sql
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
-- 5 rows (the orphan employee with dept_id = 99 is dropped by the INNER JOIN)
```

Set this as the statement's SQL (`stmt.setSqlQuery(...)`) and read the Arrow stream as shown in [Usage](#usage). For the full JOIN matrix, see the Cross-Index JOIN walkthrough (`../sql/joins.md`).

---

## Configuration

```hocon
adbc.elastic {
  batch-size            = 1000  # env: ADBC_BATCH_SIZE
  query-timeout-seconds = 120   # env: ADBC_QUERY_TIMEOUT
}

elastic.credentials {
  host     = "localhost"  # env: ES_HOST
  port     = 9200         # env: ES_PORT
  # Choose one authentication method:
  user     = "elastic"    # env: ES_USER      (username/password)
  password = "changeme"   # env: ES_PASSWORD
  # api-key  = ""         # env: ES_API_KEY   (API key)
  # bearer   = ""         # env: ES_BEARER    (bearer token)
}
```

---

## ADBC vs JDBC vs Arrow Flight SQL

| Feature           | JDBC                  | ADBC                        | Arrow Flight SQL         |
|-------------------|-----------------------|-----------------------------|--------------------------|
| **Process model** | In-process            | In-process                  | Separate server (gRPC)   |
| **Data format**   | Row-based (ResultSet) | Columnar (Arrow)            | Columnar (Arrow)         |
| **Protocol**      | JDBC API              | ADBC API                    | gRPC (HTTP/2)            |
| **Use case**      | Java apps, BI tools   | Analytics, data engineering | Multi-client, networked  |
| **Setup**         | JAR on classpath      | JAR on classpath            | Docker/server deployment |

---

## Version compatibility

| Driver | Scala | ES versions | Clients | Process model |
|--------|-------|-------------|---------|---------------|
| ADBC   | cross-built Scala 2.12 + 2.13 | ES 6.x / 7.x / 8.x / 9.x | Java/JVM only (polyglot → Flight SQL) | In-process |

The fat JARs are **Scala-version-independent for consumers** — they bundle their own Scala runtime, so you almost never need to think about the Scala axis. For non-JVM languages (Python, Go, DuckDB, C++), use the [Arrow Flight SQL server](arrow_flight_sql.md) with the Arrow project's standard `adbc_driver_flightsql` client.

---

## Licensing & self-selection

All client drivers (JDBC, ADBC, and the REPL) plus the Arrow Flight SQL sidecar are **free in Community**, including up to **2 cross-index JOINs per query** (a 3-table JOIN). A 4-table JOIN (a 3rd cross-index JOIN in one query) is rejected by the planner with a message ending `… Upgrade to Pro … See: https://portal.softclient4es.com/pricing`.

Multi-cluster **federation** — joining across *separate* ES clusters — is **Pro+** (`maxClusters` 1 / 5 / ∞). This quickstart covers the **single-cluster** shape, which is free.

---

## What does NOT work yet

Subqueries (`IN (SELECT …)`, `EXISTS`, scalar, derived tables) and CTEs (`WITH`) are not supported in the current release — they arrive in a later release. Write the JOIN explicitly instead. See the Known Limitations & Roadmap (`../sql/known_limitations.md`) for the full list.

---

## Going further

- [JDBC Driver](jdbc.md) — the row-based in-process alternative for any JDBC tool.
- [Arrow Flight SQL](arrow_flight_sql.md) — the gRPC server for polyglot and networked clients.
- Cross-Index JOIN walkthrough (`../sql/joins.md`) — the full JOIN matrix (rows 1/2/3) with worked examples.
- Multi-cluster federation operator guide (`federation_operator_guide.md`) — the Pro+ path: JOIN across separate ES clusters.
- Known Limitations & Roadmap (`../sql/known_limitations.md`) — what works in the current release vs what's coming.

---

## Telemetry

The ADBC driver sends one anonymous usage ping per day (no IP, no SQL, no PII). Opt out with `softclient4es.telemetry.enabled = false` in your HOCON config, the `SOFTCLIENT4ES_TELEMETRY_ENABLED=false` environment variable, or `-Dsoftclient4es.telemetry.enabled=false`. ADBC has no connection-string opt-out — the `adbc:elastic://` URI carries no `telemetry` option. See [Telemetry & Privacy](telemetry.md) for details.

---

## License

The ADBC driver is licensed under the **Elastic License 2.0** — free to use, not open source.
