# JDBC Driver

SoftClient4ES provides a JDBC Type 4 driver that lets you connect any JDBC-compatible tool to Elasticsearch — DBeaver, IntelliJ DataGrip, Apache Superset, Tableau, and custom Java/Scala applications.

---

## Connection Details

| Property | Value |
|----------|-------|
| **JDBC URL** | `jdbc:elastic://localhost:9200` |
| **Driver class** | `app.softnetwork.elastic.jdbc.ElasticDriver` |
| **Group ID** | `app.softnetwork.elastic` |

---

## Driver JARs

Download the self-contained fat JAR for your Elasticsearch version. The JARs are Scala-version-independent and include all required dependencies.

| Elasticsearch | Artifact |
|---------------|----------|
| ES 6.x | `softclient4es6-jdbc-driver-<R1_DRIVER_VERSION>.jar` |
| ES 7.x | `softclient4es7-jdbc-driver-<R1_DRIVER_VERSION>.jar` |
| ES 8.x | `softclient4es8-jdbc-driver-<R1_DRIVER_VERSION>.jar` |
| ES 9.x | `softclient4es9-jdbc-driver-<R1_DRIVER_VERSION>.jar` |

> Replace `<R1_DRIVER_VERSION>` with the published R1 release tag for the driver JARs.

### Build Tool Integration

**Maven:**

```xml
<dependency>
  <groupId>app.softnetwork.elastic</groupId>
  <artifactId>softclient4es8-jdbc-driver</artifactId>
  <version><R1_DRIVER_VERSION></version>
</dependency>
```

**Gradle:**

```groovy
implementation 'app.softnetwork.elastic:softclient4es8-jdbc-driver:<R1_DRIVER_VERSION>'
```

**sbt:**

```scala
libraryDependencies += "app.softnetwork.elastic" % "softclient4es8-jdbc-driver" % "<R1_DRIVER_VERSION>"
```

---

## JDBC URL Format

```
jdbc:elastic://host:port[?param=value&...]
```

### Authentication Parameters

| Parameter | Description |
|-----------|-------------|
| `user` | Username for basic authentication |
| `password` | Password for basic authentication |
| `api-key` | Elasticsearch API key |
| `bearer` | OAuth/JWT bearer token |
| `scheme` | Connection scheme (`http` or `https`, default: `http`) |

### Examples

```
# Basic connection
jdbc:elastic://localhost:9200

# With authentication
jdbc:elastic://es.example.com:9200?user=elastic&password=changeme

# HTTPS with API key
jdbc:elastic://es.example.com:9243?scheme=https&api-key=your-api-key
```

---

## Your first query

A plain `SELECT` is the fastest way to confirm the connection works:

```sql
SELECT * FROM my_index LIMIT 10;
```

```java
try (Statement stmt = conn.createStatement();
     ResultSet rs = stmt.executeQuery("SELECT * FROM my_index LIMIT 10")) {
    while (rs.next()) {
        System.out.println(rs.getString(1));
    }
}
```

---

## Your first JOIN

Elasticsearch can't JOIN across indices — SoftClient4ES does. **Free in Community: up to 2 cross-index JOINs per query** (a 3-table JOIN). The JOIN runs in an embedded DuckDB engine (`arrow-ext`); your existing single ES cluster needs no extra infrastructure.

A cross-index INNER JOIN over two ES indices — `jdbc_join_emp` (employees) and `jdbc_join_dept` (departments):

```sql
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
-- 5 rows (the orphan employee with dept_id = 99 is dropped by the INNER JOIN)
```

Denormalize the joined result into a brand-new index with `CREATE TABLE … AS SELECT` (CTAS):

```sql
CREATE TABLE jdbc_row1_ctas_join_target AS
SELECT e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id;
```

Upsert the joined rows into an existing index idempotently with `INSERT … ON CONFLICT (col) DO UPDATE`:

```sql
INSERT INTO jdbc_row1_insert_join_upsert_target
SELECT e.emp_id, e.name, e.salary, d.dept_name
FROM jdbc_join_emp e
JOIN jdbc_join_dept d ON e.dept_id = d.dept_id
ON CONFLICT (emp_id) DO UPDATE;
```

> **Note:** `CREATE TABLE … AS … ON CONFLICT` and `INSERT … ON CONFLICT … DO NOTHING` are rejected at the JDBC boundary — use `INSERT … ON CONFLICT … DO UPDATE` for idempotent upserts.

Bind parameters through a JOIN with a `PreparedStatement`:

```java
PreparedStatement ps = conn.prepareStatement(
    "SELECT e.name, d.dept_name " +
    "FROM jdbc_join_emp e " +
    "JOIN jdbc_join_dept d ON e.dept_id = d.dept_id " +
    "WHERE e.salary > ?");
ps.setDouble(1, 3500.0);   // lower threshold → more rows
ResultSet rs1 = ps.executeQuery(); /* … */ rs1.close();
ps.setDouble(1, 6500.0);   // higher threshold → fewer rows (Carol = 8000 survives)
ResultSet rs2 = ps.executeQuery(); /* … */ rs2.close();
```

For the full JOIN matrix, see the Cross-Index JOIN walkthrough (`../sql/joins.md`). <!-- pending 17.1 — wire live on merge -->

---

## Java Example

```java
import java.sql.*;

String url = "jdbc:elastic://localhost:9200";
try (Connection conn = DriverManager.getConnection(url)) {
    try (Statement stmt = conn.createStatement()) {
        // DDL
        stmt.execute("CREATE TABLE IF NOT EXISTS demo (id INT, name VARCHAR, PRIMARY KEY (id))");

        // DML
        stmt.execute("INSERT INTO demo (id, name) VALUES (1, 'Alice'), (2, 'Bob')");

        // DQL
        try (ResultSet rs = stmt.executeQuery("SELECT * FROM demo ORDER BY id")) {
            while (rs.next()) {
                System.out.printf("id=%d, name=%s%n",
                    rs.getInt("id"), rs.getString("name"));
            }
        }

        // Cleanup
        stmt.execute("DROP TABLE IF EXISTS demo");
    }
}
```

---

## Supported SQL

The JDBC driver supports the full SQL Gateway syntax:

- **DDL** — CREATE/ALTER/DROP TABLE, pipelines, watchers, enrich policies
- **DML** — INSERT, UPDATE, DELETE, COPY INTO
- **DQL** — SELECT with WHERE, GROUP BY, HAVING, ORDER BY, LIMIT, UNION ALL, JOIN UNNEST, window functions
- **SHOW/DESCRIBE** — Tables, pipelines, watchers, enrich policies

---

## BI Tool Setup

### DBeaver

1. Open **Database > New Database Connection**
2. Choose **Driver Manager > New**
3. Set Driver Name: `SoftClient4ES`
4. Add the fat JAR file
5. Set Driver Class: `app.softnetwork.elastic.jdbc.ElasticDriver`
6. Set URL Template: `jdbc:elastic://{host}:{port}`
7. Create a new connection using this driver

### IntelliJ DataGrip

1. Open **Database > + > Driver**
2. Add the fat JAR
3. Set Driver Class: `app.softnetwork.elastic.jdbc.ElasticDriver`
4. Create a new data source with URL `jdbc:elastic://localhost:9200`

---

## Version compatibility

| Driver | Scala | ES versions | Clients | Process model |
|--------|-------|-------------|---------|---------------|
| JDBC   | published for Scala 2.13 | ES 6.x / 7.x / 8.x / 9.x | Java/JVM + any JDBC tool (DBeaver, DataGrip, Superset) | In-process |

The fat JARs are **Scala-version-independent for consumers** — they bundle their own Scala runtime, so a Java application (or a Scala 2.12 project) can use the driver without matching Scala versions. You almost never need to think about the Scala axis.

---

## Licensing & self-selection

All client drivers (JDBC, ADBC, and the REPL) plus the Arrow Flight SQL sidecar are **free in Community**, including up to **2 cross-index JOINs per query** (a 3-table JOIN). A 4-table JOIN (a 3rd cross-index JOIN in one query) is rejected by the planner with a message ending `… Upgrade to Pro … See: https://portal.softclient4es.com/pricing`.

Multi-cluster **federation** — joining across *separate* ES clusters — is **Pro+** (`maxClusters` 1 / 5 / ∞). These quickstarts cover the **single-cluster** shape, which is free.

---

## What does NOT work yet

Subqueries (`IN (SELECT …)`, `EXISTS`, scalar, derived tables) and CTEs (`WITH`) are not supported in R1 — they arrive in R2a. Write the JOIN explicitly instead. See the Known Limitations & Roadmap (`../sql/known_limitations.md`) for the full list. <!-- pending 17.6 — wire live on merge -->

---

## Going further

- [ADBC Driver](adbc_driver.md) — the Arrow-native in-process columnar alternative.
- [Arrow Flight SQL](arrow_flight_sql.md) — the gRPC server for polyglot and networked clients.
- Cross-Index JOIN walkthrough (`../sql/joins.md`) — the full JOIN matrix (rows 1/2/3) with worked examples. <!-- pending 17.1 — wire live on merge -->
- Multi-cluster federation operator guide (`federation_operator_guide.md`) — the Pro+ path: JOIN across separate ES clusters. <!-- pending 17.2 — wire live on merge -->
- Known Limitations & Roadmap (`../sql/known_limitations.md`) — what works in R1 vs what's coming. <!-- pending 17.6 — wire live on merge -->

---

## Telemetry

The JDBC driver sends one anonymous usage ping per day (no IP, no SQL, no PII). Opt out either on the JDBC URL — `jdbc:elastic://localhost:9200?telemetry=false` — or with `softclient4es.telemetry.enabled = false` on your application classpath. See [Telemetry & Privacy](telemetry.md) for the full field list and per-surface details.

---

## License

The JDBC driver is licensed under the **Elastic License 2.0** — free to use, not open source.
