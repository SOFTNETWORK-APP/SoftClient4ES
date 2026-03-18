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

| Elasticsearch  | Artifact                               |
|----------------|----------------------------------------|
| ES 6.x         | `softclient4es6-adbc-driver-0.1.5.jar` |
| ES 7.x         | `softclient4es7-adbc-driver-0.1.5.jar` |
| ES 8.x         | `softclient4es8-adbc-driver-0.1.5.jar` |
| ES 9.x         | `softclient4es9-adbc-driver-0.1.5.jar` |

### Maven / Gradle / sbt

**Maven:**

```xml
<dependency>
  <groupId>app.softnetwork.elastic</groupId>
  <artifactId>softclient4es8-adbc-driver</artifactId>
  <version>0.1.5</version>
</dependency>
```

**Gradle:**

```groovy
implementation 'app.softnetwork.elastic:softclient4es8-adbc-driver:0.1.5'
```

**sbt:**

```scala
libraryDependencies += "app.softnetwork.elastic" % "softclient4es8-adbc-driver" % "0.1.5"
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

## License

The ADBC driver is licensed under the **Elastic License 2.0** — free to use, not open source.
