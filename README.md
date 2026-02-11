# ![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo_375x300.png)

![Build Status](https://github.com/SOFTNETWORK-APP/SoftClient4ES/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES/graph/badge.svg?token=XYCWBGVHAC)](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/1c13d6eb7d6c4a1495cd47e457c132dc)](https://app.codacy.com/gh/SOFTNETWORK-APP/elastic/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![License](https://img.shields.io/github/license/SOFTNETWORK-APP/elastic)](https://github.com/SOFTNETWORK-APP/elastic/blob/main/LICENSE)

**SoftClient4ES** is a powerful SQL gateway for Elasticsearch. Query, manipulate, and manage your Elasticsearch data using familiar SQL syntax — through an interactive **REPL client** or as a **Scala library**.

---

## ⚡ Quick Start — REPL Client

Get started in seconds with the interactive SQL client:

### Installation

**Linux / macOS:**
```bash
curl -fsSL https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/scripts/install.sh | bash
```

**Windows (PowerShell):**
```powershell
irm https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/scripts/install.ps1 | iex
```

### Connect and Query

```bash
softclient4es --host localhost --port 9200
```

```sql
-- Create a table (index)
CREATE TABLE users (
  id KEYWORD,
  name TEXT FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  email KEYWORD,
  age INTEGER,
  created_at DATE,
  PRIMARY KEY (id)
);

-- Insert data
INSERT INTO users (id, name, email, age) 
VALUES ('1', 'Alice', 'alice@example.com', 30);

-- Query with SQL
SELECT name, email, age FROM users WHERE age > 25 ORDER BY name;

-- Update records
UPDATE users SET age = 31 WHERE id = '1';

-- Show tables
SHOW TABLES LIKE 'user%';
```

📖 **[Full REPL Documentation](documentation/client/repl.md)**

---

## 🎯 Why SoftClient4ES?

| Feature                  | Benefit                                                      |
|--------------------------|--------------------------------------------------------------|
| 🗣️ **SQL Interface**    | Use familiar SQL syntax — no need to learn Elasticsearch DSL |
| 🔄 **Version Agnostic**  | Single codebase for Elasticsearch 6, 7, 8, and 9             |
| ⚡ **Interactive REPL**   | Auto-completion, syntax highlighting, persistent history     |
| 🔒 **Type Safe**         | Compile-time SQL validation for Scala applications           |
| 🚀 **Stream Powered**    | Akka Streams for high-performance bulk operations            |
| 🛡️ **Production Ready** | Built-in error handling, validation, and rollback            |

---

## 📋 SQL Support

### DDL — Data Definition Language

```sql
CREATE TABLE products (
  id KEYWORD,
  name TEXT FIELDS(
    raw KEYWORD
  ) OPTIONS (fielddata = true),
  email KEYWORD,
  price DOUBLE,
  tags KEYWORD,
  PRIMARY KEY (id)
);

ALTER TABLE products ADD COLUMN stock INTEGER;
DESCRIBE TABLE products;
DROP TABLE old_products;
TRUNCATE TABLE logs;
```

📖 **[DDL Documentation](documentation/sql/ddl_statements.md)**

### DML — Data Manipulation Language

```sql
INSERT INTO products (id, name, price) VALUES ('p1', 'Laptop', 999.99);
UPDATE products SET price = 899.99 WHERE id = 'p1';
DELETE FROM products WHERE price < 10;
COPY INTO products FROM '/data/products.json';
```

📖 **[DML Documentation](documentation/sql/dml_statements.md)**

### DQL — Data Query Language

```sql
SELECT name, price, COUNT(*) as sales
FROM products
WHERE category = 'electronics'
GROUP BY name, price
HAVING COUNT(*) > 10
ORDER BY sales DESC
LIMIT 100;
```

**Supported features:** `JOIN UNNEST`, window functions, aggregations, nested fields, geospatial queries, and more.

📖 **[DQL Documentation](documentation/sql/dql_statements.md)**

---

## 🛠️ Scala Library Integration

For programmatic access, add SoftClient4ES to your project:

```scala
// build.sbt
resolvers += "Softnetwork" at "https://softnetwork.jfrog.io/artifactory/releases/"

// Choose your Elasticsearch version
libraryDependencies += "app.softnetwork.elastic" %% "softclient4es8-java-client" % "0.16.0"
```

```scala
import app.softnetwork.elastic.client._

val client = ElasticClientFactory.create()

// SQL queries
val results = client.search(SQLQuery("SELECT * FROM users WHERE age > 25"))

// Type-safe queries with compile-time validation
case class User(id: String, name: String, age: Int)
val users: Source[User, NotUsed] = client.scrollAs[User](
  "SELECT id, name, age FROM users WHERE active = true"
)
```

📖 **[API Documentation](documentation/client/README.md)**

---

## ✨ Key Features

### 🔀 Zero-Downtime Mapping Migration

Automatically migrate index mappings with rollback support:

```scala
client.updateMapping("users", newMapping) // Handles backup, reindex, and rollback
```

📖 **[Mapping Migration Guide](documentation/client/mappings.md)**

### 📦 High-Performance Bulk Operations

Stream millions of documents with backpressure handling:

```scala
client.bulkFromFile("/data/products.parquet", format = Parquet, idKey = Some("id"))
```

Supported formats: JSON, NDJSON, Parquet, Delta Lake

📖 **[Bulk API Guide](documentation/client/bulk.md)**

### 🔍 Smart Scroll API

Automatically selects the optimal strategy (PIT, search_after, or scroll):

```scala
client.scroll(SQLQuery("SELECT * FROM logs WHERE level = 'ERROR'"))
  .runWith(Sink.foreach(processDocument))
```

📖 **[Scroll API Guide](documentation/client/scroll.md)**

### 🔗 Akka Persistence Integration

Seamlessly sync event-sourced systems with Elasticsearch.

📖 **[Event Sourcing Guide](documentation/client/persistence.md)**

---

## 📚 Documentation

| Topic             | Link                                               |
|-------------------|----------------------------------------------------|
| **REPL Client**   | [📖 Documentation](documentation/client/repl.md)   |
| **SQL Reference** | [📖 Documentation](documentation/sql/README.md)    |
| **API Reference** | [📖 Documentation](documentation/client/README.md) |

---

## 🗺️ Roadmap

- [ ] JDBC connector for Elasticsearch
- [ ] Advanced monitoring dashboard
- [ ] Additional SQL functions

---

## 🤝 Contributing

Contributions are welcome! See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

---

## 📄 License

Licensed under the **Apache License 2.0** — see [LICENSE](LICENSE) for details.

---

## 💬 Support

- 🐛 [GitHub Issues](https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues)
- 💬 [GitHub Discussions](https://github.com/SOFTNETWORK-APP/SoftClient4ES/discussions)
- 📧 admin@softnetwork.fr

---

<p align="center">
  <strong>Built with ❤️ by the SoftNetwork team</strong>
</p>
```
