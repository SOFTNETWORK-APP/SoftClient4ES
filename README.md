# ![SoftClient4ES Logo](https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/logo.png)


![Build Status](https://github.com/SOFTNETWORK-APP/SoftClient4ES/workflows/Build/badge.svg)
[![codecov](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES/graph/badge.svg?token=XYCWBGVHAC)](https://codecov.io/gh/SOFTNETWORK-APP/SoftClient4ES)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/1c13d6eb7d6c4a1495cd47e457c132dc)](https://app.codacy.com/gh/SOFTNETWORK-APP/elastic/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![License](https://img.shields.io/github/license/SOFTNETWORK-APP/elastic)](https://github.com/SOFTNETWORK-APP/elastic/blob/main/LICENSE)

**SoftClient4ES** is a modular and version-resilient interface built on top of Elasticsearch clients, providing a unified and stable API that simplifies migration across Elasticsearch versions, accelerates development, and offers advanced features for search, indexing, and data manipulation.

## Key Features

**Unified Elasticsearch API**  
This project provides a trait-based interface (`ElasticClientApi`) that aggregates the core functionalities of Elasticsearch: indexing, searching, updating, deleting, mapping, aliases, refreshing, and more. This design abstracts the underlying client implementation and ensures compatibility across different Elasticsearch versions.

- `JestClientApi`: For Elasticsearch 6 using the open-source [Jest client](https://github.com/searchbox-io/Jest).
- `RestHighLevelClientApi`: For Elasticsearch 6 and 7 using the official high-level REST client.
- `ElasticsearchClientApi`: For Elasticsearch 8 and 9 using the official Java client.

By relying on these concrete implementations, developers can switch between versions with minimal changes to their business logic.

**SQL to Elasticsearch Query Translation**  
Elastic Client includes a parser capable of translating SQL `SELECT` queries into Elasticsearch queries. The parser produces an intermediate representation, which is then converted into [Elastic4s](https://github.com/sksamuel/elastic4s) DSL queries and ultimately into native Elasticsearch queries. This allows data engineers and analysts to express queries in familiar [SQL](documentation/README.md) syntax.

**Dynamic Mapping Migration**  
Elastic Client provides tools to analyze and compare existing mappings with new ones. If differences are detected, it can automatically perform safe migrations. This includes creating temporary indices, reindexing, and renaming — all while preserving data integrity. This eliminates the need for manual mapping migrations and reduces downtime.

**High-Performance Bulk API with Akka Streams**  
Bulk operations leverage the power of Akka Streams to efficiently process and index large volumes of data. This stream-based approach improves performance, resilience, and backpressure handling, especially for real-time or high-throughput indexing scenarios.

**Akka Persistence Integration**  
The project offers seamless integration with Akka Persistence. This enables Elasticsearch indices to be updated reactively based on persistent events, offering a robust pattern for event-sourced systems.

## Roadmap

Future enhancements include expanding the SQL parser to support additional operations such as `INSERT`, `UPDATE`, and `DELETE`. The long-term vision is to deliver a fully functional, open-source **JDBC connector for Elasticsearch**, empowering users to interact with their data using standard SQL tooling.

## License

This project is open source and licensed under the Apache License 2.0.
