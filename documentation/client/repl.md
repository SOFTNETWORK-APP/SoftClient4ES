[Back to index](README.md)

# 📘 REPL Client — SQL Gateway for Elasticsearch

---

## Introduction

The **SoftClient4ES REPL** (Read-Eval-Print Loop) is an interactive command-line interface for executing SQL statements against Elasticsearch clusters.

It provides:

- **Interactive SQL execution** with immediate feedback
- **Full DDL, DML, and DQL support**
- **Formatted table output** with emojis and execution timing
- **Multi-line statement support**
- **Multiple output formats** (ASCII, JSON, CSV)
- **Stream consumption** for real-time data
- **Version-aware** compatibility (ES6 → ES9)

---

## Table of Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Connection](#connection)
- [Basic Usage](#basic-usage)
- [Statement Execution](#statement-execution)
- [Multi-line Statements](#multi-line-statements)
- [Output Formatting](#output-formatting)
- [Available Commands](#available-commands)
- [SQL Statement Reference](#sql-statement-reference)
- [Examples](#examples)
- [Version Compatibility](#version-compatibility)

---

## Prerequisites

### Java Requirements

| Elasticsearch Version | Minimum Java Version |
|-----------------------|----------------------|
| ES 6                  | Java 11+             |
| ES 7                  | Java 11+             |
| ES 8                  | Java 11+             |
| ES 9                  | Java 17+             |

> The 0.20+ REPL requires **Java 11+** on every ES version: the CLI bundles logback
> 1.5.x and the JOIN engine is built on Apache Arrow 18.x — both ship Java-11
> bytecode. See [Extensions](#extensions-cross-index-joins-materialized-views).

### Network Requirements

- Network access to JFrog repository (`softnetwork.jfrog.io`)

---

## Installation

### Quick Install

#### Linux / macOS

```bash
curl -fsSL https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/install.sh | bash
```

Or download and run manually:

```bash
curl -O https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/install.sh
chmod +x install.sh
./install.sh
```

#### Windows (PowerShell)

```powershell
irm https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/install.ps1 | iex
```

Or download and run manually:

```powershell
Invoke-WebRequest -Uri https://raw.githubusercontent.com/SOFTNETWORK-APP/SoftClient4ES/main/install.ps1 -OutFile install.ps1
.\install.ps1
```

### List Available Versions

Before installing, you can list all available versions for a specific Elasticsearch version:

#### Linux / macOS

```bash
./install.sh --list-versions --es-version 8
```

#### Windows

```powershell
.\install.ps1 -ListVersions -EsVersion 8
```

**Example output:**

```
═══════════════════════════════════════════════════════════════
  Available SoftClient4ES Versions for Elasticsearch 8
═══════════════════════════════════════════════════════════════

  Artifact: softclient4es8-cli-all_2.13
  Java required: 11+

  Versions:

    • 0.20.2
    • 0.20.3

  Total: 2 version(s)

  To install a specific version:
    ./install.sh --es-version 8 --version <version>
```

### Installation Options

| Option            | Linux/Mac                | Windows               | Default                       |
|-------------------|--------------------------|-----------------------|-------------------------------|
| Target directory  | `-t, --target <dir>`     | `-Target <dir>`       | `$HOME/softclient4es`         |
| ES version        | `-e, --es-version <n>`   | `-EsVersion <n>`      | `8`                           |
| Software version  | `-v, --version <ver>`    | `-Version <ver>`      | `latest`                      |
| Scala version     | `-s, --scala <ver>`      | `-ScalaVersion <ver>` | `2.13`                        |
| List versions     | `-l, --list-versions`    | `-ListVersions`       | —                             |

### Installation Examples

#### Linux / macOS

```bash
# Default installation (latest version for ES8)
./install.sh

# List available versions first
./install.sh --list-versions --es-version 8

# Install specific version
./install.sh --es-version 8 --version 0.16.0

# Install for Elasticsearch 9 (requires Java 17+)
./install.sh --es-version 9

# Custom installation directory
./install.sh --target /opt/softclient4es

# Full custom installation
./install.sh --target ~/tools/softclient4es --es-version 7 --version 0.16.0
```

#### Windows

```powershell
# Default installation (latest version for ES8)
.\install.ps1

# List available versions first
.\install.ps1 -ListVersions -EsVersion 8

# Install specific version
.\install.ps1 -EsVersion 8 -Version 0.16.0

# Install for Elasticsearch 9 (requires Java 17+)
.\install.ps1 -EsVersion 9

# Custom installation directory
.\install.ps1 -Target "C:\tools\softclient4es"

# Full custom installation
.\install.ps1 -Target "C:\tools\softclient4es" -EsVersion 7 -Version 0.16.0
```

---

### Directory Structure

After installation:

```
softclient4es/
├── bin/
│   ├── softclient4es           # Linux/Mac launcher
│   ├── softclient4es.bat       # Windows batch launcher
│   └── softclient4es.ps1       # Windows PowerShell launcher
├── conf/
│   ├── application.conf        # Application configuration
│   └── logback.xml             # Logging configuration
├── lib/
│   └── softclient4es8-cli-all_2.13-x.y.z-assembly.jar   # self-contained -all bundle
│                               # (fallback/--no-extensions installs: plain cli jar
│                               #  + extension jars and dependencies, see Extensions)
├── logs/                       # Log files directory
│   └── softclient4es.log       # (created at runtime)
├── LICENSE
├── README.md
├── VERSION
└── uninstall.sh
```

### Default install: the self-contained `-all` bundle

Two REPL artifacts are published per Elasticsearch version:

| Artifact | Contents | License |
|---|---|---|
| `softclient4es{N}-cli` (plain) | the engine assembly only | pure **Apache-2.0** |
| `softclient4es{N}-cli-all` (bundle) | engine + community extensions + arrow JOIN extension + **all** their dependencies in ONE jar | combined — see below |

**The default install downloads the ONE self-contained `-all` bundle** — a single
download, an empty `lib/`, no install-time dependency resolution. Everything —
cross-index JOINs, materialized views, quota enforcement — works out of the box.

The `-all` bundle has its **own version line** (it can be re-released for an
extension hotfix without a new engine release):

- `--list-versions` lists **bundle** versions by default, and **engine** versions
  with `--no-extensions` (the output names the artifact listed);
- `-v <version>` selects a **bundle** version on the default path; an engine-only
  version (one with no matching bundle) falls back to the plain artifact plus the
  coursier-based extensions resolution described below.

The REPL welcome banner and the `version` meta-command disclose the bundle
provenance (bundle version + the exact engine / extension versions), which the jar
also carries in its `MANIFEST.MF` and `softclient4es-bundle-info.properties`.

**License** — the `-all` bundle contains the Apache-2.0 SoftClient4ES engine PLUS
SoftClient4ES extensions under the **Elastic License 2.0** and the **proprietary
cross-index JOIN engine** (free to use; see `licenses/` inside the jar and the
`NOTICE` file). The bundle as a whole is therefore **not** a pure Apache-2.0
artifact. Quota enforcement is active out of the box. For a pure Apache-2.0
install, use `--no-extensions`. Bundle bugs are reported on the
[SoftClient4ES issue tracker](https://github.com/SOFTNETWORK-APP/SoftClient4ES/issues).

### Extensions (cross-index JOINs, materialized views)

Cross-index `JOIN`s and materialized views are delivered as **extensions** — jars
discovered on the classpath through the `ExtensionSpi` ServiceLoader mechanism:

- `softclient4es-arrow-extensions` — the cross-index JOIN engine (**required for
  `SELECT … JOIN` across indices** — the same engine the JDBC driver embeds)
- `softclient4es-community-extensions` — materialized views and quota enforcement

On the default path both ship **inside the `-all` bundle** (see above). The
coursier-based resolution below is the **fallback path** — used when no bundle is
published for the selected version, for engine-only `-v` selections, and for
pre-0.20 engines:

- **community extensions**: always installed — any engine version (matching 0.1.x line
  for engines < 0.20)
- **arrow extensions**: installed for engines ≥ 0.20 (requires Java 11+, like the
  0.20+ CLI itself — Apache Arrow 18.x ships Java-11 bytecode)

Each extension is resolved **with its full dependency closure** (Apache Arrow, DuckDB,
…; ~250 jars) via a bundled [coursier](https://get-coursier.io) resolver into `lib/`.
The launcher runs the REPL with `java -cp "<assembly>:lib/*"`, so every jar in `lib/`
is visible.

To skip extensions entirely:

```bash
./install.sh --no-extensions        # pure Apache-2.0 install — JOINs and MVs unavailable
```

**Manual installation on an existing install** (or air-gapped preparation):

```bash
# Resolve each extension with all its dependencies into lib/
cs fetch --repository https://softnetwork.jfrog.io/artifactory/releases \
  "app.softnetwork.elastic:softclient4es-arrow-extensions_2.13:<version>" \
  "app.softnetwork.elastic:softclient4es-community-extensions_2.13:<version>" \
  | xargs -I{} cp {} ~/softclient4es/lib/
```

> ⚠️ **Copying only the two extension jars into `lib/` is NOT enough** — they are thin
> jars whose engine (Arrow memory, DuckDB, Flight) arrives transitively. Always resolve
> the full dependency closure as above. Installations made with an installer older than
> the 0.20 line launched the REPL with `java -jar`, which ignores `lib/` entirely —
> re-run the installer to get the classpath-based launcher.

The launcher adds the required Arrow `--add-opens` flags automatically on Java 9+.

### Add to PATH

#### Linux / macOS

Add to `~/.bashrc` or `~/.zshrc`:

```bash
export PATH="$PATH:$HOME/softclient4es/bin"
```

#### Windows

```powershell
# Temporary (current session)
$env:PATH += ";$env:USERPROFILE\softclient4es\bin"

# Permanent (requires admin)
[Environment]::SetEnvironmentVariable("PATH", $env:PATH + ";$env:USERPROFILE\softclient4es\bin", "User")
```

### Uninstall

#### Linux / macOS

```bash
~/softclient4es/uninstall.sh
```

#### Windows

```powershell
~\softclient4es\uninstall.ps1
```

---

## Connection

### Configuration Precedence

Connection settings are resolved with the following precedence (highest first):

```text
CLI flag  >  ELASTIC_* environment variable  >  conf/application.conf (-Dconfig.file)  >  built-in defaults (http://localhost:9200)
```

Notes:

- An environment variable set to the **empty string** (or whitespace only) is treated as **unset** — it never masks a value from the configuration file or the defaults.
- Credential values (`username`, `password`, `api-key`, `bearer-token`) are passed through **verbatim**; `scheme`, `host` and `port` values are trimmed.
- `ELASTIC_IP` takes precedence over `ELASTIC_HOST`; the installer-documented `ELASTIC_USERNAME`/`ELASTIC_PASSWORD`/`ELASTIC_API_KEY`/`ELASTIC_BEARER_TOKEN` names take precedence over the library-internal `ELASTIC_CREDENTIALS_*` forms (both keep working).
- When no explicit authentication `method` is configured, the client **auto-detects** it from the supplied credentials: API key > bearer token > basic auth > none.

### Configuration File

The REPL reads default connection settings from `conf/application.conf`:

```hocon
elastic {
  credentials {
    scheme       = "http"
    scheme       = ${?ELASTIC_SCHEME}

    host         = "localhost"
    host         = ${?ELASTIC_HOST}

    port         = 9200
    port         = ${?ELASTIC_PORT}

    username     = ""
    username     = ${?ELASTIC_USERNAME}

    password     = ""
    password     = ${?ELASTIC_PASSWORD}

    api-key      = ""
    api-key      = ${?ELASTIC_API_KEY}

    bearer-token = ""
    bearer-token = ${?ELASTIC_BEARER_TOKEN}
  }
}
```

### Environment Variables

The configuration file supports environment variable overrides:

| Variable              | Description                  |
|-----------------------|------------------------------|
| `ELASTIC_SCHEME`      | Connection scheme            |
| `ELASTIC_HOST`        | Elasticsearch host           |
| `ELASTIC_PORT`        | Elasticsearch port           |
| `ELASTIC_USERNAME`    | Username for authentication  |
| `ELASTIC_PASSWORD`    | Password for authentication  |
| `ELASTIC_API_KEY`     | API key for authentication   |
| `ELASTIC_BEARER_TOKEN`| Bearer token for auth        |
| `JAVA_OPTS`           | JVM options (default: -Xmx512m)|

### Command-Line Options

Command-line options **override** the configuration file settings:

| Option                       | Short | Description                          | Default     |
|------------------------------|-------|--------------------------------------|-------------|
| `--scheme <scheme>`          | `-s`  | Connection scheme (`http` or `https`)| `http`      |
| `--host <host>`              | `-h`  | Elasticsearch host                   | `localhost` |
| `--port <port>`              | `-p`  | Elasticsearch port                   | `9200`      |
| `--username <user>`          | `-u`  | Username for authentication          | —           |
| `--password <pass>`          | `-P`  | Password for authentication          | —           |
| `--api-key <key>`            | `-k`  | API key for authentication           | —           |
| `--bearer-token <token>`     | `-b`  | Bearer token for authentication      | —           |
| `--file <path>`              | `-f`  | Execute SQL from file and exit       | —           |
| `--command <sql>`            | `-c`  | Execute SQL command and exit         | —           |
| `--help`                     |       | Show help message                    | —           |

### Authentication Methods

The REPL supports multiple authentication methods:

| Method           | Options                      | Use Case                    |
|------------------|------------------------------|-----------------------------|
| Basic Auth       | `-u` / `-P`                  | Username/password           |
| API Key          | `-k`                         | Elasticsearch API key       |
| Bearer Token     | `-b`                         | OAuth/JWT token             |

### Connection Examples

```bash
# Local connection (uses defaults from application.conf)
softclient4es

# Override host and port
softclient4es -h es.example.com -p 9200

# HTTPS with basic authentication
softclient4es -s https -h es.example.com -p 9243 -u admin -P secret

# Using API key
softclient4es -s https -h es.example.com -k "your-api-key"

# Using bearer token
softclient4es -s https -h es.example.com -b "your-bearer-token"

# Execute a single command and exit
softclient4es -c "SHOW TABLES"

# Execute SQL from a file and exit
softclient4es -f /path/to/script.sql

# Combine options
softclient4es -h es.example.com -u admin -P secret -c "SELECT * FROM users LIMIT 10"
```

### Non-Interactive Mode

The REPL can run in non-interactive mode using `-c` or `-f`:

#### Execute a single command

```bash
softclient4es -c "SELECT COUNT(*) FROM users"
```

#### Execute SQL from a file

```bash
softclient4es -f setup.sql
```

The file can contain multiple statements separated by semicolons:

```sql
-- setup.sql
CREATE TABLE IF NOT EXISTS users (
  id INT NOT NULL,
  name KEYWORD,
  PRIMARY KEY (id)
);

INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');

SELECT * FROM users;
```

#### Cold-start & batch invocation cost

Each `softclient4es -c "…"` invocation starts a fresh JVM and pays the full
initialization cost (class loading, client setup, extension discovery) before the
statement runs. For scripted or CI use, prefer batching statements into **one**
invocation — both forms below execute all `;`-separated statements in a single JVM,
so the startup cost is paid once:

```bash
# Recommended: a SQL file (N statements, one JVM)
softclient4es -f batch.sql

# Also works: several statements in one -c
softclient4es -c "CREATE TABLE t (id INT); INSERT INTO t (id) VALUES (1); SELECT * FROM t"
```

In practice the marginal cost of each extra statement is a few milliseconds, versus
1.5–2s of JVM startup for every separate invocation.

On bundle installs (the default self-contained `-all` jar) running on JDK 13+, the
launcher additionally uses [Class-Data Sharing (AppCDS)](https://docs.oracle.com/en/java/javase/17/vm/class-data-sharing.html)
to cut class-loading time (~20-25% faster start on JDK 17/21):

- **JDK 13-18** — the installer generates `cache/softclient4es.jsa` once at install
  time. After a Java upgrade, re-run the installer to regenerate it.
- **JDK 19+** — the JVM creates and refreshes the archive by itself
  (`-XX:+AutoCreateSharedArchive`); nothing to do.
- The archive is an internal cache: it is safe to delete `cache/` at any time (the
  REPL falls back to normal class loading, and JDK 19+ recreates it on the next run).
- Plain and `--no-extensions` installs are unaffected — no CDS flags, no `cache/`
  directory.

---

## Basic Usage

### Prompt

Once connected, you will see the REPL prompt:

```
sql>
```

### Executing a Statement

Type your SQL statement and press **Enter**:

```
sql> SHOW TABLES;
```

### Exiting

To exit the REPL:

```
sql> exit
```

Or use `Ctrl+D` or `\q`.

---

## Statement Execution

### Single-line Statements

Simple statements can be entered on a single line:

```
sql> SELECT * FROM users LIMIT 10;
```

### Statement Terminator

Statements must end with a **semicolon** (`;`):

```
sql> SHOW TABLES;
```

---

## Multi-line Statements

For complex statements, the REPL supports multi-line input.

Continue typing on the next line until you enter the semicolon:

```
sql> CREATE TABLE users (
   ->   id INT NOT NULL,
   ->   name VARCHAR,
   ->   birthdate DATE,
   ->   PRIMARY KEY (id)
   -> );
```

The prompt changes to `->` to indicate continuation mode.

---

## Output Formatting

### Table Output (ASCII)

Query results are displayed as formatted tables by default:

```
sql> SHOW TABLES LIKE 'show_%';

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| show_users | REGULAR | id |             |
📊 1 row(s) (7ms)
```

### Output Formats

The REPL supports multiple output formats:

| Format  | Description                    |
|---------|--------------------------------|
| `ascii` | Formatted ASCII table (default)|
| `json`  | JSON output                    |
| `csv`   | Comma-separated values         |

Change format using the `format` command:

```
sql> format json
Current format: Json

sql> SELECT * FROM users LIMIT 1;
{"id":1,"name":"Alice","age":30}
```

### DML Results

DML statements return operation counts:

```
sql> INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob');
📊 inserted: 2, updated: 0, deleted: 0, rejected: 0 (15ms)
```

### Timing Information

All statements display execution time by default:

```
📊 6 row(s) (1ms)
```

Toggle timing display:

```
sql> timing
Timing: OFF

sql> timing
Timing: ON
```

---

## Available Commands

### General Commands

| Command       | Shortcut | Description                          |
|---------------|----------|--------------------------------------|
| `help`        | `\h`     | Display help information             |
| `quit`        | `\q`     | Exit the REPL                        |
| `exit`        | `\q`     | Exit the REPL                        |
| `history`     |          | Display command history              |
| `clear`       |          | Clear the screen                     |
| `timing`      |          | Toggle timing display ON/OFF         |
| `format`      |          | Set or show output format            |
| `timeout`     |          | Set or show query timeout            |

### Table Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `tables`           | `\t`     | List all tables (`SHOW TABLES`)      |
| `\st <table>`      |          | Show table details                   |
| `\ct <table>`      |          | Show CREATE TABLE statement          |
| `\dt <table>`      |          | Describe table schema                |

### Pipeline Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `pipelines`        | `\p`     | List all pipelines (`SHOW PIPELINES`)|
| `\sp <pipeline>`   |          | Show pipeline details                |
| `\cp <pipeline>`   |          | Show CREATE PIPELINE statement       |
| `\dp <pipeline>`   |          | Describe pipeline processors         |

### Watcher Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `watchers`         | `\w`     | List all watchers (`SHOW WATCHERS`)  |
| `\sw <watcher>`    |          | Show watcher status                  |

### Enrich Policy Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `policies`         | `\pol`   | List all enrich policies             |
| `\spol <policy>`   |          | Show enrich policy details           |

### Stream Commands

| Command            | Shortcut | Description                          |
|--------------------|----------|--------------------------------------|
| `consume`          | `\c`     | Start consuming a stream             |
| `stream`           | `\s`     | Show stream status                   |
| `cancel`           | `\x`     | Cancel active stream                 |

---

### Command Details

#### `format [ascii|json|csv]`

Set the output format or display current format:

```
sql> format
Current format: Ascii

sql> format json
Current format: Json

sql> format csv
Current format: Csv
```

#### `timeout [seconds]`

Set the query timeout or display current timeout:

```
sql> timeout
Current timeout: 30s

sql> timeout 60
Timeout set to 60s
```

#### `\st <table_name>`

Show detailed table information:

```
sql> \st users

📋 Table: users [Regular]
...
```

#### `\ct <table_name>`

Show the CREATE TABLE DDL:

```
sql> \ct users

CREATE OR REPLACE TABLE users (
  id INT NOT NULL,
  ...
)
```

#### `\dt <table_name>`

Describe the table schema:

```
sql> \dt users

| Field | Type    | Null | Key | Default | Comment | Script | Extra |
|-------|---------|------|-----|---------|---------|--------|-------|
| id    | INT     | no   | PRI | NULL    |         |        | ()    |
...
```

---

## SQL Statement Reference

The REPL supports the full SQL Gateway syntax.

### DDL Statements

| Statement                  | Description                    |
|----------------------------|--------------------------------|
| `CREATE TABLE`             | Create a new table             |
| `CREATE OR REPLACE TABLE`  | Create or replace a table      |
| `CREATE TABLE AS SELECT`   | Create table from query        |
| `ALTER TABLE`              | Modify table structure         |
| `DROP TABLE`               | Delete a table                 |
| `TRUNCATE TABLE`           | Remove all documents           |
| `CREATE PIPELINE`          | Create an ingest pipeline      |
| `ALTER PIPELINE`           | Modify a pipeline              |
| `DROP PIPELINE`            | Delete a pipeline              |
| `CREATE WATCHER`           | Create a watcher               |
| `DROP WATCHER`             | Delete a watcher               |
| `CREATE ENRICH POLICY`     | Create an enrich policy        |
| `EXECUTE ENRICH POLICY`    | Execute an enrich policy       |
| `DROP ENRICH POLICY`       | Delete an enrich policy        |

### DML Statements

| Statement                       | Description              |
|---------------------------------|--------------------------|
| `INSERT INTO ... VALUES`        | Insert documents         |
| `INSERT INTO ... AS SELECT`     | Insert from query        |
| `UPDATE ... SET ... WHERE`      | Update documents         |
| `DELETE FROM ... WHERE`         | Delete documents         |
| `COPY INTO ... FROM`            | Bulk load from file      |

### DQL Statements

| Statement              | Description                     |
|------------------------|---------------------------------|
| `SELECT`               | Query documents                 |
| `SHOW TABLES`          | List all tables                 |
| `SHOW TABLE`           | Show table details              |
| `SHOW CREATE TABLE`    | Show table DDL                  |
| `DESCRIBE TABLE`       | Describe table schema           |
| `SHOW PIPELINES`       | List all pipelines              |
| `SHOW PIPELINE`        | Show pipeline details           |
| `SHOW CREATE PIPELINE` | Show pipeline DDL               |
| `DESCRIBE PIPELINE`    | Describe pipeline processors    |
| `SHOW WATCHERS`        | List all watchers               |
| `SHOW WATCHER STATUS`  | Show watcher status             |
| `SHOW ENRICH POLICIES` | List all enrich policies        |
| `SHOW ENRICH POLICY`   | Show enrich policy details      |

---

## Examples

### Create and Query a Table

```
sql> CREATE TABLE IF NOT EXISTS demo_users (
   |   id INT NOT NULL,
   |   name VARCHAR,
   |   age INT,
   |   PRIMARY KEY (id)
   | );
✔ Table created (120ms)

sql> INSERT INTO demo_users (id, name, age) VALUES
   |   (1, 'Alice', 30),
   |   (2, 'Bob', 25),
   |   (3, 'Chloe', 35);
📊 inserted: 3, updated: 0, deleted: 0, rejected: 0 (45ms)

sql> SELECT * FROM demo_users ORDER BY age DESC;

| id | name  | age |
|----|-------|-----|
| 3  | Chloe | 35  |
| 1  | Alice | 30  |
| 2  | Bob   | 25  |
📊 3 row(s) (8ms)
```

### Using Shortcut Commands

```
sql> tables

| name       | type    | pk | partitioned |
|------------|---------|----|-------------|
| demo_users | REGULAR | id |             |
📊 1 row(s) (5ms)

sql> \dt demo_users

| Field | Type    | Null | Key | Default | Comment | Script | Extra |
|-------|---------|------|-----|---------|---------|--------|-------|
| id    | INT     | no   | PRI | NULL    |         |        | ()    |
| name  | VARCHAR | yes  |     | NULL    |         |        | ()    |
| age   | INT     | yes  |     | NULL    |         |        | ()    |
📊 3 row(s) (5ms)

sql> \ct demo_users

CREATE OR REPLACE TABLE demo_users (
	id INT NOT NULL,
	name VARCHAR,
	age INT,
	PRIMARY KEY (id)
)
OPTIONS = (...)
```

### Change Output Format

```
sql> format json

sql> SELECT * FROM demo_users WHERE id = 1;
{"id":1,"name":"Alice","age":30}
📊 1 row(s) (3ms)

sql> format csv

sql> SELECT * FROM demo_users;
id,name,age
1,Alice,30
2,Bob,25
3,Chloe,35
📊 3 row(s) (4ms)

sql> format ascii
```

### Pipeline Inspection

```
sql> pipelines

| name                              | processors_count |
|-----------------------------------|------------------|
| demo_users_ddl_default_pipeline   | 1                |
📊 1 row(s) (3ms)

sql> \dp demo_users_ddl_default_pipeline

| processor_type | description        | field | ignore_failure | options                                        |
|----------------|--------------------|-------|----------------|------------------------------------------------|
| set            | PRIMARY KEY (id)   | _id   | no             | (value = "{{id}}", ignore_empty_value = false) |
📊 1 row(s) (2ms)
```

### Watcher and Policy Commands

```
sql> watchers

| id          | active | status  | ... |
|-------------|--------|---------|-----|
| my_watcher  | true   | Healthy | ... |
📊 1 row(s) (9ms)

sql> \sw my_watcher

| id          | active | status  | execution_status | ... |
|-------------|--------|---------|------------------|-----|
| my_watcher  | true   | Healthy | Executed         | ... |
📊 1 row(s) (5ms)

sql> policies

| name      | type  | indices   | match_field | enrich_fields |
|-----------|-------|-----------|-------------|---------------|
| my_policy | match | dql_users | id          | name,email    |
📊 1 row(s) (4ms)
```

### Cleanup

```
sql> DROP TABLE IF EXISTS demo_users;
✔ Table dropped (35ms)

sql> exit
Goodbye!
```

---

## Version Compatibility

| Feature           | ES6  | ES7  | ES8  | ES9  |
|-------------------|------|------|------|------|
| REPL Client       | ✔    | ✔    | ✔    | ✔    |
| DDL Statements    | ✔    | ✔    | ✔    | ✔    |
| DML Statements    | ✔    | ✔    | ✔    | ✔    |
| DQL Statements    | ✔    | ✔    | ✔    | ✔    |
| Watchers          | ✔    | ✔    | ✔    | ✔    |
| Enrich Policies   | ✖    | ✔*   | ✔    | ✔    |

\* Enrich policies require ES 7.5+

---

## Telemetry

The REPL sends one anonymous usage ping per session (no IP, no SQL, no command text). Opt out with `-Dsoftclient4es.telemetry.enabled=false`. See [Telemetry & Privacy](telemetry.md) for details.

---

[Back to index](README.md)